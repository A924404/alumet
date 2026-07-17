use std::{os::raw::c_ulong, sync::Arc, time::Duration};

use alumet::{
    measurement::{MeasurementAccumulator, MeasurementPoint, Timestamp},
    metrics::TypedMetricId,
    pipeline::{
        Source,
        control::request,
        elements::{error::PollError, source::trigger::TriggerSpec},
    },
    plugin::{
        AlumetPluginStart, AlumetPostStart, ConfigTable,
        event,
        rust::{AlumetPlugin, deserialize_config, serialize_config},
    },
    resources::{Resource, ResourceConsumer},
    units::Unit,
};
use anyhow::Context;
use papi_wrap::{
    counter::Counter,
    events_set::EventsSet,
};
use serde::{Deserialize, Serialize};

#[cfg(not(target_os = "linux"))]
compile_error!("This plugin only works on Linux.");

pub struct PapiPlugin {
    config: Config,
    shared: Option<Arc<PapiSharedState>>,
}

struct PapiSharedState {
    counters: Vec<Counter>,
    metrics: PapiMetrics,
    poll_interval: Duration,
    flush_interval: Duration,
}

impl AlumetPlugin for PapiPlugin {
    fn name() -> &'static str {
        "papi"
    }

    fn version() -> &'static str {
        env!("CARGO_PKG_VERSION")
    }

    fn default_config() -> anyhow::Result<Option<ConfigTable>> {
        Ok(Some(serialize_config(Config::default())?))
    }

    fn init(config: ConfigTable) -> anyhow::Result<Box<Self>> {
        let config = deserialize_config(config)?;
        Ok(Box::new(Self { config, shared: None }))
    }

    fn start(&mut self, alumet: &mut AlumetPluginStart) -> anyhow::Result<()> {
        papi_wrap::initialize(true).map_err(|e| anyhow::anyhow!("failed to initialize PAPI: {e:?}"))?;

        let metric_sp_ops = alumet.create_metric::<u64>(
            "papi_sp_ops",
            Unit::Unity,
            "PAPI single-precision floating-point operations (PAPI_SP_OPS)",
        )?;
        let metric_vec_sp = alumet.create_metric::<u64>(
            "papi_vec_sp",
            Unit::Unity,
            "PAPI vectorized single-precision floating-point operations (PAPI_VEC_SP)",
        )?;
        let metric_dp_ops = alumet.create_metric::<u64>(
            "papi_dp_ops",
            Unit::Unity,
            "PAPI double-precision floating-point operations (PAPI_DP_OPS)",
        )?;
        let metric_vec_dp = alumet.create_metric::<u64>(
            "papi_vec_dp",
            Unit::Unity,
            "PAPI vectorized double-precision floating-point operations (PAPI_VEC_DP)",
        )?;

        let counters = vec![
            Counter::from_name("PAPI_SP_OPS").map_err(|e| anyhow::anyhow!("PAPI_SP_OPS unavailable: {e:?}"))?,
            Counter::from_name("PAPI_VEC_SP").map_err(|e| anyhow::anyhow!("PAPI_VEC_SP unavailable: {e:?}"))?,
            Counter::from_name("PAPI_DP_OPS").map_err(|e| anyhow::anyhow!("PAPI_DP_OPS unavailable: {e:?}"))?,
            Counter::from_name("PAPI_VEC_DP").map_err(|e| anyhow::anyhow!("PAPI_VEC_DP unavailable: {e:?}"))?,
        ];

        let metrics = PapiMetrics {
            sp_ops: metric_sp_ops,
            vec_sp: metric_vec_sp,
            dp_ops: metric_dp_ops,
            vec_dp: metric_vec_dp,
        };

        self.shared = Some(Arc::new(PapiSharedState {
            counters,
            metrics,
            poll_interval: self.config.poll_interval,
            flush_interval: self.config.flush_interval,
        }));
        Ok(())
    }

    fn post_pipeline_start(&mut self, alumet: &mut AlumetPostStart) -> anyhow::Result<()> {
        let shared = self.shared.clone().context("shared state not initialized")?;
        let pipeline_control = alumet.pipeline_control();
        let runtime = alumet.async_runtime().clone();

        event::start_consumer_measurement().subscribe(move |e| {
            for consumer in e.0 {
                if let ResourceConsumer::Process { pid } = consumer {
                    let source = PapiSource {
                        counters: shared.counters.clone(),
                        event_set: None,
                        started: false,
                        target_pid: Some(pid),
                        resource: Resource::LocalMachine,
                        consumer: ResourceConsumer::Process { pid },
                        metrics: shared.metrics.clone(),
                    };
                    let trigger = TriggerSpec::builder(shared.poll_interval)
                        .flush_interval(shared.flush_interval)
                        .build()?;
                    let source_name = format!("papi-pid[{pid}]");
                    let req = request::create_one().add_source(&source_name, Box::new(source), trigger);
                    runtime.block_on(pipeline_control.dispatch(req, Duration::from_secs(1)))?;
                    log::debug!("Started PAPI source for pid {pid}.");
                }
            }
            Ok(())
        });
        Ok(())
    }

    fn stop(&mut self) -> anyhow::Result<()> {
        Ok(())
    }
}

#[derive(Clone)]
struct PapiMetrics {
    sp_ops: TypedMetricId<u64>,
    vec_sp: TypedMetricId<u64>,
    dp_ops: TypedMetricId<u64>,
    vec_dp: TypedMetricId<u64>,
}

struct PapiSource {
    counters: Vec<Counter>,
    event_set: Option<EventsSet>,
    started: bool,
    target_pid: Option<u32>,
    resource: Resource,
    consumer: ResourceConsumer,
    metrics: PapiMetrics,
}

impl PapiSource {
    fn map_err(e: papi_wrap::PapiError) -> PollError {
        PollError::Fatal(anyhow::anyhow!("PAPI runtime error: {e:?}"))
    }

    fn event_set_mut(&mut self) -> Result<&mut EventsSet, PollError> {
        if self.event_set.is_none() {
            let mut set = EventsSet::new(&self.counters).map_err(Self::map_err)?;
            if let Some(pid) = self.target_pid {
                set.attach(pid as c_ulong).map_err(Self::map_err)?;
            }
            self.event_set = Some(set);
        }
        self.event_set
            .as_mut()
            .context("internal error: PAPI event set not initialized")
            .map_err(PollError::Fatal)
    }
}

impl Source for PapiSource {
    fn poll(&mut self, acc: &mut MeasurementAccumulator, t: Timestamp) -> Result<(), PollError> {
        if !self.started {
            self.event_set_mut()?.start().map_err(Self::map_err)?;
            self.started = true;
            return Ok(());
        }

        let event_set = self.event_set_mut()?;
        let counters = event_set.read().map_err(Self::map_err)?.to_vec();
        event_set.reset().map_err(Self::map_err)?;

        let deltas: Vec<u64> = counters
            .iter()
            .map(|value| (*value).max(0) as u64)
            .collect();

        let resource = self.resource.clone();
        let consumer = self.consumer.clone();
        acc.push(MeasurementPoint::new(
            t,
            self.metrics.sp_ops,
            resource.clone(),
            consumer.clone(),
            deltas[0],
        ));
        acc.push(MeasurementPoint::new(
            t,
            self.metrics.vec_sp,
            resource.clone(),
            consumer.clone(),
            deltas[1],
        ));
        acc.push(MeasurementPoint::new(
            t,
            self.metrics.dp_ops,
            resource.clone(),
            consumer.clone(),
            deltas[2],
        ));
        acc.push(MeasurementPoint::new(
            t,
            self.metrics.vec_dp,
            resource,
            consumer,
            deltas[3],
        ));

        Ok(())
    }
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Config {
    #[serde(with = "humantime_serde")]
    poll_interval: Duration,
    #[serde(with = "humantime_serde")]
    flush_interval: Duration,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            poll_interval: Duration::from_secs(1),
            flush_interval: Duration::from_secs(5),
        }
    }
}
