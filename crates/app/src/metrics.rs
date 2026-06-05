use prometheus::{
    Encoder, GaugeVec, HistogramOpts, HistogramVec, IntCounter, IntCounterVec, IntGauge,
    IntGaugeVec, Opts, Registry, TextEncoder,
};
use std::fmt;

#[derive(Debug)]
#[allow(dead_code)]
pub struct Metrics {
    registry: Registry,
    batch_write_latency: HistogramVec,
    channel_depth: IntGaugeVec,
    channel_capacity: IntGaugeVec,
    channel_utilization: GaugeVec,
    ingest_events_total: IntCounterVec,
    last_observed_slot: IntGauge,
    last_finalized_slot: IntGauge,
    last_persisted_slot: IntGauge,
    slot_lag: IntGauge,
    reconnect_attempts_total: IntCounter,
    decode_errors_total: IntCounter,
}

#[derive(Debug)]
pub enum MetricsInitError {
    Prometheus(prometheus::Error),
}

impl fmt::Display for MetricsInitError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Prometheus(err) => write!(f, "failed to initialize prometheus metric: {err}"),
        }
    }
}

impl std::error::Error for MetricsInitError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Prometheus(err) => Some(err),
        }
    }
}

#[allow(dead_code)]
impl Metrics {
    pub fn new() -> Result<Self, MetricsInitError> {
        let registry = Registry::new();

        let buckets = prometheus::exponential_buckets(0.001, 2.0, 15)
            .map_err(MetricsInitError::Prometheus)?;

        let batch_write_latency = HistogramVec::new(
            HistogramOpts::new(
                "solana_stream_batch_write_latency_seconds",
                "Batch write latency in seconds.",
            )
            .buckets(buckets),
            &["writer"],
        )
        .map_err(MetricsInitError::Prometheus)?;
        registry
            .register(Box::new(batch_write_latency.clone()))
            .map_err(MetricsInitError::Prometheus)?;

        let channel_depth = IntGaugeVec::new(
            Opts::new("solana_stream_channel_depth", "Current channel depth."),
            &["stream_name"],
        )
        .map_err(MetricsInitError::Prometheus)?;
        registry
            .register(Box::new(channel_depth.clone()))
            .map_err(MetricsInitError::Prometheus)?;

        let channel_capacity = IntGaugeVec::new(
            Opts::new("solana_stream_channel_capacity", "Channel capacity."),
            &["stream_name"],
        )
        .map_err(MetricsInitError::Prometheus)?;
        registry
            .register(Box::new(channel_capacity.clone()))
            .map_err(MetricsInitError::Prometheus)?;

        let channel_utilization = GaugeVec::new(
            Opts::new(
                "solana_stream_channel_utilization_ratio",
                "Channel utilization ratio (depth / capacity).",
            ),
            &["stream_name"],
        )
        .map_err(MetricsInitError::Prometheus)?;
        registry
            .register(Box::new(channel_utilization.clone()))
            .map_err(MetricsInitError::Prometheus)?;

        let ingest_events_total = IntCounterVec::new(
            Opts::new(
                "solana_stream_ingest_events_total",
                "Total ingested events by source and type.",
            ),
            &["source", "event_type"],
        )
        .map_err(MetricsInitError::Prometheus)?;
        registry
            .register(Box::new(ingest_events_total.clone()))
            .map_err(MetricsInitError::Prometheus)?;

        let last_observed_slot = IntGauge::new(
            "solana_stream_last_observed_slot",
            "Last observed slot from the stream.",
        )
        .map_err(MetricsInitError::Prometheus)?;
        registry
            .register(Box::new(last_observed_slot.clone()))
            .map_err(MetricsInitError::Prometheus)?;

        let last_finalized_slot = IntGauge::new(
            "solana_stream_last_finalized_slot",
            "Last finalized slot seen by the pipeline.",
        )
        .map_err(MetricsInitError::Prometheus)?;
        registry
            .register(Box::new(last_finalized_slot.clone()))
            .map_err(MetricsInitError::Prometheus)?;

        let last_persisted_slot = IntGauge::new(
            "solana_stream_last_persisted_slot",
            "Last persisted Solana slot.",
        )
        .map_err(MetricsInitError::Prometheus)?;
        registry
            .register(Box::new(last_persisted_slot.clone()))
            .map_err(MetricsInitError::Prometheus)?;

        let slot_lag = IntGauge::new(
            "solana_stream_slot_lag",
            "Difference between last observed slot and last persisted slot.",
        )
        .map_err(MetricsInitError::Prometheus)?;
        registry
            .register(Box::new(slot_lag.clone()))
            .map_err(MetricsInitError::Prometheus)?;

        let reconnect_attempts_total = IntCounter::new(
            "solana_stream_reconnect_attempts_total",
            "Total Yellowstone reconnect attempts.",
        )
        .map_err(MetricsInitError::Prometheus)?;
        registry
            .register(Box::new(reconnect_attempts_total.clone()))
            .map_err(MetricsInitError::Prometheus)?;

        let decode_errors_total = IntCounter::new(
            "solana_stream_decode_errors_total",
            "Total malformed Yellowstone updates skipped.",
        )
        .map_err(MetricsInitError::Prometheus)?;
        registry
            .register(Box::new(decode_errors_total.clone()))
            .map_err(MetricsInitError::Prometheus)?;

        Ok(Self {
            registry,
            batch_write_latency,
            channel_depth,
            channel_capacity,
            channel_utilization,
            ingest_events_total,
            last_observed_slot,
            last_finalized_slot,
            last_persisted_slot,
            slot_lag,
            reconnect_attempts_total,
            decode_errors_total,
        })
    }

    pub fn observe_batch_write(&self, writer: &str, duration_secs: f64) {
        self.batch_write_latency
            .with_label_values(&[writer])
            .observe(duration_secs);
    }

    pub fn set_channel_state(&self, stream_name: &str, depth: usize, capacity: usize) {
        let depth_i64 = i64::try_from(depth).unwrap_or(i64::MAX);
        let capacity_i64 = i64::try_from(capacity).unwrap_or(i64::MAX);
        self.channel_depth
            .with_label_values(&[stream_name])
            .set(depth_i64);
        self.channel_capacity
            .with_label_values(&[stream_name])
            .set(capacity_i64);
        let ratio = if capacity == 0 {
            0.0
        } else {
            depth as f64 / capacity as f64
        };
        self.channel_utilization
            .with_label_values(&[stream_name])
            .set(ratio);
    }

    pub fn record_ingest_event(&self, source: &str, event_type: &str) {
        self.ingest_events_total
            .with_label_values(&[source, event_type])
            .inc();
    }

    pub fn set_last_observed_slot(&self, slot: u64) {
        let value = i64::try_from(slot).unwrap_or(i64::MAX);
        self.last_observed_slot.set(value);
    }

    pub fn set_last_finalized_slot(&self, slot: u64) {
        let value = i64::try_from(slot).unwrap_or(i64::MAX);
        self.last_finalized_slot.set(value);
    }

    pub fn set_last_persisted_slot(&self, slot: u64) {
        let value = i64::try_from(slot).unwrap_or(i64::MAX);
        self.last_persisted_slot.set(value);
    }

    pub fn set_slot_lag(&self, lag: u64) {
        let value = i64::try_from(lag).unwrap_or(i64::MAX);
        self.slot_lag.set(value);
    }

    pub fn last_observed_slot_value(&self) -> i64 {
        self.last_observed_slot.get()
    }

    pub fn last_persisted_slot_value(&self) -> i64 {
        self.last_persisted_slot.get()
    }

    pub fn inc_reconnect_attempts(&self) {
        self.reconnect_attempts_total.inc();
    }

    pub fn inc_decode_errors(&self) {
        self.decode_errors_total.inc();
    }

    pub fn render(&self) -> String {
        let encoder = TextEncoder::new();
        let metric_families = self.registry.gather();
        let mut buffer = Vec::new();
        if let Err(err) = encoder.encode(&metric_families, &mut buffer) {
            return format!("# ERROR encoding metrics: {err}\n");
        }
        String::from_utf8_lossy(&buffer).into_owned()
    }
}
