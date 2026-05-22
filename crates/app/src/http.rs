use axum::{Json, Router, extract::State, http::header, response::IntoResponse, routing::get};
use serde::Serialize;
use solana_yellowstone_stream::pipeline::PipelineSummary;
use std::{fmt, sync::Arc};
use tokio::net::TcpListener;

const METRICS_CONTENT_TYPE: &str = "text/plain; version=0.0.4; charset=utf-8";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum HealthState {
    Ready,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct StatusSnapshot {
    pub health: HealthState,
    pub stream_name: String,
    pub last_persisted_slot: Option<u64>,
    pub events_seen: usize,
    pub events_skipped: usize,
    pub batches_written: usize,
    pub events_attempted: usize,
    pub events_inserted: usize,
    pub events_deduplicated: usize,
}

impl StatusSnapshot {
    pub fn from_pipeline(stream_name: impl Into<String>, summary: PipelineSummary) -> Self {
        Self {
            health: HealthState::Ready,
            stream_name: stream_name.into(),
            last_persisted_slot: summary.last_persisted_slot,
            events_seen: summary.events_seen,
            events_skipped: summary.events_skipped,
            batches_written: summary.batches_written,
            events_attempted: summary.write_summary.attempted,
            events_inserted: summary.write_summary.inserted,
            events_deduplicated: summary.write_summary.deduplicated,
        }
    }
}

#[derive(Debug)]
pub enum HttpError {
    Bind(std::io::Error),
    Serve(std::io::Error),
}

impl fmt::Display for HttpError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Bind(err) => write!(f, "failed to bind http listener: {err}"),
            Self::Serve(err) => write!(f, "http server failed: {err}"),
        }
    }
}

impl std::error::Error for HttpError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Bind(err) | Self::Serve(err) => Some(err),
        }
    }
}

pub async fn serve(addr: &str, status: StatusSnapshot) -> Result<(), HttpError> {
    let listener = TcpListener::bind(addr).await.map_err(HttpError::Bind)?;
    axum::serve(listener, router(status))
        .await
        .map_err(HttpError::Serve)
}

fn router(snapshot: StatusSnapshot) -> Router {
    let state = Arc::new(snapshot);

    Router::new()
        .route("/healthz", get(healthz))
        .route("/readyz", get(readyz))
        .route("/status", get(status_handler))
        .route("/metrics", get(metrics_handler))
        .with_state(state)
}

async fn healthz() -> &'static str {
    "ok"
}

async fn readyz(State(status): State<Arc<StatusSnapshot>>) -> Json<ReadyResponse> {
    Json(ReadyResponse {
        ready: status.health == HealthState::Ready,
    })
}

async fn status_handler(State(status): State<Arc<StatusSnapshot>>) -> Json<StatusSnapshot> {
    Json((*status).clone())
}

async fn metrics_handler(State(status): State<Arc<StatusSnapshot>>) -> impl IntoResponse {
    (
        [(header::CONTENT_TYPE, METRICS_CONTENT_TYPE)],
        render_metrics(status.as_ref()),
    )
}

fn render_metrics(status: &StatusSnapshot) -> String {
    let mut output = String::new();
    let stream_name = escape_label_value(&status.stream_name);
    let info_labels = format!(r#"{{stream_name="{stream_name}"}}"#);

    push_gauge(
        &mut output,
        "solana_stream_info",
        "Static stream information.",
        &info_labels,
        1,
    );

    for metric in replay_counters(status) {
        push_counter(&mut output, metric.name, metric.help, metric.value);
    }

    for metric in cursor_gauges(status) {
        push_gauge(&mut output, metric.name, metric.help, "", metric.value);
    }

    output
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct UsizeMetric {
    name: &'static str,
    help: &'static str,
    value: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct U64Metric {
    name: &'static str,
    help: &'static str,
    value: u64,
}

fn replay_counters(status: &StatusSnapshot) -> [UsizeMetric; 6] {
    [
        UsizeMetric {
            name: "solana_stream_events_seen_total",
            help: "Total replay events seen.",
            value: status.events_seen,
        },
        UsizeMetric {
            name: "solana_stream_events_skipped_total",
            help: "Total replay events skipped because of cursor resume.",
            value: status.events_skipped,
        },
        UsizeMetric {
            name: "solana_stream_batches_written_total",
            help: "Total batches written to storage.",
            value: status.batches_written,
        },
        UsizeMetric {
            name: "solana_stream_events_attempted_total",
            help: "Total events attempted for storage writes.",
            value: status.events_attempted,
        },
        UsizeMetric {
            name: "solana_stream_events_inserted_total",
            help: "Total events inserted into storage.",
            value: status.events_inserted,
        },
        UsizeMetric {
            name: "solana_stream_events_deduplicated_total",
            help: "Total events deduplicated by storage.",
            value: status.events_deduplicated,
        },
    ]
}

fn cursor_gauges(status: &StatusSnapshot) -> Vec<U64Metric> {
    let mut metrics = vec![U64Metric {
        name: "solana_stream_cursor_available",
        help: "Whether a persisted cursor slot is available.",
        value: u64::from(status.last_persisted_slot.is_some()),
    }];

    if let Some(slot) = status.last_persisted_slot {
        metrics.push(U64Metric {
            name: "solana_stream_last_persisted_slot",
            help: "Last persisted Solana slot.",
            value: slot,
        });
    }

    metrics
}

fn push_counter(output: &mut String, name: &str, help: &str, value: usize) {
    push_metric(output, name, help, "counter", "", value);
}

fn push_gauge<T>(output: &mut String, name: &str, help: &str, labels: &str, value: T)
where
    T: std::fmt::Display,
{
    push_metric(output, name, help, "gauge", labels, value);
}

fn push_metric<T>(
    output: &mut String,
    name: &str,
    help: &str,
    metric_type: &str,
    labels: &str,
    value: T,
) where
    T: std::fmt::Display,
{
    output.push_str("# HELP ");
    output.push_str(name);
    output.push(' ');
    output.push_str(help);
    output.push_str("\n# TYPE ");
    output.push_str(name);
    output.push(' ');
    output.push_str(metric_type);
    output.push('\n');
    output.push_str(name);
    output.push_str(labels);
    output.push(' ');
    output.push_str(&value.to_string());
    output.push_str("\n\n");
}

fn escape_label_value(value: &str) -> String {
    value
        .replace('\\', r#"\\"#)
        .replace('\n', r#"\n"#)
        .replace('"', r#"\""#)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
struct ReadyResponse {
    ready: bool,
}

#[cfg(test)]
mod tests {
    use super::{HealthState, StatusSnapshot, render_metrics};
    use solana_yellowstone_storage::WriteSummary;
    use solana_yellowstone_stream::pipeline::PipelineSummary;

    #[test]
    fn builds_status_snapshot_from_pipeline_summary() {
        let summary = PipelineSummary {
            events_seen: 10,
            events_skipped: 4,
            batches_written: 2,
            write_summary: WriteSummary {
                attempted: 6,
                inserted: 5,
                deduplicated: 1,
            },
            last_persisted_slot: Some(42),
        };

        let status = StatusSnapshot::from_pipeline("replay", summary);

        assert_eq!(status.health, HealthState::Ready);
        assert_eq!(status.stream_name, "replay");
        assert_eq!(status.last_persisted_slot, Some(42));
        assert_eq!(status.events_seen, 10);
        assert_eq!(status.events_skipped, 4);
        assert_eq!(status.events_attempted, 6);
        assert_eq!(status.events_inserted, 5);
        assert_eq!(status.events_deduplicated, 1);
    }

    #[test]
    fn renders_prometheus_text_metrics() {
        let summary = PipelineSummary {
            events_seen: 10,
            events_skipped: 4,
            batches_written: 2,
            write_summary: WriteSummary {
                attempted: 6,
                inserted: 5,
                deduplicated: 1,
            },
            last_persisted_slot: Some(42),
        };
        let status = StatusSnapshot::from_pipeline("replay", summary);

        let metrics = render_metrics(&status);

        assert!(metrics.contains("# TYPE solana_stream_events_seen_total counter"));
        assert!(metrics.contains("solana_stream_info{stream_name=\"replay\"} 1"));
        assert!(metrics.contains("solana_stream_events_seen_total 10"));
        assert!(metrics.contains("solana_stream_events_skipped_total 4"));
        assert!(metrics.contains("solana_stream_events_inserted_total 5"));
        assert!(metrics.contains("solana_stream_cursor_available 1"));
        assert!(metrics.contains("solana_stream_last_persisted_slot 42"));
    }

    #[test]
    fn omits_last_persisted_slot_when_cursor_is_missing() {
        let summary = PipelineSummary {
            events_seen: 0,
            events_skipped: 0,
            batches_written: 0,
            write_summary: WriteSummary::default(),
            last_persisted_slot: None,
        };
        let status = StatusSnapshot::from_pipeline("replay", summary);

        let metrics = render_metrics(&status);

        assert!(metrics.contains("solana_stream_cursor_available 0"));
        assert!(!metrics.contains("solana_stream_last_persisted_slot "));
    }

    #[test]
    fn escapes_metric_label_values() {
        let summary = PipelineSummary {
            events_seen: 0,
            events_skipped: 0,
            batches_written: 0,
            write_summary: WriteSummary::default(),
            last_persisted_slot: None,
        };
        let status = StatusSnapshot::from_pipeline("replay\\test\nmainnet\"", summary);

        let metrics = render_metrics(&status);

        assert!(metrics.contains(r#"stream_name="replay\\test\nmainnet\"""#));
    }
}
