use axum::{Json, Router, extract::State, http::header, response::IntoResponse, routing::get};
use serde::Serialize;
use solana_yellowstone_stream::pipeline::PipelineSummary;
use std::{fmt, future::Future};
use tokio::{net::TcpListener, sync::watch};

const METRICS_CONTENT_TYPE: &str = "text/plain; version=0.0.4; charset=utf-8";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum HealthState {
    Ready,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum StreamMode {
    Replay,
    #[cfg(feature = "yellowstone-live")]
    Yellowstone,
}

#[cfg(feature = "yellowstone-live")]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum LiveProducerState {
    Running,
    Reconnecting,
}

#[cfg(feature = "yellowstone-live")]
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct LiveProducerStatus {
    pub producer_state: LiveProducerState,
    pub reconnect_attempts: u64,
    pub last_reconnect_delay_ms: Option<u64>,
    pub last_error_kind: Option<String>,
    pub last_error: Option<String>,
}

#[cfg(feature = "yellowstone-live")]
impl Default for LiveProducerStatus {
    fn default() -> Self {
        Self {
            producer_state: LiveProducerState::Running,
            reconnect_attempts: 0,
            last_reconnect_delay_ms: None,
            last_error_kind: None,
            last_error: None,
        }
    }
}

#[cfg(feature = "yellowstone-live")]
impl LiveProducerStatus {
    pub fn running(mut self) -> Self {
        self.producer_state = LiveProducerState::Running;
        self
    }

    pub fn reconnecting(
        reconnect_attempts: u64,
        last_reconnect_delay_ms: u64,
        last_error_kind: impl Into<String>,
        last_error: impl Into<String>,
    ) -> Self {
        Self {
            producer_state: LiveProducerState::Reconnecting,
            reconnect_attempts,
            last_reconnect_delay_ms: Some(last_reconnect_delay_ms),
            last_error_kind: Some(last_error_kind.into()),
            last_error: Some(last_error.into()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct StatusSnapshot {
    pub health: HealthState,
    pub mode: StreamMode,
    pub stream_name: String,
    pub last_persisted_slot: Option<u64>,
    pub events_seen: usize,
    pub events_skipped: usize,
    pub batches_written: usize,
    pub events_attempted: usize,
    pub events_inserted: usize,
    pub events_deduplicated: usize,
    #[cfg(feature = "yellowstone-live")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub live: Option<LiveProducerStatus>,
}

impl StatusSnapshot {
    pub fn from_pipeline(stream_name: impl Into<String>, summary: PipelineSummary) -> Self {
        Self::from_pipeline_mode(StreamMode::Replay, stream_name, summary)
    }

    pub fn from_pipeline_mode(
        mode: StreamMode,
        stream_name: impl Into<String>,
        summary: PipelineSummary,
    ) -> Self {
        Self {
            health: HealthState::Ready,
            mode,
            stream_name: stream_name.into(),
            last_persisted_slot: summary.last_persisted_slot,
            events_seen: summary.events_seen,
            events_skipped: summary.events_skipped,
            batches_written: summary.batches_written,
            events_attempted: summary.write_summary.attempted,
            events_inserted: summary.write_summary.inserted,
            events_deduplicated: summary.write_summary.deduplicated,
            #[cfg(feature = "yellowstone-live")]
            live: None,
        }
    }

    #[cfg(feature = "yellowstone-live")]
    pub fn with_live(mut self, live: LiveProducerStatus) -> Self {
        self.live = Some(live);
        self
    }
}

pub type StatusSender = watch::Sender<StatusSnapshot>;
pub type StatusReceiver = watch::Receiver<StatusSnapshot>;

#[derive(Debug, Clone)]
struct StatusState {
    status: StatusReceiver,
}

pub fn status_channel(initial: StatusSnapshot) -> (StatusSender, StatusReceiver) {
    watch::channel(initial)
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
    let (_sender, receiver) = status_channel(status);
    serve_status_updates_until_shutdown(addr, receiver, shutdown_signal()).await
}

#[cfg(feature = "yellowstone-live")]
pub async fn serve_updates_until_shutdown(
    addr: &str,
    status: StatusReceiver,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> Result<(), HttpError> {
    serve_status_updates_until_shutdown(addr, status, shutdown).await
}

#[cfg(test)]
async fn serve_until_shutdown(
    addr: &str,
    status: StatusSnapshot,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> Result<(), HttpError> {
    let (_sender, receiver) = status_channel(status);
    serve_status_updates_until_shutdown(addr, receiver, shutdown).await
}

async fn serve_status_updates_until_shutdown(
    addr: &str,
    status: StatusReceiver,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> Result<(), HttpError> {
    let listener = TcpListener::bind(addr).await.map_err(HttpError::Bind)?;
    axum::serve(listener, router(status))
        .with_graceful_shutdown(shutdown)
        .await
        .map_err(HttpError::Serve)
}

async fn shutdown_signal() {
    if let Err(err) = tokio::signal::ctrl_c().await {
        tracing::error!(error = %err, "failed to listen for shutdown signal");
    }
    tracing::info!("shutdown signal received");
}

fn router(status: StatusReceiver) -> Router {
    let state = StatusState { status };

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

async fn readyz(State(state): State<StatusState>) -> Json<ReadyResponse> {
    let status = state.status.borrow();
    Json(ReadyResponse {
        ready: status.health == HealthState::Ready,
    })
}

async fn status_handler(State(state): State<StatusState>) -> Json<StatusSnapshot> {
    Json(state.status.borrow().clone())
}

async fn metrics_handler(State(state): State<StatusState>) -> impl IntoResponse {
    let status = state.status.borrow().clone();
    (
        [(header::CONTENT_TYPE, METRICS_CONTENT_TYPE)],
        render_metrics(&status),
    )
}

fn render_metrics(status: &StatusSnapshot) -> String {
    let mut output = String::new();
    let stream_name = escape_label_value(&status.stream_name);
    let mode = match status.mode {
        StreamMode::Replay => "replay",
        #[cfg(feature = "yellowstone-live")]
        StreamMode::Yellowstone => "yellowstone",
    };
    let info_labels = format!(r#"{{stream_name="{stream_name}",mode="{mode}"}}"#);

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

    #[cfg(feature = "yellowstone-live")]
    if let Some(live) = &status.live {
        push_counter(
            &mut output,
            "solana_stream_reconnect_attempts_total",
            "Total Yellowstone reconnect attempts.",
            live.reconnect_attempts as usize,
        );
        push_gauge(
            &mut output,
            "solana_stream_producer_up",
            "Whether the Yellowstone producer is currently running.",
            "",
            u64::from(live.producer_state == LiveProducerState::Running),
        );
        if let Some(delay_ms) = live.last_reconnect_delay_ms {
            push_gauge(
                &mut output,
                "solana_stream_reconnect_delay_ms",
                "Last Yellowstone reconnect backoff delay in milliseconds.",
                "",
                delay_ms,
            );
        }
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
    use super::{
        HealthState, METRICS_CONTENT_TYPE, StatusSnapshot, render_metrics, router,
        serve_until_shutdown,
    };
    use axum::{
        body::{Body, to_bytes},
        http::{Request, StatusCode, header},
    };
    use serde_json::Value;
    use solana_yellowstone_storage::WriteSummary;
    use solana_yellowstone_stream::pipeline::PipelineSummary;
    use tower::ServiceExt;

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
        assert_eq!(status.mode, super::StreamMode::Replay);
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
        assert!(metrics.contains("solana_stream_info{stream_name=\"replay\",mode=\"replay\"} 1"));
        assert!(metrics.contains("solana_stream_events_seen_total 10"));
        assert!(metrics.contains("solana_stream_events_skipped_total 4"));
        assert!(metrics.contains("solana_stream_events_inserted_total 5"));
        assert!(metrics.contains("solana_stream_cursor_available 1"));
        assert!(metrics.contains("solana_stream_last_persisted_slot 42"));
    }

    #[cfg(feature = "yellowstone-live")]
    #[test]
    fn renders_live_producer_status_metrics() {
        let summary = PipelineSummary {
            events_seen: 0,
            events_skipped: 0,
            batches_written: 0,
            write_summary: WriteSummary::default(),
            last_persisted_slot: None,
        };
        let status =
            StatusSnapshot::from_pipeline_mode(super::StreamMode::Yellowstone, "live", summary)
                .with_live(super::LiveProducerStatus::reconnecting(
                    3,
                    1_000,
                    "receive",
                    "yellowstone stream receive failed with gRPC status Unavailable",
                ));

        let metrics = render_metrics(&status);

        assert!(
            metrics.contains("solana_stream_info{stream_name=\"live\",mode=\"yellowstone\"} 1")
        );
        assert!(metrics.contains("solana_stream_reconnect_attempts_total 3"));
        assert!(metrics.contains("solana_stream_producer_up 0"));
        assert!(metrics.contains("solana_stream_reconnect_delay_ms 1000"));
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

    #[tokio::test]
    async fn serve_returns_after_shutdown_signal() {
        serve_until_shutdown("127.0.0.1:0", empty_status(), async {})
            .await
            .expect("server should shut down cleanly");
    }

    #[tokio::test]
    async fn healthz_endpoint_returns_ok() {
        let response = request("/healthz").await;

        assert_eq!(response.status, StatusCode::OK);
        assert_eq!(response.body, "ok");
    }

    #[tokio::test]
    async fn readyz_endpoint_returns_json_ready_state() {
        let response = request("/readyz").await;

        assert_eq!(response.status, StatusCode::OK);
        assert_content_type_starts_with(&response, "application/json");
        let body: Value = serde_json::from_str(&response.body).expect("readyz json should parse");
        assert_eq!(body, serde_json::json!({ "ready": true }));
    }

    #[cfg(feature = "yellowstone-live")]
    #[tokio::test]
    async fn status_endpoint_returns_live_producer_status() {
        let summary = PipelineSummary {
            events_seen: 0,
            events_skipped: 0,
            batches_written: 0,
            write_summary: WriteSummary::default(),
            last_persisted_slot: None,
        };
        let status = StatusSnapshot::from_pipeline_mode(
            super::StreamMode::Yellowstone,
            "live-test",
            summary,
        )
        .with_live(super::LiveProducerStatus::reconnecting(
            2,
            2_000,
            "subscribe",
            "yellowstone subscribe failed with gRPC status Unavailable",
        ));
        let (_sender, receiver) = super::status_channel(status);

        let response = router(receiver)
            .oneshot(
                Request::builder()
                    .uri("/status")
                    .body(Body::empty())
                    .expect("request should build"),
            )
            .await
            .expect("request should succeed");
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body should read");
        let body: Value = serde_json::from_slice(&body).expect("status json should parse");

        assert_eq!(body["mode"], "yellowstone");
        assert_eq!(body["live"]["producer_state"], "reconnecting");
        assert_eq!(body["live"]["reconnect_attempts"], 2);
        assert_eq!(body["live"]["last_reconnect_delay_ms"], 2_000);
        assert_eq!(body["live"]["last_error_kind"], "subscribe");
        assert_eq!(
            body["live"]["last_error"],
            "yellowstone subscribe failed with gRPC status Unavailable"
        );
    }

    #[tokio::test]
    async fn status_endpoint_returns_pipeline_snapshot() {
        let response = request("/status").await;

        assert_eq!(response.status, StatusCode::OK);
        assert_content_type_starts_with(&response, "application/json");
        let body: Value = serde_json::from_str(&response.body).expect("status json should parse");
        assert_eq!(body["health"], "ready");
        assert_eq!(body["mode"], "replay");
        assert_eq!(body["stream_name"], "contract-test");
        assert_eq!(body["last_persisted_slot"], 42);
        assert_eq!(body["events_seen"], 10);
        assert_eq!(body["events_skipped"], 4);
        assert_eq!(body["batches_written"], 2);
        assert_eq!(body["events_attempted"], 6);
        assert_eq!(body["events_inserted"], 5);
        assert_eq!(body["events_deduplicated"], 1);
    }

    #[tokio::test]
    async fn metrics_endpoint_returns_prometheus_text() {
        let response = request("/metrics").await;

        assert_eq!(response.status, StatusCode::OK);
        assert_content_type_starts_with(&response, METRICS_CONTENT_TYPE);
        assert!(
            response
                .body
                .contains("# TYPE solana_stream_events_seen_total counter")
        );
        assert!(
            response
                .body
                .contains("solana_stream_info{stream_name=\"contract-test\",mode=\"replay\"} 1")
        );
        assert!(response.body.contains("solana_stream_events_seen_total 10"));
        assert!(
            response
                .body
                .contains("solana_stream_events_inserted_total 5")
        );
        assert!(
            response
                .body
                .contains("solana_stream_events_deduplicated_total 1")
        );
        assert!(
            response
                .body
                .contains("solana_stream_last_persisted_slot 42")
        );
    }

    fn empty_status() -> StatusSnapshot {
        StatusSnapshot::from_pipeline(
            "replay",
            PipelineSummary {
                events_seen: 0,
                events_skipped: 0,
                batches_written: 0,
                write_summary: WriteSummary::default(),
                last_persisted_slot: None,
            },
        )
    }

    fn contract_status() -> StatusSnapshot {
        StatusSnapshot::from_pipeline(
            "contract-test",
            PipelineSummary {
                events_seen: 10,
                events_skipped: 4,
                batches_written: 2,
                write_summary: WriteSummary {
                    attempted: 6,
                    inserted: 5,
                    deduplicated: 1,
                },
                last_persisted_slot: Some(42),
            },
        )
    }

    struct TestResponse {
        status: StatusCode,
        content_type: Option<String>,
        body: String,
    }

    async fn request(path: &str) -> TestResponse {
        let (_sender, receiver) = super::status_channel(contract_status());
        let response = router(receiver)
            .oneshot(
                Request::builder()
                    .uri(path)
                    .body(Body::empty())
                    .expect("request should build"),
            )
            .await
            .expect("request should succeed");
        let status = response.status();
        let content_type = response.headers().get(header::CONTENT_TYPE).map(|value| {
            value
                .to_str()
                .expect("content type should be ascii")
                .to_owned()
        });
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body should read");

        TestResponse {
            status,
            content_type,
            body: String::from_utf8(body.to_vec()).expect("body should be utf8"),
        }
    }

    fn assert_content_type_starts_with(response: &TestResponse, expected: &str) {
        let content_type = response
            .content_type
            .as_deref()
            .expect("content type should be set");

        assert!(
            content_type.starts_with(expected),
            "expected content type {content_type:?} to start with {expected:?}"
        );
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
