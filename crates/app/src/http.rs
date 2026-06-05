use axum::{
    Json, Router,
    extract::{Query, State},
    http::header,
    response::IntoResponse,
    routing::get,
};
use serde::{Deserialize, Serialize};
use solana_yellowstone_stream::pipeline::PipelineSummary;
#[cfg(feature = "yellowstone-live")]
use std::time::{SystemTime, UNIX_EPOCH};
use std::{fmt, future::Future, sync::Arc};
use tokio::{net::TcpListener, sync::watch};

use crate::metrics::Metrics;

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
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum LiveRecoveryState {
    Steady,
    Reconnecting,
    GapRisk,
}

#[cfg(feature = "yellowstone-live")]
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct LiveProducerStatus {
    pub producer_state: LiveProducerState,
    pub recovery_state: LiveRecoveryState,
    pub reconnect_attempts: u64,
    pub last_reconnect_delay_ms: Option<u64>,
    pub last_reconnect_from_slot: Option<u64>,
    pub gap_risk_count: u64,
    pub last_error_kind: Option<String>,
    pub last_error: Option<String>,
    pub last_event_at_unix_ms: Option<u64>,
    pub last_observed_at_unix_ms: Option<u64>,
    pub last_observed_slot: Option<u64>,
    pub last_persisted_batch_at_unix_ms: Option<u64>,
    pub seconds_since_last_event: Option<u64>,
    pub seconds_since_last_persisted_batch: Option<u64>,
    pub observed_to_persisted_slot_lag: Option<u64>,
    pub decode_errors_total: u64,
}

#[cfg(feature = "yellowstone-live")]
impl Default for LiveProducerStatus {
    fn default() -> Self {
        Self {
            producer_state: LiveProducerState::Running,
            recovery_state: LiveRecoveryState::Steady,
            reconnect_attempts: 0,
            last_reconnect_delay_ms: None,
            last_reconnect_from_slot: None,
            gap_risk_count: 0,
            last_error_kind: None,
            last_error: None,
            last_event_at_unix_ms: None,
            last_observed_at_unix_ms: None,
            last_observed_slot: None,
            last_persisted_batch_at_unix_ms: None,
            seconds_since_last_event: None,
            seconds_since_last_persisted_batch: None,
            observed_to_persisted_slot_lag: None,
            decode_errors_total: 0,
        }
    }
}

#[cfg(feature = "yellowstone-live")]
impl LiveProducerStatus {
    pub fn running(mut self) -> Self {
        self.producer_state = LiveProducerState::Running;
        if self.recovery_state == LiveRecoveryState::Reconnecting {
            self.recovery_state = LiveRecoveryState::Steady;
        }
        self
    }

    pub fn with_reconnecting(
        mut self,
        reconnect_attempts: u64,
        last_reconnect_delay_ms: u64,
        last_error_kind: impl Into<String>,
        last_error: impl Into<String>,
    ) -> Self {
        self.producer_state = LiveProducerState::Reconnecting;
        self.reconnect_attempts = reconnect_attempts;
        self.last_reconnect_delay_ms = Some(last_reconnect_delay_ms);
        self.last_error_kind = Some(last_error_kind.into());
        self.last_error = Some(last_error.into());
        self
    }

    pub fn with_recovery_reconnect(mut self, from_slot: Option<u64>) -> Self {
        self.last_reconnect_from_slot = from_slot;
        if from_slot.is_some() {
            self.recovery_state = LiveRecoveryState::Reconnecting;
        } else {
            self.recovery_state = LiveRecoveryState::GapRisk;
            self.gap_risk_count = self.gap_risk_count.saturating_add(1);
        }
        self
    }

    pub fn with_event_observed_at(mut self, now_unix_ms: u64) -> Self {
        self.last_event_at_unix_ms = Some(now_unix_ms);
        self.last_observed_at_unix_ms = Some(now_unix_ms);
        self.seconds_since_last_event = Some(0);
        self
    }

    pub fn with_event_observed(mut self, slot: u64, now_unix_ms: u64) -> Self {
        self = self.with_event_observed_at(now_unix_ms);
        self.last_observed_slot = Some(slot);
        self
    }

    pub fn with_batch_persisted_at(mut self, now_unix_ms: u64) -> Self {
        self.last_persisted_batch_at_unix_ms = Some(now_unix_ms);
        self.seconds_since_last_persisted_batch = Some(0);
        self
    }

    pub fn refresh_staleness_at(mut self, now_unix_ms: u64) -> Self {
        self.seconds_since_last_event = self
            .last_event_at_unix_ms
            .map(|event_at| seconds_between(event_at, now_unix_ms));
        self.seconds_since_last_persisted_batch = self
            .last_persisted_batch_at_unix_ms
            .map(|batch_at| seconds_between(batch_at, now_unix_ms));
        self
    }

    pub fn refresh_slot_lag(mut self, last_persisted_slot: Option<u64>) -> Self {
        self.observed_to_persisted_slot_lag = self.last_observed_slot.and_then(|observed_slot| {
            last_persisted_slot.map(|persisted_slot| observed_slot.saturating_sub(persisted_slot))
        });
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct StatusSnapshot {
    pub health: HealthState,
    pub mode: StreamMode,
    pub stream_name: String,
    pub last_persisted_slot: Option<u64>,
    pub last_contiguous_finalized_slot: Option<u64>,
    pub last_finalized_slot: Option<u64>,
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
            last_contiguous_finalized_slot: summary.last_contiguous_finalized_slot,
            last_finalized_slot: summary.last_finalized_slot,
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

    #[cfg(feature = "yellowstone-live")]
    fn refresh_live_derived(mut self) -> Self {
        let now_unix_ms = current_unix_ms();
        if let Some(live) = self.live.take() {
            self.live = Some(
                live.refresh_staleness_at(now_unix_ms)
                    .refresh_slot_lag(self.last_persisted_slot),
            );
        }
        self
    }
}

#[cfg(feature = "yellowstone-live")]
pub fn current_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(feature = "yellowstone-live")]
fn seconds_between(start_unix_ms: u64, end_unix_ms: u64) -> u64 {
    end_unix_ms.saturating_sub(start_unix_ms) / 1_000
}

pub type StatusSender = watch::Sender<StatusSnapshot>;
pub type StatusReceiver = watch::Receiver<StatusSnapshot>;

#[derive(Debug, Clone)]
struct AppState {
    status: StatusReceiver,
    metrics: Arc<Metrics>,
    pool: Option<sqlx::PgPool>,
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

pub async fn serve(
    addr: &str,
    status: StatusSnapshot,
    metrics: Arc<Metrics>,
    pool: sqlx::PgPool,
) -> Result<(), HttpError> {
    let (_sender, receiver) = status_channel(status);
    serve_status_updates_until_shutdown(addr, receiver, metrics, Some(pool), shutdown_signal()).await
}

#[cfg(feature = "yellowstone-live")]
pub async fn serve_updates_until_shutdown(
    addr: &str,
    status: StatusReceiver,
    metrics: Arc<Metrics>,
    pool: sqlx::PgPool,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> Result<(), HttpError> {
    serve_status_updates_until_shutdown(addr, status, metrics, Some(pool), shutdown).await
}

#[cfg(test)]
async fn serve_until_shutdown(
    addr: &str,
    status: StatusSnapshot,
    metrics: Arc<Metrics>,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> Result<(), HttpError> {
    let (_sender, receiver) = status_channel(status);
    serve_status_updates_until_shutdown(addr, receiver, metrics, None, shutdown).await
}

async fn serve_status_updates_until_shutdown(
    addr: &str,
    status: StatusReceiver,
    metrics: Arc<Metrics>,
    pool: Option<sqlx::PgPool>,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> Result<(), HttpError> {
    let listener = TcpListener::bind(addr).await.map_err(HttpError::Bind)?;
    axum::serve(listener, router(status, metrics, pool))
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

fn router(status: StatusReceiver, metrics: Arc<Metrics>, pool: Option<sqlx::PgPool>) -> Router {
    let state = AppState {
        status,
        metrics,
        pool,
    };

    Router::new()
        .route("/healthz", get(healthz))
        .route("/readyz", get(readyz))
        .route("/status", get(status_handler))
        .route("/metrics", get(metrics_handler))
        .route("/v1/events/recent", get(recent_events_handler))
        .route("/v1/swaps/recent", get(recent_swaps_handler))
        .route("/v1/streams/{stream_name}/lag", get(stream_lag_handler))
        .with_state(state)
}

async fn healthz() -> &'static str {
    "ok"
}

async fn readyz(State(state): State<AppState>) -> Json<ReadyResponse> {
    let status = state.status.borrow();
    Json(ReadyResponse {
        ready: status.health == HealthState::Ready,
    })
}

async fn status_handler(State(state): State<AppState>) -> Json<StatusSnapshot> {
    Json(refresh_status(state.status.borrow().clone()))
}

async fn metrics_handler(State(state): State<AppState>) -> impl IntoResponse {
    (
        [(header::CONTENT_TYPE, METRICS_CONTENT_TYPE)],
        state.metrics.render(),
    )
}

#[derive(Debug, Deserialize)]
struct RecentEventsQuery {
    event_type: Option<String>,
    #[serde(default = "default_limit")]
    limit: i64,
}

#[derive(Debug, Deserialize)]
struct RecentSwapsQuery {
    program_id: Option<String>,
    #[serde(default = "default_limit")]
    limit: i64,
}

const fn default_limit() -> i64 {
    100
}

fn require_pool(state: &AppState) -> Result<&sqlx::PgPool, ApiError> {
    state
        .pool
        .as_ref()
        .ok_or_else(|| ApiError::Storage(sqlx::Error::PoolTimedOut))
}

async fn recent_events_handler(
    State(state): State<AppState>,
    Query(params): Query<RecentEventsQuery>,
) -> Result<Json<Vec<solana_yellowstone_storage::api::RecentEvent>>, ApiError> {
    let pool = require_pool(&state)?;
    let limit = params.limit.clamp(1, 1_000);
    let events = solana_yellowstone_storage::api::recent_events(
        pool,
        params.event_type.as_deref(),
        limit,
    )
    .await
    .map_err(ApiError::Storage)?;
    Ok(Json(events))
}

async fn recent_swaps_handler(
    State(state): State<AppState>,
    Query(params): Query<RecentSwapsQuery>,
) -> Result<Json<Vec<solana_yellowstone_storage::api::RecentSwap>>, ApiError> {
    let pool = require_pool(&state)?;
    let limit = params.limit.clamp(1, 1_000);
    let swaps = solana_yellowstone_storage::api::recent_swaps(
        pool,
        params.program_id.as_deref(),
        limit,
    )
    .await
    .map_err(ApiError::Storage)?;
    Ok(Json(swaps))
}

async fn stream_lag_handler(
    State(state): State<AppState>,
    axum::extract::Path(stream_name): axum::extract::Path<String>,
) -> Result<Json<solana_yellowstone_storage::api::StreamLag>, ApiError> {
    let pool = require_pool(&state)?;
    let lag = solana_yellowstone_storage::api::stream_lag(pool, &stream_name)
        .await
        .map_err(ApiError::Storage)?;
    Ok(Json(lag))
}

#[derive(Debug)]
enum ApiError {
    Storage(sqlx::Error),
}

impl IntoResponse for ApiError {
    fn into_response(self) -> axum::response::Response {
        let (status, message) = match self {
            Self::Storage(err) => {
                tracing::error!(error = %err, "api storage query failed");
                (
                    axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                    "storage query failed".to_owned(),
                )
            }
        };
        (status, Json(serde_json::json!({ "error": message }))).into_response()
    }
}

fn refresh_status(status: StatusSnapshot) -> StatusSnapshot {
    #[cfg(feature = "yellowstone-live")]
    {
        status.refresh_live_derived()
    }

    #[cfg(not(feature = "yellowstone-live"))]
    {
        status
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
struct ReadyResponse {
    ready: bool,
}

#[cfg(test)]
mod tests {
    use super::{HealthState, METRICS_CONTENT_TYPE, StatusSnapshot, router, serve_until_shutdown};
    use axum::{
        body::{Body, to_bytes},
        http::{Request, StatusCode, header},
    };
    use serde_json::Value;
    use solana_yellowstone_storage::{
        CursorStore, EventWriter, WriteSummary,
        swaps::SwapWriter,
    };
    use solana_yellowstone_stream::pipeline::PipelineSummary;
    use std::sync::Arc;
    use tower::ServiceExt;

    use crate::metrics::Metrics;

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
                skipped: 0,
            },
            last_persisted_slot: Some(42),
            last_contiguous_finalized_slot: None,
            last_finalized_slot: None,
            ..Default::default()
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
        let metrics = Metrics::new().expect("metrics should initialize");
        metrics.record_ingest_event("replay", "transaction");
        metrics.record_ingest_event("replay", "transaction");
        metrics.record_ingest_event("replay", "slot");
        metrics.set_channel_state("replay", 5, 100);
        metrics.observe_batch_write("postgres", 0.028);
        metrics.set_last_observed_slot(42);
        metrics.set_slot_lag(3);

        let output = metrics.render();

        assert!(output.contains("# TYPE solana_stream_ingest_events_total counter"));
        assert!(output.contains(
            "solana_stream_ingest_events_total{event_type=\"transaction\",source=\"replay\"} 2"
        ));
        assert!(output.contains(
            "solana_stream_ingest_events_total{event_type=\"slot\",source=\"replay\"} 1"
        ));
        assert!(output.contains("solana_stream_channel_depth{stream_name=\"replay\"} 5"));
        assert!(output.contains("solana_stream_channel_capacity{stream_name=\"replay\"} 100"));
        assert!(
            output.contains("solana_stream_channel_utilization_ratio{stream_name=\"replay\"} 0.05")
        );
        assert!(output.contains("solana_stream_batch_write_latency_seconds_bucket"));
        assert!(output.contains("solana_stream_last_observed_slot 42"));
        assert!(output.contains("solana_stream_slot_lag 3"));
    }

    #[cfg(feature = "yellowstone-live")]
    #[test]
    fn renders_live_producer_status_metrics() {
        let metrics = Metrics::new().expect("metrics should initialize");
        metrics.inc_reconnect_attempts();
        metrics.inc_reconnect_attempts();
        metrics.inc_reconnect_attempts();
        metrics.inc_decode_errors();
        metrics.set_last_observed_slot(42);
        metrics.set_slot_lag(2);

        let output = metrics.render();

        assert!(output.contains("solana_stream_reconnect_attempts_total 3"));
        assert!(output.contains("solana_stream_decode_errors_total 1"));
        assert!(output.contains("solana_stream_last_observed_slot 42"));
        assert!(output.contains("solana_stream_slot_lag 2"));
    }

    #[test]
    fn renders_zero_last_persisted_slot_when_not_set() {
        let metrics = Metrics::new().expect("metrics should initialize");
        let output = metrics.render();
        assert!(output.contains("solana_stream_last_persisted_slot 0"));
    }

    #[tokio::test]
    async fn serve_returns_after_shutdown_signal() {
        serve_until_shutdown("127.0.0.1:0", empty_status(), test_metrics(), async {})
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
    #[test]
    fn live_recovery_marks_gap_risk_without_from_slot() {
        let live = super::LiveProducerStatus::default()
            .with_reconnecting(1, 1_000, "connect", "failed to connect")
            .with_recovery_reconnect(None)
            .running();

        assert_eq!(live.producer_state, super::LiveProducerState::Running);
        assert_eq!(live.recovery_state, super::LiveRecoveryState::GapRisk);
        assert_eq!(live.gap_risk_count, 1);
        assert_eq!(live.last_reconnect_from_slot, None);
    }

    #[cfg(feature = "yellowstone-live")]
    #[tokio::test]
    async fn status_endpoint_returns_live_producer_status() {
        let summary = PipelineSummary {
            events_seen: 0,
            events_skipped: 0,
            batches_written: 0,
            write_summary: WriteSummary::default(),
            last_persisted_slot: Some(10),
            last_contiguous_finalized_slot: None,
            last_finalized_slot: None,
            ..Default::default()
        };
        let status = StatusSnapshot::from_pipeline_mode(
            super::StreamMode::Yellowstone,
            "live-test",
            summary,
        )
        .with_live(
            super::LiveProducerStatus::default()
                .with_reconnecting(
                    2,
                    2_000,
                    "subscribe",
                    "yellowstone subscribe failed with gRPC status Unavailable",
                )
                .with_recovery_reconnect(Some(10))
                .with_event_observed(12, super::current_unix_ms().saturating_sub(2_000))
                .with_batch_persisted_at(super::current_unix_ms().saturating_sub(3_000)),
        );
        let (_sender, receiver) = super::status_channel(status);

        let response = router(receiver, test_metrics(), None)
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
        assert_eq!(body["live"]["recovery_state"], "reconnecting");
        assert_eq!(body["live"]["reconnect_attempts"], 2);
        assert_eq!(body["live"]["last_reconnect_delay_ms"], 2_000);
        assert_eq!(body["live"]["last_reconnect_from_slot"], 10);
        assert_eq!(body["live"]["gap_risk_count"], 0);
        assert_eq!(body["live"]["last_error_kind"], "subscribe");
        assert_eq!(
            body["live"]["last_error"],
            "yellowstone subscribe failed with gRPC status Unavailable"
        );
        assert!(body["live"]["last_event_at_unix_ms"].is_number());
        assert!(body["live"]["last_observed_at_unix_ms"].is_number());
        assert_eq!(body["live"]["last_observed_slot"], 12);
        assert!(body["live"]["last_persisted_batch_at_unix_ms"].is_number());
        assert!(
            body["live"]["seconds_since_last_event"]
                .as_u64()
                .unwrap_or_default()
                >= 2
        );
        assert!(
            body["live"]["seconds_since_last_persisted_batch"]
                .as_u64()
                .unwrap_or_default()
                >= 3
        );
        assert_eq!(body["live"]["observed_to_persisted_slot_lag"], 2);
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
                .contains("# TYPE solana_stream_decode_errors_total counter")
        );
        assert!(
            response
                .body
                .contains("# TYPE solana_stream_last_observed_slot gauge")
        );
        assert!(
            response
                .body
                .contains("# TYPE solana_stream_slot_lag gauge")
        );
        assert!(
            response
                .body
                .contains("# TYPE solana_stream_reconnect_attempts_total counter")
        );
    }

    fn test_metrics() -> Arc<Metrics> {
        Arc::new(Metrics::new().expect("test metrics should initialize"))
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
                last_contiguous_finalized_slot: None,
                last_finalized_slot: None,
                ..Default::default()
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
                    skipped: 0,
                },
                last_persisted_slot: Some(42),
                last_contiguous_finalized_slot: None,
                last_finalized_slot: None,
                ..Default::default()
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
        let response = router(receiver, test_metrics(), None)
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
        let metrics = Metrics::new().expect("metrics should initialize");
        metrics.set_channel_state("replay\\test\nmainnet\"", 1, 10);

        let output = metrics.render();

        assert!(output.contains(r#"stream_name="replay\\test\nmainnet\"""#));
    }

    async fn test_pool() -> Option<sqlx::PgPool> {
        let database_url = std::env::var("TEST_DATABASE_URL").ok()?;
        let pool = sqlx::PgPool::connect(&database_url).await.ok()?;
        Some(pool)
    }

    async fn api_request(path: &str, pool: sqlx::PgPool) -> TestResponse {
        let (_sender, receiver) = super::status_channel(empty_status());
        let response = router(receiver, test_metrics(), Some(pool))
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

    #[tokio::test]
    #[ignore = "requires local postgres; run `make compose-up test-postgres`"]
    async fn recent_events_endpoint_returns_seeded_events() {
        let pool = test_pool().await.expect("TEST_DATABASE_URL must be set");

        let writer = solana_yellowstone_storage::postgres::PostgresEventWriter::from_pool(pool.clone());
        let event = solana_yellowstone_domain::event::NormalizedEvent::new(
            solana_yellowstone_domain::event::EventIdentity::Transaction {
                cluster: "localnet".to_owned(),
                slot: 70_001,
                signature: "api-event-sig".to_owned(),
                index: 0,
            },
            serde_json::json!({"token_balances": []}),
        );
        writer.write_batch(&[event]).await.expect("write event");

        let response = api_request("/v1/events/recent?event_type=transaction&limit=10", pool).await;

        assert_eq!(response.status, StatusCode::OK);
        assert_content_type_starts_with(&response, "application/json");
        let body: Value = serde_json::from_str(&response.body).expect("json should parse");
        let events = body.as_array().expect("should be array");
        assert!(!events.is_empty());
        assert_eq!(events[0]["slot"], 70_001);
        assert_eq!(events[0]["event_type"], "transaction");
    }

    #[tokio::test]
    #[ignore = "requires local postgres; run `make compose-up test-postgres`"]
    async fn recent_swaps_endpoint_returns_seeded_swaps() {
        let pool = test_pool().await.expect("TEST_DATABASE_URL must be set");

        let swap_writer = solana_yellowstone_storage::swaps::PostgresSwapWriter::from_pool(pool.clone());
        let swap = solana_yellowstone_domain::decoded::DexSwap {
            slot: 70_002,
            signature: "api-swap-sig".to_owned(),
            program_id: "program-api-test".to_owned(),
            token_in: "mint-a".to_owned(),
            token_in_amount: 500,
            token_out: "mint-b".to_owned(),
            token_out_amount: 1_000,
        };
        swap_writer.write_swaps(&[swap]).await.expect("write swap");

        let response = api_request("/v1/swaps/recent?program_id=program-api-test&limit=10", pool).await;

        assert_eq!(response.status, StatusCode::OK);
        assert_content_type_starts_with(&response, "application/json");
        let body: Value = serde_json::from_str(&response.body).expect("json should parse");
        let swaps = body.as_array().expect("should be array");
        assert!(!swaps.is_empty());
        assert_eq!(swaps[0]["slot"], 70_002);
        assert_eq!(swaps[0]["program_id"], "program-api-test");
    }

    #[tokio::test]
    #[ignore = "requires local postgres; run `make compose-up test-postgres`"]
    async fn stream_lag_endpoint_returns_cursor() {
        let pool = test_pool().await.expect("TEST_DATABASE_URL must be set");

        let cursor_store = solana_yellowstone_storage::cursor::PostgresCursorStore::from_pool(pool.clone());
        cursor_store.update_after_batch("lag-test-stream", 123).await.expect("update cursor");

        let response = api_request("/v1/streams/lag-test-stream/lag", pool).await;

        assert_eq!(response.status, StatusCode::OK);
        assert_content_type_starts_with(&response, "application/json");
        let body: Value = serde_json::from_str(&response.body).expect("json should parse");
        assert_eq!(body["stream_name"], "lag-test-stream");
        assert_eq!(body["last_persisted_slot"], 123);
    }

}
