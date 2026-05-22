use axum::{Json, Router, extract::State, routing::get};
use serde::Serialize;
use solana_yellowstone_stream::pipeline::PipelineSummary;
use std::{fmt, sync::Arc};
use tokio::net::TcpListener;

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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
struct ReadyResponse {
    ready: bool,
}

#[cfg(test)]
mod tests {
    use super::{HealthState, StatusSnapshot};
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
}
