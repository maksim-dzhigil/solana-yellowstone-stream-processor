use crate::config::{Config, RunMode};
use crate::error::AppRunError;
use crate::http::{self, StatusSnapshot};
#[cfg(feature = "yellowstone-live")]
use crate::http::{LiveProducerStatus, StreamMode};
use solana_yellowstone_storage::{
    CursorStore, cursor::PostgresCursorStore, postgres::PostgresEventWriter,
};
#[cfg(feature = "yellowstone-live")]
use solana_yellowstone_storage::slots::{PostgresSlotStateStore, SlotStateStore};
#[cfg(feature = "yellowstone-live")]
use solana_yellowstone_stream::pipeline::PipelineSummary;
#[cfg(feature = "yellowstone-live")]
use solana_yellowstone_stream::pipeline::run_event_producer_pipeline_with_progress_and_activity;
use solana_yellowstone_stream::pipeline::{PipelineConfig, run_replay_pipeline};
use solana_yellowstone_stream::replay::ReplaySource;
use solana_yellowstone_stream::source::EventSource;
#[cfg(feature = "yellowstone-live")]
use solana_yellowstone_stream::yellowstone_live::{
    YellowstoneGrpcConfig, YellowstoneReconnectConfig,
    run_yellowstone_grpc_producer_with_reconnect_status_and_config,
};
#[cfg(feature = "yellowstone-live")]
use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};
#[cfg(feature = "yellowstone-live")]
use tokio::sync::watch;
use tracing::info;

pub async fn run(config: Config) -> Result<(), AppRunError> {
    info!(config = %config.redacted_summary(), "configuration loaded");

    match config.run_mode {
        RunMode::Replay => run_replay(config).await,
        RunMode::Yellowstone => run_yellowstone(config).await,
    }
}

async fn run_replay(config: Config) -> Result<(), AppRunError> {
    let replay = ReplaySource::new(config.replay_path.clone());
    info!(replay_path = %config.replay_path, "reading replay events");
    let events = EventSource::read_events(&replay)?;
    info!(events_loaded = events.len(), "replay events loaded");

    info!("connecting to postgres");
    let writer = PostgresEventWriter::connect(&config.database_url).await?;
    info!("postgres initialized");

    let cursor_store = PostgresCursorStore::from_pool(writer.pool().clone());
    let cursor = cursor_store.get_cursor(&config.stream_name).await?;
    let resume_after_slot = cursor.as_ref().map(|cursor| cursor.last_persisted_slot);

    if let Some(slot) = resume_after_slot {
        info!(
            stream_name = %config.stream_name,
            last_persisted_slot = slot,
            "loaded stream cursor"
        );
    } else {
        info!(stream_name = %config.stream_name, "stream cursor not found");
    }

    let pipeline_config = PipelineConfig {
        batch_size: config.batch_size,
        channel_capacity: config.channel_capacity,
        resume_after_slot,
        advance_finalized_watermark: false,
        use_slot_resume: true,
    };
    info!(
        stream_name = %config.stream_name,
        batch_size = config.batch_size,
        channel_capacity = config.channel_capacity,
        resume_after_slot = ?pipeline_config.resume_after_slot,
        "running replay pipeline"
    );

    let summary = run_replay_pipeline(
        events,
        &writer,
        &cursor_store,
        &config.stream_name,
        pipeline_config,
    )
    .await?;

    info!(
        events_seen = summary.events_seen,
        events_skipped = summary.events_skipped,
        batches_written = summary.batches_written,
        events_attempted = summary.write_summary.attempted,
        events_inserted = summary.write_summary.inserted,
        events_deduplicated = summary.write_summary.deduplicated,
        last_persisted_slot = ?summary.last_persisted_slot,
        "replay pipeline completed"
    );

    if config.exit_after_replay {
        info!(exit_after_replay = true, "exit after replay requested");
        return Ok(());
    }

    let status = StatusSnapshot::from_pipeline(config.stream_name.clone(), summary);
    info!(http_addr = %config.http_addr, "serving http endpoints");
    http::serve(&config.http_addr, status).await?;
    info!("http server stopped");

    Ok(())
}

#[cfg(feature = "yellowstone-live")]
async fn run_yellowstone(config: Config) -> Result<(), AppRunError> {
    info!(
        stream_name = %config.stream_name,
        yellowstone_endpoint_configured = config.yellowstone_endpoint.is_some(),
        yellowstone_x_token_configured = config.yellowstone_x_token.is_some(),
        yellowstone_cluster = %config.yellowstone_cluster,
        yellowstone_subscriptions = %config.yellowstone_subscriptions.as_csv(),
        yellowstone_transaction_account_include_count = config.yellowstone_transaction_account_include.len(),
        yellowstone_transaction_account_exclude_count = config.yellowstone_transaction_account_exclude.len(),
        yellowstone_transaction_account_required_count = config.yellowstone_transaction_account_required.len(),
        yellowstone_reconnect_initial_delay_ms = config.yellowstone_reconnect.initial_delay.as_millis(),
        yellowstone_reconnect_max_delay_ms = config.yellowstone_reconnect.max_delay.as_millis(),
        yellowstone_reconnect_max_retries = ?config.yellowstone_reconnect.max_retries,
        "yellowstone live mode selected"
    );

    info!("connecting to postgres");
    let writer = PostgresEventWriter::connect(&config.database_url).await?;
    info!("postgres initialized");

    let cursor_store = PostgresCursorStore::from_pool(writer.pool().clone());
    let cursor = cursor_store.get_cursor(&config.stream_name).await?;
    let resume_after_slot = cursor.as_ref().map(|cursor| cursor.last_persisted_slot);

    let slot_state_store = PostgresSlotStateStore::from_pool(writer.pool().clone());
    let frontier = slot_state_store
        .get_finalized_frontier(&config.stream_name)
        .await?;

    if let Some(slot) = resume_after_slot {
        info!(
            stream_name = %config.stream_name,
            last_persisted_slot = slot,
            "loaded stream cursor"
        );
    } else {
        info!(stream_name = %config.stream_name, "stream cursor not found");
    }

    if let Some(slot) = frontier.last_contiguous_finalized_slot {
        info!(
            stream_name = %config.stream_name,
            last_contiguous_finalized_slot = slot,
            "loaded contiguous finalized frontier"
        );
    }
    if let Some(slot) = frontier.last_finalized_slot {
        info!(
            stream_name = %config.stream_name,
            last_finalized_slot = slot,
            "loaded finalized head"
        );
    }

    let yellowstone_reconnect_config = YellowstoneReconnectConfig {
        initial_delay: config.yellowstone_reconnect.initial_delay,
        max_delay: config.yellowstone_reconnect.max_delay,
        max_retries: config.yellowstone_reconnect.max_retries,
    };

    let mut yellowstone_config = YellowstoneGrpcConfig::slots_only(
        config
            .yellowstone_endpoint
            .clone()
            .expect("yellowstone endpoint validated before runtime"),
        config.yellowstone_cluster.clone(),
    );
    yellowstone_config.x_token = config.yellowstone_x_token.clone();
    yellowstone_config.from_slot = frontier.last_contiguous_finalized_slot;
    yellowstone_config.filter_name = config.stream_name.clone();
    yellowstone_config.subscribe_slots = config.yellowstone_subscriptions.slots;
    yellowstone_config.subscribe_transactions = config.yellowstone_subscriptions.transactions;
    yellowstone_config.subscribe_blocks = config.yellowstone_subscriptions.blocks;
    yellowstone_config.subscribe_entries = config.yellowstone_subscriptions.entries;
    yellowstone_config.transaction_account_include =
        config.yellowstone_transaction_account_include.clone();
    yellowstone_config.transaction_account_exclude =
        config.yellowstone_transaction_account_exclude.clone();
    yellowstone_config.transaction_account_required =
        config.yellowstone_transaction_account_required.clone();

    let last_contiguous_finalized_slot = Arc::new(AtomicU64::new(encode_slot(
        frontier.last_contiguous_finalized_slot,
    )));

    let pipeline_config = PipelineConfig {
        batch_size: config.batch_size,
        channel_capacity: config.channel_capacity,
        resume_after_slot,
        advance_finalized_watermark: true,
        use_slot_resume: false,
    };
    info!(
        stream_name = %config.stream_name,
        batch_size = config.batch_size,
        channel_capacity = config.channel_capacity,
        resume_after_slot = ?pipeline_config.resume_after_slot,
        advance_finalized_watermark = pipeline_config.advance_finalized_watermark,
        use_slot_resume = pipeline_config.use_slot_resume,
        "running yellowstone pipeline"
    );

    let initial_summary = PipelineSummary {
        last_persisted_slot: resume_after_slot,
        last_contiguous_finalized_slot: frontier.last_contiguous_finalized_slot,
        last_finalized_slot: frontier.last_finalized_slot,
        ..PipelineSummary::default()
    };
    let initial_status = StatusSnapshot::from_pipeline_mode(
        StreamMode::Yellowstone,
        config.stream_name.clone(),
        initial_summary,
    )
    .with_live(LiveProducerStatus::default());
    let (status_sender, status_receiver) = http::status_channel(initial_status);
    let status_stream_name = config.stream_name.clone();

    let (shutdown_sender, shutdown_receiver) = watch::channel(false);
    let http_shutdown = wait_for_shutdown(shutdown_receiver.clone());
    info!(http_addr = %config.http_addr, "serving yellowstone http endpoints");
    let http_server =
        http::serve_updates_until_shutdown(&config.http_addr, status_receiver, http_shutdown);
    let reconnect_status_sender = status_sender.clone();
    let pipeline_status_sender = status_sender.clone();
    let activity_status_sender = status_sender.clone();
    let producer_last_contiguous_slot = last_contiguous_finalized_slot.clone();
    let progress_last_contiguous_slot = last_contiguous_finalized_slot.clone();
    let mut last_batches_written = 0;
    let mut last_activity_status_sent = None::<Instant>;
    let pipeline = run_event_producer_pipeline_with_progress_and_activity(
        move |sender| {
            run_yellowstone_grpc_producer_with_reconnect_status_and_config(
                yellowstone_config,
                yellowstone_reconnect_config,
                sender,
                move |event| {
                    let mut status = reconnect_status_sender.borrow().clone();
                    let live = status
                        .live
                        .clone()
                        .unwrap_or_default()
                        .with_reconnecting(
                            u64::from(event.retry_attempt),
                            event.delay.as_millis() as u64,
                            event.error_kind.as_str(),
                            event.error_message,
                        )
                        .with_recovery_reconnect(event.from_slot);
                    status.live = Some(live);
                    if reconnect_status_sender.send(status).is_err() {
                        tracing::debug!("yellowstone status receiver dropped");
                    }
                },
                move |attempt_config| {
                    attempt_config.from_slot =
                        decode_slot(producer_last_contiguous_slot.load(Ordering::Relaxed));
                },
            )
        },
        &writer,
        &cursor_store,
        &slot_state_store,
        &config.stream_name,
        pipeline_config,
        move |summary| {
            let mut live = pipeline_status_sender
                .borrow()
                .live
                .clone()
                .unwrap_or_default()
                .running();
            if summary.batches_written > last_batches_written {
                live = live.with_batch_persisted_at(http::current_unix_ms());
                if let Some(slot) = summary.last_contiguous_finalized_slot {
                    progress_last_contiguous_slot
                        .store(encode_slot(Some(slot)), Ordering::Relaxed);
                }
                last_batches_written = summary.batches_written;
            }
            let status = StatusSnapshot::from_pipeline_mode(
                StreamMode::Yellowstone,
                status_stream_name.clone(),
                summary,
            )
            .with_live(live);
            if pipeline_status_sender.send(status).is_err() {
                tracing::debug!("yellowstone status receiver dropped");
            }
        },
        move |activity| {
            let now = Instant::now();
            if last_activity_status_sent
                .is_some_and(|last| now.duration_since(last) < Duration::from_secs(1))
            {
                return;
            }
            last_activity_status_sent = Some(now);

            let mut status = activity_status_sender.borrow().clone();
            let live = status
                .live
                .clone()
                .unwrap_or_default()
                .with_event_observed(activity.slot, http::current_unix_ms());
            status.live = Some(live);
            if activity_status_sender.send(status).is_err() {
                tracing::debug!("yellowstone status receiver dropped");
            }
        },
    );
    let shutdown = shutdown_signal();
    tokio::pin!(http_server);
    tokio::pin!(pipeline);
    tokio::pin!(shutdown);

    tokio::select! {
        _ = &mut shutdown => {
            info!("yellowstone shutdown requested");
            let _ = shutdown_sender.send(true);
            (&mut http_server).await?;
            info!("yellowstone http server stopped");
            Ok(())
        }
        result = &mut http_server => {
            result?;
            info!("yellowstone http server stopped");
            Ok(())
        }
        result = &mut pipeline => {
            let summary = result?;
            info!(
                events_seen = summary.events_seen,
                events_skipped = summary.events_skipped,
                batches_written = summary.batches_written,
                events_attempted = summary.write_summary.attempted,
                events_inserted = summary.write_summary.inserted,
                events_deduplicated = summary.write_summary.deduplicated,
                last_persisted_slot = ?summary.last_persisted_slot,
                "yellowstone pipeline completed"
            );
            let _ = shutdown_sender.send(true);
            (&mut http_server).await?;
            info!("yellowstone http server stopped");
            Ok(())
        }
    }
}

#[cfg(feature = "yellowstone-live")]
fn encode_slot(slot: Option<u64>) -> u64 {
    slot.map_or(0, |slot| slot.saturating_add(1))
}

#[cfg(feature = "yellowstone-live")]
fn decode_slot(encoded: u64) -> Option<u64> {
    (encoded > 0).then_some(encoded - 1)
}

#[cfg(feature = "yellowstone-live")]
async fn shutdown_signal() {
    if let Err(err) = tokio::signal::ctrl_c().await {
        tracing::error!(error = %err, "failed to listen for shutdown signal");
    }
}

#[cfg(feature = "yellowstone-live")]
async fn wait_for_shutdown(mut shutdown: watch::Receiver<bool>) {
    while !*shutdown.borrow() {
        if shutdown.changed().await.is_err() {
            break;
        }
    }
}

#[cfg(not(feature = "yellowstone-live"))]
async fn run_yellowstone(config: Config) -> Result<(), AppRunError> {
    info!(
        stream_name = %config.stream_name,
        yellowstone_endpoint_configured = config.yellowstone_endpoint.is_some(),
        yellowstone_x_token_configured = config.yellowstone_x_token.is_some(),
        yellowstone_cluster = %config.yellowstone_cluster,
        yellowstone_subscriptions = %config.yellowstone_subscriptions.as_csv(),
        yellowstone_transaction_account_include_count = config.yellowstone_transaction_account_include.len(),
        yellowstone_transaction_account_exclude_count = config.yellowstone_transaction_account_exclude.len(),
        yellowstone_transaction_account_required_count = config.yellowstone_transaction_account_required.len(),
        yellowstone_reconnect_initial_delay_ms = config.yellowstone_reconnect.initial_delay.as_millis(),
        yellowstone_reconnect_max_delay_ms = config.yellowstone_reconnect.max_delay.as_millis(),
        yellowstone_reconnect_max_retries = ?config.yellowstone_reconnect.max_retries,
        "yellowstone live mode selected"
    );

    Err(AppRunError::YellowstoneRuntimeNotImplemented)
}
