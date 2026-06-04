use crate::yellowstone::proto::{
    YellowstoneProtoNormalizeError, normalize_yellowstone_proto_update,
};
use solana_yellowstone_domain::event::NormalizedEvent;
use std::collections::HashMap;
use std::fmt;
use std::future::Future;
use std::time::Duration;
use tokio::{sync::mpsc, time::sleep};
use yellowstone_grpc_proto::geyser::{
    CommitmentLevel, SubscribeRequest, SubscribeRequestFilterBlocks, SubscribeRequestFilterEntry,
    SubscribeRequestFilterSlots, SubscribeRequestFilterTransactions, geyser_client::GeyserClient,
};
use yellowstone_grpc_proto::tonic;

const DEFAULT_FILTER_NAME: &str = "default";

const DEFAULT_RECONNECT_INITIAL_DELAY: Duration = Duration::from_secs(1);
const DEFAULT_RECONNECT_MAX_DELAY: Duration = Duration::from_secs(30);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct YellowstoneReconnectConfig {
    pub initial_delay: Duration,
    pub max_delay: Duration,
    pub max_retries: Option<u32>,
}

impl Default for YellowstoneReconnectConfig {
    fn default() -> Self {
        Self {
            initial_delay: DEFAULT_RECONNECT_INITIAL_DELAY,
            max_delay: DEFAULT_RECONNECT_MAX_DELAY,
            max_retries: None,
        }
    }
}

impl YellowstoneReconnectConfig {
    fn normalized(self) -> Self {
        let initial_delay = if self.initial_delay.is_zero() {
            DEFAULT_RECONNECT_INITIAL_DELAY
        } else {
            self.initial_delay
        };
        let max_delay = self.max_delay.max(initial_delay);

        Self {
            initial_delay,
            max_delay,
            max_retries: self.max_retries,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum YellowstoneGrpcErrorKind {
    InvalidConfig,
    InvalidMetadataValue,
    Connect,
    Subscribe,
    Receive,
    Normalize,
    ReceiverClosed,
    StreamClosed,
}

impl YellowstoneGrpcErrorKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::InvalidConfig => "invalid_config",
            Self::InvalidMetadataValue => "invalid_metadata_value",
            Self::Connect => "connect",
            Self::Subscribe => "subscribe",
            Self::Receive => "receive",
            Self::Normalize => "normalize",
            Self::ReceiverClosed => "receiver_closed",
            Self::StreamClosed => "stream_closed",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct YellowstoneReconnectEvent {
    pub retry_attempt: u32,
    pub delay: Duration,
    pub error_kind: YellowstoneGrpcErrorKind,
    pub error_message: String,
    pub from_slot: Option<u64>,
}

#[derive(Clone, PartialEq, Eq)]
pub struct YellowstoneGrpcConfig {
    pub endpoint: String,
    pub x_token: Option<String>,
    pub cluster: String,
    pub commitment: YellowstoneCommitment,
    pub from_slot: Option<u64>,
    pub filter_name: String,
    pub subscribe_slots: bool,
    pub subscribe_transactions: bool,
    pub subscribe_blocks: bool,
    pub subscribe_entries: bool,
    pub transaction_account_include: Vec<String>,
    pub transaction_account_exclude: Vec<String>,
    pub transaction_account_required: Vec<String>,
}

impl fmt::Debug for YellowstoneGrpcConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("YellowstoneGrpcConfig")
            .field("endpoint_configured", &!self.endpoint.is_empty())
            .field("x_token_configured", &self.x_token.is_some())
            .field("cluster", &self.cluster)
            .field("commitment", &self.commitment)
            .field("from_slot", &self.from_slot)
            .field("filter_name", &self.filter_name)
            .field("subscribe_slots", &self.subscribe_slots)
            .field("subscribe_transactions", &self.subscribe_transactions)
            .field("subscribe_blocks", &self.subscribe_blocks)
            .field("subscribe_entries", &self.subscribe_entries)
            .field(
                "transaction_account_include_count",
                &self.transaction_account_include.len(),
            )
            .field(
                "transaction_account_exclude_count",
                &self.transaction_account_exclude.len(),
            )
            .field(
                "transaction_account_required_count",
                &self.transaction_account_required.len(),
            )
            .finish()
    }
}

impl YellowstoneGrpcConfig {
    pub fn slots_only(endpoint: impl Into<String>, cluster: impl Into<String>) -> Self {
        Self {
            endpoint: endpoint.into(),
            x_token: None,
            cluster: cluster.into(),
            commitment: YellowstoneCommitment::Confirmed,
            from_slot: None,
            filter_name: DEFAULT_FILTER_NAME.to_owned(),
            subscribe_slots: true,
            subscribe_transactions: false,
            subscribe_blocks: false,
            subscribe_entries: false,
            transaction_account_include: Vec::new(),
            transaction_account_exclude: Vec::new(),
            transaction_account_required: Vec::new(),
        }
    }

    pub fn subscribe_request(&self) -> Result<SubscribeRequest, YellowstoneGrpcError> {
        self.validate()?;

        let mut slots = HashMap::new();
        if self.subscribe_slots {
            slots.insert(
                self.filter_name.clone(),
                SubscribeRequestFilterSlots {
                    filter_by_commitment: Some(false),
                    interslot_updates: Some(false),
                },
            );
        }

        let mut transactions = HashMap::new();
        if self.subscribe_transactions {
            transactions.insert(
                self.filter_name.clone(),
                SubscribeRequestFilterTransactions {
                    vote: Some(false),
                    failed: None,
                    signature: None,
                    account_include: self.transaction_account_include.clone(),
                    account_exclude: self.transaction_account_exclude.clone(),
                    account_required: self.transaction_account_required.clone(),
                },
            );
        }

        let mut blocks = HashMap::new();
        if self.subscribe_blocks {
            blocks.insert(
                self.filter_name.clone(),
                SubscribeRequestFilterBlocks {
                    account_include: Vec::new(),
                    include_transactions: Some(false),
                    include_accounts: Some(false),
                    include_entries: Some(false),
                    cuckoo_account_include: None,
                },
            );
        }

        let mut entry = HashMap::new();
        if self.subscribe_entries {
            entry.insert(self.filter_name.clone(), SubscribeRequestFilterEntry {});
        }

        Ok(SubscribeRequest {
            slots,
            transactions,
            blocks,
            entry,
            commitment: Some(self.commitment.as_proto_i32()),
            from_slot: self.from_slot,
            ..SubscribeRequest::default()
        })
    }

    fn validate(&self) -> Result<(), YellowstoneGrpcError> {
        if self.endpoint.trim().is_empty() {
            return Err(YellowstoneGrpcError::InvalidConfig {
                message: "yellowstone endpoint must not be empty".to_owned(),
            });
        }
        if self.cluster.trim().is_empty() {
            return Err(YellowstoneGrpcError::InvalidConfig {
                message: "yellowstone cluster must not be empty".to_owned(),
            });
        }
        if self.filter_name.trim().is_empty() {
            return Err(YellowstoneGrpcError::InvalidConfig {
                message: "yellowstone filter name must not be empty".to_owned(),
            });
        }
        if !(self.subscribe_slots
            || self.subscribe_transactions
            || self.subscribe_blocks
            || self.subscribe_entries)
        {
            return Err(YellowstoneGrpcError::InvalidConfig {
                message: "at least one yellowstone subscription filter must be enabled".to_owned(),
            });
        }

        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum YellowstoneCommitment {
    Processed,
    Confirmed,
    Finalized,
}

impl YellowstoneCommitment {
    fn as_proto_i32(&self) -> i32 {
        match self {
            Self::Processed => CommitmentLevel::Processed as i32,
            Self::Confirmed => CommitmentLevel::Confirmed as i32,
            Self::Finalized => CommitmentLevel::Finalized as i32,
        }
    }
}

pub async fn run_yellowstone_grpc_producer_with_reconnect(
    config: YellowstoneGrpcConfig,
    reconnect: YellowstoneReconnectConfig,
    sender: mpsc::Sender<NormalizedEvent>,
) -> Result<(), YellowstoneGrpcError> {
    run_yellowstone_grpc_producer_with_reconnect_status(config, reconnect, sender, |_| {}).await
}

pub async fn run_yellowstone_grpc_producer_with_reconnect_status<O>(
    config: YellowstoneGrpcConfig,
    reconnect: YellowstoneReconnectConfig,
    sender: mpsc::Sender<NormalizedEvent>,
    on_reconnect: O,
) -> Result<(), YellowstoneGrpcError>
where
    O: FnMut(YellowstoneReconnectEvent) + Send,
{
    let noop = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
    run_yellowstone_grpc_producer_with_reconnect_status_and_config(
        config,
        reconnect,
        sender,
        on_reconnect,
        |_| {},
        Some(noop),
    )
    .await
}

pub async fn run_yellowstone_grpc_producer_with_reconnect_status_and_config<O, C>(
    config: YellowstoneGrpcConfig,
    reconnect: YellowstoneReconnectConfig,
    sender: mpsc::Sender<NormalizedEvent>,
    on_reconnect: O,
    configure_attempt: C,
    decode_errors: Option<std::sync::Arc<std::sync::atomic::AtomicU64>>,
) -> Result<(), YellowstoneGrpcError>
where
    O: FnMut(YellowstoneReconnectEvent) + Send,
    C: FnMut(&mut YellowstoneGrpcConfig) + Send,
{
    run_yellowstone_reconnect_loop(
        config,
        reconnect,
        move |attempt_config| {
            let sender = sender.clone();
            let decode_errors = decode_errors.clone();
            async move { run_yellowstone_grpc_producer(attempt_config, sender, decode_errors).await }
        },
        on_reconnect,
        configure_attempt,
    )
    .await
}

async fn run_yellowstone_reconnect_loop<A, F, O, C>(
    mut config: YellowstoneGrpcConfig,
    reconnect: YellowstoneReconnectConfig,
    mut attempt: A,
    mut on_reconnect: O,
    mut configure_attempt: C,
) -> Result<(), YellowstoneGrpcError>
where
    A: FnMut(YellowstoneGrpcConfig) -> F,
    F: Future<Output = Result<(), YellowstoneGrpcError>>,
    O: FnMut(YellowstoneReconnectEvent),
    C: FnMut(&mut YellowstoneGrpcConfig),
{
    let reconnect = reconnect.normalized();
    let mut retry_attempt = 0_u32;
    let mut delay = reconnect.initial_delay;

    loop {
        configure_attempt(&mut config);
        let attempt_from_slot = config.from_slot;
        match attempt(config.clone()).await {
            Ok(()) => return Ok(()),
            Err(err) if !err.is_retryable() => return Err(err),
            Err(err) => {
                retry_attempt = retry_attempt.saturating_add(1);
                if reconnect
                    .max_retries
                    .is_some_and(|max_retries| retry_attempt > max_retries)
                {
                    return Err(err);
                }

                let event = YellowstoneReconnectEvent {
                    retry_attempt,
                    delay,
                    error_kind: err.kind(),
                    error_message: err.to_string(),
                    from_slot: attempt_from_slot,
                };
                on_reconnect(event.clone());

                tracing::warn!(
                    retry_attempt,
                    delay_ms = delay.as_millis(),
                    from_slot = ?event.from_slot,
                    error_kind = event.error_kind.as_str(),
                    error = %event.error_message,
                    "yellowstone producer failed; reconnecting after backoff"
                );
                sleep(delay).await;
                delay = next_backoff_delay(delay, reconnect.max_delay);
            }
        }
    }
}

fn next_backoff_delay(current: Duration, max_delay: Duration) -> Duration {
    current.saturating_mul(2).min(max_delay)
}

pub async fn run_yellowstone_grpc_producer(
    config: YellowstoneGrpcConfig,
    sender: mpsc::Sender<NormalizedEvent>,
    decode_errors: Option<std::sync::Arc<std::sync::atomic::AtomicU64>>,
) -> Result<(), YellowstoneGrpcError> {
    let request = config.subscribe_request()?;
    let mut client = GeyserClient::connect(config.endpoint.clone())
        .await
        .map_err(YellowstoneGrpcError::Connect)?;

    let mut request = tonic::Request::new(tokio_stream::once(request));
    if let Some(token) = config.x_token.as_deref() {
        let value = tonic::metadata::MetadataValue::try_from(token)
            .map_err(YellowstoneGrpcError::InvalidMetadataValue)?;
        request.metadata_mut().insert("x-token", value);
    }

    let mut updates = client
        .subscribe(request)
        .await
        .map_err(YellowstoneGrpcError::Subscribe)?
        .into_inner();

    while let Some(update) = updates
        .message()
        .await
        .map_err(YellowstoneGrpcError::Receive)?
    {
        let events = match normalize_yellowstone_proto_update(&config.cluster, update) {
            Ok(events) => events,
            Err(err) => {
                tracing::warn!(error = %err, "skipping malformed yellowstone update");
                if let Some(ref counter) = decode_errors {
                    counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
                continue;
            }
        };
        for event in events {
            sender
                .send(event)
                .await
                .map_err(|_| YellowstoneGrpcError::ReceiverClosed)?;
        }
    }

    Err(YellowstoneGrpcError::StreamClosed)
}

#[derive(Debug)]
pub enum YellowstoneGrpcError {
    InvalidConfig { message: String },
    InvalidMetadataValue(tonic::metadata::errors::InvalidMetadataValue),
    Connect(tonic::transport::Error),
    Subscribe(tonic::Status),
    Receive(tonic::Status),
    Normalize(YellowstoneProtoNormalizeError),
    ReceiverClosed,
    StreamClosed,
}

impl fmt::Display for YellowstoneGrpcError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidConfig { message } => f.write_str(message),
            Self::InvalidMetadataValue(err) => {
                write!(f, "invalid yellowstone x-token metadata: {err}")
            }
            Self::Connect(_) => f.write_str("failed to connect to yellowstone endpoint"),
            Self::Subscribe(err) => write!(
                f,
                "yellowstone subscribe failed with gRPC status {:?}",
                err.code()
            ),
            Self::Receive(err) => write!(
                f,
                "yellowstone stream receive failed with gRPC status {:?}",
                err.code()
            ),
            Self::Normalize(err) => write!(f, "yellowstone update normalization failed: {err}"),
            Self::ReceiverClosed => f.write_str("yellowstone event receiver closed"),
            Self::StreamClosed => f.write_str("yellowstone stream closed by server"),
        }
    }
}

impl YellowstoneGrpcError {
    pub fn kind(&self) -> YellowstoneGrpcErrorKind {
        match self {
            Self::InvalidConfig { .. } => YellowstoneGrpcErrorKind::InvalidConfig,
            Self::InvalidMetadataValue(_) => YellowstoneGrpcErrorKind::InvalidMetadataValue,
            Self::Connect(_) => YellowstoneGrpcErrorKind::Connect,
            Self::Subscribe(_) => YellowstoneGrpcErrorKind::Subscribe,
            Self::Receive(_) => YellowstoneGrpcErrorKind::Receive,
            Self::Normalize(_) => YellowstoneGrpcErrorKind::Normalize,
            Self::ReceiverClosed => YellowstoneGrpcErrorKind::ReceiverClosed,
            Self::StreamClosed => YellowstoneGrpcErrorKind::StreamClosed,
        }
    }

    fn is_retryable(&self) -> bool {
        match self {
            Self::Connect(_) => true,
            Self::Subscribe(status) | Self::Receive(status) => !matches!(
                status.code(),
                tonic::Code::InvalidArgument
                    | tonic::Code::Unauthenticated
                    | tonic::Code::PermissionDenied
            ),
            Self::InvalidConfig { .. }
            | Self::InvalidMetadataValue(_)
            | Self::Normalize(_)
            | Self::ReceiverClosed => false,
            Self::StreamClosed => true,
        }
    }
}

impl std::error::Error for YellowstoneGrpcError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::InvalidConfig { .. } | Self::ReceiverClosed | Self::StreamClosed => None,
            Self::InvalidMetadataValue(err) => Some(err),
            Self::Connect(err) => Some(err),
            Self::Subscribe(err) | Self::Receive(err) => Some(err),
            Self::Normalize(err) => Some(err),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        YellowstoneCommitment, YellowstoneGrpcConfig, YellowstoneGrpcError,
        YellowstoneReconnectConfig,
    };
    use std::sync::{Arc, Mutex};
    use std::time::Duration;
    use yellowstone_grpc_proto::geyser::CommitmentLevel;

    #[test]
    fn builds_conservative_slots_only_request_by_default() {
        let config = YellowstoneGrpcConfig::slots_only("https://provider.example", "mainnet-beta");

        let request = config.subscribe_request().expect("request should build");

        assert!(request.accounts.is_empty());
        assert!(request.transactions.is_empty());
        assert!(request.blocks.is_empty());
        assert!(request.entry.is_empty());
        assert_eq!(request.slots.len(), 1);
        assert_eq!(request.commitment, Some(CommitmentLevel::Confirmed as i32));
        assert_eq!(request.from_slot, None);

        let slot_filter = request.slots.get("default").expect("slot filter exists");
        assert_eq!(slot_filter.filter_by_commitment, Some(false));
        assert_eq!(slot_filter.interslot_updates, Some(false));
    }

    #[test]
    fn builds_multi_filter_request_with_from_slot() {
        let mut config = YellowstoneGrpcConfig::slots_only("https://provider.example", "devnet");
        config.commitment = YellowstoneCommitment::Finalized;
        config.from_slot = Some(42);
        config.filter_name = "live".to_owned();
        config.subscribe_transactions = true;
        config.subscribe_blocks = true;
        config.subscribe_entries = true;
        config.transaction_account_include = vec!["include-1".to_owned(), "include-2".to_owned()];
        config.transaction_account_exclude = vec!["exclude-1".to_owned()];
        config.transaction_account_required = vec!["required-1".to_owned()];

        let request = config.subscribe_request().expect("request should build");

        assert!(request.slots.contains_key("live"));
        let transaction_filter = request
            .transactions
            .get("live")
            .expect("transaction filter exists");
        assert_eq!(
            transaction_filter.account_include,
            vec!["include-1".to_owned(), "include-2".to_owned()]
        );
        assert_eq!(
            transaction_filter.account_exclude,
            vec!["exclude-1".to_owned()]
        );
        assert_eq!(
            transaction_filter.account_required,
            vec!["required-1".to_owned()]
        );
        assert!(request.blocks.contains_key("live"));
        assert!(request.entry.contains_key("live"));
        assert_eq!(request.commitment, Some(CommitmentLevel::Finalized as i32));
        assert_eq!(request.from_slot, Some(42));
    }

    #[test]
    fn debug_does_not_include_endpoint_or_token() {
        let mut config = YellowstoneGrpcConfig::slots_only(
            "https://provider.example/secret-path?api_key=endpoint-secret",
            "mainnet-beta",
        );
        config.x_token = Some("yellowstone-secret-token".to_owned());
        config.transaction_account_include = vec!["sensitive-account-filter".to_owned()];

        let debug = format!("{config:?}");

        assert!(debug.contains("endpoint_configured"));
        assert!(debug.contains("x_token_configured"));
        assert!(!debug.contains("provider.example"));
        assert!(!debug.contains("secret-path"));
        assert!(!debug.contains("endpoint-secret"));
        assert!(!debug.contains("yellowstone-secret-token"));
        assert!(!debug.contains("sensitive-account-filter"));
    }

    #[test]
    fn default_reconnect_config_uses_bounded_unlimited_backoff() {
        let config = super::YellowstoneReconnectConfig::default();

        assert_eq!(config.initial_delay, std::time::Duration::from_secs(1));
        assert_eq!(config.max_delay, std::time::Duration::from_secs(30));
        assert_eq!(config.max_retries, None);
        assert_eq!(
            super::next_backoff_delay(
                std::time::Duration::from_secs(20),
                std::time::Duration::from_secs(30),
            ),
            std::time::Duration::from_secs(30)
        );
    }

    #[tokio::test]
    async fn reconnect_loop_refreshes_from_slot_between_attempts() {
        let latest_persisted_slot = Arc::new(Mutex::new(Some(10_u64)));
        let attempts = Arc::new(Mutex::new(Vec::new()));
        let reconnect_events = Arc::new(Mutex::new(Vec::new()));
        let attempt_count = Arc::new(Mutex::new(0_u32));

        let latest_for_config = latest_persisted_slot.clone();
        let attempts_for_callback = attempts.clone();
        let attempt_count_for_callback = attempt_count.clone();
        let reconnect_events_for_callback = reconnect_events.clone();
        let latest_for_reconnect = latest_persisted_slot.clone();

        super::run_yellowstone_reconnect_loop(
            YellowstoneGrpcConfig::slots_only("https://provider.example", "mainnet-beta"),
            YellowstoneReconnectConfig {
                initial_delay: Duration::from_millis(1),
                max_delay: Duration::from_millis(1),
                max_retries: Some(3),
            },
            move |config| {
                let attempts = attempts_for_callback.clone();
                let attempt_count = attempt_count_for_callback.clone();
                async move {
                    attempts
                        .lock()
                        .expect("attempts lock")
                        .push(config.from_slot);
                    let mut attempt_count = attempt_count.lock().expect("attempt count lock");
                    *attempt_count += 1;
                    if *attempt_count < 3 {
                        Err(YellowstoneGrpcError::Receive(
                            yellowstone_grpc_proto::tonic::Status::unavailable(
                                "provider unavailable",
                            ),
                        ))
                    } else {
                        Ok(())
                    }
                }
            },
            move |event| {
                reconnect_events_for_callback
                    .lock()
                    .expect("reconnect events lock")
                    .push((event.retry_attempt, event.from_slot));
                *latest_for_reconnect
                    .lock()
                    .expect("latest persisted slot lock") = Some(20);
            },
            move |config| {
                config.from_slot = *latest_for_config
                    .lock()
                    .expect("latest persisted slot lock");
            },
        )
        .await
        .expect("reconnect loop should eventually succeed");

        assert_eq!(
            *attempts.lock().expect("attempts lock"),
            vec![Some(10), Some(20), Some(20)]
        );
        assert_eq!(
            *reconnect_events.lock().expect("reconnect events lock"),
            vec![(1, Some(10)), (2, Some(20))]
        );
    }

    #[test]
    fn retry_classification_treats_auth_and_contract_errors_as_fatal() {
        let unauthenticated = YellowstoneGrpcError::Subscribe(
            yellowstone_grpc_proto::tonic::Status::unauthenticated("bad token"),
        );
        let unavailable = YellowstoneGrpcError::Receive(
            yellowstone_grpc_proto::tonic::Status::unavailable("provider unavailable"),
        );
        let invalid_config = YellowstoneGrpcError::InvalidConfig {
            message: "bad config".to_owned(),
        };

        assert!(!unauthenticated.is_retryable());
        assert!(unavailable.is_retryable());
        assert!(!invalid_config.is_retryable());
    }

    #[test]
    fn reconnect_config_normalizes_zero_delays() {
        let config = super::YellowstoneReconnectConfig {
            initial_delay: std::time::Duration::ZERO,
            max_delay: std::time::Duration::ZERO,
            max_retries: Some(3),
        }
        .normalized();

        assert_eq!(config.initial_delay, std::time::Duration::from_secs(1));
        assert_eq!(config.max_delay, std::time::Duration::from_secs(1));
        assert_eq!(config.max_retries, Some(3));
    }

    #[test]
    fn display_redacts_external_grpc_error_details() {
        let subscribe = YellowstoneGrpcError::Subscribe(
            yellowstone_grpc_proto::tonic::Status::unauthenticated(
                "token yellowstone-secret-token rejected by provider.example",
            ),
        );
        let receive =
            YellowstoneGrpcError::Receive(yellowstone_grpc_proto::tonic::Status::internal(
                "provider.example leaked endpoint-secret",
            ));

        assert_eq!(
            subscribe.to_string(),
            "yellowstone subscribe failed with gRPC status Unauthenticated"
        );
        assert_eq!(
            receive.to_string(),
            "yellowstone stream receive failed with gRPC status Internal"
        );
        assert!(!subscribe.to_string().contains("yellowstone-secret-token"));
        assert!(!subscribe.to_string().contains("provider.example"));
        assert!(!receive.to_string().contains("endpoint-secret"));
        assert!(!receive.to_string().contains("provider.example"));
    }

    #[test]
    fn rejects_empty_subscription_set() {
        let mut config =
            YellowstoneGrpcConfig::slots_only("https://provider.example", "mainnet-beta");
        config.subscribe_slots = false;

        let err = config
            .subscribe_request()
            .expect_err("empty subscription should fail");

        assert!(matches!(err, YellowstoneGrpcError::InvalidConfig { .. }));
        assert_eq!(
            err.to_string(),
            "at least one yellowstone subscription filter must be enabled"
        );
    }

    #[test]
    fn rejects_empty_endpoint() {
        let config = YellowstoneGrpcConfig::slots_only(" ", "mainnet-beta");

        let err = config
            .subscribe_request()
            .expect_err("empty endpoint should fail");

        assert!(matches!(err, YellowstoneGrpcError::InvalidConfig { .. }));
        assert_eq!(err.to_string(), "yellowstone endpoint must not be empty");
    }

    #[test]
    fn stream_closed_is_retryable() {
        let err = YellowstoneGrpcError::StreamClosed;
        assert!(err.is_retryable());
        assert_eq!(err.kind().as_str(), "stream_closed");
        assert_eq!(err.to_string(), "yellowstone stream closed by server");
    }

    #[tokio::test]
    async fn reconnect_loop_retries_on_stream_closed() {
        let attempts = Arc::new(Mutex::new(0_u32));
        let attempts_for_callback = attempts.clone();

        super::run_yellowstone_reconnect_loop(
            YellowstoneGrpcConfig::slots_only("https://provider.example", "mainnet-beta"),
            YellowstoneReconnectConfig {
                initial_delay: Duration::from_millis(1),
                max_delay: Duration::from_millis(1),
                max_retries: Some(2),
            },
            move |_config| {
                let attempts = attempts_for_callback.clone();
                async move {
                    let mut count = attempts.lock().expect("attempts lock");
                    *count += 1;
                    if *count < 3 {
                        Err(YellowstoneGrpcError::StreamClosed)
                    } else {
                        Ok(())
                    }
                }
            },
            |_| {},
            |_| {},
        )
        .await
        .expect("reconnect loop should eventually succeed after stream closes");

        assert_eq!(*attempts.lock().expect("attempts lock"), 3);
    }
}
