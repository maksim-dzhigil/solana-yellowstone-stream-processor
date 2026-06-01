use crate::yellowstone::proto::{
    YellowstoneProtoNormalizeError, normalize_yellowstone_proto_update,
};
use solana_yellowstone_domain::event::NormalizedEvent;
use std::collections::HashMap;
use std::fmt;
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
                    filter_by_commitment: Some(true),
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
    let reconnect = reconnect.normalized();
    let mut retry_attempt = 0_u32;
    let mut delay = reconnect.initial_delay;

    loop {
        match run_yellowstone_grpc_producer(config.clone(), sender.clone()).await {
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

                tracing::warn!(
                    retry_attempt,
                    delay_ms = delay.as_millis(),
                    error = %err,
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
        let events = normalize_yellowstone_proto_update(&config.cluster, update)
            .map_err(YellowstoneGrpcError::Normalize)?;
        for event in events {
            sender
                .send(event)
                .await
                .map_err(|_| YellowstoneGrpcError::ReceiverClosed)?;
        }
    }

    Ok(())
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
        }
    }
}

impl YellowstoneGrpcError {
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
        }
    }
}

impl std::error::Error for YellowstoneGrpcError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::InvalidConfig { .. } | Self::ReceiverClosed => None,
            Self::InvalidMetadataValue(err) => Some(err),
            Self::Connect(err) => Some(err),
            Self::Subscribe(err) | Self::Receive(err) => Some(err),
            Self::Normalize(err) => Some(err),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{YellowstoneCommitment, YellowstoneGrpcConfig, YellowstoneGrpcError};
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
        assert_eq!(slot_filter.filter_by_commitment, Some(true));
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
}
