use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::fmt;

pub const EVENT_IDENTITY_VERSION: u16 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EventKind {
    Account,
    Block,
    Entry,
    Instruction,
    Slot,
    Transaction,
}

impl EventKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Account => "account",
            Self::Block => "block",
            Self::Entry => "entry",
            Self::Instruction => "instruction",
            Self::Slot => "slot",
            Self::Transaction => "transaction",
        }
    }
}

impl fmt::Display for EventKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SlotStatus {
    Processed,
    Confirmed,
    Finalized,
    Dead,
}

impl SlotStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Processed => "processed",
            Self::Confirmed => "confirmed",
            Self::Finalized => "finalized",
            Self::Dead => "dead",
        }
    }
}

impl fmt::Display for SlotStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum EventIdentity {
    Account {
        cluster: String,
        slot: u64,
        account: String,
        write_version: u64,
        txn_signature: Option<String>,
        is_startup: bool,
    },
    Block {
        cluster: String,
        slot: u64,
        blockhash: String,
    },
    Entry {
        cluster: String,
        slot: u64,
        index: u64,
    },
    Instruction {
        cluster: String,
        slot: u64,
        signature: String,
        transaction_index: u64,
        instruction_index: u16,
        inner_instruction_index: Option<u16>,
        program_id: String,
    },
    Slot {
        cluster: String,
        slot: u64,
        status: SlotStatus,
    },
    Transaction {
        cluster: String,
        slot: u64,
        signature: String,
        index: u64,
    },
}

impl EventIdentity {
    pub fn kind(&self) -> EventKind {
        match self {
            Self::Account { .. } => EventKind::Account,
            Self::Block { .. } => EventKind::Block,
            Self::Entry { .. } => EventKind::Entry,
            Self::Instruction { .. } => EventKind::Instruction,
            Self::Slot { .. } => EventKind::Slot,
            Self::Transaction { .. } => EventKind::Transaction,
        }
    }

    pub fn slot(&self) -> u64 {
        match self {
            Self::Account { slot, .. }
            | Self::Block { slot, .. }
            | Self::Entry { slot, .. }
            | Self::Instruction { slot, .. }
            | Self::Slot { slot, .. }
            | Self::Transaction { slot, .. } => *slot,
        }
    }

    pub fn signature(&self) -> Option<&str> {
        match self {
            Self::Account { txn_signature, .. } => txn_signature.as_deref(),
            Self::Instruction { signature, .. } | Self::Transaction { signature, .. } => {
                Some(signature.as_str())
            }
            Self::Block { .. } | Self::Entry { .. } | Self::Slot { .. } => None,
        }
    }

    pub fn program_id(&self) -> Option<&str> {
        match self {
            Self::Instruction { program_id, .. } => Some(program_id.as_str()),
            Self::Account { .. }
            | Self::Block { .. }
            | Self::Entry { .. }
            | Self::Slot { .. }
            | Self::Transaction { .. } => None,
        }
    }

    pub fn account(&self) -> Option<&str> {
        match self {
            Self::Account { account, .. } => Some(account.as_str()),
            Self::Block { .. }
            | Self::Entry { .. }
            | Self::Instruction { .. }
            | Self::Slot { .. }
            | Self::Transaction { .. } => None,
        }
    }

    pub fn validate(&self) -> Result<(), EventIdentityError> {
        match self {
            Self::Account {
                cluster,
                account,
                txn_signature,
                ..
            } => {
                validate_non_empty("cluster", cluster)?;
                validate_non_empty("account", account)?;
                if let Some(signature) = txn_signature {
                    validate_non_empty("txn_signature", signature)?;
                }
            }
            Self::Block {
                cluster, blockhash, ..
            } => {
                validate_non_empty("cluster", cluster)?;
                validate_non_empty("blockhash", blockhash)?;
            }
            Self::Entry { cluster, .. } | Self::Slot { cluster, .. } => {
                validate_non_empty("cluster", cluster)?;
            }
            Self::Instruction {
                cluster,
                signature,
                program_id,
                ..
            } => {
                validate_non_empty("cluster", cluster)?;
                validate_non_empty("signature", signature)?;
                validate_non_empty("program_id", program_id)?;
            }
            Self::Transaction {
                cluster, signature, ..
            } => {
                validate_non_empty("cluster", cluster)?;
                validate_non_empty("signature", signature)?;
            }
        }

        Ok(())
    }

    pub fn canonical_key(&self) -> String {
        match self {
            Self::Account {
                cluster,
                slot,
                account,
                write_version,
                txn_signature,
                is_startup,
            } => format!(
                "v={EVENT_IDENTITY_VERSION}|kind=account|cluster={}|slot={slot}|account={}|write_version={write_version}|txn_signature={}|is_startup={is_startup}",
                escape_key_value(cluster),
                escape_key_value(account),
                encode_optional(txn_signature.as_deref())
            ),
            Self::Block {
                cluster,
                slot,
                blockhash,
            } => format!(
                "v={EVENT_IDENTITY_VERSION}|kind=block|cluster={}|slot={slot}|blockhash={}",
                escape_key_value(cluster),
                escape_key_value(blockhash)
            ),
            Self::Entry {
                cluster,
                slot,
                index,
            } => format!(
                "v={EVENT_IDENTITY_VERSION}|kind=entry|cluster={}|slot={slot}|index={index}",
                escape_key_value(cluster)
            ),
            Self::Instruction {
                cluster,
                slot,
                signature,
                transaction_index,
                instruction_index,
                inner_instruction_index,
                program_id,
            } => format!(
                "v={EVENT_IDENTITY_VERSION}|kind=instruction|cluster={}|slot={slot}|signature={}|transaction_index={transaction_index}|instruction_index={instruction_index}|inner_instruction_index={}|program_id={}",
                escape_key_value(cluster),
                escape_key_value(signature),
                inner_instruction_index
                    .map(|index| index.to_string())
                    .unwrap_or_else(|| "<none>".to_owned()),
                escape_key_value(program_id)
            ),
            Self::Slot {
                cluster,
                slot,
                status,
            } => format!(
                "v={EVENT_IDENTITY_VERSION}|kind=slot|cluster={}|slot={slot}|status={status}",
                escape_key_value(cluster)
            ),
            Self::Transaction {
                cluster,
                slot,
                signature,
                index,
            } => format!(
                "v={EVENT_IDENTITY_VERSION}|kind=transaction|cluster={}|slot={slot}|signature={}|index={index}",
                escape_key_value(cluster),
                escape_key_value(signature)
            ),
        }
    }

    pub fn event_id(&self) -> String {
        let mut hasher = Sha256::new();
        hasher.update(self.canonical_key().as_bytes());
        format!(
            "v{EVENT_IDENTITY_VERSION}:{}",
            hex::encode(hasher.finalize())
        )
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NormalizedEvent {
    pub identity: EventIdentity,
    pub payload: Value,
}

impl NormalizedEvent {
    pub fn new(identity: EventIdentity, payload: Value) -> Self {
        Self { identity, payload }
    }

    pub fn event_id(&self) -> String {
        self.identity.event_id()
    }

    pub fn identity_version(&self) -> u16 {
        EVENT_IDENTITY_VERSION
    }

    pub fn kind(&self) -> EventKind {
        self.identity.kind()
    }

    pub fn slot(&self) -> u64 {
        self.identity.slot()
    }

    pub fn signature(&self) -> Option<&str> {
        self.identity.signature()
    }

    pub fn program_id(&self) -> Option<&str> {
        self.identity.program_id()
    }

    pub fn account(&self) -> Option<&str> {
        self.identity.account()
    }

    pub fn validate(&self) -> Result<(), EventIdentityError> {
        self.identity.validate()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EventIdentityError {
    EmptyField { field: &'static str },
}

impl fmt::Display for EventIdentityError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyField { field } => {
                write!(f, "event identity field {field} must not be empty")
            }
        }
    }
}

impl std::error::Error for EventIdentityError {}

#[derive(Debug)]
pub enum NormalizedEventParseError {
    EmptyLine,
    InvalidJson(serde_json::Error),
    InvalidIdentity(EventIdentityError),
}

impl fmt::Display for NormalizedEventParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyLine => write!(f, "event line must not be empty"),
            Self::InvalidJson(err) => write!(f, "invalid event json: {err}"),
            Self::InvalidIdentity(err) => write!(f, "invalid event identity: {err}"),
        }
    }
}

impl std::error::Error for NormalizedEventParseError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::InvalidJson(err) => Some(err),
            Self::InvalidIdentity(err) => Some(err),
            Self::EmptyLine => None,
        }
    }
}

pub fn parse_normalized_event(line: &str) -> Result<NormalizedEvent, NormalizedEventParseError> {
    if line.trim().is_empty() {
        return Err(NormalizedEventParseError::EmptyLine);
    }

    let event: NormalizedEvent =
        serde_json::from_str(line).map_err(NormalizedEventParseError::InvalidJson)?;
    event
        .validate()
        .map_err(NormalizedEventParseError::InvalidIdentity)?;

    Ok(event)
}

fn validate_non_empty(field: &'static str, value: &str) -> Result<(), EventIdentityError> {
    if value.trim().is_empty() {
        return Err(EventIdentityError::EmptyField { field });
    }

    Ok(())
}

fn encode_optional(value: Option<&str>) -> String {
    value
        .map(|value| format!("some:{}", escape_key_value(value)))
        .unwrap_or_else(|| "none".to_owned())
}

fn escape_key_value(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('|', "\\|")
        .replace('=', "\\=")
}

#[cfg(test)]
mod tests {
    use super::{
        EventIdentity, EventIdentityError, EventKind, NormalizedEvent, NormalizedEventParseError,
        SlotStatus, parse_normalized_event,
    };
    use serde_json::json;

    #[test]
    fn parses_valid_transaction_event() {
        let line = r#"{"identity":{"kind":"transaction","cluster":"localnet","slot":1,"signature":"sig-1","index":0},"payload":{"source":"fixture"}}"#;

        let event = parse_normalized_event(line).expect("event should parse");

        assert_eq!(event.slot(), 1);
        assert_eq!(event.signature(), Some("sig-1"));
        assert_eq!(event.program_id(), None);
        assert_eq!(event.account(), None);
        assert_eq!(event.kind(), EventKind::Transaction);
        assert_eq!(event.payload, json!({ "source": "fixture" }));
    }

    #[test]
    fn duplicate_lines_have_same_event_id() {
        let line = r#"{"identity":{"kind":"transaction","cluster":"localnet","slot":2,"signature":"sig-2","index":0},"payload":{"index":2}}"#;

        let first = parse_normalized_event(line).expect("first event should parse");
        let second = parse_normalized_event(line).expect("second event should parse");

        assert_eq!(first.event_id(), second.event_id());
    }

    #[test]
    fn rejects_empty_line() {
        let err = parse_normalized_event("   ").expect_err("empty line should fail");

        assert!(matches!(err, NormalizedEventParseError::EmptyLine));
    }

    #[test]
    fn rejects_invalid_json() {
        let err = parse_normalized_event("not-json").expect_err("invalid json should fail");

        assert!(matches!(err, NormalizedEventParseError::InvalidJson(_)));
    }

    #[test]
    fn rejects_missing_required_fields() {
        let err = parse_normalized_event(
            r#"{"identity":{"kind":"transaction","cluster":"localnet","slot":1},"payload":{}}"#,
        )
        .expect_err("missing signature should fail");

        assert!(matches!(err, NormalizedEventParseError::InvalidJson(_)));
    }

    #[test]
    fn rejects_empty_identity_fields() {
        let err = parse_normalized_event(
            r#"{"identity":{"kind":"transaction","cluster":"localnet","slot":1,"signature":" ","index":0},"payload":{}}"#,
        )
        .expect_err("empty signature should fail");

        assert!(matches!(
            err,
            NormalizedEventParseError::InvalidIdentity(EventIdentityError::EmptyField {
                field: "signature"
            })
        ));
    }

    #[test]
    fn serializes_event_kind_as_string() {
        let kind = EventKind::Account;

        assert_eq!(
            serde_json::to_value(kind).expect("serialize"),
            json!("account")
        );
    }

    #[test]
    fn event_id_is_stable_for_same_identity() {
        let event = NormalizedEvent::new(
            EventIdentity::Transaction {
                cluster: "localnet".to_owned(),
                slot: 42,
                signature: "sig-1".to_owned(),
                index: 7,
            },
            json!({ "source": "test" }),
        );

        assert_eq!(
            event.identity.canonical_key(),
            "v=1|kind=transaction|cluster=localnet|slot=42|signature=sig-1|index=7"
        );
        assert!(event.event_id().starts_with("v1:"));
        assert_eq!(event.event_id().len(), 67);
    }

    #[test]
    fn event_id_ignores_payload_for_same_source_event() {
        let first = NormalizedEvent::new(
            EventIdentity::Transaction {
                cluster: "localnet".to_owned(),
                slot: 42,
                signature: "sig-1".to_owned(),
                index: 7,
            },
            json!({ "decoder_version": 1 }),
        );
        let second = NormalizedEvent::new(first.identity.clone(), json!({ "decoder_version": 2 }));

        assert_ne!(first.payload, second.payload);
        assert_eq!(first.event_id(), second.event_id());
    }

    #[test]
    fn transaction_index_distinguishes_transactions_with_same_signature_and_slot() {
        let first = EventIdentity::Transaction {
            cluster: "localnet".to_owned(),
            slot: 42,
            signature: "sig-1".to_owned(),
            index: 0,
        };
        let second = EventIdentity::Transaction {
            cluster: "localnet".to_owned(),
            slot: 42,
            signature: "sig-1".to_owned(),
            index: 1,
        };

        assert_ne!(first.event_id(), second.event_id());
    }

    #[test]
    fn instruction_indexes_distinguish_repeated_program_events_in_one_transaction() {
        let first = EventIdentity::Instruction {
            cluster: "mainnet-beta".to_owned(),
            slot: 42,
            signature: "sig-1".to_owned(),
            transaction_index: 3,
            instruction_index: 0,
            inner_instruction_index: None,
            program_id: "program-1".to_owned(),
        };
        let second = EventIdentity::Instruction {
            cluster: "mainnet-beta".to_owned(),
            slot: 42,
            signature: "sig-1".to_owned(),
            transaction_index: 3,
            instruction_index: 1,
            inner_instruction_index: None,
            program_id: "program-1".to_owned(),
        };
        let inner = EventIdentity::Instruction {
            cluster: "mainnet-beta".to_owned(),
            slot: 42,
            signature: "sig-1".to_owned(),
            transaction_index: 3,
            instruction_index: 0,
            inner_instruction_index: Some(0),
            program_id: "program-1".to_owned(),
        };

        assert_ne!(first.event_id(), second.event_id());
        assert_ne!(first.event_id(), inner.event_id());
    }

    #[test]
    fn account_write_version_distinguishes_multiple_updates_in_one_slot() {
        let first = EventIdentity::Account {
            cluster: "mainnet-beta".to_owned(),
            slot: 42,
            account: "account-1".to_owned(),
            write_version: 100,
            txn_signature: None,
            is_startup: false,
        };
        let second = EventIdentity::Account {
            cluster: "mainnet-beta".to_owned(),
            slot: 42,
            account: "account-1".to_owned(),
            write_version: 101,
            txn_signature: None,
            is_startup: false,
        };

        assert_ne!(first.event_id(), second.event_id());
    }

    #[test]
    fn account_identity_distinguishes_missing_and_empty_transaction_signature() {
        let missing = EventIdentity::Account {
            cluster: "mainnet-beta".to_owned(),
            slot: 42,
            account: "account-1".to_owned(),
            write_version: 100,
            txn_signature: None,
            is_startup: false,
        };
        let empty = EventIdentity::Account {
            cluster: "mainnet-beta".to_owned(),
            slot: 42,
            account: "account-1".to_owned(),
            write_version: 100,
            txn_signature: Some(String::new()),
            is_startup: false,
        };

        assert_ne!(missing.canonical_key(), empty.canonical_key());
    }

    #[test]
    fn slot_status_distinguishes_slot_lifecycle_events() {
        let processed = EventIdentity::Slot {
            cluster: "mainnet-beta".to_owned(),
            slot: 42,
            status: SlotStatus::Processed,
        };
        let finalized = EventIdentity::Slot {
            cluster: "mainnet-beta".to_owned(),
            slot: 42,
            status: SlotStatus::Finalized,
        };

        assert_ne!(processed.event_id(), finalized.event_id());
    }
}
