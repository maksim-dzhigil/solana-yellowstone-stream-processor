use serde_json::Value;
use solana_yellowstone_domain::event::{
    EventIdentity, EventIdentityError, NormalizedEvent, SlotStatus,
};
use std::fmt;

#[derive(Debug, Clone, PartialEq)]
pub enum YellowstoneEvent {
    Account(YellowstoneAccountUpdate),
    Block(YellowstoneBlock),
    Entry(YellowstoneEntry),
    Instruction(YellowstoneInstruction),
    Slot(YellowstoneSlotUpdate),
    Transaction(YellowstoneTransaction),
}

#[derive(Debug, Clone, PartialEq)]
pub struct YellowstoneTransaction {
    pub slot: u64,
    pub signature: String,
    pub index: u64,
    pub payload: Value,
}

#[derive(Debug, Clone, PartialEq)]
pub struct YellowstoneAccountUpdate {
    pub slot: u64,
    pub account: String,
    pub write_version: u64,
    pub txn_signature: Option<String>,
    pub is_startup: bool,
    pub payload: Value,
}

#[derive(Debug, Clone, PartialEq)]
pub struct YellowstoneInstruction {
    pub slot: u64,
    pub signature: String,
    pub transaction_index: u64,
    pub instruction_index: u16,
    pub inner_instruction_index: Option<u16>,
    pub program_id: String,
    pub payload: Value,
}

#[derive(Debug, Clone, PartialEq)]
pub struct YellowstoneSlotUpdate {
    pub slot: u64,
    pub status: String,
    pub payload: Value,
}

#[derive(Debug, Clone, PartialEq)]
pub struct YellowstoneBlock {
    pub slot: u64,
    pub blockhash: String,
    pub payload: Value,
}

#[derive(Debug, Clone, PartialEq)]
pub struct YellowstoneEntry {
    pub slot: u64,
    pub index: u64,
    pub payload: Value,
}

pub fn normalize_yellowstone_event(
    cluster: &str,
    event: YellowstoneEvent,
) -> Result<NormalizedEvent, YellowstoneNormalizeError> {
    match event {
        YellowstoneEvent::Account(event) => normalize_account_update(cluster, event),
        YellowstoneEvent::Block(event) => normalize_block(cluster, event),
        YellowstoneEvent::Entry(event) => normalize_entry(cluster, event),
        YellowstoneEvent::Instruction(event) => normalize_instruction(cluster, event),
        YellowstoneEvent::Slot(event) => normalize_slot_update(cluster, event),
        YellowstoneEvent::Transaction(event) => normalize_transaction(cluster, event),
    }
}

fn normalize_transaction(
    cluster: &str,
    event: YellowstoneTransaction,
) -> Result<NormalizedEvent, YellowstoneNormalizeError> {
    require_non_empty("signature", &event.signature)?;

    build_event(
        EventIdentity::Transaction {
            cluster: cluster.to_owned(),
            slot: event.slot,
            signature: event.signature,
            index: event.index,
        },
        event.payload,
    )
}

fn normalize_account_update(
    cluster: &str,
    event: YellowstoneAccountUpdate,
) -> Result<NormalizedEvent, YellowstoneNormalizeError> {
    require_non_empty("account", &event.account)?;
    if let Some(signature) = event.txn_signature.as_deref() {
        require_non_empty("txn_signature", signature)?;
    }

    build_event(
        EventIdentity::Account {
            cluster: cluster.to_owned(),
            slot: event.slot,
            account: event.account,
            write_version: event.write_version,
            txn_signature: event.txn_signature,
            is_startup: event.is_startup,
        },
        event.payload,
    )
}

fn normalize_instruction(
    cluster: &str,
    event: YellowstoneInstruction,
) -> Result<NormalizedEvent, YellowstoneNormalizeError> {
    require_non_empty("signature", &event.signature)?;
    require_non_empty("program_id", &event.program_id)?;

    build_event(
        EventIdentity::Instruction {
            cluster: cluster.to_owned(),
            slot: event.slot,
            signature: event.signature,
            transaction_index: event.transaction_index,
            instruction_index: event.instruction_index,
            inner_instruction_index: event.inner_instruction_index,
            program_id: event.program_id,
        },
        event.payload,
    )
}

fn normalize_slot_update(
    cluster: &str,
    event: YellowstoneSlotUpdate,
) -> Result<NormalizedEvent, YellowstoneNormalizeError> {
    let status = parse_slot_status(&event.status)?;

    build_event(
        EventIdentity::Slot {
            cluster: cluster.to_owned(),
            slot: event.slot,
            status,
        },
        event.payload,
    )
}

fn normalize_block(
    cluster: &str,
    event: YellowstoneBlock,
) -> Result<NormalizedEvent, YellowstoneNormalizeError> {
    require_non_empty("blockhash", &event.blockhash)?;

    build_event(
        EventIdentity::Block {
            cluster: cluster.to_owned(),
            slot: event.slot,
            blockhash: event.blockhash,
        },
        event.payload,
    )
}

fn normalize_entry(
    cluster: &str,
    event: YellowstoneEntry,
) -> Result<NormalizedEvent, YellowstoneNormalizeError> {
    build_event(
        EventIdentity::Entry {
            cluster: cluster.to_owned(),
            slot: event.slot,
            index: event.index,
        },
        event.payload,
    )
}

fn build_event(
    identity: EventIdentity,
    payload: Value,
) -> Result<NormalizedEvent, YellowstoneNormalizeError> {
    let event = NormalizedEvent::new(identity, payload);
    event
        .validate()
        .map_err(YellowstoneNormalizeError::InvalidIdentity)?;

    Ok(event)
}

fn parse_slot_status(status: &str) -> Result<SlotStatus, YellowstoneNormalizeError> {
    match status {
        "processed" => Ok(SlotStatus::Processed),
        "confirmed" => Ok(SlotStatus::Confirmed),
        "finalized" => Ok(SlotStatus::Finalized),
        "dead" => Ok(SlotStatus::Dead),
        value => Err(YellowstoneNormalizeError::UnsupportedSlotStatus {
            status: value.to_owned(),
        }),
    }
}

fn require_non_empty(field: &'static str, value: &str) -> Result<(), YellowstoneNormalizeError> {
    if value.trim().is_empty() {
        return Err(YellowstoneNormalizeError::MissingField { field });
    }

    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum YellowstoneNormalizeError {
    MissingField { field: &'static str },
    UnsupportedSlotStatus { status: String },
    InvalidIdentity(EventIdentityError),
}

impl fmt::Display for YellowstoneNormalizeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingField { field } => write!(f, "yellowstone field {field} is required"),
            Self::UnsupportedSlotStatus { status } => {
                write!(f, "unsupported yellowstone slot status {status}")
            }
            Self::InvalidIdentity(err) => write!(f, "invalid normalized event identity: {err}"),
        }
    }
}

impl std::error::Error for YellowstoneNormalizeError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::InvalidIdentity(err) => Some(err),
            Self::MissingField { .. } | Self::UnsupportedSlotStatus { .. } => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        YellowstoneAccountUpdate, YellowstoneBlock, YellowstoneEntry, YellowstoneEvent,
        YellowstoneInstruction, YellowstoneNormalizeError, YellowstoneSlotUpdate,
        YellowstoneTransaction, normalize_yellowstone_event,
    };
    use serde_json::json;
    use solana_yellowstone_domain::event::{EventIdentity, EventKind, SlotStatus};

    #[test]
    fn normalizes_transaction_identity() {
        let event = normalize_yellowstone_event(
            "mainnet-beta",
            YellowstoneEvent::Transaction(YellowstoneTransaction {
                slot: 42,
                signature: "sig-1".to_owned(),
                index: 7,
                payload: json!({ "raw": "transaction" }),
            }),
        )
        .expect("transaction should normalize");

        assert_eq!(event.kind(), EventKind::Transaction);
        assert_eq!(event.slot(), 42);
        assert_eq!(event.signature(), Some("sig-1"));
        assert_eq!(event.payload, json!({ "raw": "transaction" }));
        assert_eq!(
            event.identity,
            EventIdentity::Transaction {
                cluster: "mainnet-beta".to_owned(),
                slot: 42,
                signature: "sig-1".to_owned(),
                index: 7,
            }
        );
    }

    #[test]
    fn account_write_version_affects_event_id() {
        let first = normalize_yellowstone_event(
            "mainnet-beta",
            YellowstoneEvent::Account(YellowstoneAccountUpdate {
                slot: 42,
                account: "account-1".to_owned(),
                write_version: 100,
                txn_signature: Some("sig-1".to_owned()),
                is_startup: false,
                payload: json!({}),
            }),
        )
        .expect("first account update should normalize");
        let second = normalize_yellowstone_event(
            "mainnet-beta",
            YellowstoneEvent::Account(YellowstoneAccountUpdate {
                write_version: 101,
                ..account_update()
            }),
        )
        .expect("second account update should normalize");

        assert_eq!(first.kind(), EventKind::Account);
        assert_eq!(first.account(), Some("account-1"));
        assert_ne!(first.event_id(), second.event_id());
    }

    #[test]
    fn instruction_indexes_affect_event_id() {
        let outer = normalize_yellowstone_event(
            "mainnet-beta",
            YellowstoneEvent::Instruction(YellowstoneInstruction {
                slot: 42,
                signature: "sig-1".to_owned(),
                transaction_index: 3,
                instruction_index: 0,
                inner_instruction_index: None,
                program_id: "program-1".to_owned(),
                payload: json!({}),
            }),
        )
        .expect("outer instruction should normalize");
        let inner = normalize_yellowstone_event(
            "mainnet-beta",
            YellowstoneEvent::Instruction(YellowstoneInstruction {
                inner_instruction_index: Some(0),
                ..instruction()
            }),
        )
        .expect("inner instruction should normalize");

        assert_eq!(outer.kind(), EventKind::Instruction);
        assert_eq!(outer.program_id(), Some("program-1"));
        assert_ne!(outer.event_id(), inner.event_id());
    }

    #[test]
    fn normalizes_slot_status() {
        let event = normalize_yellowstone_event(
            "mainnet-beta",
            YellowstoneEvent::Slot(YellowstoneSlotUpdate {
                slot: 42,
                status: "finalized".to_owned(),
                payload: json!({ "raw": "slot" }),
            }),
        )
        .expect("slot should normalize");

        assert_eq!(
            event.identity,
            EventIdentity::Slot {
                cluster: "mainnet-beta".to_owned(),
                slot: 42,
                status: SlotStatus::Finalized,
            }
        );
    }

    #[test]
    fn normalizes_block_identity() {
        let event = normalize_yellowstone_event(
            "mainnet-beta",
            YellowstoneEvent::Block(YellowstoneBlock {
                slot: 42,
                blockhash: "blockhash-1".to_owned(),
                payload: json!({}),
            }),
        )
        .expect("block should normalize");

        assert_eq!(event.kind(), EventKind::Block);
        assert_eq!(event.slot(), 42);
    }

    #[test]
    fn normalizes_entry_identity() {
        let first = normalize_yellowstone_event(
            "mainnet-beta",
            YellowstoneEvent::Entry(YellowstoneEntry {
                slot: 42,
                index: 0,
                payload: json!({}),
            }),
        )
        .expect("first entry should normalize");
        let second = normalize_yellowstone_event(
            "mainnet-beta",
            YellowstoneEvent::Entry(YellowstoneEntry {
                slot: 42,
                index: 1,
                payload: json!({}),
            }),
        )
        .expect("second entry should normalize");

        assert_eq!(first.kind(), EventKind::Entry);
        assert_ne!(first.event_id(), second.event_id());
    }

    #[test]
    fn rejects_missing_required_fields() {
        let err = normalize_yellowstone_event(
            "mainnet-beta",
            YellowstoneEvent::Transaction(YellowstoneTransaction {
                signature: " ".to_owned(),
                ..transaction()
            }),
        )
        .expect_err("missing signature should fail");

        assert_eq!(
            err,
            YellowstoneNormalizeError::MissingField { field: "signature" }
        );
    }

    #[test]
    fn rejects_unsupported_slot_status() {
        let err = normalize_yellowstone_event(
            "mainnet-beta",
            YellowstoneEvent::Slot(YellowstoneSlotUpdate {
                slot: 42,
                status: "rooted".to_owned(),
                payload: json!({}),
            }),
        )
        .expect_err("unsupported status should fail");

        assert_eq!(
            err,
            YellowstoneNormalizeError::UnsupportedSlotStatus {
                status: "rooted".to_owned(),
            }
        );
    }

    #[test]
    fn rejects_empty_cluster_through_identity_validation() {
        let err = normalize_yellowstone_event(" ", YellowstoneEvent::Transaction(transaction()))
            .expect_err("empty cluster should fail");

        assert!(matches!(err, YellowstoneNormalizeError::InvalidIdentity(_)));
    }

    fn transaction() -> YellowstoneTransaction {
        YellowstoneTransaction {
            slot: 42,
            signature: "sig-1".to_owned(),
            index: 7,
            payload: json!({}),
        }
    }

    fn account_update() -> YellowstoneAccountUpdate {
        YellowstoneAccountUpdate {
            slot: 42,
            account: "account-1".to_owned(),
            write_version: 100,
            txn_signature: Some("sig-1".to_owned()),
            is_startup: false,
            payload: json!({}),
        }
    }

    fn instruction() -> YellowstoneInstruction {
        YellowstoneInstruction {
            slot: 42,
            signature: "sig-1".to_owned(),
            transaction_index: 3,
            instruction_index: 0,
            inner_instruction_index: None,
            program_id: "program-1".to_owned(),
            payload: json!({}),
        }
    }
}
