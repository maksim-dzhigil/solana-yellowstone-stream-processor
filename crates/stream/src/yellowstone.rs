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

#[cfg(feature = "yellowstone-proto")]
pub mod proto {
    use super::{
        YellowstoneAccountUpdate, YellowstoneBlock, YellowstoneEntry, YellowstoneEvent,
        YellowstoneNormalizeError, YellowstoneSlotUpdate, YellowstoneTransaction,
        normalize_yellowstone_event,
    };
    use serde_json::json;
    use solana_yellowstone_domain::event::NormalizedEvent;
    use std::fmt;
    use yellowstone_grpc_proto::geyser::{
        SlotStatus as ProtoSlotStatus, SubscribeUpdate, SubscribeUpdateAccount,
        SubscribeUpdateBlock, SubscribeUpdateEntry, SubscribeUpdateSlot,
        SubscribeUpdateTransaction, SubscribeUpdateTransactionInfo, subscribe_update,
    };
    use yellowstone_grpc_proto::solana::storage::confirmed_block::{
        CompiledInstruction, InnerInstruction, TransactionStatusMeta,
    };

    pub fn normalize_yellowstone_proto_update(
        cluster: &str,
        update: SubscribeUpdate,
    ) -> Result<Vec<NormalizedEvent>, YellowstoneProtoNormalizeError> {
        let filters = update.filters;
        match update
            .update_oneof
            .ok_or(YellowstoneProtoNormalizeError::MissingUpdate)?
        {
            subscribe_update::UpdateOneof::Account(account) => {
                normalize_proto_account(cluster, filters, account).map(|event| vec![event])
            }
            subscribe_update::UpdateOneof::Slot(slot) => {
                normalize_proto_slot(cluster, filters, slot).map(|event| vec![event])
            }
            subscribe_update::UpdateOneof::Transaction(transaction) => {
                normalize_proto_transaction(cluster, filters, transaction)
            }
            subscribe_update::UpdateOneof::Block(block) => {
                normalize_proto_block(cluster, filters, block).map(|event| vec![event])
            }
            subscribe_update::UpdateOneof::Entry(entry) => {
                normalize_proto_entry(cluster, filters, entry).map(|event| vec![event])
            }
            subscribe_update::UpdateOneof::Ping(_)
            | subscribe_update::UpdateOneof::Pong(_)
            | subscribe_update::UpdateOneof::BlockMeta(_)
            | subscribe_update::UpdateOneof::TransactionStatus(_) => Ok(Vec::new()),
        }
    }

    fn normalize_proto_account(
        cluster: &str,
        filters: Vec<String>,
        update: SubscribeUpdateAccount,
    ) -> Result<NormalizedEvent, YellowstoneProtoNormalizeError> {
        let account = update
            .account
            .ok_or(YellowstoneProtoNormalizeError::MissingNestedField {
                field: "account.account",
            })?;
        let pubkey = encode_pubkey("account.pubkey", &account.pubkey)?;
        let owner = encode_optional_pubkey("account.owner", &account.owner)?;
        let txn_signature = account
            .txn_signature
            .as_ref()
            .map(|signature| encode_signature("account.txn_signature", signature))
            .transpose()?;

        normalize_yellowstone_event(
            cluster,
            YellowstoneEvent::Account(YellowstoneAccountUpdate {
                slot: update.slot,
                account: pubkey,
                write_version: account.write_version,
                txn_signature,
                is_startup: update.is_startup,
                payload: json!({
                    "source": "yellowstone_proto",
                    "filters": filters,
                    "lamports": account.lamports,
                    "owner": owner,
                    "executable": account.executable,
                    "rent_epoch": account.rent_epoch,
                    "data_len": account.data.len(),
                }),
            }),
        )
        .map_err(YellowstoneProtoNormalizeError::Normalize)
    }

    fn normalize_proto_slot(
        cluster: &str,
        filters: Vec<String>,
        update: SubscribeUpdateSlot,
    ) -> Result<NormalizedEvent, YellowstoneProtoNormalizeError> {
        let status = proto_slot_status(update.status)?;
        normalize_yellowstone_event(
            cluster,
            YellowstoneEvent::Slot(YellowstoneSlotUpdate {
                slot: update.slot,
                status: status.to_owned(),
                payload: json!({
                    "source": "yellowstone_proto",
                    "filters": filters,
                    "parent": update.parent,
                    "dead_error": update.dead_error,
                }),
            }),
        )
        .map_err(YellowstoneProtoNormalizeError::Normalize)
    }

    fn normalize_proto_transaction(
        cluster: &str,
        filters: Vec<String>,
        update: SubscribeUpdateTransaction,
    ) -> Result<Vec<NormalizedEvent>, YellowstoneProtoNormalizeError> {
        let info =
            update
                .transaction
                .ok_or(YellowstoneProtoNormalizeError::MissingNestedField {
                    field: "transaction.transaction",
                })?;
        let signature = encode_signature("transaction.signature", &info.signature)?;
        let mut events = Vec::new();
        events.push(
            normalize_yellowstone_event(
                cluster,
                YellowstoneEvent::Transaction(YellowstoneTransaction {
                    slot: update.slot,
                    signature: signature.clone(),
                    index: info.index,
                    payload: json!({
                        "source": "yellowstone_proto",
                        "filters": filters,
                        "is_vote": info.is_vote,
                    }),
                }),
            )
            .map_err(YellowstoneProtoNormalizeError::Normalize)?,
        );

        events.extend(normalize_proto_instructions(
            cluster,
            update.slot,
            &signature,
            &info,
        )?);
        Ok(events)
    }

    struct InstructionNormalizeContext<'a> {
        cluster: &'a str,
        slot: u64,
        signature: &'a str,
        transaction_index: u64,
        account_keys: Vec<String>,
    }

    fn normalize_proto_instructions(
        cluster: &str,
        slot: u64,
        signature: &str,
        info: &SubscribeUpdateTransactionInfo,
    ) -> Result<Vec<NormalizedEvent>, YellowstoneProtoNormalizeError> {
        let Some(transaction) = info.transaction.as_ref() else {
            return Ok(Vec::new());
        };
        let Some(message) = transaction.message.as_ref() else {
            return Ok(Vec::new());
        };
        let context = InstructionNormalizeContext {
            cluster,
            slot,
            signature,
            transaction_index: info.index,
            account_keys: instruction_account_keys(info),
        };
        let mut events = Vec::new();

        for (index, instruction) in message.instructions.iter().enumerate() {
            events.push(normalize_compiled_instruction(
                &context,
                checked_u16("instruction_index", index)?,
                None,
                instruction,
            )?);
        }

        if let Some(meta) = info.meta.as_ref() {
            for inner_group in &meta.inner_instructions {
                let instruction_index =
                    checked_u16("instruction_index", inner_group.index as usize)?;
                for (inner_index, instruction) in inner_group.instructions.iter().enumerate() {
                    events.push(normalize_inner_instruction(
                        &context,
                        instruction_index,
                        checked_u16("inner_instruction_index", inner_index)?,
                        instruction,
                    )?);
                }
            }
        }

        Ok(events)
    }

    fn normalize_compiled_instruction(
        context: &InstructionNormalizeContext<'_>,
        instruction_index: u16,
        inner_instruction_index: Option<u16>,
        instruction: &CompiledInstruction,
    ) -> Result<NormalizedEvent, YellowstoneProtoNormalizeError> {
        let program_id = program_id_at(instruction.program_id_index, &context.account_keys)?;
        normalize_yellowstone_event(
            context.cluster,
            YellowstoneEvent::Instruction(super::YellowstoneInstruction {
                slot: context.slot,
                signature: context.signature.to_owned(),
                transaction_index: context.transaction_index,
                instruction_index,
                inner_instruction_index,
                program_id: program_id.clone(),
                payload: json!({
                    "source": "yellowstone_proto",
                    "program_id_index": instruction.program_id_index,
                    "program_id": program_id,
                    "accounts": instruction.accounts,
                    "data": bs58::encode(&instruction.data).into_string(),
                }),
            }),
        )
        .map_err(YellowstoneProtoNormalizeError::Normalize)
    }

    fn normalize_inner_instruction(
        context: &InstructionNormalizeContext<'_>,
        instruction_index: u16,
        inner_instruction_index: u16,
        instruction: &InnerInstruction,
    ) -> Result<NormalizedEvent, YellowstoneProtoNormalizeError> {
        let compiled = CompiledInstruction {
            program_id_index: instruction.program_id_index,
            accounts: instruction.accounts.clone(),
            data: instruction.data.clone(),
        };
        normalize_compiled_instruction(
            context,
            instruction_index,
            Some(inner_instruction_index),
            &compiled,
        )
    }

    fn normalize_proto_block(
        cluster: &str,
        filters: Vec<String>,
        update: SubscribeUpdateBlock,
    ) -> Result<NormalizedEvent, YellowstoneProtoNormalizeError> {
        normalize_yellowstone_event(
            cluster,
            YellowstoneEvent::Block(YellowstoneBlock {
                slot: update.slot,
                blockhash: update.blockhash.clone(),
                payload: json!({
                    "source": "yellowstone_proto",
                    "filters": filters,
                    "parent_slot": update.parent_slot,
                    "parent_blockhash": update.parent_blockhash,
                    "executed_transaction_count": update.executed_transaction_count,
                    "updated_account_count": update.updated_account_count,
                    "entries_count": update.entries_count,
                }),
            }),
        )
        .map_err(YellowstoneProtoNormalizeError::Normalize)
    }

    fn normalize_proto_entry(
        cluster: &str,
        filters: Vec<String>,
        update: SubscribeUpdateEntry,
    ) -> Result<NormalizedEvent, YellowstoneProtoNormalizeError> {
        normalize_yellowstone_event(
            cluster,
            YellowstoneEvent::Entry(YellowstoneEntry {
                slot: update.slot,
                index: update.index,
                payload: json!({
                    "source": "yellowstone_proto",
                    "filters": filters,
                    "num_hashes": update.num_hashes,
                    "hash": bs58::encode(&update.hash).into_string(),
                    "executed_transaction_count": update.executed_transaction_count,
                    "starting_transaction_index": update.starting_transaction_index,
                }),
            }),
        )
        .map_err(YellowstoneProtoNormalizeError::Normalize)
    }

    fn instruction_account_keys(info: &SubscribeUpdateTransactionInfo) -> Vec<String> {
        let mut keys = info
            .transaction
            .as_ref()
            .and_then(|transaction| transaction.message.as_ref())
            .map(|message| {
                message
                    .account_keys
                    .iter()
                    .map(|key| bs58::encode(key).into_string())
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();

        if let Some(meta) = info.meta.as_ref() {
            append_loaded_addresses(&mut keys, meta);
        }

        keys
    }

    fn append_loaded_addresses(keys: &mut Vec<String>, meta: &TransactionStatusMeta) {
        keys.extend(
            meta.loaded_writable_addresses
                .iter()
                .map(|key| bs58::encode(key).into_string()),
        );
        keys.extend(
            meta.loaded_readonly_addresses
                .iter()
                .map(|key| bs58::encode(key).into_string()),
        );
    }

    fn program_id_at(
        index: u32,
        account_keys: &[String],
    ) -> Result<String, YellowstoneProtoNormalizeError> {
        account_keys.get(index as usize).cloned().ok_or(
            YellowstoneProtoNormalizeError::ProgramIdIndexOutOfRange {
                index,
                account_keys_len: account_keys.len(),
            },
        )
    }

    fn proto_slot_status(status: i32) -> Result<&'static str, YellowstoneProtoNormalizeError> {
        match ProtoSlotStatus::try_from(status) {
            Ok(ProtoSlotStatus::SlotProcessed) => Ok("processed"),
            Ok(ProtoSlotStatus::SlotConfirmed) => Ok("confirmed"),
            Ok(ProtoSlotStatus::SlotFinalized) => Ok("finalized"),
            Ok(ProtoSlotStatus::SlotDead) => Ok("dead"),
            Ok(other) => Err(YellowstoneProtoNormalizeError::UnsupportedProtoSlotStatus {
                status: other.as_str_name().to_owned(),
            }),
            Err(_) => Err(YellowstoneProtoNormalizeError::UnknownProtoSlotStatus { status }),
        }
    }

    fn encode_signature(
        field: &'static str,
        value: &[u8],
    ) -> Result<String, YellowstoneProtoNormalizeError> {
        encode_non_empty_bytes(field, value)
    }

    fn encode_pubkey(
        field: &'static str,
        value: &[u8],
    ) -> Result<String, YellowstoneProtoNormalizeError> {
        encode_non_empty_bytes(field, value)
    }

    fn encode_optional_pubkey(
        field: &'static str,
        value: &[u8],
    ) -> Result<Option<String>, YellowstoneProtoNormalizeError> {
        if value.is_empty() {
            return Ok(None);
        }

        encode_pubkey(field, value).map(Some)
    }

    fn encode_non_empty_bytes(
        field: &'static str,
        value: &[u8],
    ) -> Result<String, YellowstoneProtoNormalizeError> {
        if value.is_empty() {
            return Err(YellowstoneProtoNormalizeError::MissingBytes { field });
        }

        Ok(bs58::encode(value).into_string())
    }

    fn checked_u16(
        field: &'static str,
        value: usize,
    ) -> Result<u16, YellowstoneProtoNormalizeError> {
        u16::try_from(value)
            .map_err(|_| YellowstoneProtoNormalizeError::IndexOutOfRange { field, value })
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    pub enum YellowstoneProtoNormalizeError {
        MissingUpdate,
        MissingNestedField { field: &'static str },
        MissingBytes { field: &'static str },
        IndexOutOfRange { field: &'static str, value: usize },
        ProgramIdIndexOutOfRange { index: u32, account_keys_len: usize },
        UnknownProtoSlotStatus { status: i32 },
        UnsupportedProtoSlotStatus { status: String },
        Normalize(YellowstoneNormalizeError),
    }

    impl fmt::Display for YellowstoneProtoNormalizeError {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            match self {
                Self::MissingUpdate => write!(f, "yellowstone update is missing update_oneof"),
                Self::MissingNestedField { field } => {
                    write!(f, "yellowstone nested field {field} is required")
                }
                Self::MissingBytes { field } => {
                    write!(f, "yellowstone byte field {field} is required")
                }
                Self::IndexOutOfRange { field, value } => {
                    write!(
                        f,
                        "yellowstone index field {field} value {value} does not fit u16"
                    )
                }
                Self::ProgramIdIndexOutOfRange {
                    index,
                    account_keys_len,
                } => write!(
                    f,
                    "yellowstone program_id_index {index} is outside account key len {account_keys_len}"
                ),
                Self::UnknownProtoSlotStatus { status } => {
                    write!(f, "unknown yellowstone proto slot status {status}")
                }
                Self::UnsupportedProtoSlotStatus { status } => {
                    write!(f, "unsupported yellowstone proto slot status {status}")
                }
                Self::Normalize(err) => write!(f, "yellowstone normalize failed: {err}"),
            }
        }
    }

    impl std::error::Error for YellowstoneProtoNormalizeError {
        fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
            match self {
                Self::Normalize(err) => Some(err),
                Self::MissingUpdate
                | Self::MissingNestedField { .. }
                | Self::MissingBytes { .. }
                | Self::IndexOutOfRange { .. }
                | Self::ProgramIdIndexOutOfRange { .. }
                | Self::UnknownProtoSlotStatus { .. }
                | Self::UnsupportedProtoSlotStatus { .. } => None,
            }
        }
    }
}

#[cfg(all(test, feature = "yellowstone-proto"))]
mod proto_tests {
    use super::proto::{YellowstoneProtoNormalizeError, normalize_yellowstone_proto_update};
    use solana_yellowstone_domain::event::{EventIdentity, EventKind};
    use yellowstone_grpc_proto::geyser::{
        SlotStatus as ProtoSlotStatus, SubscribeUpdate, SubscribeUpdateAccount,
        SubscribeUpdateAccountInfo, SubscribeUpdateSlot, SubscribeUpdateTransaction,
        SubscribeUpdateTransactionInfo, subscribe_update,
    };
    use yellowstone_grpc_proto::solana::storage::confirmed_block::{
        CompiledInstruction, InnerInstruction, InnerInstructions, Message, Transaction,
        TransactionStatusMeta,
    };

    #[test]
    fn proto_account_update_maps_to_account_identity() {
        let pubkey = bytes(1, 32);
        let signature = bytes(2, 64);
        let update = SubscribeUpdate {
            filters: vec!["accounts".to_owned()],
            update_oneof: Some(subscribe_update::UpdateOneof::Account(
                SubscribeUpdateAccount {
                    account: Some(SubscribeUpdateAccountInfo {
                        pubkey: pubkey.clone(),
                        lamports: 10,
                        owner: bytes(3, 32),
                        executable: false,
                        rent_epoch: 1,
                        data: vec![1, 2, 3],
                        write_version: 99,
                        txn_signature: Some(signature.clone()),
                    }),
                    slot: 42,
                    is_startup: false,
                },
            )),
            created_at: None,
        };

        let events = normalize_yellowstone_proto_update("mainnet-beta", update)
            .expect("account update should normalize");

        assert_eq!(events.len(), 1);
        assert_eq!(events[0].kind(), EventKind::Account);
        assert_eq!(
            events[0].account(),
            Some(bs58::encode(pubkey).into_string().as_str())
        );
        assert_eq!(
            events[0].signature(),
            Some(bs58::encode(signature).into_string().as_str())
        );
    }

    #[test]
    fn proto_transaction_maps_transaction_and_instruction_events() {
        let signature = bytes(9, 64);
        let program_id = bytes(7, 32);
        let update = SubscribeUpdate {
            filters: vec!["transactions".to_owned()],
            update_oneof: Some(subscribe_update::UpdateOneof::Transaction(
                SubscribeUpdateTransaction {
                    transaction: Some(SubscribeUpdateTransactionInfo {
                        signature: signature.clone(),
                        is_vote: false,
                        transaction: Some(Transaction {
                            message: Some(Message {
                                account_keys: vec![program_id.clone()],
                                instructions: vec![CompiledInstruction {
                                    program_id_index: 0,
                                    accounts: vec![0],
                                    data: vec![1, 2],
                                }],
                                ..Default::default()
                            }),
                            ..Default::default()
                        }),
                        meta: Some(TransactionStatusMeta {
                            inner_instructions: vec![InnerInstructions {
                                index: 0,
                                instructions: vec![InnerInstruction {
                                    program_id_index: 0,
                                    accounts: vec![0],
                                    data: vec![3, 4],
                                    stack_height: Some(2),
                                }],
                            }],
                            ..Default::default()
                        }),
                        index: 5,
                    }),
                    slot: 42,
                },
            )),
            created_at: None,
        };

        let events = normalize_yellowstone_proto_update("mainnet-beta", update)
            .expect("transaction should normalize");

        assert_eq!(events.len(), 3);
        assert_eq!(events[0].kind(), EventKind::Transaction);
        assert_eq!(events[1].kind(), EventKind::Instruction);
        assert_eq!(events[2].kind(), EventKind::Instruction);
        assert_ne!(events[1].event_id(), events[2].event_id());
        assert_eq!(
            events[1].program_id(),
            Some(bs58::encode(program_id).into_string().as_str())
        );
        assert!(matches!(
            &events[2].identity,
            EventIdentity::Instruction {
                inner_instruction_index: Some(0),
                ..
            }
        ));
    }

    #[test]
    fn proto_slot_rejects_unsupported_status() {
        let update = SubscribeUpdate {
            filters: Vec::new(),
            update_oneof: Some(subscribe_update::UpdateOneof::Slot(SubscribeUpdateSlot {
                slot: 42,
                parent: None,
                status: ProtoSlotStatus::SlotFirstShredReceived as i32,
                dead_error: None,
            })),
            created_at: None,
        };

        let err = normalize_yellowstone_proto_update("mainnet-beta", update)
            .expect_err("unsupported slot status should fail");

        assert_eq!(
            err,
            YellowstoneProtoNormalizeError::UnsupportedProtoSlotStatus {
                status: "SLOT_FIRST_SHRED_RECEIVED".to_owned(),
            }
        );
    }

    #[test]
    fn proto_transaction_rejects_invalid_program_id_index() {
        let update = SubscribeUpdate {
            filters: Vec::new(),
            update_oneof: Some(subscribe_update::UpdateOneof::Transaction(
                SubscribeUpdateTransaction {
                    transaction: Some(SubscribeUpdateTransactionInfo {
                        signature: bytes(9, 64),
                        transaction: Some(Transaction {
                            message: Some(Message {
                                account_keys: vec![bytes(7, 32)],
                                instructions: vec![CompiledInstruction {
                                    program_id_index: 9,
                                    accounts: Vec::new(),
                                    data: Vec::new(),
                                }],
                                ..Default::default()
                            }),
                            ..Default::default()
                        }),
                        index: 0,
                        ..Default::default()
                    }),
                    slot: 42,
                },
            )),
            created_at: None,
        };

        let err = normalize_yellowstone_proto_update("mainnet-beta", update)
            .expect_err("bad program_id_index should fail");

        assert_eq!(
            err,
            YellowstoneProtoNormalizeError::ProgramIdIndexOutOfRange {
                index: 9,
                account_keys_len: 1,
            }
        );
    }

    fn bytes(value: u8, len: usize) -> Vec<u8> {
        vec![value; len]
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
