
use serde_json::Value;
use solana_yellowstone_domain::event::{EventIdentity, NormalizedEvent, SlotStatus};
use solana_yellowstone_storage::slots::SlotStateUpdate;

pub fn slot_state_from_event(event: &NormalizedEvent) -> Option<SlotStateUpdate> {
    let EventIdentity::Slot { slot, status, .. } = &event.identity else {
        return None;
    };

    let (finalized, dead) = match status {
        SlotStatus::Finalized => (true, false),
        SlotStatus::Dead => (false, true),
        SlotStatus::Processed | SlotStatus::Confirmed => return None,
    };

    Some(SlotStateUpdate {
        slot: *slot,
        parent_slot: event.payload.get("parent").and_then(Value::as_u64),
        finalized,
        dead,
    })
}

#[cfg(test)]
mod tests {
    use super::slot_state_from_event;
    use serde_json::json;
    use solana_yellowstone_domain::event::{EventIdentity, NormalizedEvent, SlotStatus};

    fn slot_event(status: SlotStatus, payload: serde_json::Value) -> NormalizedEvent {
        NormalizedEvent::new(
            EventIdentity::Slot {
                cluster: "mainnet-beta".to_owned(),
                slot: 100,
                status,
            },
            payload,
        )
    }

    #[test]
    fn maps_finalized_slot_with_parent() {
        let event = slot_event(SlotStatus::Finalized, json!({ "parent": 98 }));

        let update = slot_state_from_event(&event).expect("finalized slot maps to an update");

        assert_eq!(update.slot, 100);
        assert_eq!(update.parent_slot, Some(98));
        assert!(update.finalized);
        assert!(!update.dead);
    }

    #[test]
    fn maps_dead_slot() {
        let event = slot_event(SlotStatus::Dead, json!({ "parent": 99 }));

        let update = slot_state_from_event(&event).expect("dead slot maps to an update");

        assert!(!update.finalized);
        assert!(update.dead);
        assert_eq!(update.parent_slot, Some(99));
    }

    #[test]
    fn finalized_slot_without_parent_is_allowed() {
        let event = slot_event(SlotStatus::Finalized, json!({ "parent": null }));

        let update = slot_state_from_event(&event).expect("finalized slot maps to an update");

        assert_eq!(update.parent_slot, None);
    }

    #[test]
    fn ignores_processed_and_confirmed_slots() {
        assert!(slot_state_from_event(&slot_event(SlotStatus::Processed, json!({}))).is_none());
        assert!(slot_state_from_event(&slot_event(SlotStatus::Confirmed, json!({}))).is_none());
    }

    #[test]
    fn ignores_non_slot_events() {
        let transaction = NormalizedEvent::new(
            EventIdentity::Transaction {
                cluster: "mainnet-beta".to_owned(),
                slot: 100,
                signature: "sig-1".to_owned(),
                index: 0,
            },
            json!({ "parent": 99 }),
        );

        assert!(slot_state_from_event(&transaction).is_none());
    }
}
