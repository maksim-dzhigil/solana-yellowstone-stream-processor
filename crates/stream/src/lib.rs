pub mod batcher;
pub mod pipeline;
pub mod replay;
pub mod source;
pub mod yellowstone;

#[cfg(test)]
mod tests {
    use super::batcher::Batcher;
    use serde_json::json;
    use solana_yellowstone_domain::event::{EventIdentity, NormalizedEvent};

    #[test]
    fn batcher_flushes_at_capacity() {
        let mut batcher = Batcher::new(2);
        let event = NormalizedEvent::new(
            EventIdentity::Slot {
                cluster: "localnet".to_owned(),
                slot: 1,
                status: solana_yellowstone_domain::event::SlotStatus::Processed,
            },
            json!({}),
        );

        assert!(batcher.push(event.clone()).is_none());
        assert_eq!(batcher.push(event).expect("batch should flush").len(), 2);
    }
}
