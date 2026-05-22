pub mod batcher;
pub mod pipeline;
pub mod replay;
pub mod source;

#[cfg(test)]
mod tests {
    use super::batcher::Batcher;
    use serde_json::json;
    use solana_yellowstone_domain::event::{EventType, NormalizedEvent};

    #[test]
    fn batcher_flushes_at_capacity() {
        let mut batcher = Batcher::new(2);
        let event = NormalizedEvent::new(
            1,
            None,
            None,
            None,
            EventType::new(EventType::SLOT).expect("static event type should be valid"),
            json!({}),
        );

        assert!(batcher.push(event.clone()).is_none());
        assert_eq!(batcher.push(event).expect("batch should flush").len(), 2);
    }
}
