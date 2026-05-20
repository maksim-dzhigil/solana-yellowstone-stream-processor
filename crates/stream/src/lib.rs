pub mod batcher;
pub mod pipeline;
pub mod replay;

#[cfg(test)]
mod tests {
    use super::batcher::Batcher;
    use solana_yellowstone_domain::event::NormalizedEvent;

    #[test]
    fn batcher_flushes_at_capacity() {
        let mut batcher = Batcher::new(2);
        let event = NormalizedEvent::new(1, None, None, None, "slot".to_owned(), "{}".to_owned());

        assert!(batcher.push(event.clone()).is_none());
        assert_eq!(batcher.push(event).expect("batch should flush").len(), 2);
    }
}
