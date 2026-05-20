pub mod event;

#[cfg(test)]
mod tests {
    use super::event::NormalizedEvent;

    #[test]
    fn event_id_is_stable_for_same_input() {
        let event = NormalizedEvent::new(
            42,
            Some("sig-1".to_owned()),
            Some("program-1".to_owned()),
            None,
            "transaction".to_owned(),
            r#"{"source":"test"}"#.to_owned(),
        );

        assert_eq!(
            event.event_id(),
            "slot=42|signature=sig-1|program=program-1|account=|type=transaction"
        );
    }
}
