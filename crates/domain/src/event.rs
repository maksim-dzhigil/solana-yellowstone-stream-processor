use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::Value;
use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct EventType(String);

impl EventType {
    pub const SLOT: &'static str = "slot";
    pub const TRANSACTION: &'static str = "transaction";
    pub const ACCOUNT: &'static str = "account";

    pub fn new(value: impl Into<String>) -> Result<Self, EventTypeError> {
        let value = value.into();

        if value.trim().is_empty() {
            return Err(EventTypeError::Empty);
        }

        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for EventType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl TryFrom<String> for EventType {
    type Error = EventTypeError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl TryFrom<&str> for EventType {
    type Error = EventTypeError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl Serialize for EventType {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for EventType {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::new(value).map_err(serde::de::Error::custom)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EventTypeError {
    Empty,
}

impl fmt::Display for EventTypeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Empty => write!(f, "event_type must not be empty"),
        }
    }
}

impl std::error::Error for EventTypeError {}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NormalizedEvent {
    pub slot: u64,
    pub signature: Option<String>,
    pub program_id: Option<String>,
    pub account: Option<String>,
    pub event_type: EventType,
    pub payload: Value,
}

impl NormalizedEvent {
    pub fn new(
        slot: u64,
        signature: Option<String>,
        program_id: Option<String>,
        account: Option<String>,
        event_type: EventType,
        payload: Value,
    ) -> Self {
        Self {
            slot,
            signature,
            program_id,
            account,
            event_type,
            payload,
        }
    }

    pub fn event_id(&self) -> String {
        format!(
            "slot={}|signature={}|program={}|account={}|type={}",
            self.slot,
            self.signature.as_deref().unwrap_or_default(),
            self.program_id.as_deref().unwrap_or_default(),
            self.account.as_deref().unwrap_or_default(),
            self.event_type
        )
    }
}

#[derive(Debug)]
pub enum NormalizedEventParseError {
    EmptyLine,
    InvalidJson(serde_json::Error),
}

impl fmt::Display for NormalizedEventParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyLine => write!(f, "event line must not be empty"),
            Self::InvalidJson(err) => write!(f, "invalid event json: {err}"),
        }
    }
}

impl std::error::Error for NormalizedEventParseError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::InvalidJson(err) => Some(err),
            Self::EmptyLine => None,
        }
    }
}

pub fn parse_normalized_event(line: &str) -> Result<NormalizedEvent, NormalizedEventParseError> {
    if line.trim().is_empty() {
        return Err(NormalizedEventParseError::EmptyLine);
    }

    serde_json::from_str(line).map_err(NormalizedEventParseError::InvalidJson)
}

#[cfg(test)]
mod tests {
    use super::{
        EventType, EventTypeError, NormalizedEvent, NormalizedEventParseError,
        parse_normalized_event,
    };
    use serde_json::json;

    #[test]
    fn creates_event_type_from_non_empty_value() {
        let event_type = EventType::new("custom.event").expect("event type should be valid");

        assert_eq!(event_type.as_str(), "custom.event");
    }

    #[test]
    fn rejects_empty_event_type_newtype() {
        let err = EventType::new(" ").expect_err("empty event type should fail");

        assert_eq!(err, EventTypeError::Empty);
    }

    #[test]
    fn parses_valid_json_line() {
        let line = r#"{"slot":1,"signature":"sig-1","program_id":"program-1","account":null,"event_type":"transaction","payload":{"source":"fixture"}}"#;

        let event = parse_normalized_event(line).expect("event should parse");

        assert_eq!(event.slot, 1);
        assert_eq!(event.signature.as_deref(), Some("sig-1"));
        assert_eq!(event.program_id.as_deref(), Some("program-1"));
        assert_eq!(event.account, None);
        assert_eq!(event.event_type.as_str(), EventType::TRANSACTION);
        assert_eq!(event.payload, json!({ "source": "fixture" }));
    }

    #[test]
    fn duplicate_lines_have_same_event_id() {
        let line = r#"{"slot":2,"signature":"sig-2","program_id":"program-1","account":null,"event_type":"transaction","payload":{"index":2}}"#;

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
        let err = parse_normalized_event(r#"{"slot":1,"payload":{}}"#)
            .expect_err("missing event_type should fail");

        assert!(matches!(err, NormalizedEventParseError::InvalidJson(_)));
    }

    #[test]
    fn rejects_empty_event_type_in_json() {
        let err = parse_normalized_event(
            r#"{"slot":1,"signature":null,"program_id":null,"account":null,"event_type":" ","payload":{}}"#,
        )
        .expect_err("empty event_type should fail");

        assert!(matches!(err, NormalizedEventParseError::InvalidJson(_)));
    }

    #[test]
    fn serializes_event_type_as_string() {
        let event_type = EventType::new(EventType::ACCOUNT).expect("event type should be valid");

        assert_eq!(
            serde_json::to_value(event_type).expect("serialize"),
            json!("account")
        );
    }

    #[test]
    fn event_id_is_stable_for_same_input() {
        let event = NormalizedEvent::new(
            42,
            Some("sig-1".to_owned()),
            Some("program-1".to_owned()),
            None,
            EventType::new(EventType::TRANSACTION).expect("event type should be valid"),
            json!({ "source": "test" }),
        );

        assert_eq!(
            event.event_id(),
            "slot=42|signature=sig-1|program=program-1|account=|type=transaction"
        );
    }
}
