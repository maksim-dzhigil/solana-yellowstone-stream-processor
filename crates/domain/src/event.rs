#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NormalizedEvent {
    pub slot: u64,
    pub signature: Option<String>,
    pub program_id: Option<String>,
    pub account: Option<String>,
    pub event_type: String,
    pub payload: String,
}

impl NormalizedEvent {
    pub fn new(
        slot: u64,
        signature: Option<String>,
        program_id: Option<String>,
        account: Option<String>,
        event_type: String,
        payload: String,
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
