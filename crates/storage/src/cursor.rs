#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamCursor {
    pub stream_name: String,
    pub last_persisted_slot: u64,
}

impl StreamCursor {
    pub fn new(stream_name: impl Into<String>) -> Self {
        Self {
            stream_name: stream_name.into(),
            last_persisted_slot: 0,
        }
    }

    pub fn advance_to(&mut self, slot: u64) {
        self.last_persisted_slot = self.last_persisted_slot.max(slot);
    }
}
