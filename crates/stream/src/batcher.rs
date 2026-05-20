use solana_yellowstone_domain::event::NormalizedEvent;

#[derive(Debug)]
pub struct Batcher {
    capacity: usize,
    pending: Vec<NormalizedEvent>,
}

impl Batcher {
    pub fn new(capacity: usize) -> Self {
        assert!(capacity > 0, "batch capacity must be greater than zero");

        Self {
            capacity,
            pending: Vec::with_capacity(capacity),
        }
    }

    pub fn push(&mut self, event: NormalizedEvent) -> Option<Vec<NormalizedEvent>> {
        self.pending.push(event);

        if self.pending.len() >= self.capacity {
            Some(std::mem::take(&mut self.pending))
        } else {
            None
        }
    }

    pub fn flush(&mut self) -> Option<Vec<NormalizedEvent>> {
        if self.pending.is_empty() {
            None
        } else {
            Some(std::mem::take(&mut self.pending))
        }
    }
}
