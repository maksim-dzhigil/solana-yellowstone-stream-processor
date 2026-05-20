use solana_yellowstone_domain::event::NormalizedEvent;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplaySource {
    pub path: String,
}

impl ReplaySource {
    pub fn new(path: impl Into<String>) -> Self {
        Self { path: path.into() }
    }

    pub fn sample_event(&self) -> NormalizedEvent {
        NormalizedEvent::new(
            1,
            Some("fixture-signature-1".to_owned()),
            Some("fixture-program-1".to_owned()),
            None,
            "transaction".to_owned(),
            format!(r#"{{"source":"{}"}}"#, self.path),
        )
    }
}
