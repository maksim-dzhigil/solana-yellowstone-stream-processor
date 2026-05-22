use solana_yellowstone_domain::event::NormalizedEvent;

pub trait EventSource {
    type Error;
    type Events: IntoIterator<Item = NormalizedEvent>;

    fn read_events(&self) -> Result<Self::Events, Self::Error>;
}
