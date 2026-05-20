#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PipelineConfig {
    pub batch_size: usize,
    pub channel_capacity: usize,
}

impl Default for PipelineConfig {
    fn default() -> Self {
        Self {
            batch_size: 500,
            channel_capacity: 10_000,
        }
    }
}
