#[allow(dead_code)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HealthState {
    Starting,
    Ready,
    Draining,
}
