#[allow(dead_code)]
#[derive(Debug)]
pub enum AppError {
    Config(String),
    Storage(String),
    Stream(String),
}
