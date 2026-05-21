use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConfigError {
    Empty { key: &'static str },
    InvalidUsize { key: &'static str, value: String },
    NotUnicode { key: &'static str },
    NonPositive { key: &'static str },
}

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Empty { key } => write!(f, "{key} must not be empty"),
            Self::InvalidUsize { key, value } => {
                write!(f, "{key} must be a positive integer, got {value:?}")
            }
            Self::NotUnicode { key } => write!(f, "{key} contains non-unicode data"),
            Self::NonPositive { key } => write!(f, "{key} must be greater than zero"),
        }
    }
}

impl std::error::Error for ConfigError {}

#[allow(dead_code)]
#[derive(Debug)]
pub enum AppError {
    Config(ConfigError),
    Storage(String),
    Stream(String),
}
