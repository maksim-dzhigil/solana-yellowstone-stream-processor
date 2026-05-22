use crate::source::EventSource;
use solana_yellowstone_domain::event::{
    NormalizedEvent, NormalizedEventParseError, parse_normalized_event,
};
use std::fmt;
use std::fs::File;
use std::io::{self, BufRead, BufReader};
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplaySource {
    pub path: PathBuf,
}

impl ReplaySource {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }

    pub fn read_events(&self) -> Result<Vec<NormalizedEvent>, ReplayReadError> {
        EventSource::read_events(self)
    }
}

impl EventSource for ReplaySource {
    type Error = ReplayReadError;
    type Events = Vec<NormalizedEvent>;

    fn read_events(&self) -> Result<Self::Events, Self::Error> {
        read_jsonl_events(&self.path)
    }
}

#[derive(Debug)]
pub enum ReplayReadError {
    Open {
        path: PathBuf,
        source: io::Error,
    },
    ReadLine {
        line_number: usize,
        source: io::Error,
    },
    ParseLine {
        line_number: usize,
        source: NormalizedEventParseError,
    },
}

impl fmt::Display for ReplayReadError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Open { path, source } => {
                write!(f, "failed to open replay file {}: {source}", path.display())
            }
            Self::ReadLine {
                line_number,
                source,
            } => {
                write!(f, "failed to read replay line {line_number}: {source}")
            }
            Self::ParseLine {
                line_number,
                source,
            } => {
                write!(f, "failed to parse replay line {line_number}: {source}")
            }
        }
    }
}

impl std::error::Error for ReplayReadError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Open { source, .. } | Self::ReadLine { source, .. } => Some(source),
            Self::ParseLine { source, .. } => Some(source),
        }
    }
}

pub fn read_jsonl_events(path: impl AsRef<Path>) -> Result<Vec<NormalizedEvent>, ReplayReadError> {
    let path = path.as_ref();
    let file = File::open(path).map_err(|source| ReplayReadError::Open {
        path: path.to_path_buf(),
        source,
    })?;

    let reader = BufReader::new(file);
    let mut events = Vec::new();

    for (index, line) in reader.lines().enumerate() {
        let line_number = index + 1;
        let line = line.map_err(|source| ReplayReadError::ReadLine {
            line_number,
            source,
        })?;

        let event = parse_normalized_event(&line).map_err(|source| ReplayReadError::ParseLine {
            line_number,
            source,
        })?;
        events.push(event);
    }

    Ok(events)
}

#[cfg(test)]
mod tests {
    use super::{ReplayReadError, ReplaySource, read_jsonl_events};
    use crate::source::EventSource;
    use std::fs;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};

    static FIXTURE_ID: AtomicU64 = AtomicU64::new(0);

    fn write_temp_fixture(contents: &str) -> PathBuf {
        let id = FIXTURE_ID.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "solana-yellowstone-replay-test-{}-{id}.jsonl",
            std::process::id()
        ));
        fs::write(&path, contents).expect("write fixture");
        path
    }

    fn write_jsonl_fixture(lines: &[&str]) -> PathBuf {
        let contents = lines.join("\n") + "\n";
        write_temp_fixture(&contents)
    }

    #[test]
    fn reads_valid_jsonl_events() {
        let first = r#"{"slot":1,"signature":"sig-1","program_id":"program-1","account":null,"event_type":"transaction","payload":{"index":1}}"#;
        let second = r#"{"slot":2,"signature":"sig-2","program_id":"program-1","account":null,"event_type":"transaction","payload":{"index":2}}"#;
        let path = write_jsonl_fixture(&[first, second]);

        let events = read_jsonl_events(&path).expect("events should read");

        assert_eq!(events.len(), 2);
        assert_eq!(events[0].slot, 1);
        assert_eq!(events[1].slot, 2);

        fs::remove_file(path).expect("remove fixture");
    }

    #[test]
    fn keeps_duplicate_rows_as_duplicate_events() {
        let line = r#"{"slot":2,"signature":"sig-2","program_id":"program-1","account":null,"event_type":"transaction","payload":{"index":2}}"#;
        let path = write_jsonl_fixture(&[line, line]);

        let events = read_jsonl_events(&path).expect("events should read");

        assert_eq!(events.len(), 2);
        assert_eq!(events[0].event_id(), events[1].event_id());

        fs::remove_file(path).expect("remove fixture");
    }

    #[test]
    fn replay_source_implements_event_source_boundary() {
        let line = r#"{"slot":3,"signature":"sig-3","program_id":"program-1","account":null,"event_type":"transaction","payload":{"index":3}}"#;
        let path = write_jsonl_fixture(&[line]);
        let source = ReplaySource::new(&path);

        let events = EventSource::read_events(&source).expect("events should read");

        assert_eq!(events.len(), 1);
        assert_eq!(events[0].slot, 3);

        fs::remove_file(path).expect("remove fixture");
    }

    #[test]
    fn reports_line_number_for_invalid_json() {
        let valid = r#"{"slot":1,"signature":"sig-1","program_id":"program-1","account":null,"event_type":"transaction","payload":{}}"#;
        let path = write_jsonl_fixture(&[valid, "not-json"]);

        let err = read_jsonl_events(&path).expect_err("invalid line should fail");

        assert!(matches!(
            err,
            ReplayReadError::ParseLine { line_number: 2, .. }
        ));

        fs::remove_file(path).expect("remove fixture");
    }

    #[test]
    fn reports_missing_file_path() {
        let path = std::env::temp_dir().join("solana-yellowstone-missing-replay-file.jsonl");

        let err = read_jsonl_events(&path).expect_err("missing file should fail");

        assert!(matches!(err, ReplayReadError::Open { .. }));
    }
}
