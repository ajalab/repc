use super::{Log, LogIndex};
use crate::{pb::raft::LogEntry, types::Term};
#[derive(Default, Clone)]
pub struct InMemoryLog {
    entries: Vec<LogEntry>,
}

impl From<Vec<LogEntry>> for InMemoryLog {
    fn from(entries: Vec<LogEntry>) -> Self {
        InMemoryLog { entries }
    }
}

impl Log for InMemoryLog {
    fn get(&self, i: LogIndex) -> Option<&LogEntry> {
        if 0 < i && i <= self.last_index() {
            Some(&self.entries[i as usize - 1])
        } else {
            None
        }
    }

    fn get_from(&self, i: LogIndex) -> &[LogEntry] {
        if i > self.entries.len() as LogIndex {
            &[]
        } else {
            &self.entries[i as usize - 1..]
        }
    }

    // Append log entries
    fn append<I: Iterator<Item = LogEntry>>(&mut self, entries: I) -> LogIndex {
        let before = self.entries.len();
        self.entries.extend(entries);
        let after = self.entries.len();
        (after - before) as LogIndex
    }

    // Truncates the log so that it drops the entry at the given index and all that follows it.
    fn truncate(&mut self, i: LogIndex) {
        self.entries.truncate((i - 1) as usize);
    }

    /// Returns the term of the last log entry.
    fn last_term(&self) -> Option<Term> {
        self.entries.last().map(|e| Term::new(e.term))
    }

    /// Returns the index of the last appended log entry.
    fn last_index(&self) -> LogIndex {
        self.entries.len() as LogIndex
    }
}

impl InMemoryLog {
    // end is inclusive
    fn _get_range(&self, start: LogIndex, end: LogIndex) -> &[LogEntry] {
        &self.entries[(start as usize - 1)..(end as usize)]
    }
}
