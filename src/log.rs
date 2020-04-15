use crate::types::{LogIndex, Term};

#[derive(Default)]
pub struct LogEntry {
    term: Term,
}

impl LogEntry {
    pub fn new(term: Term) -> Self {
        LogEntry { term }
    }

    pub fn term(&self) -> Term {
        self.term
    }
}

#[derive(Default)]
pub struct Log {
    entries: Vec<LogEntry>,
    last_applied: LogIndex,
    last_committed: LogIndex,
}

impl Log {
    pub fn get(&self, i: LogIndex) -> Option<&LogEntry> {
        if 0 < i && i - 1 < self.entries.len() as u64 {
            Some(&self.entries[i as usize - 1])
        } else {
            None
        }
    }

    pub fn append(&mut self, entries: impl Iterator<Item = LogEntry>) {
        self.entries.extend(entries);
    }

    // Truncates the log so that it drops the entry at the given index and all that follows it.
    pub fn truncate(&mut self, i: LogIndex) {
        self.entries.truncate((i - 1) as usize);
    }

    /// Returns the term of the last log entry.
    pub fn last_term(&self) -> LogIndex {
        self.entries.first().map(|e| e.term).unwrap_or(0)
    }

    /// Returns the index of the last appended log entry.
    pub fn last_index(&self) -> LogIndex {
        self.entries.len() as LogIndex
    }

    /// Returns the index of the last committed log entry.
    pub fn last_committed(&self) -> LogIndex {
        self.last_committed
    }

    /// Returns the index of the last log entry applied to the state machine.
    pub fn last_applied(&self) -> LogIndex {
        self.last_applied
    }

    pub fn entries(&self) -> &Vec<LogEntry> {
        &self.entries
    }
}
