use crate::types::{LogIndex, Term};
use bytes::{Buf, Bytes};

#[derive(Default)]
pub struct LogEntry<T> {
    term: Term,
    command: T,
}

impl<T: Clone> LogEntry<T> {
    pub fn new(term: Term, command: T) -> Self {
        LogEntry { term, command }
    }

    pub fn term(&self) -> Term {
        self.term
    }

    pub fn command(&self) -> T {
        self.command.clone()
    }
}

#[derive(Default)]
pub struct Log<T = Bytes> {
    entries: Vec<LogEntry<T>>,
    last_applied: LogIndex,
    last_committed: LogIndex,
}

impl<T: Buf> Log<T> {
    pub fn get(&self, i: LogIndex) -> Option<&LogEntry<T>> {
        if 0 < i && i <= self.last_index() {
            Some(&self.entries[i as usize - 1])
        } else {
            None
        }
    }

    // end is inclusive
    pub fn get_range(&self, start: LogIndex, end: LogIndex) -> &[LogEntry<T>] {
        &self.entries[(start as usize - 1)..(end as usize)]
    }

    // Append log entries
    pub fn append(&mut self, entries: impl Iterator<Item = LogEntry<T>>) {
        self.entries.extend(entries);
    }

    // Truncates the log so that it drops the entry at the given index and all that follows it.
    pub fn truncate(&mut self, i: LogIndex) {
        self.entries.truncate((i - 1) as usize);
    }

    // Commit log entires
    pub fn commit(&mut self, i: LogIndex) {
        self.last_committed = i;
    }

    /// Returns the term of the last log entry.
    pub fn last_term(&self) -> LogIndex {
        self.entries.last().map(|e| e.term).unwrap_or(0)
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
}
