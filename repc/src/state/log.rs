use crate::state::Command;
use crate::types::Term;

pub type LogIndex = u64;
pub struct LogEntry {
    term: Term,
    command: Command,
}

impl LogEntry {
    pub fn new(term: Term, command: Command) -> Self {
        LogEntry { term, command }
    }

    pub fn term(&self) -> Term {
        self.term
    }

    pub fn command(&self) -> &Command {
        &self.command
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
        if 0 < i && i <= self.last_index() {
            Some(&self.entries[i as usize - 1])
        } else {
            None
        }
    }

    pub fn iter_at(&self, i: LogIndex) -> impl Iterator<Item = &LogEntry> {
        if i > self.entries.len() as LogIndex {
            [].iter()
        } else {
            self.entries[i as usize - 1..].iter()
        }
    }

    // end is inclusive
    pub fn get_range(&self, start: LogIndex, end: LogIndex) -> &[LogEntry] {
        &self.entries[(start as usize - 1)..(end as usize)]
    }

    // Append log entries
    pub fn append(&mut self, entries: impl Iterator<Item = LogEntry>) -> LogIndex {
        let before = self.entries.len();
        self.entries.extend(entries);
        let after = self.entries.len();
        (after - before) as LogIndex
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
