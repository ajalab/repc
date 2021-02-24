use crate::pb::raft::LogEntry;
use crate::types::Term;

pub mod in_memory;

pub type LogIndex = u64;

pub trait Log {
    fn get(&self, i: LogIndex) -> Option<&LogEntry>;
    fn get_from(&self, i: LogIndex) -> &[LogEntry];
    fn append<I: Iterator<Item = LogEntry>>(&mut self, entries: I) -> LogIndex;
    fn truncate(&mut self, i: LogIndex);

    fn last_term(&self) -> Term;
    fn last_index(&self) -> LogIndex;
}
