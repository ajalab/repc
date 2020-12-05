pub mod error;
pub mod log;
mod state_machine;

pub use state_machine::StateMachine;

use crate::pb::raft::{log_entry::Command, LogEntry};
use bytes::Bytes;
use error::StateMachineError;
use log::{Log, LogIndex};

pub struct State<S> {
    log: Log,
    state_machine: S,
    last_applied: LogIndex,
    last_committed: LogIndex,
}

impl<S> State<S> {
    pub fn new(state_machine: S) -> Self {
        State {
            log: Log::default(),
            state_machine,
            last_applied: LogIndex::default(),
            last_committed: LogIndex::default(),
        }
    }
}

impl<S: StateMachine> State<S> {
    pub fn log(&self) -> &Log {
        &self.log
    }

    pub fn last_index(&self) -> LogIndex {
        self.log.last_index()
    }

    pub fn last_committed(&self) -> LogIndex {
        self.last_committed
    }

    pub fn last_applied(&self) -> LogIndex {
        self.last_applied
    }

    pub fn append_log_entries(&mut self, entries: impl Iterator<Item = LogEntry>) {
        self.log.append(entries);
    }

    pub fn truncate_log(&mut self, i: LogIndex) {
        self.log.truncate(i);
    }

    pub fn commit(&mut self, i: LogIndex) -> LogIndex {
        if i > self.last_committed {
            let last_committed = self.last_index().min(i);
            self.last_committed = last_committed;
        }
        self.last_committed
    }

    pub fn apply(&mut self) -> Option<Result<tonic::Response<Bytes>, StateMachineError>> {
        if self.last_applied < self.last_committed {
            let entry = self.log.get(self.last_applied + 1)?;
            let command = entry.command.as_ref()?;
            let result = match command {
                Command::Action(action) => self
                    .state_machine
                    .apply(action.path.as_ref(), action.body.as_ref()),
                Command::Register(_) => Ok(tonic::Response::new(Bytes::from(
                    (self.last_applied + 1).to_be_bytes().to_vec(),
                ))),
            };
            self.last_applied += 1;
            Some(result)
        } else {
            None
        }
    }
}
