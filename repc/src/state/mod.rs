use crate::{
    log::{Log, LogIndex},
    pb::raft::{log_entry::Command, LogEntry},
    state_machine::{error::StateMachineError, StateMachine},
};
use bytes::Bytes;

#[derive(Clone, Default)]
pub struct State<S, L> {
    state_machine: S,
    log: L,
    last_applied: LogIndex,
    last_committed: LogIndex,
}

impl<S, L> State<S, L> {
    pub fn new(state_machine: S, log: L) -> Self {
        State {
            state_machine,
            log,
            last_applied: LogIndex::default(),
            last_committed: LogIndex::default(),
        }
    }

    pub fn log(&self) -> &L {
        &self.log
    }

    pub fn last_committed(&self) -> LogIndex {
        self.last_committed
    }

    pub fn last_applied(&self) -> LogIndex {
        self.last_applied
    }
}

impl<S: StateMachine, L: Log> State<S, L> {
    pub fn last_index(&self) -> LogIndex {
        self.log.last_index()
    }

    pub fn append_log_entries(&mut self, entries: impl Iterator<Item = LogEntry>) {
        let len = self.log.append(entries);
        tracing::trace!("appended {} entries to the log", len);
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

    pub async fn apply(&mut self) -> Option<Result<tonic::Response<Bytes>, StateMachineError>> {
        if self.last_applied < self.last_committed {
            let entry = self.log.get(self.last_applied + 1)?;
            let command = entry.command.as_ref()?;
            let result = match command {
                Command::Action(action) => self
                    .state_machine
                    .apply(action.path.as_ref(), action.body.as_ref())
                    .await,
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
