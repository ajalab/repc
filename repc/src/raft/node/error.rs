use crate::{
    raft::node::leader::error::CommitError, session::error::SessionError,
    state::error::StateMachineError,
};
use std::{error, fmt};
use tonic::Status;

#[derive(Debug, Clone)]
pub enum CommandError {
    NotLeader,
    SessionError(SessionError),
    CommitError(CommitError),
    StateMachineError(StateMachineError),
}

impl fmt::Display for CommandError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CommandError::NotLeader => write!(f, "not a leader in the current term"),
            CommandError::SessionError(e) => e.fmt(f),
            CommandError::CommitError(e) => e.fmt(f),
            CommandError::StateMachineError(e) => e.fmt(f),
        }
    }
}

impl error::Error for CommandError {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        match self {
            CommandError::NotLeader => None,
            CommandError::SessionError(e) => Some(e),
            CommandError::CommitError(e) => Some(e),
            CommandError::StateMachineError(e) => Some(e),
        }
    }
}

impl From<CommandError> for Status {
    fn from(e: CommandError) -> Self {
        match e {
            CommandError::NotLeader => Status::internal(e.to_string()),
            CommandError::StateMachineError(e) => Status::from(e),
            CommandError::SessionError(e) => Status::from(e),
            CommandError::CommitError(e) => Status::from(e),
        }
    }
}
