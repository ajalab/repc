use crate::session::error::SessionError;
use crate::state::error::StateMachineError;
use std::error;
use std::fmt;
use tonic::Status;

#[derive(Debug, Clone)]
pub enum CommandError {
    NotLeader,
    StateMachineError(StateMachineError),
    SessionError(SessionError),
}

impl fmt::Display for CommandError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use CommandError::*;
        match self {
            NotLeader => write!(f, "not a leader in the current term"),
            StateMachineError(e) => e.fmt(f),
            SessionError(e) => e.fmt(f),
        }
    }
}

impl CommandError {
    pub fn into_status(self) -> Status {
        use CommandError::*;
        match self {
            StateMachineError(e) => e.into_status(),
            SessionError(e) => e.into_status(),
            _ => Status::internal(self.to_string()),
        }
    }
}

impl error::Error for CommandError {}
