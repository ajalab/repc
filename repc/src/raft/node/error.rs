use crate::state::error::StateMachineError;
use std::error;
use std::fmt;
use tonic::Status;

#[derive(Debug)]
pub enum CommandError {
    InvalidArgument,
    NotLeader,
    StateMachineError(StateMachineError),
}

impl fmt::Display for CommandError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use CommandError::*;
        match self {
            InvalidArgument => write!(f, "request has an invalid argument"),
            NotLeader => write!(f, "not a leader in the current term"),
            StateMachineError(e) => e.fmt(f),
        }
    }
}

impl CommandError {
    pub fn into_status(self) -> Status {
        use CommandError::*;
        match self {
            StateMachineError(e) => e.into_status(),
            _ => Status::internal(self.to_string()),
        }
    }
}

impl error::Error for CommandError {}
