use std::error;
use std::fmt;
use tonic::Status;

#[derive(Debug)]
pub enum CommandError {
    InvalidArgument,
    NotLeader,
}

impl fmt::Display for CommandError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CommandError::InvalidArgument => write!(f, "request has an invalid argument"),
            CommandError::NotLeader => write!(f, "not a leader in the current term"),
        }
    }
}

impl CommandError {
    pub fn into_status(self) -> Status {
        match self {
            _ => Status::internal(self.to_string()),
        }
    }
}

impl error::Error for CommandError {}
