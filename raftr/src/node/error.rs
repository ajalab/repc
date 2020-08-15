use std::error;
use std::fmt;

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

impl error::Error for CommandError {}
