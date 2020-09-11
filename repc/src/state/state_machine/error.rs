use prost::{DecodeError, EncodeError};
use std::error;
use std::fmt;
#[derive(Debug)]
pub enum ApplyError {
    Never,
}

impl fmt::Display for ApplyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            ApplyError::Never => "never",
        };
        write!(f, "{}", s)
    }
}

#[derive(Debug)]
pub enum StateMachineError {
    ManagerTerminated,
    ManagerCrashed,
    UnknownPath(String),
    DecodeRequestFailed(DecodeError),
    EncodeResponseFailed(EncodeError),
    ApplyFailed(ApplyError),
}

impl fmt::Display for StateMachineError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use StateMachineError::*;
        match self {
            ManagerTerminated => write!(f, "the state machine has been terminated"),
            ManagerCrashed => write!(
                f,
                "the state machine has been crashed during command processing"
            ),
            UnknownPath(path) => write!(f, "unknown path: {}", path),
            DecodeRequestFailed(e) => {
                write!(f, "failed to decode the request: ")?;
                e.fmt(f)
            }
            EncodeResponseFailed(e) => {
                write!(f, "failed to encode the response: ")?;
                e.fmt(f)
            }
            ApplyFailed(_) => write!(f, "failed to apply"),
        }
    }
}

impl error::Error for StateMachineError {}
