use prost::{DecodeError, EncodeError};
use std::error;
use std::fmt;
use tonic::Status;

#[derive(Clone, Debug)]
pub enum StateMachineError {
    UnknownPath(String),
    DecodeRequestFailed(DecodeError),
    EncodeResponseFailed(EncodeError),
    ApplyFailed(Status),
}

impl fmt::Display for StateMachineError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use StateMachineError::*;
        match self {
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

impl error::Error for StateMachineError {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        use StateMachineError::*;
        match self {
            UnknownPath(_) => None,
            DecodeRequestFailed(e) => Some(e),
            EncodeResponseFailed(e) => Some(e),
            ApplyFailed(e) => Some(e),
        }
    }
}

impl From<StateMachineError> for Status {
    fn from(e: StateMachineError) -> Status {
        use StateMachineError::*;
        match e {
            ApplyFailed(status) => status,
            _ => Status::internal(e.to_string()),
        }
    }
}
