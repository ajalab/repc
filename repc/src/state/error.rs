use prost::{DecodeError, EncodeError};
use std::error;
use std::fmt;

#[derive(Clone, Debug)]
pub enum StateMachineError {
    UnknownPath(String),
    DecodeRequestFailed(DecodeError),
    EncodeResponseFailed(EncodeError),
    ApplyFailed(tonic::Status),
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

impl error::Error for StateMachineError {}

impl StateMachineError {
    pub fn into_status(self) -> tonic::Status {
        use StateMachineError::*;
        match self {
            ApplyFailed(status) => status,
            _ => tonic::Status::internal(self.to_string()),
        }
    }
}
