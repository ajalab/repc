use std::error;
use std::fmt;
use tonic::Status;

use super::Sequence;

#[derive(Clone, Debug)]
pub enum SessionError {
    ClientNotRegistered,
    SessionInvalid {
        expected: Sequence,
        actual: Sequence,
    },
    RequestTooStale,
}

impl fmt::Display for SessionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SessionError::ClientNotRegistered => {
                write!(f, "session does not exist. likely client is not registered")
            }
            SessionError::SessionInvalid { expected, actual } => write!(
                f,
                "session does not match. expected: {}, actual: {}",
                expected, actual
            ),
            SessionError::RequestTooStale => write!(
                f,
                "request has too old session sequence. stored response has been lost",
            ),
        }
    }
}

impl error::Error for SessionError {}

impl From<SessionError> for Status {
    fn from(e: SessionError) -> Status {
        Status::invalid_argument(e.to_string())
    }
}
