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
        }
    }
}

impl SessionError {
    pub fn into_status(self) -> Status {
        Status::invalid_argument(self.to_string())
    }
}
