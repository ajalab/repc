use std::fmt;
use tonic::Status;

#[derive(Debug)]
pub enum Error {
    RegisterError(Status),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::RegisterError(status) => {
                write!(f, "RegisterError: ")?;
                status.fmt(f)
            }
        }
    }
}

impl std::error::Error for Error {}
