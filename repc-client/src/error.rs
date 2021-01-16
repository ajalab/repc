use std::fmt;
use tonic::Status;

#[derive(Debug)]
pub struct RegisterError(Status);

impl RegisterError {
    pub fn new(status: Status) -> Self {
        RegisterError(status)
    }
}

impl fmt::Display for RegisterError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "RegisterError: ")?;
        self.0.fmt(f)
    }
}

impl std::error::Error for RegisterError {}
