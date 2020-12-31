use std::fmt;
use tonic::Status;

#[derive(Debug, Clone)]
pub enum RepcServiceError {
    ClientIdNotExist,
    ClientIdInvalid,
}

impl fmt::Display for RepcServiceError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RepcServiceError::ClientIdNotExist => {
                write!(f, "client id does not exist in the metadata")
            }
            RepcServiceError::ClientIdInvalid => write!(f, "could not parse client id: "),
        }
    }
}

impl std::error::Error for RepcServiceError {}

impl From<RepcServiceError> for Status {
    fn from(e: RepcServiceError) -> Status {
        Status::invalid_argument(e.to_string())
    }
}
