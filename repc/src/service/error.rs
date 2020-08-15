use crate::raft::node::error::CommandError;
use std::fmt;
use tonic::body::BoxBody;
use tonic::Status;

#[derive(Debug)]
pub enum RepcServiceError {
    NodeTerminated,
    NodeCrashed,
    CommandInvalid(Status),
    CommandMissing,
    CommandFailed(CommandError),
}

impl RepcServiceError {
    pub fn description(&self) -> &'static str {
        use RepcServiceError::*;
        match self {
            NodeTerminated => "command could not be handled because the node has been terminated",
            NodeCrashed => "node failed to handle the command during its process",
            CommandInvalid(_) => "failed to decode the command",
            CommandMissing => "command is missing in the request",
            CommandFailed(_) => "failed to process command",
        }
    }

    pub fn into_status(self) -> Status {
        use RepcServiceError::*;
        match self {
            CommandInvalid(status) => status,
            _ => Status::internal(self.to_string()),
        }
    }

    pub fn into_http(self) -> http::Response<BoxBody> {
        self.into_status().to_http()
    }
}

impl fmt::Display for RepcServiceError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use RepcServiceError::*;
        write!(f, "{}", self.description())?;
        match self {
            CommandInvalid(s) => write!(f, ": {}", s),
            CommandFailed(e) => write!(f, ": {}", e),
            _ => Ok(()),
        }
    }
}

impl std::error::Error for RepcServiceError {}
