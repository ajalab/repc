use std::{error, fmt};
use tonic::Status;

#[derive(Debug, Eq, PartialEq)]
pub enum HandleError {
    ServiceDropped,
    HandleDropped,
    NoPendingResponse,
    ConversionError(ConversionError),
}

impl fmt::Display for HandleError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            HandleError::ServiceDropped => write!(f, "service has been dropped"),
            HandleError::HandleDropped => write!(f, "handle has been dropped"),
            HandleError::NoPendingResponse => write!(f, "no pending responses"),
            HandleError::ConversionError(e) => e.fmt(f),
        }
    }
}

impl error::Error for HandleError {}

#[derive(Debug, Eq, PartialEq)]
pub enum ConversionError {
    ExpectedAppendEntriesRequest,
    ExpectedRequestVoteRequest,
    ExpectedAppendEntriesResponse,
    ExpectedRequestVoteResponse,
}

impl fmt::Display for ConversionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use ConversionError::*;
        match self {
            ExpectedRequestVoteRequest => write!(
                f,
                "expected RequestVoteRequest, but was AppendEntriesRequest"
            ),
            ExpectedAppendEntriesRequest => write!(
                f,
                "expected AppendEntriesRequest, but was RequestVoteRequest"
            ),
            ExpectedRequestVoteResponse => write!(
                f,
                "expected RequestVoteResponse, but was AppendEntriesResponse"
            ),
            ExpectedAppendEntriesResponse => write!(
                f,
                "expected AppendEntriesResponse, but was RequestVoteResponse"
            ),
        }
    }
}

impl From<ConversionError> for Status {
    fn from(e: ConversionError) -> Self {
        Status::internal(e.to_string())
    }
}

impl error::Error for ConversionError {}
