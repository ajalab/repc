use crate::{
    raft::node::leader::error::CommitError, session::error::SessionError,
    state_machine::error::StateMachineError,
};
use repc_common::repc::{
    metadata::{error::MetadataDecodeError, status::StatusMetadata},
    types::NodeId,
};
use std::{error, fmt};
use tonic::{Code, Status};

#[derive(Debug)]
pub enum RequestVoteError {}

impl fmt::Display for RequestVoteError {
    fn fmt(&self, _: &mut fmt::Formatter<'_>) -> fmt::Result {
        Ok(())
    }
}

impl error::Error for RequestVoteError {}

impl From<RequestVoteError> for Status {
    fn from(_: RequestVoteError) -> Self {
        Status::internal("request vote error")
    }
}

#[derive(Debug)]
pub enum AppendEntriesError {}

impl fmt::Display for AppendEntriesError {
    fn fmt(&self, _: &mut fmt::Formatter<'_>) -> fmt::Result {
        Ok(())
    }
}

impl error::Error for AppendEntriesError {}

impl From<AppendEntriesError> for Status {
    fn from(_: AppendEntriesError) -> Self {
        Status::internal("append entries error")
    }
}

#[derive(Clone, Debug)]
pub enum CommandError {
    MetadataDecodeError(MetadataDecodeError),
    NotLeader(Option<NodeId>),
    SessionError(SessionError),
    CommitError(CommitError),
    CommitAborted,
    StateMachineError(StateMachineError),
    Terminated,
    Closed,
}

impl fmt::Display for CommandError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CommandError::MetadataDecodeError(e) => e.fmt(f),
            CommandError::NotLeader(_) => write!(f, "not a leader in the current term"),
            CommandError::SessionError(e) => e.fmt(f),
            CommandError::CommitError(e) => e.fmt(f),
            CommandError::CommitAborted => write!(f, "commit aborted"),
            CommandError::StateMachineError(e) => e.fmt(f),
            CommandError::Terminated => {
                write!(f, "node is terminated while processing the command")
            }
            CommandError::Closed => {
                write!(f, "node channel is closed so command has not been accepted")
            }
        }
    }
}

impl error::Error for CommandError {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        match self {
            CommandError::MetadataDecodeError(e) => Some(e),
            CommandError::NotLeader(_) => None,
            CommandError::SessionError(e) => Some(e),
            CommandError::CommitError(e) => Some(e),
            CommandError::CommitAborted => None,
            CommandError::StateMachineError(e) => Some(e),
            CommandError::Terminated => None,
            CommandError::Closed => None,
        }
    }
}

impl From<CommandError> for Status {
    fn from(e: CommandError) -> Self {
        if let CommandError::StateMachineError(StateMachineError::ApplyFailed(status)) = e {
            return status;
        };

        let code = match e {
            CommandError::MetadataDecodeError(_) => Code::InvalidArgument,
            CommandError::NotLeader(_) => Code::FailedPrecondition,
            CommandError::SessionError(_) => Code::InvalidArgument,
            CommandError::CommitError(ref e) => match e {
                CommitError::NotLeader => Code::FailedPrecondition,
                CommitError::Isolated(_) => Code::Unavailable,
            },
            CommandError::CommitAborted => Code::Unavailable,
            CommandError::StateMachineError(ref e) => match e {
                StateMachineError::UnknownPath(_) => Code::NotFound,
                StateMachineError::DecodeRequestFailed(_) => Code::InvalidArgument,
                StateMachineError::EncodeResponseFailed(_) => Code::Internal,
                StateMachineError::ApplyFailed(_) => unreachable!(),
            },
            CommandError::Terminated => Code::Unavailable,
            CommandError::Closed => Code::Unavailable,
        };

        let fallback = match e {
            CommandError::NotLeader(id) => id,
            _ => None,
        };
        let metadata = StatusMetadata { fallback };

        let message = e.to_string();
        let mut status = Status::new(code, message);
        metadata.encode(status.metadata_mut());

        status
    }
}
