use crate::types::NodeId;
use std::{collections::HashSet, error, fmt};
use tonic::Status;

#[derive(Debug, Clone)]
pub enum CommitError {
    /// The node turned into non-leader state during commit process.
    NotLeader,
    /// The node failed to commit an entry as it could not replicate it to
    /// the majority of nodes.
    Isolated(HashSet<NodeId>),
}

impl fmt::Display for CommitError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CommitError::NotLeader => write!(f, "node turned into non-leader state during its commit process"),
            CommitError::Isolated(nodes) => write!(f, "node failed to commit an entry as it could not replicate it to the majority of nodes: {:?}", nodes),
        }
    }
}

impl error::Error for CommitError {}

impl From<CommitError> for Status {
    fn from(e: CommitError) -> Status {
        Status::internal(e.to_string())
    }
}
