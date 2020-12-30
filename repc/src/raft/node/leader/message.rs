use crate::state::error::StateMachineError;
use crate::state::log::LogIndex;
use crate::types::NodeId;
use bytes::Bytes;

/// A message sent from the node process to an appender.
/// Represents that a new entry is ready in the log.
pub struct Ready;

/// A message sent from an appender to the commit manager.
/// Represents that an entry is replicated successfully or
/// replication is failed.
#[derive(Debug)]
pub struct Replicated {
    id: NodeId,
    index: LogIndex,
    success: bool,
}

impl Replicated {
    pub fn new(id: NodeId, index: LogIndex, success: bool) -> Self {
        Self { id, index, success }
    }

    pub fn id(&self) -> NodeId {
        self.id
    }

    pub fn index(&self) -> LogIndex {
        self.index
    }

    pub fn success(&self) -> bool {
        self.success
    }
}

/// A message sent from the commit manager to the node process.
pub struct Applied {
    pub index: LogIndex,
    pub result: Result<tonic::Response<Bytes>, StateMachineError>,
}

impl Clone for Applied {
    fn clone(&self) -> Self {
        let result = match self.result.as_ref() {
            Ok(res) => {
                let mut res_clone = tonic::Response::new(res.get_ref().clone());
                *res_clone.metadata_mut() = res.metadata().clone();
                Ok(res_clone)
            }
            Err(s) => Err(s.clone()),
        };
        Applied {
            index: self.index,
            result,
        }
    }
}
