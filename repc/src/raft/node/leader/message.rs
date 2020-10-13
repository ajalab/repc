use crate::state::error::StateMachineError;
use crate::state::log::LogIndex;
use crate::types::NodeId;
use bytes::Bytes;

pub struct Appended;

pub struct Applied {
    index: LogIndex,
    result: Result<tonic::Response<Bytes>, StateMachineError>,
}

impl Applied {
    pub fn new(index: LogIndex, result: Result<tonic::Response<Bytes>, StateMachineError>) -> Self {
        Self { index, result }
    }

    pub fn index(&self) -> LogIndex {
        self.index
    }

    pub fn into_result(self) -> Result<tonic::Response<Bytes>, StateMachineError> {
        self.result
    }
}

impl Clone for Applied {
    fn clone(&self) -> Self {
        let result = match &self.result {
            Ok(res) => {
                let mut res_clone = tonic::Response::new(res.get_ref().clone());
                *res_clone.metadata_mut() = res.metadata().clone();
                Ok(res_clone)
            }
            Err(s) => Err(s.clone()),
        };
        Self {
            index: self.index,
            result,
        }
    }
}

pub struct Replicated {
    id: NodeId,
    index: LogIndex,
}

impl Replicated {
    pub fn new(id: NodeId, index: LogIndex) -> Self {
        Self { id, index }
    }

    pub fn id(&self) -> NodeId {
        self.id
    }

    pub fn index(&self) -> LogIndex {
        self.index
    }
}
