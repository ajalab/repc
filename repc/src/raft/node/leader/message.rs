use crate::state::{error::StateMachineError, log::LogIndex};
use bytes::Bytes;

/// A message sent from the node process to a replicator.
/// Represents that a new entry is ready in the log.
pub struct Ready;

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
