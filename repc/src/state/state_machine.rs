use super::error::StateMachineError;
use bytes::Bytes;

pub trait StateMachine {
    fn apply(
        &mut self,
        path: &str,
        body: &[u8],
    ) -> Result<tonic::Response<Bytes>, StateMachineError>;
}
