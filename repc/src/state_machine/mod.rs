pub mod error;

use bytes::Bytes;

pub trait StateMachine {
    fn apply(
        &mut self,
        path: &str,
        body: &[u8],
    ) -> Result<tonic::Response<Bytes>, error::StateMachineError>;
}
