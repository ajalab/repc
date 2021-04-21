pub mod error;

use bytes::Bytes;

#[crate::async_trait]
pub trait StateMachine {
    async fn apply(
        &mut self,
        path: &str,
        body: &[u8],
    ) -> Result<tonic::Response<Bytes>, error::StateMachineError>;
}
