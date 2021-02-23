use super::error::StateMachineError;
use bytes::Bytes;

pub trait StateMachine {
    fn apply(
        &mut self,
        path: &str,
        body: &[u8],
    ) -> Result<tonic::Response<Bytes>, StateMachineError>;
}

#[cfg(test)]
pub mod test {
    use super::*;
    pub struct NoopStateMachine {}

    impl StateMachine for NoopStateMachine {
        fn apply(
            &mut self,
            _path: &str,
            _body: &[u8],
        ) -> Result<tonic::Response<Bytes>, StateMachineError> {
            Ok(tonic::Response::new(Bytes::new()))
        }
    }
}
