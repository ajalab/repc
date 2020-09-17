use super::command::Command;
use super::error::StateMachineError;
use crate::service::RepcService;
use bytes::Bytes;

pub trait StateMachine {
    type Service: RepcService;
    fn apply(&mut self, command: Command) -> Result<tonic::Response<Bytes>, StateMachineError>;
}
