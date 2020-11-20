use super::command::Command;
use super::error::StateMachineError;
use bytes::Bytes;

pub trait StateMachine {
    fn apply(&mut self, command: Command) -> Result<tonic::Response<Bytes>, StateMachineError>;
}
