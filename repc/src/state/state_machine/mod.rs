pub mod error;
use super::Command;
use crate::service::RepcService;
use bytes::Bytes;
use error::StateMachineError;
use tokio::sync::mpsc;
use tokio::sync::oneshot;

pub struct StateMachineCommand {
    command: Command,
    callback: oneshot::Sender<tonic::Response<Bytes>>,
}

impl StateMachineCommand {
    fn new(command: Command, callback: oneshot::Sender<tonic::Response<Bytes>>) -> Self {
        StateMachineCommand { command, callback }
    }
}

pub trait StateMachine {
    type Service: RepcService;
    fn apply(&mut self, command: Command) -> Result<tonic::Response<Bytes>, StateMachineError>;
}

#[derive(Clone)]
pub struct StateMachineManager {
    tx: mpsc::Sender<StateMachineCommand>,
}

impl StateMachineManager {
    pub fn spawn<S>(state_machine: S) -> Self
    where
        S: StateMachine + Send + 'static,
    {
        let (tx, rx) = mpsc::channel(100);
        let process = StateMachineManagerProcess::<S> { state_machine, rx };

        tokio::spawn(process.run());

        Self { tx }
    }

    pub async fn apply(
        &mut self,
        command: Command,
    ) -> Result<tonic::Response<Bytes>, StateMachineError> {
        let (tx, rx) = oneshot::channel();
        let command = StateMachineCommand::new(command, tx);
        self.tx
            .send(command)
            .await
            .map_err(|_| StateMachineError::ManagerTerminated)?;
        rx.await.map_err(|_| StateMachineError::ManagerCrashed)
    }
}

struct StateMachineManagerProcess<S> {
    state_machine: S,
    rx: mpsc::Receiver<StateMachineCommand>,
}

impl<S> StateMachineManagerProcess<S>
where
    S: StateMachine,
{
    pub async fn run(mut self) {
        while let Some(StateMachineCommand { command, callback }) = self.rx.recv().await {
            let res = match self.state_machine.apply(command) {
                Ok(res) => res,
                Err(e) => {
                    tracing::error!("failed to apply: {}", e);
                    break;
                }
            };
            if let Err(_) = callback.send(res) {
                tracing::warn!("couldn't notify the command sender of the application completion");
            }
        }
    }
}
