pub mod error;
use crate::service::RepcService;
use bytes::Bytes;
use error::StateMachineError;
use tokio::sync::mpsc;
use tokio::sync::oneshot;

pub struct StateMachineCommand {
    command: Bytes,
    callback: oneshot::Sender<tonic::Response<Bytes>>,
}

impl StateMachineCommand {
    fn new(command: Bytes, callback: oneshot::Sender<tonic::Response<Bytes>>) -> Self {
        StateMachineCommand { command, callback }
    }
}

pub trait StateMachine {
    type Service: RepcService;
    fn apply<P: AsRef<str>>(
        &mut self,
        path: P,
        command: Bytes,
    ) -> Result<tonic::Response<Bytes>, StateMachineError>;
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
        command: Bytes,
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
        while let Some(command) = self.rx.recv().await {
            let res = match self.state_machine.apply("path", command.command) {
                Ok(res) => res,
                Err(e) => {
                    log::warn!("failed to apply: {}", e);
                    break;
                }
            };
            if let Err(_) = command.callback.send(res) {
                log::warn!("couldn't notify the command sender of the application completion");
            }
        }
    }
}
