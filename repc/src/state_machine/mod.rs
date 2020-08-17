pub mod error;
use bytes::Bytes;
use error::StateMachineError;
use tokio::sync::mpsc;
use tokio::sync::oneshot;

pub struct StateMachineCommand {
    command: Bytes,
    tx: oneshot::Sender<()>,
}

impl StateMachineCommand {
    fn new(command: Bytes, tx: oneshot::Sender<()>) -> Self {
        StateMachineCommand { command, tx }
    }
}

pub trait StateMachine {
    fn apply<P: AsRef<str>>(&mut self, path: P, command: Bytes)
        -> Result<Bytes, StateMachineError>;
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

    pub async fn apply(&mut self, command: Bytes) -> Result<(), StateMachineError> {
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
            self.state_machine.apply("path", command.command);
            if let Err(_) = command.tx.send(()) {
                log::warn!("couldn't notify the command sender of the application completion");
            }
        }
    }
}
