use bytes::Bytes;
use prost::{DecodeError, EncodeError};
use std::error;
use std::fmt;
use tokio::sync::mpsc;
use tokio::sync::oneshot;

#[derive(Debug)]
pub enum ApplyError {
    Never,
}

impl fmt::Display for ApplyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            ApplyError::Never => "never",
        };
        write!(f, "{}", s)
    }
}

#[derive(Debug)]
pub enum StateMachineError {
    ManagerTerminated,
    ManagerCrashed,
    UnknownPath(String),
    DecodeRequestFailed(DecodeError),
    EncodeResponseFailed(EncodeError),
    ApplyFailed(ApplyError),
}

impl fmt::Display for StateMachineError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use StateMachineError::*;
        match self {
            ManagerTerminated => write!(f, "the state machine has been terminated"),
            ManagerCrashed => write!(
                f,
                "the state machine has been crashed during command processing"
            ),
            UnknownPath(path) => write!(f, "unknown path: {}", path),
            DecodeRequestFailed(e) => {
                write!(f, "failed to decode the request: ")?;
                e.fmt(f)
            }
            EncodeResponseFailed(e) => {
                write!(f, "failed to encode the response: ")?;
                e.fmt(f)
            }
            ApplyFailed(_) => write!(f, "failed to apply"),
        }
    }
}

impl error::Error for StateMachineError {}

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
