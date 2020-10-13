use super::message::{Applied, Replicated};
use crate::raft::node::error::CommandError;
use crate::state::log::LogIndex;
use crate::state::{State, StateMachine};
use crate::types::NodeId;
use bytes::Bytes;
use futures::{future, StreamExt};
use std::collections::HashMap;
use std::sync::Weak;
use tokio::sync::{broadcast, mpsc, RwLock};

pub struct CommitManager {
    tx_applied: broadcast::Sender<Applied>,
    rx_applied: broadcast::Receiver<Applied>,
}

impl CommitManager {
    pub fn spawn<S>(
        nodes: impl Iterator<Item = NodeId>,
        state: Weak<RwLock<State<S>>>,
    ) -> (Self, CommitManagerNotifier)
    where
        S: StateMachine + Send + Sync + 'static,
    {
        let (tx_applied, rx_applied) = broadcast::channel(100);
        let (tx_replicated, rx_replicated) = mpsc::channel(100);

        let process = CommitManagerProcess::new(tx_applied.clone(), rx_replicated, nodes, state);
        tokio::spawn(process.run());

        let commit_manager = CommitManager {
            tx_applied,
            rx_applied,
        };
        let commit_manager_notifier = CommitManagerNotifier { tx_replicated };

        (commit_manager, commit_manager_notifier)
    }

    pub fn subscribe(&mut self) -> CommitManagerSubscription {
        CommitManagerSubscription {
            rx: std::mem::replace(&mut self.rx_applied, self.tx_applied.subscribe()),
        }
    }
}

pub struct CommitManagerSubscription {
    rx: broadcast::Receiver<Applied>,
}

impl CommitManagerSubscription {
    pub async fn wait_applied(
        self,
        index: LogIndex,
    ) -> Result<tonic::Response<Bytes>, CommandError> {
        let stream = self.rx.into_stream();
        tokio::pin!(stream);

        stream
            .filter_map(|applied| {
                future::ready(match applied {
                    Ok(applied) if applied.index() >= index => Some(
                        applied
                            .into_result()
                            .map_err(CommandError::StateMachineError),
                    ),
                    Err(broadcast::RecvError::Closed) => Some(Err(CommandError::NotLeader)),
                    Err(broadcast::RecvError::Lagged(n)) => {
                        tracing::warn!("commit notification is lagging: {}", n);
                        None
                    }
                    _ => None,
                })
            })
            .next()
            .await
            .unwrap_or_else(|| Err(CommandError::NotLeader))
    }
}

#[derive(Clone)]
pub struct CommitManagerNotifier {
    tx_replicated: mpsc::Sender<Replicated>,
}

impl CommitManagerNotifier {
    pub async fn notify_replicated(
        &mut self,
        id: NodeId,
        index: LogIndex,
    ) -> Result<(), mpsc::error::SendError<Replicated>> {
        self.tx_replicated.send(Replicated::new(id, index)).await
    }
}

struct CommitManagerProcess<S> {
    tx_applied: broadcast::Sender<Applied>,
    rx_replicated: mpsc::Receiver<Replicated>,
    match_indices: HashMap<NodeId, LogIndex>,
    state: Weak<RwLock<State<S>>>,
}

impl<S> CommitManagerProcess<S>
where
    S: StateMachine,
{
    fn new(
        tx_applied: broadcast::Sender<Applied>,
        rx_replicated: mpsc::Receiver<Replicated>,
        nodes: impl Iterator<Item = NodeId>,
        state: Weak<RwLock<State<S>>>,
    ) -> Self {
        let match_indices = nodes.zip(std::iter::repeat(LogIndex::default())).collect();
        Self {
            tx_applied,
            rx_replicated,
            match_indices,
            state,
        }
    }

    async fn run(mut self) {
        let state = match self.state.upgrade() {
            Some(state) => state,
            None => {
                tracing::info!("failed to acquire strong reference to the state; likely not being a leader anymore");
                return;
            }
        };
        let state = state.as_ref().read().await;
        let mut committed = state.last_committed();
        drop(state);

        while let Some(replicated) = self.rx_replicated.recv().await {
            let s = self.match_indices.get_mut(&replicated.id()).unwrap();
            *s = replicated.index();

            let mut indices = self.match_indices.values().collect::<Vec<_>>();
            indices.sort_by(|a, b| b.cmp(a));

            let majority = *indices[indices.len() / 2];
            if majority > committed {
                let state = match self.state.upgrade() {
                    Some(state) => state,
                    None => {
                        tracing::info!(
                            "failed to acquire strong reference to the state; likely not being a leader anymore");
                        break;
                    }
                };
                let mut state = state.write().await;
                committed = state.commit(majority);
                tracing::trace!("new commit position: {}", committed);
                while let Some(result) = state.apply() {
                    let index = state.last_applied();
                    tracing::trace!("applied: {}", index);
                    let applied = Applied::new(index, result);
                    if let Err(_) = self.tx_applied.send(applied) {
                        tracing::info!("leader process has been terminated");
                        break;
                    }
                }
            }
        }
    }
}
