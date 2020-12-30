use super::error::CommitError;
use super::message::{Applied, Replicated};
use crate::raft::node::error::CommandError;
use crate::state::log::LogIndex;
use crate::state::{State, StateMachine};
use crate::types::NodeId;
use bytes::Bytes;
use futures::{future, StreamExt};
use std::collections::{HashMap, HashSet};
use std::sync::Weak;
use tokio::sync::{broadcast, mpsc, RwLock};

pub struct CommitManager {
    tx_applied: broadcast::Sender<Result<Applied, CommitError>>,
    rx_applied: broadcast::Receiver<Result<Applied, CommitError>>,
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
    rx: broadcast::Receiver<Result<Applied, CommitError>>,
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
                    Ok(Ok(Applied {
                        index: applied_index,
                        result,
                    })) if applied_index >= index => {
                        Some(result.map_err(CommandError::StateMachineError))
                    }
                    Ok(Err(_)) => Some(Err(CommandError::NotLeader)),
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
    pub async fn notify(
        &mut self,
        id: NodeId,
        index: LogIndex,
        success: bool,
    ) -> Result<(), mpsc::error::SendError<Replicated>> {
        self.tx_replicated
            .send(Replicated::new(id, index, success))
            .await
    }
}

struct CommitManagerProcess<S> {
    tx_applied: broadcast::Sender<Result<Applied, CommitError>>,
    rx_replicated: mpsc::Receiver<Replicated>,
    match_indices: HashMap<NodeId, LogIndex>,
    state: Weak<RwLock<State<S>>>,
    committed_index: LogIndex,
    failed_nodes: HashSet<NodeId>,
}

impl<S> CommitManagerProcess<S>
where
    S: StateMachine,
{
    fn new(
        tx_applied: broadcast::Sender<Result<Applied, CommitError>>,
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
            committed_index: 0,
            failed_nodes: HashSet::new(),
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
        self.committed_index = state.last_committed();
        drop(state);

        while let Some(replicated) = self.rx_replicated.recv().await {
            tracing::trace!("get replication result: {:?}", replicated);
            let result = if replicated.success() {
                self.handle_replication_success(replicated.id(), replicated.index())
                    .await
            } else {
                self.handle_replication_failure(replicated.id())
            };
            if let Err(e) = result {
                let _ = self.tx_applied.send(Err(e));
                break;
            }
        }
    }

    async fn handle_replication_success(
        &mut self,
        id: NodeId,
        match_index: LogIndex,
    ) -> Result<(), CommitError> {
        self.failed_nodes.remove(&id);

        let index = self.match_indices.get_mut(&id).unwrap();
        *index = match_index;

        let mut indices = self.match_indices.values().collect::<Vec<_>>();
        indices.sort();

        // The match_index of the leader itself is not included in `indices`.
        // As it must always have the largest match_index, we can compute
        // the least match_index among the majority by the following expression.
        let majority = *indices[indices.len() / 2];
        if majority > self.committed_index {
            let state = match self.state.upgrade() {
                Some(state) => state,
                None => {
                    tracing::info!(
                            "failed to acquire strong reference to the state; likely not being a leader anymore");
                    return Err(CommitError::NotLeader);
                }
            };
            let mut state = state.write().await;

            self.committed_index = state.commit(majority);
            tracing::trace!("new commit position: {}", self.committed_index);

            while let Some(result) = state.apply() {
                let index = state.last_applied();
                tracing::trace!("applied: {}", index);
                let _ = self.tx_applied.send(Ok(Applied { index, result }));
            }
        }
        Ok(())
    }

    /// Returns `false` if the node cannot stay as a leader anymore, that is,
    /// it fails to connect with the majority of nodes.
    fn handle_replication_failure(&mut self, id: NodeId) -> Result<(), CommitError> {
        self.failed_nodes.insert(id);

        let total_nodes = self.match_indices.len() + 1;
        tracing::trace!(
            "total_nodes: {}, match_indices: {}",
            total_nodes,
            self.match_indices.len()
        );
        if self.failed_nodes.len() > total_nodes / 2 {
            tracing::error!(
                "failed to connect with the majority of nodes: {:?}. stopping commit manager",
                self.failed_nodes,
            );
            Err(CommitError::Isolated(self.failed_nodes.clone()))
        } else {
            Ok(())
        }
    }
}
