use super::{error::CommitError, message::Applied};
use crate::{
    log::{Log, LogIndex},
    raft::node::error::CommandError,
    state::State,
    state_machine::StateMachine,
    types::Term,
};
use bytes::Bytes;
use futures::{future, StreamExt};
use repc_common::types::NodeId;
use std::{
    collections::{HashMap, HashSet},
    sync::Weak,
};
use tokio::sync::{broadcast, mpsc, RwLock};
use tokio_stream::wrappers::{errors::BroadcastStreamRecvError, BroadcastStream};
use tracing::Instrument;

pub struct CommitManager {
    tx_applied: broadcast::Sender<Result<Applied, CommitError>>,
    rx_applied: broadcast::Receiver<Result<Applied, CommitError>>,
}

impl CommitManager {
    pub fn spawn<S, L>(
        id: NodeId,
        term: Term,
        nodes: impl Iterator<Item = NodeId>,
        state: Weak<RwLock<State<S, L>>>,
    ) -> (Self, CommitManagerNotifier)
    where
        S: StateMachine + Send + Sync + 'static,
        L: Log + Send + Sync + 'static,
    {
        let (tx_applied, rx_applied) = broadcast::channel(100);
        let (tx_replicated, rx_replicated) = mpsc::channel(100);

        let process =
            CommitManagerProcess::new(id, term, tx_applied.clone(), rx_replicated, nodes, state);
        tokio::spawn(process.run());

        let commit_manager = CommitManager {
            tx_applied,
            rx_applied,
        };
        let commit_manager_notifier = CommitManagerNotifier { tx: tx_replicated };

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
        let stream = BroadcastStream::new(self.rx);
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
                    Ok(Err(e)) => Some(Err(CommandError::CommitError(e))),
                    Err(BroadcastStreamRecvError::Lagged(n)) => {
                        tracing::warn!("commit notification is lagging: {}", n);
                        None
                    }
                    _ => None,
                })
            })
            .next()
            .await
            .unwrap_or_else(|| Err(CommandError::CommitAborted))
    }
}

#[derive(Clone)]
pub struct CommitManagerNotifier {
    tx: mpsc::Sender<(NodeId, Result<LogIndex, ()>)>,
}

pub struct CommitManagerNotifierError;

impl CommitManagerNotifier {
    pub async fn notify_success(
        &mut self,
        id: NodeId,
        index: LogIndex,
    ) -> Result<(), CommitManagerNotifierError> {
        self.tx
            .send((id, Ok(index)))
            .await
            .map_err(|_| CommitManagerNotifierError)
    }

    pub async fn notify_failed(&mut self, id: NodeId) -> Result<(), CommitManagerNotifierError> {
        self.tx
            .send((id, Err(())))
            .await
            .map_err(|_| CommitManagerNotifierError)
    }
}

struct CommitManagerProcess<S, L> {
    id: NodeId,
    term: Term,
    tx_applied: broadcast::Sender<Result<Applied, CommitError>>,
    rx_replicated: mpsc::Receiver<(NodeId, Result<LogIndex, ()>)>,
    match_indices: HashMap<NodeId, LogIndex>,
    state: Weak<RwLock<State<S, L>>>,
    committed_index: LogIndex,
    failed_nodes: HashSet<NodeId>,
}

impl<S, L> CommitManagerProcess<S, L>
where
    S: StateMachine,
    L: Log,
{
    fn new(
        id: NodeId,
        term: Term,
        tx_applied: broadcast::Sender<Result<Applied, CommitError>>,
        rx_replicated: mpsc::Receiver<(NodeId, Result<LogIndex, ()>)>,
        nodes: impl Iterator<Item = NodeId>,
        state: Weak<RwLock<State<S, L>>>,
    ) -> Self {
        let match_indices = nodes.zip(std::iter::repeat(LogIndex::default())).collect();
        Self {
            id,
            term,
            tx_applied,
            rx_replicated,
            match_indices,
            state,
            committed_index: 0,
            failed_nodes: HashSet::new(),
        }
    }

    async fn run(mut self) {
        self.committed_index = {
            let state = match self.state.upgrade() {
                Some(state) => state,
                None => {
                    tracing::info!("failed to acquire strong reference to the state; likely not being a leader anymore");
                    return;
                }
            };
            let state = state.as_ref().read().await;
            state.last_committed()
        };

        let span = tracing::info_span!(
            target: "leader::commit_manager",
            "commit_manager",
            id = self.id,
            term = self.term.get(),
        );
        async {
            tracing::info!("started commit manager");
            while let Some((id, result)) = self.rx_replicated.recv().await {
                let span =
                    tracing::trace_span!(target: "leader::commit_manager", "handle_replication");
                if let Err(e) = self.handle_replication(id, result).instrument(span).await {
                    let _ = self.tx_applied.send(Err(e));
                    break;
                }
            }
        }
        .instrument(span)
        .await;
    }

    async fn handle_replication(
        &mut self,
        id: NodeId,
        result: Result<LogIndex, ()>,
    ) -> Result<(), CommitError> {
        match result {
            Ok(index) => self.handle_replication_success(id, index).await,
            Err(_) => self.handle_replication_failure(id),
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

            while let Some(result) = state.apply().await {
                let index = state.last_applied();
                tracing::trace!("applied: {}", index);
                let _ = self.tx_applied.send(Ok(Applied { index, result }));
            }
        }
        Ok(())
    }

    /// Returns `false` if the node cannot stay as a leader anymore, that is,
    /// it fails to connect to the majority of nodes.
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
                "failed to replicate data to the majority of nodes: {:?}. stopping commit manager",
                self.failed_nodes,
            );
            Err(CommitError::Isolated(self.failed_nodes.clone()))
        } else {
            Ok(())
        }
    }
}
