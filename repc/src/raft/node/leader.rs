use super::error::CommandError;
use crate::configuration::Configuration;
use crate::configuration::LeaderConfiguration;
use crate::pb::raft::raft_client::RaftClient;
use crate::pb::raft::AppendEntriesRequest;
use crate::pb::raft::LogEntry as PbLogEntry;
use crate::state::error::StateMachineError;
use crate::state::log::{LogEntry, LogIndex};
use crate::state::StateMachine;
use crate::state::{Command, State};
use crate::types::{NodeId, Term};
use bytes::Bytes;
use futures::future;
use futures::{FutureExt, StreamExt};
use std::collections::HashMap;
use std::iter;
use std::sync::{Arc, Weak};
use std::time::Duration;
use tokio::sync::{broadcast, mpsc, oneshot, RwLock};
use tokio::time;
use tonic::body::BoxBody;
use tonic::client::GrpcService;
use tonic::codegen::StdError;

struct NotifyAppendToAppender;

pub struct Leader<S> {
    id: NodeId,
    term: Term,
    appenders: Vec<Appender>,
    commit_manager: CommitManager,
    state: Option<Arc<RwLock<State<S>>>>,
}

impl<S> Leader<S>
where
    S: StateMachine + Send + Sync + 'static,
{
    pub fn spawn<T>(
        id: NodeId,
        conf: Arc<Configuration>,
        term: Term,
        state: State<S>,
        clients: &HashMap<NodeId, RaftClient<T>>,
    ) -> Self
    where
        T: GrpcService<BoxBody> + Clone + Send + Sync + 'static,
        T::Future: Send,
        <T::ResponseBody as http_body::Body>::Error: Into<StdError> + Send,
    {
        let leader_conf = Arc::new(conf.leader.clone());
        let state = Arc::new(RwLock::new(state));

        let nodes = iter::once(id).chain(clients.keys().copied());
        let (commit_manager, commit_manager_notifier) =
            CommitManager::spawn(nodes, Arc::downgrade(&state));

        let appenders = clients
            .iter()
            .map(|(&target_id, client)| {
                tracing::debug!(
                    id,
                    term,
                    target_id,
                    "spawn a new appender for {}",
                    target_id
                );
                Appender::spawn(
                    id,
                    term,
                    leader_conf.clone(),
                    target_id,
                    commit_manager_notifier.clone(),
                    Arc::downgrade(&state),
                    client.clone(),
                )
            })
            .collect::<Vec<_>>();

        let leader = Leader {
            id,
            term,
            appenders,
            commit_manager,
            state: Some(state),
        };

        leader
    }
}

impl<S> Leader<S>
where
    S: StateMachine,
{
    pub async fn handle_command(
        &mut self,
        command: Command,
        tx: oneshot::Sender<Result<tonic::Response<Bytes>, CommandError>>,
    ) {
        let index = {
            let mut state = self.state.as_ref().unwrap().write().await;
            state.append_log_entries(iter::once(LogEntry::new(self.term, command)));
            state.last_index()
        };

        tracing::trace!(
            id = self.id,
            term = self.term,
            "wrote a command at log index {}",
            index
        );

        for appender in &mut self.appenders {
            let _ = appender.try_notify();
        }

        let subscription = self.commit_manager.subscribe();
        tokio::spawn(
            subscription
                .wait_applied(index)
                .map(|result| tx.send(result)),
        );
    }

    pub fn extract_state(&mut self) -> State<S> {
        Arc::try_unwrap(self.state.take().unwrap())
            .ok()
            .expect("should have")
            .into_inner()
    }
}

struct Appender {
    tx: mpsc::Sender<NotifyAppendToAppender>,
}

const APPENDER_CHANNEL_BUFFER_SIZE: usize = 10;

impl Appender {
    fn spawn<S, T>(
        id: NodeId,
        term: Term,
        conf: Arc<LeaderConfiguration>,
        target_id: NodeId,
        commit_manager_notifier: CommitManagerNotifier,
        state: Weak<RwLock<State<S>>>,
        client: RaftClient<T>,
    ) -> Self
    where
        S: StateMachine + Send + Sync + 'static,
        T: GrpcService<BoxBody> + Send + Sync + 'static,
        T::Future: Send,
        <T::ResponseBody as http_body::Body>::Error: Into<StdError> + Send,
    {
        let (tx, rx) = mpsc::channel::<NotifyAppendToAppender>(APPENDER_CHANNEL_BUFFER_SIZE);
        let mut appender = Appender { tx };
        let process = AppenderProcess {
            id,
            term,
            conf,
            target_id,
            client,
            commit_manager_notifier,
            state,
            rx,
        };

        // Notify to appender beforehand to send heartbeat immediately
        if let Err(e) = appender.try_notify() {
            tracing::warn!(
                id,
                term,
                target_id,
                "failed to notify appender[{}]: {}",
                target_id,
                e
            );
        }

        tokio::spawn(process.run());
        appender
    }

    fn try_notify(&mut self) -> Result<(), mpsc::error::TrySendError<NotifyAppendToAppender>> {
        self.tx.try_send(NotifyAppendToAppender {})
    }
}

struct AppenderProcess<S, T> {
    id: NodeId,
    term: Term,
    conf: Arc<LeaderConfiguration>,
    target_id: NodeId,
    client: RaftClient<T>,
    rx: mpsc::Receiver<NotifyAppendToAppender>,
    commit_manager_notifier: CommitManagerNotifier,
    state: Weak<RwLock<State<S>>>,
}

impl<S, T> AppenderProcess<S, T>
where
    S: StateMachine,
    T: GrpcService<BoxBody>,
    T::ResponseBody: Send + 'static,
    <T::ResponseBody as http_body::Body>::Error: Into<StdError> + Send,
{
    async fn run(mut self) {
        tracing::debug!(
            id = self.id,
            term = self.term,
            target_id = self.target_id,
            "start appender process",
        );

        let mut next_index = {
            let state = self.state.upgrade();
            let state = state.unwrap();
            let state = state.read().await;
            state.log().last_index() + 1
        };
        let mut match_index;

        loop {
            tracing::trace!(
                id = self.id,
                term = self.term,
                target_id = self.target_id,
                "wait for heartbeat timeout or notification",
            );
            let wait = time::timeout(
                Duration::from_millis(self.conf.heartbeat_timeout_millis),
                self.rx.recv(),
            )
            .await;
            match wait {
                Ok(None) => break,
                Ok(Some(_)) => {
                    tracing::trace!(
                        id = self.id,
                        term = self.term,
                        target_id = self.target_id,
                        "notified to send new commands or initial heartbeat",
                    );
                }
                Err(_) => {
                    tracing::trace!(
                        id = self.id,
                        term = self.term,
                        target_id = self.target_id,
                        "timeout. send heartbeat",
                    );
                }
            }
            if let Ok(None) = wait {
                break;
            }

            let state = match self.state.upgrade() {
                Some(state) => state,
                None => {
                    tracing::info!(
                        id = self.id,
                        term = self.term,
                        target_id = self.target_id,
                        "can't read the state. likely not being a leader anymore",
                    );
                    break;
                }
            };
            let state = state.read().await;
            let log = state.log();

            let prev_log_index = next_index - 1;
            let prev_log_term = log.get(prev_log_index).map(|e| e.term()).unwrap_or(0);
            let last_committed_index = log.last_committed();
            let entries = log
                .iter_at(next_index)
                .map(|entry| {
                    let command = entry.command();
                    PbLogEntry {
                        term: entry.term(),
                        rpc: command.rpc().clone().into(),
                        body: command.body().as_ref().to_owned(),
                    }
                })
                .collect::<Vec<_>>();
            let n_entries = entries.len();
            drop(log);

            let append_entries = self.client.append_entries(AppendEntriesRequest {
                leader_id: self.id,
                term: self.term,
                prev_log_index,
                prev_log_term,
                last_committed_index,
                entries,
            });

            let res = time::timeout(
                Duration::from_millis(self.conf.wait_append_entries_response_timeout_millis),
                append_entries,
            )
            .await;

            if let Ok(Ok(res)) = res {
                let res = res.into_inner();
                if res.success {
                    match_index = prev_log_index + (n_entries as LogIndex);
                    tracing::trace!(
                        id = self.id,
                        term = self.term,
                        target_id = self.target_id,
                        "AppendRequest succeeded. updating match_index to {}",
                        match_index,
                    );
                    if let Err(e) = self
                        .commit_manager_notifier
                        .notify_replicated(self.target_id, match_index)
                        .await
                    {
                        tracing::warn!(
                            id = self.id,
                            term = self.term,
                            target_id = self.target_id,
                            "match_index update is not reported to CommitManager: {}",
                            e
                        );
                    }
                }
            }
        }
    }
}

struct Applied {
    index: LogIndex,
    result: Result<tonic::Response<Bytes>, StateMachineError>,
}

impl Clone for Applied {
    fn clone(&self) -> Self {
        let result = match &self.result {
            Ok(res) => {
                let mut res_clone = tonic::Response::new(res.get_ref().clone());
                *res_clone.metadata_mut() = res.metadata().clone();
                Ok(res_clone)
            }
            Err(s) => Err(s.clone()),
        };
        Self {
            index: self.index,
            result,
        }
    }
}

struct Replicated {
    index: LogIndex,
    id: NodeId,
}

struct CommitManager {
    tx_applied: broadcast::Sender<Applied>,
    rx_applied: broadcast::Receiver<Applied>,
}

impl CommitManager {
    fn spawn<S>(
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

    fn subscribe(&mut self) -> CommitManagerSubscription {
        CommitManagerSubscription {
            rx: std::mem::replace(&mut self.rx_applied, self.tx_applied.subscribe()),
        }
    }
}

struct CommitManagerSubscription {
    rx: broadcast::Receiver<Applied>,
}

impl CommitManagerSubscription {
    async fn wait_applied(self, index: LogIndex) -> Result<tonic::Response<Bytes>, CommandError> {
        let stream = self.rx.into_stream();
        tokio::pin!(stream);

        stream
            .filter_map(|applied| {
                future::ready(match applied {
                    Ok(applied) if applied.index >= index => {
                        Some(applied.result.map_err(CommandError::StateMachineError))
                    }
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
struct CommitManagerNotifier {
    tx_replicated: mpsc::Sender<Replicated>,
}

impl CommitManagerNotifier {
    async fn notify_replicated(
        &mut self,
        id: NodeId,
        index: LogIndex,
    ) -> Result<(), mpsc::error::SendError<Replicated>> {
        self.tx_replicated.send(Replicated { id, index }).await
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
        let match_indices = nodes.zip(iter::repeat(LogIndex::default())).collect();
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
            let s = self.match_indices.get_mut(&replicated.id).unwrap();
            *s = replicated.index;

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
                    let applied = Applied { index, result };
                    if let Err(_) = self.tx_applied.send(applied) {
                        tracing::info!("leader process has been terminated");
                        break;
                    }
                }
            }
        }
    }
}
