use super::error::CommandError;
use crate::configuration::Configuration;
use crate::configuration::LeaderConfiguration;
use crate::raft::pb;
use crate::raft::peer::RaftPeer;
use crate::state::log::{Log, LogEntry};
use crate::state::state_machine::StateMachineManager;
use crate::state::Command;
use crate::types::{LogIndex, NodeId, Term};
use bytes::Bytes;
use futures::future;
use futures::{FutureExt, StreamExt};
use std::collections::HashMap;
use std::iter;
use std::sync::{Arc, Weak};
use std::time::Duration;
use tokio::sync::{broadcast, mpsc, oneshot, RwLock};
use tokio::time;

struct NotifyAppendToAppender;

pub struct Leader {
    id: NodeId,
    term: Term,
    appenders: Vec<Appender>,
    commit_manager: CommitManager,
    log: Option<Arc<RwLock<Log>>>,
}

impl Leader {
    pub fn spawn<P: RaftPeer + Send + Sync + Clone + 'static>(
        id: NodeId,
        conf: Arc<Configuration>,
        term: Term,
        log: Log,
        peers: &HashMap<NodeId, P>,
        sm_manager: StateMachineManager,
    ) -> Self {
        let leader_conf = Arc::new(conf.leader.clone());
        let log = Arc::new(RwLock::new(log));

        let nodes = iter::once(id).chain(peers.keys().copied());
        let (commit_manager, commit_manager_notifier) =
            CommitManager::spawn(nodes, Arc::downgrade(&log), sm_manager);

        let appenders = peers
            .iter()
            .map(|(&target_id, peer)| {
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
                    Arc::downgrade(&log),
                    peer.clone(),
                )
            })
            .collect::<Vec<_>>();

        let leader = Leader {
            id,
            term,
            appenders,
            commit_manager,
            log: Some(log),
        };

        leader
    }

    pub async fn handle_command(
        &mut self,
        command: Command,
        tx: oneshot::Sender<Result<Bytes, CommandError>>,
    ) {
        let index = {
            let mut log = self.log.as_ref().unwrap().write().await;
            log.append(iter::once(LogEntry::new(self.term, command)));
            log.last_index()
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

    pub fn extract_log(&mut self) -> Log {
        Arc::try_unwrap(self.log.take().unwrap())
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
    fn spawn<P: RaftPeer + Send + Sync + 'static>(
        id: NodeId,
        term: Term,
        conf: Arc<LeaderConfiguration>,
        target_id: NodeId,
        commit_manager_notifier: CommitManagerNotifier,
        log: Weak<RwLock<Log>>,
        peer: P,
    ) -> Self {
        let (tx, rx) = mpsc::channel::<NotifyAppendToAppender>(APPENDER_CHANNEL_BUFFER_SIZE);
        let mut appender = Appender { tx };
        let process = AppenderProcess {
            id,
            term,
            conf,
            target_id,
            peer,
            commit_manager_notifier,
            log,
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

struct AppenderProcess<P: RaftPeer> {
    id: NodeId,
    term: Term,
    conf: Arc<LeaderConfiguration>,
    target_id: NodeId,
    peer: P,
    rx: mpsc::Receiver<NotifyAppendToAppender>,
    commit_manager_notifier: CommitManagerNotifier,
    log: Weak<RwLock<Log>>,
}

impl<P: RaftPeer> AppenderProcess<P> {
    async fn run(mut self) {
        tracing::debug!(
            id = self.id,
            term = self.term,
            target_id = self.target_id,
            "start appender process",
        );

        let mut next_index = {
            let log = self.log.upgrade();
            let log = log.unwrap();
            let log = log.read().await;
            log.last_index() + 1
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

            let log = match self.log.upgrade() {
                Some(log) => log,
                None => {
                    tracing::info!(
                        id = self.id,
                        term = self.term,
                        target_id = self.target_id,
                        "can't read a log. likely not being a leader anymore",
                    );
                    break;
                }
            };

            let log = log.read().await;

            let prev_log_index = next_index - 1;
            let prev_log_term = log.get(prev_log_index).map(|e| e.term()).unwrap_or(0);
            let last_committed_index = log.last_committed();
            let entries = log
                .iter_at(next_index)
                .map(|entry| {
                    let command = entry.command();
                    pb::LogEntry {
                        term: entry.term(),
                        rpc: command.rpc().clone().into(),
                        body: command.body().as_ref().to_owned(),
                    }
                })
                .collect::<Vec<_>>();
            let n_entries = entries.len();
            drop(log);

            let append_entries = self.peer.append_entries(pb::AppendEntriesRequest {
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

#[derive(Clone)]
struct Applied {
    index: LogIndex,
    res: Bytes,
    metadata: tonic::metadata::MetadataMap,
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
    fn spawn(
        nodes: impl Iterator<Item = NodeId>,
        log: Weak<RwLock<Log>>,
        sm_manager: StateMachineManager,
    ) -> (Self, CommitManagerNotifier) {
        let (tx_applied, rx_applied) = broadcast::channel(100);
        let (tx_replicated, rx_replicated) = mpsc::channel(100);

        let process =
            CommitManagerProcess::new(tx_applied.clone(), rx_replicated, nodes, log, sm_manager);
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
    async fn wait_applied(self, index: LogIndex) -> Result<Bytes, CommandError> {
        let stream = self.rx.into_stream();
        tokio::pin!(stream);

        stream
            .filter_map(|applied| {
                future::ready(match applied {
                    Ok(applied) if applied.index >= index => Some(Ok(applied.res)),
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

struct CommitManagerProcess {
    tx_applied: broadcast::Sender<Applied>,
    rx_replicated: mpsc::Receiver<Replicated>,
    match_indices: HashMap<NodeId, LogIndex>,
    log: Weak<RwLock<Log>>,
    sm_manager: StateMachineManager,
}

impl CommitManagerProcess {
    fn new(
        tx_applied: broadcast::Sender<Applied>,
        rx_replicated: mpsc::Receiver<Replicated>,
        nodes: impl Iterator<Item = NodeId>,
        log: Weak<RwLock<Log>>,
        sm_manager: StateMachineManager,
    ) -> Self {
        let match_indices = nodes.zip(iter::repeat(LogIndex::default())).collect();
        Self {
            tx_applied,
            rx_replicated,
            match_indices,
            log,
            sm_manager,
        }
    }

    async fn run(mut self) {
        let mut committed = 0;
        while let Some(replicated) = self.rx_replicated.recv().await {
            let s = self.match_indices.get_mut(&replicated.id).unwrap();
            *s = replicated.index;

            let mut indices = self.match_indices.values().collect::<Vec<_>>();
            indices.sort_by(|a, b| b.cmp(a));

            let majority = *indices[indices.len() / 2];
            if majority > committed {
                let log = match self.log.upgrade() {
                    Some(log) => log,
                    None => {
                        tracing::info!(
                            "failed to acquire strong log reference; likely not being a leader anymore");
                        break;
                    }
                };
                let log = log.read().await;
                let entries = log.get_range(committed + 1, majority);
                tracing::trace!(
                    "start committing {} entries: {}..={}",
                    entries.len(),
                    committed + 1,
                    majority,
                );
                for entry in entries {
                    let command = entry.command().clone();
                    let mut res = match self.sm_manager.apply(command).await {
                        Ok(res) => res,
                        Err(e) => {
                            tracing::error!("failed to apply command to the state machine: {}", e);
                            break;
                        }
                    };
                    let metadata = std::mem::take(res.metadata_mut());
                    committed += 1;
                    let applied = Applied {
                        index: committed,
                        res: res.into_inner(),
                        metadata,
                    };
                    if let Err(_) = self.tx_applied.send(applied) {
                        tracing::info!("leader process has been terminated");
                        break;
                    }
                }
                tracing::trace!(
                    "finished committing {} entries: committed = {}",
                    entries.len(),
                    committed,
                );
            }
        }
    }
}
