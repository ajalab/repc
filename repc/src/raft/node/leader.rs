use super::error::CommandError;
use crate::configuration::Configuration;
use crate::configuration::LeaderConfiguration;
use crate::raft::log::{Log, LogEntry};
use crate::raft::pb;
use crate::raft::peer::Peer;
use crate::state_machine::StateMachineManager;
use crate::types::{LogIndex, NodeId, Term};
use bytes::Bytes;
use futures::future;
use futures::{FutureExt, StreamExt};
use log::{debug, warn};
use std::collections::HashMap;
use std::iter;
use std::sync::{Arc, Weak};
use std::time::Duration;
use tokio::sync::{broadcast, mpsc, oneshot, RwLock};
use tokio::time;

struct NotifyAppendToAppender;

pub struct Leader {
    term: Term,
    appenders: Vec<Appender>,
    commit_manager: CommitManager,
    log: Option<Arc<RwLock<Log>>>,
}

impl Leader {
    pub fn spawn<P: Peer + Send + Sync + Clone + 'static>(
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
                debug!("spawn a new appender<{}>", target_id);
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
            log: Some(log),
            appenders,
            commit_manager,
            term,
        };

        leader
    }

    pub async fn handle_command(
        &mut self,
        command: Bytes,
        tx: oneshot::Sender<Result<(), CommandError>>,
    ) {
        let index = {
            let mut log = self.log.as_ref().unwrap().write().await;
            log.append(iter::once(LogEntry::new(self.term, command)));
            log.last_index()
        };

        for appender in &mut self.appenders {
            let _ = appender.try_notify();
        }

        let subscription = self.commit_manager.subscribe();
        tokio::spawn(subscription.wait_applied(index).map(|e| tx.send(e)));
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
    fn spawn<P: Peer + Send + Sync + 'static>(
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
            warn!("{}", e);
        }

        tokio::spawn(process.run());
        appender
    }

    fn try_notify(&mut self) -> Result<(), mpsc::error::TrySendError<NotifyAppendToAppender>> {
        self.tx.try_send(NotifyAppendToAppender {})
    }
}

struct AppenderProcess<P: Peer> {
    id: NodeId,
    term: Term,
    conf: Arc<LeaderConfiguration>,
    target_id: NodeId,
    peer: P,
    rx: mpsc::Receiver<NotifyAppendToAppender>,
    commit_manager_notifier: CommitManagerNotifier,
    log: Weak<RwLock<Log>>,
}

impl<P: Peer> AppenderProcess<P> {
    async fn run(mut self) {
        debug!("start appender process: target_id={}", self.target_id);

        let mut next_index = {
            let log = self.log.upgrade();
            let log = log.unwrap();
            let log = log.read().await;
            log.last_index() + 1
        };
        let mut match_index = 0;

        loop {
            debug!(
                "id={}, term={}, target_id={}, message=\"wait for heartbeat timeout or notification\"",
                self.id, self.term, self.target_id,
            );
            let wait = time::timeout(
                Duration::from_millis(self.conf.heartbeat_timeout_millis),
                self.rx.recv(),
            )
            .await;
            match wait {
                Ok(None) => break,
                Ok(Some(_)) => {}
                Err(_) => {
                    debug!("timeout. send heartbeat");
                }
            }
            if let Ok(None) = wait {
                break;
            }

            let log = match self.log.upgrade() {
                Some(log) => log,
                None => {
                    debug!("old");
                    break;
                }
            };

            let log = log.read().await;

            let prev_log_index = next_index - 1;
            let prev_log_term = log.get(prev_log_index).map(|e| e.term()).unwrap_or(0);
            let last_committed_index = log.last_committed();
            drop(log);

            let entries: Vec<pb::LogEntry> = vec![];
            let append_entries = self.peer.append_entries(pb::AppendEntriesRequest {
                leader_id: self.id,
                term: self.term,
                prev_log_index,
                prev_log_term,
                last_committed_index,
                entries: vec![], // TODO
            });

            let res = time::timeout(
                Duration::from_millis(self.conf.wait_append_entries_response_timeout_millis),
                append_entries,
            )
            .await;

            if let Ok(Ok(res)) = res {
                if res.success {
                    match_index = prev_log_index + (entries.len() as LogIndex);
                    if let Err(e) = self
                        .commit_manager_notifier
                        .notify_replicated(self.target_id, match_index)
                        .await
                    {
                        log::warn!("match_index update is not reported to CommitManager: {}", e);
                    }
                }
            }
        }
    }
}

#[derive(Clone)]
struct Applied {
    index: LogIndex,
}

impl From<LogIndex> for Applied {
    fn from(index: LogIndex) -> Self {
        Applied { index }
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
    async fn wait_applied(self, index: LogIndex) -> Result<(), CommandError> {
        let stream = self.rx.into_stream();
        tokio::pin!(stream);

        stream
            .filter_map(|applied| {
                future::ready(match applied {
                    Ok(applied) if applied.index >= index => Some(Ok(())),
                    Err(broadcast::RecvError::Closed) => Some(Err(CommandError::NotLeader)),
                    Err(broadcast::RecvError::Lagged(n)) => {
                        log::warn!("commit notification is lagging: {}", n);
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
        let mut committed = LogIndex::default();
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
                        log::warn!("failed to acquire strong log reference");
                        break;
                    }
                };
                let log = log.read().await;
                let entries = log.get_range(majority + 1, committed);
                for entry in entries {
                    let command = entry.command();
                    if let Err(e) = self.sm_manager.apply(command).await {
                        log::warn!("failed to apply command to the state machine: {}", e);
                        break;
                    }
                }
                if let Err(_) = self.tx_applied.send(committed.into()) {
                    log::info!("leader process has been terminated");
                    break;
                }
                committed = majority;
            }
        }
    }
}
