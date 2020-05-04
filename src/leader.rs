use crate::configuration::Configuration;
use crate::log::Log;
use crate::pb;
use crate::peer::Peer;
use crate::types::{LogIndex, NodeId, Term};
use futures::future::try_join_all;
use log::{debug, info, warn};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, RwLock};
use tokio::time;

const LEADER_PEER_BUFFER_SIZE: usize = 10;

struct NotifyAppendToAppender;
struct NotifyAppendToLeader;

pub struct Leader {
    tx: mpsc::Sender<NotifyAppendToLeader>,
}

impl Drop for Leader {
    fn drop(&mut self) {
        debug!("dropping leader");
    }
}

impl Leader {
    pub fn spawn<P: Peer + Send + Sync + Clone + 'static>(
        id: NodeId,
        term: Term,
        conf: Arc<Configuration>,
        log: Arc<RwLock<Log>>,
        peers: &HashMap<NodeId, P>,
    ) -> Self {
        let appenders = peers
            .iter()
            .map(|(&target_id, peer)| {
                debug!("spawn a new appender<{}>", target_id);
                Appender::spawn(id, term, conf.clone(), target_id, peer.clone(), log.clone())
            })
            .collect::<Vec<_>>();

        let (tx, rx) = mpsc::channel::<NotifyAppendToLeader>(LEADER_PEER_BUFFER_SIZE);
        let leader = Leader { tx };
        let process = LeaderProcess {
            log,
            rx,
            appenders,
            conf: conf,
        };

        tokio::spawn(process.run());

        leader
    }
}

struct LeaderProcess {
    log: Arc<RwLock<Log>>,
    conf: Arc<Configuration>,
    rx: mpsc::Receiver<NotifyAppendToLeader>,
    appenders: Vec<Appender>,
}

impl LeaderProcess {
    async fn run(mut self) {
        loop {
            if self.rx.recv().await.is_none() {
                break;
            }
            if let Err(e) = try_join_all(self.appenders.iter_mut().map(|a| a.notify())).await {
                warn!("error: {}", e);
            }
        }
        info!("leader process is terminated");
    }
}

#[derive(Clone)]
struct Appender {
    target_id: NodeId,
    tx: mpsc::Sender<NotifyAppendToAppender>,
}

const APPENDER_CHANNEL_BUFFER_SIZE: usize = 10;

impl Appender {
    fn spawn<P: Peer + Send + Sync + 'static>(
        id: NodeId,
        term: Term,
        conf: Arc<Configuration>,
        target_id: NodeId,
        peer: P,
        log: Arc<RwLock<Log>>,
    ) -> Self {
        let (tx, rx) = mpsc::channel::<NotifyAppendToAppender>(APPENDER_CHANNEL_BUFFER_SIZE);
        let mut appender = Appender { target_id, tx };
        let process = AppenderProcess::new(id, term, conf, target_id, peer, rx, log);

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

    async fn notify(&mut self) -> Result<(), mpsc::error::SendError<NotifyAppendToAppender>> {
        debug!("notify to appender<{}>", self.target_id);
        self.tx.send(NotifyAppendToAppender {}).await
    }
}

struct AppenderProcess<P: Peer> {
    id: NodeId,
    term: Term,
    conf: Arc<Configuration>,
    target_id: NodeId,
    peer: P,
    rx: mpsc::Receiver<NotifyAppendToAppender>,

    next_index: LogIndex,
    match_index: LogIndex,

    log: Arc<RwLock<Log>>,
}

const WAIT_APPEND_ENTRIES_RES_TIMEOUT_MILLIS: u64 = 500;

impl<P: Peer> AppenderProcess<P> {
    fn new(
        id: NodeId,
        term: Term,
        conf: Arc<Configuration>,
        target_id: NodeId,
        peer: P,
        rx: mpsc::Receiver<NotifyAppendToAppender>,
        log: Arc<RwLock<Log>>,
    ) -> Self {
        AppenderProcess {
            id,
            term,
            conf,
            target_id,
            peer,
            rx,
            next_index: 0,
            match_index: 0,

            log,
        }
    }

    async fn run(mut self) {
        info!("start appender process: target_id={}", self.target_id);

        self.next_index = {
            let log = self.log.read().await;
            log.last_index() + 1
        };
        self.match_index = 0;

        loop {
            debug!(
                "id={}, term={}, message=\"wait for heartbeat timeout or notification\"",
                self.id, self.term,
            );
            let wait = time::timeout(
                Duration::from_millis(self.conf.leader.heartbeat_timeout_millis),
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

            let log = self.log.read().await;

            let prev_log_index = self.next_index - 1;
            let prev_log_term = log.get(prev_log_index).map(|e| e.term()).unwrap_or(0);
            let last_committed_index = log.last_committed();
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
                Duration::from_millis(WAIT_APPEND_ENTRIES_RES_TIMEOUT_MILLIS),
                append_entries,
            )
            .await;

            if let Ok(Ok(res)) = res {
                if res.success {
                    self.match_index = prev_log_index + (entries.len() as LogIndex);
                }
            }
        }
    }
}
