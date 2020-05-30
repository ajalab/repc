use crate::configuration::Configuration;
use crate::configuration::LeaderConfiguration;
use crate::log::Log;
use crate::pb;
use crate::peer::Peer;
use crate::types::{LogIndex, NodeId, Term};
use futures::future::try_join_all;
use log::{debug, warn};
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
    log: Arc<Log>,
}

impl Leader {
    pub fn spawn<P: Peer + Send + Sync + Clone + 'static>(
        id: NodeId,
        conf: Arc<Configuration>,
        term: Term,
        log: Arc<Log>,
        peers: &HashMap<NodeId, P>,
    ) -> Self {
        let leader_conf = Arc::new(conf.leader.clone());
        let appenders = peers
            .iter()
            .map(|(&target_id, peer)| {
                debug!("spawn a new appender<{}>", target_id);
                Appender::spawn(id, term, leader_conf.clone(), target_id, peer.clone())
            })
            .collect::<Vec<_>>();

        let (tx, rx) = mpsc::channel::<NotifyAppendToLeader>(LEADER_PEER_BUFFER_SIZE);
        let leader = Leader { tx, log };
        let process = LeaderProcess {
            rx,
            appenders,
            conf: leader_conf,
        };

        tokio::spawn(process.run());

        leader
    }
}

struct LeaderProcess {
    // log: Arc<RwLock<Log>>,
    conf: Arc<LeaderConfiguration>,
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
        debug!("leader process is terminated");
    }
}

struct Appender {
    target_id: NodeId,
    tx: mpsc::Sender<NotifyAppendToAppender>,
}

const APPENDER_CHANNEL_BUFFER_SIZE: usize = 10;

impl Appender {
    fn spawn<P: Peer + Send + Sync + 'static>(
        id: NodeId,
        term: Term,
        conf: Arc<LeaderConfiguration>,
        target_id: NodeId,
        peer: P,
    ) -> Self {
        let (tx, rx) = mpsc::channel::<NotifyAppendToAppender>(APPENDER_CHANNEL_BUFFER_SIZE);
        let mut appender = Appender { target_id, tx };
        let process = AppenderProcess::new(id, term, conf, target_id, peer, rx);

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
    conf: Arc<LeaderConfiguration>,
    target_id: NodeId,
    peer: P,
    rx: mpsc::Receiver<NotifyAppendToAppender>,

    next_index: LogIndex,
    match_index: LogIndex,
    // log: Arc<RwLock<Log>>,
}

const WAIT_APPEND_ENTRIES_RES_TIMEOUT_MILLIS: u64 = 500;

impl<P: Peer> AppenderProcess<P> {
    fn new(
        id: NodeId,
        term: Term,
        conf: Arc<LeaderConfiguration>,
        target_id: NodeId,
        peer: P,
        rx: mpsc::Receiver<NotifyAppendToAppender>,
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
        }
    }

    async fn run(mut self) {
        debug!("start appender process: target_id={}", self.target_id);

        self.next_index = {
            // let log = self.log.read().await;
            // log.last_index() + 1
            1
        };
        self.match_index = 0;

        loop {
            debug!(
                "id={}, term={}, message=\"wait for heartbeat timeout or notification\"",
                self.id, self.term,
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

            let log = Log::default(); // self.log.read().await;

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
