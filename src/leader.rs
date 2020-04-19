use crate::grpc;
use crate::log::Log;
use crate::peer;
use crate::types::{LogIndex, NodeId, Term};
use log::{debug, info, warn};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, RwLock};
use tokio::time;

const LEADER_PEER_BUFFER_SIZE: usize = 10;

struct NotifyAppend;

pub struct Leader {
    tx: mpsc::Sender<()>,
}

impl Leader {
    pub fn spawn(
        id: NodeId,
        term: Term,
        log: Arc<RwLock<Log>>,
        peers: &HashMap<NodeId, peer::GRPCPeer>,
    ) -> Self {
        let appenders = peers
            .iter()
            .map(|(&target_id, peer)| {
                debug!("spawn a new appender<{}>", target_id);
                Appender::spawn(id, term, target_id, peer.clone(), log.clone())
            })
            .collect::<Vec<_>>();

        let (tx, rx) = mpsc::channel::<()>(LEADER_PEER_BUFFER_SIZE);
        let leader = Leader { tx };
        let process = LeaderProcess { log, rx, appenders };

        tokio::spawn(process.run());

        leader
    }
}

struct LeaderProcess {
    log: Arc<RwLock<Log>>,
    rx: mpsc::Receiver<()>,
    appenders: Vec<Appender>,
}

impl LeaderProcess {
    async fn run(mut self) {}
}

#[derive(Clone)]
struct Appender {
    target_id: NodeId,
    tx: mpsc::Sender<NotifyAppend>,
}

const APPENDER_CHANNEL_BUFFER_SIZE: usize = 10;

impl Appender {
    fn spawn(
        id: NodeId,
        term: Term,
        target_id: NodeId,
        peer: peer::GRPCPeer,
        log: Arc<RwLock<Log>>,
    ) -> Self {
        let (tx, rx) = mpsc::channel::<NotifyAppend>(APPENDER_CHANNEL_BUFFER_SIZE);
        let appender = Appender { target_id, tx };
        let process = AppenderProcess::new(id, term, target_id, peer, rx, log);

        let mut a = appender.clone();
        tokio::spawn(async move {
            // Notify to appender beforehand to send heartbeat immediately
            if let Err(e) = a.notify().await {
                warn!("{}", e);
            };

            process.run().await;
        });
        appender
    }

    async fn notify(&mut self) -> Result<(), mpsc::error::SendError<NotifyAppend>> {
        debug!("notify to appender<{}>", self.target_id);
        self.tx.send(NotifyAppend {}).await
    }
}

struct AppenderProcess {
    id: NodeId,
    term: Term,
    target_id: NodeId,
    peer: peer::GRPCPeer,
    rx: mpsc::Receiver<NotifyAppend>,

    next_index: LogIndex,
    match_index: LogIndex,

    log: Arc<RwLock<Log>>,
}

const WAIT_NOTIFY_APPEND_TIMEOUT_MILLIS: u64 = 500;
const WAIT_APPEND_ENTRIES_RES_TIMEOUT_MILLIS: u64 = 500;

impl AppenderProcess {
    fn new(
        id: NodeId,
        term: Term,
        target_id: NodeId,
        peer: peer::GRPCPeer,
        rx: mpsc::Receiver<NotifyAppend>,
        log: Arc<RwLock<Log>>,
    ) -> Self {
        AppenderProcess {
            id,
            term,
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
                Duration::from_millis(WAIT_NOTIFY_APPEND_TIMEOUT_MILLIS),
                self.wait_notify_append(),
            )
            .await;
            if let Ok(None) = wait {
                break;
            }

            let log = self.log.read().await;

            let prev_log_index = self.next_index - 1;
            let prev_log_term = log.get(prev_log_index).map(|e| e.term()).unwrap_or(0);
            let last_committed_index = log.last_committed();
            let entries: Vec<grpc::LogEntry> = vec![];
            let append_entries = self.peer.append_entries(grpc::AppendEntriesRequest {
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

    async fn wait_notify_append(&mut self) -> Option<NotifyAppend> {
        self.rx.recv().await
    }
}
