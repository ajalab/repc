use crate::error::PeerError;
use crate::log::{Log, LogEntry};
use crate::peer;
use crate::rpc::raft;
use crate::types::{LogIndex, NodeId, Term};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, RwLock};
use tokio::time;

const LEADER_PEER_BUFFER_SIZE: usize = 10;

pub struct Leader {
    tx: mpsc::Sender<()>,
}

impl Leader {
    pub async fn spawn(
        id: NodeId,
        term: Term,
        log: Arc<RwLock<Log>>,
        peers: &HashMap<NodeId, peer::GRPCPeer>,
    ) -> Self {
        let next_index = log.read().await.last_index() + 1;

        let match_index = 0;
        let appenders = peers
            .iter()
            .map(|(_, peer)| Appender::spawn(id, term, next_index, peer.clone(), log.clone()))
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

struct Appender {
    tx: mpsc::Sender<()>,
}

const APPENDER_CHANNEL_BUFFER_SIZE: usize = 10;

impl Appender {
    fn spawn(
        id: NodeId,
        term: Term,
        next_index: LogIndex,
        peer: peer::GRPCPeer,
        log: Arc<RwLock<Log>>,
    ) -> Self {
        let (tx, rx) = mpsc::channel::<()>(APPENDER_CHANNEL_BUFFER_SIZE);
        let appender = Appender { tx };
        let process = AppenderProcess {
            id,
            term,
            next_index,
            match_index: 0,
            peer,
            rx,
            log,
        };

        tokio::spawn(process.run());
        appender
    }
}

struct AppenderProcess {
    id: NodeId,
    term: Term,
    next_index: LogIndex,
    match_index: LogIndex,
    peer: peer::GRPCPeer,
    rx: mpsc::Receiver<()>,

    log: Arc<RwLock<Log>>,
}

const WAIT_NOTIFY_APPEND_TIMEOUT_MILLIS: u64 = 500;
const WAIT_APPEND_ENTRIES_RES_TIMEOUT_MILLIS: u64 = 500;

impl AppenderProcess {
    async fn run(mut self) {
        loop {
            let f = time::timeout(
                Duration::from_millis(WAIT_NOTIFY_APPEND_TIMEOUT_MILLIS),
                self.wait_notify_append(),
            )
            .await;
            if let Ok(None) = f {
                break;
            }

            let res = time::timeout(
                Duration::from_millis(WAIT_APPEND_ENTRIES_RES_TIMEOUT_MILLIS),
                self.append_entries(),
            )
            .await;
        }
    }

    async fn wait_notify_append(&mut self) -> Option<()> {
        self.rx.recv().await
    }

    async fn append_entries(&mut self) -> Result<raft::AppendEntriesResponse, PeerError> {
        let log = self.log.read().await;

        let prev_log_index = self.next_index - 1;
        let prev_log_term = log.get(prev_log_index).map(|e| e.term()).unwrap_or(0);
        let last_committed_index = log.last_committed();

        self.peer
            .append_entries(raft::AppendEntriesRequest {
                leader_id: self.id,
                term: self.term,
                prev_log_index,
                prev_log_term,
                last_committed_index,
                entries: vec![], // TODO
            })
            .await
    }
}
