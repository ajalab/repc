use crate::log::Log;
use crate::peer;
use crate::rpc::raft;
use crate::types::{LogIndex, NodeId, Term};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{mpsc, RwLock};
use tokio::time::{timeout, Duration};

const HEARTBEAT_WAIT_MS: u64 = 1000;

pub struct ReplicationManager {
    notifiers: Vec<Notifier>,
}

impl ReplicationManager {
    pub async fn run(
        log: Arc<RwLock<Log>>,
        term: Term,
        peers: &HashMap<NodeId, peer::GRPCPeer>,
    ) -> Self {
        let next_index = log.read().await.last_index() + 1;
        let match_index = 0;

        let notifiers = peers
            .iter()
            .map(|(&id, peer)| {
                Replicator::run(id, term, peer.clone(), next_index, match_index, log.clone())
            })
            .collect::<Vec<_>>();

        ReplicationManager { notifiers }
    }

    pub fn notify(&mut self) {
        for notifier in &mut self.notifiers {
            notifier.notify();
        }
    }
}

#[derive(Clone)]
struct Notifier {
    tx: mpsc::Sender<()>,
}

impl Notifier {
    fn new(tx: mpsc::Sender<()>) -> Self {
        Notifier { tx }
    }

    fn notify(&mut self) {
        self.tx.try_send(());
    }
}

#[derive(Clone)]
struct NotifierGroup {
    notifiers: Vec<Notifier>,
}

impl NotifierGroup {
    fn new(notifiers: Vec<Notifier>) -> Self {
        NotifierGroup { notifiers }
    }
}

pub struct Replicator {
    id: NodeId,
    term: Term,
    peer: peer::GRPCPeer,
    next_index: LogIndex,
    match_index: LogIndex,
    log: Arc<RwLock<Log>>,
    rx: mpsc::Receiver<()>,
}

impl Replicator {
    fn run(
        id: NodeId,
        term: Term,
        peer: peer::GRPCPeer,
        next_index: LogIndex,
        match_index: LogIndex,
        log: Arc<RwLock<Log>>,
    ) -> Notifier {
        let (tx, rx) = mpsc::channel(128);
        let replicator = Replicator {
            id,
            term,
            peer,
            next_index,
            match_index,
            log,
            rx,
        };

        tokio::spawn(replicator.replicate());

        Notifier::new(tx)
    }

    async fn replicate(mut self) {
        loop {
            let prev_log_index = self.next_index - 1;
            let (prev_log_term, last_committed_index) = {
                let log = self.log.read().await;
                let entries = log.entries();
                let prev_log_term = entries.first().map(|e| e.term()).unwrap_or(0);
                let last_committed_index = log.last_committed();

                (prev_log_term, last_committed_index)
            };

            let f_request = self.peer.append_entries(raft::AppendEntriesRequest {
                term: self.term,
                leader_id: self.id,
                prev_log_index,
                prev_log_term,
                last_committed_index,
                entries: Vec::new(),
            });

            match timeout(Duration::from_millis(1000), f_request).await {
                Ok(Ok(res)) => {}
                Ok(Err(e)) => {}
                Err(e) => {}
            };

            if let Ok(None) =
                timeout(Duration::from_millis(HEARTBEAT_WAIT_MS), self.rx.recv()).await
            {
                break;
            }
        }
    }
}
