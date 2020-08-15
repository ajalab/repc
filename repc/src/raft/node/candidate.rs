use crate::configuration::Configuration;
use crate::raft::deadline_clock::DeadlineClock;
use crate::raft::log::Log;
use crate::raft::message::Message;
use crate::raft::pb;
use crate::raft::peer::RaftPeer;
use crate::types::{NodeId, Term};
use log::{debug, info, warn};
use rand::Rng;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::mpsc;

pub struct Candidate {
    id: NodeId,
    term: Term,
    votes: HashSet<NodeId>,
    quorum: usize,
    log: Option<Log>,
    tx: mpsc::Sender<Message>,
    _deadline_clock: DeadlineClock,
}

impl Candidate {
    pub fn spawn(
        id: NodeId,
        conf: Arc<Configuration>,
        term: Term,
        quorum: usize,
        log: Log,
        tx: mpsc::Sender<Message>,
    ) -> Self {
        let mut rng = rand::thread_rng();
        let timeout_millis: u64 = conf.candidate.election_timeout_millis
            + rng.gen_range(0, conf.candidate.election_timeout_jitter_millis + 1);

        let mut tx_dc = tx.clone();

        let deadline_clock = DeadlineClock::spawn(timeout_millis, async move {
            if let Err(e) = tx_dc.send(Message::ElectionTimeout).await {
                warn!(
                    "id={}, term={}, state={}, message=\"{}: {}\"",
                    id, term, "candidate", "failed to send message ElectionTimeout", e
                );
            }
            debug!(
                "id={}, term={}, state={}, message=\"{}\"",
                id, term, "candidate", "start re-election"
            );
        });

        let mut votes = HashSet::new();
        votes.insert(id);

        Candidate {
            id,
            term,
            votes,
            quorum,
            log: Some(log),
            tx,
            _deadline_clock: deadline_clock,
        }
    }

    pub async fn handle_request_vote_response(
        &mut self,
        res: pb::RequestVoteResponse,
        id: NodeId,
    ) -> bool {
        if self.term != res.term {
            debug!(
                    "id={}, term={}, message=\"ignored vote from {}, which belongs to the different term: {}\"",
                    self.id, self.term, id, res.term,
                );
            return false;
        }

        if !res.vote_granted {
            debug!(
                "id={}, term={}, message=\"vote requested to {} is refused\"",
                self.id, self.term, id,
            );
            return false;
        }

        if !self.votes.insert(id) {
            debug!(
                    "id={}, term={}, message=\"received vote from {}, which already granted my vote in the current term\"",
                    self.id, self.term, id,
                );
            return false;
        }

        debug!(
            "id={}, term={}, message=\"received vote from {}\"",
            self.id, self.term, id
        );

        if self.votes.len() > self.quorum {
            info!(
                "id={}, term={}, message=\"get a majority of votes from {:?}\"",
                self.id, self.term, self.votes,
            );
            return true;
        }
        return false;
    }

    pub async fn handle_election_timeout<P: RaftPeer + Send + Sync + Clone + 'static>(
        &mut self,
        peers: &HashMap<NodeId, P>,
    ) {
        let log = self.log.as_ref().unwrap();
        let last_log_term = log.last_term();
        let last_log_index = log.last_index();
        for (&id, peer) in peers.iter() {
            let mut peer = peer.clone();
            let mut tx = self.tx.clone();
            let term = self.term;
            let candidate_id = self.id;
            tokio::spawn(async move {
                debug!(
                    "id={}, term={}, message=\"sending RequestVoteRequest to {}\"",
                    candidate_id, term, id
                );
                let res = peer
                    .request_vote(pb::RequestVoteRequest {
                        term,
                        candidate_id,
                        last_log_index,
                        last_log_term,
                    })
                    .await;

                match res {
                    Ok(res) => {
                        let r = tx.send(Message::RPCRequestVoteResponse { res, id }).await;
                        if let Err(e) = r {
                            warn!("failed to send message: {}", e);
                        }
                    }
                    Err(e) => {
                        warn!("request vote rpc failed: {}", e);
                    }
                }
            });
        }
    }

    pub fn extract_log(&mut self) -> Log {
        self.log.take().unwrap()
    }
}
