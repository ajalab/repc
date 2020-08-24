use crate::configuration::Configuration;
use crate::raft::deadline_clock::DeadlineClock;
use crate::raft::log::Log;
use crate::raft::message::Message;
use crate::raft::pb;
use crate::raft::peer::RaftPeer;
use crate::types::{NodeId, Term};
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
            if let Err(_) = tx_dc.send(Message::ElectionTimeout).await {
                tracing::warn!(
                    id,
                    term,
                    state = "candidate",
                    "failed to send ElectionTimeout message"
                );
            }
            tracing::debug!(id, term, state = "candidate", "start re-election");
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
            tracing::debug!(
                id = self.id,
                term = self.term,
                target_id = id,
                "ignored vote from {}, which belongs to the different term: {}",
                id,
                res.term,
            );
            return false;
        }

        if !res.vote_granted {
            tracing::debug!(
                id = self.id,
                term = self.term,
                target_id = id,
                "vote requested to {} is refused",
                id,
            );
            return false;
        }

        if !self.votes.insert(id) {
            tracing::debug!(
                id = self.id,
                term = self.term,
                target_id = id,
                "received vote from {}, which already granted my vote in the current term",
                id,
            );
            return false;
        }

        tracing::debug!(
            id = self.id,
            term = self.term,
            target_id = id,
            "received vote from {}",
            id,
        );

        if self.votes.len() > self.quorum {
            tracing::info!(
                id = self.id,
                term = self.term,
                "get a majority of votes from {:?}",
                self.votes,
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
                tracing::debug!(
                    id = candidate_id,
                    term,
                    target_id = id,
                    "sending RequestVoteRequest to {}",
                    id,
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
                            tracing::error!(
                                id = candidate_id,
                                term,
                                target_id = id,
                                "failed to send message: {}",
                                e
                            );
                        }
                    }
                    Err(e) => {
                        tracing::error!(
                            id = candidate_id,
                            term,
                            target_id = id,
                            "request vote rpc failed: {}",
                            e
                        );
                    }
                }
            });
        }
    }

    pub fn extract_log(&mut self) -> Log {
        self.log.take().unwrap()
    }
}
