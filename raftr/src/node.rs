use crate::candidate;
use crate::configuration::Configuration;
use crate::follower;
use crate::leader;
use crate::log::{Log, LogEntry};
use crate::message::Message;
use crate::pb;
use crate::peer::Peer;
use crate::state::State;
use crate::types::{LogIndex, NodeId, Term};
use bytes::Bytes;
use log::{debug, info, warn};
use std::cmp;
use std::collections::{HashMap, HashSet};
use std::error;
use std::sync::Arc;
use tokio::sync::{mpsc, RwLock};

pub struct Node<P: Peer + Clone + Send + Sync + 'static> {
    id: NodeId,
    conf: Arc<Configuration>,

    // TODO: make these persistent
    term: Term,
    voted_for: Option<NodeId>,
    log: Arc<RwLock<Log>>,

    state: State,

    tx: mpsc::Sender<Message>,
    rx: mpsc::Receiver<Message>,
    peers: HashMap<NodeId, P>,
}

impl<P: Peer + Clone + Send + Sync + 'static> Node<P> {
    pub fn new(id: NodeId, conf: Configuration) -> Self {
        let (tx, rx) = mpsc::channel(100);
        Node {
            id,
            conf: Arc::new(conf),
            term: 1,
            voted_for: None,
            log: Arc::default(),
            state: State::new(),
            tx,
            rx,
            peers: HashMap::new(),
        }
    }

    async fn handle_messages(&mut self) -> Result<(), Box<dyn error::Error + Send>> {
        while let Some(msg) = self.rx.recv().await {
            match msg {
                Message::RPCRequestVoteRequest { req, tx } => {
                    self.handle_request_vote_request(req, tx).await?
                }

                Message::RPCRequestVoteResponse { res, id } => {
                    self.handle_request_vote_response(res, id).await?
                }

                Message::RPCAppendEntriesRequest { req, tx } => {
                    self.handle_append_entries_request(req, tx).await?
                }

                Message::ElectionTimeout => self.handle_election_timeout().await?,

                Message::Command { body, tx } => self.handle_command(body, tx).await?,
                _ => {}
            }
        }
        Ok(())
    }

    // Update the node's term and return to Follower state,
    // if the term of the request is newer than the node's current term.
    fn update_term(&mut self, req_id: NodeId, req_term: Term) {
        if req_term > self.term {
            info!(
                "id={}, term={}, message=\"receive a request from {} which has higher term: {}\"",
                self.id, self.term, req_id, req_term,
            );
            self.term = req_term;
            self.voted_for = None;
            if self.state.to_ident() != State::FOLLOWER {
                self.trans_state_follower();
            }
        }
    }

    async fn handle_request_vote_request(
        &mut self,
        req: pb::RequestVoteRequest,
        mut tx: mpsc::Sender<pb::RequestVoteResponse>,
    ) -> Result<(), Box<dyn error::Error + Send>> {
        self.update_term(req.candidate_id, req.term);

        // invariant: req.term <= self.term

        let valid_term = req.term == self.term;
        let valid_candidate = match self.voted_for {
            None => true,
            Some(id) => id == req.candidate_id,
        };

        let vote_granted = valid_term && valid_candidate && {
            let last_term_index = self.get_last_log_term_index().await;
            (req.last_log_term, req.last_log_index) >= last_term_index
        };

        if vote_granted && self.voted_for == None {
            self.voted_for = Some(req.candidate_id);
        }

        if vote_granted {
            debug!(
                "id={}, term={}, message=\"granted vote from {}\"",
                self.id, self.term, req.candidate_id
            );
        } else if !valid_term {
            debug!(
                "id={}, term={}, message=\"refused vote from {} because the request has invalid term: {}\"",
                self.id, self.term, req.candidate_id, req.term,
            );
        } else if !valid_candidate {
            debug!(
                "id={}, term={}, message=\"refused vote from {} because we have voted to another: {:?}\"",
                self.id, self.term, req.candidate_id, self.voted_for,
            );
        } else {
            debug!(
                "id={}, term={}, message=\"refused vote from {} because the request has outdated last log term & index: ({}, {})\"",
                self.id, self.term, req.candidate_id,
                req.last_log_term, req.last_log_index,
            );
        }

        let term = self.term;
        tokio::spawn(async move {
            let r = tx
                .send(pb::RequestVoteResponse { term, vote_granted })
                .await;

            if let Err(e) = r {
                warn!("{}", e);
            }
        });
        Ok(())
    }

    async fn handle_request_vote_response(
        &mut self,
        res: pb::RequestVoteResponse,
        id: NodeId,
    ) -> Result<(), Box<dyn error::Error + Send>> {
        if let State::Candidate { ref mut votes, .. } = self.state {
            if self.term != res.term {
                debug!(
                    "id={}, term={}, message=\"ignored vote from {}, which belongs to the different term: {}\"",
                    self.id, self.term, id, res.term,
                );
                return Ok(());
            }

            if !res.vote_granted {
                debug!(
                    "id={}, term={}, message=\"vote requested to {} is refused\"",
                    self.id, self.term, id,
                );
                return Ok(());
            }

            if !votes.insert(id) {
                debug!(
                    "id={}, term={}, message=\"received vote from {}, which already granted my vote in the current term\"",
                    self.id, self.term, id,
                );
                return Ok(());
            }

            if votes.len() > (self.peers.len() + 1) / 2 {
                info!(
                    "id={}, term={}, message=\"get a majority of votes from {:?}\"",
                    self.id, self.term, votes,
                );
                self.trans_state_leader();
            }
        }
        Ok(())
    }

    async fn handle_append_entries_request(
        &mut self,
        req: pb::AppendEntriesRequest,
        tx: mpsc::Sender<pb::AppendEntriesResponse>,
    ) -> Result<(), Box<dyn error::Error + Send>> {
        self.update_term(req.leader_id, req.term);

        // invariant:
        //   req.term <= self.term

        if req.term != self.term {
            self.send_append_entries_response(tx, false);
            return Ok(());
        }

        // invariant:
        //   req.term == self.term

        let mut log = self.log.write().await;
        if req.prev_log_index > 0 {
            let prev_log_entry = log.get(req.prev_log_index);
            let prev_log_term = prev_log_entry.map(|e| e.term());

            if prev_log_term != Some(req.prev_log_term) {
                self.send_append_entries_response(tx, false);
                return Ok(());
            }
        }

        // append log
        let mut i = 0;
        for e in req.entries.iter() {
            let index = req.prev_log_index + 1 + i;
            let term = log.get(index).map(|e| e.term());
            match term {
                Some(term) => {
                    if term != e.term {
                        log.truncate(index);
                        break;
                    }
                }
                None => {
                    break;
                }
            }
            i += 1;
        }
        log.append(
            req.entries[i as usize..]
                .iter()
                .map(|e| LogEntry::new(e.term)),
        );

        // commit log
        let last_committed_index = log.last_committed();
        log.commit(cmp::min(req.last_committed_index, last_committed_index));

        self.send_append_entries_response(tx, true);

        if let State::Follower { ref mut follower } = self.state {
            if let Err(e) = follower.reset_deadline().await {
                warn!("failed to reset deadline: {}", e);
            };
        }

        Ok(())
    }

    fn send_append_entries_response(
        &self,
        mut tx: mpsc::Sender<pb::AppendEntriesResponse>,
        success: bool,
    ) {
        let term = self.term;
        tokio::spawn(async move {
            let r = tx.send(pb::AppendEntriesResponse { term, success }).await;
            if let Err(e) = r {
                warn!("{}", e);
            }
        });
    }

    async fn handle_election_timeout(&mut self) -> Result<(), Box<dyn error::Error + Send>> {
        self.term += 1;
        self.trans_state_candidate();

        let (last_log_term, last_log_index) = self.get_last_log_term_index().await;
        for (&id, peer) in self.peers.iter() {
            let mut peer = peer.clone();
            let mut tx = self.tx();
            let term = self.term;
            let candidate_id = self.id;
            tokio::spawn(async move {
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

        Ok(())
    }

    async fn handle_command(
        &mut self,
        command: Bytes,
        tx: mpsc::Sender<Result<(), tonic::Status>>,
    ) -> Result<(), Box<dyn error::Error + Send>> {
        if let State::Leader { ref leader } = self.state {}
        Ok(())
    }

    fn trans_state_follower(&mut self) {
        info!(
            "id={}, term={}, state={}, message=\"{}\"",
            self.id,
            self.term,
            self.state.to_ident(),
            "become a follower."
        );

        self.state = State::Follower {
            follower: follower::Follower::spawn(self.id, self.term, self.tx.clone(), &self.conf),
        };
    }

    fn trans_state_candidate(&mut self) {
        info!(
            "id={}, term={}, state={}, message=\"{}\"",
            self.id,
            self.term,
            self.state.to_ident(),
            "become a candidate."
        );

        let mut votes = HashSet::new();
        votes.insert(self.id);

        self.state = State::Candidate {
            candidate: candidate::Candidate::spawn(self.id, self.term, self.tx.clone(), &self.conf),
            votes,
        };
        self.voted_for = Some(self.id);
    }

    fn trans_state_leader(&mut self) {
        info!(
            "id={}, term={}, state={}, message=\"{}\"",
            self.id,
            self.term,
            self.state.to_ident(),
            "become a leader."
        );

        self.state = State::Leader {
            leader: leader::Leader::spawn(
                self.id,
                self.term,
                self.conf.clone(),
                self.log.clone(),
                &self.peers,
            ),
        };
    }

    pub async fn run(
        mut self,
        peers: HashMap<NodeId, P>,
    ) -> Result<(), Box<dyn error::Error + Send>> {
        self.peers = peers;

        self.trans_state_follower();

        self.handle_messages().await
    }

    pub fn tx(&self) -> mpsc::Sender<Message> {
        self.tx.clone()
    }

    async fn get_last_log_term_index(&self) -> (Term, LogIndex) {
        let log = self.log.as_ref().read().await;

        (log.last_term(), log.last_index())
    }
}
