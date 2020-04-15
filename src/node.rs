use std::collections::{HashMap, HashSet};
use std::error;

use log::{info, warn};
use std::sync::Arc;
use tokio::sync::{mpsc, RwLock};

use crate::deadline_clock;
use crate::error::InvalidStateError;
use crate::leader;
use crate::log::{Log, LogEntry};
use crate::message::Message;
use crate::peer;
use crate::rpc::raft;
use crate::types::{LogIndex, NodeId, Term};

#[derive(Default)]
pub struct Cluster {
    nodes: HashMap<NodeId, String>,
}

impl Cluster {
    pub fn add<T: Into<String>>(&mut self, id: NodeId, addr: T) {
        self.nodes.insert(id, addr.into());
    }

    pub fn len(&self) -> usize {
        self.nodes.len()
    }

    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }
}

pub struct Node {
    id: NodeId,
    cluster: Cluster,

    // TODO: make these persistent
    term: Term,
    voted_for: Option<NodeId>,
    log: Arc<RwLock<Log>>,

    state: State,

    tx: mpsc::Sender<Message>,
    rx: mpsc::Receiver<Message>,
    peers: HashMap<NodeId, peer::GRPCPeer>,
}

impl Node {
    pub fn new(id: NodeId, cluster: Cluster) -> Self {
        let (tx, rx) = mpsc::channel(100);
        Node {
            id,
            cluster,
            term: 1,
            voted_for: None,
            log: Arc::default(),
            state: State::Stopped,
            tx,
            rx,
            peers: HashMap::new(),
        }
    }

    async fn handle_messages(&mut self) -> Result<(), Box<dyn error::Error>> {
        while let Some(msg) = self.rx.recv().await {
            match msg {
                Message::RPCRequestVoteRequest { req, tx } => {
                    self.handle_request_vote_request(req, tx).await?;
                }
                Message::RPCRequestVoteResponse { res, id } => {
                    self.handle_request_vote_response(res, id).await?;
                }
                Message::RPCAppendEntriesRequest { req, tx } => {
                    self.handle_append_entries_request(req, tx).await?;
                }
                Message::ElectionTimeout => {
                    self.handle_election_timeout().await?;
                }
                _ => {}
            }
        }
        Ok(())
    }

    // Update the node's term and return to Follower state,
    // if the term of the request is newer than the node's current term.
    fn update_term(&mut self, req_term: Term) {
        if req_term > self.term {
            self.term = req_term;
            self.voted_for = None;
            if self.state.to_ident() != State::FOLLOWER {
                self.trans_state_follower();
            }
        }
    }

    async fn handle_request_vote_request(
        &mut self,
        req: raft::RequestVoteRequest,
        mut tx: mpsc::Sender<raft::RequestVoteResponse>,
    ) -> Result<(), Box<dyn error::Error>> {
        self.update_term(req.term);

        // invariant: req.term <= self.term

        let valid_term = req.term == self.term;
        let valid_candidate = match self.voted_for {
            None => true,
            Some(id) => id == req.candidate_id,
        };

        let vote_granted = valid_term && valid_candidate && {
            let last_term_index = self.get_last_log_term_index().await;
            (req.last_log_term, req.last_log_index) > last_term_index
        };

        if vote_granted && self.voted_for == None {
            self.voted_for = Some(req.candidate_id);
        }

        let term = self.term;
        tokio::spawn(async move {
            let r = tx
                .send(raft::RequestVoteResponse { term, vote_granted })
                .await;

            if let Err(e) = r {
                warn!("{}", e);
            }
        });
        Ok(())
    }

    async fn handle_request_vote_response(
        &mut self,
        res: raft::RequestVoteResponse,
        id: NodeId,
    ) -> Result<(), Box<dyn error::Error>> {
        if let State::Candidate { ref mut votes } = self.state {
            if self.term != res.term {
                log::trace!(
                    "term={}: received vote from {}, which belongs to the different term: {}",
                    self.term,
                    id,
                    res.term,
                );
                return Ok(());
            }

            if !votes.insert(id) {
                log::trace!(
                    "term={}: received vote from {}, which already granted my vote in the current term",
                    self.term, id,
                );
                return Ok(());
            }

            if votes.len() > self.cluster.len() / 2 {
                self.trans_state_leader();
            }
        }
        Ok(())
    }

    async fn handle_append_entries_request(
        &mut self,
        req: raft::AppendEntriesRequest,
        tx: mpsc::Sender<raft::AppendEntriesResponse>,
    ) -> Result<(), Box<dyn error::Error>> {
        self.update_term(req.term);

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

        self.send_append_entries_response(tx, true);

        if let State::Follower { ref mut et_reset } = self.state {
            et_reset.reset().await?;
        }

        Ok(())
    }

    fn send_append_entries_response(
        &self,
        mut tx: mpsc::Sender<raft::AppendEntriesResponse>,
        success: bool,
    ) {
        let term = self.term;
        tokio::spawn(async move {
            let r = tx.send(raft::AppendEntriesResponse { term, success }).await;
            if let Err(e) = r {
                warn!("{}", e);
            }
        });
    }

    async fn handle_election_timeout(&mut self) -> Result<(), Box<dyn error::Error>> {
        self.assert_state(State::FOLLOWER)?;

        self.term += 1;
        self.trans_state_candidate();

        let (last_log_term, last_log_index) = self.get_last_log_term_index().await;
        for (&id, peer) in self.peers.iter() {
            let mut peer = peer.clone();
            let mut tx = self.get_tx();
            let term = self.term;
            let candidate_id = self.id;
            tokio::spawn(async move {
                let res = peer
                    .request_vote(raft::RequestVoteRequest {
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

    fn assert_state(&self, expected: &'static str) -> Result<(), InvalidStateError> {
        let actual = self.state.to_ident();
        if actual == expected {
            Ok(())
        } else {
            Err(InvalidStateError::new(actual, expected))
        }
    }

    fn trans_state_follower(&mut self) {
        info!(
            "id={}, term={}, state={}, message=\"{}\"",
            self.id,
            self.term,
            self.state.to_ident(),
            "become a follower."
        );

        let (dc, et_reset) = deadline_clock::clock(2500);
        self.state = State::Follower { et_reset };

        let id = self.id;
        let term = self.term;
        let mut tx = self.tx.clone();

        // TODO: separate?
        tokio::spawn(async move {
            if dc.run().await.is_ok() {
                if let Err(e) = tx.send(Message::ElectionTimeout).await {
                    warn!(
                        "id={}, term={}, state={}, message=\"{}: {}\"",
                        id,
                        term,
                        State::FOLLOWER,
                        "failed to send message ElectionTimeout",
                        e
                    );
                }
            }
        });
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

        self.state = State::Candidate { votes };
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
            leader: leader::Leader::spawn(self.id, self.term, self.log, &self.peers).await,
        };
    }

    pub async fn run(mut self) -> Result<(), Box<dyn error::Error>> {
        // set up connections to other nodes
        let mut ids = Vec::new();
        let mut peer_futures = Vec::new();
        for (id, addr) in self.cluster.nodes.iter() {
            if *id == self.id {
                continue;
            }
            ids.push(*id);
            peer_futures.push(peer::GRPCPeer::connect(addr));
        }

        let result = futures::future::try_join_all(peer_futures).await;
        self.peers = result
            .map(|peers| ids.into_iter().zip(peers).collect::<Vec<_>>())?
            .into_iter()
            .collect();

        self.trans_state_follower();

        self.handle_messages().await
    }

    pub fn get_addr(&self) -> &String {
        // TODO: handle errors
        self.cluster.nodes.get(&self.id).unwrap()
    }

    pub fn get_tx(&self) -> mpsc::Sender<Message> {
        self.tx.clone()
    }

    async fn get_last_log_term_index(&self) -> (Term, LogIndex) {
        let log = self.log.as_ref().read().await;

        (log.last_term(), log.last_index())
    }
}

pub enum State {
    Stopped,
    Follower { et_reset: deadline_clock::Reset },
    Candidate { votes: HashSet<NodeId> },
    Leader { leader: leader::Leader },
}

impl State {
    const STOPPED: &'static str = "stopped";
    const FOLLOWER: &'static str = "follower";
    const CANDIDATE: &'static str = "candidate";
    const LEADER: &'static str = "leader";

    pub fn to_ident(&self) -> &'static str {
        match self {
            State::Stopped => State::STOPPED,
            State::Follower { .. } => State::FOLLOWER,
            State::Candidate { .. } => State::CANDIDATE,
            State::Leader { .. } => State::LEADER,
        }
    }
}
