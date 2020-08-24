use super::candidate;
use super::error::CommandError;
use super::follower;
use super::leader;
use super::state::State;
use crate::configuration::Configuration;
use crate::raft::log::Log;
use crate::raft::message::Message;
use crate::raft::pb;
use crate::raft::peer::RaftPeer;
use crate::state_machine::StateMachineManager;
use crate::types::{NodeId, Term};
use bytes::Bytes;
use std::collections::HashMap;
use std::error;
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot};

pub struct Node<P> {
    id: NodeId,
    conf: Arc<Configuration>,
    sm_manager: StateMachineManager,
    peers: HashMap<NodeId, P>,
    tx: mpsc::Sender<Message>,
    rx: mpsc::Receiver<Message>,
}

impl<P: RaftPeer + Clone + Send + Sync + 'static> Node<P> {
    pub fn new(id: NodeId, sm_manager: StateMachineManager) -> Self {
        let (tx, rx) = mpsc::channel(100);

        Self {
            id,
            conf: Arc::default(),
            sm_manager,
            peers: HashMap::new(),
            tx,
            rx,
        }
    }

    pub fn conf(mut self, conf: Arc<Configuration>) -> Self {
        self.conf = conf;
        self
    }

    pub fn peers(mut self, peers: HashMap<NodeId, P>) -> Self {
        self.peers = peers;
        self
    }

    pub fn get_tx(&self) -> mpsc::Sender<Message> {
        return self.tx.clone();
    }

    pub async fn run(self) {
        let term = 1;
        let node = State::Follower {
            follower: follower::Follower::spawn(
                self.id,
                self.conf.clone(),
                term,
                Log::default(),
                self.sm_manager.clone(),
                self.tx.clone(),
            ),
        };
        let mut process = BaseNodeProcess {
            id: self.id,
            conf: self.conf,
            term,
            node,
            tx: self.tx,
            rx: self.rx,
            sm_manager: self.sm_manager,
            peers: self.peers,
        };
        process.handle_messages().await
    }
}

struct BaseNodeProcess<P: RaftPeer + Clone + Send + Sync + 'static> {
    id: NodeId,
    conf: Arc<Configuration>,

    // TODO: make these persistent
    term: Term,

    node: State,

    tx: mpsc::Sender<Message>,
    rx: mpsc::Receiver<Message>,
    sm_manager: StateMachineManager,
    peers: HashMap<NodeId, P>,
}

impl<P: RaftPeer + Clone + Send + Sync + 'static> BaseNodeProcess<P> {
    async fn handle_messages(&mut self) {
        while let Some(msg) = self.rx.recv().await {
            match msg {
                Message::RPCRequestVoteRequest { req, tx } => {
                    self.handle_request_vote_request(req, tx).await
                }

                Message::RPCRequestVoteResponse { res, id } => {
                    self.handle_request_vote_response(res, id).await
                }

                Message::RPCAppendEntriesRequest { req, tx } => {
                    self.handle_append_entries_request(req, tx).await
                }

                Message::ElectionTimeout => self.handle_election_timeout().await,

                Message::Command { body, tx } => self.handle_command(body, tx).await,
                _ => {}
            }
        }
    }

    // Update the node's term and return to Follower state,
    // if the term of the request is newer than the node's current term.
    fn update_term(&mut self, req_id: NodeId, req_term: Term) {
        if req_term > self.term {
            tracing::debug!(
                id = self.id,
                term = self.term,
                target_id = req_id,
                "receive a request from {} which has higher term: {}",
                req_id,
                req_term,
            );
            self.term = req_term;
            self.trans_state_follower();
        }
    }

    async fn handle_request_vote_request(
        &mut self,
        req: pb::RequestVoteRequest,
        mut tx: mpsc::Sender<Result<pb::RequestVoteResponse, Box<dyn error::Error + Send>>>,
    ) {
        self.update_term(req.candidate_id, req.term);
        let res = self.node.handle_request_vote_request(req).await;

        let id = self.id;
        let term = self.term;
        tokio::spawn(async move {
            let r = tx.send(res).await;

            if let Err(e) = r {
                tracing::warn!(
                    id,
                    term,
                    "failed to send a callback for RequestVoteRequest: {}",
                    e
                );
            }
        });
    }

    async fn handle_request_vote_response(&mut self, res: pb::RequestVoteResponse, id: NodeId) {
        if self.node.handle_request_vote_response(res, id).await {
            self.trans_state_leader();
        }
    }

    async fn handle_append_entries_request(
        &mut self,
        req: pb::AppendEntriesRequest,
        mut tx: mpsc::Sender<Result<pb::AppendEntriesResponse, Box<dyn error::Error + Send>>>,
    ) {
        self.update_term(req.leader_id, req.term);
        let res = self.node.handle_append_entries_request(req).await;

        let id = self.id;
        let term = self.term;
        tokio::spawn(async move {
            let r = tx.send(res).await;

            if let Err(e) = r {
                tracing::warn!(
                    id,
                    term,
                    "failed to send a callback for RequestVoteRequest: {}",
                    e
                );
            }
        });
    }

    async fn handle_election_timeout(&mut self) {
        // TODO: handle the case where the node is already a leader.
        self.term += 1;
        self.trans_state_candidate();

        self.node.handle_election_timeout(&self.peers).await;
    }

    async fn handle_command(
        &mut self,
        command: Bytes,
        tx: oneshot::Sender<Result<Bytes, CommandError>>,
    ) {
        tracing::trace!(id = self.id, term = self.term, "received a command");
        self.node.handle_command(command, tx).await;
    }

    fn trans_state_follower(&mut self) {
        tracing::info!(id = self.id, term = self.term, "become a follower");

        self.node = State::Follower {
            follower: follower::Follower::spawn(
                self.id,
                self.conf.clone(),
                self.term,
                self.node.extract_log(),
                self.sm_manager.clone(),
                self.tx.clone(),
            ),
        }
    }

    fn trans_state_candidate(&mut self) {
        tracing::info!(id = self.id, term = self.term, "become a candidate");

        let quorum = (self.peers.len() + 1) / 2;
        self.node = State::Candidate {
            candidate: candidate::Candidate::spawn(
                self.id,
                self.conf.clone(),
                self.term,
                quorum,
                self.node.extract_log(),
                self.tx.clone(),
            ),
        };
    }

    fn trans_state_leader(&mut self) {
        tracing::info!(id = self.id, term = self.term, "become a leader");

        self.node = State::Leader {
            leader: leader::Leader::spawn(
                self.id,
                self.conf.clone(),
                self.term,
                self.node.extract_log(),
                &self.peers,
                self.sm_manager.clone(),
            ),
        };
    }
}