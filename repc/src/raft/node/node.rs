use super::candidate;
use super::error::CommandError;
use super::follower;
use super::leader;
use super::role::Role;
use crate::configuration::Configuration;
use crate::raft::message::Message;
use crate::raft::pb;
use crate::raft::peer::RaftPeer;
use crate::state::StateMachine;
use crate::state::{Command, State};
use crate::types::{NodeId, Term};
use bytes::Bytes;
use std::collections::HashMap;
use std::error;
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot};

pub struct Node<S, P> {
    id: NodeId,
    conf: Arc<Configuration>,
    state: State<S>,
    peers: HashMap<NodeId, P>,
    tx: mpsc::Sender<Message>,
    rx: mpsc::Receiver<Message>,
}

impl<S, P> Node<S, P>
where
    S: StateMachine + Send + Sync + 'static,
    P: RaftPeer + Clone + Send + Sync + 'static,
{
    pub fn new(id: NodeId, state_machine: S) -> Self {
        let (tx, rx) = mpsc::channel(100);

        Self {
            id,
            conf: Arc::default(),
            state: State::new(state_machine),
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
        let role = Role::Follower {
            follower: follower::Follower::spawn(
                self.id,
                self.conf.clone(),
                term,
                self.state,
                self.tx.clone(),
            ),
        };
        let mut process = NodeProcess {
            id: self.id,
            conf: self.conf,
            term,
            role,
            tx: self.tx,
            rx: self.rx,
            peers: self.peers,
        };
        process.handle_messages().await
    }
}

struct NodeProcess<S, P> {
    id: NodeId,
    conf: Arc<Configuration>,

    // TODO: make these persistent
    term: Term,

    role: Role<S>,

    tx: mpsc::Sender<Message>,
    rx: mpsc::Receiver<Message>,
    peers: HashMap<NodeId, P>,
}

impl<S, P> NodeProcess<S, P>
where
    S: StateMachine + Send + Sync + 'static,
    P: RaftPeer + Clone + Send + Sync + 'static,
{
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

                Message::Command { command, tx } => self.handle_command(command, tx).await,
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
        let res = self.role.handle_request_vote_request(req).await;

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
        if self.role.handle_request_vote_response(res, id).await {
            self.trans_state_leader();
        }
    }

    async fn handle_append_entries_request(
        &mut self,
        req: pb::AppendEntriesRequest,
        mut tx: mpsc::Sender<Result<pb::AppendEntriesResponse, Box<dyn error::Error + Send>>>,
    ) {
        self.update_term(req.leader_id, req.term);
        let res = self.role.handle_append_entries_request(req).await;

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

        self.role.handle_election_timeout(&self.peers).await;
    }

    async fn handle_command(
        &mut self,
        command: Command,
        tx: oneshot::Sender<Result<tonic::Response<Bytes>, CommandError>>,
    ) {
        tracing::trace!(id = self.id, term = self.term, "received a command");
        self.role.handle_command(command, tx).await;
    }

    fn trans_state_follower(&mut self) {
        tracing::info!(id = self.id, term = self.term, "become a follower");

        self.role = Role::Follower {
            follower: follower::Follower::spawn(
                self.id,
                self.conf.clone(),
                self.term,
                self.role.extract_state(),
                self.tx.clone(),
            ),
        }
    }

    fn trans_state_candidate(&mut self) {
        tracing::info!(id = self.id, term = self.term, "become a candidate");

        let quorum = (self.peers.len() + 1) / 2;
        self.role = Role::Candidate {
            candidate: candidate::Candidate::spawn(
                self.id,
                self.conf.clone(),
                self.term,
                quorum,
                self.role.extract_state(),
                self.tx.clone(),
            ),
        };
    }

    fn trans_state_leader(&mut self) {
        tracing::info!(id = self.id, term = self.term, "become a leader");

        self.role = Role::Leader {
            leader: leader::Leader::spawn(
                self.id,
                self.conf.clone(),
                self.term,
                self.role.extract_state(),
                &self.peers,
            ),
        };
    }
}
