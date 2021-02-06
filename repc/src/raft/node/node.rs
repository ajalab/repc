use super::candidate;
use super::error::CommandError;
use super::follower;
use super::leader;
use super::role::Role;
use crate::configuration::Configuration;
use crate::pb::raft::{
    log_entry::Command, raft_client::RaftClient, AppendEntriesRequest, AppendEntriesResponse,
    RequestVoteRequest, RequestVoteResponse,
};
use crate::raft::message::Message;
use crate::session::RepcClientId;
use crate::session::Sessions;
use crate::state::{State, StateMachine};
use crate::types::{NodeId, Term};
use bytes::Bytes;
use repc_proto::types::Sequence;
use std::collections::HashMap;
use std::error;
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot};
use tonic::body::BoxBody;
use tonic::client::GrpcService;
use tonic::codegen::StdError;
use tracing::Instrument;

pub struct Node<S, T> {
    id: NodeId,
    conf: Arc<Configuration>,
    state: State<S>,
    clients: HashMap<NodeId, RaftClient<T>>,
    tx: mpsc::Sender<Message>,
    rx: mpsc::Receiver<Message>,
}

impl<S, T> Node<S, T> {
    pub fn new(id: NodeId, state_machine: S) -> Self {
        let (tx, rx) = mpsc::channel(100);

        Self {
            id,
            conf: Arc::default(),
            state: State::new(state_machine),
            clients: HashMap::new(),
            tx,
            rx,
        }
    }

    pub fn conf(mut self, conf: Arc<Configuration>) -> Self {
        self.conf = conf;
        self
    }

    pub fn clients(mut self, clients: HashMap<NodeId, RaftClient<T>>) -> Self {
        self.clients = clients;
        self
    }

    pub fn get_tx(&self) -> mpsc::Sender<Message> {
        return self.tx.clone();
    }
}

impl<S, T> Node<S, T>
where
    S: StateMachine + Send + Sync + 'static,
    T: GrpcService<BoxBody> + Clone + Send + Sync + 'static,
    T::Future: Send,
    <T::ResponseBody as http_body::Body>::Error: Into<StdError> + Send,
{
    pub async fn run(self) {
        let term = 1;
        let sessions = Arc::new(Sessions::default());
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
            sessions,
            term,
            role,
            tx: self.tx,
            rx: self.rx,
            clients: self.clients,
        };
        process.handle_messages().await
    }
}

struct NodeProcess<S, T> {
    id: NodeId,
    conf: Arc<Configuration>,
    sessions: Arc<Sessions>,

    // TODO: make these persistent
    term: Term,

    role: Role<S>,

    tx: mpsc::Sender<Message>,
    rx: mpsc::Receiver<Message>,
    clients: HashMap<NodeId, RaftClient<T>>,
}

impl<S, T> NodeProcess<S, T>
where
    S: StateMachine + Send + Sync + 'static,
    T: GrpcService<BoxBody> + Clone + Send + Sync + 'static,
    T::Future: Send,
    <T::ResponseBody as http_body::Body>::Error: Into<StdError> + Send,
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

                Message::Command {
                    command,
                    client_id,
                    sequence,
                    tx,
                } => self.handle_command(command, client_id, sequence, tx).await,
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
        req: RequestVoteRequest,
        mut tx: mpsc::Sender<Result<RequestVoteResponse, Box<dyn error::Error + Send>>>,
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

    async fn handle_request_vote_response(&mut self, res: RequestVoteResponse, id: NodeId) {
        if self.role.handle_request_vote_response(res, id).await {
            self.trans_state_leader();
        }
    }

    async fn handle_append_entries_request(
        &mut self,
        req: AppendEntriesRequest,
        mut tx: mpsc::Sender<Result<AppendEntriesResponse, Box<dyn error::Error + Send>>>,
    ) {
        let span = tracing::trace_span!(
            target: "node",
            "handle_append_entries_request",
            id = self.id,
            term = self.term,
            leader_id = req.leader_id,
        );

        let res = async {
            self.update_term(req.leader_id, req.term);
            self.role.handle_append_entries_request(req).await
        }
        .instrument(span)
        .await;

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

        self.role.handle_election_timeout(&self.clients).await;
    }

    async fn handle_command(
        &mut self,
        command: Command,
        client_id: RepcClientId,
        sequence: Sequence,
        tx: oneshot::Sender<Result<tonic::Response<Bytes>, CommandError>>,
    ) {
        let span = tracing::trace_span!(
            target: "node",
            "handle_command",
            id = self.id,
            term = self.term,
            client_id = u64::from(client_id),
            sequence = u64::from(sequence),
        );
        self.role
            .handle_command(command, client_id, sequence, tx)
            .instrument(span)
            .await;
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

        let quorum = (self.clients.len() + 1) / 2;
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
                self.sessions.clone(),
                &self.clients,
            ),
        };
    }
}
