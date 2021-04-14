use super::{candidate, error::CommandError, follower, leader, role::Role};
use crate::{
    configuration::Configuration,
    pb::raft::{
        log_entry::Command, raft_client::RaftClient, AppendEntriesRequest, AppendEntriesResponse,
        RequestVoteRequest, RequestVoteResponse,
    },
    raft::message::Message,
    session::RepcClientId,
    session::Sessions,
    state::{log::Log, ElectionState, State, StateMachine},
    types::{NodeId, Term},
};
use bytes::Bytes;
use repc_proto::repc::types::Sequence;
use std::{collections::HashMap, error, sync::Arc};
use tokio::sync::{mpsc, oneshot};
use tonic::{body::BoxBody, client::GrpcService, codegen::StdError};
use tracing::Instrument;

pub struct Node<S, L, T> {
    id: NodeId,
    conf: Arc<Configuration>,
    state: State<S, L>,
    clients: HashMap<NodeId, RaftClient<T>>,
    tx: mpsc::Sender<Message>,
    rx: mpsc::Receiver<Message>,
}

impl<S, L, T> Node<S, L, T> {
    pub fn new(id: NodeId, state: State<S, L>) -> Self {
        let (tx, rx) = mpsc::channel(100);

        Self {
            id,
            conf: Arc::default(),
            state,
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

impl<S, L, T> Node<S, L, T>
where
    S: StateMachine + Send + Sync + 'static,
    L: Log + Send + Sync + 'static,
    T: GrpcService<BoxBody> + Clone + Send + Sync + 'static,
    T::Future: Send,
    <T::ResponseBody as http_body::Body>::Error: Into<StdError> + Send,
{
    pub async fn run(self) {
        let election_state = ElectionState::default();
        let sessions = Arc::new(Sessions::default());
        let role = Role::Follower {
            follower: follower::Follower::spawn(
                self.conf.clone(),
                election_state.term,
                election_state.voted_for,
                self.state,
                self.tx.clone(),
            ),
        };
        let mut process = NodeProcess {
            id: self.id,
            conf: self.conf,
            sessions,
            election_state,
            role,
            tx: self.tx,
            rx: self.rx,
            clients: self.clients,
        };
        process.run().await
    }
}

struct NodeProcess<S, L, T> {
    id: NodeId,
    conf: Arc<Configuration>,
    sessions: Arc<Sessions>,

    election_state: ElectionState,

    role: Role<S, L>,

    tx: mpsc::Sender<Message>,
    rx: mpsc::Receiver<Message>,
    clients: HashMap<NodeId, RaftClient<T>>,
}

impl<S, L, T> NodeProcess<S, L, T>
where
    S: StateMachine + Send + Sync + 'static,
    L: Log + Send + Sync + 'static,
    T: GrpcService<BoxBody> + Clone + Send + Sync + 'static,
    T::Future: Send,
    <T::ResponseBody as http_body::Body>::Error: Into<StdError> + Send,
{
    async fn run(&mut self) {
        let span = tracing::info_span!(target: "node", "node", id = self.id);
        async {
            while let Some(msg) = self.rx.recv().await {
                let span =
                    tracing::trace_span!(target: "node", "handle_message", term = self.election_state.term.get());
                self.handle_message(msg).instrument(span).await;
            }
        }
        .instrument(span)
        .await;
    }

    async fn handle_message(&mut self, msg: Message) {
        match msg {
            Message::RPCRequestVoteRequest { req, tx } => {
                let span = tracing::debug_span!(
                    target: "node",
                    "handle_request_vote_request",
                    req_term = req.term,
                    candidate_id = req.candidate_id,
                );
                self.handle_request_vote_request(req, tx)
                    .instrument(span)
                    .await
            }

            Message::RPCRequestVoteResponse { res, id } => {
                let span = tracing::debug_span!(
                    target: "node",
                    "handle_request_vote_response",
                    res_term = res.term,
                    voter_id = id,
                );
                self.handle_request_vote_response(res, id)
                    .instrument(span)
                    .await
            }

            Message::RPCAppendEntriesRequest { req, tx } => {
                let span = tracing::trace_span!(
                    target: "node",
                    "handle_append_entries_request",
                    req_term = req.term,
                    leader_id = req.leader_id,
                );
                self.handle_append_entries_request(req, tx)
                    .instrument(span)
                    .await
            }

            Message::ElectionTimeout => self.handle_election_timeout().await,

            Message::Command {
                command,
                client_id,
                sequence,
                tx,
            } => {
                let span = tracing::trace_span!(
                    target: "node",
                    "handle_command",
                    client_id = u64::from(client_id),
                    sequence = u64::from(sequence),
                );
                self.handle_command(command, client_id, sequence, tx)
                    .instrument(span)
                    .await
            }
            _ => {}
        }
    }

    async fn handle_request_vote_request(
        &mut self,
        req: RequestVoteRequest,
        tx: mpsc::Sender<Result<RequestVoteResponse, Box<dyn error::Error + Send>>>,
    ) {
        let req_term = Term::new(req.term);
        if req_term > self.election_state.term {
            self.trans_state_follower(req_term);
        }

        let span = tracing::debug_span!(
            target: "role",
            "handle_request_vote_request",
            term = self.election_state.term.get(),
            role = self.role.ident(),
        );

        let candidate_id = req.candidate_id;
        let res = self
            .role
            .handle_request_vote_request(req)
            .instrument(span)
            .await;

        if res.as_ref().map(|res| res.vote_granted).unwrap_or(false) {
            self.election_state.voted_for = Some(candidate_id);
        }

        let id = self.id;
        let term = self.election_state.term.get();
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
        let span = tracing::debug_span!(
            target: "role",
            "handle_request_vote_response",
            term = self.election_state.term.get(),
            role = self.role.ident(),
        );

        let become_leader = self
            .role
            .handle_request_vote_response(res, id)
            .instrument(span)
            .await;
        if become_leader {
            self.trans_state_leader();
        }
    }

    async fn handle_append_entries_request(
        &mut self,
        req: AppendEntriesRequest,
        tx: mpsc::Sender<Result<AppendEntriesResponse, Box<dyn error::Error + Send>>>,
    ) {
        let req_term = Term::new(req.term);
        if req_term > self.election_state.term {
            self.trans_state_follower(req_term);
        }

        let span = tracing::trace_span!(
            target: "role",
            "handle_append_entries_request",
            term = self.election_state.term.get(),
            role = self.role.ident(),
        );
        let res = self
            .role
            .handle_append_entries_request(req)
            .instrument(span)
            .await;

        let id = self.id;
        let term = self.election_state.term.get();
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
        if matches!(self.role, Role::Leader{ .. }) {
            return;
        }
        self.trans_state_candidate();

        let span = tracing::debug_span!(
            target: "role",
            "handle_election_timeout",
            term = self.election_state.term.get(),
            role = self.role.ident(),
        );
        self.role
            .handle_election_timeout(&self.clients)
            .instrument(span)
            .await;
    }

    async fn handle_command(
        &mut self,
        command: Command,
        client_id: RepcClientId,
        sequence: Sequence,
        tx: oneshot::Sender<Result<tonic::Response<Bytes>, CommandError>>,
    ) {
        let span = tracing::trace_span!(
            target: "role",
            "handle_command",
            term = self.election_state.term.get(),
            role = self.role.ident(),
        );
        self.role
            .handle_command(command, client_id, sequence, tx)
            .instrument(span)
            .await;
    }

    fn trans_state_follower(&mut self, term: Term) {
        let state = self.role.extract_state();
        self.election_state.term = term;
        self.election_state.voted_for = None;

        self.role = Role::Follower {
            follower: follower::Follower::spawn(
                self.conf.clone(),
                self.election_state.term,
                self.election_state.voted_for,
                state,
                self.tx.clone(),
            ),
        };
        tracing::info!(term = self.election_state.term.get(), "become a follower");
    }

    fn trans_state_candidate(&mut self) {
        let state = self.role.extract_state();
        self.election_state.term += 1;
        self.election_state.voted_for = Some(self.id);

        let quorum = (self.clients.len() + 1) / 2;
        self.role = Role::Candidate {
            candidate: candidate::Candidate::spawn(
                self.id,
                self.conf.clone(),
                self.election_state.term,
                quorum,
                state,
                self.tx.clone(),
            ),
        };
        tracing::info!(term = self.election_state.term.get(), "become a candidate");
    }

    fn trans_state_leader(&mut self) {
        self.role = Role::Leader {
            leader: leader::Leader::spawn(
                self.id,
                self.conf.clone(),
                self.election_state.term,
                self.role.extract_state(),
                self.sessions.clone(),
                &self.clients,
            ),
        };
        tracing::info!(term = self.election_state.term.get(), "become a leader");
    }
}
