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
    state::{log::Log, State, StateMachine},
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
        let term = self.state.term();
        let sessions = Arc::new(Sessions::default());
        let role = Role::Follower {
            follower: follower::Follower::spawn(self.conf.clone(), self.state, self.tx.clone()),
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
        process.run().await
    }
}

struct NodeProcess<S, L, T> {
    id: NodeId,
    conf: Arc<Configuration>,
    sessions: Arc<Sessions>,

    // TODO: make these persistent
    term: Term,

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
                    tracing::trace_span!(target: "node", "handle_message", term = self.term.get());
                self.handle_message(msg).instrument(span).await;
            }
        }
        .instrument(span)
        .await;
    }

    async fn handle_message(&mut self, msg: Message) {
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

    // Update the node's term and return to Follower state,
    // if the term of the request is newer than the node's current term.
    fn update_term(&mut self, term: Term) {
        if term > self.term {
            self.trans_state_follower(term);
        }
    }

    async fn handle_request_vote_request(
        &mut self,
        req: RequestVoteRequest,
        tx: mpsc::Sender<Result<RequestVoteResponse, Box<dyn error::Error + Send>>>,
    ) {
        let span = tracing::debug_span!(
            target: "node",
            "handle_request_vote_request",
            req_term = req.term,
            candidate_id = req.candidate_id,
        );

        let res = async {
            self.update_term(Term::new(req.term));
            self.role.handle_request_vote_request(self.term, req).await
        }
        .instrument(span)
        .await;

        let id = self.id;
        let term = self.term.get();
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
            target: "node",
            "handle_request_vote_response",
            res_term = res.term,
            voter_id = id,
        );

        async {
            if self.role.handle_request_vote_response(res, id).await {
                self.trans_state_leader();
            }
        }
        .instrument(span)
        .await
    }

    async fn handle_append_entries_request(
        &mut self,
        req: AppendEntriesRequest,
        tx: mpsc::Sender<Result<AppendEntriesResponse, Box<dyn error::Error + Send>>>,
    ) {
        let span = tracing::trace_span!(
            target: "node",
            "handle_append_entries_request",
            req_term = req.term,
            leader_id = req.leader_id,
        );

        let res = async {
            self.update_term(Term::new(req.term));
            self.role.handle_append_entries_request(req).await
        }
        .instrument(span)
        .await;

        let id = self.id;
        let term = self.term.get();
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
        let span = tracing::debug_span!(
            target: "node",
            "handle_election_timeout",
        );
        async {
            if matches!(self.role, Role::Leader{ .. }) {
                return;
            }
            self.trans_state_candidate();
            self.role
                .handle_election_timeout(self.term, &self.clients)
                .await;
        }
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
            target: "node",
            "handle_command",
            client_id = u64::from(client_id),
            sequence = u64::from(sequence),
        );
        self.role
            .handle_command(command, client_id, sequence, tx)
            .instrument(span)
            .await;
    }

    fn trans_state_follower(&mut self, term: Term) {
        let mut state = self.role.extract_state();
        *state.term_mut() = term;
        self.term = term;

        self.role = Role::Follower {
            follower: follower::Follower::spawn(self.conf.clone(), state, self.tx.clone()),
        };
        tracing::info!(term = self.term.get(), "become a follower");
    }

    fn trans_state_candidate(&mut self) {
        let mut state = self.role.extract_state();
        *state.term_mut() += 1;
        self.term = state.term();

        let quorum = (self.clients.len() + 1) / 2;
        self.role = Role::Candidate {
            candidate: candidate::Candidate::spawn(
                self.id,
                self.conf.clone(),
                quorum,
                state,
                self.tx.clone(),
            ),
        };
        tracing::info!(term = self.term.get(), "become a candidate");
    }

    fn trans_state_leader(&mut self) {
        tracing::info!(term = self.term.get(), "become a leader");

        self.role = Role::Leader {
            leader: leader::Leader::spawn(
                self.id,
                self.conf.clone(),
                self.role.extract_state(),
                self.sessions.clone(),
                &self.clients,
            ),
        };
    }
}
