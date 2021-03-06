use super::{
    candidate,
    error::{AppendEntriesError, CommandError, RequestVoteError},
    follower, leader,
    role::Role,
};
use crate::{
    configuration::Configuration,
    log::Log,
    pb::raft::{
        log_entry::Command, raft_client::RaftClient, AppendEntriesRequest, AppendEntriesResponse,
        RequestVoteRequest, RequestVoteResponse,
    },
    raft::{election::Election, message::Message},
    session::Sessions,
    state::State,
    state_machine::StateMachine,
    types::Term,
};
use bytes::Bytes;
use repc_common::types::{ClientId, NodeId, Sequence};
use std::{collections::HashMap, mem, sync::Arc};
use tokio::sync::{mpsc, oneshot};
use tonic::{body::BoxBody, client::GrpcService, codegen::StdError};
use tracing::Instrument;

pub struct Node<S, L, T> {
    id: NodeId,
    conf: Arc<Configuration>,
    sessions: Arc<Sessions>,
    election: Election,
    role: Role<S, L>,
    tx: mpsc::Sender<Message>,
    rx: mpsc::Receiver<Message>,
    clients: HashMap<NodeId, RaftClient<T>>,
}

impl<S, L, T> Node<S, L, T>
where
    S: StateMachine + Send + Sync + 'static,
    L: Log + Send + Sync + 'static,
    T: GrpcService<BoxBody> + Clone + Send + Sync + 'static,
    T::Future: Send,
    <T::ResponseBody as http_body::Body>::Error: Into<StdError> + Send,
{
    pub fn new(id: NodeId, conf: Configuration, state: State<S, L>) -> Self {
        // TODO set limit from conf
        let (tx, rx) = mpsc::channel(100);

        Self {
            id,
            conf: Arc::new(conf),
            sessions: Arc::default(),
            election: Election::default(),
            role: Role::new(state),
            tx,
            rx,
            clients: HashMap::new(),
        }
    }

    pub fn tx(&self) -> &mpsc::Sender<Message> {
        &self.tx
    }

    pub fn clients_mut(&mut self) -> &mut HashMap<NodeId, RaftClient<T>> {
        &mut self.clients
    }

    pub async fn run(mut self) {
        self.trans_state_follower(Term::default(), None, "initializing");

        let span = tracing::info_span!(target: "node", "node", id = self.id);
        async {
            while let Some(msg) = self.rx.recv().await {
                let span =
                    tracing::trace_span!(target: "node", "handle_message", term = self.election.term.get());
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
        tx: mpsc::Sender<Result<RequestVoteResponse, RequestVoteError>>,
    ) {
        let req_term = Term::new(req.term);
        if req_term > self.election.term {
            self.trans_state_follower(req_term, None, "received a request with higher term");
        }

        let span = tracing::debug_span!(
            target: "role",
            "handle_request_vote_request",
            term = self.election.term.get(),
            role = self.role.ident(),
        );

        let candidate_id = req.candidate_id;
        let res = self
            .role
            .handle_request_vote_request(req)
            .instrument(span)
            .await;

        if res.as_ref().map(|res| res.vote_granted).unwrap_or(false) {
            self.election.voted_for = Some(candidate_id);
        }

        let id = self.id;
        let term = self.election.term.get();
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
            term = self.election.term.get(),
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
        tx: mpsc::Sender<Result<AppendEntriesResponse, AppendEntriesError>>,
    ) {
        let req_term = Term::new(req.term);
        if req_term > self.election.term {
            self.trans_state_follower(
                req_term,
                Some(req.leader_id),
                "received an AppendEntries request with higher term",
            );
        }

        // invariant: req.term <= self.term
        if req_term == self.election.term && matches!(self.role, Role::Candidate {.. }) {
            self.trans_state_follower(
                req_term,
                Some(req.leader_id),
                "received an AppendEntries request from a new leader in the current term",
            );
        }

        let span = tracing::trace_span!(
            target: "role",
            "handle_append_entries_request",
            term = self.election.term.get(),
            role = self.role.ident(),
        );
        let res = self
            .role
            .handle_append_entries_request(req)
            .instrument(span)
            .await;

        let id = self.id;
        let term = self.election.term.get();
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
            term = self.election.term.get(),
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
        client_id: ClientId,
        sequence: Sequence,
        tx: oneshot::Sender<Result<tonic::Response<Bytes>, CommandError>>,
    ) {
        let span = tracing::trace_span!(
            target: "role",
            "handle_command",
            term = self.election.term.get(),
            role = self.role.ident(),
        );
        self.role
            .handle_command(command, client_id, sequence, tx)
            .instrument(span)
            .await;
    }

    fn trans_state_follower(&mut self, term: Term, leader: Option<NodeId>, reason: &'static str) {
        debug_assert!(term >= self.election.term);

        tracing::info!(
            term = self.election.term.get(),
            reason,
            "going to become a follower"
        );

        let role = self.extract_role();
        let state = role.into_state();
        if term > self.election.term {
            self.election.voted_for = None;
            self.election.term = term;
        }

        self.role = Role::Follower {
            follower: follower::Follower::spawn(
                self.conf.clone(),
                self.election.term,
                self.election.voted_for,
                leader,
                state,
                self.tx.clone(),
            ),
        };
    }

    fn trans_state_candidate(&mut self) {
        tracing::info!(
            term = self.election.term.get(),
            "going to become a candidate"
        );

        let role = self.extract_role();
        let state = role.into_state();
        self.election.term += 1;
        self.election.voted_for = Some(self.id);

        let quorum = (self.clients.len() + 1) / 2;
        self.role = Role::Candidate {
            candidate: candidate::Candidate::spawn(
                self.id,
                self.conf.clone(),
                self.election.term,
                quorum,
                state,
                self.tx.clone(),
            ),
        };
    }

    fn trans_state_leader(&mut self) {
        tracing::info!(term = self.election.term.get(), "going to become a leader");

        let role = self.extract_role();
        let state = role.into_state();
        self.role = Role::Leader {
            leader: leader::Leader::spawn(
                self.id,
                self.conf.clone(),
                self.election.term,
                state,
                self.sessions.clone(),
                &self.clients,
            ),
        };
    }

    fn extract_role(&mut self) -> Role<S, L> {
        mem::replace(&mut self.role, Role::default())
    }
}
