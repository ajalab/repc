use super::{candidate::Candidate, error::CommandError, follower::Follower, leader::Leader};
use crate::{
    pb::raft::{
        log_entry::Command, raft_client::RaftClient, AppendEntriesRequest, AppendEntriesResponse,
        RequestVoteRequest, RequestVoteResponse,
    },
    session::RepcClientId,
    state::{log::Log, State, StateMachine},
    types::{NodeId, Term},
};
use bytes::Bytes;
use repc_proto::repc::types::Sequence;
use std::{collections::HashMap, error::Error};
use tokio::sync::oneshot;
use tonic::{body::BoxBody, client::GrpcService, codegen::StdError};
use tracing::Instrument;

pub enum Role<S, L> {
    Follower { follower: Follower<S, L> },
    Candidate { candidate: Candidate<S, L> },
    Leader { leader: Leader<S, L> },
}

impl<S, L> Role<S, L>
where
    S: StateMachine,
    L: Log,
{
    fn to_ident(&self) -> &'static str {
        match self {
            Role::Follower { .. } => "follower",
            Role::Candidate { .. } => "candidate",
            Role::Leader { .. } => "leader",
        }
    }

    pub fn extract_state(&mut self) -> State<S, L> {
        match self {
            Role::Follower { follower } => follower.extract_state(),
            Role::Candidate { candidate } => candidate.extract_state(),
            Role::Leader { leader } => leader.extract_state(),
        }
    }

    pub async fn handle_request_vote_request(
        &mut self,
        term: Term,
        req: RequestVoteRequest,
    ) -> Result<RequestVoteResponse, Box<dyn Error + Send>> {
        let span = tracing::trace_span!(
            target: "role",
            "handle_request_vote_request",
            term = term.get(),
            role = self.to_ident(),
        );

        async {
            match self {
                Role::Follower { ref mut follower } => {
                    follower.handle_request_vote_request(req).await
                }
                _ => unimplemented!(),
            }
        }
        .instrument(span)
        .await
    }

    pub async fn handle_request_vote_response(
        &mut self,
        res: RequestVoteResponse,
        id: NodeId,
    ) -> bool {
        let span = tracing::trace_span!(
            target: "role",
            "handle_request_vote_response",
            role = self.to_ident(),
        );

        async {
            match self {
                Role::Candidate {
                    ref mut candidate, ..
                } => candidate.handle_request_vote_response(res, id).await,
                _ => {
                    tracing::debug!(target_id = id, "ignore stale RequestVote response");
                    false
                }
            }
        }
        .instrument(span)
        .await
    }

    pub async fn handle_append_entries_request(
        &mut self,
        req: AppendEntriesRequest,
    ) -> Result<AppendEntriesResponse, Box<dyn Error + Send>> {
        let span = tracing::trace_span!(
            target: "role",
            "handle_append_entries_request",
            role = self.to_ident(),
        );

        async {
            match self {
                Role::Follower { ref mut follower } => {
                    follower.handle_append_entries_request(req).await
                }
                _ => unimplemented!(),
            }
        }
        .instrument(span)
        .await
    }

    pub async fn handle_election_timeout<T>(
        &mut self,
        term: Term,
        clients: &HashMap<NodeId, RaftClient<T>>,
    ) where
        T: GrpcService<BoxBody> + Clone + Send + Sync + 'static,
        T::Future: Send,
        <T::ResponseBody as http_body::Body>::Error: Into<StdError> + Send,
    {
        let span = tracing::info_span!(
            target: "role",
            "handle_election_timeout",
            term = term.get(),
            role = self.to_ident(),
        );

        async {
            match self {
                Role::Candidate { ref mut candidate } => {
                    candidate.handle_election_timeout(clients).await
                }
                // TODO: handle the case where the node is already a leader.
                _ => unimplemented!(),
            }
        }
        .instrument(span)
        .await
    }

    pub async fn handle_command(
        &mut self,
        command: Command,
        client_id: RepcClientId,
        sequence: Sequence,
        tx: oneshot::Sender<Result<tonic::Response<Bytes>, CommandError>>,
    ) {
        match self {
            Role::Leader { ref mut leader } => {
                leader
                    .handle_command(command, client_id, sequence, tx)
                    .await;
            }
            _ => {
                let _ = tx.send(Err(CommandError::NotLeader));
            }
        }
    }
}
