use super::{
    candidate::Candidate,
    error::{AppendEntriesError, CommandError, RequestVoteError},
    follower::Follower,
    leader::Leader,
};
use crate::{
    log::Log,
    pb::raft::{
        log_entry::Command, raft_client::RaftClient, AppendEntriesRequest, AppendEntriesResponse,
        RequestVoteRequest, RequestVoteResponse,
    },
    state::State,
    state_machine::StateMachine,
};
use bytes::Bytes;
use repc_common::repc::types::{ClientId, NodeId, Sequence};
use std::collections::HashMap;
use tokio::sync::oneshot;
use tonic::{body::BoxBody, client::GrpcService, codegen::StdError};

pub enum Role<S, L> {
    Stopped { state: Option<State<S, L>> },
    Follower { follower: Follower<S, L> },
    Candidate { candidate: Candidate<S, L> },
    Leader { leader: Leader<S, L> },
}

impl<S, L> Role<S, L>
where
    S: StateMachine,
    L: Log,
{
    pub fn new(state: State<S, L>) -> Self {
        Role::Stopped { state: Some(state) }
    }

    pub fn ident(&self) -> &'static str {
        match self {
            Role::Stopped { .. } => "stopped",
            Role::Follower { .. } => "follower",
            Role::Candidate { .. } => "candidate",
            Role::Leader { .. } => "leader",
        }
    }

    pub fn extract_state(&mut self) -> State<S, L> {
        match self {
            Role::Stopped { state } => state.take().unwrap(),
            Role::Follower { follower } => follower.extract_state(),
            Role::Candidate { candidate } => candidate.extract_state(),
            Role::Leader { leader } => leader.extract_state(),
        }
    }

    pub async fn handle_request_vote_request(
        &mut self,
        req: RequestVoteRequest,
    ) -> Result<RequestVoteResponse, RequestVoteError> {
        match self {
            Role::Follower { ref mut follower } => follower.handle_request_vote_request(req).await,
            Role::Candidate { ref candidate } => candidate.handle_request_vote_request(req).await,
            Role::Leader { ref leader } => leader.handle_request_vote_request(req).await,
            _ => {
                unreachable!("role {} cannot handle RequestVote", self.ident());
            }
        }
    }

    pub async fn handle_request_vote_response(
        &mut self,
        res: RequestVoteResponse,
        id: NodeId,
    ) -> bool {
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

    pub async fn handle_append_entries_request(
        &mut self,
        req: AppendEntriesRequest,
    ) -> Result<AppendEntriesResponse, AppendEntriesError> {
        match self {
            Role::Follower { ref mut follower } => {
                follower.handle_append_entries_request(req).await
            }
            Role::Candidate { ref candidate } => candidate.handle_append_entries_request(req).await,
            Role::Leader { ref leader } => leader.handle_append_entries_request(req).await,
            _ => {
                unreachable!("role {} cannot handle AppendEntries", self.ident());
            }
        }
    }

    pub async fn handle_election_timeout<T>(&mut self, clients: &HashMap<NodeId, RaftClient<T>>)
    where
        T: GrpcService<BoxBody> + Clone + Send + Sync + 'static,
        T::Future: Send,
        <T::ResponseBody as http_body::Body>::Error: Into<StdError> + Send,
    {
        match self {
            Role::Candidate { ref mut candidate } => {
                candidate.handle_election_timeout(clients).await
            }
            // TODO: handle the case where the node is already a leader.
            _ => unimplemented!(),
        }
    }

    pub async fn handle_command(
        &mut self,
        command: Command,
        client_id: ClientId,
        sequence: Sequence,
        tx: oneshot::Sender<Result<tonic::Response<Bytes>, CommandError>>,
    ) {
        match self {
            Role::Follower { ref follower } => {
                follower
                    .handle_command(command, client_id, sequence, tx)
                    .await;
            }
            Role::Candidate { ref candidate } => {
                candidate
                    .handle_command(command, client_id, sequence, tx)
                    .await;
            }
            Role::Leader { ref mut leader } => {
                leader
                    .handle_command(command, client_id, sequence, tx)
                    .await;
            }
            _ => {
                let _ = tx.send(Err(CommandError::NotLeader(None)));
            }
        }
    }
}
