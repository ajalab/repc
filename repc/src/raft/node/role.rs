use super::candidate::Candidate;
use super::error::CommandError;
use super::follower::Follower;
use super::leader::Leader;
use crate::raft::pb;
use crate::raft::peer::RaftPeer;
use crate::state::Command;
use crate::state::State;
use crate::state::StateMachine;
use crate::types::NodeId;
use bytes::Bytes;
use std::collections::HashMap;
use std::error::Error;
use tokio::sync::oneshot;

pub enum Role<S> {
    Follower { follower: Follower<S> },
    Candidate { candidate: Candidate<S> },
    Leader { leader: Leader<S> },
}

impl<S> Role<S>
where
    S: StateMachine,
{
    pub fn extract_state(&mut self) -> State<S> {
        match self {
            Role::Follower { follower } => follower.extract_state(),
            Role::Candidate { candidate } => candidate.extract_state(),
            Role::Leader { leader } => leader.extract_state(),
        }
    }

    pub async fn handle_request_vote_request(
        &mut self,
        req: pb::RequestVoteRequest,
    ) -> Result<pb::RequestVoteResponse, Box<dyn Error + Send>> {
        match self {
            Role::Follower { ref mut follower } => follower.handle_request_vote_request(req).await,
            _ => unimplemented!(),
        }
    }

    pub async fn handle_request_vote_response(
        &mut self,
        res: pb::RequestVoteResponse,
        id: NodeId,
    ) -> bool {
        match self {
            Role::Candidate {
                ref mut candidate, ..
            } => candidate.handle_request_vote_response(res, id).await,
            _ => unimplemented!(),
        }
    }

    pub async fn handle_append_entries_request(
        &mut self,
        req: pb::AppendEntriesRequest,
    ) -> Result<pb::AppendEntriesResponse, Box<dyn Error + Send>> {
        match self {
            Role::Follower { ref mut follower } => {
                follower.handle_append_entries_request(req).await
            }
            _ => unimplemented!(),
        }
    }

    pub async fn handle_election_timeout<P: RaftPeer + Send + Sync + Clone + 'static>(
        &mut self,
        peers: &HashMap<NodeId, P>,
    ) {
        match self {
            Role::Candidate { ref mut candidate } => candidate.handle_election_timeout(peers).await,
            _ => unimplemented!(),
        }
    }

    pub async fn handle_command(
        &mut self,
        command: Command,
        tx: oneshot::Sender<Result<tonic::Response<Bytes>, CommandError>>,
    ) {
        match self {
            Role::Leader { ref mut leader } => {
                leader.handle_command(command, tx).await;
            }
            _ => {
                let _ = tx.send(Err(CommandError::NotLeader));
            }
        }
    }
}
