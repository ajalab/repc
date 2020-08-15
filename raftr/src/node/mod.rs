mod base;
mod candidate;
pub mod error;
mod follower;
mod leader;
use crate::log::Log;
use crate::pb;
use crate::peer::Peer;
use crate::types::NodeId;
pub use base::BaseNode;
use bytes::Bytes;
use error::CommandError;
use std::collections::HashMap;
use std::error::Error;
use tokio::sync::oneshot;

pub enum Node {
    Follower { follower: follower::Follower },
    Candidate { candidate: candidate::Candidate },
    Leader { leader: leader::Leader },
}

impl Node {
    pub const FOLLOWER: &'static str = "follower";
    pub const CANDIDATE: &'static str = "candidate";
    pub const LEADER: &'static str = "leader";

    pub fn to_ident(&self) -> &'static str {
        match self {
            Node::Follower { .. } => Self::FOLLOWER,
            Node::Candidate { .. } => Self::CANDIDATE,
            Node::Leader { .. } => Self::LEADER,
        }
    }

    pub fn extract_log(&mut self) -> Log {
        match self {
            Node::Follower { follower } => follower.extract_log(),
            Node::Candidate { candidate } => candidate.extract_log(),
            Node::Leader { leader } => leader.extract_log(),
        }
    }

    pub async fn handle_request_vote_request(
        &mut self,
        req: pb::RequestVoteRequest,
    ) -> Result<pb::RequestVoteResponse, Box<dyn Error + Send>> {
        match self {
            Node::Follower { ref mut follower } => follower.handle_request_vote_request(req).await,
            _ => unimplemented!(),
        }
    }

    pub async fn handle_request_vote_response(
        &mut self,
        res: pb::RequestVoteResponse,
        id: NodeId,
    ) -> bool {
        match self {
            Node::Candidate {
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
            Node::Follower { ref mut follower } => {
                follower.handle_append_entries_request(req).await
            }
            _ => unimplemented!(),
        }
    }

    pub async fn handle_election_timeout<P: Peer + Send + Sync + Clone + 'static>(
        &mut self,
        peers: &HashMap<NodeId, P>,
    ) {
        match self {
            Node::Candidate { ref mut candidate } => candidate.handle_election_timeout(peers).await,
            _ => unimplemented!(),
        }
    }

    pub async fn handle_command(
        &mut self,
        command: Bytes,
        tx: oneshot::Sender<Result<(), CommandError>>,
    ) {
        match self {
            Node::Leader { ref mut leader } => {
                leader.handle_command(command, tx).await;
            }
            _ => {
                let _ = tx.send(Err(CommandError::NotLeader));
            }
        }
    }
}
