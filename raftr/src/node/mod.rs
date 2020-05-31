mod base;
mod candidate;
mod follower;
mod leader;

pub use base::BaseNode;

use crate::log::Log;
use crate::pb;
use crate::peer::Peer;
use crate::types::NodeId;
use std::collections::HashMap;
use std::error;
use std::sync::Arc;
use tokio::sync::RwLock;

pub enum Node {
    Stopped,
    Follower { follower: follower::Follower },
    Candidate { candidate: candidate::Candidate },
    Leader { leader: leader::Leader },
}

impl Node {
    pub const STOPPED: &'static str = "stopped";
    pub const FOLLOWER: &'static str = "follower";
    pub const CANDIDATE: &'static str = "candidate";
    pub const LEADER: &'static str = "leader";

    pub fn new() -> Self {
        Node::Stopped {}
    }

    pub fn to_ident(&self) -> &'static str {
        match self {
            Node::Stopped { .. } => Self::STOPPED,
            Node::Follower { .. } => Self::FOLLOWER,
            Node::Candidate { .. } => Self::CANDIDATE,
            Node::Leader { .. } => Self::LEADER,
        }
    }

    pub fn log(&self) -> Arc<RwLock<Log>> {
        match self {
            Node::Stopped { .. } => Arc::default(),
            Node::Follower { follower } => follower.log(),
            Node::Candidate { candidate } => candidate.log(),
            _ => unimplemented!(),
        }
    }

    pub async fn handle_request_vote_request(
        &mut self,
        req: pb::RequestVoteRequest,
    ) -> Result<pb::RequestVoteResponse, Box<dyn error::Error + Send>> {
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
    ) -> Result<pb::AppendEntriesResponse, Box<dyn error::Error + Send>> {
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
}
