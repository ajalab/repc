use super::candidate::Candidate;
use super::error::CommandError;
use super::follower::Follower;
use super::leader::Leader;
use crate::raft::log::Command;
use crate::raft::log::Log;
use crate::raft::pb;
use crate::raft::peer::RaftPeer;
use crate::types::NodeId;
use bytes::Bytes;
use std::collections::HashMap;
use std::error::Error;
use tokio::sync::oneshot;

pub enum State {
    Follower { follower: Follower },
    Candidate { candidate: Candidate },
    Leader { leader: Leader },
}

impl State {
    pub fn extract_log(&mut self) -> Log {
        match self {
            State::Follower { follower } => follower.extract_log(),
            State::Candidate { candidate } => candidate.extract_log(),
            State::Leader { leader } => leader.extract_log(),
        }
    }

    pub async fn handle_request_vote_request(
        &mut self,
        req: pb::RequestVoteRequest,
    ) -> Result<pb::RequestVoteResponse, Box<dyn Error + Send>> {
        match self {
            State::Follower { ref mut follower } => follower.handle_request_vote_request(req).await,
            _ => unimplemented!(),
        }
    }

    pub async fn handle_request_vote_response(
        &mut self,
        res: pb::RequestVoteResponse,
        id: NodeId,
    ) -> bool {
        match self {
            State::Candidate {
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
            State::Follower { ref mut follower } => {
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
            State::Candidate { ref mut candidate } => {
                candidate.handle_election_timeout(peers).await
            }
            _ => unimplemented!(),
        }
    }

    pub async fn handle_command(
        &mut self,
        command: Command,
        tx: oneshot::Sender<Result<Bytes, CommandError>>,
    ) {
        match self {
            State::Leader { ref mut leader } => {
                leader.handle_command(command, tx).await;
            }
            _ => {
                let _ = tx.send(Err(CommandError::NotLeader));
            }
        }
    }
}
