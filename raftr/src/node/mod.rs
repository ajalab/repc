mod base;
mod candidate;
mod follower;
mod leader;

pub use base::BaseNode;

use crate::peer::Peer;
use crate::state::State;
use crate::types::NodeId;
use std::collections::HashSet;

pub enum Node<P: Peer> {
    Stopped {
        state: State<P>,
    },
    Follower {
        follower: follower::Follower<P>,
    },
    Candidate {
        candidate: candidate::Candidate<P>,
        votes: HashSet<NodeId>,
    },
    Leader {
        leader: leader::Leader,
    },
}

impl<P: Peer + Send + Sync> Node<P> {
    pub const STOPPED: &'static str = "stopped";
    pub const FOLLOWER: &'static str = "follower";
    pub const CANDIDATE: &'static str = "candidate";
    pub const LEADER: &'static str = "leader";

    pub fn new(state: State<P>) -> Self {
        Node::Stopped { state }
    }

    pub fn to_ident(&self) -> &'static str {
        match self {
            Node::Stopped { .. } => Self::STOPPED,
            Node::Follower { .. } => Self::FOLLOWER,
            Node::Candidate { .. } => Self::CANDIDATE,
            Node::Leader { .. } => Self::LEADER,
        }
    }

    pub fn into_state(self) -> State<P> {
        match self {
            Node::Stopped { state } => state,
            Node::Follower { follower } => follower.into_state(),
            _ => unimplemented!(),
        }
    }

    pub fn state_mut(&mut self) -> &mut State<P> {
        match self {
            Node::Follower { follower } => follower.state_mut(),
            _ => unimplemented!(),
        }
    }
}
