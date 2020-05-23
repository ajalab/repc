use crate::candidate;
use crate::follower;
use crate::leader;
use crate::types::NodeId;
use std::collections::HashSet;

pub enum State {
    Stopped,
    Follower {
        follower: follower::Follower,
    },
    Candidate {
        candidate: candidate::Candidate,
        votes: HashSet<NodeId>,
    },
    Leader {
        leader: leader::Leader,
    },
}

impl State {
    pub const STOPPED: &'static str = "stopped";
    pub const FOLLOWER: &'static str = "follower";
    pub const CANDIDATE: &'static str = "candidate";
    pub const LEADER: &'static str = "leader";

    pub fn new() -> Self {
        State::Stopped
    }

    pub fn to_ident(&self) -> &'static str {
        match self {
            State::Stopped { .. } => State::STOPPED,
            State::Follower { .. } => State::FOLLOWER,
            State::Candidate { .. } => State::CANDIDATE,
            State::Leader { .. } => State::LEADER,
        }
    }
}
