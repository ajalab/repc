use crate::types::NodeId;
use std::collections::HashMap;
use std::net::IpAddr;

const LEADER_HEARTBEAT_TIMEOUT_MILLIS: u64 = 500;
const LEADER_WAIT_APPEND_ENTRIES_RESPONSE_TIMEOUT_MILLIS: u64 = 500;
const FOLLOWER_ELECTION_TIMEOUT_MILLIS: u64 = 1000;
const FOLLOWER_ELECTION_TIMEOUT_JITTER_MILLIS: u64 = 5;
const CANDIDATE_ELECTION_TIMEOUT_MILLIS: u64 = 1000;
const CANDIDATE_ELECTION_TIMEOUT_JITTER_MILLIS: u64 = 5;

#[derive(Clone, Default)]
pub struct Configuration {
    pub group: GroupConfiguration,
    pub leader: LeaderConfiguration,
    pub candidate: CandidateConfiguration,
    pub follower: FollowerConfiguration,
}

#[derive(Clone, Default)]
pub struct GroupConfiguration {
    pub nodes: HashMap<NodeId, NodeConfiguration>,
}

#[derive(Clone)]
pub struct NodeConfiguration {
    pub ip: IpAddr,
    pub raft_port: u16,
    pub repc_port: u16,
}

impl NodeConfiguration {
    pub fn new<I: Into<IpAddr>>(ip: I, raft_port: u16, repc_port: u16) -> Self {
        Self {
            ip: ip.into(),
            raft_port,
            repc_port,
        }
    }
}

#[derive(Clone)]
pub struct LeaderConfiguration {
    pub heartbeat_timeout_millis: u64,
    pub wait_append_entries_response_timeout_millis: u64,
}

impl Default for LeaderConfiguration {
    fn default() -> Self {
        Self {
            heartbeat_timeout_millis: LEADER_HEARTBEAT_TIMEOUT_MILLIS,
            wait_append_entries_response_timeout_millis:
                LEADER_WAIT_APPEND_ENTRIES_RESPONSE_TIMEOUT_MILLIS,
        }
    }
}

#[derive(Clone)]
pub struct CandidateConfiguration {
    pub election_timeout_millis: u64,
    pub election_timeout_jitter_millis: u64,
}

impl Default for CandidateConfiguration {
    fn default() -> Self {
        Self {
            election_timeout_millis: CANDIDATE_ELECTION_TIMEOUT_MILLIS,
            election_timeout_jitter_millis: CANDIDATE_ELECTION_TIMEOUT_JITTER_MILLIS,
        }
    }
}

#[derive(Clone)]
pub struct FollowerConfiguration {
    pub election_timeout_millis: u64,
    pub election_timeout_jitter_millis: u64,
}

impl Default for FollowerConfiguration {
    fn default() -> Self {
        Self {
            election_timeout_millis: FOLLOWER_ELECTION_TIMEOUT_MILLIS,
            election_timeout_jitter_millis: FOLLOWER_ELECTION_TIMEOUT_JITTER_MILLIS,
        }
    }
}
