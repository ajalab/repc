use repc_common::repc::types::NodeId;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::net::IpAddr;

const LEADER_HEARTBEAT_TIMEOUT_MILLIS: u64 = 500;
const LEADER_WAIT_APPEND_ENTRIES_RESPONSE_TIMEOUT_MILLIS: u64 = 500;
const FOLLOWER_ELECTION_TIMEOUT_MILLIS: u64 = 1000;
const FOLLOWER_ELECTION_TIMEOUT_JITTER_MILLIS: u64 = 5;
const CANDIDATE_ELECTION_TIMEOUT_MILLIS: u64 = 1000;
const CANDIDATE_ELECTION_TIMEOUT_JITTER_MILLIS: u64 = 5;

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct Configuration {
    #[serde(default)]
    pub group: GroupConfiguration,
    #[serde(default)]
    pub leader: LeaderConfiguration,
    #[serde(default)]
    pub candidate: CandidateConfiguration,
    #[serde(default)]
    pub follower: FollowerConfiguration,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct GroupConfiguration {
    #[serde(default)]
    pub nodes: HashMap<NodeId, NodeConfiguration>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
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

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct LeaderConfiguration {
    #[serde(default)]
    pub heartbeat_timeout_millis: u64,
    #[serde(default)]
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

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct CandidateConfiguration {
    #[serde(default)]
    pub election_timeout_millis: u64,
    #[serde(default)]
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

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct FollowerConfiguration {
    #[serde(default)]
    pub election_timeout_millis: u64,
    #[serde(default)]
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
