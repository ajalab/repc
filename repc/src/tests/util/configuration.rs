use crate::configuration::*;

const FOREVER: u64 = 1000 * 60 * 60 * 24 * 365;

/// Returns a configuration to try being a candidate as soon as possible.
pub fn leader_wannabee() -> Configuration {
    Configuration {
        leader: LeaderConfiguration {
            wait_append_entries_response_timeout_millis: FOREVER,
            heartbeat_timeout_millis: FOREVER,
        },
        follower: FollowerConfiguration {
            election_timeout_millis: 0,
            election_timeout_jitter_millis: 0,
        },
        ..Default::default()
    }
}

/// Returns a configuration to try not being a candidate.
pub fn follower_wannabee() -> Configuration {
    Configuration {
        candidate: CandidateConfiguration {
            election_timeout_millis: FOREVER,
            ..Default::default()
        },
        follower: FollowerConfiguration {
            election_timeout_millis: FOREVER,
            ..Default::default()
        },
        ..Default::default()
    }
}
