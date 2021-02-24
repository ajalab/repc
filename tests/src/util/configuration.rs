use repc::configuration::*;

const FOREVER: u64 = 1000 * 60 * 60 * 24 * 365;

pub fn never_election_timeout() -> Configuration {
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
