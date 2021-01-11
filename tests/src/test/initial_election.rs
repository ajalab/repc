use crate::app::adder::{pb::adder_server::AdderStateMachine, AdderState};
use crate::util::init;
use repc::configuration::*;
use repc::test_util::{
    partitioned::group::PartitionedLocalRepcGroupBuilder,
    pb::{AppendEntriesRequest, RequestVoteRequest, RequestVoteResponse},
};

#[tokio::test]
async fn initial_election() {
    init();
    let forever = 1000 * 60 * 60 * 24 * 365;
    let conf1 = Configuration {
        leader: LeaderConfiguration {
            wait_append_entries_response_timeout_millis: forever,
            heartbeat_timeout_millis: forever,
        },
        follower: FollowerConfiguration {
            election_timeout_millis: 0,
            election_timeout_jitter_millis: 0,
        },
        ..Default::default()
    };
    let conf2 = Configuration {
        candidate: CandidateConfiguration {
            election_timeout_millis: forever,
            ..Default::default()
        },
        follower: FollowerConfiguration {
            election_timeout_millis: forever,
            ..Default::default()
        },
        ..Default::default()
    };
    let conf3 = conf2.clone();

    let state_machines = (0..3)
        .map(|_| AdderStateMachine::new(AdderState::default()))
        .collect::<Vec<_>>();
    let group = PartitionedLocalRepcGroupBuilder::new()
        .confs(vec![conf1, conf2, conf3])
        .state_machines(state_machines)
        .build();
    let mut handle = group.spawn();

    let (req, h1) = handle.pass_request_vote_request(1, 2).await.unwrap();
    assert!(matches!(
    req.into_inner(),
            RequestVoteRequest {
                term: 2,
                last_log_index: 0,
                ..
            }
        ));
    assert!(matches!(
        h1.pass_response().unwrap().unwrap().into_inner(),
        RequestVoteResponse {
            term: 2,
            vote_granted: true,
        }
    ));

    assert!(matches!(
        handle.block_request_vote_request(1, 3).await.unwrap().into_inner(),
        RequestVoteRequest {
            term: 2,
            last_log_index: 0,
            ..
        }
    ));

    let (req, _) = handle.pass_append_entries_request(1, 2).await.unwrap();
    assert!(matches!(
        req.into_inner(),
        AppendEntriesRequest {
            term: 2,
            prev_log_index: 0,
            prev_log_term: 0,
            ..
        }
    ));

    let (req, _) = handle.pass_append_entries_request(1, 3).await.unwrap();
    assert!(matches!(req
            .into_inner(),
        AppendEntriesRequest {
            term: 2,
            prev_log_index: 0,
            prev_log_term: 0,
            ..
        }
    ));
}
