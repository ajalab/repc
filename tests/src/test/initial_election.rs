use crate::util::init;
use crate::{
    app::adder::{pb::adder_server::AdderStateMachine, AdderState},
    util::configuration::{follower_wannabee, leader_wannabee},
};
use repc::test_util::{
    partitioned::group::PartitionedLocalRepcGroupBuilder,
    pb::{AppendEntriesRequest, RequestVoteRequest, RequestVoteResponse},
};

#[tokio::test]
async fn initial_election() {
    init();
    let conf1 = leader_wannabee();
    let conf2 = follower_wannabee();
    let conf3 = follower_wannabee();

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
