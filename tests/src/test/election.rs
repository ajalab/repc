use crate::app::adder::{pb::adder_server::AdderStateMachine, AdderState};
use crate::util::{init, partitioned_group};
use repc::{
    log::in_memory::InMemoryLog,
    test_util::{
        partitioned::group::PartitionedLocalRepcGroup,
        pb::raft::{AppendEntriesRequest, RequestVoteRequest, RequestVoteResponse},
    },
};

#[tokio::test]
async fn success_one_candidate() {
    init();
    let group: PartitionedLocalRepcGroup<AdderStateMachine<AdderState>, InMemoryLog> =
        partitioned_group(3);
    let mut handle = group.spawn();

    let _ = handle.force_election_timeout(1).await;

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
    assert!(matches!(
        req.into_inner(),
        AppendEntriesRequest {
            term: 2,
            prev_log_index: 0,
            prev_log_term: 0,
            ..
        }
    ));
}

#[tokio::test]
async fn success_two_candidates() {
    init();
    let group: PartitionedLocalRepcGroup<AdderStateMachine<AdderState>, InMemoryLog> =
        partitioned_group(3);
    let mut handle = group.spawn();

    // Node 1, 2 become candidate
    let _ = handle.force_election_timeout(1).await;
    let _ = handle.force_election_timeout(2).await;

    // RequestVote from 1 to 2 must be rejected
    let (req, h12) = handle.pass_request_vote_request(1, 2).await.unwrap();
    assert!(matches!(
        req.into_inner(),
        RequestVoteRequest { term: 2, .. }
    ));
    assert!(matches!(
        h12.pass_response().unwrap().unwrap().into_inner(),
        RequestVoteResponse {
            term: 2,
            vote_granted: false
        }
    ));

    // RequestVote from 1 to 3 must be successful
    let (req, h13) = handle.pass_request_vote_request(1, 3).await.unwrap();
    assert!(matches!(
        req.into_inner(),
        RequestVoteRequest { term: 2, .. }
    ));
    assert!(matches!(
        h13.pass_response().unwrap().unwrap().into_inner(),
        RequestVoteResponse {
            term: 2,
            vote_granted: true,
        }
    ));

    // RequestVote from 2 to 1 must be rejected
    let (req, h21) = handle.pass_request_vote_request(2, 1).await.unwrap();
    assert!(matches!(
        req.into_inner(),
        RequestVoteRequest { term: 2, .. }
    ));
    assert!(matches!(
        h21.pass_response().unwrap().unwrap().into_inner(),
        RequestVoteResponse {
            term: 2,
            vote_granted: false,
        }
    ));

    // RequestVote from 2 to 3 must be rejected
    let (req, h23) = handle.pass_request_vote_request(2, 3).await.unwrap();
    assert!(matches!(
        req.into_inner(),
        RequestVoteRequest { term: 2, .. }
    ));
    assert!(matches!(
        h23.pass_response().unwrap().unwrap().into_inner(),
        RequestVoteResponse {
            term: 2,
            vote_granted: false,
        }
    ));

    let (req, _) = handle.pass_append_entries_request(1, 2).await.unwrap();
    assert!(matches!(
        req.into_inner(),
        AppendEntriesRequest { term: 2, .. }
    ));

    let (req, _) = handle.pass_append_entries_request(1, 3).await.unwrap();
    assert!(matches!(req
        .into_inner(),
        AppendEntriesRequest { term: 2, .. }
    ));
}
