use super::app::{IncrRequest, IncrResponse, IncrState};
use super::util::{
    configuration::{follower_wannabee, leader_wannabee},
    init,
};
use crate::group::partitioned::{PartitionedLocalRepcGroup, PartitionedLocalRepcGroupBuilder};

fn group_leader_1() -> PartitionedLocalRepcGroup<IncrState> {
    let conf1 = leader_wannabee();
    let conf2 = follower_wannabee();
    let conf3 = follower_wannabee();

    PartitionedLocalRepcGroupBuilder::default()
        .confs(vec![conf1, conf2, conf3])
        .initial_states(vec![IncrState::default(); 3])
        .build()
}

#[tokio::test]
async fn send_command() {
    init();
    let group = group_leader_1();
    let mut handle = group.spawn();

    // Node 1 collects votes and becomes a leader
    handle.expect_request_vote_success(1, 2).await;
    handle.expect_request_vote_success(1, 3).await;

    // Node 1 sends initial heartbeats to others
    handle.expect_append_entries_success(1, 2).await;
    handle.expect_append_entries_success(1, 3).await;

    // Register
    for &i in &[2, 3] {
        let mut h = handle.raft_handle(1, i).clone();
        tokio::spawn(async move { h.expect_append_entries_success().await });
    }
    handle
        .repc_client_mut(1)
        .register()
        .await
        .expect("should be ok");

    // Send a command
    for &i in &[2, 3] {
        let mut h = handle.raft_handle(1, i).clone();
        tokio::spawn(async move { h.expect_append_entries_success().await });
    }
    let res: Result<tonic::Response<IncrResponse>, tonic::Status> = handle
        .repc_client_mut(1)
        .unary("/incr.Incr/Incr", IncrRequest { i: 10 })
        .await;

    assert_eq!(IncrResponse { n: 10 }, res.unwrap().into_inner());
}
