use crate::app::adder::{
    pb::{adder_server::AdderStateMachine, AddRequest, AddResponse},
    AdderState,
};
use crate::util::{
    configuration::{follower_wannabee, leader_wannabee},
    init,
};
use repc::test_util::partitioned::group::{
    PartitionedLocalRepcGroup, PartitionedLocalRepcGroupBuilder,
};

fn group_leader_1() -> PartitionedLocalRepcGroup<AdderStateMachine<AdderState>> {
    let conf1 = leader_wannabee();
    let conf2 = follower_wannabee();
    let conf3 = follower_wannabee();

    let state_machines = (0..3)
        .map(|_| AdderStateMachine::new(AdderState::default()))
        .collect::<Vec<_>>();
    PartitionedLocalRepcGroupBuilder::new()
        .confs(vec![conf1, conf2, conf3])
        .state_machines(state_machines)
        .build()
}

#[tokio::test]
async fn send_command_healthy() {
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
    let res: Result<tonic::Response<AddResponse>, tonic::Status> = handle
        .repc_client_mut(1)
        .unary("/adder.Adder/Add", AddRequest { i: 10 })
        .await;

    assert_eq!(AddResponse { n: 10 }, res.unwrap().into_inner());
}

#[tokio::test]
async fn send_command_failure_noncritical() {
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

    // Only a few nodes (not majority) fail
    let mut h = handle.raft_handle(1, 2).clone();
    tokio::spawn(async move { h.expect_append_entries_success().await });
    let mut h = handle.raft_handle(1, 3).clone();
    tokio::spawn(async move { h.block_append_entries_request().await });

    let res: Result<tonic::Response<AddResponse>, tonic::Status> = handle
        .repc_client_mut(1)
        .unary("/adder.Adder/Add", AddRequest { i: 10 })
        .await;

    assert_eq!(AddResponse { n: 10 }, res.unwrap().into_inner());
}

#[tokio::test]
async fn send_command_failure_critical() {
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

    // Majority of nodes fail
    let mut h = handle.raft_handle(1, 2).clone();
    tokio::spawn(async move { h.block_append_entries_request().await });
    let mut h = handle.raft_handle(1, 3).clone();
    tokio::spawn(async move { h.block_append_entries_request().await });

    let res: Result<tonic::Response<AddResponse>, tonic::Status> = handle
        .repc_client_mut(1)
        .unary("/adder.Adder/Add", AddRequest { i: 10 })
        .await;

    assert!(res.is_err());
}
