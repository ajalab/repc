use crate::app::adder::{
    pb::{adder_client::AdderClient, adder_server::AdderStateMachine, AddRequest, AddResponse},
    AdderState,
};
use crate::util::{init, partitioned_group};
use repc::state::log::in_memory::InMemoryLog;
use repc::test_util::partitioned::group::PartitionedLocalRepcGroup;
use repc_proto::repc::repc_server::RepcServer;

#[tokio::test]
async fn send_command_healthy() {
    init();
    let group: PartitionedLocalRepcGroup<AdderStateMachine<AdderState>, InMemoryLog> =
        partitioned_group(3);
    let mut handle = group.spawn();

    let _ = handle.force_election_timeout(1).await;

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
    let service = handle.repc_service(1).clone();
    let mut client = AdderClient::register(RepcServer::new(service))
        .await
        .expect("should be ok");

    // Send a command (1)
    for &i in &[2, 3] {
        let mut h = handle.raft_handle(1, i).clone();
        tokio::spawn(async move { h.expect_append_entries_success().await });
    }
    let res: Result<tonic::Response<AddResponse>, tonic::Status> =
        client.add(AddRequest { i: 10 }).await;

    assert_eq!(AddResponse { n: 10 }, res.unwrap().into_inner());

    // Send a command (2)
    for &i in &[2, 3] {
        let mut h = handle.raft_handle(1, i).clone();
        tokio::spawn(async move { h.expect_append_entries_success().await });
    }
    let res: Result<tonic::Response<AddResponse>, tonic::Status> =
        client.add(AddRequest { i: 20 }).await;

    assert_eq!(AddResponse { n: 30 }, res.unwrap().into_inner());
}

#[tokio::test]
async fn send_command_failure_noncritical() {
    init();
    let group: PartitionedLocalRepcGroup<AdderStateMachine<AdderState>, InMemoryLog> =
        partitioned_group(3);
    let mut handle = group.spawn();

    let _ = handle.force_election_timeout(1).await;

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
    let service = handle.repc_service(1).clone();
    let mut client = AdderClient::register(RepcServer::new(service))
        .await
        .expect("should be ok");

    // Only a few nodes (not majority) fail
    let mut h = handle.raft_handle(1, 2).clone();
    tokio::spawn(async move { h.expect_append_entries_success().await });
    let mut h = handle.raft_handle(1, 3).clone();
    tokio::spawn(async move { h.block_append_entries_request().await });

    let res: Result<tonic::Response<AddResponse>, tonic::Status> =
        client.add(AddRequest { i: 10 }).await;

    assert_eq!(AddResponse { n: 10 }, res.unwrap().into_inner());
}

#[tokio::test]
async fn send_command_failure_critical() {
    init();
    let group: PartitionedLocalRepcGroup<AdderStateMachine<AdderState>, InMemoryLog> =
        partitioned_group(3);
    let mut handle = group.spawn();

    let _ = handle.force_election_timeout(1).await;

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
    let service = handle.repc_service(1).clone();
    let mut client = AdderClient::register(RepcServer::new(service))
        .await
        .expect("should be ok");

    // Majority of nodes fail
    let mut h = handle.raft_handle(1, 2).clone();
    tokio::spawn(async move { h.block_append_entries_request().await });
    let mut h = handle.raft_handle(1, 3).clone();
    tokio::spawn(async move { h.block_append_entries_request().await });

    let res: Result<tonic::Response<AddResponse>, tonic::Status> =
        client.add(AddRequest { i: 10 }).await;

    assert!(res.is_err());
}
