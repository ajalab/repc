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
    let mut h12 = handle.raft_handle(1, 2).clone();
    let mut h13 = handle.raft_handle(1, 3).clone();

    let _ = handle.force_election_timeout(1).await;

    // Node 1 collects votes and becomes a leader
    futures::join!(
        h12.expect_request_vote_success(),
        h13.expect_request_vote_success(),
    );

    // Node 1 sends initial heartbeats to others
    futures::join!(
        h12.expect_append_entries_success(),
        h13.expect_append_entries_success(),
    );

    // Register
    let service = handle.repc_service(1).clone();
    let mut client = futures::join!(
        AdderClient::register(RepcServer::new(service)),
        h12.expect_append_entries_success(),
        h13.expect_append_entries_success(),
    )
    .0
    .expect("should be ok");

    // Send a command (1)
    let res = futures::join!(
        client.add(AddRequest { i: 10 }),
        h12.expect_append_entries_success(),
        h13.expect_append_entries_success(),
    )
    .0;
    assert_eq!(AddResponse { n: 10 }, res.unwrap().into_inner());

    // Send a command (2)
    let res = futures::join!(
        client.add(AddRequest { i: 20 }),
        h12.expect_append_entries_success(),
        h13.expect_append_entries_success(),
    )
    .0;
    assert_eq!(AddResponse { n: 30 }, res.unwrap().into_inner());
}

#[tokio::test]
async fn send_command_failure_noncritical() {
    init();
    let group: PartitionedLocalRepcGroup<AdderStateMachine<AdderState>, InMemoryLog> =
        partitioned_group(3);
    let mut handle = group.spawn();
    let mut h12 = handle.raft_handle(1, 2).clone();
    let mut h13 = handle.raft_handle(1, 3).clone();

    let _ = handle.force_election_timeout(1).await;

    // Node 1 collects votes and becomes a leader
    futures::join!(
        h12.expect_request_vote_success(),
        h13.expect_request_vote_success(),
    );

    // Node 1 sends initial heartbeats to others
    futures::join!(
        h12.expect_append_entries_success(),
        h13.expect_append_entries_success(),
    );

    // Register
    let service = handle.repc_service(1).clone();
    let mut client = futures::join!(
        AdderClient::register(RepcServer::new(service)),
        h12.expect_append_entries_success(),
        h13.expect_append_entries_success(),
    )
    .0
    .expect("should be ok");

    // Only a few nodes (not majority) fail
    let res = futures::join!(
        client.add(AddRequest { i: 10 }),
        h12.expect_append_entries_success(),
        h13.block_append_entries_request(),
    )
    .0;
    assert_eq!(AddResponse { n: 10 }, res.unwrap().into_inner());
}

#[tokio::test]
async fn send_command_failure_critical() {
    init();
    let group: PartitionedLocalRepcGroup<AdderStateMachine<AdderState>, InMemoryLog> =
        partitioned_group(3);
    let mut handle = group.spawn();
    let mut h12 = handle.raft_handle(1, 2).clone();
    let mut h13 = handle.raft_handle(1, 3).clone();

    let _ = handle.force_election_timeout(1).await;

    // Node 1 collects votes and becomes a leader
    futures::join!(
        h12.expect_request_vote_success(),
        h13.expect_request_vote_success(),
    );

    // Node 1 sends initial heartbeats to others
    futures::join!(
        h12.expect_append_entries_success(),
        h13.expect_append_entries_success(),
    );

    // Register
    let service = handle.repc_service(1).clone();
    let mut client = futures::join!(
        AdderClient::register(RepcServer::new(service)),
        h12.expect_append_entries_success(),
        h13.expect_append_entries_success(),
    )
    .0
    .expect("should be ok");

    // Majority of nodes fail
    let res = futures::join!(
        client.add(AddRequest { i: 10 }),
        h12.block_append_entries_request(),
        h13.block_append_entries_request(),
    )
    .0;
    assert!(res.is_err());
}
