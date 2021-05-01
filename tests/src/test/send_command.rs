use crate::app::adder::{
    pb::{adder_client::AdderClient, adder_server::AdderStateMachine, AddRequest, AddResponse},
    AdderState,
};
use crate::util::{init, partitioned_group};
use repc::{
    log::in_memory::InMemoryLog,
    test_util::{
        partitioned::group::{PartitionedLocalRepcGroup, PartitionedLocalRepcGroupHandle},
        pb::raft::raft_server::Raft,
        service::repc::RepcService,
    },
};
use repc_common::repc::repc_server::RepcServer;

async fn make_1_leader<R: Raft + Clone>(
    handle: &mut PartitionedLocalRepcGroupHandle<R>,
) -> AdderClient<RepcServer<RepcService>> {
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
    futures::join!(
        AdderClient::register(RepcServer::new(service)),
        h12.expect_append_entries_success(),
        h13.expect_append_entries_success(),
    )
    .0
    .expect("should be ok")
}

#[tokio::test]
async fn healthy() {
    init();
    let group: PartitionedLocalRepcGroup<AdderStateMachine<AdderState>, InMemoryLog> =
        partitioned_group(3);
    let mut handle = group.spawn();
    let mut h12 = handle.raft_handle(1, 2).clone();
    let mut h13 = handle.raft_handle(1, 3).clone();

    let mut client = make_1_leader(&mut handle).await;

    let res = futures::join!(
        client.add(AddRequest { i: 10 }),
        h12.expect_append_entries_success(),
        h13.expect_append_entries_success(),
    )
    .0;
    assert_eq!(AddResponse { n: 10 }, res.unwrap().into_inner());

    let res = futures::join!(
        client.add(AddRequest { i: 20 }),
        h12.expect_append_entries_success(),
        h13.expect_append_entries_success(),
    )
    .0;
    assert_eq!(AddResponse { n: 30 }, res.unwrap().into_inner());
}

#[tokio::test]
async fn block_append_entries_request_minor() {
    init();
    let group: PartitionedLocalRepcGroup<AdderStateMachine<AdderState>, InMemoryLog> =
        partitioned_group(3);
    let mut handle = group.spawn();
    let mut h12 = handle.raft_handle(1, 2).clone();
    let mut h13 = handle.raft_handle(1, 3).clone();

    let mut client = make_1_leader(&mut handle).await;

    let res = futures::join!(
        client.add(AddRequest { i: 10 }),
        h12.expect_append_entries_success(),
        h13.block_append_entries_request(),
    )
    .0;
    assert_eq!(AddResponse { n: 10 }, res.unwrap().into_inner());
}

/// TODO: Fail AppendEntries after retry count reaches the limit
#[tokio::test]
#[ignore]
async fn block_append_entries_request_major() {
    init();
    let group: PartitionedLocalRepcGroup<AdderStateMachine<AdderState>, InMemoryLog> =
        partitioned_group(3);
    let mut handle = group.spawn();
    let mut h12 = handle.raft_handle(1, 2).clone();
    let mut h13 = handle.raft_handle(1, 3).clone();

    let mut client = make_1_leader(&mut handle).await;

    // Majority of nodes fail
    let res = futures::join!(
        client.add(AddRequest { i: 10 }),
        h12.block_append_entries_request(),
        h13.block_append_entries_request(),
    )
    .0;
    assert!(res.is_err());
}
