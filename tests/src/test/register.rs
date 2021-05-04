/*
use crate::{
    app::adder::{
        pb::{adder_client::AdderClient, adder_server::AdderStateMachine},
        AdderState,
    },
    util::{init, partitioned_group},
};
use repc::{log::in_memory::InMemoryLog, test_util::partitioned::group::PartitionedLocalRepcGroup};
use repc_common::pb::repc::repc_server::RepcServer;

#[tokio::test]
async fn register() {
    init();
    let group: PartitionedLocalRepcGroup<AdderStateMachine<AdderState>, InMemoryLog> =
        partitioned_group(3);
    let mut handle = group.spawn();
    let mut h12 = handle.raft_handle(1, 2).clone();
    let mut h13 = handle.raft_handle(1, 3).clone();

    let _ = handle.force_election_timeout(1).await;

    // Node 1 collects votes from 2 and becomes a leader
    handle.expect_request_vote_success(1, 2).await;
    handle.block_request_vote_request(1, 3).await.unwrap();

    // Node 1 sends initial heartbeats to 2, 3
    handle.expect_append_entries_success(1, 2).await;
    handle.expect_append_entries_success(1, 3).await;

    // Register
    // let service = handle.repc_service(1).clone();
    let client = AdderClient::with_services(
        handle
            .repc_services()
            .map(|(&id, s)| (id, RepcServer::new(s.clone()))),
    );
    futures::join!(
        client.add(AddRequest { i: 20 }),
        h12.expect_append_entries_success(),
        h13.expect_append_entries_success(),
    )
    .0
    .expect("should be ok");
}

*/
