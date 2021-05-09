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
use repc_client::codegen::NodeId;
use repc_common::{metadata::status::StatusMetadata, pb::repc::repc_server::RepcServer};

async fn make_leader<R: Raft + Clone>(
    handle: &mut PartitionedLocalRepcGroupHandle<R>,
    leader_id: NodeId,
) {
    let mut hs = handle
        .nodes()
        .filter_map(|id| {
            if id == leader_id {
                None
            } else {
                Some(handle.raft_handle(leader_id, id).clone())
            }
        })
        .collect::<Vec<_>>();
    let _ = handle.force_election_timeout(leader_id).await;

    // Leader collects votes and becomes a leader
    futures::future::join_all(hs.iter_mut().map(|h| h.expect_request_vote_success())).await;

    // Leader sends initial heartbeats to others
    futures::future::join_all(hs.iter_mut().map(|h| h.expect_append_entries_success())).await;
}

fn get_services<I, R>(
    handle: &mut PartitionedLocalRepcGroupHandle<R>,
    ids: I,
) -> Vec<(NodeId, RepcServer<RepcService>)>
where
    R: Raft + Clone,
    I: IntoIterator<Item = NodeId>,
{
    ids.into_iter()
        .map(|id| (id, RepcServer::new(handle.repc_service(id).clone())))
        .collect()
}

#[tokio::test]
async fn success_0_partitions() {
    init();
    let group: PartitionedLocalRepcGroup<AdderStateMachine<AdderState>, InMemoryLog> =
        partitioned_group(3);
    let mut handle = group.spawn();
    let mut h12 = handle.raft_handle(1, 2).clone();
    let mut h13 = handle.raft_handle(1, 3).clone();
    let mut client1 = AdderClient::new(get_services(&mut handle, vec![1]));

    make_leader(&mut handle, 1).await;

    let res = futures::join!(
        client1.get_mut().register(),
        h12.expect_append_entries_success(),
        h13.expect_append_entries_success(),
    )
    .0;
    assert!(res.is_ok());

    let res = futures::join!(
        client1.add(AddRequest { i: 10 }),
        h12.expect_append_entries_success(),
        h13.expect_append_entries_success(),
    )
    .0;
    assert_eq!(AddResponse { n: 10 }, res.unwrap().into_inner());

    let res = futures::join!(
        client1.add(AddRequest { i: 20 }),
        h12.expect_append_entries_success(),
        h13.expect_append_entries_success(),
    )
    .0;
    assert_eq!(AddResponse { n: 30 }, res.unwrap().into_inner());
}

#[tokio::test]
async fn success_1_partitions() {
    init();
    let group: PartitionedLocalRepcGroup<AdderStateMachine<AdderState>, InMemoryLog> =
        partitioned_group(3);
    let mut handle = group.spawn();
    let mut h12 = handle.raft_handle(1, 2).clone();
    let mut h13 = handle.raft_handle(1, 3).clone();
    let mut client1 = AdderClient::new(get_services(&mut handle, vec![1]));

    make_leader(&mut handle, 1).await;

    let res = futures::join!(
        client1.get_mut().register(),
        h12.expect_append_entries_success(),
        h13.expect_append_entries_success(),
    )
    .0;
    assert!(res.is_ok());

    let res = futures::join!(
        client1.add(AddRequest { i: 10 }),
        h12.expect_append_entries_success(),
        h13.block_append_entries_request(),
    )
    .0;
    assert_eq!(AddResponse { n: 10 }, res.unwrap().into_inner());
}

/// TODO: Fail AppendEntries after retry count reaches the limit
#[tokio::test]
#[ignore]
async fn fail_2_partitions() {
    init();
    let group: PartitionedLocalRepcGroup<AdderStateMachine<AdderState>, InMemoryLog> =
        partitioned_group(3);
    let mut handle = group.spawn();
    let mut h12 = handle.raft_handle(1, 2).clone();
    let mut h13 = handle.raft_handle(1, 3).clone();

    make_leader(&mut handle, 1).await;
    let mut client1 = AdderClient::new(get_services(&mut handle, vec![1]));

    let res = futures::join!(
        client1.get_mut().register(),
        h12.expect_append_entries_success(),
        h13.expect_append_entries_success(),
    )
    .0;
    assert!(res.is_ok());

    // Majority of nodes fail
    let res = futures::join!(
        client1.add(AddRequest { i: 10 }),
        h12.block_append_entries_request(),
        h13.block_append_entries_request(),
    )
    .0;
    assert!(res.is_err());
}

#[tokio::test]
async fn fail_register_non_leader() {
    init();
    let group: PartitionedLocalRepcGroup<AdderStateMachine<AdderState>, InMemoryLog> =
        partitioned_group(3);
    let mut handle = group.spawn();
    let mut client2 = AdderClient::new(get_services(&mut handle, vec![2]));
    let mut client3 = AdderClient::new(get_services(&mut handle, vec![3]));

    make_leader(&mut handle, 1).await;

    let status = client2.get_mut().register().await.unwrap_err();
    assert_eq!(
        Ok(Some(Some(1))),
        StatusMetadata::decode_retry(status.metadata())
    );

    let status = client3.get_mut().register().await.unwrap_err();
    assert_eq!(
        Ok(Some(Some(1))),
        StatusMetadata::decode_retry(status.metadata())
    );
}

#[tokio::test]
async fn fail_command_non_leader() {
    init();
    let group: PartitionedLocalRepcGroup<AdderStateMachine<AdderState>, InMemoryLog> =
        partitioned_group(3);
    let mut handle = group.spawn();
    let mut h12 = handle.raft_handle(1, 2).clone();
    let mut h13 = handle.raft_handle(1, 3).clone();
    let mut client1 = AdderClient::new(get_services(&mut handle, vec![1]));
    let mut client2 = AdderClient::new(get_services(&mut handle, vec![2]));

    make_leader(&mut handle, 1).await;

    let res = futures::join!(
        client1.get_mut().register(),
        h12.expect_append_entries_success(),
        h13.expect_append_entries_success(),
    )
    .0;
    assert!(res.is_ok());

    let status = client2.add(AddRequest { i: 10 }).await.unwrap_err();
    assert_eq!(
        Ok(Some(Some(1))),
        StatusMetadata::decode_retry(status.metadata())
    );
}

#[tokio::test]
async fn success_2_elections() {
    init();
    let group: PartitionedLocalRepcGroup<AdderStateMachine<AdderState>, InMemoryLog> =
        partitioned_group(3);
    let mut handle = group.spawn();
    let mut h12 = handle.raft_handle(1, 2).clone();
    let mut h13 = handle.raft_handle(1, 3).clone();
    let mut h21 = handle.raft_handle(2, 1).clone();
    let mut h23 = handle.raft_handle(2, 3).clone();
    let mut client1 = AdderClient::new(get_services(&mut handle, vec![1]));
    let mut client2 = AdderClient::new(get_services(&mut handle, vec![2]));

    make_leader(&mut handle, 1).await;

    let res = futures::join!(
        client1.get_mut().register(),
        h12.expect_append_entries_success(),
        h13.expect_append_entries_success(),
    )
    .0;
    assert!(res.is_ok());

    let res = futures::join!(
        client1.add(AddRequest { i: 10 }),
        h12.expect_append_entries_success(),
        h13.expect_append_entries_success(),
    )
    .0;
    assert_eq!(AddResponse { n: 10 }, res.unwrap().into_inner());

    make_leader(&mut handle, 2).await;

    let res = futures::join!(
        client2.get_mut().register(),
        h21.expect_append_entries_success(),
        h23.expect_append_entries_success(),
    )
    .0;
    assert!(res.is_ok());

    let res = futures::join!(
        client2.add(AddRequest { i: 20 }),
        h21.expect_append_entries_success(),
        h23.expect_append_entries_success(),
    )
    .0;
    assert_eq!(AddResponse { n: 30 }, res.unwrap().into_inner());
}

#[tokio::test]
async fn success_retry() {
    init();

    let group: PartitionedLocalRepcGroup<AdderStateMachine<AdderState>, InMemoryLog> =
        partitioned_group(3);
    let mut handle = group.spawn();
    let mut h21 = handle.raft_handle(2, 1).clone();
    let mut h23 = handle.raft_handle(2, 3).clone();
    let mut client12 = AdderClient::new(get_services(&mut handle, vec![1, 2]));

    make_leader(&mut handle, 2).await;

    let res = futures::join!(
        client12.get_mut().register(),
        h21.expect_append_entries_success(),
        h23.expect_append_entries_success(),
    )
    .0;
    assert!(res.is_ok());
}

#[tokio::test]
#[ignore] // TODO: fix session
async fn success_retry_after_election() {
    init();

    let group: PartitionedLocalRepcGroup<AdderStateMachine<AdderState>, InMemoryLog> =
        partitioned_group(3);
    let mut handle = group.spawn();
    let mut h12 = handle.raft_handle(1, 2).clone();
    let mut h13 = handle.raft_handle(1, 3).clone();
    let mut h21 = handle.raft_handle(2, 1).clone();
    let mut h23 = handle.raft_handle(2, 3).clone();
    let mut client12 = AdderClient::new(get_services(&mut handle, vec![1, 2]));

    make_leader(&mut handle, 1).await;

    let res = futures::join!(
        client12.get_mut().register(),
        h12.expect_append_entries_success(),
        h13.expect_append_entries_success(),
    )
    .0;
    assert!(res.is_ok());

    let res = futures::join!(
        client12.add(AddRequest { i: 10 }),
        h12.expect_append_entries_success(),
        h13.block_append_entries_request(),
    )
    .0;
    assert_eq!(AddResponse { n: 10 }, res.unwrap().into_inner());

    make_leader(&mut handle, 2).await;

    let res = futures::join!(
        client12.add(AddRequest { i: 20 }),
        h21.expect_append_entries_success(),
        h23.block_append_entries_request(),
    )
    .0;
    assert_eq!(AddResponse { n: 30 }, res.unwrap().into_inner());
}
