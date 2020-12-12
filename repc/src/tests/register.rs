use super::{
    app::IncrState,
    util::{
        configuration::{follower_wannabee, leader_wannabee},
        init,
    },
};
use crate::group::partitioned::PartitionedLocalRepcGroupBuilder;

#[tokio::test]
async fn register() {
    init();
    let conf1 = leader_wannabee();
    let conf2 = follower_wannabee();
    let conf3 = follower_wannabee();

    let group = PartitionedLocalRepcGroupBuilder::default()
        .confs(vec![conf1, conf2, conf3])
        .initial_states(vec![IncrState::default(); 3])
        .build();
    let mut handle = group.spawn();

    // Node 1 collects votes from 2 and becomes a leader
    handle.expect_request_vote_success(1, 2).await;
    handle.block_request_vote_request(1, 3).await.unwrap();

    // Node 1 sends initial heartbeats to 2, 3
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
}
