use super::app::{IncrRequest, IncrResponse, IncrState};
use super::init;
use crate::configuration::*;
use crate::group::partitioned::PartitionedLocalRepcGroupBuilder;

#[tokio::test]
async fn send_command() {
    init();
    let forever = 1000 * 60 * 60 * 24 * 365;
    let conf1 = Configuration {
        leader: LeaderConfiguration {
            wait_append_entries_response_timeout_millis: forever,
            heartbeat_timeout_millis: forever,
        },
        follower: FollowerConfiguration {
            election_timeout_millis: 0,
            election_timeout_jitter_millis: 0,
        },
        ..Default::default()
    };
    let conf2 = Configuration {
        candidate: CandidateConfiguration {
            election_timeout_millis: forever,
            ..Default::default()
        },
        follower: FollowerConfiguration {
            election_timeout_millis: forever,
            ..Default::default()
        },
        ..Default::default()
    };
    let conf3 = conf2.clone();

    let group = PartitionedLocalRepcGroupBuilder::default()
        .confs(vec![conf1, conf2, conf3])
        .initial_states(vec![IncrState::default(); 3])
        .build();
    let mut handle = group.spawn();

    // Node 1 collects votes from 2 and becomes a leader
    let (_, h) = handle.pass_request_vote_request(1, 2).await.unwrap();
    handle.block_request_vote_request(1, 3).await.unwrap();
    h.pass_response().unwrap().unwrap();

    // Node 1 sends initial heartbeats to 2, 3
    let (_, h) = handle.pass_append_entries_request(1, 2).await.unwrap();
    h.pass_response().unwrap().unwrap();
    let (_, h) = handle.pass_append_entries_request(1, 3).await.unwrap();
    h.pass_response().unwrap().unwrap();

    // Send a command to node 2
    let mut handle2 = handle.raft_handle(1, 2).clone();
    tokio::spawn(async move {
        let (_, h) = handle2.pass_append_entries_request().await.unwrap();
        h.pass_response().unwrap().unwrap();
    });

    // Send a command to node 3
    let mut handle3 = handle.raft_handle(1, 3).clone();
    tokio::spawn(async move {
        let (_, h) = handle3.pass_append_entries_request().await.unwrap();
        h.pass_response().unwrap().unwrap();
    });

    let res: Result<tonic::Response<IncrResponse>, tonic::Status> = handle
        .unary(1, "/incr.Incr/Incr", IncrRequest { i: 10 })
        .await;

    assert_eq!(IncrResponse { n: 10 }, res.unwrap().into_inner());
}
