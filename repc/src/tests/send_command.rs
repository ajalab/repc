use super::app::{IncrRequest, IncrResponse, IncrState};
use super::init;
use crate::configuration::*;
use crate::group::partitioned::PartitionedLocalRepcGroupBuilder;
use crate::raft::pb::{AppendEntriesRequest, AppendEntriesResponse};

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
    let mut controller = group.spawn();

    // Node 1 collects votes from 2 and becomes a leader
    controller.pass_request(1, 2).await.unwrap();
    controller.discard_request(1, 3).await.unwrap();
    controller.pass_response(2, 1).await.unwrap();

    // Node 1 sends initial heartbeats to 2, 3
    controller.pass_request(1, 2).await.unwrap();
    controller.pass_request(1, 3).await.unwrap();
    controller.pass_response(2, 1).await.unwrap();
    controller.pass_response(3, 1).await.unwrap();

    // Send a command to node 1
    controller
        .pass_next_request(1, 2, |req| {
            assert!(matches!(
                req.unwrap_append_entries(),
                AppendEntriesRequest { .. }
            ))
        })
        .await;
    controller
        .pass_next_response(2, 1, |res| {
            assert!(matches!(
                res.unwrap_append_entries(),
                AppendEntriesResponse { .. }
            ))
        })
        .await;
    controller
        .pass_next_request(1, 3, |req| {
            assert!(matches!(
                req.unwrap_append_entries(),
                AppendEntriesRequest { .. }
            ))
        })
        .await;
    controller
        .pass_next_response(3, 1, |res| {
            assert!(matches!(
                res.unwrap_append_entries(),
                AppendEntriesResponse { .. }
            ))
        })
        .await;
    let res: Result<tonic::Response<IncrResponse>, tonic::Status> = controller
        .unary(1, "/incr.Incr/Incr", IncrRequest { i: 10 })
        .await;

    assert_eq!(IncrResponse { n: 10 }, res.unwrap().into_inner());
}
