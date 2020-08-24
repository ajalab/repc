use super::app::IncrState;
use super::init;
use crate::configuration::*;
use crate::group::partitioned::PartitionedLocalRepcGroupBuilder;
use crate::raft::pb::{AppendEntriesRequest, RequestVoteRequest, RequestVoteResponse};
use crate::raft::peer::partitioned::{Request, Response};

#[tokio::test]
async fn initial_election() {
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

    assert!(matches!(
        controller.pass_request(1, 2).await,
        Ok(Request::RequestVoteRequest(
            RequestVoteRequest {
                term: 2,
                last_log_index: 0,
                ..
            }
        ))
    ));

    assert!(matches!(
        controller.discard_request(1, 3).await,
        Ok(Request::RequestVoteRequest(
            RequestVoteRequest {
                term: 2,
                last_log_index: 0,
                ..
            }
        ))
    ));

    assert!(
        matches!(
            controller.pass_response(2, 1).await,
            Ok(Response::RequestVoteResponse(RequestVoteResponse {
                term: 2,
                vote_granted: true,
            }))
        )
    );

    assert!(matches!(
        controller.pass_request(1, 2).await,
        Ok(Request::AppendEntriesRequest (
            AppendEntriesRequest {
                term: 2,
                prev_log_index: 0,
                prev_log_term: 0,
                ..
            },
        ))
    ));

    assert!(matches!(
        controller.pass_request(1, 3).await,
        Ok(Request::AppendEntriesRequest (
            AppendEntriesRequest {
                term: 2,
                prev_log_index: 0,
                prev_log_term: 0,
                ..
            },
        ))
    ));
}