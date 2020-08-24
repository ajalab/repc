use super::app::{Incr, IncrState};
use super::init;
use crate::configuration::*;
use crate::group::partitioned::PartitionedLocalRepcGroupBuilder;
use crate::raft::pb::{AppendEntriesRequest, RequestVoteRequest, RequestVoteResponse};
use crate::raft::peer::partitioned::ReqItem;

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
        controller.pass(1, 2).await,
        Ok(ReqItem::RequestVoteRequest {
            req:
                RequestVoteRequest {
                    term: 2,
                    last_log_index: 0,
                    ..
                },
        })
    ));

    assert!(matches!(
        controller.discard(1, 3).await,
        Ok(ReqItem::RequestVoteRequest {
            req:
                RequestVoteRequest {
                    term: 2,
                    last_log_index: 0,
                    ..
                },
        })
    ));

    assert!(matches!(
        controller.pass(1, 2).await,
        Ok(ReqItem::RequestVoteResponse {
            res: RequestVoteResponse {
                term: 2,
                vote_granted: true,
            }
        })
    ));

    assert!(matches!(
        controller.pass(1, 2).await,
        Ok(ReqItem::AppendEntriesRequest {
            req:
                AppendEntriesRequest {
                    term: 2,
                    prev_log_index: 0,
                    prev_log_term: 0,
                    ..
                },
        })
    ));

    assert!(matches!(
        controller.pass(1, 3).await,
        Ok(ReqItem::AppendEntriesRequest {
            req:
                AppendEntriesRequest {
                    term: 2,
                    prev_log_index: 0,
                    prev_log_term: 0,
                    ..
                },
        })
    ));
}
