use crate::configuration::Configuration;
use crate::node::Node;
use crate::peer::error::PeerError;
use crate::peer::partitioned::{self, PartitionedPeer, PartitionedPeerController};
use crate::peer::service::ServicePeer;
use crate::peer::Peer;
use crate::service::RaftService;
use crate::types::NodeId;
use std::collections::HashMap;

pub struct PartitionedLocalRaftGroup {
    confs: Vec<Configuration>,
}

impl PartitionedLocalRaftGroup {
    pub fn new(confs: Vec<Configuration>) -> Self {
        PartitionedLocalRaftGroup { confs }
    }

    pub fn spawn(self) -> PartitionedLocalRaftGroupController<impl Peer> {
        let nodes: Vec<Node<PartitionedPeer>> = self
            .confs
            .into_iter()
            .enumerate()
            .map(|(i, conf)| Node::new(i as NodeId + 1, conf))
            .collect();
        let services: Vec<RaftService> = nodes
            .iter()
            .map(|node| RaftService::new(node.tx()))
            .collect();
        let n = nodes.len() as NodeId;

        let mut peers = vec![];
        let mut controllers = HashMap::new();
        for i in 1..=n {
            let i = i as NodeId;
            let mut ps = HashMap::new();
            let mut cs = HashMap::new();
            for (j, service) in services.iter().enumerate() {
                let j = (j + 1) as NodeId;
                if i == j {
                    continue;
                }
                let inner = ServicePeer::new(service.clone());
                let (peer, controller) = partitioned::peer(inner, 10);
                ps.insert(j as NodeId, peer);
                cs.insert(j as NodeId, controller);
            }
            peers.push(ps);
            controllers.insert(i, cs);
        }

        for (node, peers) in nodes.into_iter().zip(peers.into_iter()) {
            tokio::spawn(node.run(peers));
        }

        PartitionedLocalRaftGroupController { controllers }
    }
}

pub struct PartitionedLocalRaftGroupController<P: Peer + Send + Sync> {
    controllers: HashMap<NodeId, HashMap<NodeId, PartitionedPeerController<P>>>,
}

impl<P: Peer + Send + Sync> PartitionedLocalRaftGroupController<P> {
    pub async fn pass(&mut self, i: NodeId, j: NodeId) -> Result<partitioned::ReqItem, PeerError> {
        self.controllers
            .get_mut(&i)
            .unwrap()
            .get_mut(&j)
            .unwrap()
            .pass()
            .await
    }

    pub async fn discard(
        &mut self,
        i: NodeId,
        j: NodeId,
    ) -> Result<partitioned::ReqItem, PeerError> {
        self.controllers
            .get_mut(&i)
            .unwrap()
            .get_mut(&j)
            .unwrap()
            .discard()
            .await
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::configuration::*;
    use crate::pb::{
        AppendEntriesRequest, AppendEntriesResponse, RequestVoteRequest, RequestVoteResponse,
    };
    use crate::peer::partitioned::ReqItem;

    fn init() {
        let _ = env_logger::builder().is_test(true).try_init();
    }

    #[tokio::test]
    async fn initial_election() {
        init();
        // tokio::time::pause();
        // tokio::time::advance(Duration::from_millis(1)).await;
        let forever = 1000 * 60 * 60 * 24 * 365;
        let conf1 = Configuration {
            leader: LeaderConfiguration {
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

        let group = PartitionedLocalRaftGroup::new(vec![conf1, conf2, conf3]);
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
}
