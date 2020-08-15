use crate::configuration::Configuration;
use crate::raft::node::Node;
use crate::raft::peer::error::PeerError;
use crate::raft::peer::partitioned::{self, RaftPartitionedPeer, RaftPartitionedPeerController};
use crate::raft::peer::service::RaftServicePeer;
use crate::raft::peer::RaftPeer;
use crate::raft::service::RaftService;
use crate::state_machine::{StateMachine, StateMachineManager};
use crate::types::NodeId;
use std::collections::HashMap;
use std::marker::PhantomData;
use std::sync::Arc;

pub struct PartitionedLocalRepcGroupBuilder<T> {
    confs: Vec<Configuration>,
    state_machines: T,
}

impl PartitionedLocalRepcGroupBuilder<()> {
    pub fn new() -> PartitionedLocalRepcGroupBuilder<()> {
        Self {
            confs: vec![],
            state_machines: (),
        }
    }
}

impl<S> Default for PartitionedLocalRepcGroupBuilder<PhantomData<S>> {
    fn default() -> Self {
        Self {
            state_machines: PhantomData,
            confs: Default::default(),
        }
    }
}

impl<T> PartitionedLocalRepcGroupBuilder<T> {
    pub fn state_machines(self, state_machines: T) -> Self {
        Self {
            state_machines,
            ..self
        }
    }

    pub fn confs(self, confs: Vec<Configuration>) -> Self {
        Self { confs, ..self }
    }
}

impl<S> PartitionedLocalRepcGroupBuilder<PhantomData<S>>
where
    S: StateMachine + Send + Default + 'static,
{
    pub fn build(self) -> PartitionedLocalRepcGroup<S> {
        let n = self.confs.len();
        PartitionedLocalRepcGroup {
            confs: self.confs,
            state_machines: (0..n).map(|_| S::default()).collect(),
        }
    }
}

impl<S> PartitionedLocalRepcGroupBuilder<S>
where
    S: StateMachine + Send + Clone + 'static,
{
    pub fn build(self) -> PartitionedLocalRepcGroup<S> {
        let n = self.confs.len();
        PartitionedLocalRepcGroup {
            confs: self.confs,
            state_machines: vec![self.state_machines; n],
        }
    }
}

impl<S> PartitionedLocalRepcGroupBuilder<Vec<S>>
where
    S: StateMachine + Send + Clone + 'static,
{
    pub fn build(self) -> PartitionedLocalRepcGroup<S> {
        debug_assert_eq!(self.confs.len(), self.state_machines.len());
        PartitionedLocalRepcGroup {
            confs: self.confs,
            state_machines: self.state_machines,
        }
    }
}
pub struct PartitionedLocalRepcGroup<S> {
    confs: Vec<Configuration>,
    state_machines: Vec<S>,
}

impl<S> PartitionedLocalRepcGroup<S>
where
    S: StateMachine + Send + 'static,
{
    pub fn spawn(self) -> PartitionedLocalRaftGroupController<impl RaftPeer> {
        let sm_managers = self
            .state_machines
            .into_iter()
            .map(|state_machine| StateMachineManager::spawn(state_machine))
            .collect::<Vec<_>>();
        let nodes: Vec<Node<RaftPartitionedPeer>> = self
            .confs
            .into_iter()
            .zip(sm_managers.into_iter())
            .enumerate()
            .map(|(i, (conf, smm_sender))| {
                Node::new(i as NodeId + 1, smm_sender).conf(Arc::new(conf))
            })
            .collect();
        let services: Vec<RaftService> = nodes
            .iter()
            .map(|node| RaftService::new(node.get_tx()))
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
                let inner = RaftServicePeer::new(service.clone());
                let (peer, controller) = partitioned::peer(inner, 10);
                ps.insert(j as NodeId, peer);
                cs.insert(j as NodeId, controller);
            }
            peers.push(ps);
            controllers.insert(i, cs);
        }

        for (node, peers) in nodes.into_iter().zip(peers.into_iter()) {
            tokio::spawn(node.peers(peers).run());
        }

        PartitionedLocalRaftGroupController { controllers }
    }
}

pub struct PartitionedLocalRaftGroupController<P: RaftPeer + Send + Sync> {
    controllers: HashMap<NodeId, HashMap<NodeId, RaftPartitionedPeerController<P>>>,
}

impl<P: RaftPeer + Send + Sync> PartitionedLocalRaftGroupController<P> {
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
    use crate::raft::pb::{AppendEntriesRequest, RequestVoteRequest, RequestVoteResponse};
    use crate::raft::peer::partitioned::ReqItem;
    use bytes::Bytes;

    #[derive(Default, Clone)]
    struct NoopStateMachine {}

    impl StateMachine for NoopStateMachine {
        fn apply<P: AsRef<str>>(&mut self, path: P, command: Bytes) {}
    }

    fn init() {
        let _ = env_logger::builder().is_test(true).try_init();
    }

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

        let group: PartitionedLocalRepcGroup<NoopStateMachine> =
            PartitionedLocalRepcGroupBuilder::default()
                .confs(vec![conf1, conf2, conf3])
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

        let group: PartitionedLocalRepcGroup<NoopStateMachine> =
            PartitionedLocalRepcGroupBuilder::default()
                .confs(vec![conf1, conf2, conf3])
                .build();
        let mut controller = group.spawn();

        controller.pass(1, 2).await;
        controller.discard(1, 3).await;
        controller.pass(1, 2).await;
        controller.pass(1, 2).await;

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
