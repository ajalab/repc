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
use std::sync::Arc;

#[derive(Default)]
pub struct PartitionedLocalRepcGroupBuilder<S> {
    confs: Vec<Configuration>,
    state_machines: Vec<S>,
}

impl<S> PartitionedLocalRepcGroupBuilder<S> {
    pub fn state_machines(self, state_machines: Vec<S>) -> Self {
        Self {
            state_machines,
            ..self
        }
    }

    pub fn confs(self, confs: Vec<Configuration>) -> Self {
        Self { confs, ..self }
    }
}

impl<S> PartitionedLocalRepcGroupBuilder<S>
where
    S: StateMachine + Send + 'static,
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
