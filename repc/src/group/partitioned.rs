use crate::configuration::Configuration;
use crate::raft::node::Node;
use crate::raft::peer::error::PeerError;
use crate::raft::peer::partitioned::{self, RaftPartitionedPeer, RaftPartitionedPeerController};
use crate::raft::peer::service::RaftServicePeer;
use crate::raft::peer::RaftPeer;
use crate::raft::service::RaftService;
use crate::service::RepcService;
use crate::state_machine::{StateMachine, StateMachineManager};
use crate::types::NodeId;
use bytes::BytesMut;
use std::collections::HashMap;
use std::sync::Arc;
use tonic::body::BoxBody;
use tonic::server::UnaryService;
use tonic::Status;
use tower_service::Service;

#[derive(Default)]
pub struct PartitionedLocalRepcGroupBuilder<S> {
    confs: Vec<Configuration>,
    initial_states: Vec<S>,
}

impl<S> PartitionedLocalRepcGroupBuilder<S> {
    pub fn initial_states(self, initial_states: Vec<S>) -> Self {
        Self {
            initial_states,
            ..self
        }
    }

    pub fn confs(self, confs: Vec<Configuration>) -> Self {
        Self { confs, ..self }
    }

    pub fn build(self) -> PartitionedLocalRepcGroup<S> {
        debug_assert_eq!(self.confs.len(), self.initial_states.len());
        PartitionedLocalRepcGroup {
            confs: self.confs,
            state_machines: self.initial_states,
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
    pub fn spawn(self) -> PartitionedLocalRaftGroupController<S::Service, impl RaftPeer> {
        let sm_managers = self
            .state_machines
            .into_iter()
            .map(|state_machine| StateMachineManager::spawn(state_machine))
            .collect::<Vec<_>>();
        let nodes: Vec<Node<RaftPartitionedPeer<_>>> = self
            .confs
            .into_iter()
            .zip(sm_managers.into_iter())
            .enumerate()
            .map(|(i, (conf, smm_sender))| {
                Node::new(i as NodeId + 1, smm_sender).conf(Arc::new(conf))
            })
            .collect();
        let raft_services: Vec<RaftService> = nodes
            .iter()
            .map(|node| RaftService::new(node.get_tx()))
            .collect();
        let repc_services: HashMap<NodeId, S::Service> = nodes
            .iter()
            .enumerate()
            .map(|(i, node)| (i as NodeId + 1, S::Service::from_tx(node.get_tx())))
            .collect();
        let n = nodes.len() as NodeId;

        let mut peers = vec![];
        let mut controllers = HashMap::new();
        for i in 1..=n {
            let i = i as NodeId;
            let mut ps = HashMap::new();
            let mut cs = HashMap::new();
            for (j, service) in raft_services.iter().enumerate() {
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

        PartitionedLocalRaftGroupController {
            controllers,
            repc_services,
        }
    }
}

pub struct PartitionedLocalRaftGroupController<S, P> {
    controllers: HashMap<NodeId, HashMap<NodeId, RaftPartitionedPeerController<P>>>,
    repc_services: HashMap<NodeId, S>,
}

impl<S, P> PartitionedLocalRaftGroupController<S, P>
where
    S: RepcService + Service<http::Request<BoxBody>>,
    P: RaftPeer + Send + Sync,
{
    fn peer(&mut self, i: NodeId, j: NodeId) -> &mut RaftPartitionedPeerController<P> {
        self.controllers.get_mut(&i).unwrap().get_mut(&j).unwrap()
    }

    pub async fn pass_request(
        &mut self,
        i: NodeId,
        j: NodeId,
    ) -> Result<partitioned::Request, PeerError> {
        self.peer(i, j).pass_request().await
    }

    pub async fn pass_response(
        &mut self,
        i: NodeId,
        j: NodeId,
    ) -> Result<partitioned::Response, PeerError> {
        self.peer(j, i).pass_response().await
    }

    pub async fn discard_request(
        &mut self,
        i: NodeId,
        j: NodeId,
    ) -> Result<partitioned::Request, PeerError> {
        self.peer(i, j).discard_request().await
    }

    pub async fn discard_response(
        &mut self,
        i: NodeId,
        j: NodeId,
    ) -> Result<partitioned::Response, PeerError> {
        self.peer(j, i).discard_response().await
    }

    pub async fn pass_next_request(
        &mut self,
        i: NodeId,
        j: NodeId,
        verifier: impl Fn(partitioned::Request) -> () + Send + Sync + 'static,
    ) {
        self.peer(i, j).pass_next_request(verifier).await;
    }

    pub async fn pass_next_response(
        &mut self,
        i: NodeId,
        j: NodeId,
        verifier: impl Fn(partitioned::Response) -> () + Send + Sync + 'static,
    ) {
        self.peer(j, i).pass_next_response(verifier).await;
    }

    pub async fn unary<T1, T2>(
        &mut self,
        i: NodeId,
        command: T1,
    ) -> Result<tonic::Response<T2>, Status>
    where
        T1: prost::Message,
        T2: prost::Message + Default,
    {
        let repc_service = &mut self.repc_services.get(&i).unwrap();
        let mut req_bytes_inner = BytesMut::new();
        command.encode(&mut req_bytes_inner).unwrap();
        let req_bytes = tonic::Request::new(req_bytes_inner.into());

        let mut unary_service = repc_service.repc().into_unary_service();
        let mut res_bytes = unary_service.call(req_bytes).await?;
        let metadata = std::mem::take(res_bytes.metadata_mut());

        let res_message_inner = T2::decode(res_bytes.into_inner())
            .map_err(|e| Status::internal(format!("failed to decode: {}", e)))?;

        let mut res_message = tonic::Response::new(res_message_inner);
        *res_message.metadata_mut() = metadata;
        Ok(res_message)
    }
}
