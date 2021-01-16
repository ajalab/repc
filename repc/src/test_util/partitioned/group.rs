use super::service::raft::{error::HandleError, partition, Handle, ResponseHandle};
use crate::configuration::Configuration;
use crate::pb::raft::{
    raft_client::RaftClient,
    raft_server::{Raft, RaftServer},
    AppendEntriesRequest, AppendEntriesResponse, RequestVoteRequest, RequestVoteResponse,
};
use crate::raft::node::Node;
use crate::service::raft::RaftService;
use crate::service::repc::RepcService;
use crate::state::StateMachine;
use crate::types::NodeId;
use repc_client::{error::RegisterError, RepcClient};
use repc_proto::repc_server::RepcServer;
use std::collections::HashMap;
use std::sync::Arc;

pub struct PartitionedLocalRepcGroupBuilder<S> {
    confs: Vec<Configuration>,
    state_machines: Vec<S>,
}

impl<S> PartitionedLocalRepcGroupBuilder<S> {
    pub fn new() -> Self {
        Self {
            confs: vec![],
            state_machines: vec![],
        }
    }

    pub fn state_machines(self, state_machines: Vec<S>) -> Self {
        Self {
            state_machines,
            ..self
        }
    }

    pub fn confs(self, confs: Vec<Configuration>) -> Self {
        Self { confs, ..self }
    }

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
    S: StateMachine + Send + Sync + 'static,
{
    pub fn spawn(self) -> PartitionedLocalRepcGroupHandle<RaftService> {
        const BUFFER: usize = 10;

        let nodes: HashMap<NodeId, _> = self
            .confs
            .into_iter()
            .zip(self.state_machines.into_iter())
            .enumerate()
            .map(|(i, (conf, state_machine))| {
                let id = i as NodeId + 1;
                let node = Node::new(id, state_machine).conf(Arc::new(conf));
                (id, node)
            })
            .collect();

        let raft_services: HashMap<_, _> = nodes
            .iter()
            .map(|(&id, node)| (id, RaftService::new(node.get_tx())))
            .collect();

        let mut raft_clients = HashMap::new();
        let mut raft_client_handles = HashMap::new();
        for &src_id in nodes.keys() {
            let mut clients = HashMap::new();
            let mut handles = HashMap::new();
            for (&dst_id, service) in &raft_services {
                if src_id == dst_id {
                    continue;
                }
                let (service, handle) = partition(service.clone(), BUFFER);
                let server = RaftServer::new(service);
                let client = RaftClient::new(server);
                clients.insert(dst_id, client);
                handles.insert(dst_id, handle);
            }
            raft_clients.insert(src_id, clients);
            raft_client_handles.insert(src_id, handles);
        }

        let repc_services: HashMap<NodeId, _> = nodes
            .iter()
            .map(|(&i, node)| (i, RepcService::new(node.get_tx())))
            .collect();

        for (id, node) in nodes.into_iter() {
            let clients = raft_clients.remove(&id).unwrap();
            tokio::spawn(node.clients(clients).run());
        }

        PartitionedLocalRepcGroupHandle {
            handles: raft_client_handles,
            repc_services,
        }
    }
}

pub struct PartitionedLocalRepcGroupHandle<R> {
    handles: HashMap<NodeId, HashMap<NodeId, Handle<R>>>,
    repc_services: HashMap<NodeId, RepcService>,
}

impl<R> PartitionedLocalRepcGroupHandle<R>
where
    R: Raft,
{
    pub fn raft_handle(&mut self, i: NodeId, j: NodeId) -> &Handle<R> {
        self.handles.get(&i).unwrap().get(&j).unwrap()
    }

    pub fn raft_handle_mut(&mut self, i: NodeId, j: NodeId) -> &mut Handle<R> {
        self.handles.get_mut(&i).unwrap().get_mut(&j).unwrap()
    }

    pub async fn register_client(
        &self,
        i: NodeId,
    ) -> Result<RepcClient<RepcServer<RepcService>>, RegisterError> {
        let service = self.repc_services.get(&i).unwrap().clone();
        RepcClient::register(RepcServer::new(service)).await
    }

    pub async fn pass_request_vote_request(
        &mut self,
        i: NodeId,
        j: NodeId,
    ) -> Result<
        (
            tonic::Request<RequestVoteRequest>,
            ResponseHandle<RequestVoteResponse>,
        ),
        HandleError,
    > {
        self.raft_handle_mut(i, j).pass_request_vote_request().await
    }

    pub async fn pass_append_entries_request(
        &mut self,
        i: NodeId,
        j: NodeId,
    ) -> Result<
        (
            tonic::Request<AppendEntriesRequest>,
            ResponseHandle<AppendEntriesResponse>,
        ),
        HandleError,
    > {
        self.raft_handle_mut(i, j)
            .pass_append_entries_request()
            .await
    }

    pub async fn block_request_vote_request(
        &mut self,
        i: NodeId,
        j: NodeId,
    ) -> Result<tonic::Request<RequestVoteRequest>, HandleError> {
        self.raft_handle_mut(i, j)
            .block_request_vote_request()
            .await
    }

    pub async fn block_append_entries_request(
        &mut self,
        i: NodeId,
        j: NodeId,
    ) -> Result<tonic::Request<AppendEntriesRequest>, HandleError> {
        self.raft_handle_mut(i, j)
            .block_append_entries_request()
            .await
    }

    pub async fn expect_request_vote_success(&mut self, i: NodeId, j: NodeId) {
        self.raft_handle_mut(i, j)
            .expect_request_vote_success()
            .await;
    }

    pub async fn expect_append_entries_success(&mut self, i: NodeId, j: NodeId) {
        self.raft_handle_mut(i, j)
            .expect_append_entries_success()
            .await;
    }
}
