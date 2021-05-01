use repc_common::{
    pb::admin::{admin_server::Admin, ForceElectionTimeoutRequest, ForceElectionTimeoutResponse},
    types::NodeId,
};

use super::service::raft::{error::HandleError, partition, Handle, ResponseHandle};
use crate::{
    configuration::Configuration,
    log::Log,
    pb::raft::{
        raft_client::RaftClient,
        raft_server::{Raft, RaftServer},
        AppendEntriesRequest, AppendEntriesResponse, RequestVoteRequest, RequestVoteResponse,
    },
    raft::node::Node,
    service::{admin::AdminService, raft::RaftService, repc::RepcService},
    state::State,
    state_machine::StateMachine,
};
use std::collections::HashMap;

pub struct PartitionedLocalRepcGroupBuilder<S, L> {
    confs: Vec<Configuration>,
    states: Vec<State<S, L>>,
}

impl<S, L> PartitionedLocalRepcGroupBuilder<S, L> {
    pub fn new() -> Self {
        Self {
            confs: vec![],
            states: vec![],
        }
    }

    pub fn states(self, states: Vec<State<S, L>>) -> Self {
        Self { states, ..self }
    }

    pub fn confs(self, confs: Vec<Configuration>) -> Self {
        Self { confs, ..self }
    }

    pub fn build(self) -> PartitionedLocalRepcGroup<S, L> {
        debug_assert_eq!(self.confs.len(), self.states.len());
        PartitionedLocalRepcGroup {
            confs: self.confs,
            states: self.states,
        }
    }
}

pub struct PartitionedLocalRepcGroup<S, L> {
    confs: Vec<Configuration>,
    states: Vec<State<S, L>>,
}

impl<S, L> PartitionedLocalRepcGroup<S, L>
where
    S: StateMachine + Send + Sync + 'static,
    L: Log + Send + Sync + 'static,
{
    pub fn spawn(self) -> PartitionedLocalRepcGroupHandle<RaftService> {
        const BUFFER: usize = 10;

        let nodes: HashMap<NodeId, _> = self
            .confs
            .into_iter()
            .zip(self.states.into_iter())
            .enumerate()
            .map(|(i, (conf, state))| {
                let id = i as NodeId + 1;
                let node = Node::new(id, conf, state);
                (id, node)
            })
            .collect();

        let raft_services: HashMap<_, _> = nodes
            .iter()
            .map(|(&id, node)| (id, RaftService::new(node.tx().clone())))
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
            .map(|(&i, node)| (i, RepcService::new(node.tx().clone())))
            .collect();

        let admin_services: HashMap<NodeId, _> = nodes
            .iter()
            .map(|(&i, node)| (i, AdminService::new(node.tx().clone())))
            .collect();

        for (id, mut node) in nodes.into_iter() {
            let clients = raft_clients.remove(&id).unwrap();
            *node.clients_mut() = clients;
            tokio::spawn(node.run());
        }

        PartitionedLocalRepcGroupHandle {
            handles: raft_client_handles,
            repc_services,
            admin_services,
        }
    }
}

pub struct PartitionedLocalRepcGroupHandle<R> {
    handles: HashMap<NodeId, HashMap<NodeId, Handle<R>>>,
    repc_services: HashMap<NodeId, RepcService>,
    admin_services: HashMap<NodeId, AdminService>,
}

impl<R> PartitionedLocalRepcGroupHandle<R>
where
    R: Raft,
{
    pub fn raft_handle(&self, i: NodeId, j: NodeId) -> &Handle<R> {
        self.handles.get(&i).unwrap().get(&j).unwrap()
    }

    pub fn raft_handle_mut(&mut self, i: NodeId, j: NodeId) -> &mut Handle<R> {
        self.handles.get_mut(&i).unwrap().get_mut(&j).unwrap()
    }

    pub fn repc_service(&self, i: NodeId) -> &RepcService {
        self.repc_services.get(&i).unwrap()
    }

    pub fn admin_service(&self, i: NodeId) -> &AdminService {
        self.admin_services.get(&i).unwrap()
    }

    pub async fn force_election_timeout(
        &mut self,
        i: NodeId,
    ) -> Result<tonic::Response<ForceElectionTimeoutResponse>, tonic::Status> {
        self.admin_services
            .get_mut(&i)
            .unwrap()
            .force_election_timeout(tonic::Request::new(ForceElectionTimeoutRequest {}))
            .await
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
