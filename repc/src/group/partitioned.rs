use crate::configuration::Configuration;
use crate::pb::raft::raft_client::RaftClient;
use crate::pb::raft::raft_server::{Raft, RaftServer};
use crate::pb::raft::{
    AppendEntriesRequest, AppendEntriesResponse, RequestVoteRequest, RequestVoteResponse,
};
use crate::raft::node::Node;
use crate::service::raft::partitioned::{error::HandleError, partition, Handle};
use crate::service::raft::RaftService;
use crate::service::repc::RepcService;
use crate::state::StateMachine;
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
    S: StateMachine + Send + Sync + 'static,
{
    pub fn spawn(self) -> PartitionedLocalRaftGroupHandle<S::Service, impl Raft> {
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
        for (&src_id, service) in &raft_services {
            let mut clients = HashMap::new();
            let mut handles = HashMap::new();
            for &dst_id in nodes.keys() {
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

        let repc_services: HashMap<NodeId, S::Service> = nodes
            .iter()
            .map(|(&i, node)| (i, S::Service::from_tx(node.get_tx())))
            .collect();

        for (id, node) in nodes.into_iter() {
            let clients = raft_clients.remove(&id).unwrap();
            tokio::spawn(node.clients(clients).run());
        }

        PartitionedLocalRaftGroupHandle {
            handles: raft_client_handles,
            repc_services,
        }
    }
}

pub struct PartitionedLocalRaftGroupHandle<S, R> {
    handles: HashMap<NodeId, HashMap<NodeId, Handle<R>>>,
    repc_services: HashMap<NodeId, S>,
}

impl<S, R> PartitionedLocalRaftGroupHandle<S, R>
where
    S: RepcService + Service<http::Request<BoxBody>>,
    R: Raft,
{
    fn handle(&mut self, i: NodeId, j: NodeId) -> &mut Handle<R> {
        self.handles.get_mut(&i).unwrap().get_mut(&j).unwrap()
    }

    pub async fn pass_request_vote_request(
        &mut self,
        i: NodeId,
        j: NodeId,
    ) -> Result<tonic::Request<RequestVoteRequest>, HandleError> {
        self.handle(i, j).pass_request_vote_request().await
    }

    pub async fn pass_append_entries_request(
        &mut self,
        i: NodeId,
        j: NodeId,
    ) -> Result<tonic::Request<AppendEntriesRequest>, HandleError> {
        self.handle(i, j).pass_append_entries_request().await
    }

    pub async fn block_request_vote_request(
        &mut self,
        i: NodeId,
        j: NodeId,
    ) -> Result<tonic::Request<RequestVoteRequest>, HandleError> {
        self.handle(i, j).block_request_vote_request().await
    }

    pub async fn block_append_entries_request(
        &mut self,
        i: NodeId,
        j: NodeId,
    ) -> Result<tonic::Request<AppendEntriesRequest>, HandleError> {
        self.handle(i, j).block_append_entries_request().await
    }

    pub async fn pass_request_vote_response(
        &mut self,
        i: NodeId,
        j: NodeId,
    ) -> Result<Result<tonic::Response<RequestVoteResponse>, tonic::Status>, HandleError> {
        self.handle(j, i).pass_request_vote_response().await
    }

    pub async fn pass_append_entries_response(
        &mut self,
        i: NodeId,
        j: NodeId,
    ) -> Result<Result<tonic::Response<AppendEntriesResponse>, tonic::Status>, HandleError> {
        self.handle(j, i).pass_append_entries_response().await
    }

    pub async fn block_request_vote_response(
        &mut self,
        i: NodeId,
        j: NodeId,
    ) -> Result<Result<tonic::Response<RequestVoteResponse>, tonic::Status>, HandleError> {
        self.handle(j, i).block_request_vote_response().await
    }

    pub async fn block_append_entries_response(
        &mut self,
        i: NodeId,
        j: NodeId,
    ) -> Result<Result<tonic::Response<AppendEntriesResponse>, tonic::Status>, HandleError> {
        self.handle(j, i).block_append_entries_response().await
    }

    pub async fn unary<C, T1, T2>(
        &mut self,
        i: NodeId,
        rpc: C,
        command: T1,
    ) -> Result<tonic::Response<T2>, Status>
    where
        C: AsRef<str>,
        T1: prost::Message,
        T2: prost::Message + Default,
    {
        let repc_service = &mut self.repc_services.get(&i).unwrap();
        let mut req_bytes_inner = BytesMut::new();
        command.encode(&mut req_bytes_inner).unwrap();
        let req_bytes = tonic::Request::new(req_bytes_inner.into());

        let mut unary_service = repc_service.repc().to_unary_service(rpc.as_ref().into());
        let mut res_bytes = unary_service.call(req_bytes).await?;
        let metadata = std::mem::take(res_bytes.metadata_mut());

        let res_message_inner = T2::decode(res_bytes.into_inner())
            .map_err(|e| Status::internal(format!("failed to decode: {}", e)))?;

        let mut res_message = tonic::Response::new(res_message_inner);
        *res_message.metadata_mut() = metadata;
        Ok(res_message)
    }
}
