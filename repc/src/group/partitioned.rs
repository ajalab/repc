use crate::configuration::Configuration;
use crate::pb::raft::raft_client::RaftClient;
use crate::pb::raft::raft_server::{Raft, RaftServer};
use crate::pb::raft::{
    AppendEntriesRequest, AppendEntriesResponse, RequestVoteRequest, RequestVoteResponse,
};
use crate::pb::repc::repc_server::Repc;
use crate::pb::repc::CommandRequest;
use crate::raft::node::Node;
use crate::service::raft::partitioned::{error::HandleError, partition, Handle, ResponseHandle};
use crate::service::raft::RaftService;
use crate::service::repc::RepcService;
use crate::state::StateMachine;
use crate::types::NodeId;
use bytes::{Bytes, BytesMut};
use std::collections::HashMap;
use std::sync::Arc;
use tonic::Status;

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
    pub fn spawn(self) -> PartitionedLocalRaftGroupHandle<RaftService> {
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

        let repc_services: HashMap<NodeId, RepcService> = nodes
            .iter()
            .map(|(&i, node)| (i, RepcService::new(node.get_tx())))
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

#[derive(Clone)]
pub struct PartitionedLocalRaftGroupHandle<R> {
    handles: HashMap<NodeId, HashMap<NodeId, Handle<R>>>,
    repc_services: HashMap<NodeId, RepcService>,
}

impl<R> PartitionedLocalRaftGroupHandle<R>
where
    R: Raft,
{
    pub fn raft_handle(&mut self, i: NodeId, j: NodeId) -> &Handle<R> {
        self.handles.get(&i).unwrap().get(&j).unwrap()
    }

    pub fn raft_handle_mut(&mut self, i: NodeId, j: NodeId) -> &mut Handle<R> {
        self.handles.get_mut(&i).unwrap().get_mut(&j).unwrap()
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

    pub async fn unary<P, T1, T2>(
        &mut self,
        i: NodeId,
        path: P,
        req: T1,
    ) -> Result<tonic::Response<T2>, Status>
    where
        P: AsRef<str>,
        T1: prost::Message,
        T2: prost::Message + Default,
    {
        let repc_service = &mut self.repc_services.get(&i).unwrap();
        let mut body = BytesMut::new();
        req.encode(&mut body).unwrap();
        let request = tonic::Request::new(CommandRequest {
            path: path.as_ref().to_string(),
            body: body.to_vec(),
            sequence: 0,
        });

        let mut response = repc_service.unary_command(request).await?;
        let metadata = std::mem::take(response.metadata_mut());

        let res_message_inner = T2::decode(Bytes::from(response.into_inner().response))
            .map_err(|e| Status::internal(format!("failed to decode: {}", e)))?;

        let mut res_message = tonic::Response::new(res_message_inner);
        *res_message.metadata_mut() = metadata;
        Ok(res_message)
    }
}
