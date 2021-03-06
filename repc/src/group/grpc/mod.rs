pub mod error;

use self::error::GrpcRepcGroupError;
use crate::{
    configuration::Configuration,
    log::in_memory::InMemoryLog,
    pb::raft::{raft_client::RaftClient, raft_server::RaftServer},
    raft::node::Node,
    service::{raft::RaftService, repc::RepcService},
    state::State,
    state_machine::StateMachine,
};
use http::Uri;
use repc_common::{pb::repc::repc_server::RepcServer, types::NodeId};
use std::{collections::HashMap, net::SocketAddr};
use tonic::transport::{Channel, Server};

pub struct GrpcRepcGroup<S> {
    id: NodeId,
    conf: Configuration,
    state_machine: S,
}

impl<S> GrpcRepcGroup<S>
where
    S: StateMachine + Send + Sync + 'static,
{
    pub fn new(id: NodeId, conf: Configuration, state_machine: S) -> Self {
        GrpcRepcGroup {
            id,
            conf,
            state_machine,
        }
    }

    pub async fn run(self) -> Result<(), GrpcRepcGroupError> {
        let nodes = self.conf.group.nodes.clone();
        let node_conf = nodes.get(&self.id).unwrap();
        let state = State::new(self.state_machine, InMemoryLog::default());
        let mut node = Node::new(self.id, self.conf, state);

        // start raft server
        let raft_addr = SocketAddr::new(node_conf.ip, node_conf.raft_port);
        let raft_service = RaftService::new(node.tx().clone());
        let raft_server = Server::builder().add_service(RaftServer::new(raft_service));
        tracing::info!("start serving gRPC Raft service on {}", raft_addr);
        tokio::spawn(raft_server.serve(raft_addr));

        // start rpc server
        let repc_addr = SocketAddr::new(node_conf.ip, node_conf.repc_port);
        let repc_service = RepcService::new(node.tx().clone());
        let repc_server = Server::builder().add_service(RepcServer::new(repc_service));
        tracing::info!("start serving Repc service on {}", repc_addr);
        tokio::spawn(repc_server.serve(repc_addr));

        // set up connections to other nodes
        let mut clients = HashMap::new();
        for (&id, conf) in nodes.iter() {
            if id == self.id {
                continue;
            }
            let authority = format!("{}:{}", conf.ip, conf.raft_port);
            let uri = Uri::builder()
                .scheme("http")
                .authority(authority.as_bytes())
                .path_and_query("/")
                .build()
                .map_err(GrpcRepcGroupError::HttpError)?;
            let channel = Channel::builder(uri)
                .connect_lazy()
                .map_err(GrpcRepcGroupError::TransportError)?;
            clients.insert(id, RaftClient::new(channel));
        }

        *node.clients_mut() = clients;
        node.run().await;
        Ok(())
    }
}
