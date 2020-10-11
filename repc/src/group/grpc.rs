use crate::configuration::Configuration;
use crate::pb::raft::raft_client::RaftClient;
use crate::pb::raft::raft_server::RaftServer;
use crate::raft::node::Node;
use crate::service::raft::RaftService;
use crate::state::StateMachine;
use crate::types::NodeId;
use http::Uri;
use std::collections::HashMap;
use std::error;
use std::net::SocketAddr;
use std::sync::Arc;
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

    pub async fn run(self) -> Result<(), Box<dyn error::Error>> {
        let conf = Arc::new(self.conf);
        let node_conf = conf.group.nodes.get(&self.id).unwrap();
        let node = Node::new(self.id, self.state_machine).conf(conf.clone());

        // start raft server
        let raft_addr = SocketAddr::new(node_conf.ip, node_conf.raft_port);
        let raft_service = RaftService::new(node.get_tx());
        let raft_server = Server::builder().add_service(RaftServer::new(raft_service));
        tracing::info!("start serving gRPC Raft service on {}", raft_addr);
        tokio::spawn(raft_server.serve(raft_addr));

        // start rpc server
        // let repc_addr = SocketAddr::new(node_conf.ip, node_conf.repc_port);
        // let repc_service = RepcService::new(node.get_tx());
        // let repc_server = Server::builder().add_service(repc_service);
        // info!("start serving Repc service on {}", repc_addr);
        // tokio::spawn(repc_server.serve(repc_addr));

        // set up connections to other nodes
        let mut clients = HashMap::new();
        for (&id, conf) in conf.group.nodes.iter() {
            if id == self.id {
                continue;
            }
            let authority = format!("{}:{}", conf.ip, conf.raft_port);
            let uri = Uri::builder()
                .scheme("http")
                .authority(authority.as_bytes())
                .build()?;
            let channel = Channel::builder(uri).connect_lazy()?;
            clients.insert(id, RaftClient::new(channel));
        }

        node.clients(clients).run().await;
        Ok(())
    }
}
