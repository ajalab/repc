use crate::configuration::Configuration;
use crate::raft::node::Node;
use crate::raft::pb::raft_server::RaftServer;
use crate::raft::peer::grpc::RaftGrpcPeer;
use crate::raft::service::RaftService;
use crate::state::state_machine::{StateMachine, StateMachineManager};
use crate::types::NodeId;
use std::collections::HashMap;
use std::error;
use std::net::SocketAddr;
use std::sync::Arc;
use tonic::transport::Server;

pub struct GrpcRepcGroup<S> {
    id: NodeId,
    conf: Configuration,
    state_machine: S,
}

impl<S> GrpcRepcGroup<S>
where
    S: StateMachine + Send + 'static,
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
        let sm_manager = StateMachineManager::spawn(self.state_machine);
        let node = Node::new(self.id, sm_manager).conf(conf.clone());

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
        let mut ids = Vec::new();
        let mut peer_futures = Vec::new();
        for (&id, conf) in conf.group.nodes.iter() {
            if id == self.id {
                continue;
            }
            ids.push(id);
            let addr = SocketAddr::new(conf.ip, conf.raft_port);
            peer_futures.push(RaftGrpcPeer::connect(addr.to_string()));
        }

        let result = futures::future::try_join_all(peer_futures).await;
        let peers = result
            .map(|peers| ids.into_iter().zip(peers).collect::<Vec<_>>())?
            .into_iter()
            .collect::<HashMap<NodeId, RaftGrpcPeer>>();

        node.peers(peers).run().await;
        Ok(())
    }
}
