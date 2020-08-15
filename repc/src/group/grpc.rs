use crate::configuration::Configuration;
use crate::raft::node::BaseNode;
use crate::raft::pb::raft_server::RaftServer;
use crate::raft::peer::grpc::GrpcPeer;
use crate::raft::service::RaftService;
use crate::state_machine::{StateMachine, StateMachineManager};
use crate::types::NodeId;
use log::info;
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
        let sm_manager = StateMachineManager::spawn(self.state_machine);
        let node = BaseNode::new(self.id, sm_manager).conf(conf.clone());

        // start server
        // let addr = self.addrs.get(&self.id).unwrap().parse()?;
        let conf_node = conf.group.nodes.get(&self.id).unwrap();
        let raft_addr = SocketAddr::new(conf_node.ip, conf_node.raft_port);
        let service = RaftService::new(node.get_tx());
        let server = Server::builder().add_service(RaftServer::new(service));
        info!(
            "start serving gRPC Raft interface on {} for inter-cluster communication",
            raft_addr
        );
        tokio::spawn(server.serve(raft_addr));

        // start rpc server

        // set up connections to other nodes
        let mut ids = Vec::new();
        let mut peer_futures = Vec::new();
        for (&id, conf) in conf.group.nodes.iter() {
            if id == self.id {
                continue;
            }
            ids.push(id);
            let addr = SocketAddr::new(conf.ip, conf.raft_port);
            peer_futures.push(GrpcPeer::connect(addr.to_string()));
        }

        let result = futures::future::try_join_all(peer_futures).await;
        let peers = result
            .map(|peers| ids.into_iter().zip(peers).collect::<Vec<_>>())?
            .into_iter()
            .collect::<HashMap<NodeId, GrpcPeer>>();

        node.peers(peers).run().await;
        Ok(())
    }
}
