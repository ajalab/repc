use crate::configuration::Configuration;
use crate::node::BaseNode;
use crate::pb::raft_server::RaftServer;
use crate::peer::grpc::GRPCPeer;
use crate::service::RaftService;
use crate::types::NodeId;
use log::info;
use std::collections::HashMap;
use std::error;
use tonic::transport::Server;

pub struct GRPCRaftGroup {
    id: NodeId,
    conf: Configuration,
    addrs: HashMap<NodeId, String>,
}

impl GRPCRaftGroup {
    pub fn new(id: NodeId, conf: Configuration, addrs: HashMap<NodeId, String>) -> Self {
        GRPCRaftGroup { id, addrs, conf }
    }

    pub async fn run(self) -> Result<(), Box<dyn error::Error>> {
        let node = BaseNode::new(self.id, self.conf);

        // start server
        let addr = self.addrs.get(&self.id).unwrap().parse()?;
        let service = RaftService::new(node.tx());
        let server = Server::builder().add_service(RaftServer::new(service));

        tokio::spawn(server.serve(addr));

        // set up connections to other nodes
        let mut ids = Vec::new();
        let mut peer_futures = Vec::new();
        for (id, addr) in self.addrs.iter() {
            if *id == self.id {
                continue;
            }
            ids.push(*id);
            peer_futures.push(GRPCPeer::connect(addr));
        }

        let result = futures::future::try_join_all(peer_futures).await;
        let peers = result
            .map(|peers| ids.into_iter().zip(peers).collect::<Vec<_>>())?
            .into_iter()
            .collect::<HashMap<NodeId, GRPCPeer>>();

        info!(
            "start serving gRPC Raft interface on {} for inter-cluster communication",
            addr
        );

        node.run(peers).await;
        Ok(())
    }
}
