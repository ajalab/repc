use std::error;

use log::info;
use tokio::sync::mpsc;
use tonic::transport::Server;
use tonic::{Request, Response, Status};

use crate::grpc::raft_server::{self, Raft};
use crate::grpc::{
    AppendEntriesRequest, AppendEntriesResponse, RequestVoteRequest, RequestVoteResponse,
};
use crate::message::Message;
use crate::node;
use crate::types::NodeId;

pub struct RaftServer {
    tx_msg: mpsc::Sender<Message>,
}

impl RaftServer {
    pub fn new(tx: mpsc::Sender<Message>) -> Self {
        RaftServer { tx_msg: tx }
    }
}

#[tonic::async_trait]
impl Raft for RaftServer {
    async fn request_vote(
        &self,
        request: Request<RequestVoteRequest>,
    ) -> Result<Response<RequestVoteResponse>, Status> {
        let (tx, mut rx) = mpsc::channel(1);

        self.tx_msg
            .clone()
            .send(Message::RPCRequestVoteRequest {
                req: request.into_inner(),
                tx,
            })
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        rx.recv()
            .await
            .ok_or_else(|| {
                Status::internal("couldn't get a response (maybe the node is going to shut down)")
            })
            .map(Response::new)
    }

    async fn append_entries(
        &self,
        request: Request<AppendEntriesRequest>,
    ) -> Result<Response<AppendEntriesResponse>, Status> {
        let (tx, mut rx) = mpsc::channel(1);

        self.tx_msg
            .clone()
            .send(Message::RPCAppendEntriesRequest {
                req: request.into_inner(),
                tx,
            })
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        rx.recv()
            .await
            .ok_or_else(|| {
                Status::internal("couldn't get a response (maybe the node is going to shut down")
            })
            .map(Response::new)
    }
}

pub struct GRPCNode {
    node: node::Node,
}

impl GRPCNode {
    pub fn new(id: NodeId, cluster: node::Cluster) -> Self {
        GRPCNode {
            node: node::Node::new(id, cluster),
        }
    }

    pub async fn serve(self) -> Result<(), Box<dyn error::Error>> {
        let server = RaftServer::new(self.node.get_tx());
        let addr = self.node.get_addr().parse()?;
        let f1 = Server::builder()
            .add_service(raft_server::RaftServer::new(server))
            .serve(addr);
        let f2 = self.node.run();
        info!(
            "start serving gRPC Raft interface on {} for inter-cluster communication",
            addr
        );
        futures::join!(f1, f2).0.map_err(|e| e.into())
    }
}
