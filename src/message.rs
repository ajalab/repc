use crate::grpc;
use crate::types::NodeId;
use tokio::sync::mpsc;

#[derive(Clone)]
pub enum Message {
    RPCRequestVoteRequest {
        req: grpc::RequestVoteRequest,
        tx: mpsc::Sender<grpc::RequestVoteResponse>,
    },

    RPCRequestVoteResponse {
        res: grpc::RequestVoteResponse,
        id: NodeId,
    },

    RPCAppendEntriesRequest {
        req: grpc::AppendEntriesRequest,
        tx: mpsc::Sender<grpc::AppendEntriesResponse>,
    },

    RPCAppendEntriesResponse {
        res: grpc::AppendEntriesResponse,
        id: NodeId,
    },

    ElectionTimeout,
}
