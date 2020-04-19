use crate::rpc::raft;
use crate::types::NodeId;
use tokio::sync::mpsc;

#[derive(Clone)]
pub enum Message {
    RPCRequestVoteRequest {
        req: raft::RequestVoteRequest,
        tx: mpsc::Sender<raft::RequestVoteResponse>,
    },

    RPCRequestVoteResponse {
        res: raft::RequestVoteResponse,
        id: NodeId,
    },

    RPCAppendEntriesRequest {
        req: raft::AppendEntriesRequest,
        tx: mpsc::Sender<raft::AppendEntriesResponse>,
    },

    RPCAppendEntriesResponse {
        res: raft::AppendEntriesResponse,
        id: NodeId,
    },

    ElectionTimeout,
}
