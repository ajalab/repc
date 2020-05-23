use crate::pb::{
    AppendEntriesRequest, AppendEntriesResponse, RequestVoteRequest, RequestVoteResponse,
};
use crate::types::NodeId;
use bytes::Bytes;
use std::sync::Arc;
use tokio::sync::mpsc;
use tonic::Status;

pub enum Message {
    RPCRequestVoteRequest {
        req: RequestVoteRequest,
        tx: mpsc::Sender<RequestVoteResponse>,
    },

    RPCRequestVoteResponse {
        res: RequestVoteResponse,
        id: NodeId,
    },

    RPCAppendEntriesRequest {
        req: AppendEntriesRequest,
        tx: mpsc::Sender<AppendEntriesResponse>,
    },

    RPCAppendEntriesResponse {
        res: AppendEntriesResponse,
        id: NodeId,
    },

    Command {
        body: Bytes,
        tx: mpsc::Sender<Result<(), Status>>,
    },

    ElectionTimeout,
}
