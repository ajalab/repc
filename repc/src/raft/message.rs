use super::node::error::CommandError;
use super::pb::{
    AppendEntriesRequest, AppendEntriesResponse, RequestVoteRequest, RequestVoteResponse,
};
use crate::types::NodeId;
use bytes::Bytes;
use std::error;
use tokio::sync::{mpsc, oneshot};

pub enum Message {
    RPCRequestVoteRequest {
        req: RequestVoteRequest,
        tx: mpsc::Sender<Result<RequestVoteResponse, Box<dyn error::Error + Send>>>,
    },

    RPCRequestVoteResponse {
        res: RequestVoteResponse,
        id: NodeId,
    },

    RPCAppendEntriesRequest {
        req: AppendEntriesRequest,
        tx: mpsc::Sender<Result<AppendEntriesResponse, Box<dyn error::Error + Send>>>,
    },

    RPCAppendEntriesResponse {
        res: AppendEntriesResponse,
        id: NodeId,
    },

    Command {
        body: Bytes,
        tx: oneshot::Sender<Result<Bytes, CommandError>>,
    },

    ElectionTimeout,
}
