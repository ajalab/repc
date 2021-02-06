use super::node::error::CommandError;
use crate::pb::raft::{
    log_entry::Command, AppendEntriesRequest, AppendEntriesResponse, RequestVoteRequest,
    RequestVoteResponse,
};
use crate::session::RepcClientId;
use crate::types::NodeId;
use bytes::Bytes;
use repc_proto::types::Sequence;
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
        command: Command,
        client_id: RepcClientId,
        sequence: Sequence,
        tx: oneshot::Sender<Result<tonic::Response<Bytes>, CommandError>>,
    },

    ElectionTimeout,
}
