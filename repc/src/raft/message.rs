use super::node::error::{AppendEntriesError, CommandError, RequestVoteError};
use crate::pb::raft::{
    log_entry::Command, AppendEntriesRequest, AppendEntriesResponse, RequestVoteRequest,
    RequestVoteResponse,
};
use bytes::Bytes;
use repc_common::repc::types::{ClientId, NodeId, Sequence};
use tokio::sync::{mpsc, oneshot};

pub enum Message {
    RPCRequestVoteRequest {
        req: RequestVoteRequest,
        tx: mpsc::Sender<Result<RequestVoteResponse, RequestVoteError>>,
    },

    RPCRequestVoteResponse {
        res: RequestVoteResponse,
        id: NodeId,
    },

    RPCAppendEntriesRequest {
        req: AppendEntriesRequest,
        tx: mpsc::Sender<Result<AppendEntriesResponse, AppendEntriesError>>,
    },

    RPCAppendEntriesResponse {
        res: AppendEntriesResponse,
        id: NodeId,
    },

    Command {
        command: Command,
        client_id: ClientId,
        sequence: Sequence,
        tx: oneshot::Sender<Result<tonic::Response<Bytes>, CommandError>>,
    },

    ElectionTimeout,
}
