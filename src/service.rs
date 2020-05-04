use tokio::sync::mpsc;
use tonic::{Request, Response, Status};

use crate::message::Message;
pub use crate::pb::raft_server::Raft;
use crate::pb::{
    AppendEntriesRequest, AppendEntriesResponse, RequestVoteRequest, RequestVoteResponse,
};

#[derive(Clone)]
pub struct RaftService {
    tx_msg: mpsc::Sender<Message>,
}

impl RaftService {
    pub fn new(tx: mpsc::Sender<Message>) -> Self {
        Self { tx_msg: tx }
    }
}

#[tonic::async_trait]
impl Raft for RaftService {
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
