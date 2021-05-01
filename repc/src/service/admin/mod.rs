use crate::raft::message::Message;
use repc_common::admin::pb::{
    admin_server::Admin, ForceElectionTimeoutRequest, ForceElectionTimeoutResponse,
};
use tokio::sync::mpsc;
use tonic::{Request, Response, Status};

#[derive(Clone)]
pub struct AdminService {
    tx: mpsc::Sender<Message>,
}

impl AdminService {
    pub fn new(tx: mpsc::Sender<Message>) -> Self {
        Self { tx }
    }
}

#[tonic::async_trait]
impl Admin for AdminService {
    async fn force_election_timeout(
        &self,
        _request: Request<ForceElectionTimeoutRequest>,
    ) -> Result<Response<ForceElectionTimeoutResponse>, Status> {
        self.tx
            .clone()
            .send(Message::ElectionTimeout)
            .await
            .map(|_| Response::new(ForceElectionTimeoutResponse {}))
            .map_err(|_| Status::internal("failed to send ElectionTimeout message to the node"))
    }
}
