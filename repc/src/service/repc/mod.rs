pub mod codec;

use crate::pb::repc::{
    repc_server::Repc, CommandRequest, CommandResponse, RegisterRequest, RegisterResponse,
};
use crate::raft::message::Message;
use crate::state::Command;
use bytes::Bytes;
use tokio::sync::{mpsc, oneshot};
use tonic::{Request, Response, Status};

#[derive(Clone)]
pub struct RepcService {
    tx: mpsc::Sender<Message>,
}

impl RepcService {
    pub fn new(tx: mpsc::Sender<Message>) -> Self {
        RepcService { tx }
    }
}

#[tonic::async_trait]
impl Repc for RepcService {
    async fn unary_command(
        &self,
        request: Request<CommandRequest>,
    ) -> Result<Response<CommandResponse>, Status> {
        let CommandRequest {
            id,
            sequence,
            command_path,
            command_body,
        } = request.into_inner();
        let (callback_tx, callback_rx) = oneshot::channel();
        let command = Message::Command {
            command: Command::new(command_path, Bytes::from(command_body)),
            tx: callback_tx,
        };
        let mut tx = self.tx.clone();
        if tx.send(command).await.is_ok() {
            match callback_rx.await {
                Ok(Ok(response)) => Ok(response.map(|res| CommandResponse {
                    status: true,
                    leader: "unimplemented".to_string(),
                    response: res.to_vec(),
                })),
                Ok(Err(e)) => Err(e.into_status()),
                Err(e) => Err(Status::internal(e.to_string())),
            }
        } else {
            Err(Status::internal("terminated"))
        }
    }

    async fn register(
        &self,
        request: Request<RegisterRequest>,
    ) -> Result<Response<RegisterResponse>, Status> {
        Err(Status::internal("unimplemented"))
    }
}
