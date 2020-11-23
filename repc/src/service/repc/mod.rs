pub mod codec;

use crate::pb::raft::{log_entry::Command, Action};
use crate::pb::repc::{
    repc_server::Repc, CommandRequest, CommandResponse, RegisterRequest, RegisterResponse,
};
use crate::raft::message::Message;
use tokio::sync::{mpsc, oneshot};
use tonic::{Request, Response, Status, Streaming};

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
            path,
            body,
            sequence,
        } = request.into_inner();
        let (callback_tx, callback_rx) = oneshot::channel();
        let command = Message::Command {
            command: Command::Action(Action { path, body }),
            tx: callback_tx,
        };
        let mut tx = self.tx.clone();
        if tx.send(command).await.is_ok() {
            match callback_rx.await {
                Ok(Ok(response)) => Ok(response.map(|res| CommandResponse {
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

    async fn client_stream_command(
        &self,
        request: Request<Streaming<CommandRequest>>,
    ) -> Result<tonic::Response<CommandResponse>, Status> {
        Err(Status::internal("unimplemented"))
    }

    type ServerStreamCommandStream = mpsc::Receiver<Result<CommandResponse, Status>>;

    async fn server_stream_command(
        &self,
        request: Request<CommandRequest>,
    ) -> Result<Response<Self::ServerStreamCommandStream>, Status> {
        Err(Status::internal("unimplemented"))
    }
}

impl RepcService {
    async fn handle_command(&self, sequence: u64, command: Command) {}
}
