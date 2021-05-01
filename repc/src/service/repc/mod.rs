pub mod codec;

use crate::{
    pb::raft::{log_entry::Command, Action, Register},
    raft::{message::Message, node::error::CommandError},
};
use bytes::{Buf, Bytes};
use repc_common::{
    metadata::request::RequestMetadata,
    pb::repc::{
        repc_server::Repc, CommandRequest, CommandResponse, RegisterRequest, RegisterResponse,
    },
    types::{ClientId, Sequence},
};
use tokio::sync::{mpsc, oneshot};
use tokio_stream::wrappers::ReceiverStream;
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
    async fn register(
        &self,
        _request: Request<RegisterRequest>,
    ) -> Result<Response<RegisterResponse>, Status> {
        let command = Command::Register(Register {});
        self.handle_command(command, ClientId::default(), Sequence::default())
            .await
            .map(|response| response.map(|mut body| RegisterResponse { id: body.get_u64() }))
            .map_err(Status::from)
    }

    async fn unary_command(
        &self,
        request: Request<CommandRequest>,
    ) -> Result<Response<CommandResponse>, Status> {
        let metadata = match RequestMetadata::decode(request.metadata()) {
            Ok(metadata) => metadata,
            Err(e) => return Err(Status::from(CommandError::MetadataDecodeError(e))),
        };

        let CommandRequest { path, body } = request.into_inner();
        let command = Command::Action(Action { path, body });

        self.handle_command(command, metadata.client_id, metadata.sequence)
            .await
            .map(|response| {
                response.map(|body| CommandResponse {
                    body: body.to_vec(),
                })
            })
            .map_err(Status::from)
    }

    async fn client_stream_command(
        &self,
        _request: Request<Streaming<CommandRequest>>,
    ) -> Result<tonic::Response<CommandResponse>, Status> {
        Err(Status::internal("unimplemented"))
    }

    type ServerStreamCommandStream = ReceiverStream<Result<CommandResponse, Status>>;

    async fn server_stream_command(
        &self,
        _request: Request<CommandRequest>,
    ) -> Result<Response<Self::ServerStreamCommandStream>, Status> {
        Err(Status::internal("unimplemented"))
    }
}

impl RepcService {
    async fn handle_command(
        &self,
        command: Command,
        client_id: ClientId,
        sequence: Sequence,
    ) -> Result<tonic::Response<Bytes>, CommandError> {
        let (callback_tx, callback_rx) = oneshot::channel();
        let command = Message::Command {
            command,
            client_id,
            sequence,
            tx: callback_tx,
        };

        if self.tx.send(command).await.is_ok() {
            match callback_rx.await {
                Ok(Ok(response)) => Ok(response),
                Ok(Err(e)) => Err(e),
                Err(_) => Err(CommandError::Terminated),
            }
        } else {
            Err(CommandError::Closed)
        }
    }
}
