pub mod codec;

use crate::{
    pb::raft::{log_entry::Command, Action, Register},
    raft::message::Message,
};
use bytes::{Buf, Bytes};
use repc_proto::repc::{
    metadata::RequestMetadata,
    repc_server::Repc,
    types::{ClientId, Sequence},
    CommandRequest, CommandResponse, RegisterRequest, RegisterResponse,
};
use tokio::sync::{mpsc, oneshot};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};
use tracing::Instrument;

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
        let span = tracing::trace_span!("register");

        let command = Command::Register(Register {});
        self.handle_command(command, ClientId::default(), Sequence::default())
            .instrument(span)
            .await
            .map(|response| response.map(|mut body| RegisterResponse { id: body.get_u64() }))
    }

    async fn unary_command(
        &self,
        request: Request<CommandRequest>,
    ) -> Result<Response<CommandResponse>, Status> {
        let metadata = RequestMetadata::decode(request.metadata()).map_err(Status::from)?;
        let CommandRequest { path, body } = request.into_inner();
        let command = Command::Action(Action { path, body });

        let span = tracing::trace_span!(
            "unary_command",
            client_id = metadata.client_id.get(),
            sequence = metadata.sequence,
        );

        self.handle_command(command, metadata.client_id, metadata.sequence)
            .instrument(span)
            .await
            .map(|response| {
                response.map(|body| CommandResponse {
                    body: body.to_vec(),
                })
            })
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
    ) -> Result<tonic::Response<Bytes>, Status> {
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
                Ok(Err(e)) => Err(Status::from(e)),
                Err(e) => Err(Status::internal(format!(
                    "channel to the node process has been closed: {}",
                    e
                ))),
            }
        } else {
            Err(Status::internal("terminated"))
        }
    }
}
