pub mod codec;
mod error;

use self::error::RepcServiceError;
use crate::pb::raft::{log_entry::Command, Action, Register};
use crate::raft::message::Message;
use crate::session::RepcClientId;
use bytes::{Buf, Bytes};
use repc_proto::{
    metadata::METADATA_REPC_CLIENT_ID_KEY, repc_server::Repc, types::Sequence, CommandRequest,
    CommandResponse, RegisterRequest, RegisterResponse,
};
use tokio::sync::{mpsc, oneshot};
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

fn get_client_id<T>(request: &Request<T>) -> Result<RepcClientId, RepcServiceError> {
    request
        .metadata()
        .get(METADATA_REPC_CLIENT_ID_KEY)
        .ok_or_else(|| RepcServiceError::ClientIdNotExist)
        .and_then(|id| id.to_str().map_err(|_| RepcServiceError::ClientIdInvalid))
        .and_then(|id| {
            id.parse::<u64>()
                .map_err(|_| RepcServiceError::ClientIdInvalid)
        })
        .map(|id| RepcClientId::from(id))
}

#[tonic::async_trait]
impl Repc for RepcService {
    async fn register(
        &self,
        _request: Request<RegisterRequest>,
    ) -> Result<Response<RegisterResponse>, Status> {
        let span = tracing::trace_span!("register");

        let command = Command::Register(Register {});
        self.handle_command(command, RepcClientId::default(), Sequence::default())
            .instrument(span)
            .await
            .map(|response| response.map(|mut body| RegisterResponse { id: body.get_u64() }))
    }

    async fn unary_command(
        &self,
        request: Request<CommandRequest>,
    ) -> Result<Response<CommandResponse>, Status> {
        let client_id = get_client_id(&request).map_err(Status::from)?;
        let CommandRequest {
            path,
            body,
            sequence,
        } = request.into_inner();
        let command = Command::Action(Action { path, body });
        let sequence = Sequence::from(sequence);

        let span = tracing::trace_span!(
            "unary_command",
            client_id = u64::from(client_id),
            sequence = sequence
        );

        self.handle_command(command, client_id, sequence)
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

    type ServerStreamCommandStream = mpsc::Receiver<Result<CommandResponse, Status>>;

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
        client_id: RepcClientId,
        sequence: Sequence,
    ) -> Result<tonic::Response<Bytes>, Status> {
        let (callback_tx, callback_rx) = oneshot::channel();
        let command = Message::Command {
            command,
            client_id,
            sequence,
            tx: callback_tx,
        };

        let mut tx = self.tx.clone();
        if tx.send(command).await.is_ok() {
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
