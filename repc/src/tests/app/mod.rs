use crate::raft::message::Message;
use crate::service::repc::codec::IdentCodec;
use crate::service::repc::{Repc, RepcService};
use crate::state::error::StateMachineError;
use crate::state::{Command, StateMachine};
use bytes::{Bytes, BytesMut};
use prost::Message as ProstMessage;
use std::error::Error;
use std::task::{Context, Poll};
use tokio::sync::mpsc;
use tonic::body::BoxBody;
use tonic::codegen::{BoxFuture, HttpBody};
use tonic::server::Grpc;
use tonic::transport::NamedService;
use tower_service::Service;

#[derive(Clone, PartialEq, prost::Message)]
pub struct IncrRequest {
    #[prost(uint32, tag = "1")]
    pub i: u32,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct IncrResponse {
    #[prost(uint32, tag = "1")]
    pub n: u32,
}

pub trait Incr {
    fn incr(&mut self, req: IncrRequest) -> Result<tonic::Response<IncrResponse>, tonic::Status>;
}

impl<S> StateMachine for S
where
    S: Incr,
{
    type Service = IncrService;
    fn apply(&mut self, command: Command) -> Result<tonic::Response<Bytes>, StateMachineError> {
        let rpc = command.rpc().as_ref();
        match rpc {
            "/incr.Incr/Incr" => {
                let req = IncrRequest::decode(command.body().clone())
                    .map_err(|e| StateMachineError::DecodeRequestFailed(e))?;
                let mut res = self.incr(req).map_err(StateMachineError::ApplyFailed)?;
                let mut res_bytes = tonic::Response::new(BytesMut::new());
                std::mem::swap(res.metadata_mut(), res_bytes.metadata_mut());
                res.into_inner()
                    .encode(res_bytes.get_mut())
                    .map_err(StateMachineError::EncodeResponseFailed)?;
                Ok(res_bytes.map(Bytes::from))
            }
            _ => Err(StateMachineError::UnknownPath(rpc.into())),
        }
    }
}

#[derive(Default, Clone)]
pub struct IncrState {
    n: u32,
}

impl Incr for IncrState {
    fn incr(&mut self, req: IncrRequest) -> Result<tonic::Response<IncrResponse>, tonic::Status> {
        self.n += req.i;
        Ok(tonic::Response::new(IncrResponse { n: self.n }))
    }
}

#[derive(Clone)]
pub struct IncrService {
    repc: Repc,
}

impl RepcService for IncrService {
    fn from_tx(tx: mpsc::Sender<Message>) -> Self {
        IncrService {
            repc: Repc::new(tx),
        }
    }

    fn repc(&self) -> Repc {
        self.repc.clone()
    }
}

impl<B> Service<http::Request<B>> for IncrService
where
    B: HttpBody + Send + Sync + 'static,
    B::Error: Into<Box<dyn Error + Send + Sync + 'static>> + Send + 'static,
{
    type Response = http::Response<BoxBody>;
    type Error = futures::never::Never;
    type Future = BoxFuture<Self::Response, Self::Error>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: http::Request<B>) -> Self::Future {
        let repc = self.repc();
        let path = req.uri().path();
        let rpc = path.to_string().into();
        match path {
            "/incr.Incr/Incr" => Box::pin(async move {
                let service = repc.to_unary_service(rpc);
                let codec = IdentCodec;
                let mut grpc = Grpc::new(codec);
                let res = grpc.unary(service, req).await;
                Ok(res)
            }),
            _ => Box::pin(async move {
                Ok(http::Response::builder()
                    .status(200)
                    .header("grpc-status", "12")
                    .body(BoxBody::empty())
                    .unwrap())
            }),
        }
    }
}

impl NamedService for IncrService {
    const NAME: &'static str = "incr.Incr";
}
