use crate::message::Message;
use crate::node::error::CommandError;
use bytes::buf::Buf;
use bytes::Bytes;
use futures_util::{TryFutureExt, TryStreamExt};
use std::error;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::sync::{mpsc, oneshot};
use tonic::body::BoxBody;
use tonic::codec::{DecodeBuf, Decoder, Streaming};
use tonic::transport::Body;
use tonic::Status;
use tower_service::Service;

#[derive(Debug)]
pub enum RSMServiceError {
    NodeTerminated,
    NodeCrashed,
    CommandInvalid(Status),
    CommandMissing,
    CommandFailed(CommandError),
}

impl RSMServiceError {
    fn description(&self) -> &'static str {
        use RSMServiceError::*;
        match self {
            NodeTerminated => "command could not be handled because the node has been terminated",
            NodeCrashed => "node failed to handle the command during its process",
            CommandInvalid(_) => "failed to decode the command",
            CommandMissing => "command is missing in the request",
            CommandFailed(_) => "failed to process command",
        }
    }

    fn into_status(self) -> Status {
        use RSMServiceError::*;
        match self {
            CommandInvalid(status) => status,
            _ => Status::internal(self.to_string()),
        }
    }

    fn into_http(self) -> http::Response<BoxBody> {
        self.into_status().to_http()
    }
}

impl fmt::Display for RSMServiceError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use RSMServiceError::*;
        write!(f, "{}", self.description())?;
        match self {
            CommandInvalid(s) => write!(f, ": {}", s),
            CommandFailed(e) => write!(f, ": {}", e),
            _ => Ok(()),
        }
    }
}

impl error::Error for RSMServiceError {}

#[derive(Clone)]
struct RSMInnerService {
    tx: mpsc::Sender<Message>,
}

impl RSMInnerService {
    pub fn new(tx: mpsc::Sender<Message>) -> Self {
        RSMInnerService { tx }
    }

    pub async fn handle(
        &mut self,
        req: tonic::Request<Bytes>,
    ) -> Result<tonic::Response<Bytes>, RSMServiceError> {
        let body = req.into_inner();
        let (tx_callback, rx_callback) = oneshot::channel();
        let command = Message::Command {
            body,
            tx: tx_callback,
        };
        self.tx
            .send(command)
            .map_err(|_| RSMServiceError::NodeTerminated)
            .await?;

        rx_callback
            .await
            .map_err(|_| RSMServiceError::NodeCrashed)
            .and_then(|res| res.map_err(RSMServiceError::CommandFailed))?;

        // TODO: service returns tonic::Response

        Ok(tonic::Response::new(Bytes::new()))
    }
}

#[derive(Default)]
struct IdentDecoder;

impl Decoder for IdentDecoder {
    type Item = bytes::Bytes;
    type Error = Status;
    fn decode(&mut self, src: &mut DecodeBuf<'_>) -> Result<Option<Self::Item>, Self::Error> {
        Ok(Some(src.to_bytes()))
    }
}

struct RSMService {
    inner: RSMInnerService,
}

impl RSMService {
    fn new(tx: mpsc::Sender<Message>) -> Self {
        RSMService {
            inner: RSMInnerService::new(tx),
        }
    }
}

impl RSMService {
    async fn handle(
        mut inner: RSMInnerService,
        req: http::Request<Body>,
    ) -> Result<http::Response<BoxBody>, RSMServiceError> {
        let (parts, body) = req.into_parts();

        let stream = Streaming::new_request(IdentDecoder::default(), body);
        futures_util::pin_mut!(stream);

        let body = match stream.try_next().await {
            Ok(Some(req)) => Ok(req),
            Ok(None) => Err(RSMServiceError::CommandMissing),
            Err(e) => Err(RSMServiceError::CommandInvalid(e)),
        }?;

        let req = tonic::Request::from_http(http::Request::from_parts(parts, body));
        // TODO: merge trailers

        // TODO: convert tonic::Response to http::Response
        let _ = inner.handle(req).await?;
        Ok(http::Response::new(BoxBody::empty()))
    }
}

impl Service<http::Request<Body>> for RSMService {
    type Response = http::Response<BoxBody>;
    type Error = futures::never::Never;
    type Future =
        Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send + 'static>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: http::Request<Body>) -> Self::Future {
        let inner = self.inner.clone();
        Box::pin(async move {
            let res = RSMService::handle(inner, req).await;
            res.or_else(|e| Ok(e.into_http()))
                .and_then(Ok::<_, Self::Error>)
        })
    }
}
