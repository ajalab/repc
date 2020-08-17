mod error;
mod peer;

use crate::raft::message::Message;
use bytes::buf::Buf;
use bytes::Bytes;
use error::RepcServiceError;
use futures_util::{TryFutureExt, TryStreamExt};
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::sync::{mpsc, oneshot};
use tonic::body::BoxBody;
use tonic::codec::{DecodeBuf, Decoder, Streaming};
use tonic::transport::{Body, NamedService};
use tonic::Status;
use tower_service::Service;

#[derive(Clone)]
struct RepcInnerService {
    tx: mpsc::Sender<Message>,
}

impl RepcInnerService {
    pub fn new(tx: mpsc::Sender<Message>) -> Self {
        RepcInnerService { tx }
    }

    pub async fn handle(
        &mut self,
        req: tonic::Request<Bytes>,
    ) -> Result<tonic::Response<Bytes>, RepcServiceError> {
        let body = req.into_inner();
        let (tx_callback, rx_callback) = oneshot::channel();
        let command = Message::Command {
            body,
            tx: tx_callback,
        };
        self.tx
            .send(command)
            .map_err(|_| RepcServiceError::NodeTerminated)
            .await?;

        rx_callback
            .await
            .map_err(|_| RepcServiceError::NodeCrashed)
            .and_then(|res| res.map_err(RepcServiceError::CommandFailed))?;

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

#[derive(Clone)]
pub struct RepcService {
    inner: RepcInnerService,
}

impl RepcService {
    pub fn new(tx: mpsc::Sender<Message>) -> Self {
        RepcService {
            inner: RepcInnerService::new(tx),
        }
    }
}

impl RepcService {
    async fn handle(
        mut inner: RepcInnerService,
        req: http::Request<Body>,
    ) -> Result<http::Response<BoxBody>, RepcServiceError> {
        let (parts, body) = req.into_parts();

        let stream = Streaming::new_request(IdentDecoder::default(), body);
        futures_util::pin_mut!(stream);

        let body = match stream.try_next().await {
            Ok(Some(req)) => Ok(req),
            Ok(None) => Err(RepcServiceError::CommandMissing),
            Err(e) => Err(RepcServiceError::CommandInvalid(e)),
        }?;

        let req = tonic::Request::from_http(http::Request::from_parts(parts, body));
        // TODO: merge trailers

        // TODO: convert tonic::Response to http::Response
        let _ = inner.handle(req).await?;
        Ok(http::Response::new(BoxBody::empty()))
    }
}

impl Service<http::Request<Body>> for RepcService {
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
            let res = RepcService::handle(inner, req).await;
            res.or_else(|e| Ok(e.into_http()))
                .and_then(Ok::<_, Self::Error>)
        })
    }
}

impl NamedService for RepcService {
    const NAME: &'static str = "repc.Repc";
}
