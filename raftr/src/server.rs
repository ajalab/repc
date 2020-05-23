use crate::message::Message;
use bytes::buf::Buf;
use futures::{FutureExt, TryFutureExt};
use futures_util::TryStreamExt;
use http::request::Request;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::sync::mpsc;
use tonic::body::BoxBody;
use tonic::codec::{DecodeBuf, Decoder, Streaming};
use tonic::transport::Body;
use tonic::Status;
use tower_service::Service;

struct RSMService {
    tx: mpsc::Sender<Message>,
}

#[derive(Debug)]
pub enum Never {}

#[derive(Default)]
struct IdentDecoder;

impl Decoder for IdentDecoder {
    type Item = bytes::Bytes;
    type Error = Status;
    fn decode(&mut self, src: &mut DecodeBuf<'_>) -> Result<Option<Self::Item>, Self::Error> {
        Ok(Some(src.to_bytes()))
    }
}

impl Service<Request<Body>> for RSMService {
    type Response = http::Response<tonic::body::BoxBody>;
    type Error = futures::never::Never;
    type Future =
        Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send + 'static>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: http::Request<Body>) -> Self::Future {
        let (parts, body) = req.into_parts();

        let body = async move {
            let stream = Streaming::new_request(IdentDecoder::default(), body);
            futures_util::pin_mut!(stream);

            stream
                .try_next()
                .map(|m| m.and_then(|body| body.ok_or(Status::internal("missing message"))))
                .await
        };

        // FIXME: This will create unused tx in case of failure above.
        let mut tx = self.tx.clone();
        let (tx_callback, mut rx_callback) = mpsc::channel(1);

        let send = body.and_then(|body| async move {
            let command = Message::Command {
                body,
                tx: tx_callback,
            };
            tx.send(command)
                .map_err(|e| Status::internal("failed to send"))
                .await
        });

        let res = send
            .and_then(|_| async move {
                rx_callback
                    .recv()
                    .await
                    .unwrap_or_else(|| Err(Status::internal("closed")))
            })
            .map_ok_or_else(
                |status| http::Response::new(BoxBody::empty()),
                |res| http::Response::new(BoxBody::empty()),
            )
            .never_error();

        Box::pin(res)
    }
}
