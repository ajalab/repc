pub mod codec;
mod error;
mod peer;

use crate::raft::message::Message;
use bytes::Bytes;
use tokio::sync::{mpsc, oneshot};
use tonic::codegen::BoxFuture;
use tonic::Status;

pub trait RepcService {
    fn repc(&self) -> Repc;

    fn from_tx(tx: mpsc::Sender<Message>) -> Self;
}

pub struct RepcUnaryService {
    tx: mpsc::Sender<Message>,
}

impl RepcUnaryService {
    fn new(tx: mpsc::Sender<Message>) -> Self {
        RepcUnaryService { tx }
    }
}

impl tonic::server::UnaryService<Bytes> for RepcUnaryService {
    type Response = Bytes;
    type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;

    fn call(&mut self, req: tonic::Request<Bytes>) -> Self::Future {
        let (callback_tx, callback_rx) = oneshot::channel();
        let command = Message::Command {
            body: req.into_inner(),
            tx: callback_tx,
        };
        let mut tx = self.tx.clone();
        let fut = async move {
            if tx.send(command).await.is_ok() {
                match callback_rx.await {
                    Ok(Ok(body)) => Ok(tonic::Response::new(body)),
                    Ok(Err(e)) => Err(e.into_status()),
                    Err(e) => Err(Status::internal(e.to_string())),
                }
            } else {
                Err(Status::internal("terminated"))
            }
        };
        Box::pin(fut)
    }
}

#[derive(Clone)]
pub struct Repc {
    tx: mpsc::Sender<Message>,
}

impl Repc {
    pub fn new(tx: mpsc::Sender<Message>) -> Self {
        Repc { tx }
    }

    pub fn into_unary_service(self) -> RepcUnaryService {
        RepcUnaryService::new(self.tx)
    }
}
