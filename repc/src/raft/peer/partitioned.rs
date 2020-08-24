use super::error::PeerError;
use super::RaftPeer;
use crate::raft::pb::{
    AppendEntriesRequest, AppendEntriesResponse, RequestVoteRequest, RequestVoteResponse,
};
use std::borrow::BorrowMut;
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot};
use tokio::sync::{RwLock, RwLockWriteGuard};

#[derive(Debug, Clone)]
pub enum Request {
    RequestVoteRequest(RequestVoteRequest),
    AppendEntriesRequest(AppendEntriesRequest),
}

impl Request {
    pub fn unwrap_request_vote(self) -> RequestVoteRequest {
        if let Request::RequestVoteRequest(req) = self {
            req
        } else {
            panic!("request is not RequestVoteRequest");
        }
    }

    pub fn unwrap_append_entries(self) -> AppendEntriesRequest {
        if let Request::AppendEntriesRequest(req) = self {
            req
        } else {
            panic!("request is not AppendEntriesRequest");
        }
    }
}

struct RequestWithCallback {
    req: Request,
    callback_tx: oneshot::Sender<Response>,
}

#[derive(Debug, Clone)]
pub enum Response {
    RequestVoteResponse(RequestVoteResponse),
    AppendEntriesResponse(AppendEntriesResponse),
}

impl Response {
    pub fn unwrap_request_vote(self) -> RequestVoteResponse {
        if let Response::RequestVoteResponse(res) = self {
            res
        } else {
            panic!("response is not ResponseVoteResponse");
        }
    }

    pub fn unwrap_append_entries(self) -> AppendEntriesResponse {
        if let Response::AppendEntriesResponse(res) = self {
            res
        } else {
            panic!("response is not AppendEntriesResponse");
        }
    }
}

struct ResponseWithCallback {
    res: Response,
    callback_tx: oneshot::Sender<Response>,
}

#[derive(Clone)]
pub struct RaftPartitionedPeer<P> {
    inner: P,
    req_queue_tx: mpsc::Sender<RequestWithCallback>,
    res_queue_tx: mpsc::Sender<ResponseWithCallback>,
    req_verifier: Arc<RwLock<Option<Box<dyn Fn(Request) -> () + Send + Sync>>>>,
    res_verifier: Arc<RwLock<Option<Box<dyn Fn(Response) -> () + Send + Sync>>>>,
}

#[tonic::async_trait]
impl<P: RaftPeer + Send + Sync> RaftPeer for RaftPartitionedPeer<P> {
    async fn request_vote(
        &mut self,
        req: RequestVoteRequest,
    ) -> Result<RequestVoteResponse, PeerError> {
        let (callback_tx, callback_rx) = oneshot::channel();
        {
            let mut req_verifier = self.req_verifier.write().await;
            if let Some(verifier) = req_verifier.take() {
                tracing::trace!("sending RequestVote request with verification: {:?}", req);
                let res = self.inner.request_vote(req.clone()).await?;
                let res = Response::RequestVoteResponse(res);
                verifier(Request::RequestVoteRequest(req));

                let res_verifier = self.res_verifier.write().await;
                handle_response(res, callback_tx, &mut self.res_queue_tx, res_verifier).await?;
            } else {
                tracing::trace!("adding RequestVote request to the queue: {:?}", req);
                let req = RequestWithCallback {
                    req: Request::RequestVoteRequest(req),
                    callback_tx,
                };
                self.req_queue_tx
                    .send(req)
                    .await
                    .map_err(|e| PeerError::new(e.to_string()))?;
            }
        }

        match callback_rx.await {
            Ok(Response::RequestVoteResponse(res)) => Ok(res),
            Ok(_) => unreachable!(),
            Err(_) => Err(PeerError::new("request is discarded".to_owned())),
        }
    }

    async fn append_entries(
        &mut self,
        req: AppendEntriesRequest,
    ) -> Result<AppendEntriesResponse, PeerError> {
        let (callback_tx, callback_rx) = oneshot::channel();
        {
            let mut req_verifier = self.req_verifier.write().await;
            if let Some(verifier) = req_verifier.take() {
                tracing::trace!("sending AppendEntries request with verification: {:?}", req);
                let res = self.inner.append_entries(req.clone()).await?;
                let res = Response::AppendEntriesResponse(res);
                verifier(Request::AppendEntriesRequest(req));

                let res_verifier = self.res_verifier.write().await;
                handle_response(res, callback_tx, &mut self.res_queue_tx, res_verifier).await?;
            } else {
                tracing::trace!("adding AppendEntries request to the queue: {:?}", req);
                let req = RequestWithCallback {
                    req: Request::AppendEntriesRequest(req),
                    callback_tx,
                };
                self.req_queue_tx
                    .send(req)
                    .await
                    .map_err(|e| PeerError::new(e.to_string()))?;
            }
        }

        match callback_rx.await {
            Ok(Response::AppendEntriesResponse(res)) => Ok(res),
            Ok(_) => unreachable!(),
            Err(_) => Err(PeerError::new("request is discarded".to_owned())),
        }
    }
}

pub struct RaftPartitionedPeerController<P> {
    inner: P,
    req_queue_rx: mpsc::Receiver<RequestWithCallback>,
    res_queue_tx: mpsc::Sender<ResponseWithCallback>,
    res_queue_rx: mpsc::Receiver<ResponseWithCallback>,
    req_verifier: Arc<RwLock<Option<Box<dyn Fn(Request) -> () + Send + Sync>>>>,
    res_verifier: Arc<RwLock<Option<Box<dyn Fn(Response) -> () + Send + Sync>>>>,
}

impl<P: RaftPeer + Send + Sync> RaftPartitionedPeerController<P> {
    pub async fn pass_request(&mut self) -> Result<Request, PeerError> {
        let RequestWithCallback { req, callback_tx } = self.req_queue_rx.recv().await.unwrap();
        tracing::trace!("pass request: {:?}", req);
        match req.clone() {
            Request::RequestVoteRequest(req) => {
                let res = self.inner.request_vote(req).await?;
                let res = Response::RequestVoteResponse(res);
                let res_verifier = self.res_verifier.write().await;
                handle_response(res, callback_tx, &mut self.res_queue_tx, res_verifier).await?;
            }
            Request::AppendEntriesRequest(req) => {
                let res = self.inner.append_entries(req).await?;
                let res = Response::AppendEntriesResponse(res);
                let res_verifier = self.res_verifier.write().await;
                handle_response(res, callback_tx, &mut self.res_queue_tx, res_verifier).await?;
            }
        }
        Ok(req)
    }

    pub async fn pass_response(&mut self) -> Result<Response, PeerError> {
        let ResponseWithCallback { res, callback_tx } = self.res_queue_rx.recv().await.unwrap();
        tracing::trace!("pass response: {:?}", res);
        callback_tx
            .send(res.clone())
            .map_err(|_| PeerError::new("failed to callback".to_owned()))
            .map(|_| res)
    }

    pub async fn discard_request(&mut self) -> Result<Request, PeerError> {
        self.req_queue_rx
            .recv()
            .await
            .map(|req| {
                tracing::trace!("discard request: {:?}", req.req);
                req.req
            })
            .ok_or_else(|| PeerError::new("queue is closed".to_string()))
    }

    pub async fn discard_response(&mut self) -> Result<Response, PeerError> {
        self.res_queue_rx
            .recv()
            .await
            .map(|res| {
                tracing::trace!("discard response: {:?}", res.res);
                res.res
            })
            .ok_or_else(|| PeerError::new("queue is closed".to_string()))
    }

    pub async fn pass_next_request(
        &mut self,
        verifier: impl Fn(Request) -> () + Send + Sync + 'static,
    ) {
        let mut req_verifier = self.req_verifier.write().await;
        *req_verifier = Some(Box::new(verifier));
        tracing::trace!("set a request verifier");
    }

    pub async fn pass_next_response(
        &mut self,
        verifier: impl Fn(Response) -> () + Send + Sync + 'static,
    ) {
        let mut res_verifier = self.res_verifier.write().await;
        *res_verifier = Some(Box::new(verifier));
        tracing::trace!("set a response verifier");
    }
}

async fn handle_response<'a>(
    res: Response,
    callback_tx: oneshot::Sender<Response>,
    res_queue_tx: &mut mpsc::Sender<ResponseWithCallback>,
    mut verifier: RwLockWriteGuard<'a, Option<Box<dyn Fn(Response) -> () + Send + Sync>>>,
) -> Result<(), PeerError> {
    let verifier: &mut Option<_> = verifier.borrow_mut();
    if let Some(verifier) = verifier.take() {
        callback_tx
            .send(res.clone())
            .map_err(|_| PeerError::new("failed to send callback".to_owned()))?;
        verifier(res);
    } else {
        let res = ResponseWithCallback { res, callback_tx };
        res_queue_tx
            .send(res)
            .await
            .map_err(|e| PeerError::new(e.to_string()))?;
    }
    Ok(())
}

pub fn peer<P: RaftPeer + Send + Sync + Clone>(
    inner: P,
    queue_size: usize,
) -> (RaftPartitionedPeer<P>, RaftPartitionedPeerController<P>) {
    let (req_queue_tx, req_queue_rx) = mpsc::channel(queue_size);
    let (res_queue_tx, res_queue_rx) = mpsc::channel(queue_size);
    let req_verifier: Arc<RwLock<_>> = Arc::default();
    let res_verifier: Arc<RwLock<_>> = Arc::default();
    let peer = RaftPartitionedPeer {
        inner: inner.clone(),
        req_queue_tx: req_queue_tx.clone(),
        res_queue_tx: res_queue_tx.clone(),
        req_verifier: req_verifier.clone(),
        res_verifier: res_verifier.clone(),
    };
    let controller = RaftPartitionedPeerController {
        inner,
        req_queue_rx,
        res_queue_tx,
        res_queue_rx,
        req_verifier,
        res_verifier,
    };

    (peer, controller)
}
