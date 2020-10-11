pub mod error;
mod message;

use self::error::{ConversionError, HandleError};
use self::message::{RaftRequest, RaftResponse, ResponseResult};
use crate::pb::raft::raft_server::Raft;
use crate::pb::raft::{
    AppendEntriesRequest, AppendEntriesResponse, RequestVoteRequest, RequestVoteResponse,
};
use std::convert::TryFrom;
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot, Mutex};
use tonic::{Request, Response, Status};

pub struct PartitionedRaftService {
    tx: mpsc::Sender<(
        Request<RaftRequest>,
        oneshot::Sender<ResponseResult<RaftResponse>>,
    )>,
}

#[tonic::async_trait]
impl Raft for PartitionedRaftService {
    async fn request_vote(
        &self,
        request: Request<RequestVoteRequest>,
    ) -> Result<Response<RequestVoteResponse>, Status> {
        let (tx, rx) = oneshot::channel();
        let mut tx_handle = self.tx.clone();
        let request = request.map(RaftRequest::RequestVote);
        tx_handle
            .send((request, tx))
            .await
            .map_err(|e| Status::internal(format!("handle dropped: {}", e)))?;

        let mut response = rx
            .await
            .unwrap_or_else(|e| Err(Status::internal(format!("handle dropped: {}", e))))?;
        let metadata = std::mem::take(response.metadata_mut());
        let message =
            RequestVoteResponse::try_from(response.into_inner()).map_err(|e| Status::from(e))?;
        let mut response = Response::new(message);
        *response.metadata_mut() = metadata;

        Ok(response)
    }

    async fn append_entries(
        &self,
        request: Request<AppendEntriesRequest>,
    ) -> Result<Response<AppendEntriesResponse>, Status> {
        let (tx, rx) = oneshot::channel();
        let mut tx_handle = self.tx.clone();
        let request = request.map(RaftRequest::AppendEntries);
        tx_handle
            .send((request, tx))
            .await
            .map_err(|e| Status::internal(format!("handle dropped: {}", e)))?;

        let mut response = rx
            .await
            .unwrap_or_else(|e| Err(Status::internal(format!("handle dropped: {}", e))))?;
        let metadata = std::mem::take(response.metadata_mut());
        let message = AppendEntriesResponse::try_from(response.into_inner())?;
        let mut response = Response::new(message);
        *response.metadata_mut() = metadata;

        Ok(response)
    }
}

#[derive(Clone)]
pub struct Handle<S> {
    service: S,
    rx: Arc<
        Mutex<
            mpsc::Receiver<(
                Request<RaftRequest>,
                oneshot::Sender<ResponseResult<RaftResponse>>,
            )>,
        >,
    >,
}

fn clone_request<T: Clone>(request: &Request<T>) -> Request<T> {
    let mut clone = Request::new(request.get_ref().clone());
    *clone.metadata_mut() = request.metadata().clone();
    clone
}

fn clone_response<T: Clone>(response: &Response<T>) -> Response<T> {
    let mut clone = Response::new(response.get_ref().clone());
    *clone.metadata_mut() = response.metadata().clone();
    clone
}

impl<S: Raft> Handle<S> {
    async fn get_request_of<T>(
        &mut self,
    ) -> Result<(Request<T>, oneshot::Sender<ResponseResult<RaftResponse>>), HandleError>
    where
        T: TryFrom<RaftRequest, Error = ConversionError>,
    {
        let (mut request, tx) = {
            let mut rx = self.rx.lock().await;
            rx.recv().await.ok_or_else(|| HandleError::ServiceDropped)?
        };
        let metadata = std::mem::take(request.metadata_mut());
        let message = T::try_from(request.into_inner()).map_err(HandleError::ConversionError)?;

        let mut request = Request::new(message);
        *request.metadata_mut() = metadata;
        Ok((request, tx))
    }

    pub async fn pass_request_vote_request(
        &mut self,
    ) -> Result<
        (
            Request<RequestVoteRequest>,
            ResponseHandle<RequestVoteResponse>,
        ),
        HandleError,
    > {
        let (request, tx) = self.get_request_of::<RequestVoteRequest>().await?;
        let response = self
            .service
            .request_vote(clone_request(&request))
            .await
            .map(|r| r.map(RaftResponse::RequestVote));
        let handle = ResponseHandle::<RequestVoteResponse>::new(response, tx);
        Ok((request, handle))
    }

    pub async fn pass_append_entries_request(
        &mut self,
    ) -> Result<
        (
            Request<AppendEntriesRequest>,
            ResponseHandle<AppendEntriesResponse>,
        ),
        HandleError,
    > {
        let (request, tx) = self.get_request_of::<AppendEntriesRequest>().await?;
        let response = self
            .service
            .append_entries(clone_request(&request))
            .await
            .map(|r| r.map(RaftResponse::AppendEntries));
        let handle = ResponseHandle::<AppendEntriesResponse>::new(response, tx);
        Ok((request, handle))
    }

    pub async fn block_request_vote_request(
        &mut self,
    ) -> Result<Request<RequestVoteRequest>, HandleError> {
        let (request, tx) = self.get_request_of::<RequestVoteRequest>().await?;
        tx.send(Err(Status::internal("request blocked")))
            .map_err(|_| HandleError::ServiceDropped)?;
        Ok(request)
    }

    pub async fn block_append_entries_request(
        &mut self,
    ) -> Result<Request<AppendEntriesRequest>, HandleError> {
        let (request, tx) = self.get_request_of::<AppendEntriesRequest>().await?;
        tx.send(Err(Status::internal("request blocked")))
            .map_err(|_| HandleError::ServiceDropped)?;
        Ok(request)
    }
}

pub struct ResponseHandle<Response> {
    response: ResponseResult<RaftResponse>,
    tx: oneshot::Sender<ResponseResult<RaftResponse>>,
    _r: std::marker::PhantomData<Response>,
}

impl<Response> ResponseHandle<Response> {
    fn new(
        response: ResponseResult<RaftResponse>,
        tx: oneshot::Sender<ResponseResult<RaftResponse>>,
    ) -> Self {
        ResponseHandle {
            response,
            tx,
            _r: std::marker::PhantomData,
        }
    }
}

impl<Response> ResponseHandle<Response>
where
    Response: TryFrom<RaftResponse, Error = ConversionError>,
{
    pub fn pass_response(self) -> Result<ResponseResult<Response>, HandleError> {
        let response = self
            .response
            .as_ref()
            .map(|r| clone_response(r))
            .map_err(|s| s.clone());

        self.tx
            .send(response)
            .map_err(|_| HandleError::ServiceDropped)?;

        Ok(self
            .response
            .map(|res| res.map(|r| Response::try_from(r).unwrap())))
    }

    pub fn block_response(self) -> Result<ResponseResult<Response>, HandleError> {
        self.tx
            .send(Err(Status::internal("response blocked")))
            .map_err(|_| HandleError::ServiceDropped)?;

        Ok(self
            .response
            .map(|res| res.map(|r| Response::try_from(r).unwrap())))
    }
}

pub fn partition<S>(service: S, buffer: usize) -> (PartitionedRaftService, Handle<S>)
where
    S: Raft,
{
    let (tx, rx) = mpsc::channel(buffer);
    let partitioned = PartitionedRaftService { tx };
    let handle = Handle {
        service,
        rx: Arc::new(Mutex::new(rx)),
    };
    (partitioned, handle)
}
