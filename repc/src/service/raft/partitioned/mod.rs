pub mod error;
mod message;

use self::error::{ConversionError, HandleError};
use self::message::{RaftRequest, RaftResponse, ResponseResult};
use crate::pb::raft::raft_server::Raft;
use crate::pb::raft::{
    AppendEntriesRequest, AppendEntriesResponse, RequestVoteRequest, RequestVoteResponse,
};
use std::collections::VecDeque;
use std::convert::TryFrom;
use tokio::sync::{mpsc, oneshot};
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

pub struct Handle<S> {
    service: S,
    pendings: VecDeque<(
        ResponseResult<RaftResponse>,
        oneshot::Sender<ResponseResult<RaftResponse>>,
    )>,
    rx: mpsc::Receiver<(
        Request<RaftRequest>,
        oneshot::Sender<ResponseResult<RaftResponse>>,
    )>,
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
        let (mut request, tx) = self
            .rx
            .recv()
            .await
            .ok_or_else(|| HandleError::ServiceDropped)?;
        let metadata = std::mem::take(request.metadata_mut());
        let message = T::try_from(request.into_inner()).map_err(HandleError::ConversionError)?;

        let mut request = Request::new(message);
        *request.metadata_mut() = metadata;
        Ok((request, tx))
    }

    pub async fn pass_request_vote_request(
        &mut self,
    ) -> Result<Request<RequestVoteRequest>, HandleError> {
        let (request, tx) = self.get_request_of::<RequestVoteRequest>().await?;
        let response = self
            .service
            .request_vote(clone_request(&request))
            .await
            .map(|r| r.map(RaftResponse::RequestVote));
        self.pendings.push_back((response, tx));
        Ok(request)
    }

    pub async fn pass_append_entries_request(
        &mut self,
    ) -> Result<Request<AppendEntriesRequest>, HandleError> {
        let (request, tx) = self.get_request_of::<AppendEntriesRequest>().await?;
        let response = self
            .service
            .append_entries(clone_request(&request))
            .await
            .map(|r| r.map(RaftResponse::AppendEntries));
        self.pendings.push_back((response, tx));
        Ok(request)
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

    async fn get_response<T>(
        &mut self,
    ) -> Result<
        (
            ResponseResult<T>,
            oneshot::Sender<ResponseResult<RaftResponse>>,
        ),
        HandleError,
    >
    where
        T: TryFrom<RaftResponse, Error = ConversionError>,
    {
        let (result, tx) = self
            .pendings
            .pop_front()
            .ok_or_else(|| HandleError::NoPendingResponse)?;
        let result = match result {
            Ok(mut response) => {
                let metadata = std::mem::take(response.metadata_mut());
                let message =
                    T::try_from(response.into_inner()).map_err(HandleError::ConversionError)?;
                let mut response = Response::new(message);
                *response.metadata_mut() = metadata;
                Ok(response)
            }
            Err(status) => Err(status),
        };
        Ok((result, tx))
    }

    pub async fn pass_request_vote_response(
        &mut self,
    ) -> Result<ResponseResult<RequestVoteResponse>, HandleError> {
        let (response, tx) = self.get_response::<RequestVoteResponse>().await?;

        tx.send(
            response
                .as_ref()
                .map(|r| clone_response(r).map(RaftResponse::RequestVote))
                .map_err(|s| s.clone()),
        )
        .map_err(|_| HandleError::ServiceDropped)?;

        Ok(response)
    }

    pub async fn pass_append_entries_response(
        &mut self,
    ) -> Result<ResponseResult<AppendEntriesResponse>, HandleError> {
        let (response, tx) = self.get_response::<AppendEntriesResponse>().await?;

        tx.send(
            response
                .as_ref()
                .map(|r| clone_response(r).map(RaftResponse::AppendEntries))
                .map_err(|s| s.clone()),
        )
        .map_err(|_| HandleError::ServiceDropped)?;

        Ok(response)
    }

    pub async fn block_request_vote_response(
        &mut self,
    ) -> Result<ResponseResult<RequestVoteResponse>, HandleError> {
        let (response, tx) = self.get_response::<RequestVoteResponse>().await?;

        tx.send(Err(Status::internal("response blocked")))
            .map_err(|_| HandleError::ServiceDropped)?;

        Ok(response)
    }

    pub async fn block_append_entries_response(
        &mut self,
    ) -> Result<ResponseResult<AppendEntriesResponse>, HandleError> {
        let (response, tx) = self.get_response::<AppendEntriesResponse>().await?;

        tx.send(Err(Status::internal("response blocked")))
            .map_err(|_| HandleError::ServiceDropped)?;

        Ok(response)
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
        rx,
        pendings: VecDeque::new(),
    };
    (partitioned, handle)
}
