use super::error::ConversionError;
use crate::pb::raft::{
    AppendEntriesRequest, AppendEntriesResponse, RequestVoteRequest, RequestVoteResponse,
};
use std::convert::TryFrom;
use tonic::{Response, Status};

#[derive(Clone, Debug)]
pub enum RaftRequest {
    AppendEntries(AppendEntriesRequest),
    RequestVote(RequestVoteRequest),
}

impl TryFrom<RaftRequest> for AppendEntriesRequest {
    type Error = ConversionError;

    fn try_from(res: RaftRequest) -> Result<Self, Self::Error> {
        match res {
            RaftRequest::AppendEntries(res) => Ok(res),
            _ => Err(ConversionError::ExpectedAppendEntriesRequest),
        }
    }
}

impl TryFrom<RaftRequest> for RequestVoteRequest {
    type Error = ConversionError;

    fn try_from(res: RaftRequest) -> Result<Self, Self::Error> {
        match res {
            RaftRequest::RequestVote(res) => Ok(res),
            _ => Err(ConversionError::ExpectedRequestVoteRequest),
        }
    }
}

#[derive(Clone, Debug)]
pub enum RaftResponse {
    AppendEntries(AppendEntriesResponse),
    RequestVote(RequestVoteResponse),
}

impl TryFrom<RaftResponse> for AppendEntriesResponse {
    type Error = ConversionError;

    fn try_from(res: RaftResponse) -> Result<Self, Self::Error> {
        match res {
            RaftResponse::AppendEntries(res) => Ok(res),
            _ => Err(ConversionError::ExpectedAppendEntriesResponse),
        }
    }
}

impl TryFrom<RaftResponse> for RequestVoteResponse {
    type Error = ConversionError;

    fn try_from(res: RaftResponse) -> Result<Self, Self::Error> {
        match res {
            RaftResponse::RequestVote(res) => Ok(res),
            _ => Err(ConversionError::ExpectedRequestVoteResponse),
        }
    }
}

pub type ResponseResult<T> = Result<Response<T>, Status>;
