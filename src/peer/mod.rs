use crate::pb::{
    AppendEntriesRequest, AppendEntriesResponse, RequestVoteRequest, RequestVoteResponse,
};

pub mod error;
pub mod grpc;
#[cfg(test)]
pub mod partitioned;
#[cfg(test)]
pub mod service;

#[tonic::async_trait]
pub trait Peer {
    async fn request_vote(
        &mut self,
        req: RequestVoteRequest,
    ) -> Result<RequestVoteResponse, error::PeerError>;

    async fn append_entries(
        &mut self,
        req: AppendEntriesRequest,
    ) -> Result<AppendEntriesResponse, error::PeerError>;
}
