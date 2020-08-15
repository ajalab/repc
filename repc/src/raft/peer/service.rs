use super::error::PeerError;
use super::RaftPeer;
use crate::raft::pb::{
    raft_server::Raft, AppendEntriesRequest, AppendEntriesResponse, RequestVoteRequest,
    RequestVoteResponse,
};
use tonic::Request;

#[derive(Clone)]
pub struct RaftServicePeer<S: Raft> {
    service: S,
}

impl<S: Raft> RaftServicePeer<S> {
    pub fn new(service: S) -> Self {
        RaftServicePeer { service }
    }
}

#[tonic::async_trait]
impl<S: Raft> RaftPeer for RaftServicePeer<S> {
    async fn request_vote(
        &mut self,
        req: RequestVoteRequest,
    ) -> Result<RequestVoteResponse, PeerError> {
        self.service
            .request_vote(Request::new(req))
            .await
            .map(|r| r.into_inner())
            .map_err(|status| PeerError::new(status.to_string()))
    }

    async fn append_entries(
        &mut self,
        req: AppendEntriesRequest,
    ) -> Result<AppendEntriesResponse, PeerError> {
        self.service
            .append_entries(Request::new(req))
            .await
            .map(|r| r.into_inner())
            .map_err(|status| PeerError::new(status.to_string()))
    }
}
