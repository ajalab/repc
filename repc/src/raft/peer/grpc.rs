use super::error::{ConnectionError, PeerError};
use super::Peer;
use crate::raft::pb::{
    raft_client, AppendEntriesRequest, AppendEntriesResponse, RequestVoteRequest,
    RequestVoteResponse,
};
use log::warn;
use std::error;
use tokio::time::{timeout, Duration};

#[derive(Clone)]
pub struct GrpcPeer {
    client: raft_client::RaftClient<tonic::transport::Channel>,
}

const CONNECTION_RETRY_TIMEOUT_MILLIS: u64 = 5000;
const CONNECTION_RETRY_INTERVAL_MILLIS: u64 = 5000;
const CONNECTION_RETRY_MAX: usize = 100;

impl GrpcPeer {
    pub async fn connect<T>(addr: T) -> Result<Self, ConnectionError>
    where
        T: AsRef<str>,
    {
        let addr = "http://".to_string() + addr.as_ref();
        for _ in 0..CONNECTION_RETRY_MAX {
            let f = timeout(
                Duration::from_millis(CONNECTION_RETRY_TIMEOUT_MILLIS),
                raft_client::RaftClient::connect(addr.clone()),
            )
            .await;

            let e: Box<dyn error::Error> = match f {
                Ok(Ok(client)) => {
                    return Ok(GrpcPeer { client });
                }
                Ok(Err(e)) => e.into(),
                Err(e) => e.into(),
            };

            warn!(
                "attempt to connect to {} failed: {}. retry in {} ms.",
                &addr, e, CONNECTION_RETRY_INTERVAL_MILLIS
            );

            tokio::time::delay_for(Duration::from_millis(CONNECTION_RETRY_INTERVAL_MILLIS)).await;
        }
        Err(ConnectionError::new(addr))
    }
}

#[tonic::async_trait]
impl Peer for GrpcPeer {
    async fn request_vote(
        &mut self,
        req: RequestVoteRequest,
    ) -> Result<RequestVoteResponse, PeerError> {
        self.client
            .request_vote(req)
            .await
            .map(|r| r.into_inner())
            .map_err(|status| PeerError::new(status.to_string()))
    }

    async fn append_entries(
        &mut self,
        req: AppendEntriesRequest,
    ) -> Result<AppendEntriesResponse, PeerError> {
        self.client
            .append_entries(req)
            .await
            .map(|r| r.into_inner())
            .map_err(|status| PeerError::new(status.to_string()))
    }
}
