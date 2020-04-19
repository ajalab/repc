use crate::error::{ConnectionError, PeerError};
use crate::grpc::{self, raft_client};

use std::error;

use log::warn;
use tokio::time::{timeout, Duration};

#[derive(Clone)]
pub struct GRPCPeer {
    client: raft_client::RaftClient<tonic::transport::Channel>,
}

const CONNECTION_RETRY_INTERVAL_MS: u64 = 5000;
const CONNECTION_RETRY_MAX: usize = 100;

impl GRPCPeer {
    pub async fn connect<T>(addr: T) -> Result<Self, ConnectionError>
    where
        T: AsRef<str>,
    {
        let addr = "http://".to_string() + addr.as_ref();
        for _ in 0..CONNECTION_RETRY_MAX {
            let f = timeout(
                Duration::from_millis(5000),
                raft_client::RaftClient::connect(addr.clone()),
            )
            .await;

            let e: Box<dyn error::Error> = match f {
                Ok(Ok(client)) => {
                    return Ok(GRPCPeer { client });
                }
                Ok(Err(e)) => e.into(),
                Err(e) => e.into(),
            };

            warn!(
                "attempt to connect to {} failed: {}. retry in {} ms.",
                &addr, e, CONNECTION_RETRY_INTERVAL_MS
            );

            tokio::time::delay_for(Duration::from_millis(CONNECTION_RETRY_INTERVAL_MS)).await;
        }
        Err(ConnectionError::new(addr))
    }

    pub async fn request_vote(
        &mut self,
        req: grpc::RequestVoteRequest,
    ) -> Result<grpc::RequestVoteResponse, PeerError> {
        self.client
            .request_vote(req)
            .await
            .map(|r| r.into_inner())
            .map_err(|status| PeerError::new(status.to_string()))
    }

    pub async fn append_entries(
        &mut self,
        req: grpc::AppendEntriesRequest,
    ) -> Result<grpc::AppendEntriesResponse, PeerError> {
        self.client
            .append_entries(req)
            .await
            .map(|r| r.into_inner())
            .map_err(|status| PeerError::new(status.to_string()))
    }
}
