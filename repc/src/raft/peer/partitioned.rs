use super::error::PeerError;
use super::RaftPeer;
use crate::raft::pb::{
    AppendEntriesRequest, AppendEntriesResponse, RequestVoteRequest, RequestVoteResponse,
};
use tokio::sync::mpsc;

pub fn peer<P: RaftPeer + Send + Sync>(
    inner: P,
    queue_size: usize,
) -> (RaftPartitionedPeer, RaftPartitionedPeerController<P>) {
    let (tx, rx) = mpsc::channel(queue_size);
    let peer = RaftPartitionedPeer { tx: tx.clone() };
    let controller = RaftPartitionedPeerController { inner, tx, rx };

    (peer, controller)
}

#[derive(Debug, Clone)]
pub enum ReqItem {
    RequestVoteRequest { req: RequestVoteRequest },
    RequestVoteResponse { res: RequestVoteResponse },
    AppendEntriesRequest { req: AppendEntriesRequest },
    AppendEntriesResponse { res: AppendEntriesResponse },
}

struct ReqItemWithCallback {
    item: ReqItem,
    tx: mpsc::Sender<ReqItem>,
}

#[derive(Clone)]
pub struct RaftPartitionedPeer {
    tx: mpsc::Sender<ReqItemWithCallback>,
}

pub struct RaftPartitionedPeerController<P> {
    inner: P,
    tx: mpsc::Sender<ReqItemWithCallback>,
    rx: mpsc::Receiver<ReqItemWithCallback>,
}

impl<P: RaftPeer + Send + Sync> RaftPartitionedPeerController<P> {
    pub async fn pass(&mut self) -> Result<ReqItem, PeerError> {
        let ReqItemWithCallback { item, mut tx } = self.rx.recv().await.unwrap();
        tracing::trace!("pass item: {:?}", item);
        match item.clone() {
            ReqItem::RequestVoteRequest { req } => {
                let res = self.inner.request_vote(req).await?;
                let item = ReqItemWithCallback {
                    item: ReqItem::RequestVoteResponse { res },
                    tx,
                };
                self.tx
                    .send(item)
                    .await
                    .map_err(|e| PeerError::new(e.to_string()))
            }
            ReqItem::AppendEntriesRequest { req } => {
                let res = self.inner.append_entries(req).await?;
                let item = ReqItemWithCallback {
                    item: ReqItem::AppendEntriesResponse { res },
                    tx,
                };
                self.tx
                    .send(item)
                    .await
                    .map_err(|e| PeerError::new(e.to_string()))
            }
            res => tx
                .send(res)
                .await
                .map_err(|e| PeerError::new(e.to_string())),
        }
        .map(|_| item)
    }

    pub async fn discard(&mut self) -> Result<ReqItem, PeerError> {
        self.rx
            .recv()
            .await
            .map(|i| {
                tracing::trace!("discard item: {:?}", i.item);
                i.item
            })
            .ok_or_else(|| PeerError::new("queue is closed".to_string()))
    }
}

#[tonic::async_trait]
impl RaftPeer for RaftPartitionedPeer {
    async fn request_vote(
        &mut self,
        req: RequestVoteRequest,
    ) -> Result<RequestVoteResponse, PeerError> {
        let (tx, mut rx) = mpsc::channel(1);
        let item = ReqItemWithCallback {
            item: ReqItem::RequestVoteRequest { req },
            tx,
        };
        self.tx
            .send(item)
            .await
            .map_err(|e| PeerError::new(e.to_string()))?;

        match rx.recv().await {
            Some(ReqItem::RequestVoteResponse { res }) => Ok(res),
            None => Err(PeerError::new("request is discarded".to_owned())),
            _ => unreachable!(),
        }
    }

    async fn append_entries(
        &mut self,
        req: AppendEntriesRequest,
    ) -> Result<AppendEntriesResponse, PeerError> {
        let (tx, mut rx) = mpsc::channel(1);
        let item = ReqItemWithCallback {
            item: ReqItem::AppendEntriesRequest { req },
            tx,
        };
        self.tx
            .send(item)
            .await
            .map_err(|e| PeerError::new(e.to_string()))?;

        if let Some(ReqItem::AppendEntriesResponse { res }) = rx.recv().await {
            Ok(res)
        } else {
            Err(PeerError::new("invalid response".to_owned()))
        }
    }
}
