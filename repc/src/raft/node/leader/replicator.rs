use super::{commit_manager::CommitManagerNotifier, message::Ready};
use crate::{
    configuration::LeaderConfiguration,
    log::{Log, LogIndex},
    pb::raft::raft_client::RaftClient,
    pb::raft::AppendEntriesRequest,
    state::State,
    state_machine::StateMachine,
    types::Term,
};
use futures::FutureExt;
use repc_common::repc::types::NodeId;
use std::{
    fmt,
    sync::{Arc, Weak},
    time::Duration,
};
use tokio::{
    sync::{
        mpsc::{self, error::TrySendError},
        RwLock,
    },
    time,
};
use tonic::{body::BoxBody, client::GrpcService, codegen::StdError, Status};
use tracing::Instrument;

pub struct Replicator {
    tx: mpsc::Sender<Ready>,
}

impl Replicator {
    pub fn spawn<S, L, T>(
        id: NodeId,
        term: Term,
        conf: Arc<LeaderConfiguration>,
        target_id: NodeId,
        commit_manager_notifier: CommitManagerNotifier,
        state: Weak<RwLock<State<S, L>>>,
        client: RaftClient<T>,
    ) -> Self
    where
        S: StateMachine + Send + Sync + 'static,
        L: Log + Send + Sync + 'static,
        T: GrpcService<BoxBody> + Send + Sync + 'static,
        T::Future: Send,
        <T::ResponseBody as http_body::Body>::Error: Into<StdError> + Send,
    {
        let (tx, rx) = mpsc::channel::<Ready>(1);
        let replicator = Replicator { tx };
        let process = ReplicatorProcess::new(
            id,
            term,
            conf,
            target_id,
            client,
            rx,
            commit_manager_notifier,
            state,
        );

        tokio::spawn(process.run());
        replicator
    }

    /// Returns true if succeeded.
    /// Otherwise false if the target replicator process is down.
    pub fn try_notify(&mut self) -> bool {
        let result = self.tx.try_send(Ready);
        !matches!(result, Err(TrySendError::Closed(_)))
    }
}

struct ReplicatorProcess<S, L, T> {
    id: NodeId,
    term: Term,
    conf: Arc<LeaderConfiguration>,
    target_id: NodeId,
    client: RaftClient<T>,
    rx: mpsc::Receiver<Ready>,
    commit_manager_notifier: CommitManagerNotifier,
    state: Weak<RwLock<State<S, L>>>,
}

impl<S, L, T> ReplicatorProcess<S, L, T>
where
    S: StateMachine,
    L: Log,
    T: GrpcService<BoxBody>,
    T::ResponseBody: Send + 'static,
    <T::ResponseBody as http_body::Body>::Error: Into<StdError> + Send,
{
    fn new(
        id: NodeId,
        term: Term,
        conf: Arc<LeaderConfiguration>,
        target_id: NodeId,
        client: RaftClient<T>,
        rx: mpsc::Receiver<Ready>,
        commit_manager_notifier: CommitManagerNotifier,
        state: Weak<RwLock<State<S, L>>>,
    ) -> Self {
        ReplicatorProcess {
            id,
            term,
            conf,
            target_id,
            client,
            commit_manager_notifier,
            state,
            rx,
        }
    }

    async fn replicate(&mut self, next_index: LogIndex) -> Result<LogIndex, ReplicateError> {
        let state = self
            .state
            .upgrade()
            .ok_or_else(|| ReplicateError::NotReadableState)?;
        let state = state.read().await;
        let log = state.log();

        let prev_log_index = next_index - 1;
        let prev_log_term = log.get(prev_log_index).map(|e| e.term).unwrap_or(0);
        let last_committed_index = state.last_committed();
        let entries = log
            .get_from(next_index)
            .iter()
            .map(Clone::clone)
            .collect::<Vec<_>>();
        let n_entries = entries.len();
        drop(log);

        let append_entries = self.client.append_entries(AppendEntriesRequest {
            leader_id: self.id,
            term: self.term.get(),
            prev_log_index,
            prev_log_term,
            last_committed_index,
            entries,
        });

        let res = time::timeout(
            Duration::from_millis(self.conf.wait_append_entries_response_timeout_millis),
            append_entries,
        )
        .await;

        match res {
            Err(_) => Err(ReplicateError::Timeout),
            Ok(Err(e)) => Err(ReplicateError::ConnectionError(e)),
            Ok(Ok(res)) => {
                let res = res.into_inner();
                if res.success {
                    if n_entries > 0 {
                        tracing::trace!(
                            "AppendRequest succeeded in appending {} entries",
                            n_entries,
                        );
                    } else {
                        tracing::trace!("AppendRequest succeeded in sending a heartbeat");
                    }
                    Ok(prev_log_index + (n_entries as LogIndex))
                } else {
                    if res.term == self.term.get() {
                        Err(ReplicateError::InconsistentLog)
                    } else {
                        Err(ReplicateError::InvalidTerm)
                    }
                }
            }
        }
    }

    async fn run(mut self) {
        let mut next_index = {
            let state = self.state.upgrade();
            let state = state.unwrap();
            let state = state.read().await;
            state.log().last_index() + 1
        };
        let mut match_index = 0;
        let mut wait = false;

        let span = tracing::info_span!(
            target: "leader::replicator", "replicator",
            id = self.id,
            term = self.term.get(),
            target_id = self.target_id,
        );

        async {
            tracing::info!("started replicator");
            loop {
                if wait {
                    tracing::trace!("wait for heartbeat timeout or notification");
                    let wait = time::timeout(
                        Duration::from_millis(self.conf.heartbeat_timeout_millis),
                        self.rx.recv(),
                    )
                    .await;
                    if let Ok(None) = wait {
                        break;
                    }
                } else {
                    let _ = self.rx.recv().now_or_never();
                }

                let span = tracing::trace_span!(
                    target: "leader::replicator", "replicate",
                    match_index = match_index,
                    next_index = next_index,
                );

                let result = self.replicate(next_index).instrument(span).await;
                match result {
                    Ok(m_idx) => {
                        let _ = self
                            .commit_manager_notifier
                            .notify_success(self.target_id, m_idx)
                            .await;
                        match_index = m_idx;
                        next_index = m_idx + 1;
                        wait = true;
                    }
                    Err(e) => {
                        let error = e.to_string();
                        let error = AsRef::<str>::as_ref(&error);
                        match e {
                            ReplicateError::Timeout | ReplicateError::ConnectionError(_) => {
                                tracing::warn!(
                                    error,
                                    "failed to replicate logs due to an error. going to retry after backoff",
                                );
                                wait = true;
                            }
                            ReplicateError::InconsistentLog => {
                                tracing::debug!(
                                    error,
                                    "failed to replicate logs due to an error. going to retry soon",
                                );
                                next_index -= 1;
                                wait = false;
                            }
                            ReplicateError::InvalidTerm | ReplicateError::NotReadableState => {
                                let _ = self
                                    .commit_manager_notifier
                                    .notify_failed(self.target_id)
                                    .await;
                                tracing::warn!(error, "shutdown Replicator due to an unexpected error");
                                break;
                            }
                        }
                    }
                };
            }
        }
        .instrument(span)
        .await
    }
}

enum ReplicateError {
    InconsistentLog,
    InvalidTerm,
    NotReadableState,
    Timeout,
    ConnectionError(Status),
}

impl fmt::Display for ReplicateError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ReplicateError::InconsistentLog => write!(f, "AppendEntries request was failed due to inconsistent log entries; previous log term in the request doesn't match the actual term"),
            ReplicateError::InvalidTerm => write!(f, "AppendEntries request was failed because the target node has different term"),
            ReplicateError::NotReadableState => write!(f, "AppendEntries request was canceled because it couldn't load the state; likely not being a leader anymore"),
            ReplicateError::Timeout => write!(f, "AppendEntries request aborted due to timeout"),
            ReplicateError::ConnectionError(e) => write!(f, "AppendEntries request aborted due to a connection error: {:?}", e),
        }
    }
}
