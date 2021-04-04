use super::{commit_manager::CommitManagerNotifier, message::Ready};
use crate::{
    configuration::LeaderConfiguration,
    pb::raft::raft_client::RaftClient,
    pb::raft::AppendEntriesRequest,
    state::log::{Log, LogIndex},
    state::{State, StateMachine},
    types::{NodeId, Term},
};
use futures::FutureExt;
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

pub struct Appender {
    tx: mpsc::Sender<Ready>,
}

impl Appender {
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
        let appender = Appender { tx };
        let process = AppenderProcess::new(
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
        appender
    }

    /// Returns true if succeeded.
    /// Otherwise false if the target appender process is down.
    pub fn try_notify(&mut self) -> bool {
        let result = self.tx.try_send(Ready);
        !matches!(result, Err(TrySendError::Closed(_)))
    }
}

struct AppenderProcess<S, L, T> {
    id: NodeId,
    term: Term,
    conf: Arc<LeaderConfiguration>,
    target_id: NodeId,
    client: RaftClient<T>,
    rx: mpsc::Receiver<Ready>,
    commit_manager_notifier: CommitManagerNotifier,
    state: Weak<RwLock<State<S, L>>>,
}

impl<S, L, T> AppenderProcess<S, L, T>
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
        AppenderProcess {
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

    async fn append(&mut self, next_index: LogIndex) -> Result<LogIndex, AppendError> {
        let state = self
            .state
            .upgrade()
            .ok_or_else(|| AppendError::NotReadableState)?;
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
            term: self.term,
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
            Err(_) => Err(AppendError::Timeout),
            Ok(Err(e)) => Err(AppendError::ConnectionError(e)),
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
                    if res.term == self.term {
                        Err(AppendError::InconsistentLog)
                    } else {
                        Err(AppendError::InvalidTerm)
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
            target: "leader::appender", "appender",
            id = self.id,
            term = self.term,
            target_id = self.target_id,
        );

        async {
            tracing::info!("started appender");
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
                    target: "leader::appender", "append",
                    match_index = match_index,
                    next_index = next_index,
                );

                let result = self.append(next_index).instrument(span).await;
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
                    Err(e) if e.retryable() => {
                        next_index -= 1;
                        wait = false;
                    }
                    Err(e) => {
                        let _ = self
                            .commit_manager_notifier
                            .notify_failed(self.target_id)
                            .await;
                        if e.graceful() {
                            tracing::info!("shutdown Appender gracefully: {}", e);
                        } else {
                            tracing::warn!("shutdown Appender due to an unexpected error: {}", e);
                        }
                        break;
                    }
                };
            }
        }
        .instrument(span)
        .await
    }
}

enum AppendError {
    InconsistentLog,
    InvalidTerm,
    NotReadableState,
    Timeout,
    ConnectionError(Status),
}

impl AppendError {
    fn retryable(&self) -> bool {
        match self {
            AppendError::InconsistentLog => true,
            _ => false,
        }
    }

    fn graceful(&self) -> bool {
        match self {
            AppendError::InconsistentLog
            | AppendError::InvalidTerm
            | AppendError::NotReadableState => true,
            AppendError::Timeout | AppendError::ConnectionError(_) => false,
        }
    }
}

impl fmt::Display for AppendError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AppendError::InconsistentLog => write!(f, "AppendEntries request was failed due to inconsistent log entries; previous log term in the request doesn't match the actual term"),
            AppendError::InvalidTerm => write!(f, "AppendEntries request was failed because the target node has different term"),
            AppendError::NotReadableState => write!(f, "AppendEntries request was canceled because it couldn't load the state; likely not being a leader anymore"),
            AppendError::Timeout => write!(f, "AppendEntries request aborted due to timeout"),
            AppendError::ConnectionError(e) => write!(f, "AppendEntries request aborted due to a connection error: {:?}", e),
        }
    }
}
