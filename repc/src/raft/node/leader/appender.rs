use super::commit_manager::CommitManagerNotifier;
use super::message::Ready;
use crate::configuration::LeaderConfiguration;
use crate::pb::raft::raft_client::RaftClient;
use crate::pb::raft::AppendEntriesRequest;
use crate::state::log::LogIndex;
use crate::state::{State, StateMachine};
use crate::types::{NodeId, Term};
use std::sync::{Arc, Weak};
use std::time::Duration;
use tokio::sync::{
    mpsc::{self, error::TrySendError},
    RwLock,
};
use tokio::time;
use tonic::body::BoxBody;
use tonic::client::GrpcService;
use tonic::codegen::StdError;
use tracing::Instrument;

pub struct Appender {
    tx: mpsc::Sender<Ready>,
}

impl Appender {
    pub fn spawn<S, T>(
        id: NodeId,
        term: Term,
        conf: Arc<LeaderConfiguration>,
        target_id: NodeId,
        commit_manager_notifier: CommitManagerNotifier,
        state: Weak<RwLock<State<S>>>,
        client: RaftClient<T>,
    ) -> Self
    where
        S: StateMachine + Send + Sync + 'static,
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

struct AppenderProcess<S, T> {
    id: NodeId,
    term: Term,
    conf: Arc<LeaderConfiguration>,
    target_id: NodeId,
    client: RaftClient<T>,
    rx: mpsc::Receiver<Ready>,
    commit_manager_notifier: CommitManagerNotifier,
    state: Weak<RwLock<State<S>>>,

    wait_notification: bool,
    next_index: LogIndex,
    match_index: LogIndex,
}

impl<S, T> AppenderProcess<S, T>
where
    S: StateMachine,
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
        state: Weak<RwLock<State<S>>>,
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

            wait_notification: false,
            next_index: 0,
            match_index: 0,
        }
    }

    async fn append(&mut self) {
        if self.wait_notification {
            tracing::trace!("wait for heartbeat timeout or notification");
            let wait = time::timeout(
                Duration::from_millis(self.conf.heartbeat_timeout_millis),
                self.rx.recv(),
            )
            .await;
            if let Ok(None) = wait {
                return;
            }
        } else {
            let _ = self.rx.try_recv();
        }

        let state = match self.state.upgrade() {
            Some(state) => state,
            None => {
                tracing::info!("can't read the state. likely not being a leader anymore",);
                return;
            }
        };
        let state = state.read().await;
        let log = state.log();

        let prev_log_index = self.next_index - 1;
        let prev_log_term = log.get(prev_log_index).map(|e| e.term).unwrap_or(0);
        let last_committed_index = log.last_committed();
        let entries = log
            .iter_at(self.next_index)
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

        let (notify, success) = match res {
            Err(_) => {
                tracing::warn!(
                    "AppendRequest aborted because it reached the deadline: {}ms",
                    self.conf.wait_append_entries_response_timeout_millis,
                );
                (true, false)
            }
            Ok(Err(e)) => {
                tracing::warn!("AppendRequest failed due to a connection error: {}", e,);
                (true, false)
            }
            Ok(Ok(res)) => {
                let res = res.into_inner();
                if res.success {
                    self.match_index = prev_log_index + (n_entries as LogIndex);
                    tracing::trace!(
                            "AppendRequest succeeded in appending {} entries. Updating match_index from {} to {}",
                            n_entries,
                            prev_log_index,
                            self.match_index,
                        );
                    self.wait_notification = true;
                    (true, true)
                } else {
                    self.next_index -= 1;
                    self.wait_notification = false;
                    (false, false)
                }
            }
        };
        if notify {
            if let Err(e) = self
                .commit_manager_notifier
                .notify(self.target_id, self.match_index, success)
                .await
            {
                tracing::warn!("append result is not notified to CommitManager: {}", e);
            }
        }
    }

    async fn run(mut self) {
        self.next_index = {
            let state = self.state.upgrade();
            let state = state.unwrap();
            let state = state.read().await;
            state.log().last_index() + 1
        };

        let span = tracing::info_span!(
            target: "leader::appender", "appender",
            id = self.id,
            term = self.term,
            target_id = self.target_id,
        );

        async {
            tracing::info!("started appender");
            loop {
                let span = tracing::trace_span!(
                    target: "leader::appender", "append",
                    next_index = self.next_index,
                    match_index = self.match_index,
                );
                self.append().instrument(span).await;
            }
        }
        .instrument(span)
        .await
    }
}
