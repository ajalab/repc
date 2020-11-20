use super::commit_manager::CommitManagerNotifier;
use super::message::Appended;
use crate::configuration::LeaderConfiguration;
use crate::pb::raft::raft_client::RaftClient;
use crate::pb::raft::AppendEntriesRequest;
use crate::pb::raft::LogEntry as PbLogEntry;
use crate::state::log::LogIndex;
use crate::state::{State, StateMachine};
use crate::types::{NodeId, Term};
use std::sync::{Arc, Weak};
use std::time::Duration;
use tokio::sync::{mpsc, RwLock};
use tokio::time;
use tonic::body::BoxBody;
use tonic::client::GrpcService;
use tonic::codegen::StdError;

pub struct Appender {
    tx: mpsc::Sender<Appended>,
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
        let (tx, rx) = mpsc::channel::<Appended>(1);
        let mut appender = Appender { tx };
        let process = AppenderProcess {
            id,
            term,
            conf,
            target_id,
            client,
            commit_manager_notifier,
            state,
            rx,
        };

        // Notify to appender beforehand to send heartbeat immediately
        if let Err(e) = appender.try_notify() {
            tracing::warn!(
                id,
                term,
                target_id,
                "failed to notify appender[{}]: {}",
                target_id,
                e
            );
        }

        tokio::spawn(process.run());
        appender
    }

    pub fn try_notify(&mut self) -> Result<(), mpsc::error::TrySendError<Appended>> {
        self.tx.try_send(Appended)
    }
}

struct AppenderProcess<S, T> {
    id: NodeId,
    term: Term,
    conf: Arc<LeaderConfiguration>,
    target_id: NodeId,
    client: RaftClient<T>,
    rx: mpsc::Receiver<Appended>,
    commit_manager_notifier: CommitManagerNotifier,
    state: Weak<RwLock<State<S>>>,
}

impl<S, T> AppenderProcess<S, T>
where
    S: StateMachine,
    T: GrpcService<BoxBody>,
    T::ResponseBody: Send + 'static,
    <T::ResponseBody as http_body::Body>::Error: Into<StdError> + Send,
{
    async fn run(mut self) {
        tracing::debug!(
            id = self.id,
            term = self.term,
            target_id = self.target_id,
            "start appender process",
        );

        // Do not wait for append notification if
        // - first time (send heartbeat immediately)
        // - replication was failed before
        let mut wait_notification = false;
        let mut next_index = {
            let state = self.state.upgrade();
            let state = state.unwrap();
            let state = state.read().await;
            state.log().last_index() + 1
        };
        let mut match_index;

        loop {
            if wait_notification {
                tracing::trace!(
                    id = self.id,
                    term = self.term,
                    target_id = self.target_id,
                    "wait for heartbeat timeout or notification",
                );
                let wait = time::timeout(
                    Duration::from_millis(self.conf.heartbeat_timeout_millis),
                    self.rx.recv(),
                )
                .await;
                if let Ok(None) = wait {
                    break;
                }
            } else {
                let _ = self.rx.try_recv();
            }

            let state = match self.state.upgrade() {
                Some(state) => state,
                None => {
                    tracing::info!(
                        id = self.id,
                        term = self.term,
                        target_id = self.target_id,
                        "can't read the state. likely not being a leader anymore",
                    );
                    break;
                }
            };
            let state = state.read().await;
            let log = state.log();

            let prev_log_index = next_index - 1;
            let prev_log_term = log.get(prev_log_index).map(|e| e.term()).unwrap_or(0);
            let last_committed_index = log.last_committed();
            let entries = log
                .iter_at(next_index)
                .map(|entry| {
                    let command = entry.command();
                    PbLogEntry {
                        term: entry.term(),
                        command_path: command.path().clone().into(),
                        command_body: command.body().as_ref().to_owned(),
                    }
                })
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
                Err(_) => {
                    // TODO: fall into follower if it can't reach the majority of nodes
                    tracing::warn!(
                        id = self.id,
                        term = self.term,
                        target_id = self.target_id,
                        "AppendRequest aborted because it reached the deadline: {}ms",
                        self.conf.wait_append_entries_response_timeout_millis,
                    );
                }
                Ok(Err(e)) => {
                    // TODO: fall into follower if it can't reach the majority of nodes
                    tracing::warn!(
                        id = self.id,
                        term = self.term,
                        target_id = self.target_id,
                        "AppendRequest failed due to a connection error: {}",
                        e,
                    );
                }
                Ok(Ok(res)) => {
                    let res = res.into_inner();
                    if res.success {
                        match_index = prev_log_index + (n_entries as LogIndex);
                        tracing::trace!(
                            id = self.id,
                            term = self.term,
                            target_id = self.target_id,
                            "AppendRequest succeeded. updating match_index to {}",
                            match_index,
                        );
                        if let Err(e) = self
                            .commit_manager_notifier
                            .notify_replicated(self.target_id, match_index)
                            .await
                        {
                            tracing::warn!(
                                id = self.id,
                                term = self.term,
                                target_id = self.target_id,
                                "match_index update is not reported to CommitManager: {}",
                                e
                            );
                        }
                        wait_notification = true;
                    } else {
                        next_index -= 1;
                        wait_notification = false;
                    }
                }
            }
        }
    }
}
