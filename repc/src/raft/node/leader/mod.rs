mod appender;
mod commit_manager;
pub mod error;
mod message;

use self::appender::Appender;
use self::commit_manager::CommitManager;
use super::error::CommandError;
use crate::configuration::Configuration;
use crate::pb::raft::{log_entry::Command, raft_client::RaftClient, LogEntry};
use crate::session::{RepcClientId, Sessions};
use crate::state::{State, StateMachine};
use crate::types::{NodeId, Term};
use bytes::{Buf, Bytes};
use futures::{future::BoxFuture, FutureExt};
use repc_proto::types::Sequence;
use std::collections::HashMap;
use std::iter;
use std::sync::Arc;
use tokio::sync::{oneshot, RwLock};
use tonic::body::BoxBody;
use tonic::client::GrpcService;
use tonic::codegen::StdError;
use tracing::{Instrument, Level};

pub struct Leader<S> {
    term: Term,
    appenders: Vec<Appender>,
    commit_manager: CommitManager,
    state: Option<Arc<RwLock<State<S>>>>,
    sessions: Arc<Sessions>,
}

impl<S> Leader<S>
where
    S: StateMachine + Send + Sync + 'static,
{
    pub fn spawn<T>(
        id: NodeId,
        conf: Arc<Configuration>,
        term: Term,
        state: State<S>,
        sessions: Arc<Sessions>,
        clients: &HashMap<NodeId, RaftClient<T>>,
    ) -> Self
    where
        T: GrpcService<BoxBody> + Clone + Send + Sync + 'static,
        T::Future: Send,
        <T::ResponseBody as http_body::Body>::Error: Into<StdError> + Send,
    {
        let leader_conf = Arc::new(conf.leader.clone());
        let state = Arc::new(RwLock::new(state));

        let nodes = clients.keys().copied();
        let (commit_manager, commit_manager_notifier) =
            CommitManager::spawn(id, term, nodes, Arc::downgrade(&state));

        let appenders = clients
            .iter()
            .map(|(&target_id, client)| {
                Appender::spawn(
                    id,
                    term,
                    leader_conf.clone(),
                    target_id,
                    commit_manager_notifier.clone(),
                    Arc::downgrade(&state),
                    client.clone(),
                )
            })
            .collect::<Vec<_>>();

        let leader = Leader {
            term,
            appenders,
            commit_manager,
            state: Some(state),
            sessions,
        };

        leader
    }
}

impl<S> Leader<S>
where
    S: StateMachine,
{
    pub async fn handle_command(
        &mut self,
        command: Command,
        client_id: RepcClientId,
        sequence: Sequence,
        tx: oneshot::Sender<Result<tonic::Response<Bytes>, CommandError>>,
    ) {
        tracing::event!(Level::TRACE, "verification phase");
        if let Command::Action(_) = command {
            match self.sessions.verify(client_id, sequence).await {
                Ok(Some(res)) => {
                    tx.send(res).unwrap();
                    return;
                }
                Err(e) => {
                    tx.send(Err(CommandError::SessionError(e))).unwrap();
                    return;
                }
                _ => {}
            }
        }

        let coprocessor = self.resolve_result_coprocessor(&command);

        let index = {
            let mut state = self.state.as_ref().unwrap().write().await;
            let entry = LogEntry {
                term: self.term,
                command: Some(command),
            };
            state.append_log_entries(iter::once(entry));
            state.last_index()
        };
        tracing::trace!("appended a command at log index {}", index);

        let subscription = self.commit_manager.subscribe();
        let span = tracing::trace_span!(target: "leader", "coprocess_result");
        tokio::spawn(
            subscription
                .wait_applied(index)
                .then(coprocessor)
                .map(|result| tx.send(result))
                .instrument(span),
        );

        for appender in &mut self.appenders {
            let _ = appender.try_notify();
        }
    }

    fn resolve_result_coprocessor(
        &self,
        command: &Command,
    ) -> Box<
        dyn FnOnce(
                Result<tonic::Response<Bytes>, CommandError>,
            ) -> BoxFuture<'static, Result<tonic::Response<Bytes>, CommandError>>
            + Send,
    > {
        match command {
            Command::Register(_) => {
                let sessions = self.sessions.clone();
                Box::new(move |result| {
                    Box::pin(async move {
                        if let Ok(response) = result.as_ref() {
                            let client_id =
                                RepcClientId::from(response.get_ref().clone().get_u64());
                            sessions.register(client_id).await;
                        }
                        result
                    })
                })
            }
            Command::Action(_) => Box::new(|result| Box::pin(async { result })),
        }
    }

    pub fn extract_state(&mut self) -> State<S> {
        Arc::try_unwrap(self.state.take().unwrap())
            .ok()
            .expect("should have")
            .into_inner()
    }
}
