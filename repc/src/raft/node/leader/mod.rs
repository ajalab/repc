mod appender;
mod commit_manager;
mod message;

use self::appender::Appender;
use self::commit_manager::CommitManager;
use super::error::CommandError;
use crate::configuration::Configuration;
use crate::pb::raft::{log_entry::Command, raft_client::RaftClient, LogEntry};
use crate::session::{RepcClientId, Sequence, Sessions};
use crate::state::{State, StateMachine};
use crate::types::{NodeId, Term};
use bytes::{Buf, Bytes};
use futures::FutureExt;
use std::collections::HashMap;
use std::iter;
use std::sync::Arc;
use tokio::sync::{oneshot, RwLock};
use tonic::body::BoxBody;
use tonic::client::GrpcService;
use tonic::codegen::StdError;

pub struct Leader<S> {
    id: NodeId,
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

        let nodes = iter::once(id).chain(clients.keys().copied());
        let (commit_manager, commit_manager_notifier) =
            CommitManager::spawn(nodes, Arc::downgrade(&state));

        let appenders = clients
            .iter()
            .map(|(&target_id, client)| {
                tracing::debug!(
                    id,
                    term,
                    target_id,
                    "spawn a new appender for {}",
                    target_id
                );
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
            id,
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
        let sessions = self.sessions.clone();
        let is_register = match command {
            Command::Action(_) => match sessions.verify(client_id, sequence).await {
                Ok(Some(res)) => {
                    tx.send(res).unwrap();
                    return;
                }
                Err(e) => {
                    tx.send(Err(CommandError::SessionError(e))).unwrap();
                    return;
                }
                _ => false,
            },
            Command::Register(_) => true,
        };

        let index = {
            let mut state = self.state.as_ref().unwrap().write().await;
            let entry = LogEntry {
                term: self.term,
                command: Some(command),
            };
            state.append_log_entries(iter::once(entry));
            state.last_index()
        };

        tracing::trace!(
            id = self.id,
            term = self.term,
            "wrote a command at log index {}",
            index
        );

        for appender in &mut self.appenders {
            let _ = appender.try_notify();
        }

        let subscription = self.commit_manager.subscribe();
        tokio::spawn(
            subscription
                .wait_applied(index)
                .then(move |result| async move {
                    tracing::trace!("sending a result to the service");
                    if is_register {
                        if let Ok(response) = result.as_ref() {
                            let client_id =
                                RepcClientId::from(response.get_ref().clone().get_u64());
                            sessions.register(client_id).await;
                            tracing::debug!(
                                client_id = u64::from(client_id),
                                "registered a new client"
                            );
                        }
                    }
                    tx.send(result)
                }),
        );
    }

    pub fn extract_state(&mut self) -> State<S> {
        Arc::try_unwrap(self.state.take().unwrap())
            .ok()
            .expect("should have")
            .into_inner()
    }
}
