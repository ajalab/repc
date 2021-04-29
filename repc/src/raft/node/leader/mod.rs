mod commit_manager;
pub mod error;
mod message;
mod replicator;

use self::{commit_manager::CommitManager, replicator::Replicator};
use super::error::{AppendEntriesError, CommandError, RequestVoteError};
use crate::{
    configuration::Configuration,
    log::Log,
    pb::raft::{
        log_entry::Command, raft_client::RaftClient, AppendEntriesRequest, AppendEntriesResponse,
        LogEntry, RequestVoteRequest, RequestVoteResponse,
    },
    session::Sessions,
    state::State,
    state_machine::StateMachine,
    types::{NodeId, Term},
};
use bytes::{Buf, Bytes};
use futures::{future::BoxFuture, FutureExt};
use repc_proto::repc::types::{ClientId, Sequence};
use std::{collections::HashMap, iter, sync::Arc};
use tokio::sync::{oneshot, RwLock};
use tonic::{body::BoxBody, client::GrpcService, codegen::StdError};
use tracing::{Instrument, Level};

pub struct Leader<S, L> {
    term: Term,
    replicators: Vec<Replicator>,
    commit_manager: CommitManager,
    state: Option<Arc<RwLock<State<S, L>>>>,
    sessions: Arc<Sessions>,
}

impl<S, L> Leader<S, L>
where
    S: StateMachine + Send + Sync + 'static,
    L: Log + Send + Sync + 'static,
{
    pub fn spawn<T>(
        id: NodeId,
        conf: Arc<Configuration>,
        term: Term,
        state: State<S, L>,
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

        let replicators = clients
            .iter()
            .map(|(&target_id, client)| {
                Replicator::spawn(
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
            replicators,
            commit_manager,
            state: Some(state),
            sessions,
        };

        leader
    }
}

impl<S, L> Leader<S, L>
where
    S: StateMachine,
    L: Log,
{
    pub async fn handle_request_vote_request(
        &self,
        _req: RequestVoteRequest,
    ) -> Result<RequestVoteResponse, RequestVoteError> {
        // The following invariant holds:
        //   req.term <= self.term
        // because the node must have updated its term

        // In this term candidate must have voted to itself, so refuse another vote
        tracing::debug!("refused vote because the leader has voted to itself");
        Ok(RequestVoteResponse {
            term: self.term.get(),
            vote_granted: false,
        })
    }

    pub async fn handle_append_entries_request(
        &self,
        req: AppendEntriesRequest,
    ) -> Result<AppendEntriesResponse, AppendEntriesError> {
        let term = self.term.get();
        debug_assert!(req.term <= term);

        if req.term == term {
            tracing::error!("received AppendEntries request from another leader in the same term; likely split brain");
        } else {
            tracing::debug!(
                reason = "request term is smaller",
                "refused AppendEntries request",
            );
        }
        Ok(AppendEntriesResponse {
            term: self.term.get(),
            success: false,
        })
    }

    pub async fn handle_command(
        &mut self,
        command: Command,
        client_id: ClientId,
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
                term: self.term.get(),
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

        for replicator in &mut self.replicators {
            let _ = replicator.try_notify();
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
                            let client_id = ClientId::from(response.get_ref().clone().get_u64());
                            sessions.register(client_id).await;
                        }
                        result
                    })
                })
            }
            Command::Action(_) => Box::new(|result| Box::pin(async { result })),
        }
    }

    pub fn extract_state(&mut self) -> State<S, L> {
        Arc::try_unwrap(self.state.take().unwrap())
            .ok()
            .expect("should have")
            .into_inner()
    }
}
