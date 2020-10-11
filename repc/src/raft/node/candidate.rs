use crate::configuration::Configuration;
use crate::pb::raft::raft_client::RaftClient;
use crate::pb::raft::{RequestVoteRequest, RequestVoteResponse};
use crate::raft::deadline_clock::DeadlineClock;
use crate::raft::message::Message;
use crate::state::State;
use crate::state::StateMachine;
use crate::types::{NodeId, Term};
use rand::Rng;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::mpsc;
use tonic::body::BoxBody;
use tonic::client::GrpcService;
use tonic::codegen::StdError;

pub struct Candidate<S> {
    id: NodeId,
    term: Term,
    votes: HashSet<NodeId>,
    quorum: usize,
    state: Option<State<S>>,
    tx: mpsc::Sender<Message>,
    _deadline_clock: DeadlineClock,
}

impl<S> Candidate<S>
where
    S: StateMachine,
{
    pub fn spawn(
        id: NodeId,
        conf: Arc<Configuration>,
        term: Term,
        quorum: usize,
        state: State<S>,
        tx: mpsc::Sender<Message>,
    ) -> Self {
        let mut rng = rand::thread_rng();
        let timeout_millis: u64 = conf.candidate.election_timeout_millis
            + rng.gen_range(0, conf.candidate.election_timeout_jitter_millis + 1);

        let mut tx_dc = tx.clone();

        let deadline_clock = DeadlineClock::spawn(timeout_millis, async move {
            if let Err(_) = tx_dc.send(Message::ElectionTimeout).await {
                tracing::warn!(
                    id,
                    term,
                    state = "candidate",
                    "failed to send ElectionTimeout message"
                );
            }
            tracing::debug!(id, term, state = "candidate", "start re-election");
        });

        let mut votes = HashSet::new();
        votes.insert(id);

        Candidate {
            id,
            term,
            votes,
            quorum,
            state: Some(state),
            tx,
            _deadline_clock: deadline_clock,
        }
    }

    pub async fn handle_request_vote_response(
        &mut self,
        res: RequestVoteResponse,
        id: NodeId,
    ) -> bool {
        if self.term != res.term {
            tracing::debug!(
                id = self.id,
                term = self.term,
                target_id = id,
                "ignored vote from {}, which belongs to the different term: {}",
                id,
                res.term,
            );
            return false;
        }

        if !res.vote_granted {
            tracing::debug!(
                id = self.id,
                term = self.term,
                target_id = id,
                "vote requested to {} is refused",
                id,
            );
            return false;
        }

        if !self.votes.insert(id) {
            tracing::debug!(
                id = self.id,
                term = self.term,
                target_id = id,
                "received vote from {}, which already granted my vote in the current term",
                id,
            );
            return false;
        }

        tracing::debug!(
            id = self.id,
            term = self.term,
            target_id = id,
            "received vote from {}",
            id,
        );

        if self.votes.len() > self.quorum {
            tracing::info!(
                id = self.id,
                term = self.term,
                "get a majority of votes from {:?}",
                self.votes,
            );
            return true;
        }
        return false;
    }

    pub async fn handle_election_timeout<T>(&mut self, clients: &HashMap<NodeId, RaftClient<T>>)
    where
        T: GrpcService<BoxBody> + Clone + Send + Sync + 'static,
        T::Future: Send,
        <T::ResponseBody as http_body::Body>::Error: Into<StdError> + Send,
    {
        let log = self.state.as_ref().unwrap().log();
        let last_log_term = log.last_term();
        let last_log_index = log.last_index();
        for (&id, client) in clients.iter() {
            let mut client = client.clone();
            let mut tx = self.tx.clone();
            let term = self.term;
            let candidate_id = self.id;
            tokio::spawn(async move {
                tracing::debug!(
                    id = candidate_id,
                    term,
                    target_id = id,
                    "sending RequestVoteRequest to {}",
                    id,
                );
                let res = client
                    .request_vote(RequestVoteRequest {
                        term,
                        candidate_id,
                        last_log_index,
                        last_log_term,
                    })
                    .await;

                match res {
                    Ok(res) => {
                        let r = tx
                            .send(Message::RPCRequestVoteResponse {
                                res: res.into_inner(),
                                id,
                            })
                            .await;
                        if let Err(e) = r {
                            tracing::error!(
                                id = candidate_id,
                                term,
                                target_id = id,
                                "failed to send message: {}",
                                e
                            );
                        }
                    }
                    Err(e) => {
                        tracing::error!(
                            id = candidate_id,
                            term,
                            target_id = id,
                            "request vote rpc failed: {}",
                            e
                        );
                    }
                }
            });
        }
    }

    pub fn extract_state(&mut self) -> State<S> {
        self.state.take().unwrap()
    }
}
