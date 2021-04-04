use super::deadline_clock::DeadlineClock;
use crate::{
    configuration::Configuration,
    pb::raft::{raft_client::RaftClient, RequestVoteRequest, RequestVoteResponse},
    raft::message::Message,
    state::{log::Log, State, StateMachine},
    types::{NodeId, Term},
};
use rand::Rng;
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};
use tokio::sync::mpsc;
use tonic::{body::BoxBody, client::GrpcService, codegen::StdError};
use tracing::Instrument;

pub struct Candidate<S, L> {
    id: NodeId,
    term: Term,
    votes: HashSet<NodeId>,
    quorum: usize,
    state: Option<State<S, L>>,
    tx: mpsc::Sender<Message>,
    _deadline_clock: DeadlineClock,
}

impl<S, L> Candidate<S, L>
where
    S: StateMachine,
    L: Log,
{
    pub fn spawn(
        id: NodeId,
        conf: Arc<Configuration>,
        term: Term,
        quorum: usize,
        state: State<S, L>,
        tx: mpsc::Sender<Message>,
    ) -> Self {
        let mut rng = rand::thread_rng();
        let timeout_millis: u64 = conf.candidate.election_timeout_millis
            + rng.gen_range(0..=(conf.candidate.election_timeout_jitter_millis));

        let tx_dc = tx.clone();
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
                "ignored vote from {}, which belongs to the different term: {}",
                id,
                res.term,
            );
            return false;
        }

        if !res.vote_granted {
            tracing::debug!("vote requested to {} was refused", id);
            return false;
        }

        if !self.votes.insert(id) {
            tracing::debug!(
                "received vote from {}, which already granted my vote in the current term",
                id,
            );
            return false;
        }

        tracing::debug!("received valid vote from {}", id);

        if self.votes.len() > self.quorum {
            tracing::info!(
                "got a majority of votes from nodes {:?}. will become a leader",
                self.votes
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
            let tx = self.tx.clone();
            let term = self.term;
            let candidate_id = self.id;
            let span = tracing::debug_span!(target: "candidate", "send_request_vote_request", target_id = id);
            tokio::spawn(
                async move {
                    tracing::debug!("sending request vote request");
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
                            tracing::debug!("request vote request successful");
                            let r = tx
                                .send(Message::RPCRequestVoteResponse {
                                    res: res.into_inner(),
                                    id,
                                })
                                .await;
                            if let Err(e) = r {
                                tracing::error!("failed to send message to node: {}", e);
                            }
                        }
                        Err(e) => {
                            tracing::error!("sending request vote request failed: {}", e);
                        }
                    }
                }
                .instrument(span),
            );
        }
    }

    pub fn extract_state(&mut self) -> State<S, L> {
        self.state.take().unwrap()
    }
}
