use super::deadline_clock::DeadlineClock;
use crate::configuration::Configuration;
use crate::pb::raft::LogEntry as PbLogEntry;
use crate::pb::raft::{
    AppendEntriesRequest, AppendEntriesResponse, RequestVoteRequest, RequestVoteResponse,
};
use crate::raft::message::Message;
use crate::state::log::LogEntry;
use crate::state::Command;
use crate::state::State;
use crate::state::StateMachine;
use crate::types::{NodeId, Term};
use rand::Rng;
use std::error;
use std::sync::Arc;
use tokio::sync::mpsc;

pub struct Follower<S> {
    id: NodeId,
    term: Term,
    deadline_clock: DeadlineClock,
    voted_for: Option<NodeId>,
    state: Option<State<S>>,
}

impl<S> Follower<S>
where
    S: StateMachine,
{
    pub fn spawn(
        id: NodeId,
        conf: Arc<Configuration>,
        term: Term,
        state: State<S>,
        mut tx: mpsc::Sender<Message>,
    ) -> Self {
        let mut rng = rand::thread_rng();
        let timeout_millis: u64 = conf.follower.election_timeout_millis
            + rng.gen_range(0, conf.follower.election_timeout_jitter_millis + 1);

        let deadline_clock = DeadlineClock::spawn(timeout_millis, async move {
            if let Err(_) = tx.send(Message::ElectionTimeout).await {
                tracing::warn!(
                    id,
                    term,
                    state = "follower",
                    "failed to send message ElectionTimeout",
                );
            }
        });

        Follower {
            id,
            term,
            voted_for: None,
            deadline_clock,
            state: Some(state),
        }
    }

    pub async fn reset_deadline(&mut self) -> Result<(), impl error::Error> {
        self.deadline_clock.reset_deadline().await
    }

    pub async fn handle_request_vote_request(
        &mut self,
        req: RequestVoteRequest,
    ) -> Result<RequestVoteResponse, Box<dyn error::Error + Send>> {
        // invariant: req.term <= self.term

        let valid_term = req.term == self.term;
        let valid_candidate = match self.voted_for {
            None => true,
            Some(id) => id == req.candidate_id,
        };

        let vote_granted = valid_term && valid_candidate && {
            let state = self.state.as_ref().unwrap();
            let log = state.log();
            let last_term = log.last_term();
            let last_index = log.last_index();
            (req.last_log_term, req.last_log_index) >= (last_term, last_index)
        };

        if vote_granted && self.voted_for == None {
            self.voted_for = Some(req.candidate_id);
        }

        if vote_granted {
            tracing::debug!(
                id = self.id,
                term = self.term,
                target_id = req.candidate_id,
                "granted vote from {}",
                req.candidate_id,
            );
        } else if !valid_term {
            tracing::debug!(
                id = self.id,
                term = self.term,
                target_id = req.candidate_id,
                "refused vote from {} because the request has invalid term: {}",
                req.candidate_id,
                req.term,
            );
        } else if !valid_candidate {
            tracing::debug!(
                id = self.id,
                term = self.term,
                target_id = req.candidate_id,
                "refused vote from {} because we have voted to another: {:?}",
                req.candidate_id,
                self.voted_for,
            );
        } else {
            tracing::debug!(
                id=self.id,
                term=self.term,
                target_id = req.candidate_id,
                "refused vote from {} because the request has outdated last log term & index: ({}, {})",
                req.candidate_id,
                req.last_log_term, req.last_log_index,
            );
        }

        Ok(RequestVoteResponse {
            term: self.term,
            vote_granted,
        })
    }

    pub async fn handle_append_entries_request(
        &mut self,
        req: AppendEntriesRequest,
    ) -> Result<AppendEntriesResponse, Box<dyn error::Error + Send>> {
        // invariant:
        //   req.term <= self.term

        if req.term != self.term {
            tracing::debug!(
                id = self.id,
                term = self.term,
                target_id = req.leader_id,
                "refuse AppendEntry request from {}: invalid term {}",
                req.leader_id,
                req.term,
            );
            return Ok(AppendEntriesResponse {
                term: self.term,
                success: false,
            });
        }

        // invariant:
        //   req.term == self.term

        let state = self.state.as_mut().unwrap();
        if req.prev_log_index > 0 {
            let prev_log_entry = state.log().get(req.prev_log_index);
            let prev_log_term = prev_log_entry.map(|e| e.term());

            if prev_log_term != Some(req.prev_log_term) {
                tracing::debug!(
                    id = self.id,
                    term = self.term,
                    target_id = req.leader_id,
                    "refuse AppendEntry request from {}: previous log term of the request ({}) doesn't match the actual term ({})",
                    req.leader_id,
                    req.prev_log_term,
                    prev_log_term.unwrap_or(0),
                );
                return Ok(AppendEntriesResponse {
                    term: self.term,
                    success: false,
                });
            }
        }

        // append log
        let mut i = 0;
        for e in req.entries.iter() {
            let index = req.prev_log_index + 1 + i;
            let term = state.log().get(index).map(|e| e.term());
            match term {
                Some(term) => {
                    if term != e.term {
                        state.truncate_log(index);
                        break;
                    }
                }
                None => {
                    break;
                }
            }
            i += 1;
        }
        state.append_log_entries(
            req.entries
                .into_iter()
                .skip(i as usize)
                .map(|e: PbLogEntry| {
                    LogEntry::new(e.term, Command::new(e.rpc.into(), e.body.into()))
                }),
        );
        tracing::trace!(
            id = self.id,
            term = self.term,
            target_id = req.leader_id,
            "append entries replicated from {}",
            req.leader_id,
        );

        // commit log
        let last_committed = state.commit(req.last_committed_index);
        tracing::trace!(
            id = self.id,
            term = self.term,
            target_id = req.leader_id,
            "commit log at index {}",
            last_committed,
        );

        if let Err(e) = self.reset_deadline().await {
            tracing::warn!(
                id = self.id,
                term = self.term,
                "failed to reset deadline: {}",
                e
            );
        };

        Ok(AppendEntriesResponse {
            term: self.term,
            success: true,
        })
    }

    pub fn extract_state(&mut self) -> State<S> {
        self.state.take().unwrap()
    }
}
