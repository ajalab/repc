use crate::configuration::Configuration;
use crate::raft::deadline_clock::DeadlineClock;
use crate::raft::log::{Log, LogEntry};
use crate::raft::message::Message;
use crate::raft::pb;
use crate::state_machine::StateMachineManager;
use crate::types::{NodeId, Term};
use rand::Rng;
use std::cmp;
use std::error;
use std::fmt;
use std::sync::Arc;
use tokio::sync::mpsc;

#[derive(Debug)]
pub struct ReferenceError;

impl fmt::Display for ReferenceError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "reference error")
    }
}

impl error::Error for ReferenceError {}

pub struct Follower {
    id: NodeId,
    term: Term,
    deadline_clock: DeadlineClock,
    voted_for: Option<NodeId>,
    sm_manager: StateMachineManager,
    log: Option<Log>,
}

impl Follower {
    pub fn spawn(
        id: NodeId,
        conf: Arc<Configuration>,
        term: Term,
        log: Log,
        sm_manager: StateMachineManager,
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
            sm_manager,
            log: Some(log),
        }
    }

    pub async fn reset_deadline(&mut self) -> Result<(), impl error::Error> {
        self.deadline_clock.reset_deadline().await
    }

    pub async fn handle_request_vote_request(
        &mut self,
        req: pb::RequestVoteRequest,
    ) -> Result<pb::RequestVoteResponse, Box<dyn error::Error + Send>> {
        // invariant: req.term <= self.term

        let valid_term = req.term == self.term;
        let valid_candidate = match self.voted_for {
            None => true,
            Some(id) => id == req.candidate_id,
        };

        let vote_granted = valid_term && valid_candidate && {
            let log = self.log.as_ref().unwrap();
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

        Ok(pb::RequestVoteResponse {
            term: self.term,
            vote_granted,
        })
    }

    pub async fn handle_append_entries_request(
        &mut self,
        req: pb::AppendEntriesRequest,
    ) -> Result<pb::AppendEntriesResponse, Box<dyn error::Error + Send>> {
        // invariant:
        //   req.term <= self.term

        if req.term != self.term {
            return Ok(pb::AppendEntriesResponse {
                term: self.term,
                success: false,
            });
        }

        // invariant:
        //   req.term == self.term

        let log = self.log.as_mut().unwrap();
        if req.prev_log_index > 0 {
            let prev_log_entry = log.get(req.prev_log_index);
            let prev_log_term = prev_log_entry.map(|e| e.term());

            if prev_log_term != Some(req.prev_log_term) {
                return Ok(pb::AppendEntriesResponse {
                    term: self.term,
                    success: false,
                });
            }
        }

        // append log
        let mut i = 0;
        for e in req.entries.iter() {
            let index = req.prev_log_index + 1 + i;
            let term = log.get(index).map(|e| e.term());
            match term {
                Some(term) => {
                    if term != e.term {
                        log.truncate(index);
                        break;
                    }
                }
                None => {
                    break;
                }
            }
            i += 1;
        }
        log.append(
            req.entries
                .into_iter()
                .skip(i as usize)
                .map(|e: pb::LogEntry| LogEntry::new(e.term, e.command.into())),
        );

        // commit log
        let last_committed_index = log.last_committed();
        log.commit(cmp::min(req.last_committed_index, last_committed_index));

        if let Err(e) = self.reset_deadline().await {
            tracing::warn!(
                id = self.id,
                term = self.term,
                "failed to reset deadline: {}",
                e
            );
        };

        Ok(pb::AppendEntriesResponse {
            term: self.term,
            success: true,
        })
    }

    pub fn extract_log(&mut self) -> Log {
        self.log.take().unwrap()
    }
}
