use super::deadline_clock::DeadlineClock;
use crate::configuration::Configuration;
use crate::pb::raft::{
    AppendEntriesRequest, AppendEntriesResponse, RequestVoteRequest, RequestVoteResponse,
};
use crate::raft::message::Message;
use crate::state::{log::Log, State, StateMachine};
use crate::types::{NodeId, Term};
use rand::Rng;
use std::error;
use std::sync::Arc;
use tokio::sync::mpsc;

pub struct Follower<S, L> {
    deadline_clock: DeadlineClock,
    inner: InnerFollower<S, L>,
}

impl<S, L> Follower<S, L>
where
    S: StateMachine,
    L: Log,
{
    pub fn spawn(
        conf: Arc<Configuration>,
        term: Term,
        state: State<S, L>,
        tx: mpsc::Sender<Message>,
    ) -> Self {
        let mut rng = rand::thread_rng();
        let timeout_millis: u64 = conf.follower.election_timeout_millis
            + rng.gen_range(0..=conf.follower.election_timeout_jitter_millis);

        let deadline_clock = DeadlineClock::spawn(timeout_millis, async move {
            if let Err(_) = tx.send(Message::ElectionTimeout).await {
                tracing::warn!("failed to send message ElectionTimeout",);
            }
        });

        Follower {
            deadline_clock,
            inner: InnerFollower::new(term, state),
        }
    }

    pub async fn handle_request_vote_request(
        &mut self,
        req: RequestVoteRequest,
    ) -> Result<RequestVoteResponse, Box<dyn error::Error + Send>> {
        self.inner.handle_request_vote_request(req).await
    }

    pub async fn handle_append_entries_request(
        &mut self,
        req: AppendEntriesRequest,
    ) -> Result<AppendEntriesResponse, Box<dyn error::Error + Send>> {
        let res = self.inner.handle_append_entries_request(req).await;

        if let Err(e) = self.deadline_clock.reset_deadline().await {
            tracing::warn!("failed to reset deadline: {}", e);
        }

        res
    }

    pub fn extract_state(&mut self) -> State<S, L> {
        self.inner.extract_state()
    }
}

struct InnerFollower<S, L> {
    term: Term,
    voted_for: Option<NodeId>,
    state: Option<State<S, L>>,
}

impl<S, L> InnerFollower<S, L>
where
    S: StateMachine,
    L: Log,
{
    fn new(term: Term, state: State<S, L>) -> Self {
        Self {
            term,
            voted_for: None,
            state: Some(state),
        }
    }

    async fn handle_request_vote_request(
        &mut self,
        req: RequestVoteRequest,
    ) -> Result<RequestVoteResponse, Box<dyn error::Error + Send>> {
        // The following invariant holds:
        //   req.term <= self.term
        // because the node must have updated its term

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
            tracing::debug!("granted vote");
        } else if !valid_term {
            tracing::debug!("refused vote because the request has invalid term");
        } else if !valid_candidate {
            tracing::debug!(
                "refused vote because we have voted to another: {:?}",
                self.voted_for,
            );
        } else {
            tracing::debug!(
                "refused vote because the request has outdated last log term & index: ({}, {})",
                req.last_log_term,
                req.last_log_index,
            );
        }

        Ok(RequestVoteResponse {
            term: self.term,
            vote_granted,
        })
    }

    async fn handle_append_entries_request(
        &mut self,
        req: AppendEntriesRequest,
    ) -> Result<AppendEntriesResponse, Box<dyn error::Error + Send>> {
        // The following invariant holds:
        //   req.term <= self.term
        // because the node must have updated its term

        if req.term != self.term {
            tracing::debug!(
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

        //
        //                   prev_log_index
        //                   v
        //                ---=--------
        // follower log    s s t t t t
        //                ---=--------
        // req entries       | t t u u
        //                ---=--------
        // leader log      s s t t u u
        //                ---=--------
        // where s, t, u: term, s <= t < u
        let state = self.state.as_mut().unwrap();
        if req.prev_log_index > 0 {
            let prev_log_entry = state.log().get(req.prev_log_index);
            let prev_log_term = prev_log_entry.map(|e| e.term);

            if prev_log_term != Some(req.prev_log_term) {
                tracing::debug!(
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
            let term = state.log().get(index).map(|e| e.term);
            match term {
                Some(term) => {
                    if term != e.term {
                        // Truncate the log entries at index.
                        //
                        //                   prev  index                       index
                        //                   v     v                           v
                        //                ---------=--                ---------=
                        // follower log    s s t t t t                 s s t t |
                        //                ---------=-- => truncate => ---------=--
                        // req entries       | t t u u                   | t t u u
                        //                   +-----=--                   +-----=--
                        //                         ^
                        //                         i
                        // where s, t, u: term, s <= t < u
                        state.truncate_log(index);
                        break;
                    }
                }
                None => {
                    // Reached the end of the log.
                    break;
                }
            }
            // Entries match.
            i += 1;
        }
        state.append_log_entries(req.entries.into_iter().skip(i as usize));

        // commit log
        state.commit(req.last_committed_index);

        Ok(AppendEntriesResponse {
            term: self.term,
            success: true,
        })
    }

    fn extract_state(&mut self) -> State<S, L> {
        self.state.take().unwrap()
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::pb::raft::LogEntry;
    use crate::state::{log::in_memory::InMemoryLog, state_machine::test::NoopStateMachine, State};

    fn make_follower(
        term: Term,
        log: impl Into<InMemoryLog>,
    ) -> InnerFollower<NoopStateMachine, InMemoryLog> {
        let state_machine = NoopStateMachine {};
        let state = State::new(state_machine, log.into());
        InnerFollower::new(term, state)
    }

    fn log_entry(term: Term) -> LogEntry {
        LogEntry {
            term,
            command: None,
        }
    }

    #[tokio::test]
    async fn refuse_request_vote_invalid_term() {
        let term = 10;
        let log = vec![];
        let mut follower = make_follower(term, log);

        let candidate_id = 2;
        let req = RequestVoteRequest {
            term: term + 1,
            candidate_id,
            last_log_index: 1,
            last_log_term: 1,
        };
        let res = follower
            .handle_request_vote_request(req)
            .await
            .expect("should be ok");

        assert!(!res.vote_granted);
    }

    #[tokio::test]
    async fn refuse_request_vote_already_voted_to_another() {
        let term = 10;
        let log = vec![];
        let mut follower = make_follower(term, log);

        let candidate_id = 2;
        follower.voted_for = Some(candidate_id);

        let req = RequestVoteRequest {
            term,
            candidate_id: candidate_id + 1,
            last_log_index: 1,
            last_log_term: 1,
        };
        let res = follower
            .handle_request_vote_request(req)
            .await
            .expect("should be ok");

        assert!(!res.vote_granted);
    }

    #[tokio::test]
    async fn refuse_request_vote_smaller_log_term() {
        let term = 10;
        let last_log_index = 1;
        let log = vec![log_entry(term); last_log_index as usize];
        let mut follower = make_follower(term, log);

        let candidate_id = 2;
        let req = RequestVoteRequest {
            term: term,
            candidate_id: candidate_id,
            last_log_index: last_log_index,
            last_log_term: term - 1,
        };
        let res = follower
            .handle_request_vote_request(req)
            .await
            .expect("should be ok");

        assert!(!res.vote_granted);
    }

    #[tokio::test]
    async fn refuse_request_vote_smaller_log_index() {
        let term = 10;
        let last_log_index = 2;
        let log = vec![log_entry(term); last_log_index as usize];
        let mut follower = make_follower(term, log);

        let candidate_id = 2;
        let req = RequestVoteRequest {
            term: term,
            candidate_id: candidate_id,
            last_log_index: last_log_index - 1,
            last_log_term: term,
        };
        let res = follower
            .handle_request_vote_request(req)
            .await
            .expect("should be ok");

        assert!(!res.vote_granted);
    }

    #[tokio::test]
    async fn accept_request_vote() {
        let term = 10;
        let last_log_index = 2;
        let log = vec![log_entry(term); last_log_index as usize];
        let mut follower = make_follower(term, log);

        let candidate_id = 2;
        let req = RequestVoteRequest {
            term: term,
            candidate_id: candidate_id,
            last_log_index,
            last_log_term: term,
        };
        let res = follower
            .handle_request_vote_request(req)
            .await
            .expect("should be ok");

        assert!(res.vote_granted);
    }
}
