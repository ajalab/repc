use crate::deadline_clock::DeadlineClock;
use crate::message::Message;
use crate::state::State;
use crate::types::{NodeId, Term};

use log::{info, warn};
use rand::Rng;
use tokio::sync::mpsc;

const CANDIDATE_ELECTION_TIMEOUT_MILLIS: u64 = 5000;
const CANDIDATE_ELECTION_TIMETOUT_JITTER_MILLIS: u64 = 100;

pub struct Candidate {
    deadline_clock: DeadlineClock,
}

impl Candidate {
    pub fn spawn(id: NodeId, term: Term, mut tx: mpsc::Sender<Message>) -> Self {
        let mut rng = rand::thread_rng();
        let timeout_millis: u64 = CANDIDATE_ELECTION_TIMEOUT_MILLIS
            + rng.gen_range(0, CANDIDATE_ELECTION_TIMETOUT_JITTER_MILLIS);

        let deadline_clock = DeadlineClock::spawn(timeout_millis, async move {
            if let Err(e) = tx.send(Message::ElectionTimeout).await {
                warn!(
                    "id={}, term={}, state={}, message=\"{}: {}\"",
                    id,
                    term,
                    State::CANDIDATE,
                    "failed to send message ElectionTimeout",
                    e
                );
            }
            info!(
                "id={}, term={}, state={}, message=\"{}\"",
                id,
                term,
                State::CANDIDATE,
                "start re-election"
            );
        });

        Candidate { deadline_clock }
    }

    pub async fn reset_deadline(&mut self) -> Result<(), impl std::error::Error> {
        self.deadline_clock.reset_deadline().await
    }
}
