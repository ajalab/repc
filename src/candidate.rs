use crate::configuration::Configuration;
use crate::deadline_clock::DeadlineClock;
use crate::message::Message;
use crate::state::State;
use crate::types::{NodeId, Term};

use log::{info, warn};
use rand::Rng;
use tokio::sync::mpsc;

pub struct Candidate {
    deadline_clock: DeadlineClock,
}

impl Candidate {
    pub fn spawn(
        id: NodeId,
        term: Term,
        mut tx: mpsc::Sender<Message>,
        conf: &Configuration,
    ) -> Self {
        let mut rng = rand::thread_rng();
        let timeout_millis: u64 = conf.candidate.election_timeout_millis
            + rng.gen_range(0, conf.candidate.election_timeout_jitter_millis + 1);

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
}
