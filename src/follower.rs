use crate::configuration::Configuration;
use crate::deadline_clock::DeadlineClock;
use crate::message::Message;
use crate::state::State;
use crate::types::{NodeId, Term};

use rand::Rng;
use std::error;
use tokio::sync::mpsc;

pub struct Follower {
    deadline_clock: DeadlineClock,
}

impl Follower {
    pub fn spawn(
        id: NodeId,
        term: Term,
        mut tx: mpsc::Sender<Message>,
        conf: &Configuration,
    ) -> Self {
        let mut rng = rand::thread_rng();
        let timeout_millis: u64 = conf.follower.election_timeout_millis
            + rng.gen_range(0, conf.follower.election_timeout_jitter_millis + 1);
        let deadline_clock = DeadlineClock::spawn(timeout_millis, async move {
            if let Err(e) = tx.send(Message::ElectionTimeout).await {
                log::warn!(
                    "id={}, term={}, state={}, message=\"{}: {}\"",
                    id,
                    term,
                    State::FOLLOWER,
                    "failed to send message ElectionTimeout",
                    e
                );
            }
        });

        Follower { deadline_clock }
    }

    pub async fn reset_deadline(&mut self) -> Result<(), impl error::Error> {
        self.deadline_clock.reset_deadline().await
    }
}
