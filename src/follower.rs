use crate::deadline_clock::DeadlineClock;
use crate::message::Message;
use crate::state::State;
use crate::types::{NodeId, Term};

use tokio::sync::mpsc;

const FOLLOWER_ELECTION_TIMEOUT_MILLIS: u64 = 1000;

pub struct Follower {
    deadline_clock: DeadlineClock,
}

impl Follower {
    pub fn spawn(id: NodeId, term: Term, mut tx: mpsc::Sender<Message>) -> Self {
        let deadline_clock = DeadlineClock::spawn(FOLLOWER_ELECTION_TIMEOUT_MILLIS, async move {
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

    pub async fn reset_deadline(&mut self) -> Result<(), impl std::error::Error> {
        self.deadline_clock.reset_deadline().await
    }
}
