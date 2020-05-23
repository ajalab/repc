use crate::deadline_clock::DeadlineClock;
use crate::message::Message;
use crate::peer::Peer;
use crate::state::State;

use rand::Rng;
use std::error;
use tokio::sync::mpsc;

pub struct Follower<P: Peer> {
    state: State<P>,
    deadline_clock: DeadlineClock,
}

impl<P: Peer + Send + Sync> Follower<P> {
    pub fn spawn(state: State<P>, mut tx: mpsc::Sender<Message>) -> Self {
        let id = state.id();
        let term = state.term();
        let conf = state.conf();

        let mut rng = rand::thread_rng();
        let timeout_millis: u64 = conf.follower.election_timeout_millis
            + rng.gen_range(0, conf.follower.election_timeout_jitter_millis + 1);

        let deadline_clock = DeadlineClock::spawn(timeout_millis, async move {
            if let Err(e) = tx.send(Message::ElectionTimeout).await {
                log::warn!(
                    "id={}, term={}, state={}, message=\"{}: {}\"",
                    id,
                    term,
                    "follower",
                    "failed to send message ElectionTimeout",
                    e
                );
            }
        });

        Follower {
            state,
            deadline_clock,
        }
    }

    pub async fn reset_deadline(&mut self) -> Result<(), impl error::Error> {
        self.deadline_clock.reset_deadline().await
    }

    pub fn state_mut(&mut self) -> &mut State<P> {
        &mut self.state
    }

    pub fn into_state(self) -> State<P> {
        self.state
    }
}
