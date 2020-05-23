use crate::deadline_clock::DeadlineClock;
use crate::message::Message;
use crate::peer::Peer;
use crate::state::State;

use log::{info, warn};
use rand::Rng;
use tokio::sync::mpsc;

pub struct Candidate<P: Peer> {
    state: State<P>,
    _deadline_clock: DeadlineClock,
}

impl<P: Peer + Send + Sync> Candidate<P> {
    pub fn spawn(state: State<P>, mut tx: mpsc::Sender<Message>) -> Self {
        let id = state.id();
        let term = state.term();
        let conf = state.conf();
        let mut rng = rand::thread_rng();
        let timeout_millis: u64 = conf.candidate.election_timeout_millis
            + rng.gen_range(0, conf.candidate.election_timeout_jitter_millis + 1);

        let deadline_clock = DeadlineClock::spawn(timeout_millis, async move {
            if let Err(e) = tx.send(Message::ElectionTimeout).await {
                warn!(
                    "id={}, term={}, state={}, message=\"{}: {}\"",
                    id, term, "candidate", "failed to send message ElectionTimeout", e
                );
            }
            info!(
                "id={}, term={}, state={}, message=\"{}\"",
                id, term, "candidate", "start re-election"
            );
        });

        Candidate {
            state,
            _deadline_clock: deadline_clock,
        }
    }
}
