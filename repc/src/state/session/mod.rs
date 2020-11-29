pub mod error;

use self::error::SessionError;
use std::collections::HashMap;
use std::fmt;

#[derive(Copy, Clone, Ord, Hash, PartialOrd, Eq, PartialEq)]
pub struct RepcClientId(u64);

impl Default for RepcClientId {
    fn default() -> Self {
        RepcClientId(u64::MAX)
    }
}

impl From<u64> for RepcClientId {
    fn from(id: u64) -> Self {
        RepcClientId(id)
    }
}

#[derive(Copy, Clone, Default, Debug)]
pub struct Sequence(u64);

impl Sequence {
    fn succ(&self) -> Sequence {
        Sequence(self.0 + 1)
    }

    fn is_succ(&self, sequence: Sequence) -> bool {
        self.0 == sequence.0 + 1
    }

    fn is_stale(&self, sequence: Sequence) -> bool {
        self.0 <= sequence.0
    }
}

impl From<u64> for Sequence {
    fn from(sequence: u64) -> Self {
        Sequence(sequence)
    }
}

impl fmt::Display for Sequence {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

#[derive(Default)]
pub struct Sessions {
    sessions: HashMap<RepcClientId, Sequence>,
}

impl Sessions {
    pub fn register(&mut self, client_id: RepcClientId) {
        self.sessions.insert(client_id, Sequence::default());
    }

    pub fn verify(
        &self,
        client_id: RepcClientId,
        sequence: Sequence,
    ) -> Result<bool, SessionError> {
        let seq = self
            .sessions
            .get(&client_id)
            .ok_or_else(|| SessionError::ClientNotRegistered)?;

        if sequence.is_succ(sequence) {
            Ok(true)
        } else if sequence.is_stale(sequence) {
            Ok(false)
        } else {
            Err(SessionError::SessionInvalid {
                expected: seq.succ(),
                actual: sequence,
            })
        }
    }
}
