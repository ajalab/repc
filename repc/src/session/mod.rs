pub mod error;

use self::error::SessionError;
use crate::raft::node::error::CommandError;
use crate::util;
use bytes::Bytes;
use std::collections::HashMap;
use std::fmt;
use tokio::sync::RwLock;
use tonic::Response;

#[derive(Copy, Clone, Ord, Hash, PartialOrd, Eq, PartialEq, Debug)]
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

impl From<RepcClientId> for u64 {
    fn from(id: RepcClientId) -> Self {
        id.0
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

struct Session {
    sequence: Sequence,
    response: Option<Result<Response<Bytes>, CommandError>>,
}

#[derive(Default)]
pub struct Sessions {
    sessions: RwLock<HashMap<RepcClientId, Session>>,
}

impl Sessions {
    pub async fn register(&self, client_id: RepcClientId) {
        let mut sessions = self.sessions.write().await;
        sessions.insert(
            client_id,
            Session {
                sequence: Sequence::default(),
                response: None,
            },
        );
    }

    pub async fn verify(
        &self,
        client_id: RepcClientId,
        sequence: Sequence,
    ) -> Result<Option<Result<Response<Bytes>, CommandError>>, SessionError> {
        let sessions = self.sessions.read().await;
        let session = sessions
            .get(&client_id)
            .ok_or_else(|| SessionError::ClientNotRegistered)?;

        if sequence.is_succ(session.sequence) {
            Ok(None)
        } else if sequence.is_stale(session.sequence) {
            match session.response.as_ref() {
                // TODO: Remove clone
                Some(response) => Ok(Some(match response {
                    Ok(res) => Ok(util::clone_response(res)),
                    Err(e) => Err(e.clone()),
                })),
                None => Err(SessionError::RequestTooStale),
            }
        } else {
            Err(SessionError::SessionInvalid {
                expected: session.sequence.succ(),
                actual: sequence,
            })
        }
    }

    pub async fn update(&self, client_id: RepcClientId, sequence: Sequence) {}
}
