pub mod error;

use self::error::SessionError;
use crate::{raft::node::error::CommandError, util};
use bytes::Bytes;
use repc_common::repc::types::{ClientId, Sequence};
use std::collections::HashMap;
use tokio::sync::RwLock;
use tonic::Response;

struct Session {
    sequence: Sequence,
    response: Option<Result<Response<Bytes>, CommandError>>,
}

#[derive(Default)]
pub struct Sessions {
    sessions: RwLock<HashMap<ClientId, Session>>,
}

impl Sessions {
    pub async fn register(&self, client_id: ClientId) {
        let mut sessions = self.sessions.write().await;
        sessions.insert(
            client_id,
            Session {
                sequence: Sequence::default(),
                response: None,
            },
        );
        tracing::debug!(client_id = u64::from(client_id), "registered a new client");
    }

    pub async fn verify(
        &self,
        client_id: ClientId,
        sequence: Sequence,
    ) -> Result<Option<Result<Response<Bytes>, CommandError>>, SessionError> {
        let sessions = self.sessions.read().await;
        let session = sessions
            .get(&client_id)
            .ok_or_else(|| SessionError::ClientNotRegistered)?;

        if sequence == session.sequence + 1 {
            tracing::trace!("verification succeeded");
            Ok(None)
        } else if sequence == session.sequence {
            match session.response.as_ref() {
                Some(response) => {
                    tracing::trace!("duplicated request");
                    // TODO: Remove clone
                    Ok(Some(match response {
                        Ok(res) => Ok(util::clone_response(res)),
                        Err(e) => Err(e.clone()),
                    }))
                }
                None => Err(SessionError::RequestTooStale),
            }
        } else {
            Err(SessionError::SessionInvalid {
                expected: session.sequence + 1,
                actual: sequence,
            })
        }
    }

    pub async fn _update(&self, _client_id: ClientId, _sequence: Sequence) {}
}
