use repc_common::types::{ClientId, Sequence};

#[derive(Debug, Clone)]
pub struct Session {
    pub client_id: ClientId,
    pub sequence: Sequence,
}

impl Session {
    pub fn new<T: Into<ClientId>>(client_id: T) -> Self {
        Session {
            client_id: client_id.into(),
            sequence: 1,
        }
    }
}
