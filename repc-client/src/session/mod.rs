use repc_common::repc::types::{ClientId, Sequence};

#[derive(Debug, Clone)]
pub struct Session {
    client_id: ClientId,
    sequence: Sequence,
}

impl Session {
    pub fn new<T: Into<ClientId>>(client_id: T) -> Self {
        Session {
            client_id: client_id.into(),
            sequence: 1,
        }
    }

    pub fn client_id(&self) -> ClientId {
        self.client_id
    }

    pub fn sequence(&self) -> Sequence {
        self.sequence
    }
}
