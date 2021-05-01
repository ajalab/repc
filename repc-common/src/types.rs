pub type NodeId = u32;

/// Type of sequence number in a session.
pub type Sequence = u64;

#[derive(Copy, Clone, Ord, Hash, PartialOrd, Eq, PartialEq, Debug)]
pub struct ClientId(u64);

impl Default for ClientId {
    fn default() -> Self {
        ClientId(u64::MAX)
    }
}

impl From<u64> for ClientId {
    fn from(id: u64) -> Self {
        ClientId(id)
    }
}

impl From<ClientId> for u64 {
    fn from(id: ClientId) -> Self {
        id.0
    }
}

impl ClientId {
    pub fn new(id: u64) -> Self {
        ClientId(id)
    }

    pub fn get(&self) -> u64 {
        self.0
    }
}
