pub mod encoder;

#[derive(Debug, Clone)]
pub struct Session {
    id: u64,
    sequence: u64,
}

impl Session {
    pub fn new(id: u64) -> Self {
        Session { id, sequence: 1 }
    }

    pub fn id(&self) -> u64 {
        self.id
    }

    pub fn sequence(&self) -> u64 {
        self.sequence
    }
}
