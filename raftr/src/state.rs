use crate::configuration::Configuration;
use crate::log::Log;
use crate::types::{NodeId, Term};

pub struct State {
    id: NodeId,
    conf: Configuration,
    term: Term,
    log: Log,
}

impl State {
    pub fn new(id: NodeId, conf: Configuration) -> Self {
        Self {
            id,
            conf,
            term: 1,
            log: Log::default(),
        }
    }

    pub fn id(&self) -> NodeId {
        self.id
    }

    pub fn conf(&self) -> &Configuration {
        &self.conf
    }

    pub fn term(&self) -> Term {
        self.term
    }
}
