use crate::configuration::Configuration;
use crate::log::Log;
use crate::peer::Peer;
use crate::types::{NodeId, Term};
use std::collections::HashMap;

pub struct State<P: Peer> {
    id: NodeId,
    conf: Configuration,
    term: Term,
    log: Log,
    peers: HashMap<NodeId, P>,
}

impl<P: Peer> State<P> {
    pub fn new(id: NodeId, conf: Configuration) -> Self {
        Self {
            id,
            conf,
            term: 1,
            log: Log::default(),
            peers: HashMap::new(),
        }
    }

    pub fn id(&self) -> NodeId {
        self.id
    }

    pub fn term(&self) -> Term {
        self.term
    }

    pub fn conf(&self) -> &Configuration {
        &self.conf
    }

    pub fn peers(&self) -> &HashMap<NodeId, P> {
        &self.peers
    }

    pub fn peers_mut(&self) -> &mut HashMap<NodeId, P> {
        &mut self.peers
    }
}
