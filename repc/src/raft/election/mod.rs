mod writer;

use crate::types::{NodeId, Term};

#[derive(PartialEq, Eq, Default, Clone)]
pub struct Election {
    pub term: Term,
    pub voted_for: Option<NodeId>,
}
