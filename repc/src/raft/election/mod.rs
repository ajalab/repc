mod writer;

use crate::types::Term;
use repc_common::repc::types::NodeId;

#[derive(PartialEq, Eq, Default, Clone)]
pub struct Election {
    pub term: Term,
    pub voted_for: Option<NodeId>,
}
