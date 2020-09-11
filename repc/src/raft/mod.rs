pub(crate) mod node;
pub(crate) mod peer;
pub(crate) mod pb {
    tonic::include_proto!("raft");
}
mod deadline_clock;
pub(crate) mod message;
pub(crate) mod service;
