mod candidate;
pub mod configuration;
mod deadline_clock;
mod error;
mod follower;
pub mod group;
mod leader;
mod log;
mod message;
mod node;
mod peer;
mod service;
mod state;
mod types;
mod pb {
    tonic::include_proto!("raft");
}
