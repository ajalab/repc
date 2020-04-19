mod candidate;
mod deadline_clock;
mod error;
mod follower;
mod leader;
mod log;
mod message;
mod node;
mod peer;
mod rpc;
mod state;
mod types;
mod grpc {
    tonic::include_proto!("raft");
}

pub use node::Cluster;
pub use rpc::GRPCNode;
