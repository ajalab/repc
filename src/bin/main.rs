use clap::{App, Arg};
use env_logger;

use raftr::{Cluster, GRPCNode};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();

    let matches = App::new("raftr")
        .arg(Arg::with_name("id").index(1).required(true))
        .get_matches();

    let id: u32 = matches.value_of("id").unwrap().parse()?;
    let mut cluster = Cluster::default();
    cluster.add(1, "[::1]:50051");
    cluster.add(2, "[::1]:50052");
    cluster.add(3, "[::1]:50053");

    let node = GRPCNode::new(id, cluster);
    node.serve().await?;

    Ok(())
}
