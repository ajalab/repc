use clap::{App, Arg};
use env_logger;

use std::collections::HashMap;

use raftr::configuration::Configuration;
use raftr::group::grpc::GRPCRaftGroup;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();

    let matches = App::new("raftr")
        .arg(Arg::with_name("id").index(1).required(true))
        .get_matches();

    let id: u32 = matches.value_of("id").unwrap().parse()?;
    let mut addrs = HashMap::new();
    addrs.insert(1, "[::1]:50051".to_owned());
    addrs.insert(2, "[::1]:50052".to_owned());
    addrs.insert(3, "[::1]:50053".to_owned());

    let conf = Configuration::default();
    let group = GRPCRaftGroup::new(id, conf, addrs);
    group.run().await?;

    Ok(())
}
