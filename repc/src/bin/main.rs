use bytes::Bytes;
use clap::{App, Arg};
use env_logger;
use repc::configuration::Configuration;
use repc::group::grpc::GRPCRaftGroup;
use repc::StateMachine;
use std::collections::HashMap;

struct Logger {}

impl StateMachine for Logger {
    fn apply<P: AsRef<str>>(&mut self, path: P, command: Bytes) {
        log::debug!("{}", path.as_ref());
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();

    let logger = Logger {};
    let matches = App::new("repc")
        .arg(Arg::with_name("id").index(1).required(true))
        .get_matches();

    let id: u32 = matches.value_of("id").unwrap().parse()?;
    let mut addrs = HashMap::new();
    addrs.insert(1, "[::1]:50051".to_owned());
    addrs.insert(2, "[::1]:50052".to_owned());
    addrs.insert(3, "[::1]:50053".to_owned());

    let conf = Configuration::default();
    let group = GRPCRaftGroup::new(id, conf, addrs, logger);
    group.run().await?;

    Ok(())
}
