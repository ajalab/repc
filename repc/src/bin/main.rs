/*
use bytes::Bytes;
use clap::{App, Arg};
use env_logger;
use repc::configuration::{Configuration, NodeConfiguration};
use repc::group::grpc::GrpcRepcGroup;
use repc::state_machine::{error::StateMachineError, StateMachine};
use std::net::Ipv6Addr;

struct Logger {}

impl StateMachine for Logger {
    fn apply<P: AsRef<str>>(
        &mut self,
        path: P,
        _command: Bytes,
    ) -> Result<Bytes, StateMachineError> {
        log::debug!("{}", path.as_ref());
        Ok(Bytes::new())
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

    let mut conf = Configuration::default();
    let ip = "::1".parse::<Ipv6Addr>()?;
    let nodes = &mut conf.group.nodes;
    nodes.insert(1, NodeConfiguration::new(ip, 50051, 60051));
    nodes.insert(2, NodeConfiguration::new(ip, 50052, 60052));
    nodes.insert(3, NodeConfiguration::new(ip, 50053, 60053));

    let group = GrpcRepcGroup::new(id, conf, logger);
    group.run().await?;

    Ok(())
}

*/

fn main() {}
