pub mod app;

mod initial_election;
mod send_command;

use tracing_subscriber::fmt::format;
use tracing_subscriber::prelude::*;

pub fn init() {
    let formatter = format::DefaultFields::new().delimited(",");
    tracing_subscriber::fmt()
        .with_env_filter("trace")
        .fmt_fields(formatter)
        .init();
}
