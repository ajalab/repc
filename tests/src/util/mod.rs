pub mod configuration;

use tracing_subscriber::prelude::*;
use tracing_subscriber::{fmt::format::DefaultFields, EnvFilter};

pub fn init() {
    let formatter = DefaultFields::new().delimited(",");
    let _ = tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .fmt_fields(formatter)
        .try_init();
}
