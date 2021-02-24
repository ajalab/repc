pub mod configuration;

use repc::{
    state::State,
    test_util::partitioned::group::{PartitionedLocalRepcGroup, PartitionedLocalRepcGroupBuilder},
};
use tracing_subscriber::prelude::*;
use tracing_subscriber::{fmt::format::DefaultFields, EnvFilter};

pub fn init() {
    let formatter = DefaultFields::new().delimited(",");
    let _ = tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .fmt_fields(formatter)
        .try_init();
}

pub fn partitioned_group<S, L>(n: usize) -> PartitionedLocalRepcGroup<S, L>
where
    S: Clone + Default,
    L: Clone + Default,
{
    let confs = vec![configuration::never_election_timeout(); n];
    let states = vec![State::default(); n];

    PartitionedLocalRepcGroupBuilder::new()
        .confs(confs)
        .states(states)
        .build()
}
