pub mod app;

mod initial_election;
mod send_command;

pub fn init() {
    let _ = env_logger::builder().is_test(true).try_init();
}
