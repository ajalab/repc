# RePC

_RePC_ (Replicated gRPC) is a framework to augment your [Rust](https://www.rust-lang.org/) distributed applications with _replicated state_.

## Features

- **Fault tolerant**. State replication is controlled by [Raft](https://raft.github.io/) concensus algortihm.
- **Easy-to-use**. Built on top of [tonic](https://github.com/hyperium/tonic) and [Tokio](https://tokio.rs/) stack,
you can auto-generate state machine interface from your [Protocol Buffers](https://developers.google.com/protocol-buffers) service definition. The state machine interacts with clients via [gRPC](https://grpc.io/).

