use super::RpcId;
use bytes::Bytes;
#[derive(Clone)]
pub struct Command {
    rpc: RpcId,
    body: Bytes,
}

impl Command {
    pub fn new(rpc: RpcId, body: Bytes) -> Self {
        Command { rpc, body }
    }

    pub fn rpc(&self) -> &RpcId {
        &self.rpc
    }

    pub fn body(&self) -> &Bytes {
        &self.body
    }
}
