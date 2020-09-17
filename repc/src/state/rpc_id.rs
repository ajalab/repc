#[derive(Clone)]
pub struct RpcId(String);

impl<T: ToString> From<T> for RpcId {
    fn from(id: T) -> Self {
        RpcId(id.to_string())
    }
}

impl From<RpcId> for String {
    fn from(id: RpcId) -> Self {
        id.0
    }
}

impl AsRef<str> for RpcId {
    fn as_ref(&self) -> &str {
        self.0.as_ref()
    }
}
