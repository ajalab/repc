use bytes::Bytes;
#[derive(Clone)]
pub struct Command {
    path: String,
    body: Bytes,
}

impl Command {
    pub fn new(path: String, body: Bytes) -> Self {
        Command { path, body }
    }

    pub fn path(&self) -> &String {
        &self.path
    }

    pub fn body(&self) -> &Bytes {
        &self.body
    }
}
