use std::error;
use std::fmt;

#[derive(Debug, Clone)]
pub struct ConnectionError {
    addr: String,
}

impl ConnectionError {
    pub fn new(addr: String) -> Self {
        ConnectionError { addr }
    }
}

impl fmt::Display for ConnectionError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "retries exhausted for connecting to {}", self.addr)
    }
}

impl error::Error for ConnectionError {}

#[derive(Debug, Clone)]
pub struct PeerError {
    description: String,
}

impl PeerError {
    pub fn new(description: String) -> Self {
        PeerError { description }
    }
}

impl fmt::Display for PeerError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "peer error: {}", self.description)
    }
}

impl error::Error for PeerError {}
