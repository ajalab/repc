use std::{error, fmt};

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

impl error::Error for ConnectionError {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        None
    }
}

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

impl error::Error for PeerError {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        None
    }
}

#[derive(Debug, Clone)]
pub struct InvalidStateError {
    actual: &'static str,
    expected: &'static str,
}

impl InvalidStateError {
    pub fn new(actual: &'static str, expected: &'static str) -> Self {
        InvalidStateError { actual, expected }
    }
}

impl fmt::Display for InvalidStateError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "invalid state: expected to be {}, but currently {}",
            self.expected, self.actual,
        )
    }
}

impl error::Error for InvalidStateError {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        None
    }
}
