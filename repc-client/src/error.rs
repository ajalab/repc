use std::fmt;

#[derive(Debug)]
pub enum ToChannelError {
    HttpError(http::Error),
    TransportError(tonic::transport::Error),
}

impl fmt::Display for ToChannelError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ToChannelError::HttpError(e) => write!(f, "failed to parse: {}", e),
            ToChannelError::TransportError(e) => write!(f, "failed to connect: {}", e),
        }
    }
}

impl std::error::Error for ToChannelError {}
