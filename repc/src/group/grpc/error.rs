use std::fmt;

#[derive(Debug)]
pub enum GrpcRepcGroupError {
    HttpError(http::Error),
    TransportError(tonic::transport::Error),
}

impl fmt::Display for GrpcRepcGroupError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            GrpcRepcGroupError::HttpError(e) => write!(f, "failed to parse: {}", e),
            GrpcRepcGroupError::TransportError(e) => write!(f, "failed to connect: {}", e),
        }
    }
}

impl std::error::Error for GrpcRepcGroupError {}
