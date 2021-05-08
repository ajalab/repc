use std::fmt;
#[derive(Clone, Debug, PartialEq)]
pub enum MetadataDecodeError {
    NotExist(&'static str),
    Invalid { key: &'static str, error: String },
}

impl fmt::Display for MetadataDecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MetadataDecodeError::NotExist(key) => write!(f, "key does not exist: key={}", key),
            MetadataDecodeError::Invalid { key, error } => write!(
                f,
                "failed to parse metadata value: key={}, error={}",
                key, error
            ),
        }
    }
}

impl std::error::Error for MetadataDecodeError {}
