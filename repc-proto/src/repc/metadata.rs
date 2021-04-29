use super::types::{ClientId, Sequence};
use std::fmt;
use tonic::{
    metadata::{MetadataMap, MetadataValue},
    Status,
};

pub const METADATA_REPC_CLIENT_ID_KEY: &str = "repc-client-id";
pub const METADATA_REPC_SEQUENCE_KEY: &str = "repc-sequence";

pub struct RequestMetadata {
    pub client_id: ClientId,
    pub sequence: Sequence,
}

impl RequestMetadata {
    pub fn encode(self, metadata: &mut MetadataMap) {
        metadata.insert(
            METADATA_REPC_CLIENT_ID_KEY,
            MetadataValue::from(self.client_id.get()),
        );
        metadata.insert(
            METADATA_REPC_SEQUENCE_KEY,
            MetadataValue::from(self.sequence),
        );
    }

    pub fn decode(metadata: &MetadataMap) -> Result<Self, RequestMetadataDecodeError> {
        let client_id = Self::decode_client_id(metadata)?;
        let sequence = Self::decode_sequence(metadata)?;

        Ok(Self {
            client_id,
            sequence,
        })
    }

    pub fn decode_client_id(
        metadata: &MetadataMap,
    ) -> Result<ClientId, RequestMetadataDecodeError> {
        Self::decode_u64(metadata, METADATA_REPC_CLIENT_ID_KEY).map(ClientId::new)
    }

    pub fn decode_sequence(metadata: &MetadataMap) -> Result<Sequence, RequestMetadataDecodeError> {
        Self::decode_u64(metadata, METADATA_REPC_SEQUENCE_KEY)
    }

    fn decode_u64(
        metadata: &MetadataMap,
        key: &'static str,
    ) -> Result<u64, RequestMetadataDecodeError> {
        metadata
            .get(key)
            .ok_or_else(|| RequestMetadataDecodeError::NotExist(key))
            .and_then(|id| {
                id.to_str()
                    .map_err(|e| RequestMetadataDecodeError::Invalid { key, e: e.into() })
            })
            .and_then(|id| {
                id.parse::<u64>()
                    .map_err(|e| RequestMetadataDecodeError::Invalid { key, e: e.into() })
            })
    }
}

#[derive(Debug)]
pub enum RequestMetadataDecodeError {
    NotExist(&'static str),
    Invalid {
        key: &'static str,
        e: Box<dyn std::error::Error + Send + Sync + 'static>,
    },
}

impl fmt::Display for RequestMetadataDecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RequestMetadataDecodeError::NotExist(key) => {
                write!(f, "key does not exist: key={}", key)
            }
            RequestMetadataDecodeError::Invalid { key, e } => write!(
                f,
                "failed to parse metadata value: key={}, error={}",
                key, e
            ),
        }
    }
}

impl From<RequestMetadataDecodeError> for Status {
    fn from(e: RequestMetadataDecodeError) -> Self {
        Status::invalid_argument(e.to_string())
    }
}
