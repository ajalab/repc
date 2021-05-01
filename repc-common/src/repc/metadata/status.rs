use super::{decode_ascii_option, error::MetadataDecodeError, key::METADATA_REPC_FALLBACK};
use crate::repc::types::NodeId;
use tonic::metadata::{MetadataMap, MetadataValue};

pub struct StatusMetadata {
    pub fallback: Option<NodeId>,
}

impl StatusMetadata {
    pub fn encode(self, metadata: &mut MetadataMap) {
        if let Some(fallback) = self.fallback {
            metadata.insert(METADATA_REPC_FALLBACK, MetadataValue::from(fallback));
        }
    }

    pub fn decode(metadata: &MetadataMap) -> Result<Self, MetadataDecodeError> {
        let fallback = Self::decode_fallback(metadata)?;

        Ok(Self { fallback })
    }

    pub fn decode_fallback(metadata: &MetadataMap) -> Result<Option<NodeId>, MetadataDecodeError> {
        decode_ascii_option(metadata, METADATA_REPC_FALLBACK)
    }
}
