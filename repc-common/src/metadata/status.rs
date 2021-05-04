use super::{
    decode_ascii_option_with, error::MetadataDecodeError, key::METADATA_REPC_RETRY, parse_from_str,
};
use crate::types::NodeId;
use tonic::metadata::{MetadataMap, MetadataValue};

pub struct StatusMetadata {
    /// Represents how a client can retry the RPC call.
    /// - `None`: client cannot retry automatically.
    /// - `Some(None)`: client can retry with artbirary nodes.
    /// - `Some(Some(id))`: client can retry with the given node.
    pub retry: Option<Option<NodeId>>,
}

impl StatusMetadata {
    pub fn encode(self, metadata: &mut MetadataMap) {
        if let Some(retry) = self.retry {
            let value = match retry {
                Some(id) => MetadataValue::from(id),
                None => MetadataValue::from_static(""),
            };
            metadata.insert(METADATA_REPC_RETRY, value);
        }
    }

    pub fn decode(metadata: &MetadataMap) -> Result<Self, MetadataDecodeError> {
        let retry = Self::decode_retry(metadata)?;

        Ok(Self { retry })
    }

    pub fn decode_retry(
        metadata: &MetadataMap,
    ) -> Result<Option<Option<NodeId>>, MetadataDecodeError> {
        decode_ascii_option_with(metadata, METADATA_REPC_RETRY, |v| {
            if v.is_empty() {
                Ok(None)
            } else {
                parse_from_str(v).map(Some)
            }
        })
    }
}
