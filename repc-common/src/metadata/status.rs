use super::{
    decode_ascii_option_with,
    error::MetadataDecodeError,
    key::{METADATA_REPC_REGISTER, METADATA_REPC_RETRY},
    parse_from_str,
};
use crate::types::NodeId;
use tonic::metadata::{MetadataMap, MetadataValue};

pub struct StatusMetadata {
    /// Represents how a client can retry the RPC call.
    /// - `None`: client cannot retry automatically.
    /// - `Some(None)`: client can retry with artbirary nodes.
    /// - `Some(Some(id))`: client can retry with the given node.
    pub retry: Option<Option<NodeId>>,

    /// Represents whether a client should register itself before sending a next request (including retry).
    pub register: bool,
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

        if self.register {
            metadata.insert(METADATA_REPC_REGISTER, MetadataValue::from_static(""));
        }
    }

    pub fn decode(metadata: &MetadataMap) -> Result<Self, MetadataDecodeError> {
        let retry = Self::decode_retry(metadata)?;
        let register = Self::decode_register(metadata)?;

        Ok(Self { retry, register })
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

    pub fn decode_register(metadata: &MetadataMap) -> Result<bool, MetadataDecodeError> {
        decode_ascii_option_with(metadata, METADATA_REPC_REGISTER, |_| {
            Ok::<_, MetadataDecodeError>(())
        })
        .map(|r| r.is_some())
    }
}
