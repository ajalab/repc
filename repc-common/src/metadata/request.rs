use super::{
    decode_ascii,
    error::MetadataDecodeError,
    key::{METADATA_REPC_CLIENT_ID_KEY, METADATA_REPC_SEQUENCE_KEY},
};
use crate::types::{ClientId, Sequence};
use tonic::metadata::{MetadataMap, MetadataValue};

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

    pub fn decode(metadata: &MetadataMap) -> Result<Self, MetadataDecodeError> {
        let client_id = Self::decode_client_id(metadata)?;
        let sequence = Self::decode_sequence(metadata)?;

        Ok(Self {
            client_id,
            sequence,
        })
    }

    pub fn decode_client_id(metadata: &MetadataMap) -> Result<ClientId, MetadataDecodeError> {
        let key = METADATA_REPC_CLIENT_ID_KEY;
        decode_ascii::<u64>(metadata, key).map(ClientId::from)
    }

    pub fn decode_sequence(metadata: &MetadataMap) -> Result<Sequence, MetadataDecodeError> {
        decode_ascii::<u64>(metadata, METADATA_REPC_SEQUENCE_KEY).map(Sequence::from)
    }
}
