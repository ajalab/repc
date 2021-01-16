use super::Session;
use repc_proto::{METADATA_REPC_CLIENT_ID_KEY, METADATA_REPC_SEQUENCE_KEY};
use tonic::metadata::{MetadataMap, MetadataValue};

pub trait MetadataEncoder {
    type Item;
    type Error: std::error::Error;

    fn encode(&self, item: &Self::Item, metadata: &mut MetadataMap) -> Result<(), Self::Error>;
}

pub struct SessionMetadataEncoder;

impl MetadataEncoder for SessionMetadataEncoder {
    type Item = Session;
    type Error = SessionMetadataEncoderError;

    fn encode(&self, item: &Session, metadata: &mut MetadataMap) -> Result<(), Self::Error> {
        metadata.insert(METADATA_REPC_CLIENT_ID_KEY, MetadataValue::from(item.id()));
        metadata.insert(
            METADATA_REPC_SEQUENCE_KEY,
            MetadataValue::from(item.sequence()),
        );
        Ok(())
    }
}

#[derive(Debug)]
pub enum SessionMetadataEncoderError {}

impl std::fmt::Display for SessionMetadataEncoderError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "failed to encode session into metadata")
    }
}

impl std::error::Error for SessionMetadataEncoderError {}
