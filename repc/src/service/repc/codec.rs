use bytes::buf::Buf;
use bytes::{BufMut, Bytes};
use tonic::codec::{Codec, DecodeBuf, Decoder, EncodeBuf, Encoder};
use tonic::Status;

pub struct IdentDecoder;

impl Decoder for IdentDecoder {
    type Item = Bytes;
    type Error = Status;
    fn decode(&mut self, src: &mut DecodeBuf<'_>) -> Result<Option<Self::Item>, Self::Error> {
        let len = src.remaining();
        Ok(Some(src.copy_to_bytes(len)))
    }
}

pub struct IdentEncoder;

impl Encoder for IdentEncoder {
    type Item = Bytes;
    type Error = Status;
    fn encode(&mut self, item: Self::Item, dst: &mut EncodeBuf<'_>) -> Result<(), Self::Error> {
        dst.put(item);
        Ok(())
    }
}

#[derive(Default)]
pub struct IdentCodec;

impl Codec for IdentCodec {
    type Encode = bytes::Bytes;
    type Decode = bytes::Bytes;

    type Encoder = IdentEncoder;
    type Decoder = IdentDecoder;

    fn encoder(&mut self) -> Self::Encoder {
        IdentEncoder
    }

    fn decoder(&mut self) -> Self::Decoder {
        IdentDecoder
    }
}
