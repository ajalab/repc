pub mod error;
mod key;
pub mod request;
pub mod status;

use std::{fmt::Display, str::FromStr};

use self::error::MetadataDecodeError;
use tonic::metadata::{AsciiMetadataValue, MetadataMap};

fn parse_from_str<F>(value: &AsciiMetadataValue) -> Result<F, String>
where
    F: FromStr,
    <F as FromStr>::Err: Display,
{
    value
        .to_str()
        .map_err(|e| e.to_string())
        .and_then(|v| v.parse::<F>().map_err(|e| e.to_string()))
}

fn decode_ascii<F>(metadata: &MetadataMap, key: &'static str) -> Result<F, MetadataDecodeError>
where
    F: FromStr,
    <F as FromStr>::Err: Display,
{
    decode_ascii_option_with(metadata, key, parse_from_str)
        .and_then(|v| v.ok_or_else(|| MetadataDecodeError::NotExist(key)))
}

fn decode_ascii_option_with<P, T, E>(
    metadata: &MetadataMap,
    key: &'static str,
    parser: P,
) -> Result<Option<T>, MetadataDecodeError>
where
    P: FnOnce(&AsciiMetadataValue) -> Result<T, E>,
    E: Display,
{
    let value = match metadata.get(key) {
        Some(value) => value,
        None => return Ok(None),
    };

    parser(value)
        .map_err(|e| MetadataDecodeError::Invalid {
            key,
            error: e.to_string(),
        })
        .map(Some)
}
