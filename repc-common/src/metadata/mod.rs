pub mod error;
mod key;
pub mod request;
pub mod status;

use std::{fmt::Display, str::FromStr};

use self::error::MetadataDecodeError;
use tonic::metadata::MetadataMap;

fn decode_ascii_option<F: FromStr>(
    metadata: &MetadataMap,
    key: &'static str,
) -> Result<Option<F>, MetadataDecodeError>
where
    F: FromStr,
    <F as FromStr>::Err: Display,
{
    let value = match metadata.get(key) {
        Some(value) => value,
        None => return Ok(None),
    };

    value
        .to_str()
        .map_err(|e| MetadataDecodeError::Invalid {
            key,
            error: e.to_string(),
        })
        .and_then(|v| {
            v.parse::<F>().map_err(|e| MetadataDecodeError::Invalid {
                key,
                error: e.to_string(),
            })
        })
        .map(Some)
}

fn decode_ascii<F: FromStr>(
    metadata: &MetadataMap,
    key: &'static str,
) -> Result<F, MetadataDecodeError>
where
    F: FromStr,
    <F as FromStr>::Err: Display,
{
    decode_ascii_option(metadata, key)
        .and_then(|v| v.ok_or_else(|| MetadataDecodeError::NotExist(key)))
}
