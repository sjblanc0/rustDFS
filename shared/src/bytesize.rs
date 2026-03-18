use serde::de::Error as SerdeError;
use serde::{Deserialize, Deserializer};
use std::result::Result as StdResult;

use crate::error::RustDFSError;
use crate::result::Result;

/**
 * A size in bytes, deserialized from a human-readable string.
 * Supports suffixes: B, KB, MB, GB (case-insensitive).
 *
 * Examples: "64KB", "2MB", "1024B", "1GB"
 */
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ByteSize(pub usize);

impl ByteSize {
    pub fn as_usize(&self) -> usize {
        self.0
    }
}

impl<'de> Deserialize<'de> for ByteSize {
    fn deserialize<D>(deserializer: D) -> StdResult<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        parse_byte_size(&s).map_err(SerdeError::custom)
    }
}

fn parse_byte_size(s: &str) -> Result<ByteSize> {
    let s = s.trim();

    let (num_part, suffix) = match s.find(|c: char| c.is_alphabetic()) {
        Some(i) => (&s[..i], s[i..].to_uppercase()),
        None => {
            return s
                .parse::<usize>()
                .map(ByteSize)
                .map_err(|_| err_invalid_byte_size(s));
        }
    };

    let num: usize = num_part
        .trim()
        .parse()
        .map_err(|_| err_invalid_num_byte_size(s))?;

    let multiplier = match suffix.as_str() {
        "B" => 1,
        "KB" => 1024,
        "MB" => 1024 * 1024,
        "GB" => 1024 * 1024 * 1024,
        _ => {
            let err = err_invalid_suffix_byte_size(&suffix);
            return Err(err);
        }
    };

    Ok(ByteSize(num * multiplier))
}

// Error helpers

fn err_invalid_byte_size(size_str: &str) -> RustDFSError {
    let str = format!("Invalid byte size: {}", size_str);
    RustDFSError::CustomError(str)
}

fn err_invalid_num_byte_size(size_str: &str) -> RustDFSError {
    let str = format!("Invalid number in byte size: {}", size_str);
    RustDFSError::CustomError(str)
}

fn err_invalid_suffix_byte_size(suffix: &str) -> RustDFSError {
    let str = format!(
        "Invalid suffix '{}' in byte size. Expected: B, KB, MB, GB.",
        suffix
    );
    RustDFSError::CustomError(str)
}
