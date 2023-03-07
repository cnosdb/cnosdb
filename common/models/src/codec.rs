use std::str::FromStr;

use serde::{Deserialize, Serialize};

pub const BIGINT_CODEC: [Encoding; 4] = [
    Encoding::Default,
    Encoding::Null,
    Encoding::Delta,
    Encoding::Quantile,
];
// Because timestamp, bigint, and unsigned bigint are all integers,
// so their compression algorithms are the same
pub const TIMESTAMP_CODEC: [Encoding; 4] = BIGINT_CODEC;
pub const UNSIGNED_BIGINT_CODEC: [Encoding; 4] = BIGINT_CODEC;

pub const DOUBLE_CODEC: [Encoding; 4] = [
    Encoding::Default,
    Encoding::Null,
    Encoding::Gorilla,
    Encoding::Quantile,
];

pub const STRING_CODEC: [Encoding; 7] = [
    Encoding::Default,
    Encoding::Null,
    Encoding::Gzip,
    Encoding::Bzip,
    Encoding::Zstd,
    Encoding::Snappy,
    Encoding::Zlib,
];

pub const BOOLEAN_CODEC: [Encoding; 3] = [Encoding::Default, Encoding::Null, Encoding::BitPack];

#[derive(Copy, Clone, PartialEq, Eq, Debug, Serialize, Deserialize, Hash, Default)]
pub enum Encoding {
    #[default]
    Default = 0,
    Null = 1,
    Delta = 2,
    Quantile = 3,
    Gzip = 4,
    Bzip = 5,
    Gorilla = 6,
    Snappy = 7,
    Zstd = 8,
    Zlib = 9,
    BitPack = 10,
    Unknown = 15,
}

impl Encoding {
    pub fn is_timestamp_encoding(&self) -> bool {
        TIMESTAMP_CODEC.contains(self)
    }

    pub fn is_bigint_encoding(&self) -> bool {
        BIGINT_CODEC.contains(self)
    }

    pub fn is_unsigned_encoding(&self) -> bool {
        UNSIGNED_BIGINT_CODEC.contains(self)
    }

    pub fn is_double_encoding(&self) -> bool {
        DOUBLE_CODEC.contains(self)
    }

    pub fn is_string_encoding(&self) -> bool {
        STRING_CODEC.contains(self)
    }

    pub fn is_bool_encoding(&self) -> bool {
        BOOLEAN_CODEC.contains(self)
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Encoding::Default => "DEFAULT",
            Encoding::Null => "NULL",
            Encoding::Delta => "DELTA",
            Encoding::Quantile => "QUANTILE",
            Encoding::Gzip => "GZIP",
            Encoding::Bzip => "BZIP",
            Encoding::Gorilla => "GORILLA",
            Encoding::Snappy => "SNAPPY",
            Encoding::Zstd => "ZSTD",
            Encoding::Zlib => "ZLIB",
            Encoding::BitPack => "BITPACK",
            Encoding::Unknown => "UNKNOWN",
        }
    }
}

impl FromStr for Encoding {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_uppercase().as_str() {
            "DEFAULT" => Ok(Self::Default),
            "NULL" => Ok(Self::Null),
            "DELTA" => Ok(Self::Delta),
            "QUANTILE" => Ok(Self::Quantile),
            "GZIP" => Ok(Self::Gzip),
            "BZIP" => Ok(Self::Bzip),
            "GORILLA" => Ok(Self::Gorilla),
            "SNAPPY" => Ok(Self::Snappy),
            "ZSTD" => Ok(Self::Zstd),
            "ZLIB" => Ok(Self::Zlib),
            "BITPACK" => Ok(Self::BitPack),
            _ => Err(s.to_string()),
        }
    }
}

impl From<u8> for Encoding {
    fn from(value: u8) -> Self {
        match value {
            0 => Encoding::Default,
            1 => Encoding::Null,
            2 => Encoding::Delta,
            3 => Encoding::Quantile,
            4 => Encoding::Gzip,
            5 => Encoding::Bzip,
            6 => Encoding::Gorilla,
            7 => Encoding::Snappy,
            8 => Encoding::Zstd,
            9 => Encoding::Zlib,
            10 => Encoding::BitPack,
            _ => Encoding::Unknown,
        }
    }
}
