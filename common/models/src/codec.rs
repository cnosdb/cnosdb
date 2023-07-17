use std::fmt::{Display, Formatter};
use std::hash::Hash;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

pub const BIGINT_CODEC: [Encoding; 5] = [
    Encoding::Default,
    Encoding::Null,
    Encoding::Delta,
    Encoding::Quantile,
    Encoding::SDT { deviation: 0.0 },
];
// Because timestamp, bigint, and unsigned bigint are all integers,
// so their compression algorithms are the same
pub const TIMESTAMP_CODEC: [Encoding; 5] = BIGINT_CODEC;
pub const UNSIGNED_BIGINT_CODEC: [Encoding; 5] = BIGINT_CODEC;

pub const DOUBLE_CODEC: [Encoding; 5] = [
    Encoding::Default,
    Encoding::Null,
    Encoding::Gorilla,
    Encoding::Quantile,
    Encoding::SDT { deviation: 0.0 },
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

#[derive(Copy, Clone, Debug, Serialize, Deserialize, Default)]
pub enum Encoding {
    #[default]
    Default,
    Null,
    Delta,
    Quantile,
    Gzip,
    Bzip,
    Gorilla,
    Snappy,
    Zstd,
    Zlib,
    BitPack,
    SDT {
        deviation: f64,
    },
    Unknown,
}

impl PartialEq<Self> for Encoding {
    fn eq(&self, other: &Self) -> bool {
        self.id() == other.id()
    }
}

impl Eq for Encoding {}

impl Hash for Encoding {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.id().hash(state);
    }
}

impl Display for Encoding {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Encoding::Default => write!(f, "DEFAULT"),
            Encoding::Null => write!(f, "NULL"),
            Encoding::Delta => write!(f, "DELTA"),
            Encoding::Quantile => write!(f, "QUANTILE"),
            Encoding::Gzip => write!(f, "GZIP"),
            Encoding::Bzip => write!(f, "BZIP"),
            Encoding::Gorilla => write!(f, "GORILLA"),
            Encoding::Snappy => write!(f, "SNAPPY"),
            Encoding::Zstd => write!(f, "ZSTD"),
            Encoding::Zlib => write!(f, "ZLIB"),
            Encoding::BitPack => write!(f, "BITPACK"),
            Encoding::SDT { deviation } => write!(f, "SDT(deviation = {})", deviation),
            Encoding::Unknown => write!(f, "UNKNOWN"),
        }
    }
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

    pub fn id(&self) -> u8 {
        match self {
            Encoding::Default => 0,
            Encoding::Null => 1,
            Encoding::Delta => 2,
            Encoding::Quantile => 3,
            Encoding::Gzip => 4,
            Encoding::Bzip => 5,
            Encoding::Gorilla => 6,
            Encoding::Snappy => 7,
            Encoding::Zstd => 8,
            Encoding::Zlib => 9,
            Encoding::BitPack => 10,
            Encoding::SDT { .. } => 11,
            Encoding::Unknown => 15,
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
            "SDT" => Ok(Self::SDT { deviation: 0.0 }),
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
            11 => Encoding::SDT { deviation: 0.0 },
            _ => Encoding::Unknown,
        }
    }
}
