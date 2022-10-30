pub const TIMESTAMP_CODEC: [&str; 4] = ["DEFAULT", "NULL", "DELTA", "QUANTILE"];
pub const BIGINT_CODEC: [&str; 4] = ["DEFAULT", "NULL", "DELTA", "QUANTILE"];
pub const UNSIGNED_BIGINT_CODEC: [&str; 4] = ["DEFAULT", "NULL", "DELTA", "QUANTILE"];
pub const DOUBLE_CODEC: [&str; 4] = ["DEFAULT", "NULL", "GORILLA", "QUANTILE"];
pub const STRING_CODEC: [&str; 7] = ["DEFAULT", "NULL", "GZIP", "BZIP", "ZSTD", "SNAPPY", "ZLIB"];
pub const BOOLEAN_CODEC: [&str; 3] = ["DEFAULT", "NULL", "BITPACK"];

#[repr(u8)]
#[derive(Copy, Clone, PartialEq, Eq, Debug)]
pub enum Encoding {
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

impl Default for Encoding {
    fn default() -> Self {
        Encoding::Default
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

pub fn codec_to_codec_name(codec: u8) -> String {
    match Encoding::from(codec) {
        Encoding::Default => "Default".to_string(),
        Encoding::Null => "Null".to_string(),
        Encoding::Delta => "Delta".to_string(),
        Encoding::Quantile => "Quantile".to_string(),
        Encoding::Gzip => "Gzip".to_string(),
        Encoding::Bzip => "Bzip".to_string(),
        Encoding::Gorilla => "Gorilla".to_string(),
        Encoding::Snappy => "Snappy".to_string(),
        Encoding::Zstd => "Zstd".to_string(),
        Encoding::Zlib => "Zlib".to_string(),
        Encoding::BitPack => "BitPack".to_string(),
        Encoding::Unknown => "Unknown".to_string(),
    }
}

pub fn codec_name_to_codec(codec_name: &str) -> u8 {
    match codec_name {
        "DEFAULT" => 0,
        "NULL" => 1,
        "DELTA" => 2,
        "QUANTILE" => 3,
        "GZIP" => 4,
        "BZIP" => 5,
        "GORILLA" => 6,
        "SNAPPY" => 7,
        "ZSTD" => 8,
        "ZLIB" => 9,
        "BITPACK" => 10,
        _ => 15,
    }
}
