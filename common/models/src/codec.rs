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
