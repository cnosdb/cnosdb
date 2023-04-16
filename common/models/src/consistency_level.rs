use std::fmt::Display;

#[allow(dead_code)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConsistencyLevel {
    /// allows for hinted handoff, potentially no write happened yet.
    Any,
    /// at least one data node acknowledged a write or read.
    One,
    /// a quorum of data nodes to acknowledge a write or read.
    Quorum,
    /// requires all data nodes to acknowledge a write or read.
    All,
}

impl From<&str> for ConsistencyLevel {
    fn from(value: &str) -> Self {
        match value.to_uppercase().as_str() {
            "ANY" => Self::Any,
            "ONE" => Self::One,
            "QUORUM" => Self::Quorum,
            "ALL" => Self::All,
            _ => Self::All,
        }
    }
}

impl From<u8> for ConsistencyLevel {
    fn from(value: u8) -> Self {
        match value {
            0 => Self::Any,
            1 => Self::One,
            2 => Self::Quorum,
            3 => Self::All,
            _ => Self::All,
        }
    }
}

impl Display for ConsistencyLevel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Any => write!(f, "ANY"),
            Self::One => write!(f, "ONE"),
            Self::Quorum => write!(f, "QUORUM"),
            Self::All => write!(f, "ALL"),
        }
    }
}
