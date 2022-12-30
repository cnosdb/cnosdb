#[allow(dead_code)]
#[derive(Debug)]
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
