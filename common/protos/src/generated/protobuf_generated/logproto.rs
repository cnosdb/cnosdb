#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PushRequest {
    #[prost(message, repeated, tag = "1")]
    pub streams: ::prost::alloc::vec::Vec<Stream>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Stream {
    #[prost(string, tag = "1")]
    pub labels: ::prost::alloc::string::String,
    #[prost(message, repeated, tag = "2")]
    pub entries: ::prost::alloc::vec::Vec<Entry>,
    /// hash contains the original hash of the stream.
    #[prost(uint64, tag = "3")]
    pub hash: u64,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Entry {
    #[prost(message, optional, tag = "1")]
    pub timestamp: ::core::option::Option<super::vector::Timestamp>,
    #[prost(string, tag = "2")]
    pub line: ::prost::alloc::string::String,
}
