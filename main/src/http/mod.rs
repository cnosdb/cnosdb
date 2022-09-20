use std::net::AddrParseError;

use snafu::Snafu;
use spi::server::ServerError;
use tokio::sync::mpsc::error::SendError;
use tokio::sync::oneshot::error::RecvError;

// use async_channel as channel;
use warp::reject;

pub mod http_service;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum Error {
    #[snafu(display("Failed to parse address. err: {}", source))]
    AddrParse { source: AddrParseError },

    #[snafu(display("Body oversize: {}", size))]
    BodyOversize { size: usize },

    #[snafu(display("Message is not valid UTF-8"))]
    NotUtf8,

    #[snafu(display("Error parsing message: {}", source))]
    ParseLineProtocol { source: line_protocol::Error },

    #[snafu(display("Error sending to channel receiver: {}", source))]
    ChannelSend { source: SendError<tskv::Task> },

    #[snafu(display("Error receiving from channel receiver: {}", source))]
    ChannelReceive { source: RecvError },

    // #[snafu(display("Error sending to channel receiver: {}", source))]
    // AsyncChanSend {
    //     source: channel::SendError<tskv::Task>,
    // },
    #[snafu(display("Error executiong query: {}", source))]
    Query { source: ServerError },

    #[snafu(display("Error from tskv: {}", source))]
    Tskv { source: tskv::Error },

    #[snafu(display("Invalid message: {}", reason))]
    Syntax { reason: String },
}

impl reject::Reject for Error {}
