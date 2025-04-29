use std::collections::HashMap;
use std::fmt;

use arrow_flight::sql::{Any, ProstMessageExt};
use arrow_flight::{FlightDescriptor, FlightEndpoint, IpcMessage, Location, SchemaAsIpc, Ticket};
use base64::prelude::{Engine, BASE64_STANDARD};
use datafusion::arrow::array::ArrayRef;
use datafusion::arrow::buffer::Buffer;
use datafusion::arrow::datatypes::{Schema, SchemaRef};
use datafusion::arrow::error::ArrowError;
use datafusion::arrow::ipc::writer::IpcWriteOptions;
use datafusion::arrow::ipc::{self, reader};
use datafusion::arrow::record_batch::RecordBatch;
use http_protocol::header::AUTHORIZATION;
use prost::Message;
use tonic::metadata::{AsciiMetadataValue, MetadataMap};
use tonic::{Request, Status};

#[macro_export]
macro_rules! status_with_location {
    ($desc:expr, $err:expr) => {
        Status::internal(format!("{}: {} at {}:{}", $desc, $err, file!(), line!()))
    };
}

#[macro_export]
macro_rules! status {
    ($desc:expr, $err:expr) => {
        Status::internal(format!("{}: {}", $desc, $err))
    };
}

/// Helper method for retrieving a value from the Authorization header.
///
/// headers     The headers to inspect.
/// valuePrefix The prefix within the value portion of the header to extract away.
///
/// @return The header value.
pub fn get_value_from_auth_header(headers: &MetadataMap, value_prefix: &str) -> Option<String> {
    get_value_from_header(headers, AUTHORIZATION.as_str(), value_prefix)
}

pub fn get_value_from_header(
    headers: &MetadataMap,
    header: &str,
    value_prefix: &str,
) -> Option<String> {
    if let Some(val) = headers.get(header) {
        return val
            .to_str()
            .ok()
            .and_then(|v| v.strip_prefix(value_prefix))
            .map(|e| e.to_string());
    }

    None
}

/// Set basic authorization header
pub fn insert_basic_auth<N>(
    map: &mut MetadataMap,
    username: N,
    password: Option<&str>,
) -> std::result::Result<(), String>
where
    N: fmt::Display,
{
    let auth = match password {
        Some(password) => format!("{}:{}", username, password),
        None => format!("{}:", username),
    };

    let token = format!("Basic {}", BASE64_STANDARD.encode(auth));

    let token = AsciiMetadataValue::try_from(token).map_err(|e| e.to_string())?;

    map.insert(AUTHORIZATION.as_str(), token);

    Ok(())
}

/// Set bearer authentication header
pub fn insert_bearer_auth<T>(map: &mut MetadataMap, token: T) -> std::result::Result<(), String>
where
    T: fmt::Display,
{
    let token =
        AsciiMetadataValue::try_from(format!("Bearer {}", token)).map_err(|e| e.to_string())?;

    map.insert(AUTHORIZATION.as_str(), token);

    Ok(())
}

pub fn parse_authorization_header(
    request: &Request<FlightDescriptor>,
) -> std::result::Result<&str, String> {
    let authorization = request
        .metadata()
        .get(AUTHORIZATION.to_string())
        .ok_or_else(|| "authorization field not present".to_string())?
        .to_str()
        .map_err(|_| "authorization not parsable".to_string())?;

    Ok(authorization)
}

pub fn endpoint(
    ticket: impl ProstMessageExt,
    location_uris: &[&str],
) -> std::result::Result<FlightEndpoint, String> {
    let any_tkt = Any::pack(&ticket).map_err(|e| format!("maybe a bug, error: {}", e))?;

    let location = location_uris
        .iter()
        .map(|e| Location { uri: (*e).into() })
        .collect::<Vec<_>>();

    Ok(FlightEndpoint {
        ticket: Some(Ticket {
            ticket: any_tkt.encode_to_vec().into(),
        }),
        location,
        expiration_time: None,
        app_metadata: prost::bytes::Bytes::new(),
    })
}

pub fn record_batch_from_message(
    message: ipc::Message<'_>,
    data_body: &Buffer,
    schema_ref: SchemaRef,
    dictionaries_by_id: &HashMap<i64, ArrayRef>,
) -> Result<RecordBatch, Status> {
    let ipc_batch = message
        .header_as_record_batch()
        .ok_or_else(|| Status::internal("Could not parse message header as record batch"))?;

    let arrow_batch_result = reader::read_record_batch(
        data_body,
        ipc_batch,
        schema_ref,
        dictionaries_by_id,
        None,
        &message.version(),
    );

    arrow_batch_result
        .map_err(|e| Status::internal(format!("Could not convert to RecordBatch: {:?}", e)))
}

pub fn dictionary_from_message(
    message: ipc::Message<'_>,
    data_body: &Buffer,
    schema_ref: SchemaRef,
    dictionaries_by_id: &mut HashMap<i64, ArrayRef>,
) -> Result<(), Status> {
    let ipc_batch = message
        .header_as_dictionary_batch()
        .ok_or_else(|| Status::internal("Could not parse message header as dictionary batch"))?;

    let dictionary_batch_result = reader::read_dictionary(
        data_body,
        ipc_batch,
        &schema_ref,
        dictionaries_by_id,
        &message.version(),
    );
    dictionary_batch_result
        .map_err(|e| Status::internal(format!("Could not convert to Dictionary: {:?}", e)))
}

pub fn schema_to_ipc_message(schema: &Schema) -> Result<IpcMessage, ArrowError> {
    let options = IpcWriteOptions::default();
    SchemaAsIpc::new(schema, &options).try_into()
}
