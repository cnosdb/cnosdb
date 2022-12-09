use std::{collections::HashMap, fmt::format};

use arrow_flight::{
    sql::{ProstAnyExt, ProstMessageExt},
    FlightDescriptor, FlightEndpoint, Location, Ticket,
};
use datafusion::arrow::{
    array::ArrayRef,
    buffer::Buffer,
    datatypes::SchemaRef,
    ipc::{self, reader},
    record_batch::RecordBatch,
};
use http_protocol::header::{AUTHORIZATION, BASIC_PREFIX};
use models::auth::user::UserInfo;
use prost::Message;
use prost_types::Any;
use tonic::{Request, Status};

use crate::http::header::Header;

pub fn parse_user_info(
    request: &Request<FlightDescriptor>,
) -> std::result::Result<UserInfo, String> {
    let authorization = request
        .metadata()
        .get(AUTHORIZATION.to_string())
        .ok_or_else(|| "authorization field not present".to_string())?
        .to_str()
        .map_err(|_| "authorization not parsable".to_string())?;

    Header::with(None, authorization.to_string())
        .try_get_basic_auth()
        .map_err(|e| format!("authorization not parsable, error: {}", e))
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
            ticket: any_tkt.encode_to_vec(),
        }),
        location,
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
