use arrow_flight::sql::{ActionCreatePreparedStatementResult, ProstAnyExt, SqlInfo};
use arrow_flight::{
    flight_service_server, Action, FlightData, FlightEndpoint, HandshakeRequest, HandshakeResponse,
    IpcMessage, SchemaAsIpc, Ticket,
};
use chrono::format::Item;
use datafusion::arrow::ipc::writer::{DictionaryTracker, IpcDataGenerator, IpcWriteOptions};
use futures::Stream;
use http_protocol::header::{AUTHORIZATION, BASIC_PREFIX};
use moka::sync::Cache;
use prost::Message;
use prost_types::Any;
use query::dispatcher::manager::SimpleQueryDispatcher;
use query::instance::Cnosdbms;
use spi::query::dispatcher::QueryDispatcher;
use spi::query::execution::Output;
use spi::server::dbms::{DBMSRef, DatabaseManagerSystem, DatabaseManagerSystemMock};
use spi::service::protocol::{Context, ContextBuilder, Query, QueryHandle, QueryId};
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use tonic::transport::Server;
use tonic::{Request, Response, Status, Streaming};
use trace::debug;

use arrow_flight::{
    flight_service_server::FlightService,
    flight_service_server::FlightServiceServer,
    sql::{
        server::FlightSqlService, ActionClosePreparedStatementRequest,
        ActionCreatePreparedStatementRequest, CommandGetCatalogs, CommandGetCrossReference,
        CommandGetDbSchemas, CommandGetExportedKeys, CommandGetImportedKeys, CommandGetPrimaryKeys,
        CommandGetSqlInfo, CommandGetTableTypes, CommandGetTables, CommandPreparedStatementQuery,
        CommandPreparedStatementUpdate, CommandStatementQuery, CommandStatementUpdate,
        TicketStatementQuery,
    },
    utils as flight_utils, FlightDescriptor, FlightInfo,
};

use crate::flight_sql::utils;
use crate::http::header::Header;

#[derive(Clone)]
pub struct FlightSqlServiceImpl {
    instance: DBMSRef,
    result_cache: Cache<Vec<u8>, Output>,
}

impl FlightSqlServiceImpl {
    pub fn new(instance: DBMSRef) -> Self {
        let result_cache = Cache::builder()
            // Time to live (TTL): 30 minutes
            .time_to_live(Duration::from_secs(10 * 60))
            // Create the cache.
            .build();

        Self {
            instance,
            result_cache,
        }
    }
}

#[tonic::async_trait]
impl FlightSqlService for FlightSqlServiceImpl {
    type FlightService = FlightSqlServiceImpl;

    /// TODO support
    async fn do_handshake(
        &self,
        request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<
        Response<Pin<Box<dyn Stream<Item = Result<HandshakeResponse, Status>> + Send>>>,
        Status,
    > {
        debug!("do_handshake: {:?}", request);

        let ori_authorization = request
            .metadata()
            .get(AUTHORIZATION.to_string())
            .ok_or(Status::invalid_argument("authorization field not present"))?;
        let authorization = ori_authorization
            .to_str()
            .map_err(|_| Status::invalid_argument("authorization not parsable"))?;

        let header = Header::with(None, authorization.to_string());

        let _user_info = header
            .try_get_basic_auth()
            .map_err(|e| Status::invalid_argument(e.to_string()))?;

        // TODO 校验用户密码
        // if user_info.user != "admin" || user_info.password != "password" {
        //     Err(Status::unauthenticated("Invalid credentials!"))?
        // }

        let output: Pin<Box<dyn Stream<Item = Result<HandshakeResponse, Status>> + Send>> =
            Box::pin(futures::stream::iter(vec![]));

        let mut resp = Response::new(output);

        resp.metadata_mut()
            .insert(AUTHORIZATION.as_str(), ori_authorization.clone());

        return Ok(resp);
    }

    /// TODO support
    async fn get_flight_info_statement(
        &self,
        query: CommandStatementQuery,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        debug!(
            "get_flight_info_statement: query: {:?}, request: {:?}",
            query, request
        );

        let user_info =
            utils::parse_user_info(&request).map_err(|e| Status::invalid_argument(e))?;
        // TODO parse tenant & default database
        let tenant = None;
        let database = None;
        let ctx = ContextBuilder::new(user_info)
            .with_tenant(tenant)
            .with_database(database)
            .build();
        let sql = query.query;
        let query = Query::new(ctx, sql);
        let querh_result = self.instance.execute(&query).await.map_err(|e| {
            // TODO convert error message
            Status::internal(format!("{}", e))
        })?;

        let statement_handle: Vec<u8> = querh_result.id().into();
        let output = querh_result.result();
        let schema = output.schema();
        let total_records = output.num_rows();
        // cache result wait cli fetching
        self.result_cache.insert(statement_handle.clone(), output);

        // construct response start
        let option = IpcWriteOptions::default();
        let ipc_message = SchemaAsIpc::new(&schema, &option)
            .try_into()
            .map_err(|e| Status::internal(format!("{}", e)))?;
        let flight_descriptor = request.into_inner();
        let tkt = TicketStatementQuery { statement_handle };
        let endpoint = utils::endpoint(tkt, Default::default()).map_err(Status::internal)?;

        let flight_info = FlightInfo::new(
            ipc_message,
            Some(flight_descriptor),
            vec![endpoint],
            total_records as i64,
            -1,
        );

        Ok(Response::new(flight_info))
    }

    /// not support
    async fn get_flight_info_prepared_statement(
        &self,
        query: CommandPreparedStatementQuery,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        debug!(
            "get_flight_info_prepared_statement: query: {:?}, request: {:?}",
            query, request
        );

        Err(Status::unimplemented(
            "get_flight_info_prepared_statement not implemented",
        ))
    }

    /// TODO support
    async fn get_flight_info_catalogs(
        &self,
        query: CommandGetCatalogs,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        debug!(
            "get_flight_info_catalogs: query: {:?}, request: {:?}",
            query, request
        );

        Err(Status::unimplemented(
            "get_flight_info_catalogs not implemented",
        ))
    }

    /// TODO support
    async fn get_flight_info_schemas(
        &self,
        query: CommandGetDbSchemas,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        debug!(
            "get_flight_info_schemas: query: {:?}, request: {:?}",
            query, request
        );

        Err(Status::unimplemented(
            "get_flight_info_schemas not implemented",
        ))
    }

    /// TODO support
    async fn get_flight_info_tables(
        &self,
        query: CommandGetTables,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        debug!(
            "get_flight_info_tables: query: {:?}, request: {:?}",
            query, request
        );

        Err(Status::unimplemented(
            "get_flight_info_tables not implemented",
        ))
    }

    /// TODO support
    async fn get_flight_info_table_types(
        &self,
        query: CommandGetTableTypes,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        debug!(
            "get_flight_info_table_types: query: {:?}, request: {:?}",
            query, request
        );

        Err(Status::unimplemented(
            "get_flight_info_table_types not implemented",
        ))
    }

    /// not support
    async fn get_flight_info_sql_info(
        &self,
        query: CommandGetSqlInfo,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        debug!(
            "get_flight_info_sql_info: query: {:?}, request: {:?}",
            query, request
        );

        Err(Status::unimplemented(
            "get_flight_info_sql_info not implemented",
        ))
    }

    /// not support
    async fn get_flight_info_primary_keys(
        &self,
        query: CommandGetPrimaryKeys,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        debug!(
            "get_flight_info_primary_keys: query: {:?}, request: {:?}",
            query, request
        );

        Err(Status::unimplemented(
            "get_flight_info_primary_keys not implemented",
        ))
    }

    /// not support
    async fn get_flight_info_exported_keys(
        &self,
        query: CommandGetExportedKeys,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        debug!(
            "get_flight_info_exported_keys: query: {:?}, request: {:?}",
            query, request
        );

        Err(Status::unimplemented(
            "get_flight_info_exported_keys not implemented",
        ))
    }

    /// not support
    async fn get_flight_info_imported_keys(
        &self,
        query: CommandGetImportedKeys,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        debug!(
            "get_flight_info_imported_keys: query: {:?}, request: {:?}",
            query, request
        );

        Err(Status::unimplemented(
            "get_flight_info_imported_keys not implemented",
        ))
    }

    /// not support
    async fn get_flight_info_cross_reference(
        &self,
        query: CommandGetCrossReference,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        debug!(
            "get_flight_info_cross_reference: query: {:?}, request: {:?}",
            query, request
        );

        Err(Status::unimplemented(
            "get_flight_info_imported_keys not implemented",
        ))
    }

    // do_get
    /// support
    async fn do_get_statement(
        &self,
        ticket: TicketStatementQuery,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        debug!(
            "do_get_statement: query: {:?}, request: {:?}",
            ticket, request
        );

        let statement_handle = ticket.statement_handle;
        let result_data = self.result_cache.get(&statement_handle).ok_or_else(|| {
            Status::internal(format!(
                "The result of query({:?}) does not exist or has expired",
                statement_handle
            ))
        })?;

        let options = IpcWriteOptions::default();
        let batches = result_data
            .chunk_result()
            .iter()
            .enumerate()
            .flat_map(|(counter, batch)| {
                let (dictionary_flight_data, mut batch_flight_data) =
                    flight_utils::flight_data_from_arrow_batch(&batch, &options);

                // Only the record batch's FlightData gets app_metadata
                let metadata = counter.to_string().into_bytes();
                batch_flight_data.app_metadata = metadata;

                dictionary_flight_data
                    .into_iter()
                    .chain(std::iter::once(batch_flight_data))
                    .map(Ok)
            })
            .collect::<Vec<_>>();
        let output = futures::stream::iter(batches);

        // clear cache of this query
        self.result_cache.invalidate(&statement_handle);

        Ok(Response::new(
            Box::pin(output) as <Self as FlightService>::DoGetStream
        ))
    }

    /// not support
    async fn do_get_prepared_statement(
        &self,
        query: CommandPreparedStatementQuery,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        debug!(
            "do_get_prepared_statement: query: {:?}, request: {:?}",
            query, request
        );

        Err(Status::unimplemented(
            "do_get_prepared_statement not implemented",
        ))
    }

    /// TODO support
    async fn do_get_catalogs(
        &self,
        query: CommandGetCatalogs,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        debug!(
            "do_get_catalogs: query: {:?}, request: {:?}",
            query, request
        );

        Err(Status::unimplemented("do_get_catalogs not implemented"))
    }

    /// TODO support
    async fn do_get_schemas(
        &self,
        query: CommandGetDbSchemas,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        debug!("do_get_schemas: query: {:?}, request: {:?}", query, request);

        Err(Status::unimplemented("do_get_schemas not implemented"))
    }

    /// TODO support
    async fn do_get_tables(
        &self,
        query: CommandGetTables,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        debug!("do_get_tables: query: {:?}, request: {:?}", query, request);

        Err(Status::unimplemented("do_get_tables not implemented"))
    }

    /// TODO support
    async fn do_get_table_types(
        &self,
        query: CommandGetTableTypes,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        debug!(
            "do_get_table_types: query: {:?}, request: {:?}",
            query, request
        );

        Err(Status::unimplemented("do_get_table_types not implemented"))
    }

    /// not support
    async fn do_get_sql_info(
        &self,
        query: CommandGetSqlInfo,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        debug!(
            "do_get_sql_info: query: {:?}, request: {:?}",
            query, request
        );

        Err(Status::unimplemented("do_get_sql_info not implemented"))
    }

    /// not support
    async fn do_get_primary_keys(
        &self,
        query: CommandGetPrimaryKeys,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        debug!(
            "do_get_primary_keys: query: {:?}, request: {:?}",
            query, request
        );

        Err(Status::unimplemented("do_get_primary_keys not implemented"))
    }

    /// not support
    async fn do_get_exported_keys(
        &self,
        query: CommandGetExportedKeys,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        debug!(
            "do_get_exported_keys: query: {:?}, request: {:?}",
            query, request
        );

        Err(Status::unimplemented(
            "do_get_exported_keys not implemented",
        ))
    }

    /// not support
    async fn do_get_imported_keys(
        &self,
        _query: CommandGetImportedKeys,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        Err(Status::unimplemented(
            "do_get_imported_keys not implemented",
        ))
    }

    /// not support
    async fn do_get_cross_reference(
        &self,
        _query: CommandGetCrossReference,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        Err(Status::unimplemented(
            "do_get_cross_reference not implemented",
        ))
    }

    // do_put
    /// TODO support
    async fn do_put_statement_update(
        &self,
        ticket: CommandStatementUpdate,
        request: Request<Streaming<FlightData>>,
    ) -> Result<i64, Status> {
        debug!(
            "do_put_statement_update: query: {:?}, request: {:?}",
            ticket, request
        );

        Err(Status::unimplemented(
            "do_put_statement_update not implemented",
        ))
    }

    /// not support
    async fn do_put_prepared_statement_query(
        &self,
        query: CommandPreparedStatementQuery,
        request: Request<Streaming<FlightData>>,
    ) -> Result<Response<<Self as FlightService>::DoPutStream>, Status> {
        debug!(
            "do_put_prepared_statement_query: query: {:?}, request: {:?}",
            query, request
        );

        Err(Status::unimplemented(
            "do_put_prepared_statement_query not implemented",
        ))
    }

    /// not support
    async fn do_put_prepared_statement_update(
        &self,
        query: CommandPreparedStatementUpdate,
        request: Request<Streaming<FlightData>>,
    ) -> Result<i64, Status> {
        debug!(
            "do_put_prepared_statement_update: query: {:?}, request: {:?}",
            query, request
        );

        Err(Status::unimplemented(
            "do_put_prepared_statement_update not implemented",
        ))
    }

    // do_action
    /// not support
    async fn do_action_create_prepared_statement(
        &self,
        query: ActionCreatePreparedStatementRequest,
        request: Request<Action>,
    ) -> Result<ActionCreatePreparedStatementResult, Status> {
        debug!(
            "do_action_create_prepared_statement: query: {:?}, request: {:?}",
            query, request
        );

        Err(Status::unimplemented("Not yet implemented"))
    }

    /// not support
    async fn do_action_close_prepared_statement(
        &self,
        query: ActionClosePreparedStatementRequest,
        request: Request<Action>,
    ) {
        debug!(
            "do_action_close_prepared_statement: query: {:?}, request: {:?}",
            query, request
        );

        unimplemented!("Not yet implemented")
    }

    /// not support
    async fn register_sql_info(&self, _id: i32, _result: &SqlInfo) {
        debug!("register_sql_info: _id: {:?}, request: {:?}", _id, _result);
    }
}

#[cfg(test)]
mod test {
    use std::{collections::HashMap, sync::Arc, time::Duration};

    use arrow_flight::{
        flight_service_client::FlightServiceClient,
        flight_service_server::FlightServiceServer,
        sql::{CommandStatementQuery, ProstAnyExt},
        utils as flight_utils, FlightData, FlightDescriptor, HandshakeRequest, IpcMessage,
    };
    use datafusion::arrow::{self, buffer::Buffer, datatypes::Schema, ipc};
    use futures::{StreamExt, TryStreamExt};
    use http_protocol::header::AUTHORIZATION;
    use moka::sync::{Cache, CacheBuilder};
    use prost::Message;
    use spi::server::dbms::DatabaseManagerSystemMock;
    use tokio::time;
    use tonic::{
        client::Grpc,
        metadata::{AsciiMetadataValue, KeyAndValueRef, MetadataValue},
        transport::{Channel, Endpoint, Server},
        Code, Request, Status, Streaming,
    };

    use crate::flight_sql::{flight_sql_server::FlightSqlServiceImpl, utils};

    async fn run_test_server() {
        let addr = "0.0.0.0:31004".parse().expect("parse address");

        let svc = FlightServiceServer::new(FlightSqlServiceImpl::new(Arc::new(
            DatabaseManagerSystemMock {},
        )));

        println!("Listening on {:?}", addr);

        let server = Server::builder().add_service(svc).serve(addr);

        let _handle = tokio::spawn(server);
    }

    #[tokio::test]
    async fn test_client() {
        run_test_server().await;

        let endpoint = Endpoint::from_static("http://localhost:31004");
        let mut client = FlightServiceClient::connect(endpoint)
            .await
            .expect("connect");

        let handshake_request = HandshakeRequest {
            protocol_version: 0,
            payload: vec![],
        };

        let mut req = Request::new(futures::stream::iter(vec![handshake_request]));
        req.metadata_mut().insert(
            AUTHORIZATION.as_str(),
            MetadataValue::from_static("Basic eHg6eHgK"),
        );

        let resp = client.handshake(req).await.expect("handshake");

        let cmd = CommandStatementQuery {
            query: "select 1;".to_string(),
        };
        let any = prost_types::Any::pack(&cmd).expect("pack");
        let fd = FlightDescriptor::new_cmd(any.encode_to_vec());
        let mut req = Request::new(fd);

        req.metadata_mut().insert(
            AUTHORIZATION.as_str(),
            MetadataValue::from_static("Basic eHg6eHgK"),
        );

        let resp = client.get_flight_info(req).await.expect("get_flight_info");

        let flight_info = resp.into_inner();

        let schema = Schema::try_from(IpcMessage(flight_info.schema)).expect("Schema::try_from");

        let schema_ref = Arc::new(schema);

        for ep in flight_info.endpoint {
            if let Some(tkt) = ep.ticket {
                let resp = client.do_get(tkt).await.expect("do_get");

                let mut stream = resp.into_inner();
                let mut dictionaries_by_id = HashMap::new();
                let mut chunks = vec![];

                while let Some(Ok(data)) = stream.next().await {
                    let message = arrow::ipc::root_as_message(&data.data_header[..])
                        .expect("root_as_message");

                    match message.header_type() {
                        ipc::MessageHeader::Schema => {
                            panic!("Not expecting a schema when messages are read",);
                        }
                        ipc::MessageHeader::RecordBatch => {
                            let batch = utils::record_batch_from_message(
                                message,
                                &Buffer::from(data.data_body),
                                schema_ref.clone(),
                                &dictionaries_by_id,
                            )
                            .expect("record_batch_from_message");

                            println!("ipc::MessageHeader::RecordBatch: {:?}", batch);

                            chunks.push(batch);
                        }
                        ipc::MessageHeader::DictionaryBatch => {
                            utils::dictionary_from_message(
                                message,
                                &Buffer::from(data.data_body),
                                schema_ref.clone(),
                                &mut dictionaries_by_id,
                            )
                            .expect("dictionary_from_message");
                        }
                        t => {
                            panic!("Reading types other than record batches not yet supported");
                        }
                    }
                }
            };
        }
    }
}
