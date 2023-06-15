use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use arrow_flight::flight_service_server::FlightService;
use arrow_flight::sql::server::FlightSqlService;
use arrow_flight::sql::{
    ActionClosePreparedStatementRequest, ActionCreatePreparedStatementRequest,
    ActionCreatePreparedStatementResult, CommandGetCatalogs, CommandGetCrossReference,
    CommandGetDbSchemas, CommandGetExportedKeys, CommandGetImportedKeys, CommandGetPrimaryKeys,
    CommandGetSqlInfo, CommandGetTableTypes, CommandGetTables, CommandPreparedStatementQuery,
    CommandPreparedStatementUpdate, CommandStatementQuery, CommandStatementUpdate, SqlInfo,
    TicketStatementQuery,
};
use arrow_flight::{
    utils as flight_utils, Action, FlightData, FlightDescriptor, FlightInfo, HandshakeRequest,
    HandshakeResponse, SchemaAsIpc, Ticket,
};
use datafusion::arrow::datatypes::{Schema, SchemaRef, ToByteSlice};
use datafusion::arrow::ipc::writer::IpcWriteOptions;
use futures::Stream;
use http_protocol::header::{DB, STREAM_TRIGGER_INTERVAL, TARGET_PARTITIONS, TENANT};
use models::auth::user::User;
use models::oid::UuidGenerator;
use moka::sync::Cache;
use prost::bytes::Bytes;
use spi::query::config::StreamTriggerInterval;
use spi::query::execution::{Output, QueryStateMachineRef};
use spi::query::logical_planner::Plan;
use spi::server::dbms::DBMSRef;
use spi::service::protocol::{Context, ContextBuilder, Query, QueryHandle};
use tonic::metadata::MetadataMap;
use tonic::{Extensions, Request, Response, Status, Streaming};
use trace::{debug, SpanContext, SpanExt, SpanRecorder};

use super::auth_middleware::CallHeaderAuthenticator;
use crate::flight_sql::auth_middleware::AuthResult;
use crate::flight_sql::utils;

pub struct FlightSqlServiceImpl<T> {
    instance: DBMSRef,
    authenticator: T,
    id_generator: UuidGenerator,
    result_cache: Cache<Vec<u8>, (Option<Plan>, QueryStateMachineRef)>,
}

impl<T> FlightSqlServiceImpl<T> {
    pub fn new(instance: DBMSRef, authenticator: T) -> Self {
        let result_cache = Cache::builder()
            .thread_pool_enabled(false)
            // Time to live (TTL): 2 minutes
            // The query results are only cached for 2 minutes and expire after 2 minutes
            .time_to_live(Duration::from_secs(2 * 60))
            .build();

        Self {
            instance,
            authenticator,
            id_generator: Default::default(),
            result_cache,
        }
    }
}

impl<T> FlightSqlServiceImpl<T>
where
    T: CallHeaderAuthenticator + Send + Sync + 'static,
{
    async fn pre_precess_statement_query_req(
        &self,
        sql: impl Into<String>,
        req_headers: &MetadataMap,
        span_ctx: Option<&SpanContext>,
    ) -> Result<(Option<Plan>, QueryStateMachineRef), Status> {
        // auth request
        let auth_result = {
            let _span_recorder = SpanRecorder::new(span_ctx.child_span("authenticate"));
            self.authenticator.authenticate(req_headers).await?
        };
        let user = auth_result.identity();

        // construct context by user_info and headers(parse tenant & default database)
        let ctx = {
            let _span_recorder = SpanRecorder::new(span_ctx.child_span("construct context"));
            self.construct_context(user, req_headers)?
        };

        // build query state machine
        let query_state_machine = {
            let _span_recorder =
                SpanRecorder::new(span_ctx.child_span("build query_state_machine"));
            self.build_query_state_machine(sql.into(), ctx, span_ctx)
                .await?
        };

        // build logical plan
        let logical_plan = self.build_logical_plan(query_state_machine.clone()).await?;

        Ok((logical_plan, query_state_machine))
    }

    async fn pre_precess_statement_query_req_and_save(
        &self,
        sql: impl Into<String>,
        req_headers: &MetadataMap,
        span_ctx: Option<&SpanContext>,
    ) -> Result<(Vec<u8>, SchemaRef), Status> {
        let (logical_plan, query_state_machine) = self
            .pre_precess_statement_query_req(sql, req_headers, span_ctx)
            .await?;

        let schema = logical_plan
            .as_ref()
            .map(|e| e.schema())
            .unwrap_or(Arc::new(Schema::empty()));

        // generate result identifier
        let result_ident = self.id_generator.next_id().to_le_bytes().to_vec();

        // cache result wait cli fetching
        self.result_cache
            .insert(result_ident.clone(), (logical_plan, query_state_machine));

        Ok((result_ident, schema))
    }

    async fn precess_flight_info_req(
        &self,
        sql: impl Into<String>,
        request: Request<FlightDescriptor>,
        span_ctx: Option<&SpanContext>,
    ) -> Result<Response<FlightInfo>, Status> {
        let (result_ident, schema) = self
            .pre_precess_statement_query_req_and_save(sql, request.metadata(), span_ctx)
            .await?;

        // construct response start
        let flight_info =
            self.construct_flight_info(result_ident, schema.as_ref(), 0, request.into_inner())?;

        Ok(Response::new(flight_info))
    }

    fn construct_flight_info(
        &self,
        result_ident: impl Into<Bytes>,
        schema: &Schema,
        total_records: i64,
        flight_descriptor: FlightDescriptor,
    ) -> Result<FlightInfo, Status> {
        let option = IpcWriteOptions::default();
        let ipc_message = SchemaAsIpc::new(schema, &option)
            .try_into()
            .map_err(|e| Status::internal(format!("{}", e)))?;
        let tkt = TicketStatementQuery {
            statement_handle: result_ident.into(),
        };
        let endpoint = utils::endpoint(tkt, Default::default()).map_err(Status::internal)?;

        let flight_info = FlightInfo::new(
            ipc_message,
            Some(flight_descriptor),
            vec![endpoint],
            total_records,
            -1,
        );

        Ok(flight_info)
    }

    fn construct_context(
        &self,
        user_info: User,
        metadata: &MetadataMap,
    ) -> Result<Context, Status> {
        // parse tenant & default database
        let tenant = utils::get_value_from_header(metadata, TENANT, "");
        let db = utils::get_value_from_header(metadata, DB, "");
        let target_partitions = utils::get_value_from_header(metadata, TARGET_PARTITIONS, "")
            .map(|e| e.parse::<usize>())
            .transpose()
            .map_err(|e| {
                Status::invalid_argument(format!(
                    "parse {} failed, error: {}",
                    TARGET_PARTITIONS, e
                ))
            })?;
        let stream_trigger_interval =
            utils::get_value_from_header(metadata, STREAM_TRIGGER_INTERVAL, "")
                .map(|e| e.parse::<StreamTriggerInterval>())
                .transpose()
                .map_err(|e| {
                    Status::invalid_argument(format!(
                        "parse {} failed, error: {}",
                        STREAM_TRIGGER_INTERVAL, e
                    ))
                })?;
        let ctx = ContextBuilder::new(user_info)
            .with_tenant(tenant)
            .with_database(db)
            .with_target_partitions(target_partitions)
            .with_stream_trigger_interval(stream_trigger_interval)
            .build();

        Ok(ctx)
    }

    async fn build_query_state_machine(
        &self,
        sql: impl Into<String>,
        ctx: Context,
        span_context: Option<&SpanContext>,
    ) -> Result<QueryStateMachineRef, Status> {
        let query = Query::new(ctx, sql.into());
        // TODO
        let query_state_machine = self
            .instance
            .build_query_state_machine(query, span_context)
            .await
            .map_err(|e| {
                // TODO convert error message
                Status::internal(format!("{}", e))
            })?;
        Ok(query_state_machine)
    }

    async fn build_logical_plan(
        &self,
        query_state_machine: QueryStateMachineRef,
    ) -> Result<Option<Plan>, Status> {
        let logical_plan = self
            .instance
            .build_logical_plan(query_state_machine)
            .await
            .map_err(|e| {
                // TODO convert error message
                Status::internal(format!("{}", e))
            })?;
        Ok(logical_plan)
    }

    async fn execute_logical_plan(
        &self,
        logical_plan: Option<Plan>,
        query_state_machine: QueryStateMachineRef,
    ) -> Result<QueryHandle, Status> {
        let query_result = match logical_plan {
            None => QueryHandle::new(
                query_state_machine.query_id,
                query_state_machine.query.clone(),
                Output::Nil(()),
            ),
            Some(logical_plan) => self
                .instance
                .execute_logical_plan(logical_plan, query_state_machine)
                .await
                .map_err(|e| {
                    // TODO convert error message
                    Status::internal(format!("{}", e))
                })?,
        };
        Ok(query_result)
    }

    fn get_plan_and_qsm(
        &self,
        statement_handle: &[u8],
        span_ctx: Option<SpanContext>,
    ) -> Result<(Option<Plan>, QueryStateMachineRef), Status> {
        let (logical_plan, query_state_machine) =
            self.result_cache.get(statement_handle).ok_or_else(|| {
                Status::internal(format!(
                    "The result of query({:?}) does not exist or has expired",
                    statement_handle
                ))
            })?;
        let query_state_machine = Arc::new(query_state_machine.with_span_ctx(span_ctx));

        Ok((logical_plan, query_state_machine))
    }

    async fn fetch_result_set(
        &self,
        statement_handle: &[u8],
        span_ctx: Option<&SpanContext>,
    ) -> Result<<Self as FlightService>::DoGetStream, Status> {
        let (logical_plan, query_state_machine) =
            self.get_plan_and_qsm(statement_handle, span_ctx.cloned())?;

        // execute plan
        let query_result = self
            .execute_logical_plan(logical_plan, query_state_machine)
            .await?;
        let output = query_result.result();

        let schema = (*output.schema()).clone();
        let batches = output
            .chunk_result()
            .await
            .map_err(|e| Status::internal(format!("Could not chunk result, error: {}", e)))?;

        let flight_data = flight_utils::batches_to_flight_data(schema, batches)
            .map_err(|e| Status::internal(format!("Could not convert batches, error: {}", e)))?
            .into_iter()
            .map(Ok);
        let stream: Pin<Box<dyn Stream<Item = Result<FlightData, Status>> + Send>> =
            Box::pin(futures::stream::iter(flight_data));
        Ok(stream)
    }
}

/// use jdbc to execute statement query:
///
/// e.g.
/// ```java
/// .   final Properties properties = new Properties();
/// .   
/// .   properties.put(ArrowFlightConnectionProperty.USER.camelName(), user);
/// .   properties.put(ArrowFlightConnectionProperty.PASSWORD.camelName(), password);
/// .   properties.put("tenant", "cnosdb");
/// .   //        properties.put("db", "db1");
/// .   properties.put("useEncryption", false);
/// .   
/// .   try (Connection connection = DriverManager.getConnection(
/// .           "jdbc:arrow-flight-sql://" + host + ":" + port, properties);
/// .        Statement stmt = connection.createStatement()) {
/// .   //            assert stmt.execute("DROP DATABASE IF EXISTS oceanic_station;");
/// .   //            assert stmt.execute("CREATE DATABASE IF NOT EXISTS oceanic_station;");
/// .       stmt.execute("CREATE TABLE IF NOT EXISTS air\n" +
/// .               "(\n" +
/// .               "    visibility  DOUBLE,\n" +
/// .               "    temperature DOUBLE,\n" +
/// .               "    pressure    DOUBLE,\n" +
/// .               "    TAGS(station)\n" +
/// .               ");");
/// .       stmt.execute("INSERT INTO air (TIME, station, visibility, temperature, pressure) VALUES\n" +
/// .               "    (1666165200290401000, 'XiaoMaiDao', 56, 69, 77);");
/// .   
/// .       ResultSet resultSet = stmt.executeQuery("select * from air limit 1;");
/// .   
/// .       while (resultSet.next()) {
/// .           assertNotNull(resultSet.getString(1));
/// .       }
/// .   }
/// ```
/// 1. do_handshake: basic auth -> baerar token
/// 2. do_action_create_prepared_statement: sql(baerar token) -> sql
/// 3. do_put_prepared_statement_update: not use
/// 4. get_flight_info_prepared_statement: sql(baerar token) -> address of resut set
/// 5. do_get_statement: address of resut set(baerar token) -> resut set stream
/// ```
///
/// use flight sql to execute statement query:
///
/// e.g.
///
/// 1. do_handshake: basic auth -> baerar token
/// 4. get_flight_info_statement: sql(baerar token) -> address of resut set
/// 5. do_get_statement: address of resut set(baerar token) -> resut set stream
///
#[tonic::async_trait]
impl<T> FlightSqlService for FlightSqlServiceImpl<T>
where
    T: CallHeaderAuthenticator + Send + Sync + 'static,
{
    type FlightService = FlightSqlServiceImpl<T>;

    /// Perform client authentication
    async fn do_handshake(
        &self,
        request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<
        Response<Pin<Box<dyn Stream<Item = Result<HandshakeResponse, Status>> + Send>>>,
        Status,
    > {
        debug!("do_handshake: {:?}", request);

        let _span_recorder = get_span_recorder(request.extensions(), "flight sql do_handshake");

        let meta_data = request.metadata();
        let auth_result = self.authenticator.authenticate(meta_data).await?;

        let result = HandshakeResponse {
            protocol_version: 0,
            payload: "NULL".into(),
        };
        let result = Ok(result);
        let output: Pin<Box<dyn Stream<Item = Result<HandshakeResponse, Status>> + Send>> =
            Box::pin(futures::stream::iter(vec![result]));
        let mut resp = Response::new(output);

        // Append the token generated by authenticator to the response header
        auth_result.append_to_outgoing_headers(resp.metadata_mut())?;

        return Ok(resp);
    }

    /// Execute an ad-hoc SQL query.
    ///
    /// Return the address of the result set,
    /// waiting to call [`Self::do_get_statement`] to get the result set.
    async fn get_flight_info_statement(
        &self,
        query: CommandStatementQuery,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        debug!(
            "get_flight_info_statement: query: {:?}, request: {:?}",
            query, request
        );

        let span_recorder =
            get_span_recorder(request.extensions(), "flight sql get_flight_info_statement");

        let CommandStatementQuery { query: sql } = query;

        self.precess_flight_info_req(sql, request, span_recorder.span_ctx())
            .await
    }

    /// Fetch meta of the prepared statement.
    ///
    /// The prepared statement can be reused after fetching results.
    async fn get_flight_info_prepared_statement(
        &self,
        query: CommandPreparedStatementQuery,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        debug!(
            "get_flight_info_prepared_statement: query: {:?}, request: {:?}",
            query, request
        );

        let _span_recorder = get_span_recorder(
            request.extensions(),
            "flight sql get_flight_info_prepared_statement",
        );

        let CommandPreparedStatementQuery {
            prepared_statement_handle,
        } = query;
        let prepared_statement_handle = prepared_statement_handle.to_byte_slice().to_owned();

        // construct response start
        let flight_info = self.construct_flight_info(
            prepared_statement_handle,
            &Schema::empty(),
            0,
            request.into_inner(),
        )?;

        Ok(Response::new(flight_info))
    }

    async fn get_flight_info_catalogs(
        &self,
        query: CommandGetCatalogs,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        debug!(
            "get_flight_info_catalogs: query: {:?}, request: {:?}",
            query, request
        );

        let span_recorder =
            get_span_recorder(request.extensions(), "flight sql get_flight_info_catalogs");

        self.precess_flight_info_req(
            "SELECT 
                TENANT_NAME AS TABLE_CAT 
            FROM 
                CLUSTER_SCHEMA.TENANTS 
            ORDER BY 
                TABLE_CAT",
            request,
            span_recorder.span_ctx(),
        )
        .await
    }

    async fn get_flight_info_schemas(
        &self,
        query: CommandGetDbSchemas,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        debug!(
            "get_flight_info_schemas: query: {:?}, request: {:?}",
            query, request
        );

        let span_recorder =
            get_span_recorder(request.extensions(), "flight sql get_flight_info_catalogs");

        let CommandGetDbSchemas {
            catalog,
            db_schema_filter_pattern,
        } = query;

        let mut filters = vec![];
        let _ = catalog.map(|e| filters.push(format!("TABLE_CATALOG = '{}'", e)));
        let _ =
            db_schema_filter_pattern.map(|e| filters.push(format!("DATABASE_NAME LIKE '{e}'",)));

        let filter = if filters.is_empty() {
            "".to_string()
        } else {
            format!("WHERE {}", filters.join(" AND "))
        };

        self.precess_flight_info_req(
            format!(
                "SELECT
                    DATABASE_NAME AS TABLE_SCHEM,
                    TENANT_NAME AS TABLE_CATALOG
                FROM
                    INFORMATION_SCHEMA.DATABASES
                {filter}
                ORDER BY
                    TABLE_CATALOG, TABLE_SCHEM"
            ),
            request,
            span_recorder.span_ctx(),
        )
        .await
    }

    async fn get_flight_info_tables(
        &self,
        query: CommandGetTables,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        debug!(
            "get_flight_info_tables: query: {:?}, request: {:?}",
            query, request
        );

        let span_recorder =
            get_span_recorder(request.extensions(), "flight sql get_flight_info_tables");

        let CommandGetTables {
            catalog,
            db_schema_filter_pattern,
            table_name_filter_pattern,
            table_types,
            include_schema: _,
        } = query;

        let mut filters = vec![];
        let _ = catalog.map(|e| filters.push(format!("TABLE_CATALOG = '{}'", e)));
        let _ =
            db_schema_filter_pattern.map(|e| filters.push(format!("TABLE_DATABASE LIKE '{e}'")));
        let _ = table_name_filter_pattern.map(|e| filters.push(format!("TABLE_NAME LIKE '{e}'")));
        if !table_types.is_empty() {
            let table_types = table_types
                .iter()
                .map(|e| format!("'{}'", e))
                .collect::<Vec<_>>()
                .join(",");
            filters.push(format!("TABLE_TYPE IN ({})", table_types));
        }

        let filter = if filters.is_empty() {
            "".to_string()
        } else {
            format!("WHERE {}", filters.join(" AND "))
        };

        let sql = format!(
            "SELECT
                TABLE_TENANT as TABLE_CAT,
                TABLE_DATABASE as TABLE_SCHEM,
                TABLE_NAME,
                TABLE_TYPE
            FROM
                INFORMATION_SCHEMA.TABLES
            {filter}
            ORDER BY
                TABLE_TYPE, TABLE_CAT, TABLE_SCHEM, TABLE_NAME"
        );

        trace::warn!("CommandGetTables:\n{sql}");

        self.precess_flight_info_req(sql, request, span_recorder.span_ctx())
            .await
    }

    async fn get_flight_info_table_types(
        &self,
        query: CommandGetTableTypes,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        debug!(
            "get_flight_info_table_types: query: {:?}, request: {:?}",
            query, request
        );

        let span_recorder = get_span_recorder(
            request.extensions(),
            "flight sql get_flight_info_table_types",
        );

        self.precess_flight_info_req(
            "SELECT TABLE_TYPE 
            FROM 
                (VALUES('TABLE'),('VIEW'),('LOCAL TEMPORARY')) t(TABLE_TYPE)",
            request,
            span_recorder.span_ctx(),
        )
        .await
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

    /// Fetch the ad-hoc SQL query's result set
    ///
    /// [`TicketStatementQuery`] is the result obtained after calling [`Self::get_flight_info_statement`]
    async fn do_get_statement(
        &self,
        ticket: TicketStatementQuery,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        debug!(
            "do_get_statement: query: {:?}, request: {:?}",
            ticket, request
        );

        let span_recorder = get_span_recorder(request.extensions(), "flight sql do_get_statement");

        let TicketStatementQuery { statement_handle } = ticket;

        let output = self
            .fetch_result_set(&statement_handle, span_recorder.span_ctx())
            .await?;

        // clear cache of this query
        self.result_cache
            .invalidate(statement_handle.to_byte_slice());

        Ok(Response::new(output))
    }

    /// Fetch the prepared SQL query's result set
    ///
    /// [`CommandPreparedStatementQuery`] is the result obtained after calling [`Self::get_flight_info_prepared_statement`]
    async fn do_get_prepared_statement(
        &self,
        query: CommandPreparedStatementQuery,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        debug!(
            "do_get_prepared_statement: query: {:?}, request: {:?}",
            query, request
        );

        let span_recorder =
            get_span_recorder(request.extensions(), "flight sql do_get_prepared_statement");

        let prepared_statement_handle = query.prepared_statement_handle.to_byte_slice();

        let output = self
            .fetch_result_set(prepared_statement_handle, span_recorder.span_ctx())
            .await?;

        // clear cache of this query
        self.result_cache
            .invalidate(prepared_statement_handle.to_byte_slice());

        Ok(Response::new(output))
    }

    /// TODO support
    /// wait for <https://github.com/cnosdb/cnosdb/issues/642>
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
    /// wait for <https://github.com/cnosdb/cnosdb/issues/642>
    async fn do_get_schemas(
        &self,
        query: CommandGetDbSchemas,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        debug!("do_get_schemas: query: {:?}, request: {:?}", query, request);

        Err(Status::unimplemented("do_get_schemas not implemented"))
    }

    /// TODO support
    /// wait for `<https://github.com/cnosdb/cnosdb/issues/642>`
    async fn do_get_tables(
        &self,
        query: CommandGetTables,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        debug!("do_get_tables: query: {:?}, request: {:?}", query, request);

        Err(Status::unimplemented("do_get_tables not implemented"))
    }

    /// TODO support
    /// wait for <https://github.com/cnosdb/cnosdb/issues/642>
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

    /// Execute an ad-hoc SQL query and return the number of affected rows.
    async fn do_put_statement_update(
        &self,
        ticket: CommandStatementUpdate,
        request: Request<Streaming<FlightData>>,
    ) -> Result<i64, Status> {
        debug!(
            "do_put_statement_update: query: {:?}, request: {:?}",
            ticket, request
        );

        let span_recorder =
            get_span_recorder(request.extensions(), "flight sql do_put_statement_update");
        let span_ctx = span_recorder.span_ctx();

        let CommandStatementUpdate { query } = ticket;
        let req_headers = request.metadata();

        let (logical_plan, query_state_machine) = self
            .pre_precess_statement_query_req(query, req_headers, span_ctx)
            .await?;

        // execute plan
        let query_result = self
            .execute_logical_plan(logical_plan, query_state_machine)
            .await?;

        let affected_rows = query_result.result().affected_rows().await;

        Ok(affected_rows)
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

    /// Execute the query and return the number of affected rows.
    /// The prepared statement can be reused afterwards.
    ///
    /// Prepared statement is not supported,
    /// because ad-hoc statement of flight jdbc needs to call this interface, so it is simple to implement
    async fn do_put_prepared_statement_update(
        &self,
        _query: CommandPreparedStatementUpdate,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<i64, Status> {
        // debug!(
        //     "do_put_prepared_statement_update: query: {:?}, request: {:?}",
        //     query, request
        // );
        //
        // let prepared_statement_handle = query.prepared_statement_handle.to_byte_slice();
        //
        // let rows_count = self.fetch_affected_rows_count(prepared_statement_handle)?;
        //
        // Ok(rows_count)
        Err(Status::unimplemented(
            "do_put_prepared_statement_update not implemented",
        ))
    }

    /// Prepared statement is not supported,
    /// because ad-hoc statement of flight jdbc needs to call this interface,
    /// so directly return the sql as the result.
    async fn do_action_create_prepared_statement(
        &self,
        query: ActionCreatePreparedStatementRequest,
        request: Request<Action>,
    ) -> Result<ActionCreatePreparedStatementResult, Status> {
        debug!(
            "do_action_create_prepared_statement: query: {:?}, request: {:?}",
            query, request
        );

        let span_recorder = get_span_recorder(
            request.extensions(),
            "flight sql do_action_create_prepared_statement",
        );

        let ActionCreatePreparedStatementRequest { query: sql } = query;

        let (result_ident, _schema) = self
            .pre_precess_statement_query_req_and_save(
                sql,
                request.metadata(),
                span_recorder.span_ctx(),
            )
            .await?;

        let result = ActionCreatePreparedStatementResult {
            prepared_statement_handle: result_ident.into(),
            ..Default::default()
        };

        Ok(result)
    }

    /// Close a previously created prepared statement.
    ///
    /// Empty logic, because we not save created prepared statement.
    async fn do_action_close_prepared_statement(
        &self,
        query: ActionClosePreparedStatementRequest,
        request: Request<Action>,
    ) {
        debug!(
            "do_action_close_prepared_statement: query: {:?}, request: {:?}",
            query, request
        );
    }

    /// not support
    async fn register_sql_info(&self, _id: i32, _result: &SqlInfo) {
        debug!("register_sql_info: _id: {:?}, request: {:?}", _id, _result);
    }
}

fn get_span_recorder(extensions: &Extensions, child_span_name: &'static str) -> SpanRecorder {
    let span_context = extensions.get::<SpanContext>();
    SpanRecorder::new(span_context.child_span(child_span_name))
}

#[cfg(test)]
mod test {
    use std::collections::HashMap;
    use std::sync::Arc;

    use arrow_flight::flight_service_client::FlightServiceClient;
    use arrow_flight::flight_service_server::FlightServiceServer;
    use arrow_flight::sql::client::FlightSqlServiceClient;
    use arrow_flight::sql::{Any, CommandStatementQuery};
    use arrow_flight::utils::flight_data_to_batches;
    use arrow_flight::{FlightDescriptor, HandshakeRequest, IpcMessage};
    use datafusion::arrow::buffer::Buffer;
    use datafusion::arrow::datatypes::Schema;
    use datafusion::arrow::{self, ipc};
    use futures::{StreamExt, TryStreamExt};
    use http_protocol::header::AUTHORIZATION;
    use prost::Message;
    use spi::server::dbms::DatabaseManagerSystemMock;
    use tonic::metadata::MetadataValue;
    use tonic::transport::{Channel, Endpoint, Server};
    use tonic::Request;

    use crate::flight_sql::auth_middleware::basic_call_header_authenticator::BasicCallHeaderAuthenticator;
    use crate::flight_sql::auth_middleware::generated_bearer_token_authenticator::GeneratedBearerTokenAuthenticator;
    use crate::flight_sql::flight_sql_server::FlightSqlServiceImpl;
    use crate::flight_sql::utils;

    async fn run_test_server() {
        let addr = "0.0.0.0:8904".parse().expect("parse address");

        let instance = Arc::new(DatabaseManagerSystemMock {});
        let authenticator = GeneratedBearerTokenAuthenticator::new(
            BasicCallHeaderAuthenticator::new(instance.clone()),
        );

        let svc = FlightServiceServer::new(FlightSqlServiceImpl::new(instance, authenticator));

        println!("Listening on {:?}", addr);

        let server = Server::builder().add_service(svc).serve(addr);

        let _handle = tokio::spawn(server);
    }

    #[tokio::test]
    async fn test_client() {
        trace::init_default_global_tracing("/tmp", "test_rust.log", "info");

        run_test_server().await;

        let endpoint = Endpoint::from_static("http://localhost:8904");
        let mut client = FlightServiceClient::connect(endpoint)
            .await
            .expect("connect");

        // 1. handshake, basic authentication
        let mut req = Request::new(futures::stream::iter(vec![HandshakeRequest::default()]));
        req.metadata_mut().insert(
            AUTHORIZATION.as_str(),
            MetadataValue::from_static("Basic cm9vdDo="),
        );
        let resp = client.handshake(req).await.expect("handshake");
        println!("handshake resp: {:?}", resp.metadata());

        // 2. execute query, get result metadata
        let cmd = CommandStatementQuery {
            query: "select 1;".to_string(),
        };
        let any = Any::pack(&cmd).expect("pack");
        let fd = FlightDescriptor::new_cmd(any.encode_to_vec());
        let mut req = Request::new(fd);
        req.metadata_mut().insert(
            AUTHORIZATION.as_str(),
            resp.metadata().get(AUTHORIZATION.as_str()).unwrap().clone(),
        );
        let resp = client.get_flight_info(req).await.expect("get_flight_info");

        // 3. get result set
        let flight_info = resp.into_inner();
        let schema_ref =
            Arc::new(Schema::try_from(IpcMessage(flight_info.schema)).expect("Schema::try_from"));

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
                            println!("a schema when messages are read",);
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
                        _t => {
                            panic!("Reading types other than record batches not yet supported");
                        }
                    }
                }
            };
        }
    }

    async fn flight_channel(host: &str, port: u16) -> Channel {
        Endpoint::new(format!("http://{}:{}", host, port))
            .unwrap()
            .connect()
            .await
            .unwrap()
    }

    #[ignore]
    #[tokio::test]
    async fn test_flight_sql_client() {
        let channel = flight_channel("localhost", 8904).await;

        let mut client = FlightSqlServiceClient::new(channel);
        // <trace_id>:<span_id>:<parent_span_id>:<sampled>
        client.set_header("uber-trace-id", "114:1:2:1");

        // 1. handshake, basic authentication
        let _ = client.handshake("root", "").await.unwrap();

        // 2. execute query, get result metadata
        let mut stmt = client.prepare("select 1".into()).await.unwrap();
        let flight_info = stmt.execute().await.unwrap();

        let mut batches = vec![];
        for ep in &flight_info.endpoint {
            if let Some(tkt) = &ep.ticket {
                let stream = client.do_get(tkt.clone()).await.unwrap();
                let flight_data = stream.try_collect::<Vec<_>>().await.unwrap();
                batches.extend(flight_data_to_batches(&flight_data).unwrap());
            };
        }
    }
}
