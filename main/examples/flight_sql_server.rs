use std::pin::Pin;

use arrow_flight::flight_service_server::{FlightService, FlightServiceServer};
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
    Action, FlightData, FlightDescriptor, FlightInfo, HandshakeRequest, HandshakeResponse, Ticket,
};
use futures::Stream;
use tonic::transport::Server;
use tonic::{Request, Response, Status, Streaming};

#[derive(Clone)]
pub struct FlightSqlServiceImpl {}

#[tonic::async_trait]
impl FlightSqlService for FlightSqlServiceImpl {
    type FlightService = FlightSqlServiceImpl;

    async fn do_handshake(
        &self,
        request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<
        Response<Pin<Box<dyn Stream<Item = Result<HandshakeResponse, Status>> + Send>>>,
        Status,
    > {
        println!("do_handshake: {:?}", request);

        let basic = "Basic ";
        let authorization = request
            .metadata()
            .get("authorization")
            .ok_or_else(|| Status::invalid_argument("authorization field not present"))?
            .to_str()
            .map_err(|_| Status::invalid_argument("authorization not parsable"))?;
        if !authorization.starts_with(basic) {
            Err(Status::invalid_argument(format!(
                "Auth type not implemented: {}",
                authorization
            )))?;
        }
        let base64 = &authorization[basic.len()..];
        let bytes = base64::decode(base64)
            .map_err(|_| Status::invalid_argument("authorization not parsable"))?;
        let str = String::from_utf8(bytes)
            .map_err(|_| Status::invalid_argument("authorization not parsable"))?;
        let parts: Vec<_> = str.split(':').collect();
        if parts.len() != 2 {
            Err(Status::invalid_argument(
                "Invalid authorization header".to_string(),
            ))?;
        }
        let user = parts[0];
        let pass = parts[1];
        if user != "admin" || pass != "password" {
            Err(Status::unauthenticated("Invalid credentials!"))?
        }
        let result = HandshakeResponse {
            protocol_version: 0,
            payload: "random_uuid_token".as_bytes().into(),
        };
        let result = Ok(result);
        let output = futures::stream::iter(vec![result]);
        return Ok(Response::new(Box::pin(output)));
    }

    // get_flight_info
    async fn get_flight_info_statement(
        &self,
        query: CommandStatementQuery,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        println!(
            "get_flight_info_statement: query: {:?}, request: {:?}",
            query, request
        );

        Err(Status::unimplemented(
            "get_flight_info_statement not implemented",
        ))
    }

    async fn get_flight_info_prepared_statement(
        &self,
        query: CommandPreparedStatementQuery,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        println!(
            "get_flight_info_prepared_statement: query: {:?}, request: {:?}",
            query, request
        );

        Err(Status::unimplemented(
            "get_flight_info_prepared_statement not implemented",
        ))
    }

    async fn get_flight_info_catalogs(
        &self,
        query: CommandGetCatalogs,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        println!(
            "get_flight_info_catalogs: query: {:?}, request: {:?}",
            query, request
        );

        Err(Status::unimplemented(
            "get_flight_info_catalogs not implemented",
        ))
    }

    async fn get_flight_info_schemas(
        &self,
        query: CommandGetDbSchemas,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        println!(
            "get_flight_info_schemas: query: {:?}, request: {:?}",
            query, request
        );

        Err(Status::unimplemented(
            "get_flight_info_schemas not implemented",
        ))
    }

    async fn get_flight_info_tables(
        &self,
        query: CommandGetTables,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        println!(
            "get_flight_info_tables: query: {:?}, request: {:?}",
            query, request
        );

        Err(Status::unimplemented(
            "get_flight_info_tables not implemented",
        ))
    }

    async fn get_flight_info_table_types(
        &self,
        query: CommandGetTableTypes,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        println!(
            "get_flight_info_table_types: query: {:?}, request: {:?}",
            query, request
        );

        Err(Status::unimplemented(
            "get_flight_info_table_types not implemented",
        ))
    }

    async fn get_flight_info_sql_info(
        &self,
        query: CommandGetSqlInfo,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        println!(
            "get_flight_info_sql_info: query: {:?}, request: {:?}",
            query, request
        );

        Err(Status::unimplemented(
            "get_flight_info_sql_info not implemented",
        ))
    }

    async fn get_flight_info_primary_keys(
        &self,
        query: CommandGetPrimaryKeys,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        println!(
            "get_flight_info_primary_keys: query: {:?}, request: {:?}",
            query, request
        );

        Err(Status::unimplemented(
            "get_flight_info_primary_keys not implemented",
        ))
    }

    async fn get_flight_info_exported_keys(
        &self,
        query: CommandGetExportedKeys,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        println!(
            "get_flight_info_exported_keys: query: {:?}, request: {:?}",
            query, request
        );

        Err(Status::unimplemented(
            "get_flight_info_exported_keys not implemented",
        ))
    }

    async fn get_flight_info_imported_keys(
        &self,
        query: CommandGetImportedKeys,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        println!(
            "get_flight_info_imported_keys: query: {:?}, request: {:?}",
            query, request
        );

        Err(Status::unimplemented(
            "get_flight_info_imported_keys not implemented",
        ))
    }

    async fn get_flight_info_cross_reference(
        &self,
        query: CommandGetCrossReference,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        println!(
            "get_flight_info_cross_reference: query: {:?}, request: {:?}",
            query, request
        );

        Err(Status::unimplemented(
            "get_flight_info_imported_keys not implemented",
        ))
    }

    // do_get
    async fn do_get_statement(
        &self,
        ticket: TicketStatementQuery,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        println!(
            "do_get_statement: query: {:?}, request: {:?}",
            ticket, request
        );

        Err(Status::unimplemented("do_get_statement not implemented"))
    }

    async fn do_get_prepared_statement(
        &self,
        query: CommandPreparedStatementQuery,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        println!(
            "do_get_prepared_statement: query: {:?}, request: {:?}",
            query, request
        );

        Err(Status::unimplemented(
            "do_get_prepared_statement not implemented",
        ))
    }

    async fn do_get_catalogs(
        &self,
        query: CommandGetCatalogs,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        println!(
            "do_get_catalogs: query: {:?}, request: {:?}",
            query, request
        );

        Err(Status::unimplemented("do_get_catalogs not implemented"))
    }

    async fn do_get_schemas(
        &self,
        query: CommandGetDbSchemas,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        println!("do_get_schemas: query: {:?}, request: {:?}", query, request);

        Err(Status::unimplemented("do_get_schemas not implemented"))
    }

    async fn do_get_tables(
        &self,
        query: CommandGetTables,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        println!("do_get_tables: query: {:?}, request: {:?}", query, request);

        Err(Status::unimplemented("do_get_tables not implemented"))
    }

    async fn do_get_table_types(
        &self,
        query: CommandGetTableTypes,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        println!(
            "do_get_table_types: query: {:?}, request: {:?}",
            query, request
        );

        Err(Status::unimplemented("do_get_table_types not implemented"))
    }

    async fn do_get_sql_info(
        &self,
        query: CommandGetSqlInfo,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        println!(
            "do_get_sql_info: query: {:?}, request: {:?}",
            query, request
        );

        Err(Status::unimplemented("do_get_sql_info not implemented"))
    }

    async fn do_get_primary_keys(
        &self,
        query: CommandGetPrimaryKeys,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        println!(
            "do_get_primary_keys: query: {:?}, request: {:?}",
            query, request
        );

        Err(Status::unimplemented("do_get_primary_keys not implemented"))
    }

    async fn do_get_exported_keys(
        &self,
        query: CommandGetExportedKeys,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        println!(
            "do_get_exported_keys: query: {:?}, request: {:?}",
            query, request
        );

        Err(Status::unimplemented(
            "do_get_exported_keys not implemented",
        ))
    }

    async fn do_get_imported_keys(
        &self,
        _query: CommandGetImportedKeys,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        Err(Status::unimplemented(
            "do_get_imported_keys not implemented",
        ))
    }

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
    async fn do_put_statement_update(
        &self,
        ticket: CommandStatementUpdate,
        request: Request<Streaming<FlightData>>,
    ) -> Result<i64, Status> {
        println!(
            "do_put_statement_update: query: {:?}, request: {:?}",
            ticket, request
        );

        Err(Status::unimplemented(
            "do_put_statement_update not implemented",
        ))
    }

    async fn do_put_prepared_statement_query(
        &self,
        query: CommandPreparedStatementQuery,
        request: Request<Streaming<FlightData>>,
    ) -> Result<Response<<Self as FlightService>::DoPutStream>, Status> {
        println!(
            "do_put_prepared_statement_query: query: {:?}, request: {:?}",
            query, request
        );

        Err(Status::unimplemented(
            "do_put_prepared_statement_query not implemented",
        ))
    }

    async fn do_put_prepared_statement_update(
        &self,
        query: CommandPreparedStatementUpdate,
        request: Request<Streaming<FlightData>>,
    ) -> Result<i64, Status> {
        println!(
            "do_put_prepared_statement_update: query: {:?}, request: {:?}",
            query, request
        );

        Err(Status::unimplemented(
            "do_put_prepared_statement_update not implemented",
        ))
    }

    // do_action
    async fn do_action_create_prepared_statement(
        &self,
        query: ActionCreatePreparedStatementRequest,
        request: Request<Action>,
    ) -> Result<ActionCreatePreparedStatementResult, Status> {
        println!(
            "do_action_create_prepared_statement: query: {:?}, request: {:?}",
            query, request
        );

        Err(Status::unimplemented("Not yet implemented"))
    }
    async fn do_action_close_prepared_statement(
        &self,
        query: ActionClosePreparedStatementRequest,
        request: Request<Action>,
    ) {
        println!(
            "do_action_close_prepared_statement: query: {:?}, request: {:?}",
            query, request
        );

        unimplemented!("Not yet implemented")
    }

    async fn register_sql_info(&self, _id: i32, _result: &SqlInfo) {
        println!("register_sql_info: _id: {:?}, request: {:?}", _id, _result);
    }
}

/// This example shows how to run a FlightSql server
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "0.0.0.0:50051".parse()?;

    let svc = FlightServiceServer::new(FlightSqlServiceImpl {});

    println!("Listening on {:?}", addr);

    Server::builder().add_service(svc).serve(addr).await?;

    Ok(())
}
