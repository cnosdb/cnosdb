use arrow_flight::sql::{ActionCreatePreparedStatementResult, SqlInfo};
use arrow_flight::{Action, FlightData, HandshakeRequest, HandshakeResponse, Ticket};
use chrono::format::Item;
use futures::Stream;
use http_protocol::header::{AUTHORIZATION, BASIC_PREFIX};
use query::dispatcher::manager::SimpleQueryDispatcher;
use query::instance::Cnosdbms;
use spi::query::dispatcher::QueryDispatcher;
use spi::server::dbms::{DatabaseManagerSystem, DatabaseManagerSystemMock};
use spi::service::protocol::{Context, ContextBuilder, Query};
use std::pin::Pin;
use std::sync::Arc;
use tonic::transport::Server;
use tonic::{Request, Response, Status, Streaming};

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
    FlightDescriptor, FlightInfo,
};

use crate::flight_sql::utils;
use crate::http::header::Header;

#[derive(Clone)]
pub struct FlightSqlServiceImpl<D> {
    instance: Arc<D>,
}

#[tonic::async_trait]
impl<D> FlightSqlService for FlightSqlServiceImpl<D>
where
    D: DatabaseManagerSystem + Send + Sync + 'static,
{
    type FlightService = FlightSqlServiceImpl<D>;

    /// TODO support
    async fn do_handshake(
        &self,
        request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<
        Response<Pin<Box<dyn Stream<Item = Result<HandshakeResponse, Status>> + Send>>>,
        Status,
    > {
        println!("do_handshake: {:?}", request);

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
        println!(
            "get_flight_info_statement: query: {:?}, request: {:?}",
            query, request
        );

        // TODO 执行发送的sql
        let user_info =
            utils::parse_user_info(&request).map_err(|e| Status::invalid_argument(e))?;

        let sql = query.query;
        let ctx = ContextBuilder::new(user_info).build();
        let query = Query::new(ctx, sql);
        let _xx = self.instance.execute(&query).await;

        Err(Status::unimplemented(
            "get_flight_info_statement not implemented",
        ))
    }

    /// not support
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

    /// TODO support
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

    /// TODO support
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

    /// TODO support
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

    /// TODO support
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

    /// not support
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

    /// not support
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

    /// not support
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

    /// not support
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

    /// not support
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
    /// TODO support
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

    /// not support
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

    /// TODO support
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

    /// TODO support
    async fn do_get_schemas(
        &self,
        query: CommandGetDbSchemas,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        println!("do_get_schemas: query: {:?}, request: {:?}", query, request);

        Err(Status::unimplemented("do_get_schemas not implemented"))
    }

    /// TODO support
    async fn do_get_tables(
        &self,
        query: CommandGetTables,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        println!("do_get_tables: query: {:?}, request: {:?}", query, request);

        Err(Status::unimplemented("do_get_tables not implemented"))
    }

    /// TODO support
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

    /// not support
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

    /// not support
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

    /// not support
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
        println!(
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
        println!(
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
        println!(
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
        println!(
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
        println!(
            "do_action_close_prepared_statement: query: {:?}, request: {:?}",
            query, request
        );

        unimplemented!("Not yet implemented")
    }

    /// not support
    async fn register_sql_info(&self, _id: i32, _result: &SqlInfo) {
        println!("register_sql_info: _id: {:?}, request: {:?}", _id, _result);
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use arrow_flight::{
        flight_service_client::FlightServiceClient,
        flight_service_server::FlightServiceServer,
        sql::{CommandStatementQuery, ProstAnyExt},
        FlightDescriptor, HandshakeRequest,
    };
    use http_protocol::header::AUTHORIZATION;
    use prost::Message;
    use spi::server::dbms::DatabaseManagerSystemMock;
    use tokio::time;
    use tonic::{
        client::Grpc,
        metadata::{AsciiMetadataValue, KeyAndValueRef, MetadataValue},
        transport::{Channel, Endpoint, Server},
        Code, Request, Streaming,
    };

    use crate::flight_sql::flight_sql_server::FlightSqlServiceImpl;

    #[tokio::test]
    #[ignore]
    async fn test_server() {
        let addr = "0.0.0.0:50051".parse().expect("parse address");

        let svc = FlightServiceServer::new(FlightSqlServiceImpl {
            instance: Arc::new(DatabaseManagerSystemMock {}),
        });

        println!("Listening on {:?}", addr);

        Server::builder()
            .add_service(svc)
            .serve(addr)
            .await
            .expect("start server");
    }

    #[tokio::test]
    #[ignore]
    async fn test_client() {
        let endpoint = Endpoint::from_static("http://0.0.0.0:50051");
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

        let _resp = client.handshake(req).await.expect("handshake");

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

        let resp = client.get_flight_info(req).await;

        match resp {
            Ok(x) => {
                panic!("TODO support")
            }
            Err(err) => assert_eq!(err.code(), Code::Unimplemented),
        }
    }
}
