use std::collections::HashMap;

use protos::schema_service::{
    schema_service_server::SchemaService, Database, GetDatabaseRequest, GetDatabaseResponse,
};
// use query::schema::SchemaStore;
use tonic::{Request, Response, Status};

pub struct SchemaServiceImpl {
    // schema_store: Arc<Mutex<SchemaStore>>,
}

#[tonic::async_trait]
impl SchemaService for SchemaServiceImpl {
    async fn get_database(
        &self,
        request: Request<GetDatabaseRequest>,
    ) -> Result<Response<GetDatabaseResponse>, Status> {
        let req = request.into_inner();
        dbg!(req);
        // let schema_store = self.schema_store.lock();
        // let db = schema_store.get_database(&req.database).await;

        let db = Database {
            id: 1,
            name: "dba".to_string(),
            tables: HashMap::new(),
        };
        Ok(Response::new(GetDatabaseResponse { database: Some(db) }))
    }
}
