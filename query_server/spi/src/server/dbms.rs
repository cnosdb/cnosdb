use std::sync::Arc;

use async_trait::async_trait;
use datafusion::{
    arrow::{
        array::{Float32Array, Float64Array},
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    },
    from_slice::FromSlice,
};
use models::auth::{
    role::UserRole,
    user::{User, UserDesc, UserInfo, UserOptionsBuilder},
};

use crate::{
    query::execution::Output,
    service::protocol::{Query, QueryHandle, QueryId},
};

use super::Result;

pub type DBMSRef = Arc<dyn DatabaseManagerSystem + Send + Sync>;

#[async_trait]
pub trait DatabaseManagerSystem {
    fn authenticate(&self, user_info: &UserInfo) -> Result<User>;
    async fn execute(&self, query: &Query) -> Result<QueryHandle>;
    fn metrics(&self) -> String;
    fn cancel(&self, query_id: &QueryId);
}

pub struct DatabaseManagerSystemMock {}

#[async_trait]
impl DatabaseManagerSystem for DatabaseManagerSystemMock {
    fn authenticate(&self, user_info: &UserInfo) -> Result<User> {
        let options = unsafe {
            UserOptionsBuilder::default()
                .password(user_info.password.to_string())
                .build()
                .unwrap_unchecked()
        };
        let mock_desc = UserDesc::new(0_u128, user_info.user.to_string(), options);
        let mock_user = User::new(mock_desc, UserRole::Dba.to_privileges());
        Ok(mock_user)
    }

    async fn execute(&self, query: &Query) -> Result<QueryHandle> {
        println!("DatabaseManagerSystemMock::execute({:?})", query.content());

        // define a schema.
        let schema = Arc::new(Schema::new(vec![
            Field::new("f32", DataType::Float32, false),
            Field::new("f64", DataType::Float64, false),
        ]));

        // define data.
        let batch_size = 2;
        let batches = (0..10 / batch_size)
            .map(|i| {
                RecordBatch::try_new(
                    schema.clone(),
                    vec![
                        Arc::new(Float32Array::from_slice(vec![i as f32; batch_size])),
                        Arc::new(Float64Array::from_slice(vec![i as f64; batch_size])),
                    ],
                )
                .unwrap()
            })
            .collect::<Vec<_>>();

        Ok(QueryHandle::new(
            QueryId::next_id(),
            query.clone(),
            Output::StreamData(schema, batches),
        ))
    }

    fn metrics(&self) -> String {
        "todo!()".to_string()
    }

    fn cancel(&self, query_id: &QueryId) {
        println!("DatabaseManagerSystemMock::cancel({:?})", query_id);
    }
}
