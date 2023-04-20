use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::{Float32Array, Float64Array};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::from_slice::FromSlice;
use models::auth::role::UserRole;
use models::auth::user::{User, UserDesc, UserInfo, UserOptionsBuilder};
use trace::SpanContext;

use crate::query::execution::{Output, QueryStateMachine, QueryStateMachineRef};
use crate::query::logical_planner::Plan;
use crate::query::recordbatch::RecordBatchStreamWrapper;
use crate::service::protocol::{Query, QueryHandle, QueryId};
use crate::Result;

pub type DBMSRef = Arc<dyn DatabaseManagerSystem + Send + Sync>;

#[async_trait]
pub trait DatabaseManagerSystem {
    async fn start(&self) -> Result<()>;
    async fn authenticate(&self, user_info: &UserInfo, tenant_name: Option<&str>) -> Result<User>;
    async fn execute(&self, query: &Query) -> Result<QueryHandle>;
    async fn build_query_state_machine(&self, query: Query) -> Result<QueryStateMachineRef>;
    async fn build_logical_plan(
        &self,
        query_state_machine: QueryStateMachineRef,
    ) -> Result<Option<Plan>>;
    async fn execute_logical_plan(
        &self,
        logical_plan: Plan,
        query_state_machine: QueryStateMachineRef,
    ) -> Result<QueryHandle>;
    fn metrics(&self) -> String;
    fn cancel(&self, query_id: &QueryId);
}

pub struct DatabaseManagerSystemMock {}

#[async_trait]
impl DatabaseManagerSystem for DatabaseManagerSystemMock {
    async fn start(&self) -> Result<()> {
        Ok(())
    }
    async fn authenticate(&self, user_info: &UserInfo, _tenant_name: Option<&str>) -> Result<User> {
        let options = unsafe {
            UserOptionsBuilder::default()
                .password(user_info.password.to_string())
                .build()
                .unwrap_unchecked()
        };
        let mock_desc = UserDesc::new(0_u128, user_info.user.to_string(), options, true);
        let mock_user = User::new(mock_desc, UserRole::Dba.to_privileges());
        Ok(mock_user)
    }

    async fn execute(
        &self,
        query: &Query,
        _span_context: Option<SpanContext>,
    ) -> Result<QueryHandle> {
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
        let stream = Box::pin(RecordBatchStreamWrapper::new(schema, batches));
        Ok(QueryHandle::new(
            QueryId::next_id(),
            query.clone(),
            Output::StreamData(stream),
        ))
    }

    async fn build_query_state_machine(&self, query: Query) -> Result<QueryStateMachineRef> {
        Ok(Arc::new(QueryStateMachine::test(query)))
    }

    async fn build_logical_plan(
        &self,
        _query_state_machine: QueryStateMachineRef,
    ) -> Result<Option<Plan>> {
        Ok(None)
    }

    async fn execute_logical_plan(
        &self,
        _logical_plan: Plan,
        query_state_machine: QueryStateMachineRef,
    ) -> Result<QueryHandle> {
        self.execute(&query_state_machine.query).await
    }

    fn metrics(&self) -> String {
        "todo!()".to_string()
    }

    fn cancel(&self, query_id: &QueryId) {
        println!("DatabaseManagerSystemMock::cancel({:?})", query_id);
    }
}
