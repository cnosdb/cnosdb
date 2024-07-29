use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::{Float32Array, Float64Array};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use models::auth::role::UserRole;
use models::auth::user::{User, UserDesc, UserInfo, UserOptionsBuilder};
use models::schema::query_info::QueryId;
use trace::span_ext::SpanExt;
use trace::SpanContext;

use crate::query::execution::{Output, QueryStateMachine, QueryStateMachineRef};
use crate::query::logical_planner::Plan;
use crate::query::recordbatch::RecordBatchStreamWrapper;
use crate::service::protocol::{Query, QueryHandle};
use crate::QueryResult;

pub type DBMSRef = Arc<dyn DatabaseManagerSystem + Send + Sync>;

#[async_trait]
pub trait DatabaseManagerSystem {
    async fn start(&self) -> QueryResult<()>;
    async fn authenticate(&self, user_info: &UserInfo, tenant_name: &str) -> QueryResult<User>;
    async fn execute(
        &self,
        query: &Query,
        span_context: Option<&SpanContext>,
    ) -> QueryResult<QueryHandle>;
    async fn build_query_state_machine(
        &self,
        query: Query,
        span_context: Option<&SpanContext>,
    ) -> QueryResult<QueryStateMachineRef>;
    async fn build_logical_plan(
        &self,
        query_state_machine: QueryStateMachineRef,
    ) -> QueryResult<Option<Plan>>;
    async fn execute_logical_plan(
        &self,
        logical_plan: Plan,
        query_state_machine: QueryStateMachineRef,
    ) -> QueryResult<QueryHandle>;
    fn metrics(&self) -> String;
    fn cancel(&self, query_id: &QueryId);
}

pub struct DatabaseManagerSystemMock {}

#[async_trait]
impl DatabaseManagerSystem for DatabaseManagerSystemMock {
    async fn start(&self) -> QueryResult<()> {
        Ok(())
    }
    async fn authenticate(&self, user_info: &UserInfo, _tenant_name: &str) -> QueryResult<User> {
        let options = unsafe {
            UserOptionsBuilder::default()
                .password(user_info.password.clone())
                .unwrap_unchecked()
                .build()
                .unwrap_unchecked()
        };
        let mock_desc = UserDesc::new(0_u128, user_info.user.to_string(), options, true);
        let mock_user = User::new(mock_desc, UserRole::Dba.to_privileges(), None);
        Ok(mock_user)
    }

    async fn execute(
        &self,
        query: &Query,
        _span_context: Option<&SpanContext>,
    ) -> QueryResult<QueryHandle> {
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
                        Arc::new(Float32Array::from(vec![i as f32; batch_size])),
                        Arc::new(Float64Array::from(vec![i as f64; batch_size])),
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

    async fn build_query_state_machine(
        &self,
        query: Query,
        span_context: Option<&SpanContext>,
    ) -> QueryResult<QueryStateMachineRef> {
        Ok(Arc::new(QueryStateMachine::test(
            query,
            span_context.cloned(),
        )))
    }

    async fn build_logical_plan(
        &self,
        _query_state_machine: QueryStateMachineRef,
    ) -> QueryResult<Option<Plan>> {
        Ok(None)
    }

    async fn execute_logical_plan(
        &self,
        _logical_plan: Plan,
        query_state_machine: QueryStateMachineRef,
    ) -> QueryResult<QueryHandle> {
        self.execute(
            &query_state_machine.query,
            query_state_machine
                .session
                .get_child_span("mock execute logical plan")
                .context()
                .as_ref(),
        )
        .await
    }

    fn metrics(&self) -> String {
        "todo!()".to_string()
    }

    fn cancel(&self, query_id: &QueryId) {
        println!("DatabaseManagerSystemMock::cancel({:?})", query_id);
    }
}
