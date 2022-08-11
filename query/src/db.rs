use std::sync::Arc;

use datafusion::arrow::record_batch::RecordBatch;
use futures::TryStreamExt;
use parking_lot::RwLock;
use tskv::engine::EngineRef;

use crate::{
    catalog::UserCatalog,
    context::IsiphoSessionCtx,
    exec::{Executor, ExecutorType},
};

#[derive(Debug, Eq, PartialEq, Clone)]
pub struct DatabaseRule {
    pub name: String,
    // TODO: place rule
}
impl DatabaseRule {
    pub fn new(name: String) -> Self {
        Self { name }
    }

    pub fn db_name(&self) -> &str {
        self.name.as_str()
    }
}

pub struct Db {
    rule: RwLock<Arc<DatabaseRule>>,
    exec: Arc<Executor>,
    catalog: Arc<UserCatalog>,
}

impl Db {
    pub fn new(engine: EngineRef) -> Self {
        Self {
            rule: RwLock::new(Arc::new(DatabaseRule::new("default".to_string()))),
            exec: Arc::new(Executor::new(1)), //todo: add config
            catalog: Arc::new(UserCatalog::new(engine)),
        }
    }
}

impl Db {
    fn new_query_context(&self) -> IsiphoSessionCtx {
        self.exec
            .new_execution_config(ExecutorType::Query)
            .with_default_catalog(Arc::clone(&self.catalog) as _)
            .build()
    }
    pub async fn run_query(&mut self, query: &str) -> Option<Vec<RecordBatch>> {
        let ctx = self.new_query_context();
        let task = ctx.inner().task_ctx();
        let frame = ctx.inner().sql(query).await.unwrap();
        let plan = frame.create_physical_plan().await.unwrap();
        let stream = self.exec.run(plan, task).unwrap().stream();
        let result: Vec<_> = stream.try_collect().await.ok()?;
        Some(result)
    }
}

#[cfg(test)]
mod tests {
    use std::{ops::Range, sync::Arc};

    use async_trait::async_trait;
    use chrono::Utc;
    use datafusion::{
        arrow::{
            array::{ArrayRef, PrimitiveArray},
            datatypes::{
                ArrowPrimitiveType, DataType, Field, Float64Type, Int32Type, Schema, SchemaRef,
            },
            record_batch::RecordBatch,
            util::pretty::pretty_format_batches,
        },
        catalog::{catalog::CatalogProvider, schema::SchemaProvider},
        datasource::{MemTable, TableProvider, TableType},
        error::Result,
        execution::context::SessionState,
        logical_expr::TableProviderFilterPushDown,
        physical_plan::{empty::EmptyExec, ExecutionPlan},
        prelude::{Expr, SessionContext},
        scheduler::Scheduler,
    };
    use futures::TryStreamExt;
    use rand::{distributions::uniform::SampleUniform, thread_rng, Rng};
    use config::get_config;
    use tskv::{kv_option, TsKv};

    use crate::{catalog::IsiphoSchema, db::Db};

    #[derive(Debug)]
    pub struct Column {
        name: String,
        data_type: DataType,
    }

    #[derive(Debug)]
    pub struct Table {}

    impl Table {
        fn test_columns() -> Vec<Column> {
            vec![
                Column {
                    name: "fa".to_string(),
                    data_type: DataType::Int32,
                },
                Column {
                    name: "fb".to_string(),
                    data_type: DataType::Int32,
                },
                Column {
                    name: "fc".to_string(),
                    data_type: DataType::Float32,
                },
            ]
        }

        fn test_schema() -> SchemaRef {
            Arc::new(Schema::new(
                Table::test_columns()
                    .iter()
                    .map(|c| Field::new(&c.name, c.data_type.clone(), true))
                    .collect(),
            ))
        }
    }

    #[async_trait]
    impl TableProvider for Table {
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

        fn schema(&self) -> SchemaRef {
            Table::test_schema()
        }

        fn table_type(&self) -> TableType {
            TableType::Base
        }

        async fn scan(
            &self,
            _ctx: &SessionState,
            projection: &Option<Vec<usize>>,
            filters: &[Expr],
            limit: Option<usize>,
        ) -> Result<Arc<dyn ExecutionPlan>> {
            println!("projection: {:?}", projection);
            println!("filters: {:?}", filters);
            println!("limit: {:?}", limit);
            let empty = EmptyExec::new(false, self.schema());
            return Ok(Arc::new(empty));
        }
        fn supports_filter_pushdown(&self, _filter: &Expr) -> Result<TableProviderFilterPushDown> {
            Ok(TableProviderFilterPushDown::Exact)
        }
    }

    const BATCHES_PER_PARTITION: usize = 20;
    const ROWS_PER_BATCH: usize = 100;
    const NUM_PARTITIONS: usize = 2;

    fn generate_primitive<T, R>(
        rng: &mut R,
        len: usize,
        valid_percent: f64,
        range: Range<T::Native>,
    ) -> ArrayRef
    where
        T: ArrowPrimitiveType,
        T::Native: SampleUniform,
        R: Rng,
    {
        Arc::new(PrimitiveArray::<T>::from_iter((0..len).map(|_| {
            rng.gen_bool(valid_percent)
                .then(|| rng.gen_range(range.clone()))
        })))
    }

    fn generate_batch<R: Rng>(rng: &mut R, row_count: usize, id_offset: i32) -> RecordBatch {
        let id_range = id_offset..(row_count as i32 + id_offset);
        let a = generate_primitive::<Int32Type, _>(rng, row_count, 0.5, 0..1000);
        let b = generate_primitive::<Float64Type, _>(rng, row_count, 0.5, 0. ..1000.);
        let id = PrimitiveArray::<Int32Type>::from_iter_values(id_range);

        RecordBatch::try_from_iter_with_nullable([
            ("a", a, true),
            ("b", b, true),
            ("id", Arc::new(id), false),
        ])
        .unwrap()
    }
    fn make_batches() -> Vec<Vec<RecordBatch>> {
        let mut rng = thread_rng();

        let mut id_offset = 0;

        (0..NUM_PARTITIONS)
            .map(|_| {
                (0..BATCHES_PER_PARTITION)
                    .map(|_| {
                        let batch = generate_batch(&mut rng, ROWS_PER_BATCH, id_offset);
                        id_offset += ROWS_PER_BATCH as i32;
                        batch
                    })
                    .collect()
            })
            .collect()
    }

    fn make_provider() -> Arc<dyn TableProvider> {
        let batches = make_batches();
        let schema = batches.first().unwrap().first().unwrap().schema();
        Arc::new(MemTable::try_new(schema, make_batches()).unwrap())
    }

    async fn get_tskv() -> TsKv {
        let mut global_config = (*get_config("../config/config.toml")).clone();
        global_config.wal_config_dir = "/tmp/test/wal".to_string();
        let opt = kv_option::Options::from(&global_config);

        TsKv::open(opt, global_config.tsfamily_num).await.unwrap()
    }

    async fn run_query(db: Arc<Db>) {
        // 1    44882000
        // 2    23680000
        // 3    18857000
        // 4    16241000
        // 5    16445000

        let tskv = get_tskv().await;

        let scheduler = Scheduler::new(4);
        let ctx = db.new_query_context();

        // todo： init tables
        let table = Arc::new(Table {});
        let schema = Arc::new(IsiphoSchema::new(Arc::new(tskv)));
        schema.register_table("table1".to_string(), table).unwrap();
        db.catalog
            .register_schema("public", schema.clone())
            .unwrap();

        let query = "select distinct * from cnosdb.public.table1 where fb > 1 order by fa limit 1";
        let task = ctx.inner().task_ctx();
        let frame = ctx.inner().sql(query).await.unwrap();

        let plan = frame.create_physical_plan().await.unwrap();
        let start = Utc::now();
        let stream = scheduler.schedule(plan, task).unwrap().stream();

        let scheduled: Vec<_> = stream.try_collect().await.unwrap();
        let end = Utc::now();
        println!("run time: {:?} ", end - start);
        let scheduled = pretty_format_batches(&scheduled).unwrap().to_string();
        println!("{}", scheduled);
    }

    #[tokio::test]
    #[ignore]
    async fn query_exec() {
        let tskv = get_tskv().await;
        let db = Arc::new(Db::new(Arc::new(tskv)));
        run_query(db).await;
    }

    /// use dafault session context
    async fn run_query1(db: Arc<Db>) {
        let scheduler = Scheduler::new(4);
        let mut ctx = db.new_query_context();

        let context = SessionContext::new();
        ctx.set_cxt(context);
        ctx.inner()
            .register_table("table1", make_provider())
            .unwrap();

        let query = "select distinct * from table1 where b > 100 order by a limit 1";
        let task = ctx.inner().task_ctx();
        let frame = ctx.inner().sql(query).await.unwrap();

        let plan = frame.create_physical_plan().await.unwrap();
        let start = Utc::now();
        let stream = scheduler.schedule(plan, task).unwrap().stream();

        let scheduled: Vec<_> = stream.try_collect().await.unwrap();
        let end = Utc::now();
        println!("run time: {:?} ", end - start);
        let scheduled = pretty_format_batches(&scheduled).unwrap().to_string();
        println!("{}", scheduled);
    }
    #[tokio::test]
    #[ignore]
    async fn basic_query() {
        let tskv = get_tskv().await;
        let db = Arc::new(Db::new(Arc::new(tskv)));
        run_query1(db).await;
    }
}
