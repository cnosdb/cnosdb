use std::{result, sync::Arc, time::Duration};

use async_trait::async_trait;
use datafusion::{
    arrow::{
        array::{ArrayRef, Float32Array, Int32Array, StringArray},
        datatypes::{DataType, Field, Schema, SchemaRef},
        error::ArrowError,
        record_batch::RecordBatch,
    },
    common::DataFusionError,
    datasource::{self, TableProvider},
    execution::context::{SessionState, TaskContext},
    logical_expr::TableType,
    logical_plan::{
        DFSchemaRef, LogicalPlan, LogicalPlanBuilder, ToDFSchema, UserDefinedLogicalNode,
    },
    physical_expr::{planner, PhysicalSortExpr},
    physical_plan::{
        filter::FilterExec, planner::ExtensionPlanner, ExecutionPlan, Partitioning,
        PhysicalPlanner, RecordBatchStream, SendableRecordBatchStream, Statistics,
    },
    prelude::*,
    scalar::ScalarValue,
};
use futures::Stream;
use tokio::time;

pub type Result<T> = result::Result<T, DataFusionError>;

#[derive(Debug)]
pub struct Column {
    name: String,
    data_type: DataType,
}

#[derive(Debug)]
pub struct Table {}

impl Table {
    fn test_columns() -> Vec<Column> {
        vec![Column { name: "fa".to_string(), data_type: DataType::Int32 },
             Column { name: "fb".to_string(), data_type: DataType::Utf8 },
             Column { name: "fc".to_string(), data_type: DataType::Float32 },]
    }

    fn test_data() -> Vec<ArrayRef> {
        vec![Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5])),
             Arc::new(StringArray::from(vec!["Apple",
                                             "Blackberry",
                                             "Coconut",
                                             "Durian",
                                             "Elderberry"])),
             Arc::new(Float32Array::from(vec![1.0, 2.0, 3.0, 4.0, 5.0])),]
    }

    fn test_schema() -> SchemaRef {
        Arc::new(Schema::new(Table::test_columns().iter()
                                                  .map(|c| {
                                                      Field::new(&c.name, c.data_type.clone(), true)
                                                  })
                                                  .collect()))
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

    /// Create an ExecutionPlan that will scan the table.
    /// The table provider will be usually responsible of grouping
    /// the source data into partitions that can be efficiently
    /// parallelized or distributed.
    async fn scan(&self,
                  ctx: &SessionState,
                  _projection: &Option<Vec<usize>>,
                  filters: &[Expr],
                  _limit: Option<usize>)
                  -> Result<Arc<dyn ExecutionPlan>> {
        let table_scan_exec =
            TableScanExec { columns: Table::test_columns(), data: Arc::new(Table::test_data()) };
        let schema = Table::test_schema();
        let df_schema = schema.clone().to_dfschema_ref()?;

        if filters.len() > 0 {
            let predicate = planner::create_physical_expr(&filters[0],
                                                          df_schema.as_ref(),
                                                          schema.as_ref(),
                                                          &ctx.execution_props)?;
            let filter = FilterExec::try_new(predicate, Arc::new(table_scan_exec))?;
            return Ok(Arc::new(filter));
        } else {
            return Ok(Arc::new(table_scan_exec));
        }
    }
}

pub struct TableScanStream {
    data: Vec<RecordBatch>,
    schema: SchemaRef,
    index: usize,
}

impl TableScanStream {
    pub fn new(data: Vec<RecordBatch>, schema: SchemaRef) -> Self {
        Self { data, schema, index: 0 }
    }
}

type ArrowResult<T> = std::result::Result<T, ArrowError>;

impl Stream for TableScanStream {
    type Item = ArrowResult<RecordBatch>;

    fn poll_next(mut self: std::pin::Pin<&mut Self>,
                 _cx: &mut std::task::Context<'_>)
                 -> std::task::Poll<Option<Self::Item>> {
        std::task::Poll::Ready(if self.index < self.data.len() {
                                   self.index += 1;
                                   let batch = &self.data[self.index - 1];

                                   Some(Ok(batch.clone()))
                               } else {
                                   None
                               })
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.data.len(), Some(self.data.len()))
    }
}

impl RecordBatchStream for TableScanStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

#[derive(Debug)]
pub struct TableScanExec {
    columns: Vec<Column>,
    data: Arc<Vec<ArrayRef>>,
}

impl ExecutionPlan for TableScanExec {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::new(Schema::new(self.columns
                                 .iter()
                                 .map(|c| Field::new(&c.name, c.data_type.clone(), true))
                                 .collect()))
    }

    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(1)
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        None
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(self: Arc<Self>,
                         _children: Vec<Arc<dyn ExecutionPlan>>)
                         -> Result<Arc<dyn ExecutionPlan>> {
        Err(DataFusionError::Internal(format!("Children cannot be replacd in {:?}", self,)))
    }

    fn execute(&self,
               _partition: usize,
               _context: Arc<TaskContext>)
               -> Result<SendableRecordBatchStream> {
        let batch = RecordBatch::try_new(self.schema(), self.data.to_vec())?;

        Ok(Box::pin(TableScanStream::new(vec![batch], self.schema())))
    }

    fn statistics(&self) -> Statistics {
        Statistics { num_rows: None,
                     total_byte_size: None,
                     column_statistics: None,
                     is_exact: false }
    }
}

#[derive(Debug)]
pub struct TableScanPlan {
    schema: SchemaRef,
    data: Arc<Vec<ArrayRef>>,
}

impl ExecutionPlan for TableScanPlan {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(1)
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        None
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(self: Arc<Self>,
                         _children: Vec<Arc<dyn ExecutionPlan>>)
                         -> Result<Arc<dyn ExecutionPlan>> {
        Err(DataFusionError::Internal(format!("Children cannot be replaced in {:?}", self)))
    }

    fn execute(&self,
               _partition: usize,
               _context: Arc<TaskContext>)
               -> Result<SendableRecordBatchStream> {
        let batch = RecordBatch::try_new(self.schema.clone(), self.data.to_vec())?;
        Ok(Box::pin(TableScanStream::new(vec![batch], self.schema.clone())))
    }

    fn statistics(&self) -> Statistics {
        Statistics { num_rows: None,
                     total_byte_size: None,
                     column_statistics: None,
                     is_exact: false }
    }
}

#[derive(Debug)]
pub struct TableScanNode {
    schema: DFSchemaRef,
}

impl UserDefinedLogicalNode for TableScanNode {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<Expr> {
        todo!()
    }

    fn fmt_for_explain(&self, _f: &mut std::fmt::Formatter) -> std::fmt::Result {
        todo!()
    }

    fn from_template(&self,
                     _exprs: &[Expr],
                     _inputs: &[datafusion::logical_plan::LogicalPlan])
                     -> Arc<dyn UserDefinedLogicalNode + Send + Sync> {
        todo!()
    }

    fn prevent_predicate_push_down_columns(&self) -> std::collections::HashSet<String> {
        // default (safe) is all columns in the schema.
        self.schema().fields().iter().map(|f| f.name().clone()).collect()
    }
}

pub struct TableScanPlanner {}

impl ExtensionPlanner for TableScanPlanner {
    fn plan_extension(&self,
                      _planner: &dyn PhysicalPlanner,
                      node: &dyn UserDefinedLogicalNode,
                      _logical_inputs: &[&LogicalPlan],
                      _physical_inputs: &[Arc<dyn ExecutionPlan>],
                      _session_state: &SessionState)
                      -> Result<Option<Arc<dyn ExecutionPlan>>> {
        dbg!(node);
        Ok(Some(Arc::new(TableScanPlan { schema: Table::test_schema(),
                                         data: Arc::new(Table::test_data()) })))
    }
}

#[tokio::test]
async fn test_dataframe() {
    let table = Arc::new(Table {});

    let ctx = SessionContext::new();
    ctx.register_table("tbl", table.clone()).unwrap();

    let logical_plan = LogicalPlanBuilder::scan_with_filters(
        "tbl",
        datasource::provider_as_source(table),
        None,
        vec![col("fa").gt_eq(Expr::Literal(ScalarValue::Int32(Some(2))))],
    )
    .unwrap()
    .build()
    .unwrap();

    let dataframe = DataFrame::new(ctx.state, &logical_plan).select_columns(&["fa", "fb"]).unwrap();

    time::timeout(Duration::from_secs(10), async move {
        let result = dataframe.collect().await.unwrap();
        let record_batch = result.get(0).unwrap();

        dbg!(record_batch.columns());
    }).await
      .unwrap();
}

#[tokio::test]
async fn test_physical_planner() {
    // let physical_planner = DefaultPhysicalPlanner::with_extension_planners(vec![Arc::new(data)]);
}
