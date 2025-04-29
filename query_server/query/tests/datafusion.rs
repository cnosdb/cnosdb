use std::result;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use datafusion::arrow::array::{ArrayRef, Float32Array, Int32Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::Session;
use datafusion::common::{DFSchemaRef, DataFusionError, ToDFSchema};
use datafusion::datasource::{self, TableProvider};
use datafusion::execution::context::TaskContext;
use datafusion::logical_expr::logical_plan::TableScanAggregate;
use datafusion::logical_expr::{
    LogicalPlan, LogicalPlanBuilder, TableType, UserDefinedLogicalNodeCore,
};
use datafusion::physical_expr::{planner, EquivalenceProperties};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties, RecordBatchStream,
    SendableRecordBatchStream, Statistics,
};
use datafusion::prelude::*;
use datafusion::scalar::ScalarValue;
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
        vec![
            Column {
                name: "fa".to_string(),
                data_type: DataType::Int32,
            },
            Column {
                name: "fb".to_string(),
                data_type: DataType::Utf8,
            },
            Column {
                name: "fc".to_string(),
                data_type: DataType::Float32,
            },
        ]
    }

    fn test_data() -> Vec<ArrayRef> {
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5])),
            Arc::new(StringArray::from(vec![
                "Apple",
                "Blackberry",
                "Coconut",
                "Durian",
                "Elderberry",
            ])),
            Arc::new(Float32Array::from(vec![1.0, 2.0, 3.0, 4.0, 5.0])),
        ]
    }

    fn test_schema() -> SchemaRef {
        Arc::new(Schema::new(
            Table::test_columns()
                .iter()
                .map(|c| Field::new(&c.name, c.data_type.clone(), true))
                .collect::<Vec<_>>(),
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

    /// Create an ExecutionPlan that will scan the table.
    /// The table provider will be usually responsible of grouping
    /// the source data into partitions that can be efficiently
    /// parallelized or distributed.
    async fn scan(
        &self,
        state: &dyn Session,
        _projection: Option<&Vec<usize>>,
        filters: &[Expr],
        _: Option<&TableScanAggregate>,
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let schema = Table::test_schema();
        let df_schema = schema.clone().to_dfschema_ref()?;

        let table_scan_exec = TableScanExec {
            properties: PlanProperties::new(
                EquivalenceProperties::new(schema),
                Partitioning::UnknownPartitioning(1),
                EmissionType::Incremental,
                Boundedness::Bounded,
            ),
            columns: Table::test_columns(),
            data: Arc::new(Table::test_data()),
        };
        // todo: sid, timerange
        if !filters.is_empty() {
            let predicate = planner::create_physical_expr(
                &filters[0],
                df_schema.as_ref(),
                state.execution_props(),
            )?;
            let filter = FilterExec::try_new(predicate, Arc::new(table_scan_exec))?;
            Ok(Arc::new(filter))
        } else {
            Ok(Arc::new(table_scan_exec))
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
        Self {
            data,
            schema,
            index: 0,
        }
    }
}

impl Stream for TableScanStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
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
    properties: PlanProperties,
    columns: Vec<Column>,
    data: Arc<Vec<ArrayRef>>,
}

impl ExecutionPlan for TableScanExec {
    fn name(&self) -> &str {
        "TableScanExec"
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::new(Schema::new(
            self.columns
                .iter()
                .map(|c| Field::new(&c.name, c.data_type.clone(), true))
                .collect::<Vec<_>>(),
        ))
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Err(DataFusionError::Internal(format!(
            "Children cannot be replaced in {:?}",
            self,
        )))
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let batch = RecordBatch::try_new(self.schema(), self.data.to_vec())?;

        Ok(Box::pin(TableScanStream::new(vec![batch], self.schema())))
    }

    fn statistics(&self) -> Result<Statistics> {
        Ok(Statistics::default())
    }
}

impl DisplayAs for TableScanExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "TableScanExec: columns=[[")?;
                for (i, c) in self.columns.iter().enumerate() {
                    write!(f, "{}", c.name)?;
                    if i < self.columns.len() - 1 {
                        write!(f, ", ")?;
                    }
                }
                write!(f, "]]")
            }
            DisplayFormatType::TreeRender => {
                // TODO(zipper): implement this.
                write!(f, "")
            }
        }
    }
}

#[derive(Debug)]
pub struct TableScanPlan {
    properties: PlanProperties,
    schema: SchemaRef,
    data: Arc<Vec<ArrayRef>>,
}

impl ExecutionPlan for TableScanPlan {
    fn name(&self) -> &str {
        "TableScanPlan"
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Err(DataFusionError::Internal(format!(
            "Children cannot be replaced in {:?}",
            self
        )))
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let batch = RecordBatch::try_new(self.schema.clone(), self.data.to_vec())?;
        Ok(Box::pin(TableScanStream::new(
            vec![batch],
            self.schema.clone(),
        )))
    }

    fn statistics(&self) -> Result<Statistics> {
        Ok(Statistics::default())
    }
}

impl DisplayAs for TableScanPlan {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "TableScanPlan")
            }
            DisplayFormatType::TreeRender => {
                // TODO(zipper): implement this.
                write!(f, "")
            }
        }
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct TableScanNode {
    schema: DFSchemaRef,
}

impl PartialOrd for TableScanNode {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.schema.fields().partial_cmp(other.schema.fields())
    }
}

impl UserDefinedLogicalNodeCore for TableScanNode {
    fn name(&self) -> &str {
        "TableScanNode"
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

    fn prevent_predicate_push_down_columns(&self) -> std::collections::HashSet<String> {
        // default (safe) is all columns in the schema.
        self.schema
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect()
    }

    fn fmt_for_explain(&self, _f: &mut std::fmt::Formatter) -> std::fmt::Result {
        todo!()
    }

    fn with_exprs_and_inputs(
        &self,
        _exprs: Vec<Expr>,
        _inputs: Vec<LogicalPlan>,
    ) -> datafusion::error::Result<Self> {
        todo!()
    }
}

// pub struct TableScanPlanner {}
//
// impl ExtensionPlanner for TableScanPlanner {
//     fn plan_extension(
//         &self,
//         _planner: &dyn PhysicalPlanner,
//         node: &dyn UserDefinedLogicalNode,
//         _logical_inputs: &[&LogicalPlan],
//         _physical_inputs: &[Arc<dyn ExecutionPlan>],
//         _session_state: &SessionState,
//     ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
//         dbg!(node);
//         Ok(Some(Arc::new(TableScanPlan {
//             schema: Table::test_schema(),
//             data: Arc::new(Table::test_data()),
//         })))
//     }
// }

#[tokio::test]
async fn test_dataframe() {
    let table = Arc::new(Table {});

    let ctx = SessionContext::new();
    ctx.register_table("tbl", table.clone()).unwrap();

    let logical_plan = LogicalPlanBuilder::scan_with_filters(
        "tbl",
        datasource::provider_as_source(table),
        Some(vec![0, 1]),
        vec![col("fa").gt_eq(Expr::Literal(ScalarValue::Int32(Some(2))))],
    )
    .unwrap()
    .build()
    .unwrap();

    let dataframe = DataFrame::new(ctx.state(), logical_plan);
    // .select_columns(&["fa", "fb"])
    // .unwrap();

    time::timeout(Duration::from_secs(10), async move {
        let result = dataframe.collect().await.unwrap();
        let record_batch = result.first().unwrap();

        dbg!(record_batch.columns());
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn test_physical_planner() {
    // let physical_planner = DefaultPhysicalPlanner::with_extension_planners(vec![Arc::new(data)]);
}
