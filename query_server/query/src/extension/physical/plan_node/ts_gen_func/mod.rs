use std::fmt::Debug;
use std::sync::Arc;

use datafusion::common::Statistics;
use datafusion::execution::TaskContext;
use datafusion::physical_expr::{PhysicalSortExpr, PhysicalSortRequirement};
use datafusion::physical_plan::{
    DisplayFormatType, Distribution, ExecutionPlan, Partitioning, PhysicalExpr,
    SendableRecordBatchStream,
};
use models::arrow::SchemaRef;
use spi::DFResult;

use self::stream::TSGenFuncStream;
use crate::extension::expr::TSGenFunc;

mod stream;

#[allow(dead_code)]
trait GenerateTimeSeries {
    fn generate_time_series(
        timestamps: &mut [i64],
        fields: &mut [Vec<f64>],
        arg_str: Option<&str>,
    ) -> DFResult<(Vec<i64>, Vec<f64>)>;
}

pub struct TSGenFuncExec {
    input: Arc<dyn ExecutionPlan>,
    time_expr: Arc<dyn PhysicalExpr>,
    field_exprs: Vec<Arc<dyn PhysicalExpr>>,
    arg_expr: Option<Arc<dyn PhysicalExpr>>,
    symbol: TSGenFunc,
    schema: SchemaRef,
}

impl TSGenFuncExec {
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        time_expr: Arc<dyn PhysicalExpr>,
        field_exprs: Vec<Arc<dyn PhysicalExpr>>,
        arg_expr: Option<Arc<dyn PhysicalExpr>>,
        symbol: TSGenFunc,
        schema: SchemaRef,
    ) -> Self {
        Self {
            input,
            time_expr,
            field_exprs,
            arg_expr,
            symbol,
            schema,
        }
    }
}

impl Debug for TSGenFuncExec {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "TSGenFuncExec",)
    }
}

impl ExecutionPlan for TSGenFuncExec {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(1)
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        vec![Distribution::SinglePartition]
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        self.input.output_ordering()
    }

    fn required_input_ordering(&self) -> Vec<Option<Vec<PhysicalSortRequirement>>> {
        let sort_expr = PhysicalSortExpr {
            expr: self.time_expr.clone(),
            options: Default::default(),
        };
        vec![Some(PhysicalSortRequirement::from_sort_exprs([&sort_expr]))]
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![Arc::clone(&self.input)]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(Self {
            input: children[0].clone(),
            time_expr: self.time_expr.clone(),
            field_exprs: self.field_exprs.clone(),
            arg_expr: self.arg_expr.clone(),
            symbol: self.symbol,
            schema: self.schema.clone(),
        }))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(datafusion::error::DataFusionError::Internal(format!(
                "TSGenFuncExec invalid partition {partition}, there can be only one partition"
            )));
        }

        let input_stream = self.input.execute(partition, context)?;

        Ok(Box::pin(TSGenFuncStream::new(
            input_stream,
            self.time_expr.clone(),
            self.field_exprs.clone(),
            self.arg_expr.clone(),
            self.symbol,
            self.schema.clone(),
        )))
    }

    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "TSGenFuncExec: time_expr={}, field_exprs=[{}], arg_expr={:?}, func={}",
            self.time_expr,
            self.field_exprs
                .iter()
                .map(|expr| expr.to_string())
                .collect::<Vec<_>>()
                .join(","),
            self.arg_expr.as_ref().map(|expr| expr.to_string()),
            self.symbol.name(),
        )
    }

    fn statistics(&self) -> Statistics {
        Statistics::default()
    }
}
