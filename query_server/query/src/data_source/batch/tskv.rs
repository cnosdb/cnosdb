use std::any::Any;
use std::ops::Deref;
use std::sync::Arc;

use async_trait::async_trait;
use coordinator::service::CoordinatorRef;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::context::SessionState;
use datafusion::logical_expr::expr::AggregateFunction;
use datafusion::logical_expr::logical_plan::AggWithGrouping;
use datafusion::logical_expr::{
    aggregate_function, Expr, TableProviderAggregationPushDown, TableProviderFilterPushDown,
};
use datafusion::optimizer::utils::split_conjunction;
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::physical_plan::{project_schema, ExecutionPlan};
use datafusion::prelude::Column;
use meta::error::MetaError;
use meta::model::MetaClientRef;
use models::predicate::domain::{Predicate, PredicateRef, PushedAggregateFunction};
use models::schema::{TskvTableSchema, TskvTableSchemaRef};
use trace::debug;

use crate::data_source::sink::tskv::TskvRecordBatchSinkProvider;
use crate::data_source::split::tskv::TableLayoutHandle;
use crate::data_source::split::SplitManagerRef;
use crate::data_source::WriteExecExt;
use crate::extension::expr::expr_utils;
use crate::extension::physical::plan_node::aggregate_filter_scan::AggregateFilterTskvExec;
use crate::extension::physical::plan_node::table_writer::TableWriterExec;
use crate::extension::physical::plan_node::tag_scan::TagScanExec;
use crate::extension::physical::plan_node::tskv_exec::TskvExec;

#[derive(Clone)]
pub struct ClusterTable {
    coord: CoordinatorRef,
    split_manager: SplitManagerRef,
    _meta: MetaClientRef,
    schema: TskvTableSchemaRef,
}

impl ClusterTable {
    async fn create_table_scan_physical_plan(
        &self,
        ctx: &SessionState,
        projection: Option<&Vec<usize>>,
        predicate: PredicateRef,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let proj_schema = self.project_schema(projection)?;

        let table_layout = TableLayoutHandle {
            table: self.schema.clone(),
            predicate: predicate.clone(),
        };
        // TODO Record the time it takes to get the shards
        let splits = self
            .split_manager
            .splits(ctx, table_layout)
            .await
            .map_err(|err| DataFusionError::External(Box::new(err)))?;
        if splits.is_empty() {
            return Ok(Arc::new(EmptyExec::new(false, proj_schema)));
        }

        Ok(Arc::new(TskvExec::new(
            self.schema.clone(),
            proj_schema,
            predicate,
            self.coord.clone(),
            splits,
        )))
    }

    async fn create_agg_filter_scan(
        &self,
        ctx: &SessionState,
        filter: Arc<Predicate>,
        agg_with_grouping: &AggWithGrouping,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let AggWithGrouping {
            group_expr: _,
            agg_expr,
            schema,
        } = agg_with_grouping;
        let proj_schema = SchemaRef::from(schema.deref());

        let table_layout = TableLayoutHandle {
            table: self.schema.clone(),
            predicate: filter.clone(),
        };
        // TODO Record the time it takes to get the shards
        let splits = self
            .split_manager
            .splits(ctx, table_layout)
            .await
            .map_err(|err| DataFusionError::External(Box::new(err)))?;
        if splits.is_empty() {
            return Ok(Arc::new(EmptyExec::new(false, proj_schema)));
        }

        let time_col = unsafe {
            // use first column(time column)
            Column::from_name(&self.schema.column_by_index(0).unwrap_unchecked().name)
        };

        let aggs = agg_expr
            .iter()
            .map(|e| match e {
                Expr::AggregateFunction(agg) => Ok(agg),
                _ => Err(DataFusionError::Plan(
                    "Invalid plan, pushed aggregate functions contains unsupported".to_string(),
                )),
            })
            .collect::<Result<Vec<_>>>()?;

        let pushed_aggs = aggs
        .into_iter()
        .map(|agg| {
            let AggregateFunction { fun, args, .. } = agg;

            args.iter()
                .map(|expr| {
                    // The parameter of the aggregate function pushed down must be a column column
                    match expr {
                        Expr::Column(c) => Ok(c),
                        Expr::Literal(_) => Ok(&time_col),
                        _ => Err(DataFusionError::Internal(format!(
                            "Pushed aggregate functions's args contains non-column or non-literal value: {expr:?}."
                        ))),
                    }
                })
                .collect::<Result<Vec<_>>>()
                .and_then(|columns| {
                    // Convert pushdown aggregate functions to intermediate structures
                    match fun {
                        aggregate_function::AggregateFunction::Count => {
                            let column = columns
                                .first()
                                .ok_or_else(|| {
                                    DataFusionError::Internal(
                                        "Pushed aggregate functions's args is none.".to_string(),
                                    )
                                })?
                                .deref()
                                .clone();
                            Ok(PushedAggregateFunction::Count(column.name))
                        }
                        // aggregate_function::AggregateFunction::Max => {},
                        // aggregate_function::AggregateFunction::Min => {},
                        _ => Err(DataFusionError::Internal(
                            "Pushed aggregate functions's args is none.".to_string(),
                        )),
                    }
                })
        })
        .collect::<Result<Vec<_>>>()?;

        Ok(Arc::new(AggregateFilterTskvExec::new(
            self.coord.clone(),
            proj_schema,
            self.schema.clone(),
            pushed_aggs,
            filter,
            splits,
        )))
    }

    pub async fn create_tag_scan_physical_plan(
        &self,
        ctx: &SessionState,
        projected_schema: SchemaRef,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let predicate = Arc::new(
            Predicate::default()
                .set_limit(limit)
                .push_down_filter(filters, &self.schema),
        );

        let table_layout = TableLayoutHandle {
            table: self.schema.clone(),
            predicate: predicate.clone(),
        };
        // TODO Record the time it takes to get the shards
        let splits = self
            .split_manager
            .splits(ctx, table_layout)
            .await
            .map_err(|err| DataFusionError::External(Box::new(err)))?;
        if splits.is_empty() {
            return Ok(Arc::new(EmptyExec::new(false, projected_schema)));
        }

        Ok(Arc::new(TagScanExec::new(
            self.schema.clone(),
            projected_schema,
            predicate,
            self.coord.clone(),
            splits,
        )))
    }

    pub fn new(
        coord: CoordinatorRef,
        split_manager: SplitManagerRef,
        meta: MetaClientRef,
        schema: TskvTableSchemaRef,
    ) -> Self {
        ClusterTable {
            coord,
            split_manager,
            _meta: meta,
            schema,
        }
    }

    pub fn table_schema(&self) -> TskvTableSchemaRef {
        self.schema.clone()
    }

    // Check and return the projected schema
    fn project_schema(&self, projection: Option<&Vec<usize>>) -> Result<SchemaRef> {
        valid_project(&self.schema, projection)
            .map_err(|err| DataFusionError::External(Box::new(err)))?;
        project_schema(&self.schema.to_arrow_schema(), projection)
    }
}

#[async_trait]
impl TableProvider for ClusterTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.to_arrow_schema()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        ctx: &SessionState,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        agg_with_grouping: Option<&AggWithGrouping>,
        // limit can be used to reduce the amount scanned
        // from the datasource as a performance optimization.
        // If set, it contains the amount of rows needed by the `LogicalPlan`,
        // The datasource should return *at least* this number of rows if available.
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let filter = Arc::new(
            Predicate::default()
                .set_limit(limit)
                .push_down_filter(filters, &self.schema),
        );

        if let Some(agg_with_grouping) = agg_with_grouping {
            debug!("Create aggregate filter tskv scan.");
            return self
                .create_agg_filter_scan(ctx, filter, agg_with_grouping)
                .await;
        }

        return self
            .create_table_scan_physical_plan(ctx, projection, filter)
            .await;
    }

    fn supports_filter_pushdown(&self, expr: &Expr) -> Result<TableProviderFilterPushDown> {
        let exprs = split_conjunction(expr);
        let exprs = exprs.into_iter().cloned().collect::<Vec<_>>();
        if expr_utils::find_exprs_in_exprs(&exprs, &|nested_expr| {
            !expr_utils::is_time_filter(nested_expr)
        })
        .is_empty()
        {
            // all exprs are time range filter
            return Ok(TableProviderFilterPushDown::Exact);
        }

        Ok(TableProviderFilterPushDown::Inexact)
    }

    fn supports_aggregate_pushdown(
        &self,
        group_expr: &[Expr],
        aggr_expr: &[Expr],
    ) -> Result<TableProviderAggregationPushDown> {
        if !group_expr.is_empty() {
            return Ok(TableProviderAggregationPushDown::Unsupported);
        }

        let result = if aggr_expr.iter().all(|e| {
            match e {
                Expr::AggregateFunction(AggregateFunction {
                    fun,
                    args,
                    distinct,
                    filter,
                }) => {
                    let support_agg_func = matches!(
                        fun,
                        aggregate_function::AggregateFunction::Count // TODO
                                                                     // | aggregate_function::AggregateFunction::Max
                                                                     // | aggregate_function::AggregateFunction::Min
                                                                     // | aggregate_function::AggregateFunction::Sum
                    );

                    support_agg_func
                        && args.len() == 1
                        // count(*) | count(1) | count(col)
                        && (matches!(args[0], Expr::Column(_)) || matches!(args[0], Expr::Literal(_)))
                        // not distinct
                        && !*distinct
                        && filter.is_none()
                }
                _ => false,
            }
        }) {
            TableProviderAggregationPushDown::Ungrouped
        } else {
            TableProviderAggregationPushDown::Unsupported
        };

        Ok(result)
    }

    fn push_down_projection(&self, proj: &[usize]) -> Option<Vec<usize>> {
        let mut contain_time = false;
        let mut contain_field = false;

        proj.iter()
            .flat_map(|i| self.schema.column_by_index(*i))
            .for_each(|c| {
                if c.column_type.is_field() {
                    contain_field = true;
                } else if c.column_type.is_time() {
                    contain_time = true;
                }
            });

        if contain_time && !contain_field {
            let new_projection = proj
                .iter()
                .cloned()
                .chain(
                    self.schema
                        .columns()
                        .iter()
                        .enumerate()
                        .filter(|(_, c)| c.column_type.is_field())
                        .map(|(i, _)| i),
                )
                .collect::<Vec<usize>>();

            return Some(new_projection);
        };

        None
    }
}

#[async_trait]
impl WriteExecExt for ClusterTable {
    async fn write(
        &self,
        _state: &SessionState,
        input: Arc<dyn ExecutionPlan>,
    ) -> Result<Arc<TableWriterExec>> {
        let record_batch_sink_privider = Arc::new(TskvRecordBatchSinkProvider::new(
            self.coord.clone(),
            self.schema.clone(),
        ));

        Ok(Arc::new(TableWriterExec::new(
            input,
            self.schema.name.clone(),
            record_batch_sink_privider,
        )))
    }
}

/// Check the validity of the projection
///
/// 1. If the projection contains the time column, it must contain the field column, otherwise an error will be reported
pub fn valid_project(
    schema: &TskvTableSchema,
    projection: Option<&Vec<usize>>,
) -> std::result::Result<(), MetaError> {
    let mut field_count = 0;
    let mut contains_time_column = false;

    if let Some(e) = projection {
        e.iter().cloned().for_each(|idx| {
            if let Some(c) = schema.column_by_index(idx) {
                if c.column_type.is_time() {
                    contains_time_column = true;
                }
                if c.column_type.is_field() {
                    field_count += 1;
                }
            };
        });
    }

    if contains_time_column && field_count == 0 {
        return Err(MetaError::CommonError {
            msg: "If the projection contains the time column, it must contain the field column"
                .to_string(),
        });
    }

    Ok(())
}
