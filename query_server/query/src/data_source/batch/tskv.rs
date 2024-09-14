use std::any::Any;
use std::ops::Deref;
use std::sync::Arc;

use async_trait::async_trait;
use coordinator::service::CoordinatorRef;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::DFSchema;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::context::SessionState;
use datafusion::logical_expr::expr::AggregateFunction;
use datafusion::logical_expr::logical_plan::AggWithGrouping;
use datafusion::logical_expr::{
    aggregate_function, Expr, TableProviderAggregationPushDown, TableProviderFilterPushDown,
};
use datafusion::optimizer::utils::{conjunction, split_conjunction};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::physical_plan::{project_schema, ExecutionPlan};
use datafusion::prelude::Column;
use meta::error::MetaError;
use meta::model::MetaClientRef;
use models::predicate::domain::{Predicate, PredicateRef, PushedAggregateFunction};
use models::schema::tskv_table_schema::{TskvTableSchema, TskvTableSchemaRef};
use models::schema::TIME_FIELD_NAME;
use trace::debug;

use crate::data_source::batch::filter_expr_rewriter::{has_udf_function, rewrite_filters};
use crate::data_source::sink::tskv::TskvRecordBatchSinkProvider;
use crate::data_source::split::tskv::TableLayoutHandle;
use crate::data_source::split::SplitManagerRef;
use crate::data_source::{UpdateExecExt, WriteExecExt};
use crate::extension::expr::expr_utils;
use crate::extension::physical::plan_node::aggregate_filter_scan::AggregateFilterTskvExec;
use crate::extension::physical::plan_node::table_writer::TableWriterExec;
use crate::extension::physical::plan_node::tag_scan::TagScanExec;
use crate::extension::physical::plan_node::tskv_exec::TskvExec;
use crate::extension::physical::plan_node::update_tag::UpdateTagExec;

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
                                })?;
                            Ok(PushedAggregateFunction::Count(column.name.to_owned()))
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
        let filter = conjunction(filters.iter().cloned());

        let df_schema = self.schema.to_df_schema()?;

        let df_fields = projected_schema
            .fields()
            .iter()
            .map(|f| df_schema.field_with_unqualified_name(f.name()))
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .cloned()
            .collect::<Vec<_>>();

        let df_schema = DFSchema::new_with_metadata(df_fields, df_schema.metadata().clone())?;

        // Generate physical expressions using projected schema
        let predicate = Arc::new(
            Predicate::push_down_filter(filter, &df_schema, &projected_schema, limit)
                .map_err(|e| DataFusionError::External(Box::new(e)))?,
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
        let df_schema = self.schema.to_df_schema()?;
        let arrow_schema = self.schema.to_arrow_schema();
        // projection schema
        let (df_schema, arrow_schema) = if let Some(p) = projection {
            let df_fields = p
                .iter()
                .cloned()
                .map(|i| df_schema.fields()[i].clone())
                .collect::<Vec<_>>();
            let arrow_schema = arrow_schema.project(p)?;
            let df_schema = Arc::new(DFSchema::new_with_metadata(
                df_fields,
                df_schema.metadata().clone(),
            )?);
            let arrow_schema = Arc::new(arrow_schema);
            (df_schema, arrow_schema)
        } else {
            (df_schema, arrow_schema)
        };

        let filters = rewrite_filters(filters, df_schema.clone())?;
        // Generate physical expressions using projected schema
        let filter = Arc::new(
            Predicate::push_down_filter(filters, &df_schema, &arrow_schema, limit)
                .map_err(|e| DataFusionError::External(Box::new(e)))?,
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
        if has_udf_function(expr)? {
            return Ok(TableProviderFilterPushDown::Inexact);
        }

        // FIXME: tag support Exact Filter PushDown
        // TODO: REMOVE
        let exprs = split_conjunction(expr);
        let exprs = exprs.into_iter().cloned().collect::<Vec<_>>();
        if expr_utils::find_exprs_in_exprs(&exprs, &|nested_expr| {
            !expr_utils::can_exact_filter(nested_expr, self.table_schema())
        })
        .is_empty()
        {
            // all exprs are time range filter
            return Ok(TableProviderFilterPushDown::Exact);
        }

        // tskv handle all filters
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
        if aggr_expr.len() == 1 {
            if let Expr::AggregateFunction(AggregateFunction {
                fun,
                args,
                distinct,
                filter,
                order_by,
            }) = &aggr_expr[0]
            {
                if matches!(fun, aggregate_function::AggregateFunction::Count)
                    && args.len() == 1
                    && !*distinct
                    && filter.is_none()
                    && order_by.is_none()
                {
                    match &args[0] {
                        Expr::Column(expr) => {
                            if let Some(col) = self.schema.column(&expr.name) {
                                if col.column_type.is_tag() {
                                    return Ok(TableProviderAggregationPushDown::Unsupported);
                                }
                            }
                        }
                        Expr::Literal(expr) => {
                            if expr.is_null() {
                                return Ok(TableProviderAggregationPushDown::Unsupported);
                            }
                        }
                        _ => {}
                    }
                    
                    return Ok(TableProviderAggregationPushDown::Ungrouped);
                }
            }
        }

        Ok(TableProviderAggregationPushDown::Unsupported)
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

        if contain_field && !contain_time {
            if let Some(idx) = self.schema.column_index(TIME_FIELD_NAME) {
                let new_proj = std::iter::once(idx)
                    .chain(proj.iter().cloned())
                    .collect::<Vec<_>>();
                return Some(new_proj);
            }
        }

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

#[async_trait]
impl UpdateExecExt for ClusterTable {
    async fn update(
        &self,
        assigns: Vec<(String, Arc<dyn PhysicalExpr>)>,
        scan: Arc<dyn ExecutionPlan>,
    ) -> Result<Arc<UpdateTagExec>> {
        Ok(Arc::new(UpdateTagExec::new(
            scan,
            self.schema.clone(),
            assigns,
            self.coord.clone(),
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
    let mut contains_time_column = false;

    if let Some(e) = projection {
        e.iter().for_each(|idx| {
            if let Some(c) = schema.column_by_index(*idx) {
                if c.column_type.is_time() {
                    contains_time_column = true;
                }
            };
        });
    }

    if !contains_time_column {
        return Err(MetaError::CommonError {
            msg: "The projection must contains the time column".to_string(),
        });
    }

    Ok(())
}
