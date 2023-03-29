use datafusion::logical_expr::{LogicalPlan, PlanVisitor, TableScan};
use spi::query::datasource::stream::StreamProviderRef;
use spi::query::logical_planner::QueryPlan;
use spi::QueryError;

use crate::data_source::source_downcast_adapter;
use crate::data_source::table_source::TableHandle;

pub fn extract_stream_providers(plan: &QueryPlan) -> Vec<StreamProviderRef> {
    let mut stream_providers = vec![];

    let _ = plan.df_plan.accept(&mut ExtractStreamProvider {
        stream_providers: &mut stream_providers,
    });

    stream_providers
}

pub struct ExtractStreamProvider<'a> {
    stream_providers: &'a mut Vec<StreamProviderRef>,
}

impl<'a> PlanVisitor for ExtractStreamProvider<'a> {
    type Error = QueryError;

    fn pre_visit(&mut self, plan: &LogicalPlan) -> std::result::Result<bool, Self::Error> {
        if let LogicalPlan::TableScan(TableScan { source, .. }) = plan {
            if let TableHandle::StreamProvider(s) = source_downcast_adapter(source)?.table_handle()
            {
                self.stream_providers.push(s.clone());
            }
        }

        Ok(true)
    }
}
