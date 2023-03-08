pub mod factory;
pub mod provider;

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use coordinator::service_mock::MockCoordinator;
    use datafusion::arrow::datatypes::Schema;
    use datafusion::logical_expr::{Extension, LogicalPlan, TableSource};
    use datafusion::prelude::Column;
    use meta::meta_client_mock::MockMetaClient;
    use models::schema::StreamTable;
    use spi::query::datasource::stream::StreamProviderManager;
    use spi::QueryError;

    use crate::data_source::stream::tskv::factory::TskvStreamProviderFactory;
    use crate::data_source::table_source::TableSourceAdapter;
    use crate::extension::logical::plan_node::stream_scan::StreamScanPlanNode;
    use crate::extension::EVENT_TIME_COLUMN;

    #[test]
    fn test_tskv() -> Result<(), QueryError> {
        let mut manager = StreamProviderManager::default();
        let coord = Arc::new(MockCoordinator::default());
        let meta = Arc::new(MockMetaClient::default());
        let factory = Arc::new(TskvStreamProviderFactory::new(coord));
        manager.register_stream_provider_factory("tskv", factory)?;

        let table = StreamTable::new(
            "tenant",
            "db",
            "name",
            Arc::new(Schema::empty()),
            "tskv",
            Default::default(),
        );
        // error EventTimeColumnNotSpecified
        let provider = manager.create_provider(meta.clone(), &table);
        assert!(matches!(
            provider,
            Err(QueryError::EventTimeColumnNotSpecified { .. })
        ));

        let table = StreamTable::new(
            "tenant",
            "db",
            "name",
            Arc::new(Schema::empty()),
            "tskv",
            HashMap::from_iter([(EVENT_TIME_COLUMN.into(), "time".into())]),
        );
        let provider = manager.create_provider(meta, &table)?;

        assert_eq!(provider.event_time_column(), &Column::from_name("time"));
        assert_eq!(provider.schema(), Arc::new(Schema::empty()));

        let source = TableSourceAdapter::try_new(1, "tenant", "db", "name", provider)?;

        let plan = source.get_logical_plan().unwrap();

        match plan {
            LogicalPlan::Extension(Extension { node }) => {
                let StreamScanPlanNode {
                    table_name,
                    projection,
                    projected_schema,
                    filters,
                    agg_with_grouping,
                    fetch,
                    ..
                } = node.as_any().downcast_ref::<StreamScanPlanNode>().unwrap();

                assert_eq!(table_name, "name");
                assert_eq!(projection, &None);
                assert_eq!(filters, &vec![]);
                assert!(agg_with_grouping.is_none());
                assert!(fetch.is_none());

                println!("{:?}", projected_schema);
            }
            _ => panic!("unexpected plan: {}", plan.display_indent()),
        }

        Ok(())
    }
}
