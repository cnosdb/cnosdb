use std::collections::HashMap;

use spi::QueryError;

pub mod factory;
pub mod provider;

const STREAM_DB_KEY: &str = "db";
const STREAM_TABLE_KEY: &str = "table";

pub fn get_target_db_name(options: &HashMap<String, String>) -> Option<&str> {
    options.get(STREAM_DB_KEY).map(|e| e.as_ref())
}

pub fn get_target_table_name<'a>(
    table: &'a str,
    options: &'a HashMap<String, String>,
) -> Result<&'a str, QueryError> {
    options
        .get(STREAM_TABLE_KEY)
        .ok_or_else(|| QueryError::MissingTableOptions {
            option_name: STREAM_TABLE_KEY.into(),
            table_name: table.into(),
        })
        .map(|e| e.as_ref())
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::time::Duration;

    use coordinator::service_mock::MockCoordinator;
    use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use datafusion::logical_expr::TableSource;
    use datafusion::sql::TableReference;
    use meta::model::meta_client_mock::MockMetaClient;
    use models::schema::{StreamTable, Watermark};
    use spi::query::datasource::stream::StreamProviderManager;
    use spi::QueryError;

    use crate::data_source::split::default_split_manager_ref_only_for_test;
    use crate::data_source::stream::tskv::factory::TskvStreamProviderFactory;
    use crate::data_source::stream::tskv::{STREAM_DB_KEY, STREAM_TABLE_KEY};
    use crate::data_source::table_source::TableSourceAdapter;

    #[test]
    fn test_tskv() -> Result<(), QueryError> {
        let mut manager = StreamProviderManager::default();
        let split_m = default_split_manager_ref_only_for_test();
        let coord = Arc::new(MockCoordinator::default());
        let meta = Arc::new(MockMetaClient::default());
        let factory = Arc::new(TskvStreamProviderFactory::new(coord, split_m));
        manager.register_stream_provider_factory("tskv", factory)?;

        let table = StreamTable::new(
            "tenant",
            "db",
            "name",
            Arc::new(Schema::empty()),
            "tskv",
            Watermark {
                column: "time".into(),
                delay: Duration::default(),
            },
            Default::default(),
        );
        // error EventTimeColumnNotSpecified
        let provider = manager.create_provider(meta.clone(), &table);
        assert!(matches!(
            provider,
            Err(QueryError::MissingTableOptions { .. })
        ));

        let schema = Arc::new(Schema::new(vec![Field::new(
            "time",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        )]));

        let table = StreamTable::new(
            "tenant",
            "db",
            "name",
            schema.clone(),
            "tskv",
            Watermark {
                column: "time".into(),
                delay: Duration::default(),
            },
            HashMap::from_iter([
                (STREAM_DB_KEY.into(), "db".into()),
                (STREAM_TABLE_KEY.into(), "name".into()),
            ]),
        );
        let provider = manager.create_provider(meta, &table)?;

        assert_eq!(&provider.watermark().column, "time");
        assert_eq!(provider.schema(), schema);

        let source =
            TableSourceAdapter::try_new(TableReference::bare("name"), "db", "name", provider)?;

        let plan = source.get_logical_plan().unwrap();

        let result = format!("{}", plan.display_indent());

        assert_eq!(
            "Watermark: event_time=time, delay=0ms\
            \n  StreamScan: [name.time]",
            result
        );

        Ok(())
    }
}
