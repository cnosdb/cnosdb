use std::sync::Arc;

use async_trait::async_trait;
use catalog::factory::TemporaryCatalogManager;
use datafusion::scheduler::Scheduler;
use query::{
    dispatcher::manager::SimpleQueryDispatcherBuilder,
    extension::datafusion::expr::load_all_functions,
    function::simple_func_manager::SimpleFunctionMetadataManager,
    sql::{optimizer::CascadeOptimizerBuilder, parser::DefaultParser},
};
use spi::{
    catalog::factory::CatalogManager,
    query::{dispatcher::QueryDispatcher, session::IsiphoSessionCtxFactory},
    server::dbms::DatabaseManagerSystem,
    server::BuildSnafu,
    server::Result,
    server::{LoadFunctionSnafu, QuerySnafu},
    service::protocol::{Query, QueryHandle},
};

use snafu::ResultExt;
use tskv::engine::EngineRef;

pub struct Cnosdbms {
    // query dispatcher & query execution
    query_dispatcher: Arc<dyn QueryDispatcher>,
}

#[async_trait]
impl DatabaseManagerSystem for Cnosdbms {
    async fn execute(&self, query: &Query) -> Result<QueryHandle> {
        let id = self.query_dispatcher.create_query_id();

        let result = self
            .query_dispatcher
            .execute_query(id, query)
            .await
            .context(QuerySnafu)?;

        Ok(QueryHandle::new(id, query.clone(), result))
    }
}

pub fn make_cnosdbms(tskv_client: EngineRef) -> Result<Cnosdbms> {
    // TODO refactor catalog
    let catalog_manager = Arc::new(TemporaryCatalogManager::new(tskv_client.clone()));
    // TODO init metadata
    catalog_manager.init();

    let mut function_manager = SimpleFunctionMetadataManager::default();
    load_all_functions(&mut function_manager).context(LoadFunctionSnafu)?;

    // TODO session config need load global system config
    let session_factory = Arc::new(IsiphoSessionCtxFactory::default());
    let parser = Arc::new(DefaultParser::default());
    let optimizer = Arc::new(CascadeOptimizerBuilder::default().build());
    // TODO wrap, and num_threads configurable
    let scheduler = Arc::new(Scheduler::new(num_cpus::get() * 2));

    let simple_query_dispatcher = SimpleQueryDispatcherBuilder::default()
        .with_catalog_manager(catalog_manager)
        .with_function_manager(Arc::new(function_manager))
        .with_session_factory(session_factory)
        .with_parser(parser)
        .with_optimizer(optimizer)
        .with_scheduler(scheduler)
        .build()
        .context(BuildSnafu)?;

    Ok(Cnosdbms {
        query_dispatcher: Arc::new(simple_query_dispatcher),
    })
}

#[cfg(test)]
mod tests {
    use std::{ops::DerefMut, task::Poll};

    use super::*;
    use datafusion::arrow::util::pretty::pretty_format_batches;
    use futures::StreamExt;
    use tskv::engine::MockEngine;

    #[macro_export]
    macro_rules! assert_batches_eq {
        ($EXPECTED_LINES: expr, $CHUNKS: expr) => {
            let expected_lines: Vec<String> = $EXPECTED_LINES.iter().map(|&s| s.into()).collect();

            let formatted = pretty_format_batches($CHUNKS).unwrap().to_string();

            let actual_lines: Vec<&str> = formatted.trim().lines().collect();

            assert_eq!(
                expected_lines, actual_lines,
                "\n\nexpected:\n\n{:#?}\nactual:\n\n{:#?}\n\n",
                expected_lines, actual_lines
            );
        };
    }

    #[tokio::test]
    async fn test_simple_sql() {
        let result_db = make_cnosdbms(Arc::new(MockEngine::default()));

        let db = result_db.unwrap();

        let query = Query::new(
            Default::default(),
            "SELECT * FROM (VALUES (1, 'one'), (2, 'two'), (3, 'three')) AS t (num,letter) order by num"
                .to_string(),
        );

        let mut actual = vec![];

        let mut result = db.execute(&query).await.unwrap();
        for ele in result.result().iter_mut() {
            while let Some(next) = ele.next().await {
                let batch = next.unwrap();

                actual.push(batch);
            }
        }

        let expected = vec![
            "+-----+--------+",
            "| num | letter |",
            "+-----+--------+",
            "| 1   | one    |",
            "| 2   | two    |",
            "| 3   | three  |",
            "+-----+--------+",
        ];

        let formatted = pretty_format_batches(actual.deref_mut())
            .unwrap()
            .to_string();

        println!("{}", formatted);

        assert_batches_eq!(expected, actual.deref_mut());
    }
}
