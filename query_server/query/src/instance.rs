use std::sync::Arc;

use async_trait::async_trait;
use datafusion::scheduler::Scheduler;
use spi::{
    query::{dispatcher::QueryDispatcher, session::IsiphoSessionCtxFactory},
    server::dbms::DatabaseManagerSystem,
    server::BuildSnafu,
    server::Result,
    server::{LoadFunctionSnafu, QuerySnafu},
    service::protocol::{Query, QueryHandle},
};

use crate::dispatcher::manager::SimpleQueryDispatcherBuilder;
use crate::extension::expr::load_all_functions;
use crate::function::simple_func_manager::SimpleFunctionMetadataManager;
use crate::metadata::LocalCatalogMeta;
use crate::sql::optimizer::CascadeOptimizerBuilder;
use crate::sql::parser::DefaultParser;
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

pub fn make_cnosdbms(engine: EngineRef) -> Result<Cnosdbms> {
    // todo: add query config
    // for now only support local mode
    let mut function_manager = SimpleFunctionMetadataManager::default();
    load_all_functions(&mut function_manager).context(LoadFunctionSnafu)?;

    let meta = Arc::new(LocalCatalogMeta::new_with_default(
        engine,
        Arc::new(function_manager),
    ));

    // TODO session config need load global system config
    let session_factory = Arc::new(IsiphoSessionCtxFactory::default());
    let parser = Arc::new(DefaultParser::default());
    let optimizer = Arc::new(CascadeOptimizerBuilder::default().build());
    // TODO wrap, and num_threads configurable
    let scheduler = Arc::new(Scheduler::new(num_cpus::get() * 2));

    let simple_query_dispatcher = SimpleQueryDispatcherBuilder::default()
        .with_metadata(meta)
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
    use chrono::Utc;
    use std::ops::DerefMut;
    use trace::debug;

    use super::*;
    use datafusion::arrow::{
        datatypes::Schema, record_batch::RecordBatch, util::pretty::pretty_format_batches,
    };
    use futures::StreamExt;
    use spi::query::execution::Output;
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

    async fn exec_sql(db: &Cnosdbms, sql: &str) -> Vec<RecordBatch> {
        let query = Query::new(Default::default(), sql.to_string());

        // let db = make_cnosdbms(Arc::new(MockEngine::default())).unwrap();
        let mut actual = vec![];

        let mut result = db.execute(&query).await.unwrap();

        for ele in result.result().iter_mut() {
            match ele {
                Output::StreamData(data) => {
                    while let Some(next) = data.next().await {
                        let batch = next.unwrap();
                        actual.push(batch);
                    }
                }
                Output::Nil(_) => {
                    let batch = RecordBatch::new_empty(Arc::new(Schema::empty()));
                    actual.push(batch);
                }
            }
        }
        actual
    }

    #[tokio::test]
    async fn test_simple_sql() {
        let db = make_cnosdbms(Arc::new(MockEngine::default())).unwrap();

        let mut result = exec_sql(&db, "SELECT * FROM (VALUES (1, 'one'), (2, 'two'), (3, 'three')) AS t (num,letter) order by num").await;

        let expected = vec![
            "+-----+--------+",
            "| num | letter |",
            "+-----+--------+",
            "| 1   | one    |",
            "| 2   | two    |",
            "| 3   | three  |",
            "+-----+--------+",
        ];

        // let formatted = pretty_format_batches(result.deref_mut())
        //     .unwrap()
        //     .to_string();

        // println!("{}", formatted);

        assert_batches_eq!(expected, result.deref_mut());
    }

    fn generate_data(n: usize) -> String {
        // let mut random = rand::thread_rng();

        // random.gen_range();
        debug!("start generate data.");
        let rows: Vec<String> = (0..n)
            .into_iter()
            .map(|i| {
                format!(
                    "({}, '{}----xxxxxx=====3333444hhhhhhxx324r9cc')",
                    i % 1000,
                    i % 100
                )
            })
            .collect();
        // .reduce(|l, r| {
        //     format!("{}, {}", l, r)
        // }).unwrap();

        let result = rows.join(",");

        debug!("end generate data.");

        result
    }

    #[tokio::test]
    #[ignore]
    async fn test_topk_sql() {
        // trace::init_default_global_tracing("/tmp", "test_rust.log", "debug");

        let db = make_cnosdbms(Arc::new(MockEngine::default())).unwrap();

        let sql = format!(
            "SELECT * FROM
        (VALUES  {}) AS t (num,letter)
        order by num limit 20",
            generate_data(1_000_000)
        );

        let start = Utc::now();

        let mut result = exec_sql(&db, &sql).await;

        let end = Utc::now();

        println!("used time: {}", (start - end).num_milliseconds());

        let expected = vec![
            "+-----+--------+",
            "| num | letter |",
            "+-----+--------+",
            "| 1   | one    |",
            "| 2   | two    |",
            "+-----+--------+",
        ];

        assert_batches_eq!(expected, result.deref_mut());
    }

    #[tokio::test]
    async fn test_topk_desc_sql() {
        // trace::init_default_global_tracing("/tmp", "test_rust.log", "debug");

        let db = make_cnosdbms(Arc::new(MockEngine::default())).unwrap();

        let mut result = exec_sql(
            &db,
            "
        SELECT * FROM
        (VALUES  (9, 'nine'),(2, 'two'), (1, 'one'), (3, 'three')) AS t (num,letter)
        order by num desc limit 2",
        )
        .await;

        let expected = vec![
            "+-----+--------+",
            "| num | letter |",
            "+-----+--------+",
            "| 9   | nine   |",
            "| 3   | three  |",
            "+-----+--------+",
        ];

        // let formatted = pretty_format_batches(result.deref_mut())
        //     .unwrap()
        //     .to_string();

        // println!("{}", formatted);

        assert_batches_eq!(expected, result.deref_mut());
    }

    #[tokio::test]
    async fn test_drop() {
        let result_db = make_cnosdbms(Arc::new(MockEngine::default()));

        let db = result_db.unwrap();

        let query = Query::new(
            Default::default(),
            "drop database if exists test; \
                    drop database test; \
                    drop table if exists test; \
                    drop table test; \
                    "
            .to_string(),
        );
        let mut result = db.execute(&query).await.unwrap();
        for ele in result.result().iter_mut() {
            match ele {
                Output::StreamData(_data) => {
                    panic!("should not happen");
                }
                Output::Nil(_res) => {
                    println!("sql excuted ok")
                }
            }
        }
    }

    #[tokio::test]
    async fn test_explain() {
        // trace::init_default_global_tracing("/tmp", "test_rust.log", "debug");

        let db = make_cnosdbms(Arc::new(MockEngine::default())).unwrap();

        let mut result = exec_sql(
            &db,
            "
        EXPLAIN
            SELECT * FROM
            (VALUES  (9, 'nine'),(2, 'two'), (1, 'one'), (3, 'three')) AS t (num,letter)
            order by num desc limit 2;
        EXPLAIN
            SELECT * FROM
            (VALUES  (9, 'nine'),(2, 'two'), (1, 'one'), (3, 'three')) AS t (num,letter)
            order by num desc, letter limit 3;
        ",
        )
        .await;

        let num_cpu = num_cpus::get().to_string();
        let mut re_partition = format!(
            "|               |           RepartitionExec: partitioning=RoundRobinBatch({})",
            num_cpu
        );
        for _ in 0..60 - num_cpu.chars().count() + 1 {
            re_partition.push(' ');
        }
        re_partition.push('|');

        let expected = vec![
            "+---------------+-----------------------------------------------------------------------------------------------------------------------+",
            "| plan_type     | plan                                                                                                                  |",
            "+---------------+-----------------------------------------------------------------------------------------------------------------------+",
            "| logical_plan  | TopK: [#t.num DESC NULLS FIRST], TopKOptions: k=2, skip=None, step=SINGLE                                             |",
            "|               |   Projection: #t.num, #t.letter                                                                                       |",
            "|               |     Projection: #t.column1 AS num, #t.column2 AS letter, alias=t                                                      |",
            "|               |       Projection: #column1, #column2, alias=t                                                                         |",
            "|               |         Values: (Int64(9), Utf8(\"nine\")), (Int64(2), Utf8(\"two\")), (Int64(1), Utf8(\"one\")), (Int64(3), Utf8(\"three\")) |",
            "| physical_plan | TopKExec: [num@0 DESC], TopKOptions: k=2, skip=None, step=SINGLE                                                      |",
            "|               |   CoalescePartitionsExec                                                                                              |",
            "|               |     ProjectionExec: expr=[num@0 as num, letter@1 as letter]                                                           |",
            "|               |       ProjectionExec: expr=[column1@0 as num, column2@1 as letter]                                                    |",
            "|               |         ProjectionExec: expr=[column1@0 as column1, column2@1 as column2]                                             |",
            re_partition.as_ref(),
            "|               |             ValuesExec                                                                                                |",
            "|               |                                                                                                                       |",
            "| logical_plan  | TopK: [#t.num DESC NULLS FIRST,#t.letter ASC NULLS LAST], TopKOptions: k=3, skip=None, step=SINGLE                    |",
            "|               |   Projection: #t.num, #t.letter                                                                                       |",
            "|               |     Projection: #t.column1 AS num, #t.column2 AS letter, alias=t                                                      |",
            "|               |       Projection: #column1, #column2, alias=t                                                                         |",
            "|               |         Values: (Int64(9), Utf8(\"nine\")), (Int64(2), Utf8(\"two\")), (Int64(1), Utf8(\"one\")), (Int64(3), Utf8(\"three\")) |",
            "| physical_plan | TopKExec: [num@0 DESC,letter@1 ASC NULLS LAST], TopKOptions: k=3, skip=None, step=SINGLE                              |",
            "|               |   CoalescePartitionsExec                                                                                              |",
            "|               |     ProjectionExec: expr=[num@0 as num, letter@1 as letter]                                                           |",
            "|               |       ProjectionExec: expr=[column1@0 as num, column2@1 as letter]                                                    |",
            "|               |         ProjectionExec: expr=[column1@0 as column1, column2@1 as column2]                                             |",
            re_partition.as_ref(),
            "|               |             ValuesExec                                                                                                |",
            "|               |                                                                                                                       |",
            "+---------------+-----------------------------------------------------------------------------------------------------------------------+",
        ];

        assert_batches_eq!(expected, result.deref_mut());
    }

    #[tokio::test]
    #[ignore]
    async fn test_create_external_csv_table() {
        let db = make_cnosdbms(Arc::new(MockEngine::default())).unwrap();

        assert_batches_eq!(
            vec!["++", "++", "++",],
            exec_sql(
                &db,
                "CREATE EXTERNAL TABLE decimal_simple (
                    c1  DECIMAL(10,6) NOT NULL,
                    c2  DOUBLE NOT NULL,
                    c3  BIGINT NOT NULL,
                    c4  BOOLEAN NOT NULL,
                    c5  DECIMAL(12,7) NOT NULL
                )
                STORED AS CSV
                WITH HEADER ROW
                LOCATION 'tests/data/csv/decimal_data.csv'",
            )
            .await
            .deref_mut()
        );

        assert_batches_eq!(
            vec![
                "+----------+----------------+-----+-------+-----------+",
                "| c1       | c2             | c3  | c4    | c5        |",
                "+----------+----------------+-----+-------+-----------+",
                "| 0.000010 | 0.000000000001 | 1   | true  | 0.0000140 |",
                "| 0.000020 | 0.000000000002 | 2   | true  | 0.0000250 |",
                "| 0.000020 | 0.000000000002 | 3   | false | 0.0000190 |",
                "| 0.000030 | 0.000000000003 | 4   | true  | 0.0000320 |",
                "| 0.000030 | 0.000000000003 | 5   | false | 0.0000350 |",
                "| 0.000030 | 0.000000000003 | 5   | true  | 0.0000110 |",
                "| 0.000040 | 0.000000000004 | 5   | true  | 0.0000440 |",
                "| 0.000040 | 0.000000000004 | 8   | false | 0.0000440 |",
                "| 0.000040 | 0.000000000004 | 12  | false | 0.0000400 |",
                "| 0.000040 | 0.000000000004 | 14  | true  | 0.0000400 |",
                "| 0.000050 | 0.000000000005 | 1   | false | 0.0001000 |",
                "| 0.000050 | 0.000000000005 | 4   | true  | 0.0000780 |",
                "| 0.000050 | 0.000000000005 | 8   | false | 0.0000330 |",
                "| 0.000050 | 0.000000000005 | 9   | true  | 0.0000520 |",
                "| 0.000050 | 0.000000000005 | 100 | true  | 0.0000680 |",
                "+----------+----------------+-----+-------+-----------+",
            ],
            exec_sql(&db, "select * from decimal_simple order by c1,c2,c3,c4,c5;",)
                .await
                .deref_mut()
        );
    }

    #[tokio::test]
    #[ignore]
    async fn test_create_external_parquet_table() {
        let db = make_cnosdbms(Arc::new(MockEngine::default())).unwrap();

        assert_batches_eq!(
            vec!["++", "++", "++",],
            exec_sql(
                &db,
                "
            CREATE EXTERNAL TABLE rep
            STORED AS PARQUET
            LOCATION 'tests/data/parquet/userdata1.parquet';",
            )
            .await
            .deref_mut()
        );

        assert_batches_eq!(vec![
            "+---------------------+----+---------+---------+--------------------------+--------+----------------+------------------+------------------------+------------+-----------+--------------------------+-------+",
            "| COUNT(UInt8(1))     |    |         |         |                          |        |                |                  |                        |            |           |                          |       |",
            "+---------------------+----+---------+---------+--------------------------+--------+----------------+------------------+------------------------+------------+-----------+--------------------------+-------+",
            "| 1000                |    |         |         |                          |        |                |                  |                        |            |           |                          |       |",
            "| 2016-02-03 07:55:29 | 1  | Amanda  | Jordan  | ajordan0@com.com         | Female | 1.197.201.2    | 6759521864920116 | Indonesia              | 3/8/1971   | 49756.53  | Internal Auditor         | 1E+02 |",
            "| 2016-02-03 17:04:03 | 2  | Albert  | Freeman | afreeman1@is.gd          | Male   | 218.111.175.34 |                  | Canada                 | 1/16/1968  | 150280.17 | Accountant IV            |       |",
            "| 2016-02-03 01:09:31 | 3  | Evelyn  | Morgan  | emorgan2@altervista.org  | Female | 7.161.136.94   | 6767119071901597 | Russia                 | 2/1/1960   | 144972.51 | Structural Engineer      |       |",
            "| 2016-02-03 00:36:21 | 4  | Denise  | Riley   | driley3@gmpg.org         | Female | 140.35.109.83  | 3576031598965625 | China                  | 4/8/1997   | 90263.05  | Senior Cost Accountant   |       |",
            "| 2016-02-03 05:05:31 | 5  | Carlos  | Burns   | cburns4@miitbeian.gov.cn |        | 169.113.235.40 | 5602256255204850 | South Africa           |            |           |                          |       |",
            "| 2016-02-03 07:22:34 | 6  | Kathryn | White   | kwhite5@google.com       | Female | 195.131.81.179 | 3583136326049310 | Indonesia              | 2/25/1983  | 69227.11  | Account Executive        |       |",
            "| 2016-02-03 08:33:08 | 7  | Samuel  | Holmes  | sholmes6@foxnews.com     | Male   | 232.234.81.197 | 3582641366974690 | Portugal               | 12/18/1987 | 14247.62  | Senior Financial Analyst |       |",
            "| 2016-02-03 06:47:06 | 8  | Harry   | Howell  | hhowell7@eepurl.com      | Male   | 91.235.51.73   |                  | Bosnia and Herzegovina | 3/1/1962   | 186469.43 | Web Developer IV         |       |",
            "| 2016-02-03 03:52:53 | 9  | Jose    | Foster  | jfoster8@yelp.com        | Male   | 132.31.53.61   |                  | South Korea            | 3/27/1992  | 231067.84 | Software Test Engineer I | 1E+02 |",
            "| 2016-02-03 18:29:47 | 10 | Emily   | Stewart | estewart9@opensource.org | Female | 143.28.251.245 | 3574254110301671 | Nigeria                | 1/28/1997  | 27234.28  | Health Coach IV          |       |",
            "+---------------------+----+---------+---------+--------------------------+--------+----------------+------------------+------------------------+------------+-----------+--------------------------+-------+",
        ], exec_sql(
            &db,
            "
            select count(1) from rep;
            select * from rep limit 10;
            ",
        )
        .await.deref_mut());
    }

    #[tokio::test]
    #[ignore]
    async fn test_create_external_json_table() {
        let db = make_cnosdbms(Arc::new(MockEngine::default())).unwrap();

        assert_batches_eq!(
            vec!["++", "++", "++",],
            exec_sql(
                &db,
                "
            CREATE EXTERNAL TABLE rep
            STORED AS NDJSON
            LOCATION 'tests/data/json/schema_infer_limit.json';",
            )
            .await
            .deref_mut()
        );

        assert_batches_eq!(
            vec![
                "+-----+------+-------+---+",
                "| a   | b    | c     | d |",
                "+-----+------+-------+---+",
                "| 1   |      |       |   |",
                "| -10 | -3.5 |       |   |",
                "| 2   | 0.6  | false |   |",
                "| 1   | 2    | false | 4 |",
                "| 4   |      |       |   |",
                "+-----+------+-------+---+",
            ],
            exec_sql(
                &db,
                "
            select * from rep limit 10;
            select count(1) from rep;
            ",
            )
            .await
            .deref_mut()
        );
    }
}
