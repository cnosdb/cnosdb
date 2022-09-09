#[cfg(test)]
mod tests {
    use server::instance::{make_cnosdbms, Cnosdbms};
    use spi::service::protocol::Query;
    use std::{ops::DerefMut, sync::Arc};
    use trace::debug;

    use datafusion::arrow::{record_batch::RecordBatch, util::pretty::pretty_format_batches};
    use futures::StreamExt;
    use spi::query::execution::Output;
    use spi::server::dbms::DatabaseManagerSystem;
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
                    todo!();
                }
            }
        }
        actual
    }

    fn assert_record_batches(expected: Vec<&str>, result: Vec<RecordBatch>) {
        let mut result = result;
        assert_batches_eq!(expected, result.deref_mut());
    }

    #[tokio::test]
    async fn test_topk_sql() {
        // trace::init_default_global_tracing("/tmp", "test_rust.log", "debug");

        let db = make_cnosdbms(Arc::new(MockEngine::default())).unwrap();

        assert_record_batches(
            vec![
                "+-----+--------+",
                "| num | letter |",
                "+-----+--------+",
                "| 3   | three  |",
                "| 2   | two    |",
                "+-----+--------+",
            ],
            exec_sql(
                &db,
                "SELECT topk(num, 2), letter
                 FROM
                   (VALUES (1, 'one'), (2, 'two'), (3, 'three')) AS t (num,letter)",
            )
            .await,
        );

        assert_record_batches(
            vec![
                "+-----+-----+--------+",
                "| num | num | letter |",
                "+-----+-----+--------+",
                "| 3   | 3   | three  |",
                "| 2   | 2   | two    |",
                "+-----+-----+--------+",
            ],
            exec_sql(
                &db,
                "SELECT topk(num, 2), *
                 FROM
                   (VALUES (1, 'one'), (2, 'two'), (3, 'three')) AS t (num,letter)",
            )
            .await,
        );

        assert_record_batches(
            vec![
                "+-----+--------+---------------------+",
                "| num | letter | t.num Plus Int64(1) |",
                "+-----+--------+---------------------+",
                "| 3   | three  | 4                   |",
                "| 2   | two    | 3                   |",
                "+-----+--------+---------------------+",
            ],
            exec_sql(
                &db,
                "SELECT topk(num, 2), letter, num + 1
                 FROM
                   (VALUES (1, 'one'), (2, 'two'), (3, 'three')) AS t (num,letter)",
            )
            .await,
        );

        assert_record_batches(
            vec![
                "+--------+---------------------+",
                "| letter | t.num Plus Int64(1) |",
                "+--------+---------------------+",
                "| two    | 3                   |",
                "| three  | 4                   |",
                "+--------+---------------------+",
            ],
            exec_sql(
                &db,
                "SELECT topk(letter, 2), num + 1
                 FROM
                   (VALUES (1, 'one'), (2, 'two'), (3, 'three')) AS t (num,letter)",
            )
            .await,
        );

        assert_record_batches(
            vec![
                "+--------+--------+--------+-----------+---------+--------+",
                "| bigint | bigint | double | string    | boolean | letter |",
                "+--------+--------+--------+-----------+---------+--------+",
                "| 6      | 6      | 6.6666 | 666666666 | false   | three  |",
                "+--------+--------+--------+-----------+---------+--------+",
            ],
            exec_sql(
                &db,
                "SELECT topk(BIGINT, 1), *
                FROM
                (VALUES
                    (1, 1.1111, '111111111', true, 'one'),
                    (2, 2.2222, '222222222', true, 'two'), 
                    (3, 3.3333, '333333333', true, 'three'),
                    (4, 4.4444, '444444444', false, 'three'),
                    (5, 5.5555, '555555555', false, 'three'),
                    (6, 6.6666, '666666666', false, 'three')
                ) AS t (BIGINT, DOUBLE, STRING, BOOLEAN, letter)",
            )
            .await,
        );

        assert_record_batches(
            vec![
                "+--------+--------+--------+-----------+---------+--------+",
                "| double | bigint | double | string    | boolean | letter |",
                "+--------+--------+--------+-----------+---------+--------+",
                "| 6.6666 | 6      | 6.6666 | 666666666 | false   | three  |",
                "| 5.5555 | 5      | 5.5555 | 555555555 | false   | three  |",
                "+--------+--------+--------+-----------+---------+--------+",
            ],
            exec_sql(
                &db,
                "SELECT topk(DOUBLE, 2), *
                FROM
                (VALUES
                    (1, 1.1111, '111111111', true, 'one'),
                    (2, 2.2222, '222222222', true, 'two'), 
                    (3, 3.3333, '333333333', true, 'three'),
                    (4, 4.4444, '444444444', false, 'three'),
                    (5, 5.5555, '555555555', false, 'three'),
                    (6, 6.6666, '666666666', false, 'three')
                ) AS t (BIGINT, DOUBLE, STRING, BOOLEAN, letter)",
            )
            .await,
        );

        assert_record_batches(
            vec![
                "+-----------+--------+--------+-----------+---------+--------+",
                "| string    | bigint | double | string    | boolean | letter |",
                "+-----------+--------+--------+-----------+---------+--------+",
                "| 666666666 | 6      | 6.6666 | 666666666 | false   | three  |",
                "| 555555555 | 5      | 5.5555 | 555555555 | false   | three  |",
                "| 444444444 | 4      | 4.4444 | 444444444 | false   | three  |",
                "+-----------+--------+--------+-----------+---------+--------+",
            ],
            exec_sql(
                &db,
                "SELECT topk(STRING, 3), *
                FROM
                (VALUES
                    (1, 1.1111, '111111111', true, 'one'),
                    (2, 2.2222, '222222222', true, 'two'), 
                    (3, 3.3333, '333333333', true, 'three'),
                    (4, 4.4444, '444444444', false, 'three'),
                    (5, 5.5555, '555555555', false, 'three'),
                    (6, 6.6666, '666666666', false, 'three')
                ) AS t (BIGINT, DOUBLE, STRING, BOOLEAN, letter)",
            )
            .await,
        );

        assert_record_batches(
            vec![
                "+---------+--------+--------+-----------+---------+--------+",
                "| boolean | bigint | double | string    | boolean | letter |",
                "+---------+--------+--------+-----------+---------+--------+",
                "| true    | 2      | 2.2222 | 222222222 | true    | two    |",
                "| true    | 1      | 1.1111 | 111111111 | true    | one    |",
                "| true    | 3      | 3.3333 | 333333333 | true    | three  |",
                "| false   | 4      | 4.4444 | 444444444 | false   | three  |",
                "+---------+--------+--------+-----------+---------+--------+",
            ],
            exec_sql(
                &db,
                "SELECT topk(BOOLEAN, 4), *
                FROM
                (VALUES
                    (1, 1.1111, '111111111', true, 'one'),
                    (2, 2.2222, '222222222', true, 'two'), 
                    (3, 3.3333, '333333333', true, 'three'),
                    (4, 4.4444, '444444444', false, 'three'),
                    (5, 5.5555, '555555555', false, 'three'),
                    (6, 6.6666, '666666666', false, 'three')
                ) AS t (BIGINT, DOUBLE, STRING, BOOLEAN, letter)",
            )
            .await,
        );

        assert_record_batches(
            vec![
                "+--------+--------+--------+-----------+---------+--------+",
                "| letter | bigint | double | string    | boolean | letter |",
                "+--------+--------+--------+-----------+---------+--------+",
                "| two    | 2      | 2.2222 | 222222222 | true    | two    |",
                "| three  | 4      | 4.4444 | 444444444 | false   | three  |",
                "| three  | 6      | 6.6666 | 666666666 | false   | three  |",
                "| three  | 5      | 5.5555 | 555555555 | false   | three  |",
                "| three  | 3      | 3.3333 | 333333333 | true    | three  |",
                "+--------+--------+--------+-----------+---------+--------+",
            ],
            exec_sql(
                &db,
                "SELECT topk(letter, 5), *
                FROM
                (VALUES
                    (1, 1.1111, '111111111', true, 'one'),
                    (2, 2.2222, '222222222', true, 'two'), 
                    (3, 3.3333, '333333333', true, 'three'),
                    (4, 4.4444, '444444444', false, 'three'),
                    (5, 5.5555, '555555555', false, 'three'),
                    (6, 6.6666, '666666666', false, 'three')
                ) AS t (BIGINT, DOUBLE, STRING, BOOLEAN, letter)",
            )
            .await,
        );
    }

    #[tokio::test]
    async fn test_bottom_sql() {
        // trace::init_default_global_tracing("/tmp", "test_rust.log", "debug");

        let db = make_cnosdbms(Arc::new(MockEngine::default())).unwrap();

        assert_record_batches(
            vec![
                "+-----+--------+",
                "| num | letter |",
                "+-----+--------+",
                "| 1   | one    |",
                "| 2   | two    |",
                "+-----+--------+",
            ],
            exec_sql(
                &db,
                "SELECT bottom(num, 2), letter
                 FROM
                   (VALUES (1, 'one'), (2, 'two'), (3, 'three')) AS t (num,letter)",
            )
            .await,
        );

        assert_record_batches(
            vec![
                "+-----+-----+--------+",
                "| num | num | letter |",
                "+-----+-----+--------+",
                "| 1   | 1   | one    |",
                "| 2   | 2   | two    |",
                "+-----+-----+--------+",
            ],
            exec_sql(
                &db,
                "SELECT bottom(num, 2), *
                 FROM
                   (VALUES (1, 'one'), (2, 'two'), (3, 'three')) AS t (num,letter)",
            )
            .await,
        );

        assert_record_batches(
            vec![
                "+-----+--------+---------------------+",
                "| num | letter | t.num Plus Int64(1) |",
                "+-----+--------+---------------------+",
                "| 1   | one    | 2                   |",
                "| 2   | two    | 3                   |",
                "+-----+--------+---------------------+",
            ],
            exec_sql(
                &db,
                "SELECT bottom(num, 2), letter, num + 1
                 FROM
                   (VALUES (1, 'one'), (2, 'two'), (3, 'three')) AS t (num,letter)",
            )
            .await,
        );

        assert_record_batches(
            vec![
                "+--------+---------------------+",
                "| letter | t.num Plus Int64(1) |",
                "+--------+---------------------+",
                "| one    | 2                   |",
                "| three  | 4                   |",
                "+--------+---------------------+",
            ],
            exec_sql(
                &db,
                "SELECT bottom(letter, 2), num + 1
                 FROM
                   (VALUES (1, 'one'), (2, 'two'), (3, 'three')) AS t (num,letter)",
            )
            .await,
        );

        assert_record_batches(
            vec![
                "+--------+--------+--------+-----------+---------+--------+",
                "| bigint | bigint | double | string    | boolean | letter |",
                "+--------+--------+--------+-----------+---------+--------+",
                "| 1      | 1      | 1.1111 | 111111111 | true    | one    |",
                "+--------+--------+--------+-----------+---------+--------+",
            ],
            exec_sql(
                &db,
                "SELECT bottom(BIGINT, 1), *
                FROM
                (VALUES
                    (1, 1.1111, '111111111', true, 'one'),
                    (2, 2.2222, '222222222', true, 'two'), 
                    (3, 3.3333, '333333333', true, 'three'),
                    (4, 4.4444, '444444444', false, 'three'),
                    (5, 5.5555, '555555555', false, 'three'),
                    (6, 6.6666, '666666666', false, 'three')
                ) AS t (BIGINT, DOUBLE, STRING, BOOLEAN, letter)",
            )
            .await,
        );

        assert_record_batches(
            vec![
                "+--------+--------+--------+-----------+---------+--------+",
                "| double | bigint | double | string    | boolean | letter |",
                "+--------+--------+--------+-----------+---------+--------+",
                "| 1.1111 | 1      | 1.1111 | 111111111 | true    | one    |",
                "| 2.2222 | 2      | 2.2222 | 222222222 | true    | two    |",
                "+--------+--------+--------+-----------+---------+--------+",
            ],
            exec_sql(
                &db,
                "SELECT bottom(DOUBLE, 2), *
                FROM
                (VALUES
                    (1, 1.1111, '111111111', true, 'one'),
                    (2, 2.2222, '222222222', true, 'two'), 
                    (3, 3.3333, '333333333', true, 'three'),
                    (4, 4.4444, '444444444', false, 'three'),
                    (5, 5.5555, '555555555', false, 'three'),
                    (6, 6.6666, '666666666', false, 'three')
                ) AS t (BIGINT, DOUBLE, STRING, BOOLEAN, letter)",
            )
            .await,
        );

        assert_record_batches(
            vec![
                "+-----------+--------+--------+-----------+---------+--------+",
                "| string    | bigint | double | string    | boolean | letter |",
                "+-----------+--------+--------+-----------+---------+--------+",
                "| 111111111 | 1      | 1.1111 | 111111111 | true    | one    |",
                "| 222222222 | 2      | 2.2222 | 222222222 | true    | two    |",
                "| 333333333 | 3      | 3.3333 | 333333333 | true    | three  |",
                "+-----------+--------+--------+-----------+---------+--------+",
            ],
            exec_sql(
                &db,
                "SELECT bottom(STRING, 3), *
                FROM
                (VALUES
                    (1, 1.1111, '111111111', true, 'one'),
                    (2, 2.2222, '222222222', true, 'two'), 
                    (3, 3.3333, '333333333', true, 'three'),
                    (4, 4.4444, '444444444', false, 'three'),
                    (5, 5.5555, '555555555', false, 'three'),
                    (6, 6.6666, '666666666', false, 'three')
                ) AS t (BIGINT, DOUBLE, STRING, BOOLEAN, letter)",
            )
            .await,
        );

        assert_record_batches(
            vec![
                "+---------+--------+--------+-----------+---------+--------+",
                "| boolean | bigint | double | string    | boolean | letter |",
                "+---------+--------+--------+-----------+---------+--------+",
                "| false   | 6      | 6.6666 | 666666666 | false   | three  |",
                "| false   | 5      | 5.5555 | 555555555 | false   | three  |",
                "| false   | 4      | 4.4444 | 444444444 | false   | three  |",
                "| true    | 2      | 2.2222 | 222222222 | true    | two    |",
                "+---------+--------+--------+-----------+---------+--------+",
            ],
            exec_sql(
                &db,
                "SELECT bottom(BOOLEAN, 4), *
                FROM
                (VALUES
                    (1, 1.1111, '111111111', true, 'one'),
                    (2, 2.2222, '222222222', true, 'two'), 
                    (3, 3.3333, '333333333', true, 'three'),
                    (4, 4.4444, '444444444', false, 'three'),
                    (5, 5.5555, '555555555', false, 'three'),
                    (6, 6.6666, '666666666', false, 'three')
                ) AS t (BIGINT, DOUBLE, STRING, BOOLEAN, letter)",
            )
            .await,
        );

        assert_record_batches(
            vec![
                "+--------+--------+--------+-----------+---------+--------+",
                "| letter | bigint | double | string    | boolean | letter |",
                "+--------+--------+--------+-----------+---------+--------+",
                "| one    | 1      | 1.1111 | 111111111 | true    | one    |",
                "| three  | 4      | 4.4444 | 444444444 | false   | three  |",
                "| three  | 6      | 6.6666 | 666666666 | false   | three  |",
                "| three  | 5      | 5.5555 | 555555555 | false   | three  |",
                "| three  | 3      | 3.3333 | 333333333 | true    | three  |",
                "+--------+--------+--------+-----------+---------+--------+",
            ],
            exec_sql(
                &db,
                "SELECT bottom(letter, 5), *
                FROM
                (VALUES
                    (1, 1.1111, '111111111', true, 'one'),
                    (2, 2.2222, '222222222', true, 'two'), 
                    (3, 3.3333, '333333333', true, 'three'),
                    (4, 4.4444, '444444444', false, 'three'),
                    (5, 5.5555, '555555555', false, 'three'),
                    (6, 6.6666, '666666666', false, 'three')
                ) AS t (BIGINT, DOUBLE, STRING, BOOLEAN, letter)",
            )
            .await,
        );
    }

    #[tokio::test]
    #[should_panic(
        expected = "called `Result::unwrap()` on an `Err` value: Query { source: LogicalOptimize { source: Plan(\"Routine not match. Maybe (field_name, k). k is integer literal value. The range of values for k is [1, 255].\") } }"
    )]
    async fn test_topk_invalid_not_match_col_1() {
        let db = make_cnosdbms(Arc::new(MockEngine::default())).unwrap();

        assert_record_batches(
            vec![],
            exec_sql(
                &db,
                "SELECT topk(BOOLEAN), *
                FROM
                (VALUES
                    (1, 1.1111, '111111111', true, 'one'),
                    (2, 2.2222, '222222222', true, 'two'), 
                    (3, 3.3333, '333333333', true, 'three'),
                    (4, 4.4444, '444444444', false, 'three'),
                    (5, 5.5555, '555555555', false, 'three'),
                    (6, 6.6666, '666666666', false, 'three')
                ) AS t (BIGINT, DOUBLE, STRING, BOOLEAN, letter)",
            )
            .await,
            // exec_sql(
            //     &db,
            //     "SELECT topk(letter, 2), *
            //     FROM
            //     (VALUES
            //         (1, CAST(11 as SMALLINT), CAST(111111111 as INT), CAST(111111111 as BIGINT), CAST(1.1111 as DECIMAL(10, 4)), CAST(1.1 as REAL), CAST(1111.11111 as DOUBLE), date '1994-01-31' + interval '1' month as date, time '08:01:10.123456789' as time_nano,  to_timestamp_micros(1235865600000001), CAST('111111111' as CHAR(10)), CAST('111111111' as VARCHAR), true, 'one'),
            //         (2, CAST(22 as SMALLINT), CAST(222222222 as INT), CAST(222222222 as BIGINT), CAST(2.2222 as DECIMAL(10, 4)), CAST(2.2 as REAL), CAST(2222.22222 as DOUBLE), date '1994-02-30' + interval '1' month as date, time '08:02:10.123456'    as time_micro, to_timestamp_micros(1235865600100001), CAST('222222222' as CHAR(10)), CAST('222222222' as VARCHAR), true, 'two'),
            //         (3, CAST(33 as SMALLINT), CAST(333333333 as INT), CAST(333333333 as BIGINT), CAST(3.3333 as DECIMAL(10, 4)), CAST(3.3 as REAL), CAST(3333.33333 as DOUBLE), date '1994-03-31' + interval '1' month as date, time '08:03:10.123'       as time_milli, to_timestamp_micros(1235865600200001), CAST('333333333' as CHAR(10)), CAST('333333333' as VARCHAR), true, 'three'),
            //         (4, CAST(44 as SMALLINT), CAST(444444444 as INT), CAST(444444444 as BIGINT), CAST(4.4444 as DECIMAL(10, 4)), CAST(4.4 as REAL), CAST(4444.44444 as DOUBLE), date '1994-04-30' + interval '1' month as date, time '08:04:10'           as time,       to_timestamp_micros(1235865600300001), CAST('444444444' as CHAR(10)), CAST('444444444' as VARCHAR), false, 'three'),
            //         (5, CAST(55 as SMALLINT), CAST(555555555 as INT), CAST(555555555 as BIGINT), CAST(5.5555 as DECIMAL(10, 4)), CAST(5.5 as REAL), CAST(5555.55555 as DOUBLE), date '1994-05-31' + interval '1' month as date, time '08:05:10.123456789' as time_nano,  to_timestamp_micros(1235865600400001), CAST('555555555' as CHAR(10)), CAST('555555555' as VARCHAR), false, 'three'),
            //         (6, CAST(66 as SMALLINT), CAST(666666666 as INT), CAST(666666666 as BIGINT), CAST(6.6666 as DECIMAL(10, 4)), CAST(6.6 as REAL), CAST(6666.66666 as DOUBLE), date '1994-06-30' + interval '1' month as date, time '08:06:10.123456789' as time_nano,  to_timestamp_micros(1235865600500001), CAST('666666666' as CHAR(10)), CAST('666666666' as VARCHAR), false, 'three'),
            //     ) AS t (lit_BIGINT, SMALLINT, INT, BIGINT, DECIMAL, REAL, DOUBLE, DATE, TIME, TIMESTAMP, CHAR, VARCHAR, BOOLEAN, letter)",
            // )
            // .await,
        );
    }

    #[tokio::test]
    #[should_panic(
        expected = "called `Result::unwrap()` on an `Err` value: Query { source: LogicalOptimize { source: Plan(\"Routine not match. Maybe (field_name, k). k is integer literal value. The range of values for k is [1, 255].\") } }"
    )]
    async fn test_topk_invalid_not_match_col_3() {
        let db = make_cnosdbms(Arc::new(MockEngine::default())).unwrap();

        assert_record_batches(
            vec![],
            exec_sql(
                &db,
                "SELECT topk(BOOLEAN, 2, 9)
                FROM
                (VALUES
                    (1, 1.1111, '111111111', true, 'one'),
                    (2, 2.2222, '222222222', true, 'two'), 
                    (3, 3.3333, '333333333', true, 'three'),
                    (4, 4.4444, '444444444', false, 'three'),
                    (5, 5.5555, '555555555', false, 'three'),
                    (6, 6.6666, '666666666', false, 'three')
                ) AS t (BIGINT, DOUBLE, STRING, BOOLEAN, letter)",
            )
            .await,
        );
    }

    #[tokio::test]
    #[should_panic(
        expected = "called `Result::unwrap()` on an `Err` value: Query { source: LogicalOptimize { source: Plan(\"1. There cannot be nested selection functions. 2. There cannot be multiple selection functions., found: [\\n    TOPK(#t.boolean, Int64(2)),\\n    TOPK(#t.bigint, Int64(3)),\\n]\") } }"
    )]
    async fn test_topk_invalid_multi_selection_func() {
        let db = make_cnosdbms(Arc::new(MockEngine::default())).unwrap();

        assert_record_batches(
            vec![],
            exec_sql(
                &db,
                "SELECT topk(BOOLEAN, 2), topk(BIGINT, 3)
                FROM
                (VALUES
                    (1, 1.1111, '111111111', true, 'one'),
                    (2, 2.2222, '222222222', true, 'two'), 
                    (3, 3.3333, '333333333', true, 'three'),
                    (4, 4.4444, '444444444', false, 'three'),
                    (5, 5.5555, '555555555', false, 'three'),
                    (6, 6.6666, '666666666', false, 'three')
                ) AS t (BIGINT, DOUBLE, STRING, BOOLEAN, letter)",
            )
            .await,
        );
    }

    #[tokio::test]
    #[should_panic(
        expected = "called `Result::unwrap()` on an `Err` value: Query { source: LogicalOptimize { source: Plan(\"Routine not match. Maybe (field_name, k). k is integer literal value. The range of values for k is [1, 255].\") } }"
    )]
    async fn test_bottom_invalid_not_match_col_1() {
        let db = make_cnosdbms(Arc::new(MockEngine::default())).unwrap();

        assert_record_batches(
            vec![],
            exec_sql(
                &db,
                "SELECT bottom(BOOLEAN), *
                FROM
                (VALUES
                    (1, 1.1111, '111111111', true, 'one')
                ) AS t (BIGINT, DOUBLE, STRING, BOOLEAN, letter)",
            )
            .await,
        );
    }

    #[tokio::test]
    #[should_panic(
        expected = "called `Result::unwrap()` on an `Err` value: Query { source: LogicalOptimize { source: Plan(\"Routine not match. Maybe (field_name, k). k is integer literal value. The range of values for k is [1, 255].\") } }"
    )]
    async fn test_bottom_invalid_not_match_col_3() {
        let db = make_cnosdbms(Arc::new(MockEngine::default())).unwrap();

        assert_record_batches(
            vec![],
            exec_sql(
                &db,
                "SELECT bottom(BOOLEAN, 2, 9)
                FROM
                (VALUES
                    (1, 1.1111, '111111111', true, 'one'),
                    (2, 2.2222, '222222222', true, 'two'), 
                    (3, 3.3333, '333333333', true, 'three'),
                    (4, 4.4444, '444444444', false, 'three'),
                    (5, 5.5555, '555555555', false, 'three'),
                    (6, 6.6666, '666666666', false, 'three')
                ) AS t (BIGINT, DOUBLE, STRING, BOOLEAN, letter)",
            )
            .await,
        );
    }

    #[tokio::test]
    #[should_panic(
        expected = "called `Result::unwrap()` on an `Err` value: Query { source: LogicalOptimize { source: Plan(\"1. There cannot be nested selection functions. 2. There cannot be multiple selection functions., found: [\\n    BOTTOM(#t.boolean, Int64(2)),\\n    BOTTOM(#t.bigint, Int64(3)),\\n]\") } }"
    )]
    async fn test_bottom_invalid_multi_selection_func() {
        let db = make_cnosdbms(Arc::new(MockEngine::default())).unwrap();

        assert_record_batches(
            vec![],
            exec_sql(
                &db,
                "SELECT bottom(BOOLEAN, 2), bottom(BIGINT, 3)
                FROM
                (VALUES
                    (1, 1.1111, '111111111', true, 'one'),
                    (2, 2.2222, '222222222', true, 'two'), 
                    (3, 3.3333, '333333333', true, 'three'),
                    (4, 4.4444, '444444444', false, 'three'),
                    (5, 5.5555, '555555555', false, 'three'),
                    (6, 6.6666, '666666666', false, 'three')
                ) AS t (BIGINT, DOUBLE, STRING, BOOLEAN, letter)",
            )
            .await,
        );
    }
}
