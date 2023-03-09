--#DATABASE=explain_stream_query
--#SLEEP=100
DROP DATABASE IF EXISTS explain_stream_query;
CREATE DATABASE explain_stream_query;

CREATE TABLE test0(
    column1 BIGINT CODEC(DELTA),
    column2 STRING CODEC(GZIP),
    column3 BIGINT UNSIGNED CODEC(NULL),
    column4 BOOLEAN,
    column5 DOUBLE CODEC(GORILLA),
    TAGS(column6, column7));

CREATE STREAM TABLE TskvTable (
    time TIMESTAMP,
    column1 STRING,
    column6 STRING
) WITH (
    db = 'explain_stream_query',
    table = 'test0',
    event_time_column = 'time'
) engine = tskv;

CREATE STREAM TABLE TskvTableWithoutSchema WITH (
    db = 'explain_stream_query',
    table = 'test0',
    event_time_column = 'time'
) engine = tskv;

CREATE STREAM TABLE TskvTableWithoutDB WITH (
    table = 'test0',
    event_time_column = 'time'
) engine = tskv;


CREATE STREAM TABLE TskvTableWithoutTable WITH (
    db = 'explain_stream_query',
    event_time_column = 'time'
) engine = tskv;

CREATE STREAM TABLE TskvTableWithoutWM WITH (
    db = 'explain_stream_query',
    table = 'test0'
) engine = tskv;

select * from information_schema.tables where table_database = 'explain_stream_query' order by table_name;

explain select * from TskvTable;

explain select * from TskvTableWithoutSchema;
