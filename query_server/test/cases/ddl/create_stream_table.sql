--#DATABASE=createstreamtable
--#SLEEP=100
DROP DATABASE IF EXISTS createstreamtable;
CREATE DATABASE createstreamtable;

CREATE TABLE test0(
    column1 BIGINT CODEC(DELTA),
    column2 STRING CODEC(GZIP),
    column3 BIGINT UNSIGNED CODEC(NULL),
    column4 BOOLEAN,
    column5 DOUBLE CODEC(GORILLA),
    TAGS(column6, column7));

CREATE STREAM TABLE TskvTable (
    time TIMESTAMP,
    name STRING,
    driver STRING,
    load_capacity DOUBLE
) WITH (
    db = 'createstreamtable',
    table = 'test0',
    event_time_column = 'time'
) engine = tskv;

select * from information_schema.tables where table_database = 'createstreamtable' order by table_name;
