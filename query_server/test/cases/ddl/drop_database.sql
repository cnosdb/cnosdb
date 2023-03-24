--#DATABASE=dropdatabase
--#SLEEP=100
CREATE DATABASE dropdatabase WITH TTL '100000d';

CREATE TABLE test0(
    column1 BIGINT CODEC(DELTA),
    column2 STRING CODEC(GZIP),
    column3 BIGINT UNSIGNED CODEC(NULL),
    column4 BOOLEAN,
    column5 DOUBLE CODEC(GORILLA),
    TAGS(column6, column7));

insert test0(TIME, column1, column2, column3, column4, column5, column6, column7) values (100, -1234, 'hello', 1234, false, 1.2, 'beijing', 'shanghai');

DROP DATABASE dropdatabase;

--#SLEEP=1000
CREATE TABLE test0(
    column1 BIGINT CODEC(DELTA),
    column2 STRING CODEC(GZIP),
    column3 BIGINT UNSIGNED CODEC(NULL),
    column4 BOOLEAN,
    column5 DOUBLE CODEC(GORILLA),
    TAGS(column6, column7));

--#SLEEP=1000
CREATE DATABASE dropdatabase WITH TTL '100000d';

CREATE TABLE test0(
    column1 BIGINT CODEC(DELTA),
    column2 STRING CODEC(GZIP),
    column3 BIGINT UNSIGNED CODEC(NULL),
    column4 BOOLEAN,
    column5 DOUBLE CODEC(GORILLA),
    TAGS(column6, column7));

select ALL * from test0;

DROP DATABASE dropdatabase;

DROP DATABASE public;