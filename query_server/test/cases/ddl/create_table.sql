--#DATABASE=createtable
--#SORT = true
--#SLEEP=100
DROP DATABASE IF EXISTS createtable;
CREATE DATABASE createtable WITH TTL '100000d';
CREATE TABLE createtable."tesT"(column1 BIGINT,);
CREATE TABLE createtable.tesT;
CREATE TABLE createtable.tesT(column1 BIGINT,);
SHOW TABLES;

CREATE TABLE test0(
    column1 BIGINT CODEC(DELTA),
    column2 STRING CODEC(GZIP),
    column3 BIGINT UNSIGNED CODEC(NULL),
    column4 BOOLEAN,
    column5 DOUBLE CODEC(GORILLA),
    TAGS(column6, column7));

insert createtable.test0(TIME, column1, column2, column3, column4, column5, column6, column7) values (100, -1234, 'hello', 1234, false, 1.2, 'beijing', 'shanghai');
insert createtable.test0(TIME, column1, column2, column3, column4, column5, column6, column7) values (101, -1234, 'hello', -1234, false, 1.2, 'beijing', 'shanghai');
insert createtable.test0(TIME, column1, column2, column3, column4, column5, column6, column7) values (102, -1234, 'hello', 1234, false, 'failed', 'beijing', 'shanghai');
insert createtable.test0(TIME, column1, column2, column3, column4, column5, column6, column7) values (0.1, -1234, 'hello', 1234, true, 1.2, 'beijing', 'shanghai');
insert createtable.test0(TIME, column1, column2, column3, column4, column5, column6, column7) values (103, -1234, 'hello', 1234, false, 1.2, 'beijing', 'shanghai');
insert createtable.test0(TIME, column1, column2, column3, column4, column5, column6, column7) values (104, -1234, 'hello', 1234, false, 1.2, 'beijing', 'shanghai');
insert createtable.test0(TIME, column2, column3, column4, column5, column6) values (100, 'hello', 1234, false, 1.2, 'beijing');
insert createtable.test0(TIME, column1, column2, column3, column4, column5) values (100, -1234, 'hello', 1234, false, 1.2);


select ALL * from createtable.test0;
select ALL * from test0;
select ALL * from public.test0;

ALTER TABLE test0 DROP column2;

select ALL * from test0;

ALTER TABLE test0 DROP column7;

SELECT ALL * FROM test0;

ALTER TABLE test0 ADD TAG column8;

SELECT ALL * FROM test0;

ALTER TABLE test0 ADD TAG column7;

SELECT ALL * FROM test0;


CREATE TABLE test1(
    column0 TIMESTAMP CODEC(DELTA),
    column1 BIGINT CODEC(DELTA),
    column2 STRING CODEC(GZIP),
    column3 BIGINT UNSIGNED CODEC(NULL),
    column4 BOOLEAN,
    column5 DOUBLE CODEC(GORILLA),
    TAGS(column6, column7));

CREATE TABLE test2(
    column1 BIGINT CODEC(DELTA),
    column2 STRING CODEC(GZIP),
    column3 BIGINT UNSIGNED CODEC(NULL),
    column4 BOOLEAN,
    column5 DOUBLE CODEC(GORILLA));

CREATE TABLE test0(
    column1 BIGINT CODEC(DELTA),
    column2 STRING CODEC(GZIP),
    column3 BIGINT UNSIGNED CODEC(NULL),
    column4 BOOLEAN,
    column5 DOUBLE CODEC(GORILLA),
    TAGS(column6, column7));

CREATE TABLE IF NOT EXISTS test0(
    column1 BIGINT CODEC(DELTA),
    column2 STRING CODEC(GZIP),
    column3 BIGINT UNSIGNED CODEC(NULL),
    column4 BOOLEAN,
    column5 DOUBLE CODEC(GORILLA),
    TAGS(column6, column7));

CREATE TABLE IF NOT EXISTS test3(
    column1 BIGINT CODEC(DELTA),
    column1 BIGINT CODEC(DELTA),
    column2 STRING CODEC(GZIP),
    column3 BIGINT UNSIGNED CODEC(NULL),
    column4 BOOLEAN,
    column5 DOUBLE CODEC(GORILLA),
    TAGS(column6, column7));

CREATE TABLE IF NOT EXISTS test4(
    column1 BIGINT CODEC(DEL),
    column2 STRING CODEC(GZIP),
    column3 BIGINT UNSIGNED CODEC(NULL),
    column4 BOOLEAN,
    column5 DOUBLE CODEC(GORILLA),
    TAGS(column6, column7));

CREATE TABLE IF NOT EXISTS test5(
    column1 BIGINT CODEC(DELTA),
    column2 STRING CODEC(GZIP),
    column3 BIGINT UNSIGNED CODEC(NULL),
    column4 BOOLE,
    column5 DOUBLE CODEC(GORILLA),
    TAGS(column6, column7));

CREATE TABLE IF NOT EXISTS test6(
    column1 BIGINT CODEC(delta),
    column2 STRING CODEC(GZIP),
    column3 BIGINT UNSIGNED CODEC(null),
    column4 BOOLEAN CODEC(BITPACK),
    column5 DOUBLE CODEC(GORILLA),
    TAGS(column6, column7));

CREATE TABLE IF NOT EXISTS test7(
    column1 BIGINT CODEC(DELTA),
    column2 STRING CODEC(GZIP),
    column3 BIGINT UNSIGNED CODEC(),
    column4 BOOLEAN,
    column5 DOUBLE CODEC(GORILLA),
    TAGS(column6, column7));

DROP TABLE test0;

SELECT ALL * FROM test0;

CREATE TABLE test0(
    column1 BIGINT CODEC(DELTA),
    column3 BIGINT UNSIGNED CODEC(NULL),
    column4 BOOLEAN,
    column5 DOUBLE CODEC(GORILLA),
    TAGS(column6, column8, column7));

SELECT ALL * FROM test0;

DROP DATABASE IF EXISTS createtable;
