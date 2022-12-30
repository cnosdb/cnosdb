--#DATABASE=createdatabase
--#SLEEP=100
DROP DATABASE IF EXISTS createdatabase;

CREATE TABLE test0(
    column1 BIGINT CODEC(DELTA),
    column2 STRING CODEC(GZIP),
    column3 BIGINT UNSIGNED CODEC(NULL),
    column4 BOOLEAN,
    column5 DOUBLE CODEC(GORILLA),
    TAGS(column6, column7));

CREATE DATABASE createdatabase WITH TTL '100000d';

CREATE TABLE createddddatabase.test0(
    column1 BIGINT CODEC(DELTA),
    column2 STRING CODEC(GZIP),
    column3 BIGINT UNSIGNED CODEC(NULL),
    column4 BOOLEAN,
    column5 DOUBLE CODEC(GORILLA),
    TAGS(column6, column7));

CREATE TABLE test0(
    column1 BIGINT CODEC(DELTA),
    column2 STRING CODEC(GZIP),
    column3 BIGINT UNSIGNED CODEC(NULL),
    column4 BOOLEAN,
    column5 DOUBLE CODEC(GORILLA),
    TAGS(column6, column7));

DROP DATABASE IF EXISTS createdatabase;
