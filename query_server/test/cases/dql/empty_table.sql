--#DATABASE=empty_table
--#SLEEP=100
DROP DATABASE IF EXISTS empty_table;

CREATE DATABASE empty_table;

DROP TABLE IF EXISTS empty;

CREATE TABLE empty (
    f DOUBLE,
    TAGS(t));

SELECT * FROM empty;
