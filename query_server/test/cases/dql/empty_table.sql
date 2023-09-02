DROP DATABASE IF EXISTS empty_table;

CREATE DATABASE empty_table;

--#DATABASE=empty_table
--#SLEEP=100

CREATE TABLE empty (
    f DOUBLE,
    TAGS(t));

SELECT * FROM empty;
