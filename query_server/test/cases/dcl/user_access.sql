--#TENANT=cnosdb
--#USER_NAME=root
--#DATABASE=public
--#SORT=true
DROP USER root;
DROP TENANT IF EXISTS tenant_a;
DROP TENANT IF EXISTS tenant_b;
DROP USER IF EXISTS user_a;
DROP USER IF EXISTS user_b;
DROP USER IF EXISTS user_c;
DROP ROLE IF EXISTS role_a;


CREATE TENANT tenant_a;
CREATE USER user_a;
ALTER TENANT tenant_a ADD USER user_a AS owner;
CREATE ROLE role_a INHERIT member;
CREATE TENANT tenant_b;
CREATE USER user_b;
ALTER TENANT tenant_b ADD USER user_b AS owner;
CREATE USER user_c;


--#TENANT=tenant_a
--#USER_NAME=user_a
CREATE DATABASE db_a;
--#DATABASE=db_a
CREATE TABLE air_a (visibility DOUBLE,temperature DOUBLE,pressure DOUBLE,TAGS(station));
INSERT INTO air_a (TIME, station, visibility, temperature, pressure) VALUES(1666165200290401000, 'XiaoMaiDao', 56, 69, 77);
SELECT * FROM air_a;
--#TENANT=tenant_b
--#USER_NAME=user_b
SELECT * FROM air_a;
CREATE DATABASE db_b;
--#DATABASE=db_b
CREATE TABLE air_b (visibility DOUBLE,temperature DOUBLE,pressure DOUBLE,TAGS(station));
INSERT INTO air_b (TIME, station, visibility, temperature, pressure) VALUES(1666165200290401000, 'XiaoMaiDao', 56, 69, 77);
SELECT * FROM air_b;

--#USER_NAME=user_c
SELECT * FROM air_b;

--#USER_NAME=root
--#TENANT=tenant_a
ALTER TENANT tenant_a ADD USER user_c AS role_a;
GRANT READ ON DATABASE db_a TO ROLE role_a;
CREATE TABLE t1(id BIGINT);
INSERT t1 VALUES (1, 1);

--#USER_NAME=user_c
SELECT * FROM t1;
INSERT t1 VALUES (2, 2);

--#USER_NAME=root
GRANT WRITE ON DATABASE db_a TO ROLE role_a;

--#USER_NAME=user_c
SELECT * FROM t1;
INSERT t1 VALUES (3, 3);

--#USER_NAME=root
DROP USER user_c;
--#USER_NAME=user_c
SELECT * FROM t1;