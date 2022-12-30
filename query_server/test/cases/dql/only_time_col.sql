--#DATABASE=only_time_col
--#SLEEP=100
--#SORT=true
DROP DATABASE IF EXISTS only_time_col;
CREATE DATABASE only_time_col WITH TTL '100000d';

drop table if exists m2;
CREATE TABLE IF NOT EXISTS m2(f0 BIGINT , f1 DOUBLE , TAGS(t0, t1, t2) );

INSERT m2(TIME, f0, f1, t0, t1) VALUES(101, 111, 444, 'tag11', 'tag21');
INSERT m2(TIME, f0, f1, t0, t1) VALUES(102, 222, 333, 'tag12', 'tag22');
INSERT m2(TIME, f0, f1, t0, t1) VALUES(103, 333, 222, 'tag13', 'tag23');
INSERT m2(TIME, f0, f1, t0, t1) VALUES(104, 444, 111, 'tag14', 'tag24');

-- expect error start
select time from m2;
select time, t0 from m2;
select time, t0, t1 from m2;
-- expect error end

select time, f0 from m2;
select time, f1 from m2;
