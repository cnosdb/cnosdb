--#DATABASE=time_window
--#SLEEP=100
--#SORT=true
drop database if exists time_window;
create database time_window WITH TTL '1000000d';

drop table if exists m2;
CREATE TABLE IF NOT EXISTS m2(f0 BIGINT , f1 DOUBLE , TAGS(t0, t1, t2) );

INSERT m2(TIME, f0, f1, t0, t1) VALUES('1999-12-31 00:00:00.000', 111, 444, 'tag11', 'tag21');
INSERT m2(TIME, f0, f1, t0, t1) VALUES('1999-12-31 00:00:00.005', 222, 333, 'tag12', 'tag22');
INSERT m2(TIME, f0, f1, t0, t1) VALUES('1999-12-31 00:00:00.010', 333, 222, 'tag13', 'tag23');
INSERT m2(TIME, f0, f1, t0, t1) VALUES('1999-12-31 00:00:00.015', 444, 111, 'tag14', 'tag24');

-- error 
select time_window(time, '0.003ms', '0.001ms'), * from m2;
select time_window(time, '3ms', '0.001ms'), * from m2;
select time_window(time, '1ms', '366d'), * from m2;
-- error not single time_window
select time_window(time, '10ms', '6ms') as window, time_window(time, '10ms', '7ms'), * 
from m2 order by window, time;

-- normal
select time_window(time, '10ms', '6ms') as window, * from m2 order by window, time;

drop table if exists err;
CREATE TABLE IF NOT EXISTS err(f0 BIGINT , f1 DOUBLE , TAGS(t0, t1, t2) );

-- max timestamp
INSERT err(TIME, f0, f1, t0, t1) VALUES('2262-04-11 23:47:16.854775807', 111, 444, 'tag11', 'tag21');
-- min timestamp
INSERT err(TIME, f0, f1, t0, t1) VALUES('1677-09-21 00:12:44.0', 111, 444, 'tag11', 'tag21');
INSERT err(TIME, f0, f1, t0, t1) VALUES('1688-09-21 00:12:44.0', 111, 444, 'tag11', 'tag21');
INSERT err(TIME, f0, f1, t0, t1) VALUES('1970-01-01T00:00:00', 111, 444, 'tag11', 'tag21');
INSERT err(TIME, f0, f1, t0, t1) VALUES('1980-01-01T00:00:00', 111, 444, 'tag11', 'tag21');

select time_window(time, '10ms', '6ms') as window, * from err order by window, time;

select time_window(time, '10ms', '6ms') as window_start, sum(f0) as sum, count(f1) 
from err  
group by window_start 
order by window_start, sum;
