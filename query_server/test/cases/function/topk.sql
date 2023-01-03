--#DATABASE=topk_func
--#SLEEP=100
--#SORT=true
drop database if exists topk_func;
create database topk_func WITH TTL '100000d';

drop table if exists m2;
CREATE TABLE IF NOT EXISTS m2(f0 BIGINT , f1 DOUBLE , TAGS(t0, t1, t2) ); -- 1ms;

INSERT m2(TIME, f0, f1, t0, t1) VALUES(101, 111, 444, 'tag11', 'tag21');
INSERT m2(TIME, f0, f1, t0, t1) VALUES(102, 222, 333, 'tag12', 'tag22');
INSERT m2(TIME, f0, f1, t0, t1) VALUES(103, 333, 222, 'tag13', 'tag23');
INSERT m2(TIME, f0, f1, t0, t1) VALUES(104, 444, 111, 'tag14', 'tag24');

select topk(time, 2), * from m2;
select topk(t0, 3), * from m2;
select topk(t1, 2), * from m2;
select topk(f0, 2), * from m2;
select topk(f1, 3), * from m2;

-- error
select topk(time, 2), topk(t0, 3) from m2;
