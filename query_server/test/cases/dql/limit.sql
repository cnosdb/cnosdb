--#DATABASE=limit
--#SLEEP=100
--#SORT=false
drop database if exists limit;
create database limit WITH TTL '100000d';

drop table if exists limit_test;
CREATE TABLE IF NOT EXISTS limit_test(f0 BIGINT , f1 DOUBLE , TAGS(t0, t1) );

INSERT limit_test(TIME, f0, f1, t0, t1) VALUES
    (101, 111, 444, 'tag11', 'tag21'),
    (102, 222, 333, 'tag12', 'tag22'),
    (103, 333, 222, 'tag13', 'tag23'),
    (104, 444, 111, 'tag14', 'tag24'),
    (201, 111, 444, 'tag11', 'tag21'),
    (202, 222, 333, 'tag12', 'tag22'),
    (203, 333, 222, 'tag13', 'tag23'),
    (204, 444, 111, 'tag14', 'tag24'),
    (301, 111, 444, 'tag11', 'tag26'),
    (302, 222, 333, 'tag12', 'tag27');

-- Verify limit table scan --
select count(*) from (select * from limit_test limit 5);
