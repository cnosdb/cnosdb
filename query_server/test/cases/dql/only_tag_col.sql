--#DATABASE=only_tag_col
--#SLEEP=100
--#SORT=true
drop database if exists only_tag_col;
create database only_tag_col WITH TTL '100000d';

drop table if exists m2;
CREATE TABLE IF NOT EXISTS m2(f0 BIGINT , f1 DOUBLE , TAGS(t0, t1, t2) );

INSERT m2(TIME, f0, f1, t0, t1) VALUES
(101, 111, 444, 'tag11', 'tag21'),
(102, 222, 333, 'tag12', 'tag22'),
(103, 333, 222, 'tag13', 'tag23'),
(104, 444, 111, 'tag14', 'tag24'),
(201, 111, 444, 'tag11', 'tag21'),
(202, 222, 333, 'tag12', 'tag22'),
(203, 333, 222, 'tag13', 'tag23'),
(204, 444, 111, 'tag14', 'tag24'),
(301, 111, 444, 'tag11', 'tag26'),
(302, 222, 333, 'tag12', 'tag27'),
(303, 333, 222, 'tag13', 'tag28'),
(304, 444, 111, 'tag14', 'tag29'),
(101, 111, 444, 'tag16', 'tag21'),
(102, 222, 333, 'tag17', 'tag22'),
(103, 333, 222, 'tag18', 'tag23'),
(104, 444, 111, 'tag19', 'tag24');

-- tag scan
-- select t0 from m2;
-- select t1 from m2;
-- error
select time, t0 from m2;
-- not tag scan
select t0, f0 from m2;
select t0, t1, f0 from m2;
select time, t0, t1, f0 from m2;
