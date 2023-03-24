--#SORT=true
--#SLEEP=100
-- normal
-- public.test(time i64, ta utf8, tb utf8, fa , fb)
-- insert all columns with single record
--#LP_BEGIN
test,ta=a1,tb=b1 fa=1,fb=2 3
--#LP_END

--#LP_BEGIN
test,ta=a1,tb=b1 fa=1,fb=2 1667456411000000000
--#LP_END

insert public.test(TIME, ta, tb, fa, fb)
values
    (1667456411000000007, '7a', '7b', 7, 7);

select * from public.test order by fa, fb;

-- insert all columns with multi records
insert public.test(TIME, ta, tb, fa, fb)
values
    (1667456411000000008, '8a', '8b', 8, 8),
    (1667456411000000009, '9a', '9b', 9, 9),
    (1667456411000000010, '10a', '10b', 10, 10);

-- query all columns
select * from public.test order by fa, fb;

-- query time/tag/field column
select time, ta, fa from public.test order by fa, fb;

-- query time/field column
select time, fa from public.test order by fa, fb;

-- query time/tag column
select time, ta from public.test order by fa, fb;

-- query time column
select time from public.test order by fa, fb;

-- query tag column
select ta from public.test order by fa, fb;

-- query field column
select fa from public.test order by fa, fb;

-- error
-- query duplicate column
select time, ta, fa, * from public.test order by fa, fb;

CREATE TABLE air (
     visibility DOUBLE,
     temperature DOUBLE,
     presssure DOUBLE,
     TAGS(station,region)
);

INSERT INTO air (TIME, station, visibility, temperature, presssure) VALUES ('2022-10-19 06:40:00', 'XiaoMaiDao', 55, 68, 76);
INSERT INTO air (TIME, station, visibility, temperature, presssure) VALUES ('2022-10-19 06:41:00', NULL, 56, 69, 77);
INSERT INTO air (TIME, station, visibility, temperature, presssure) VALUES ('2022-10-19 06:42:00', 'XiaoMaiDao', NULL, NULL, NULL);
INSERT INTO air (TIME, station, visibility, temperature, presssure) VALUES ('2022-10-19 06:43:00', NULL, NULL, NULL, NULL);
