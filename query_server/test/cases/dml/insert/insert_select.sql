--#SORT=true
--#SLEEP=100
-- normal

-- public.test_insert_subquery(time i64, ta utf8, tb utf8, fa , fb)
-- insert all columns
--   1. single record
--   2. column name not match
--#LP_BEGIN
test_insert_subquery,ta=a1,tb=b1 fa=1,fb=2 3
--#LP_END

--#LP_BEGIN
test_insert_subquery,ta=a1,tb=b1 fa=1,fb=2 1667456411000000000
--#LP_END

insert public.test_insert_subquery(TIME, ta, tb, fa, fb)
select column1, column2, column3, column4, column5
from
(values
    (1667456411000000007, '7a', '7b', 7, 7));

select * from public.test_insert_subquery order by fa, fb;

-- insert all columns
--   1. single record
--   2. column name match
insert public.test_insert_subquery(TIME, ta, tb, fa, fb)
select TIME, ta, tb, fa, fb
from
(values
    (1667456411000000008, '8a', '8b', 8, 8)) as t (TIME, ta, tb, fa, fb);

select * from public.test_insert_subquery order by fa, fb;

-- insert all columns
--   1. single record
--   2. column name *
insert public.test_insert_subquery(TIME, ta, tb, fa, fb)
select *
from
(values
    (1667456411000000009, '9a', '9b', 9, 9)) as t (TIME, ta, tb, fa, fb);

select * from public.test_insert_subquery order by fa, fb;

-- insert partial columns
--   1. single record
--   2. column name *
--   3. not specify insert columns
insert public.test_insert_subquery
select *
from
(values
    (1667456411000000009, '9a', '9b', 9, 9)) as t (TIME, ta, tb, fa, fb);

select * from public.test_insert_subquery order by fa, fb;

-- error
--   1. single record
--   2. Insert columns and Source columns not match
--   3. not specify insert columns
insert public.test_insert_subquery
select TIME, ta
from
(values
    (1667456411000000010, '10a', '10b', 10, 10)) as t (TIME, ta, tb, fa, fb);

select * from public.test_insert_subquery order by fa, fb;

-- error
--   1. single record
--   2. Insert columns and Source columns not match
--   3. specify partial insert columns
insert public.test_insert_subquery(TIME, fa)
select TIME, ta
from
(values
    (1667456411000000010, '10a', '10b', 10, 10)) as t (TIME, ta, tb, fa, fb);

select * from public.test_insert_subquery order by fa, fb;
