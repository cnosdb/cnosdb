-- normal
-- public.test(time i64, ta utf8, tb utf8, fa , fb)
-- curl -i -H "user_id: cnosdb" -H "database: public"  -XPOST 'http://127.0.0.1:31007/write/line_protocol' \
-- -d 'test,ta=a1,tb=b1 fa=1,fb=2 3'

-- insert all columns with single record
insert public.test(TIME, ta, tb, fa, fb)
values
    (7, '7a', '7b', 7, 7);

select * from public.test order by fa, fb;

-- insert all columns with multi records
insert public.test(TIME, ta, tb, fa, fb)
values
    (8, '8a', '8b', 8, 8),
    (9, '9a', '9b', 9, 9),
    (10, '10a', '10b', 10, 10);

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
