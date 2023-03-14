--#DATABASE=precision
--#precision=us
--#SLEEP=100
CREATE DATABASE precision WITH TTL '100000d' precision 'us';

create table test(col bigint,);

insert into test(time, col) values (1667456411000001, 10);

select * from test;

drop database precision;

CREATE DATABASE precision WITH TTL '100000d' precision 'ms';

create table test(col bigint,);

insert into test(time, col) values (1667456411000001, 10);

select * from test;

alter database precision set precision 'us';
