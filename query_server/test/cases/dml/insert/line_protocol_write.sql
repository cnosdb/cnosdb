
drop database if exists test_lp_writer;
create database test_lp_writer with TTL '10000d';

drop user if exists writer;
create user writer;

drop role if exists lp_writer;
create role lp_writer inherit member;

grant read on database lp_writer to role lp_writer;
alter tenant cnosdb add user writer as lp_writer;

--#USER_NAME=writer
--#DATABASE=test_lp_writer
-- TODO Beautify the error message
--#m,t0=t0,t1=t1 f0=1,f1=2I 1675153612615241000
