--
-- Test cases for COPY (select) TO
--
create table test1 (id serial, t text);
insert into test1 (t) values ('a');
insert into test1 (t) values ('b');
insert into test1 (t) values ('c');
insert into test1 (t) values ('d');
insert into test1 (t) values ('e');

create table test2 (id serial, t text);
insert into test2 (t) values ('A');
insert into test2 (t) values ('B');
insert into test2 (t) values ('C');
insert into test2 (t) values ('D');
insert into test2 (t) values ('E');

create view v_test1
as select 'v_'||t from test1;

--
-- Test COPY table TO
--
copy test1 to stdout;
--
-- This should fail
--
copy v_test1 to stdout;
--
-- Test COPY (select) TO
--
-- POLAR_TAG: EQUAL
copy (select t from test1 where id=1) to stdout;
--
-- Test COPY (select for update) TO
--
-- POLAR_TAG: REPLICA_ERR 
copy (select t from test1 where id=3 for update) to stdout;
--
-- This should fail
--
-- POLAR_TAG: EQUAL
copy (select t into temp test3 from test1 where id=3) to stdout;
--
-- This should fail
--
copy (select * from test1) from stdin;
-- POLAR_END_COPY
--
-- This should fail
--
-- POLAR_TAG: EQUAL
copy (select * from test1) (t,id) to stdout;
--
-- Test JOIN
--
-- POLAR_TAG: EQUAL
copy (select * from test1 join test2 using (id)) to stdout;
--
-- Test UNION SELECT
--
-- POLAR_TAG: EQUAL
copy (select t from test1 where id = 1 UNION select * from v_test1 ORDER BY 1) to stdout;
--
-- Test subselect
--
-- POLAR_TAG: EQUAL
copy (select * from (select t from test1 where id = 1 UNION select * from v_test1 ORDER BY 1) t1) to stdout;
--
-- Test headers, CSV and quotes
--
-- POLAR_TAG: EQUAL
copy (select t from test1 where id = 1) to stdout csv header force quote t;
--
-- Test psql builtins, plain table
--
\copy test1 to stdout
--
-- This should fail
--
\copy v_test1 to stdout
--
-- Test \copy (select ...)
--
\copy (select "id",'id','id""'||t,(id + 1)*id,t,"test1"."t" from test1 where id=3) to stdout
--
-- Drop everything
--
drop table test2;
drop view v_test1;
drop table test1;

-- psql handling of COPY in multi-command strings
copy (select 1) to stdout\; select 1/0;	-- row, then error
select 1/0\; copy (select 1) to stdout; -- error only
copy (select 1) to stdout\; copy (select 2) to stdout\; select 0\; select 3; -- 1 2 3

create table test3 (c int);
-- POLAR_SESSION_BEGIN
-- POLAR_TAG: REPLICA_ERR
select 0\; copy test3 from stdin\; copy test3 from stdin\; select 1; -- 1		
1
\.
2
\.
-- POLAR_SESSION_END
select * from test3;
drop table test3;
