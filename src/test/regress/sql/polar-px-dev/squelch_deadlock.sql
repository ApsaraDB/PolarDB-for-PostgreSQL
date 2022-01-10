-- 
-- Squelch deadlock testing
--
-- ignore all the outputs, we just expect the timeout won't happen.
-- 

set statement_timeout = '15s';

-- Limit squelch deadlock test
-- (could deadlock only under 'explain analyze')

set client_min_messages to 'warning';
drop table if exists t_limit_squelch;
reset client_min_messages;

create table t_limit_squelch (id int);
insert into  t_limit_squelch values (generate_series(1,1000));

\o /dev/null

explain analyze
select *
from t_limit_squelch as v1, t_limit_squelch as v2
limit 10;

\o

-- Shared scan squelch deadlock test
-- 

set client_min_messages to 'warning';
drop table if exists t_shared_scan_squelch;
reset client_min_messages;

create table t_shared_scan_squelch (a int, b int);
insert into  t_shared_scan_squelch values
    (10,20), (11,20), (12,20), (13,20), (14,20), (15,20),
    (20,20), (21,20), (22,20), (23,20), (24,20), (25,20);

\o /dev/null

explain analyze
with v as (select a from t_shared_scan_squelch)
select * from v as v1, v as v2, v as v3
limit 10;

\o
