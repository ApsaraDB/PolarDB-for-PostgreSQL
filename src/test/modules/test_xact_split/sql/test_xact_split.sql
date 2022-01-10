CREATE EXTENSION polar_monitor;
CREATE EXTENSION test_xact_split;
create table tmp(id int);

select test_xact_split_mock_ro();
SELECT test_xact_split();

begin;
set polar_xact_split_xids = '1,100';
reset polar_xact_split_xids;
select * from tmp; /* should report ERROR */
end;

select test_xact_split_reset_mock();

create table t(id int);
begin;
lock table t;
select splittable from polar_stat_xact_split_info();
end;

select splittable from polar_stat_xact_split_info();

begin;
lock table t;
savepoint a;
select splittable from polar_stat_xact_split_info();
insert into t select 1;
select splittable from polar_stat_xact_split_info();
savepoint b;
select splittable from polar_stat_xact_split_info();
rollback to savepoint a;
select splittable from polar_stat_xact_split_info();
end;

select splittable from polar_stat_xact_split_info();

begin;
insert into t select 1;
select splittable from polar_stat_xact_split_info();
savepoint a;
select splittable from polar_stat_xact_split_info();
lock table t;
select splittable from polar_stat_xact_split_info();
savepoint b;
select splittable from polar_stat_xact_split_info();
rollback to savepoint a;
select splittable from polar_stat_xact_split_info();
end;

select splittable from polar_stat_xact_split_info();
