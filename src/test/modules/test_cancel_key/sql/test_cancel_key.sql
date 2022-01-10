CREATE EXTENSION test_cancel_key;

-- disable cancel key
select pg_backend_pid() != 0 as result;

select count(*) from pg_stat_activity where pid = 0;
select count(*) from pg_stat_get_activity(pg_backend_pid()) where pid = 0;
select count(*) from pg_stat_get_activity(-1) where pid = 0;
select count(*) from pg_stat_get_activity(-2) where pid = 0;

set polar_virtual_pid = 0;
select pg_backend_pid() != 0 as result;
set polar_virtual_pid = 999;
select pg_backend_pid() != 999 as result;
set polar_virtual_pid = 10000009;
select pg_backend_pid() != 10000009 as result;

select count(*) from pg_stat_activity where pid > 10000000;
select count(*) from pg_stat_get_activity(pg_backend_pid()) where pid > 10000000;
select count(*) from pg_stat_get_activity(-1) where pid > 10000000;
select count(*) from pg_stat_get_activity(-2) where pid > 10000000;

set polar_cancel_key = 0;
set polar_cancel_key = -1;
set polar_cancel_key = 999;

select pg_cancel_backend(pg_backend_pid());

-- enable cancel key, we can set vpid and ckey
SELECT test_cancel_key(true);

select pg_backend_pid() != 0 as result;

select count(*) from pg_stat_activity where pid = 0;
select count(*) from pg_stat_get_activity(pg_backend_pid()) where pid = 0;
select count(*) from pg_stat_get_activity(-1) where pid = 0;
select count(*) from pg_stat_get_activity(-2) where pid = 0;

set polar_virtual_pid = 0;
select pg_backend_pid() != 0 as result;
set polar_virtual_pid = 999;
select pg_backend_pid() != 999 as result;
set polar_virtual_pid = 10000009;
select pg_backend_pid() = 10000009 as result;

select count(*) from pg_stat_activity where pid > 10000000;
select count(*) from pg_stat_get_activity(pg_backend_pid()) where pid > 10000000;
select count(*) from pg_stat_get_activity(-1) where pid > 10000000;
select count(*) from pg_stat_get_activity(-2) where pid > 10000000;

set polar_cancel_key = 0;
set polar_cancel_key = -1;
set polar_cancel_key = 999;

select pg_cancel_backend(pg_backend_pid());

-- disable cancel key again, vpid is gone
SELECT test_cancel_key(false);

select pg_backend_pid() = 10000009 as result;

select count(*) from pg_stat_activity where pid > 10000000;
select count(*) from pg_stat_get_activity(pg_backend_pid()) where pid > 10000000;
select count(*) from pg_stat_get_activity(-1) where pid > 10000000;
select count(*) from pg_stat_get_activity(-2) where pid > 10000000;

-- parallel woker test
SELECT test_cancel_key(true);
select pg_backend_pid() = 10000009 as result;
create table t(id int);
insert into t select generate_series(1,1234567);
explain (costs off) select count(*) from t;
select count(*) from t;

DROP EXTENSION test_cancel_key;
