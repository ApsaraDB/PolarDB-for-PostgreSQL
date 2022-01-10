-- This is a normal process
CREATE EXTENSION test_cancel_key;

select pg_sleep(1); -- 1.0s
select pid from pg_stat_activity where pid = 0; -- None (b is unset and will return real pid)
select pid from pg_stat_activity where pid > 10000000 order by pid; -- 10000009

select pg_sleep(1.5); -- 2.5s
select pid from pg_stat_activity where pid = 0; -- None
select pid from pg_stat_activity where pid > 10000000 order by pid; -- 10000001 and 10000009
select pid from pg_locks where pid = 0; -- for coverage test

select pg_sleep(2); -- 3.5s
select pg_cancel_backend(10000009); -- kill c
select pg_cancel_backend(10000099); -- no such process
DROP EXTENSION test_cancel_key;
