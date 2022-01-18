-- This is a proxy process
select pg_sleep(0.5); -- 0.5s
SELECT test_cancel_key(true);

select pg_sleep(0.5); -- 1.0s
select pid from pg_stat_activity where pid = 0; -- None (b is unset and will return real pid)
select pid from pg_stat_activity where pid > 10000000 order by pid; -- 10000009

select pg_sleep(0.5); -- 1.5s
set polar_virtual_pid = 10000001;
select pid from pg_stat_activity where pid = 0; -- None
select pid from pg_stat_activity where pid > 10000000 order by pid; -- 10000001 and 10000009

select pg_sleep(2); -- 3.5 and will be canceled
