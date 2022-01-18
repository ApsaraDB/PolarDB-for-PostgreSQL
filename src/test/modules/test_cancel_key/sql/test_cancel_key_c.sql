-- This is a proxy process
select pg_sleep(0.5); -- 0.5s
SELECT test_cancel_key(true);
set polar_virtual_pid = 10000009;

select pg_sleep(0.5); -- 1.0s
select pid from pg_stat_activity where pid = 0; -- None (b is unset and will return real pid)
select pid from pg_stat_activity where pid > 10000000 order by pid; -- 10000009

select pg_sleep(2); -- 3.0s
set polar_virtual_pid = 10000001; -- will report error, for b is using it
select pg_cancel_backend(10000001); -- cancel b

select pg_sleep(10); -- 13s and will be canceled by a
