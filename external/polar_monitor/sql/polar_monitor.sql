-- Test access privileges
--
--setup
create extension polar_monitor;

-- buffer monitor
select COUNT(*) >= 0 AS result from polar_normal_buffercache;
select COUNT(*) >= 0 AS result from polar_copy_buffercache;
select COUNT(polar_flushlist()) >= 0 As result;
select COUNT(polar_cbuf()) >= 0 As result;

--cleanup
drop extension polar_monitor;
