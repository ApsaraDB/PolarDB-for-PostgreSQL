-- Test access privileges
--
--setup
create extension polar_monitor;

create user polar_monitor password 'test';
create table polar_monitor(pass varchar);
insert into polar_monitor select rolpassword from pg_authid where rolname='polar_monitor';
drop user polar_monitor;

create user polar_monitor with superuser password 'test';
drop user polar_monitor;
drop table polar_monitor;

-- test polar multi version snapshot store dynamic view
select count(*) from polar_multi_version_snapshot_store_info;

-- slru stat
SELECT DISTINCT is_equal_result FROM (SELECT slots_number = (valid_pages + empty_pages + reading_pages + writing_pages) is_equal_result FROM polar_stat_slru()) t;
SELECT COUNT(*) >= 9 FROM polar_stat_slru();

select polar_used_logindex_fullpage_snapshot_mem_tbl_size() >= 0 as result;

-- test polar_stat_shmem
SELECT COUNT(*) > 10 AS result FROM polar_stat_shmem;

-- test polar_stat_shmem_total_size
SELECT COUNT(*) > 0 AS result FROM polar_stat_shmem_total_size;

-- polar_stat_activity
select a = b is_equal from (select
(select count(*) from polar_stat_activity) a,
(select count(*) from pg_stat_activity) b
) c;
-- polar_stat_io_info
select count(*)>6 from polar_stat_io_info;
-- polar_stat_io_latency
select count(*) from polar_stat_io_latency;

-- check polar dynamic bgworker
SELECT COUNT(*) = 1 FROM polar_stat_activity WHERE backend_type='polar worker process';

-- check polar stat activity_rt
select COUNT(*) >= 7 AS backends from polar_stat_activity_rt;

-- buffer monitor
select COUNT(*) >= 0 AS result from polar_copy_buffercache;
select COUNT(polar_flushlist()) >= 0 As result;
select COUNT(polar_cbuf()) >= 0 As result;

-- check stat cgroup
select COUNT(*)  >= 0 AS result from polar_stat_cgroup;
select COUNT(*)  >= 0 AS result from polar_cgroup_quota;

-- check polar wal pipeline view
select COUNT(*)  >= 0 AS result from polar_wal_pipeline_info;
select COUNT(*)  >= 0 AS result from polar_wal_pipeline_stats;

-- check polar proxy info view
select count(*) = 11 from polar_stat_proxy_info;
select count(*) = 11 from polar_stat_proxy_info_rt;
select count(*) = 0 from polar_stat_reset_proxy_info();

select count(*) >= 0 from polar_stat_session where backend_type='client backend';
select count(*) >= 0 from polar_stat_dispatcher;
select polar_dispatcher_pid() - polar_dispatcher_pid(), polar_session_backend_pid() - polar_session_backend_pid(), pg_backend_pid() - pg_backend_pid();

--cleanup
drop extension polar_monitor;
