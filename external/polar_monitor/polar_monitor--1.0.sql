/* external/polar_monitor/polar_monitor--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION polar_monitor" to load this file. \quit

-- Register the function.
CREATE FUNCTION polar_consistent_lsn()
RETURNS pg_lsn
AS 'MODULE_PATHNAME', 'polar_consistent_lsn'
LANGUAGE C PARALLEL SAFE;

CREATE FUNCTION polar_oldest_apply_lsn()
RETURNS pg_lsn
AS 'MODULE_PATHNAME', 'polar_oldest_apply_lsn'
LANGUAGE C PARALLEL SAFE;

CREATE FUNCTION polar_oldest_lock_lsn()
RETURNS pg_lsn
AS 'MODULE_PATHNAME', 'polar_oldest_lock_lsn'
LANGUAGE C PARALLEL SAFE;

-- Register the normal buffer function.
CREATE FUNCTION polar_get_normal_buffercache_pages()
RETURNS SETOF RECORD
AS 'MODULE_PATHNAME', 'polar_get_normal_buffercache_pages'
LANGUAGE C PARALLEL SAFE;

-- Create a view for normal buffer convenient access.
CREATE VIEW polar_normal_buffercache AS
        SELECT P.* FROM polar_get_normal_buffercache_pages() AS P
            (bufferid integer, relfilenode oid, reltablespace oid, reldatabase oid,
             relforknumber int2, relblocknumber int8, isdirty bool, usagecount int2,
             oldest_lsn pg_lsn, newest_lsn pg_lsn, flushnext int4, flushprev int4,
             incopybuf bool,first_touched_after_copy bool, pinning_backends int4,
             recently_modified_count int2, oldest_lsn_is_fake bool);

-- Don't want these to be available to public.
REVOKE ALL ON FUNCTION polar_get_normal_buffercache_pages() FROM PUBLIC;
REVOKE ALL ON polar_normal_buffercache FROM PUBLIC;

-- Register the copy buffer function.
CREATE FUNCTION polar_get_copy_buffercache_pages()
RETURNS SETOF RECORD
AS 'MODULE_PATHNAME', 'polar_get_copy_buffercache_pages'
LANGUAGE C PARALLEL SAFE;

-- Create a view for copy buffer convenient access.
CREATE VIEW polar_copy_buffercache AS
        SELECT P.* FROM polar_get_copy_buffercache_pages() AS P
        (bufferid integer, relfilenode oid, reltablespace oid, reldatabase oid,
         relforknumber int2, relblocknumber int8, freenext int4, passcount int4,
         state int2, oldest_lsn pg_lsn, newest_lsn pg_lsn, is_flushed bool);

-- Don't want these to be available to public.
REVOKE ALL ON FUNCTION polar_get_copy_buffercache_pages() FROM PUBLIC;
REVOKE ALL ON polar_copy_buffercache FROM PUBLIC;

CREATE FUNCTION polar_flushlist(OUT size int8,
                                OUT put int8,
                                OUT remove int8,
                                OUT find int8,
                                OUT batchread int8,
                                OUT cbuf int8)
RETURNS record
AS 'MODULE_PATHNAME', 'polar_flushlist'
LANGUAGE C PARALLEL SAFE;

CREATE FUNCTION polar_cbuf(OUT flush int8,
                           OUT copy int8,
                           OUT unavailable int8,
                           OUT full int8,
                           OUT release int8)
RETURNS record
AS 'MODULE_PATHNAME', 'polar_cbuf'
LANGUAGE C PARALLEL SAFE;

CREATE FUNCTION polar_node_type()
    RETURNS text
AS 'MODULE_PATHNAME', 'polar_get_node_type'
    LANGUAGE C PARALLEL SAFE;

-- POLAR: watch async ddl lock replay worker stat
CREATE FUNCTION polar_stat_async_ddl_lock_replay_worker()
RETURNS SETOF RECORD
AS 'MODULE_PATHNAME', 'polar_stat_async_ddl_lock_replay_worker'
LANGUAGE C PARALLEL SAFE;

CREATE VIEW polar_stat_async_ddl_lock_replay_worker AS
	SELECT P.* FROM polar_stat_async_ddl_lock_replay_worker() AS P
		(id int4, pid int4, xid int4, lsn pg_lsn, commit_state text,
		dbOid oid, relOid oid, rtime timestamp with time zone, state text);

-- POLAR: watch async ddl lock replay transaction stat
CREATE FUNCTION polar_stat_async_ddl_lock_replay_transaction()
RETURNS SETOF RECORD
AS 'MODULE_PATHNAME', 'polar_stat_async_ddl_lock_replay_transaction'
LANGUAGE C PARALLEL SAFE;

CREATE VIEW polar_stat_async_ddl_lock_replay_transaction AS
	SELECT P.* FROM polar_stat_async_ddl_lock_replay_transaction() AS P
		(xid int4, lsn pg_lsn, commit_state text, dbOid oid, relOid oid,
		rtime timestamp with time zone, state text, worker_id int4, worker_pid int4);

-- POLAR: watch async ddl lock replay lock stat
CREATE FUNCTION polar_stat_async_ddl_lock_replay_lock()
RETURNS SETOF RECORD
AS 'MODULE_PATHNAME', 'polar_stat_async_ddl_lock_replay_lock'
LANGUAGE C PARALLEL SAFE;

CREATE VIEW polar_stat_async_ddl_lock_replay_lock AS
	SELECT P.* FROM polar_stat_async_ddl_lock_replay_lock() AS P
		(xid int4, dbOid oid, relOid oid, lsn pg_lsn, rtime timestamp with time zone,
		state text, commit_state text, worker_id int4, worker_pid int4);

REVOKE ALL ON FUNCTION polar_stat_async_ddl_lock_replay_worker() FROM PUBLIC;
REVOKE ALL ON FUNCTION polar_stat_async_ddl_lock_replay_transaction() FROM PUBLIC;
REVOKE ALL ON FUNCTION polar_stat_async_ddl_lock_replay_lock() FROM PUBLIC;

-- Create customized polar process info func
CREATE FUNCTION polar_stat_process(
            OUT pid oid,
            OUT wait_object int4,
            OUT wait_time float8,
            OUT cpu_user int8,
            OUT cpu_sys int8,
            OUT rss  int8,
            OUT shared_read_ps int8,
            OUT shared_write_ps int8,
            OUT shared_read_throughput int8,
            OUT shared_write_throughput int8,
            OUT shared_read_latency_ms float8,
            OUT shared_write_latency_ms float8,
            OUT local_read_ps int8,
            OUT local_write_ps int8,
            OUT local_read_throughput int8,
            OUT local_write_throughput int8,
            OUT local_read_latency_ms float8,
            OUT local_write_latency_ms float8,
            OUT wait_type text,
            OUT queryid int8
)
RETURNS record
AS 'MODULE_PATHNAME', 'polar_stat_process'
LANGUAGE C PARALLEL SAFE;


CREATE VIEW polar_stat_activity AS
  SELECT b.*, a.queryid,
  (CASE WHEN a.wait_object IS NULL and a.wait_time IS NOT NULL THEN CAST(pg_blocking_pids(b.pid) as text) ELSE CAST(a.wait_object as text) END)::text AS wait_object,
  wait_type,wait_time as "wait_time_ms", cpu_user, cpu_sys, rss, 
  shared_read_ps, shared_write_ps, shared_read_throughput, shared_write_throughput, shared_read_latency_ms, shared_write_latency_ms,
  local_read_ps, local_write_ps, local_read_throughput, local_write_throughput, local_read_latency_ms, local_write_latency_ms
  FROM pg_stat_activity AS b left join polar_stat_process() AS a on b.pid = a.pid;

-- Create customized polar IO info func
CREATE FUNCTION polar_stat_io_info(
            OUT pid oid,
            OUT filetype text,
            OUT fileloc text,
            OUT open_count int8,
            OUT open_latency_us float8,
            OUT close_count int8,
            OUT read_count int8,
            OUT write_count int8,
            OUT read_throughput int8,
            OUT write_throughput int8,
            OUT read_latency_us float8,
            OUT write_latency_us float8,
            OUT seek_count int8,
            OUT seek_latency_us float8,
            OUT creat_count int8,
            OUT creat_latency_us float8,
            OUT fsync_count int8,
            OUT fsync_latency_us float8,
            OUT falloc_count int8,
            OUT falloc_latency_us float8
)
RETURNS record
AS 'MODULE_PATHNAME', 'polar_stat_io_info'
LANGUAGE C PARALLEL SAFE;

CREATE VIEW polar_stat_io_info AS
  SELECT filetype, fileloc, sum(open_count) as open_count, sum(open_latency_us) as open_latency_us,
  sum(close_count) as close_count, sum(read_count) as read_count, sum(write_count) as write_count, sum(read_throughput)  as read_throughput,
  sum(write_throughput) as write_throughput,
  sum(read_latency_us) as read_latency_us, sum(write_latency_us) as write_latency_us, sum(seek_count) as seek_count, sum(seek_latency_us) as seek_latency_us,
  sum(creat_count) as creat_count, sum(creat_latency_us) as creat_latency_us, sum(fsync_count) as fsync_count,
  sum(fsync_latency_us) as fsync_latency_us, sum(falloc_count) as falloc_count, sum(falloc_latency_us) as falloc_latency_us
  FROM polar_stat_io_info() GROUP BY filetype, fileloc;

-- Create customized io latency info func
CREATE FUNCTION polar_io_latency_info(
	          OUT pid oid,
            OUT IOKind text,
            OUT Num_LessThan200us int8,
            OUT Num_LessThan400us int8,
            OUT Num_LessThan600us int8,
            OUT Num_LessThan800us int8,
            OUT Num_LessThan1ms int8,
            OUT Num_LessThan10ms int8,
            OUT Num_LessThan100ms int8,
            OUT Num_MoreThan100ms int8
)
RETURNS record
AS 'MODULE_PATHNAME', 'polar_io_latency_info'
LANGUAGE C PARALLEL SAFE;

CREATE VIEW polar_stat_io_latency AS
  SELECT IOKind, sum(Num_LessThan200us) Num_LessThan200us, sum(Num_LessThan400us) Num_LessThan400us,
  sum(Num_LessThan600us) Num_LessThan600us, sum(Num_LessThan800us) Num_LessThan800us,
  sum(Num_LessThan1ms) Num_LessThan1ms, sum(Num_LessThan10ms) Num_LessThan10ms,
  sum(Num_LessThan100ms) Num_LessThan100ms, sum(Num_MoreThan100ms) Num_MoreThan100ms
  FROM polar_io_latency_info() GROUP BY IOKind;