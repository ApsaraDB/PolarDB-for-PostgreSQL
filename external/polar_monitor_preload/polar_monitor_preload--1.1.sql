/* external/polar_monitor/polar_monitor_preload--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION polar_monitor_preload" to load this file. \quit

/* POLAR: memory context status */
CREATE FUNCTION polar_get_mcxt(
    IN pid int4,
    OUT pid int4,
    OUT name text,
    OUT level int4,
	OUT nblocks int8,
    OUT freechunks int8,
    OUT totalspace int8,
    OUT freespace int8,
	OUT ident text,
	OUT is_shared boolean,
	OUT type int8,
	OUT lock_area_count int8,
	OUT lock_area_time_us int8,
	OUT lock_area_max_time_us int8,
	OUT lock_sclass_count int8,
	OUT lock_sclass_time_us int8,
	OUT lock_sclass_max_time_us int8,
	OUT lock_freelist_count int8,
	OUT lock_freelist_time_us int8,
	OUT lock_freelist_max_time_us int8
	)
RETURNS SETOF record
AS 'MODULE_PATHNAME', 'polar_get_memory_stats'
LANGUAGE C IMMUTABLE;

/* POLAR: local memory context status */
CREATE FUNCTION polar_get_local_mcxt(
        OUT pid int4,
		OUT name text,
        OUT level int4,
		OUT nblocks int8,
        OUT freechunks int8,
        OUT totalspace int8,
        OUT freespace int8,
		OUT ident text,
		OUT is_shared boolean,
        OUT type int8,
		OUT lock_area_count int8,
		OUT lock_area_time_us int8,
		OUT lock_area_max_time_us int8,
		OUT lock_sclass_count int8,
		OUT lock_sclass_time_us int8,
		OUT lock_sclass_max_time_us int8,
		OUT lock_freelist_count int8,
		OUT lock_freelist_time_us int8,
		OUT lock_freelist_max_time_us int8
		)
RETURNS SETOF record
AS 'MODULE_PATHNAME', 'polar_get_local_memory_stats'
LANGUAGE C IMMUTABLE;

/* POLAR: memory context record type */
CREATE TYPE polar_mcxt_record AS (
	pid int,
	name text,
	nblocks int8,
	freechunks int8,
	totalspace int8,
	freespace int8,
	is_shared boolean,
	type int8,
	lock_area_count int8,
	lock_area_time_us int8,
	lock_area_max_time_us int8,
	lock_sclass_count int8,
	lock_sclass_time_us int8,
	lock_sclass_max_time_us int8,
	lock_freelist_count int8,
	lock_freelist_time_us int8,
	lock_freelist_max_time_us int8
);
/* POLAR: UNION ALL memory context from all backends together */
CREATE OR REPLACE FUNCTION polar_backends_mcxt() 
RETURNS SETOF polar_mcxt_record
AS
$x$
DECLARE
    sql text;
    selfpid int;
    p int;
BEGIN
    selfpid := (SELECT PG_BACKEND_PID());
    /* just a mock schema */
    sql := '(SELECT 0 AS pid, ' || quote_nullable('hello') || ' AS name, '
	  || ' 0::BIGINT AS nblocks, 0::BIGINT AS freechunks, '
	  || ' 0::BIGINT AS totalspace, 0::BIGINT  AS freespace, '
	  || ' 0::BOOLEAN AS is_shared, 0::BIGINT AS type, '
	  || ' 0::BIGINT AS lock_area_count, 0::BIGINT  AS lock_area_time_us, 0::BIGINT  AS lock_area_max_time_us, '
	  || ' 0::BIGINT AS lock_sclass_count, 0::BIGINT  AS lock_sclass_time_us, 0::BIGINT  AS lock_sclass_max_time_us, '
	  || ' 0::BIGINT AS lock_freelist_count, 0::BIGINT  AS lock_freelist_time_us, 0::BIGINT  AS lock_freelist_max_time_us '
	  || ' LIMIT 0)';

    FOR p IN SELECT pid FROM pg_stat_activity WHERE backend_type='client backend'
    LOOP
	sql := sql || ' UNION ALL '
		|| 'SELECT pid, name, '
		|| 'SUM(nblocks)::BIGINT AS nblocks, SUM(freechunks)::BIGINT AS freechunks, '
		|| 'SUM(totalspace)::BIGINT AS totalspace, SUM(freespace)::BIGINT AS freespace, '
		|| 'is_shared, type, '
		|| 'SUM(lock_area_count)::BIGINT AS lock_area_count, SUM(lock_area_time_us)::BIGINT AS lock_area_time_us, MAX(lock_area_max_time_us)::BIGINT AS lock_area_max_time_us, '
		|| 'SUM(lock_sclass_count)::BIGINT AS lock_sclass_count, SUM(lock_sclass_time_us)::BIGINT AS lock_sclass_time_us, MAX(lock_sclass_max_time_us)::BIGINT AS lock_sclass_max_time_us, '
		|| 'SUM(lock_freelist_count)::BIGINT AS lock_freelist_count, SUM(lock_freelist_time_us)::BIGINT AS lock_freelist_time_us, MAX(lock_freelist_max_time_us)::BIGINT AS lock_freelist_max_time_us ';
	IF selfpid != p THEN
	    sql := sql || 'FROM polar_get_mcxt(' || p || ') ';
	ELSE
	    sql := sql || 'FROM polar_get_local_mcxt()';
	END IF;
	sql := sql || ' GROUP BY pid, name, is_shared, type ';
    END LOOP;
    sql := 'SELECT pid, name, nblocks, freechunks, totalspace, freespace, '
		|| 'is_shared, type, '
		|| 'lock_area_count, lock_area_time_us, lock_area_max_time_us, lock_sclass_count, lock_sclass_time_us, lock_sclass_max_time_us, '
		|| 'lock_freelist_count, lock_freelist_time_us, lock_freelist_max_time_us '
		|| 'FROM ( ' || sql || ') t';
    RETURN QUERY EXECUTE sql;
END
$x$
LANGUAGE plpgsql;

CREATE VIEW polar_backends_mcxt AS
	SELECT * FROM polar_backends_mcxt() ORDER BY totalspace DESC, pid;

/*
 * polar_stat_lwlock
 */
CREATE FUNCTION polar_stat_lwlock
(
	OUT tranche int2,
	OUT name text,
	OUT sh_acquire_count int8,
	OUT ex_acquire_count int8,
	OUT block_count int8,
	OUT wait_time int8
)
RETURNS SETOF record
AS 'MODULE_PATHNAME', 'polar_stat_lwlock'
LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

/*
 * POLAR: 
 *	lwlock wait nums for each lwlock
 */
CREATE FUNCTION polar_lwlock_stat_waiters
(
	OUT tranche int2,
	OUT lock_waiters int4,
	OUT sh_nums boolean,
	OUT ex_nums boolean
)
RETURNS SETOF record
AS 'MODULE_PATHNAME', 'polar_lwlock_stat_waiters'
LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE VIEW polar_lwlock_stat_waiters AS
	SELECT tranche, count(lock_waiters) lock_nums, max(lock_waiters) max_lock_waiters, cast (avg(lock_waiters) as float) avg_lock_waiters, sum(cast (sh_nums as int)) sh_nums, sum(cast (ex_nums as int)) ex_nums FROM polar_lwlock_stat_waiters() GROUP BY tranche ORDER BY tranche ASC;

CREATE VIEW polar_stat_lwlock AS
	SELECT a.*, b.lock_nums, b.max_lock_waiters,  b.avg_lock_waiters, b.sh_nums, b.ex_nums FROM polar_stat_lwlock() a left JOIN polar_lwlock_stat_waiters b on a.tranche=b.tranche ORDER BY tranche ASC;

/*
 * POLAR: 
 *	lwlock stat for each backend
 */
CREATE FUNCTION polar_proc_stat_lwlock
(
	OUT pid int4,
	OUT tranche int2,
	OUT name text,
	OUT sh_acquire_count int8,
	OUT ex_acquire_count int8,
	OUT block_count int8,
	OUT wait_time int8
)
RETURNS SETOF record
AS 'MODULE_PATHNAME', 'polar_proc_stat_lwlock'
LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;



/*
 * polar_stat_lock
 */
CREATE FUNCTION polar_stat_lock
(
	OUT id int4,
	OUT lock_type text,
	OUT lock_mode text,
	OUT lock_count int8,
	OUT block_count int8,
	OUT fastpath_count int8,
	OUT wait_time int8
)
RETURNS SETOF record
AS 'MODULE_PATHNAME', 'polar_stat_lock'
LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE VIEW polar_stat_lock AS
	SELECT
		id AS id
		, lock_type AS lock_type
		, SUM(CASE WHEN lock_mode='INVALID' THEN lock_count ELSE 0 END) AS INVALID
		, SUM(CASE WHEN lock_mode='AccessShareLock' THEN lock_count ELSE 0 END) AS AccessShareLock
		, SUM(CASE WHEN lock_mode='RowShareLock' THEN lock_count ELSE 0 END) AS RowShareLock
		, SUM(CASE WHEN lock_mode='RowExclusiveLock' THEN lock_count ELSE 0 END) AS RowExclusiveLock
		, SUM(CASE WHEN lock_mode='ShareUpdateExclusiveLock' THEN lock_count ELSE 0 END) AS ShareUpdateExclusiveLock
		, SUM(CASE WHEN lock_mode='ShareLock' THEN lock_count ELSE 0 END) AS ShareLock
		, SUM(CASE WHEN lock_mode='ShareRowExclusiveLock' THEN lock_count ELSE 0 END) AS ShareRowExclusiveLock
		, SUM(CASE WHEN lock_mode='ExclusiveLock' THEN lock_count ELSE 0 END) AS ExclusiveLock
		, SUM(CASE WHEN lock_mode='AccessExclusiveLock' THEN lock_count ELSE 0 END) AS AccessExclusiveLock
		, SUM(block_count) AS block_count
		, SUM(fastpath_count) AS fastpath_count
		, SUM(wait_time) AS wait_time
	FROM
		polar_stat_lock()
	GROUP BY
		id,
		lock_type
	ORDER BY
		id;

/*
 * POLAR: 
 *	lock stat for each backend
 */
CREATE FUNCTION polar_proc_stat_lock
(
	OUT pid int4,
	OUT id int4,
	OUT lock_type text,
	OUT lock_mode text,
	OUT lock_count int8,
	OUT block_count int8,
	OUT fastpath_count int8,
	OUT wait_time int8
)
RETURNS SETOF record
AS 'MODULE_PATHNAME', 'polar_proc_stat_lock'
LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

/*
 * polar_stat_network
 */
CREATE FUNCTION polar_stat_network
(
	OUT send_count int8,
	OUT send_bytes int8,
	OUT send_block_time int8,
	OUT recv_count int8,
	OUT recv_bytes int8,
	OUT recv_block_time int8,
	OUT retrans int8
)
RETURNS SETOF record
AS 'MODULE_PATHNAME', 'polar_stat_network'
LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE VIEW polar_stat_network AS
	SELECT * FROM polar_stat_network();

/*
 * POLAR: 
 *	network stat for each backend
 */
CREATE FUNCTION polar_proc_stat_network
(
	OUT pid int4,
	OUT send_count int8,
	OUT send_bytes int8,
	OUT send_block_time int8,
	OUT recv_count int8,
	OUT recv_bytes int8,
	OUT recv_block_time int8,
	OUT sendq int8,
	OUT recvq int8,
	OUT cwnd int8,
	OUT rtt int8,
	OUT rttvar int8,
	OUT retrans int8,
	OUT tcpinfo_update_time int8
)
RETURNS SETOF record
AS 'MODULE_PATHNAME', 'polar_proc_stat_network'
LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

-- CREATE VIEW polar_stat_network_rt_mid AS
-- 	SELECT S.pid AS d_pid, backend_type AS d_backend_type, client_addr AS d_client,
-- 		send_count AS v_send_count,
-- 		send_bytes AS v_send_bytes,
-- 		recv_count AS v_recv_count,
-- 		recv_bytes AS v_recv_bytes,
-- 		retrans AS v_retrans
-- FROM polar_proc_stat_network() N JOIN pg_stat_get_activity(NULL) S ON N.pid=S.pid;
-- 
-- /*
--  * POLAR: watch each backend cpu, rss, iops and so on.
--  */
-- CREATE VIEW polar_stat_network_rt AS
-- 	SELECT d_pid AS pid
-- 		, d_backend_type AS backend_type
-- 		, d_client AS client
-- 		, v_send_count AS send_count
-- 		, ROUND(v_send_bytes / 1024 / 1024.0, 2) AS send_bytes
-- 		, v_recv_count AS recv_count
-- 		, ROUND(v_recv_bytes / 1024 / 1024.0, 2) AS recv_bytes
-- 		, v_retrans AS retrans
-- 	FROM polar_delta(NULL::polar_stat_network_rt_mid);

CREATE FUNCTION polar_stat_backend(
    OUT backend_type    text,
    OUT cpu_user        int8,
    OUT cpu_sys         int8,
	OUT rss 			int8,
	OUT shared_read_ps int8,
	OUT shared_write_ps int8,
	OUT shared_read_throughput int8,
	OUT shared_write_throughput int8,
	OUT shared_read_latency float8,
	OUT shared_write_latency float8,
	OUT local_read_ps int8,
	OUT local_write_ps int8,
	OUT local_read_throughput int8,
	OUT local_write_throughput int8,
	OUT local_read_latency float8,
	OUT local_write_latency float8,
	OUT send_count int8,
	OUT send_bytes int8,
	OUT recv_count int8,
	OUT recv_bytes int8
)
AS 'MODULE_PATHNAME', 'polar_stat_backend'
LANGUAGE C PARALLEL SAFE;

CREATE VIEW polar_stat_cpu_backend AS
	SELECT backend_type, cpu_user, cpu_sys, rss FROM polar_stat_backend();

CREATE VIEW polar_stat_io_backend AS
	SELECT backend_type,
		shared_read_ps, shared_write_ps,
	 	shared_read_throughput, shared_write_throughput,
		shared_read_latency, shared_write_latency,
		local_read_ps, local_write_ps,
		local_read_throughput, local_write_throughput,
		local_read_latency, local_write_latency 
		FROM polar_stat_backend();

CREATE VIEW polar_stat_network_backend AS
	SELECT backend_type, send_count, send_bytes, recv_count, recv_bytes FROM polar_stat_backend();

/* POLAR: Print current plan */
CREATE FUNCTION polar_log_current_plan(IN pid int4)
RETURNS BOOLEAN
AS 'MODULE_PATHNAME', 'polar_log_current_plan'
LANGUAGE C IMMUTABLE;

CREATE FUNCTION polar_current_backend_pid()
RETURNS int4
AS 'MODULE_PATHNAME', 'polar_current_backend_pid'
LANGUAGE C STABLE PARALLEL RESTRICTED;
