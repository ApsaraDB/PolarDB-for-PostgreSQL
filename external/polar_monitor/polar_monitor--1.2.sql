/* contrib/polar_monitor/polar_monitor--1.2.sql */

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

CREATE FUNCTION polar_replica_min_used_lsn()
RETURNS pg_lsn
AS 'MODULE_PATHNAME', 'polar_replica_min_used_lsn'
LANGUAGE C PARALLEL SAFE;

CREATE FUNCTION polar_min_used_lsn()
RETURNS pg_lsn
AS 'MODULE_PATHNAME', 'polar_min_used_lsn'
LANGUAGE C PARALLEL SAFE;

CREATE FUNCTION polar_get_logindex_mem_tbl_size()
RETURNS bigint
AS 'MODULE_PATHNAME', 'polar_get_logindex_mem_tbl_size'
LANGUAGE C PARALLEL SAFE;

CREATE FUNCTION polar_used_logindex_mem_tbl_size()
RETURNS bigint
AS 'MODULE_PATHNAME', 'polar_used_logindex_mem_tbl_size'
LANGUAGE C PARALLEL SAFE;

CREATE FUNCTION polar_used_logindex_fullpage_snapshot_mem_tbl_size()
RETURNS bigint
AS 'MODULE_PATHNAME', 'polar_used_logindex_fullpage_snapshot_mem_tbl_size'
LANGUAGE C PARALLEL SAFE;

CREATE FUNCTION polar_replica_use_xlog_queue()
RETURNS bool
AS 'MODULE_PATHNAME', 'polar_replica_use_xlog_queue'
LANGUAGE C PARALLEL SAFE;

CREATE FUNCTION polar_replica_bg_replay_lsn()
RETURNS pg_lsn
AS 'MODULE_PATHNAME', 'polar_replica_bg_replay_lsn'
LANGUAGE C PARALLEL SAFE;

-- useless function, keep them just for not influence performance collection tools
CREATE FUNCTION polar_xlog_buffer_full()		
RETURNS bool		
AS 'MODULE_PATHNAME', 'polar_xlog_buffer_full'		
LANGUAGE C PARALLEL SAFE;

CREATE FUNCTION polar_page_hashtable_full()		
RETURNS bool		
AS 'MODULE_PATHNAME', 'polar_page_hashtable_full'		
LANGUAGE C PARALLEL SAFE;

CREATE FUNCTION polar_page_hashtable_used_size()		
RETURNS bigint		
AS 'MODULE_PATHNAME', 'polar_page_hashtable_used_size'		
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
							  OUT cbuf int8,
							  OUT fakelsn int8)
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

CREATE FUNCTION polar_csnlog(OUT all_fetches int8,
						 OUT ub_fetches int8,
						 OUT ub_hits int8)
RETURNS record
AS 'MODULE_PATHNAME', 'polar_csnlog'
LANGUAGE C PARALLEL SAFE;

-- Create customized replication slot func
CREATE FUNCTION polar_get_replication_slots(
            OUT slot_name name,
            OUT plugin name,
            OUT slot_type text,
            OUT datoid oid,
            OUT temporary boolean,
            OUT active boolean,
            OUT active_pid integer,
            OUT xmin xid,
            OUT catalog_xmin xid,
            OUT restart_lsn pg_lsn,
            OUT confirmed_flush_lsn pg_lsn,
            OUT polar_replica_apply_lsn pg_lsn,
            OUT wal_status text,
            OUT safe_wal_size int8
)
RETURNS record
AS 'MODULE_PATHNAME', 'polar_get_replication_slots'
LANGUAGE C PARALLEL SAFE;

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

-- Create customized replication slot view using polar_get_replication_slots
CREATE VIEW polar_replication_slots AS
SELECT
  L.slot_name,
  L.plugin,
  L.slot_type,
  L.datoid,
  D.datname AS database,
  L.temporary,
  L.active,
  L.active_pid,
  L.xmin,
  L.catalog_xmin,
  L.restart_lsn,
  L.confirmed_flush_lsn,
  L.polar_replica_apply_lsn,
  L.wal_status,
  L.safe_wal_size
FROM polar_get_replication_slots() AS L
       LEFT JOIN pg_database D ON (L.datoid = D.oid);

/* bulk read stats */
/* per table (or index) */
CREATE FUNCTION polar_pg_stat_get_bulk_read_calls(
            IN  oid,
            OUT int8
)
AS 'MODULE_PATHNAME', 'polar_pg_stat_get_bulk_read_calls'
LANGUAGE C PARALLEL SAFE;

CREATE FUNCTION polar_pg_stat_get_bulk_read_calls_IO(
            IN  oid,
            OUT int8
)
AS 'MODULE_PATHNAME', 'polar_pg_stat_get_bulk_read_calls_IO'
LANGUAGE C PARALLEL SAFE;

CREATE FUNCTION polar_pg_stat_get_bulk_read_blocks_IO(
            IN  oid,
            OUT int8
)
AS 'MODULE_PATHNAME', 'polar_pg_stat_get_bulk_read_blocks_IO'
LANGUAGE C PARALLEL SAFE;

/* data per database */
CREATE FUNCTION polar_pg_stat_get_db_bulk_read_calls(
            IN  oid,
            OUT int8
)
AS 'MODULE_PATHNAME', 'polar_pg_stat_get_db_bulk_read_calls'
LANGUAGE C PARALLEL SAFE;

CREATE FUNCTION polar_pg_stat_get_db_bulk_read_calls_IO(
            IN  oid,
            OUT int8
)
AS 'MODULE_PATHNAME', 'polar_pg_stat_get_db_bulk_read_calls_IO'
LANGUAGE C PARALLEL SAFE;

CREATE FUNCTION polar_pg_stat_get_db_bulk_read_blocks_IO(
            IN  oid,
            OUT int8
)
AS 'MODULE_PATHNAME', 'polar_pg_stat_get_db_bulk_read_blocks_IO'
LANGUAGE C PARALLEL SAFE;


CREATE VIEW polar_pg_statio_all_tables AS
    SELECT
            C.oid AS relid,
            N.nspname AS schemaname,
            C.relname AS relname,
			polar_pg_stat_get_bulk_read_calls(C.oid) AS heap_bulk_read_calls,
			polar_pg_stat_get_bulk_read_calls_IO(C.oid) AS heap_bulk_read_calls_IO,
			polar_pg_stat_get_bulk_read_blocks_IO(C.oid) AS heap_bulk_read_blks_IO,

            sum(polar_pg_stat_get_bulk_read_calls(I.indexrelid))::bigint AS idx_bulk_read_calls,
            sum(polar_pg_stat_get_bulk_read_calls_IO(I.indexrelid))::bigint AS idx_bulk_read_calls_IO,
            sum(polar_pg_stat_get_bulk_read_blocks_IO(I.indexrelid))::bigint AS idx_bulk_read_blks_IO,

			polar_pg_stat_get_bulk_read_calls(T.oid) AS toast_bulk_read_calls,
			polar_pg_stat_get_bulk_read_calls_IO(T.oid) AS toast_bulk_read_calls_IO,
			polar_pg_stat_get_bulk_read_blocks_IO(T.oid) AS toast_bulk_read_blks_IO,

            sum(polar_pg_stat_get_bulk_read_calls(X.indexrelid))::bigint AS tidx_bulk_read_calls,
			sum(polar_pg_stat_get_bulk_read_calls_IO(X.indexrelid))::bigint AS tidx_bulk_read_calls_IO,
			sum(polar_pg_stat_get_bulk_read_blocks_IO(X.indexrelid))::bigint AS tidx_bulk_read_blocks_IO

    FROM pg_class C LEFT JOIN
            pg_index I ON C.oid = I.indrelid LEFT JOIN
            pg_class T ON C.reltoastrelid = T.oid LEFT JOIN
            pg_index X ON T.oid = X.indrelid
            LEFT JOIN pg_namespace N ON (N.oid = C.relnamespace)
    WHERE C.relkind IN ('r', 't', 'm')
    GROUP BY C.oid, N.nspname, C.relname, T.oid, X.indrelid;

CREATE VIEW polar_pg_statio_sys_tables AS
    SELECT * FROM polar_pg_statio_all_tables
    WHERE schemaname IN ('pg_catalog', 'information_schema') OR
          schemaname ~ '^pg_toast';

CREATE VIEW polar_pg_statio_user_tables AS
    SELECT * FROM polar_pg_statio_all_tables
    WHERE schemaname NOT IN ('pg_catalog', 'information_schema') AND
          schemaname !~ '^pg_toast';

CREATE VIEW polar_pg_stat_database AS
    SELECT
            D.oid AS datid,
            D.datname AS datname,
			polar_pg_stat_get_db_bulk_read_calls(D.oid) AS bulk_read_calls,
			polar_pg_stat_get_db_bulk_read_calls_IO(D.oid) AS bulk_read_calls_IO,
			polar_pg_stat_get_db_bulk_read_blocks_IO(D.oid) AS bulk_read_blks_IO
    FROM pg_database D;

CREATE FUNCTION polar_node_type()
RETURNS text
AS 'MODULE_PATHNAME', 'polar_get_node_type'
LANGUAGE C PARALLEL SAFE;

-- polar replica multi version snapshot store function and dynamic view
CREATE FUNCTION polar_get_multi_version_snapshot_store_info(
            OUT shmem_size            bigint,
            OUT slot_num              integer,
            OUT retry_times           integer,
            OUT curr_slot_no          integer,
            OUT next_slot_no          integer,
            OUT read_retried_times    bigint,
            OUT read_switched_times   bigint,
            OUT write_retried_times   bigint,
            OUT write_switched_times  bigint
)
RETURNS RECORD
AS 'MODULE_PATHNAME', 'polar_get_multi_version_snapshot_store_info'
LANGUAGE C PARALLEL SAFE;

CREATE VIEW polar_multi_version_snapshot_store_info AS
  SELECT * FROM polar_get_multi_version_snapshot_store_info();

-- only used by superuser
REVOKE ALL ON FUNCTION polar_get_multi_version_snapshot_store_info FROM PUBLIC;
REVOKE ALL ON polar_multi_version_snapshot_store_info FROM PUBLIC;

/* POLAR: delay dml count */
CREATE FUNCTION polar_pg_stat_get_delay_dml_count(
            IN oid,
            OUT int8
)
AS 'MODULE_PATHNAME', 'polar_pg_stat_get_delay_dml_count'
LANGUAGE C PARALLEL SAFE;

CREATE FUNCTION polar_stat_slru(
            OUT slru_type text,
            OUT slots_number int4,
            OUT valid_pages int4,
            OUT empty_pages int4,
            OUT reading_pages int4,
            OUT writing_pages int4,
            OUT wait_readings int4,
            OUT wait_writings int4,
            OUT read_count int8,
            OUT read_only_count int8,
            OUT read_upgrade_count int8,
            OUT victim_count int8,
            OUT victim_write_count int8,
            OUT write_count int8,
            OUT zero_count int8,
            OUT flush_count int8,
            OUT truncate_count int8,
            OUT storage_read_count int8,
            OUT storage_write_count int8
)
AS 'MODULE_PATHNAME', 'polar_stat_slru'
LANGUAGE C PARALLEL SAFE;

-- Register the polar_show_shared_memory.
CREATE FUNCTION polar_show_shared_memory()
RETURNS SETOF RECORD
AS 'MODULE_PATHNAME', 'polar_show_shared_memory'
LANGUAGE C PARALLEL SAFE;

-- Register the polar_shmem_total_size.
CREATE FUNCTION polar_shmem_total_size()
RETURNS SETOF RECORD
AS 'MODULE_PATHNAME', 'polar_shmem_total_size'
LANGUAGE C PARALLEL SAFE;

CREATE VIEW polar_stat_shmem_total_size AS
		SELECT P.* FROM polar_shmem_total_size() AS P
		(shmsize int8, shmtype text);

CREATE VIEW polar_stat_shmem AS
        SELECT P.* FROM polar_show_shared_memory() AS P 
        (shmname text, shmsize int8, shmtype text);

/* 
 * POLAR:
 *      This is an naive implementation for 
 * 	getting delta value or watching delta change on accumulated values.
 *      NOTE: we'd better not use this function online especially as a routine.
 * 	
 * 	What you need to do is:
 *      1. Create a view with dimension columns and value columns. 
 *         Dimension columns name must starts with 'd_', we use these columns as join condition.
 *	   Value columns name must starts with 'v_', we calculate delta for these columns.
 * 	   Other columns we just get newest value in view;
 * 	2. Then use
 *	   "select * from polar_delta(NULL::view_name)"
 *  	   to get delta value of view, or use
 *	   "\watch 1 select * from polar_delta(NULL::view_name)"
 *   Follow up I give an simple example on watching cpu/mem/iops for every backends
 *
 */
CREATE OR REPLACE FUNCTION polar_delta(IN _tbl_type anyelement)
  RETURNS SETOF anyelement AS
$func$
DECLARE
    r record;
    pid int;
    sql text;
    firstselect bool;
    firstjoin bool;
    fieldstr text;
    join_condition text;
    old_tmp_table text;
    new_tmp_table text;
    real_tmp_table text;
BEGIN
    sql := ' ';
    join_condition := ' ';
    fieldstr := ' ';
    firstselect := true;
    firstjoin := true;
    pid := (SELECT PG_BACKEND_PID());
    old_tmp_table := pg_typeof(_tbl_type) || '_' || pid || '_old';
    new_tmp_table := pg_typeof(_tbl_type) || '_' || pid || '_new';
    real_tmp_table := pg_typeof(_tbl_type) || '_' || pid || '_tmp';
    
    SET LOCAL client_min_messages=ERROR;
    /* 1. try to create two tmp table */
    EXECUTE FORMAT('CREATE TEMP TABLE IF NOT EXISTS %s AS SELECT * FROM %s',
                    old_tmp_table, pg_typeof(_tbl_type));
    EXECUTE FORMAT('CREATE TEMP TABLE IF NOT EXISTS %s AS SELECT * FROM %s',
                    new_tmp_table, pg_typeof(_tbl_type));

    /* 2. exchange two tmp talbe for keeping old data in old_tmp_table  */
    EXECUTE FORMAT('ALTER TABLE %s RENAME TO %s', old_tmp_table, real_tmp_table);
    EXECUTE FORMAT('ALTER TABLE %s RENAME TO %s', new_tmp_table, old_tmp_table);
    EXECUTE FORMAT('ALTER TABLE %s RENAME TO %s', real_tmp_table, new_tmp_table);

    /* 3. truncate and insert data into new tmp table. */
    EXECUTE FORMAT('TRUNCATE TABLE %s', new_tmp_table);
    EXECUTE FORMAT('INSERT INTO %s SELECT * FROM %s', new_tmp_table, pg_typeof(_tbl_type));

    /* 4. Then we join old tmp table and new tmp table on dimension column to get delta values */
    FOR r IN SELECT attname FROM pg_class, pg_attribute WHERE pg_class.reltype=pg_typeof(_tbl_type) AND pg_class.oid=pg_attribute.attrelid ORDER BY attnum
    LOOP
        IF NOT firstselect THEN
            fieldstr := fieldstr || ', ';
        ELSE
            firstselect := false;
        END IF;

        IF starts_with(r.attname, 'd_') THEN
            IF NOT firstjoin THEN
                join_condition := join_condition || ' AND ';
            ELSE
                firstjoin := false;
            END IF;

            join_condition := join_condition || FORMAT(' old.%s = new.%s ', r.attname, r.attname);
            fieldstr := fieldstr || FORMAT(' new.%s ', r.attname);
        ELSE
            IF starts_with(r.attname, 'v_') THEN
                fieldstr := fieldstr || FORMAT(' new.%s - old.%s ', r.attname, r.attname);
            ELSE
                fieldstr := fieldstr || FORMAT(' new.%s', r.attname);
            END IF;
        END IF;
    END LOOP;

    sql := FORMAT('SELECT %s FROM %s new JOIN %s old ON %s', 
                fieldstr, new_tmp_table, old_tmp_table, join_condition);

    RETURN QUERY EXECUTE sql;
END
$func$ LANGUAGE plpgsql;

/* 
 * POLAR: with clause would not register meta in pg_class and pg_attribute
 *   so we can only create reduncdent view here now.
 */
CREATE VIEW polar_stat_activity_rt_mid AS
	SELECT pid AS d_pid
		, backend_type AS d_backend_type
		, cpu_user AS v_cpu_user
		, cpu_sys AS v_cpu_sys
		, rss AS rss
		, local_read_ps AS v_local_read_iops
		, local_write_ps AS v_local_write_iops
		, local_read_throughput AS v_local_read_iothroughput
		, local_write_throughput AS v_local_write_iothroughput
		, local_read_latency_ms AS v_local_read_latency
		, local_write_latency_ms AS v_local_write_latency
	FROM
		polar_stat_activity;

/*
 * POLAR: watch each backend cpu, rss, iops and so on.
 */
CREATE VIEW polar_stat_activity_rt AS
	SELECT d_pid AS pid
		, d_backend_type AS backend_type
		, v_cpu_user AS cpu_user
		, v_cpu_sys AS cpu_sys
		, ROUND(rss * 4096 / 1024 / 1024.0, 2) AS rss
		, v_local_read_iops AS local_read_iops
		, v_local_write_iops AS local_write_iops
		, ROUND(v_local_read_iothroughput / 1024 / 1024, 2) AS local_read_iothroughput
		, ROUND(v_local_write_iothroughput / 1024 / 1024, 2) AS local_write_iothroughput
		, v_local_read_latency AS local_read_latency
		, v_local_write_latency AS local_write_latency
	FROM polar_delta(NULL::polar_stat_activity_rt_mid);

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

-- POLAR: control group info stat
CREATE FUNCTION polar_stat_cgroup(
    OUT subtype text,
    OUT infotype text,
    OUT count int8
)
RETURNS SETOF record
AS 'MODULE_PATHNAME', 'polar_stat_cgroup'
LANGUAGE C STRICT VOLATILE PARALLEL SAFE;

CREATE VIEW polar_stat_cgroup AS
  SELECT * FROM polar_stat_cgroup();

GRANT SELECT ON polar_stat_cgroup TO PUBLIC;

-- POLAR: control group quato
CREATE FUNCTION polar_cgroup_quota(
    OUT subtype text,
    OUT infotype text,
    OUT count int8
)
RETURNS SETOF record
AS 'MODULE_PATHNAME', 'polar_cgroup_quota'
LANGUAGE C STRICT VOLATILE PARALLEL SAFE;

CREATE VIEW polar_cgroup_quota AS
  SELECT * FROM polar_cgroup_quota();

GRANT SELECT ON polar_cgroup_quota TO PUBLIC;

/*
 * POLAR dma
 */
CREATE FUNCTION polar_dma_get_member_status(
            OUT server_id 						int8,
            OUT current_term          int8,
            OUT current_leader				int8,
            OUT commit_index					int8,
            OUT last_log_term 				int8,
            OUT last_log_index				int8,
            OUT paxos_role						int4,
            OUT vote_for							int8
)
RETURNS RECORD
AS 'MODULE_PATHNAME', 'polar_dma_get_member_status'
LANGUAGE C PARALLEL SAFE;

CREATE VIEW polar_dma_member_status AS
    SELECT * FROM polar_dma_get_member_status();

CREATE FUNCTION polar_dma_get_cluster_status(
            OUT server_id 						int8,
            OUT ip_port								text,
            OUT match_index						int8,
            OUT next_index						int8,
            OUT role									int4,
            OUT has_voted							bool,
            OUT force_sync						bool,
            OUT election_weight				int4,
            OUT learner_source				int8,
            OUT pipelining						bool	
)
RETURNS SETOF record
AS 'MODULE_PATHNAME', 'polar_dma_get_cluster_status'
LANGUAGE C PARALLEL SAFE;

CREATE VIEW polar_dma_cluster_status AS
    SELECT * FROM polar_dma_get_cluster_status();

CREATE FUNCTION polar_dma_get_msg_stats(
            OUT server_id 						int8,
            OUT count_msg_append_log	int8,
            OUT count_msg_request_vote	int8,
            OUT count_heartbeat					int8,
            OUT count_on_msg_append_log int8,
            OUT count_on_msg_request_vote	int8,
            OUT count_on_heartbeat		int8,
            OUT count_replicate_log		int8
)
RETURNS RECORD
AS 'MODULE_PATHNAME', 'polar_dma_get_msg_stats'
LANGUAGE C PARALLEL SAFE;

CREATE VIEW polar_dma_msg_stats AS
    SELECT * FROM polar_dma_get_msg_stats();

CREATE FUNCTION polar_dma_get_consensus_status(
            OUT state									int4,
            OUT term									int8,
            OUT log_term 							int8,
            OUT xlog_term							int8,
            OUT leader_id							int8,
            OUT sync_rqst_lsn 				pg_lsn,
            OUT synced_tli						int4,
            OUT synced_lsn						pg_lsn,
            OUT purge_lsn							pg_lsn,
            OUT commit_index 					int8,
            OUT flushed_lsn						pg_lsn,
            OUT flushed_tli						int4,
            OUT appened_lsn						pg_lsn,
            OUT commit_queue_length		int8,
            OUT stats_queue_length		int8
)
RETURNS RECORD
AS 'MODULE_PATHNAME', 'polar_dma_get_consensus_status'
LANGUAGE C PARALLEL SAFE;

CREATE VIEW polar_dma_consensus_status AS
    SELECT * FROM polar_dma_get_consensus_status();

CREATE FUNCTION polar_dma_get_log_status(
            OUT term									int8,
            OUT current_index					int8,
            OUT sync_index						int8,
            OUT last_writ_lsn					pg_lsn,
            OUT last_write_timeline		int4,
            OUT last_append_term			int8,
            OUT next_append_term			int8,
            OUT variable_length_log_next_offset	int8,
            OUT disable_purge					bool
)
RETURNS RECORD
AS 'MODULE_PATHNAME', 'polar_dma_get_log_status'
LANGUAGE C PARALLEL SAFE;

CREATE VIEW polar_dma_log_status AS
    SELECT * FROM polar_dma_get_log_status();

CREATE FUNCTION polar_dma_get_meta_status(
            OUT term									int8,
            OUT vote_for							int8,
            OUT last_leader_term			int8,
            OUT last_leader_log_index	int8,
            OUT scan_index						int8,
            OUT cluster_id						int8,
            OUT commit_index					int8,
            OUT purge_index						int8,
            OUT member_info_str				text,
            OUT learner_info_str			text	
)
RETURNS RECORD
AS 'MODULE_PATHNAME', 'polar_dma_get_meta_status'
LANGUAGE C PARALLEL SAFE;

CREATE VIEW polar_dma_meta_status AS
    SELECT * FROM polar_dma_get_meta_status();

CREATE FUNCTION polar_dma_get_consensus_stats(
            OUT transit_waits					int8,
            OUT xlog_transit_waits		int8,
            OUT xlog_flush_waits			int8,
            OUT consensus_appends			int8,
            OUT consensus_wakeups			int8,
            OUT consensus_backend_wakeups	int8,
            OUT consensus_commit_waits		int8,
            OUT consensus_commit_wait_time	int8
)
RETURNS RECORD
AS 'MODULE_PATHNAME', 'polar_dma_get_consensus_stats'
LANGUAGE C PARALLEL SAFE;

CREATE VIEW polar_dma_consensus_stats AS
    SELECT * FROM polar_dma_get_consensus_stats();

CREATE FUNCTION polar_dma_get_log_stats(
            OUT log_reads							int8,
            OUT variable_log_reads		int8,
            OUT log_appends						int8,
            OUT variable_log_appends	int8,
            OUT log_flushes						int8,
            OUT meta_flushes					int8,
            OUT xlog_flush_tries			int8,
            OUT xlog_flush_failed_tries	int8,
            OUT xlog_transit_waits		int8
)
RETURNS RECORD
AS 'MODULE_PATHNAME', 'polar_dma_get_log_stats'
LANGUAGE C PARALLEL SAFE;

CREATE VIEW polar_dma_log_stats AS
    SELECT * FROM polar_dma_get_log_stats();

CREATE FUNCTION polar_dma_get_slru_stats(
            OUT slru_type text,
            OUT slots_number int4,
            OUT valid_pages int4,
            OUT empty_pages int4,
            OUT reading_pages int4,
            OUT writing_pages int4,
            OUT wait_readings int4,
            OUT wait_writings int4,
            OUT read_count int8,
            OUT read_only_count int8,
            OUT read_upgrade_count int8,
            OUT victim_count int8,
            OUT victim_write_count int8,
            OUT write_count int8,
            OUT zero_count int8,
            OUT flush_count int8,
            OUT truncate_forward_count int8,
            OUT truncate_backward_count int8,
            OUT storage_read_count int8,
            OUT storage_write_count int8
)
RETURNS SETOF record
AS 'MODULE_PATHNAME', 'polar_dma_get_slru_stats'
LANGUAGE C PARALLEL SAFE;

CREATE VIEW polar_dma_slru_stats AS
    SELECT * FROM polar_dma_get_slru_stats();

CREATE FUNCTION polar_dma_wal_committed_lsn()
RETURNS pg_lsn
AS 'MODULE_PATHNAME', 'polar_dma_wal_committed_lsn'
LANGUAGE C PARALLEL SAFE;
		
-- POLAR: DataMax exposed sql interface
CREATE FUNCTION polar_get_datamax_info(
		OUT min_received_timeline 	INT4,
		OUT min_received_lsn		pg_lsn,
		OUT last_received_timeline 	INT4,
		OUT last_received_lsn 		pg_lsn,
        OUT last_valid_received_lsn pg_lsn,
		OUT clean_reserved_lsn 		pg_lsn,
		OUT force_clean 		BOOL
)
RETURNS RECORD
AS 'MODULE_PATHNAME', 'polar_get_datamax_info'
LANGUAGE C PARALLEL SAFE;

CREATE FUNCTION polar_set_datamax_reserved_lsn(
		IN reserved_lsn 	pg_lsn,
		IN force 		boolean
)
RETURNS BOOL
AS 'MODULE_PATHNAME', 'polar_set_datamax_reserved_lsn'
LANGUAGE C PARALLEL SAFE;

-- POLAR: get xact split xids and splittable info, for debug and test purpose
CREATE FUNCTION polar_stat_xact_split_info(OUT xids text, OUT splittable bool, OUT lsn bigint)
RETURNS RECORD
AS 'MODULE_PATHNAME', 'polar_stat_xact_split_info'
LANGUAGE C PARALLEL SAFE;

-- polar wal function and dynamic view
CREATE FUNCTION polar_wal_pipeline_info(
            OUT wal_current_insert_lsn          bigint,
            OUT wal_continuous_insert_lsn       bigint,
            OUT wal_write_lsn                   bigint,
            OUT wal_flush_lsn                   bigint,
            OUT unflushed_xlog_add_slot_no 		bigint,
            OUT unflushed_xlog_del_slot_no 		bigint,
			OUT last_notify_lsn1 				bigint,
			OUT last_notify_lsn2 				bigint,
			OUT last_notify_lsn3 				bigint,
			OUT last_notify_lsn4 				bigint
)
RETURNS RECORD
AS 'MODULE_PATHNAME', 'polar_wal_pipeline_info'
LANGUAGE C PARALLEL SAFE;

CREATE FUNCTION polar_wal_pipeline_stats(
            OUT write_wroker_timeout_waits      bigint,
            OUT write_wroker_wakeup_waits       bigint,
            OUT advance_wroker_timeout_waits    bigint,
            OUT advance_wroker_wakeup_waits     bigint,
            OUT flush_wroker_timeout_waits      bigint,
            OUT flush_wroker_wakeup_waits       bigint,
            OUT notify_wroker1_timeout_waits    bigint,
            OUT notify_wroker1_wakeup_waits     bigint,
            OUT notify_wroker2_timeout_waits    bigint,
            OUT notify_wroker2_wakeup_waits     bigint,
            OUT notify_wroker3_timeout_waits    bigint,
            OUT notify_wroker3_wakeup_waits     bigint,
            OUT notify_wroker4_timeout_waits    bigint,
            OUT notify_wroker4_wakeup_waits     bigint,
            OUT total_user_group_commits        bigint,
            OUT total_user_spin_commits         bigint,
            OUT total_user_timeout_commits      bigint,
            OUT total_user_wakeup_commits       bigint,
			OUT total_user_miss_timeouts        bigint,
            OUT total_user_miss_wakeups         bigint,
            OUT total_advance_callups			bigint,	
            OUT total_advances                  bigint,
            OUT total_write_callups             bigint,
            OUT total_writes                    bigint,
            OUT unflushed_xlog_slot_waits  		bigint,
            OUT total_flush_callups             bigint,  
            OUT total_flushes                   bigint,
            OUT total_flush_merges              bigint,
            OUT total_notify_callups            bigint,
            OUT total_notifies                  bigint,
            OUT total_notified_users            bigint
)
RETURNS RECORD
AS 'MODULE_PATHNAME', 'polar_wal_pipeline_stats'
LANGUAGE C PARALLEL SAFE;

CREATE VIEW polar_wal_pipeline_info AS
  SELECT * FROM polar_wal_pipeline_info();

CREATE VIEW polar_wal_pipeline_stats AS
  SELECT * FROM polar_wal_pipeline_stats();

CREATE FUNCTION polar_wal_pipeline_reset_stats()
RETURNS BOOL
AS 'MODULE_PATHNAME', 'polar_wal_pipeline_reset_stats'
LANGUAGE C PARALLEL SAFE;

-- only used by superuser
REVOKE ALL ON FUNCTION polar_wal_pipeline_reset_stats FROM PUBLIC;
REVOKE ALL ON FUNCTION polar_wal_pipeline_info FROM PUBLIC;
REVOKE ALL ON FUNCTION polar_wal_pipeline_stats FROM PUBLIC;
REVOKE ALL ON polar_wal_pipeline_info FROM PUBLIC;
REVOKE ALL ON polar_wal_pipeline_stats FROM PUBLIC;

-- POLAR: get flashback log write status
CREATE FUNCTION polar_stat_flashback_log_write(
  OUT write_total_num bigint,
  OUT bg_worker_write_num bigint,
  OUT segs_added_total_num bigint,
  OUT max_seg_no bigint,
  OUT write_result pg_lsn,
  OUT fbpoint_flog_start_ptr pg_lsn)
RETURNS RECORD
AS 'MODULE_PATHNAME', 'polar_stat_flashback_log_write'
LANGUAGE C PARALLEL SAFE;

-- POLAR: get flashback log buffer status
CREATE FUNCTION polar_stat_flashback_log_buf(
  OUT insert_curr_ptr pg_lsn, 
  OUT insert_prev_ptr pg_lsn,
  OUT initalized_upto pg_lsn,
  OUT keep_wal_lsn pg_lsn,
  OUT is_ready boolean)
RETURNS RECORD
AS 'MODULE_PATHNAME', 'polar_stat_flashback_log_buf'
LANGUAGE C PARALLEL SAFE;

-- POLAR: get flashback log list status
CREATE FUNCTION polar_stat_flashback_log_list(
  OUT insert_total_num bigint,
  OUT remove_total_num bigint,
  OUT bg_remove_num bigint)
RETURNS RECORD
AS 'MODULE_PATHNAME', 'polar_stat_flashback_log_list'
LANGUAGE C PARALLEL SAFE;

-- POLAR: get flashback logindex status
CREATE FUNCTION polar_stat_flashback_logindex(
  OUT max_ptr_in_mem bigint,
  OUT max_ptr_in_disk bigint,
  OUT is_ready boolean)
RETURNS RECORD
AS 'MODULE_PATHNAME', 'polar_stat_flashback_logindex'
LANGUAGE C PARALLEL SAFE;

-- POLAR: get flashback logindex queue status
CREATE FUNCTION polar_stat_flashback_logindex_queue(
  OUT free_up_total_times bigint,
  OUT read_from_file_rec_nums bigint,
  OUT read_from_queue_rec_nums bigint)
RETURNS RECORD
AS 'MODULE_PATHNAME', 'polar_stat_flashback_logindex_queue'
LANGUAGE C PARALLEL SAFE;

-- Don't want these to be available to public.
REVOKE ALL ON FUNCTION polar_show_shared_memory() FROM PUBLIC;
REVOKE ALL ON FUNCTION polar_shmem_total_size() FROM PUBLIC;
REVOKE ALL ON polar_stat_shmem FROM PUBLIC;
REVOKE ALL ON polar_stat_shmem_total_size FROM PUBLIC;
REVOKE ALL on FUNCTION polar_delta FROM PUBLIC;
REVOKE ALL on polar_stat_activity_rt_mid FROM PUBLIC;
REVOKE ALL on polar_stat_activity_rt FROM PUBLIC;
REVOKE ALL ON FUNCTION polar_stat_async_ddl_lock_replay_worker() FROM PUBLIC;
REVOKE ALL ON FUNCTION polar_stat_async_ddl_lock_replay_transaction() FROM PUBLIC;
REVOKE ALL ON FUNCTION polar_stat_async_ddl_lock_replay_lock() FROM PUBLIC;

CREATE FUNCTION polar_stat_smgr_shared_pool(
            OUT total_num int4,
            OUT locked_num int4,
            OUT valid_num int4,
            OUT dirty_num int4,
            OUT syncing_num int4,
            OUT just_dirtied_num int4
)
AS 'MODULE_PATHNAME', 'polar_stat_smgr_shared_pool'
LANGUAGE C PARALLEL SAFE;

CREATE FUNCTION polar_stat_get_smgr_shared_mem(
            IN boolean default true,
            OUT spc_node OID,
            OUT db_node OID,
            OUT rel_node OID,
            OUT backend int4,
            OUT in_cache boolean,
            OUT main_cache int4,
            OUT fsm_cache int4,
            OUT vm_cache int4,
            OUT flags int4,
            OUT generation int8,
            OUT usecount int8
)
AS 'MODULE_PATHNAME', 'polar_stat_get_smgr_shared_mem'
LANGUAGE C PARALLEL SAFE;

CREATE FUNCTION polar_get_current_db_smgr_shared_mem(
            IN boolean default true,
            OUT spc_node OID,
            OUT db_node OID,
            OUT rel_node OID,
            OUT backend int4,
            OUT main_real int4,
            OUT fsm_real int4,
            OUT vm_real int4
)
AS 'MODULE_PATHNAME', 'polar_get_current_db_smgr_shared_mem'
LANGUAGE C PARALLEL SAFE;

CREATE FUNCTION polar_stat_get_smgr_shared_mem_ng(
            OUT sr_offset int4,
            OUT is_locked boolean,
            OUT is_valid boolean,
            OUT is_dirty boolean,
            OUT is_syncing boolean,
            OUT is_just_dirtied boolean
)
AS 'MODULE_PATHNAME', 'polar_stat_get_smgr_shared_mem_ng'
LANGUAGE C PARALLEL SAFE;

CREATE FUNCTION polar_release_target_smgr_shared_mem(
            IN int4
) RETURNS VOID
AS 'MODULE_PATHNAME', 'polar_release_target_smgr_shared_mem'
LANGUAGE C PARALLEL SAFE;

-- global io read time monitor
CREATE FUNCTION polar_io_read_delta_info(
	        OUT count int4,
            OUT force_delay_times int8,
            OUT less_than_delay_times int8,
            OUT more_than_delay_times int8,
            OUT read_size_avg int8,
            OUT read_time_avg float8,
            OUT current_throughtput float8,
            OUT max_throuhgtput int8,
            OUT user_set_throughtput bool,
            OUT top_throughtput int8[]
)
RETURNS record
AS 'MODULE_PATHNAME', 'polar_io_read_delta_info'
LANGUAGE C PARALLEL SAFE;

-- global proxy monitor
CREATE FUNCTION polar_stat_get_real_pid(pid int default NULL)
RETURNS INT
AS 'MODULE_PATHNAME', 'polar_stat_get_real_pid'
LANGUAGE C PARALLEL SAFE;
CREATE FUNCTION polar_stat_get_virtual_pid(pid int default NULL)
RETURNS INT
AS 'MODULE_PATHNAME', 'polar_stat_get_virtual_pid'
LANGUAGE C PARALLEL SAFE;

CREATE FUNCTION polar_stat_proxy_info()
RETURNS SETOF RECORD
AS 'MODULE_PATHNAME', 'polar_stat_proxy_info'
LANGUAGE C PARALLEL SAFE;
CREATE FUNCTION polar_stat_reset_proxy_info()
RETURNS void
AS 'MODULE_PATHNAME', 'polar_stat_reset_proxy_info'
LANGUAGE C PARALLEL SAFE;

CREATE VIEW polar_stat_proxy_info AS
	SELECT * FROM polar_stat_proxy_info() AS P (reason text, count int8);
CREATE VIEW polar_stat_proxy_info_rt_mid AS
	SELECT * FROM polar_stat_proxy_info() AS P
		(d_reason text, v_count int8);
CREATE VIEW polar_stat_proxy_info_rt AS
	SELECT * FROM polar_delta(NULL::polar_stat_proxy_info_rt_mid);

-- POLAR: cluster info
CREATE FUNCTION polar_cluster_info()
RETURNS SETOF RECORD
AS 'MODULE_PATHNAME', 'polar_cluster_info'
LANGUAGE C PARALLEL SAFE;

CREATE VIEW polar_cluster_info AS
	SELECT P.* FROM polar_cluster_info() AS P
		(name text, host text, port int4, release_date text, version text, slot_name text,
		 type text, state text, cpu int, cpu_quota int, memory int, memory_quota int,
		 iops int, iops_quota int, connection int, connection_quota int,
		 px_connection int, px_connection_quota int);

CREATE FUNCTION polar_set_available(available bool)
RETURNS VOID
AS 'MODULE_PATHNAME', 'polar_set_available'
LANGUAGE C PARALLEL UNSAFE;

CREATE FUNCTION polar_is_available()
RETURNS BOOL
AS 'MODULE_PATHNAME', 'polar_is_available'
LANGUAGE C PARALLEL UNSAFE;

REVOKE ALL ON polar_cluster_info FROM PUBLIC;
REVOKE ALL ON FUNCTION polar_cluster_info FROM PUBLIC;
REVOKE ALL ON FUNCTION polar_set_available FROM PUBLIC;
REVOKE ALL ON FUNCTION polar_is_available FROM PUBLIC;

-- DSA and AllocSet based on DSA
CREATE OR REPLACE FUNCTION polar_stat_dsa(
            OUT name TEXT,
            OUT total_segment_size int8,
            OUT pinned bool,
            OUT refcnt int4,
            OUT usable_pages int8,
            OUT max_contiguous_pages int8,
            OUT max_total_segment_size int8,
            OUT usable_space_pct float4)
AS 'MODULE_PATHNAME', 'polar_stat_dsa'
LANGUAGE C PARALLEL SAFE;

CREATE OR REPLACE VIEW polar_stat_dsa AS
	SELECT * FROM polar_stat_dsa();

CREATE OR REPLACE FUNCTION polar_shm_aset_ctl(
                    OUT type int2,
                    OUT name TEXT,
                    OUT size int8)
AS 'MODULE_PATHNAME', 'polar_shm_aset_ctl'
LANGUAGE C PARALLEL SAFE;

CREATE OR REPLACE VIEW polar_shm_aset_ctl AS
	SELECT * FROM polar_shm_aset_ctl();

-- shared server
CREATE FUNCTION polar_dispatcher_pid()
RETURNS int4
AS 'MODULE_PATHNAME', 'polar_dispatcher_pid'
LANGUAGE C STABLE PARALLEL RESTRICTED;

/* POLAR: Shared Server */
CREATE FUNCTION polar_session_backend_pid()
RETURNS int4
AS 'MODULE_PATHNAME', 'polar_session_backend_pid'
LANGUAGE C STABLE PARALLEL RESTRICTED;

CREATE FUNCTION polar_is_dedicated_backend()
RETURNS bool
AS 'MODULE_PATHNAME', 'polar_is_dedicated_backend'
LANGUAGE C STABLE PARALLEL RESTRICTED;

CREATE OR REPLACE FUNCTION polar_stat_dispatcher
(
	OUT id                    int4,
	OUT pid                   int4,
	OUT n_clients             int4,
	OUT n_ssl_clients         int4,
	OUT n_idle_clients        int4,
	OUT n_pending_clients     int4,
	OUT n_signalling_clients   int4,
	OUT n_startup_clients     int4,
	OUT n_pools               int4,
	OUT n_backends            int4,
	OUT n_idle_backends       int4,
	OUT n_dedicated_backends  int4,
	OUT tx_bytes              int8,
	OUT rx_bytes              int8,
	OUT n_transactions        int8
)
RETURNS SETOF record
AS 'MODULE_PATHNAME', 'polar_stat_dispatcher'
LANGUAGE C STRICT PARALLEL SAFE;

CREATE OR REPLACE VIEW polar_stat_dispatcher AS
  SELECT * FROM polar_stat_dispatcher();

REVOKE ALL ON FUNCTION polar_stat_dispatcher FROM PUBLIC;
REVOKE ALL ON polar_stat_dispatcher FROM PUBLIC;

CREATE OR REPLACE FUNCTION polar_stat_session
(
  pid integer,
  OUT datid oid,
  OUT pid integer,
  OUT usesysid oid,
  OUT application_name text,
  OUT state text,
  OUT query text,
  OUT wait_event_type text,
  OUT wait_event text,
  OUT xact_start timestamp with time zone,
  OUT query_start timestamp with time zone,
  OUT backend_start timestamp with time zone,
  OUT state_change timestamp with time zone,
  OUT client_addr inet,
  OUT client_hostname text,
  OUT client_port integer,
  OUT backend_xid xid,
  OUT backend_xmin xid,
  OUT backend_type text,
  OUT ssl boolean,
  OUT sslversion text,
  OUT sslcipher text,
  OUT sslbits integer,
  OUT sslcompression boolean,
  OUT sslclientdn text,
  OUT dispatcher_pid integer,
  OUT id integer,
  OUT last_backend_pid integer,
  OUT saved_guc_count integer,
  OUT last_wait_start timestamp with time zone
)
RETURNS SETOF record
AS 'MODULE_PATHNAME', 'polar_stat_session'
LANGUAGE C STABLE PARALLEL RESTRICTED;

CREATE VIEW polar_stat_session AS
    SELECT
            S.datid AS datid,
            D.datname AS datname,
            S.dispatcher_pid,
            S.id,
            S.pid as session_id,
            S.last_backend_pid,
            S.usesysid,
            U.rolname AS usename,
            S.application_name,
            S.client_addr,
            S.client_hostname,
            S.client_port,
            S.saved_guc_count,
            S.last_wait_start,
            S.backend_start,
            S.xact_start,
            S.query_start,
            S.state_change,
            S.wait_event_type,
            S.wait_event,
            S.state,
            S.backend_xid,
            s.backend_xmin,
            S.query,
            S.backend_type
    FROM polar_stat_session(NULL) AS S
        LEFT JOIN pg_database AS D ON (S.datid = D.oid)
        LEFT JOIN pg_authid AS U ON (S.usesysid = U.oid);

CREATE OR REPLACE FUNCTION polar_dump_dsa(name TEXT)
    RETURNS void
AS 'MODULE_PATHNAME', 'polar_dump_dsa'
    LANGUAGE C STRICT PARALLEL SAFE;

REVOKE ALL ON FUNCTION polar_dump_dsa FROM PUBLIC;
