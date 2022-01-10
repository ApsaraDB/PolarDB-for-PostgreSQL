/* contrib/polar_monitor/polar_monitor--1.1--1.2.sql */
 

 -- complain if script is sourced in psql, rather than via ALTER EXTENSION
\echo Use "ALTER EXTENSION polar_monitor UPDATE TO '1.2'" to load this file. \quit

/* First we have to remove them from the extension */
ALTER EXTENSION polar_monitor DROP FUNCTION polar_get_replication_slots(OUT name, OUT name, OUT text, 
OUT oid, OUT boolean, OUT boolean, OUT integer, OUT xid, OUT xid, OUT pg_lsn, OUT pg_lsn, OUT pg_lsn);
ALTER EXTENSION polar_monitor DROP VIEW polar_replication_slots;

/* Then we can drop them */
DROP VIEW polar_replication_slots;
DROP FUNCTION polar_get_replication_slots(OUT name, OUT name, OUT text, OUT oid, OUT boolean, OUT boolean,
OUT integer, OUT xid, OUT xid, OUT pg_lsn, OUT pg_lsn, OUT pg_lsn);

/* Now redefine */
CREATE OR replace FUNCTION polar_get_replication_slots(
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
 