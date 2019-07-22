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
