/* contrib/polar_monitor/polar_monitor--1.0--1.1.sql */

-- complain if script is sourced in psql, rather than via ALTER EXTENSION
\echo Use "ALTER EXTENSION polar_monitor UPDATE to '1.1'" to load this file. \quit

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
