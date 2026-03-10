/* external/polar_monitor/polar_monitor--1.2--1.3.sql */

-- complain if script is sourced in psql, rather than via ALTER EXTENSION
\echo Use "ALTER EXTENSION polar_monitor UPDATE TO '1.3'" to load this file. \quit

DROP FUNCTION IF EXISTS polar_rsc_stat_counters();

CREATE FUNCTION polar_rsc_stat_counters(
    OUT nblocks_pointer_hit int8,
    OUT nblocks_mapping_hit int8,
    OUT nblocks_mapping_miss int8,
    OUT mapping_update_hit int8,
    OUT mapping_update_evict int8,
    OUT mapping_update_invalidate int8,
    OUT drop_buffer_full_scan int8,
    OUT drop_buffer_hash_search int8
)
RETURNS RECORD
AS 'MODULE_PATHNAME', 'polar_monitor_stat_rsc_counters_v2'
LANGUAGE C PARALLEL SAFE;
