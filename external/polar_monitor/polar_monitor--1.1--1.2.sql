/* external/polar_monitor/polar_monitor--1.1--1.2.sql */

-- complain if script is sourced in psql, rather than via ALTER EXTENSION
\echo Use "ALTER EXTENSION polar_monitor UPDATE TO '1.2'" to load this file. \quit

-- one-line WAL buffer stats
CREATE FUNCTION polar_wal_buffer_stat_counter(
    OUT buf_init_async int8,
    OUT buf_init_sync int8,
    OUT buf_map_passed int8,
    OUT buf_map_slept int8,
    OUT buf_map_after_write_passed int8,
    OUT buf_map_after_write_slept int8
)
RETURNS RECORD
AS 'MODULE_PATHNAME', 'polar_monitor_stat_wal_buffer_counters'
LANGUAGE C PARALLEL SAFE;

CREATE VIEW polar_wal_buffer_stat AS
    SELECT
        '' AS d_dummy,
        buf_init_async AS v_buf_init_async,
        buf_init_sync AS v_buf_init_sync,
        buf_map_passed AS v_buf_map_passed,
        buf_map_slept AS v_buf_map_slept,
        buf_map_after_write_passed AS v_buf_map_after_write_passed,
        buf_map_after_write_slept AS v_buf_map_after_write_slept
    FROM polar_wal_buffer_stat_counter();

CREATE VIEW polar_wal_buffer_stat_delta AS
    SELECT
        v_buf_init_async AS buf_init_async,
        v_buf_init_sync AS buf_init_sync,
        v_buf_map_passed AS buf_map_passed,
        v_buf_map_slept AS buf_map_slept,
        v_buf_map_after_write_passed AS buf_map_after_write_passed,
        v_buf_map_after_write_slept AS buf_map_after_write_slept
    FROM polar_delta(NULL::polar_wal_buffer_stat);

-- each line for different process type
CREATE FUNCTION polar_wal_io_stat_counter(
    OUT backend_type text,
    OUT context text,
    OUT writes int8,
    OUT write_bytes numeric,
    OUT fsyncs int8
)
RETURNS SETOF RECORD
AS 'MODULE_PATHNAME', 'polar_monitor_stat_wal_io_counters'
LANGUAGE C PARALLEL SAFE;

CREATE VIEW polar_wal_write_io_stat AS
    SELECT
        backend_type AS d_backend_type,
        context AS d_context,
        writes AS v_writes,
        write_bytes AS v_write_bytes,
        fsyncs AS v_fsyncs
    FROM polar_wal_io_stat_counter();

CREATE VIEW polar_wal_write_io_stat_delta AS
    SELECT
        d_backend_type AS backend_type,
        d_context AS context,
        pg_size_pretty(v_write_bytes) AS write_throughput,
        pg_size_pretty(
            CASE
                WHEN v_writes = 0 THEN 0
                ELSE v_write_bytes / v_writes
            END
        ) AS write_unit_size
    FROM polar_delta(NULL::polar_wal_write_io_stat);

CREATE FUNCTION polar_wal_buffer_stat_reset()
RETURNS VOID
AS 'MODULE_PATHNAME', 'polar_monitor_stat_wal_buffer_counters_reset'
LANGUAGE C PARALLEL SAFE;
