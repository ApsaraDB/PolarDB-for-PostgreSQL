/* external/polar_monitor/polar_monitor--1.0--1.1.sql */

-- complain if script is sourced in psql, rather than via ALTER EXTENSION
\echo Use "ALTER EXTENSION polar_monitor UPDATE TO '1.1'" to load this file. \quit

CREATE FUNCTION polar_write_combine_latency(
    OUT blocks int4,
    OUT count int8,
    OUT latency int4
)
RETURNS SETOF RECORD
AS 'MODULE_PATHNAME'
LANGUAGE C;

CREATE VIEW polar_write_combine_latency AS
    SELECT * FROM polar_write_combine_latency();

CREATE VIEW polar_write_combine_stats AS
    SELECT COALESCE((SUM(blocks * current_setting('block_size')::int4 * count) / SUM(count))::int4, 0) AS size,
           COALESCE(SUM(count), 0) AS count,
           COALESCE((SUM(latency * count) / SUM(count))::int4, 0) AS latency
    FROM polar_write_combine_latency();

CREATE FUNCTION polar_write_combine_stats_reset()
RETURNS VOID
AS 'MODULE_PATHNAME'
LANGUAGE C;
