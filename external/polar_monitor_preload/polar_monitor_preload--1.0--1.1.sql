/* contrib/polar_monitor_preload/polar_monitor_preload--1.0--1.1.sql */

-- complain if script is sourced in psql, rather than via ALTER EXTENSION
\echo Use "ALTER EXTENSION polar_monitor_preload UPDATE to '1.1'" to load this file. \quit

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
