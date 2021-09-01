/* contrib/polar_monitor/polar_monitor--1.1.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION polar_monitor" to load this file. \quit

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

