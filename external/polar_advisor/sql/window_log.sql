/*
 * external/polar_advisor/sql/window_log.sql
 * Test the advisor log function and table.
 */

CREATE EXTENSION IF NOT EXISTS polar_advisor;

--
-- Relation level log
--

-- insert log
-- ok
SELECT polar_advisor.action_log_start(exec_id => 1, db_name => 'postgres', schema_name => 'public',  relation_name => 't1',
    event_type => 'VACUUM ANALYZE', sql_cmd => 'VACUUM (VERBOSE, ANALYZE) t1', size_before => 10);
-- ok
SELECT polar_advisor.action_log_start(exec_id => 3, db_name => 'polardb_admin', schema_name => 'pg_catalog',  relation_name => 'pg_class',
    event_type => 'ANALYZE', sql_cmd => 'ANALYZE (VERBOSE) pg_class', size_before => 30);
-- ok even if every thing is null
SELECT polar_advisor.action_log_start(exec_id => NULL, db_name => NULL, schema_name => NULL,  relation_name => NULL,
    event_type => NULL, sql_cmd => NULL, size_before => NULL, start_time => NULL);

SELECT id, exec_id, db_name, schema_name, relation_name, event_type, sql_cmd, detail, tuples_deleted, tuples_dead_now,
    tuples_now, pages_scanned, pages_pinned, pages_frozen_now, pages_truncated, pages_now, idx_tuples_deleted, idx_tuples_now, idx_pages_now, idx_pages_deleted,
    idx_pages_reusable, size_before, size_now, age_decreased, others, now() - start_time < INTERVAL '5s' AS "inserted just now", end_time
FROM polar_advisor.advisor_log ORDER BY id;

-- update end time and other info of the inserted log
-- update with log id 1
SELECT polar_advisor.action_log_end(id => 1, detail => 'test detail', tuples_deleted => 1, tuples_dead_now => 2, tuples_now => 3,
    pages_scanned => 4, pages_pinned => 5, pages_frozen_now => 6, pages_truncated => 7, pages_now => 8,
    idx_tuples_deleted => 9, idx_tuples_now => 10, idx_pages_now => 11, idx_pages_deleted => 12, idx_pages_reusable => 13,
    size_now => 14, age_decreased => 15);
-- update with log id 2
SELECT polar_advisor.action_log_end(id => 2, detail => 'test detail2', tuples_deleted => 1, tuples_dead_now => 2, tuples_now => 3,
    pages_scanned => 4, pages_pinned => 5, pages_frozen_now => 6, pages_truncated => 7, pages_now => 8,
    idx_tuples_deleted => 9, idx_tuples_now => 10, idx_pages_now => 11, idx_pages_deleted => 12, idx_pages_reusable => 13,
    size_now => 14, age_decreased => 15);
-- update with log id 3, ok even if every thing is null
SELECT polar_advisor.action_log_end(id => 3, detail => NULL, tuples_deleted => NULL, tuples_dead_now => NULL, tuples_now => NULL,
    pages_scanned => NULL, pages_pinned => NULL, pages_frozen_now => NULL, pages_truncated => NULL, pages_now => NULL,
    idx_tuples_deleted => NULL, idx_tuples_now => NULL, idx_pages_now => NULL, idx_pages_deleted => NULL, idx_pages_reusable => NULL,
    size_now => NULL, age_decreased => NULL);
-- update with log id 4, it doesn't exists and update 0 row
SELECT polar_advisor.action_log_end(id => 4, detail => 'test detail2', tuples_deleted => 1, tuples_dead_now => 2, tuples_now => 3,
    pages_scanned => 4, pages_pinned => 5, pages_frozen_now => 6, pages_truncated => 7, pages_now => 8,
    idx_tuples_deleted => 9, idx_tuples_now => 10, idx_pages_now => 11, idx_pages_deleted => 12, idx_pages_reusable => 13,
    size_now => 14, age_decreased => 15);

SELECT id, exec_id, db_name, schema_name, relation_name, event_type, sql_cmd, detail, tuples_deleted, tuples_dead_now,
    tuples_now, pages_scanned, pages_pinned, pages_frozen_now, pages_truncated, pages_now, idx_tuples_deleted, idx_tuples_now, idx_pages_now, idx_pages_deleted,
    idx_pages_reusable, size_before, size_now, age_decreased, others, now() - start_time < INTERVAL '10s' AS "inserted just now", now() - end_time < INTERVAL '5s' AS "updated just now"
FROM polar_advisor.advisor_log ORDER BY id;


--
-- DB level log
--
-- insert db log
-- ok
SELECT polar_advisor.action_db_log_start(exec_id => 1, db_name => 'postgres', event_type => 'VACUUM ANALYZE', total_relation => 10,
    age_before => 1000, db_size_before => 100000, cluster_age_before => 1001);
-- ok
SELECT polar_advisor.action_db_log_start(exec_id => 3, db_name => 'polardb_admin', event_type => 'ANALYZE', total_relation => 20,
    age_before => 500, db_size_before => 20000, cluster_age_before => 600);
-- ok even if every thing is null
SELECT polar_advisor.action_db_log_start(exec_id => NULL, db_name => NULL, event_type => NULL, total_relation => NULL,
    age_before => NULL, db_size_before => NULL, cluster_age_before => NULL, start_time => NULL);

SELECT id, exec_id, db_name, event_type, total_relation, acted_relation, age_before, age_after, others,
    now() - start_time < INTERVAL '5s' AS "inserted just now", end_time
FROM polar_advisor.db_level_advisor_log ORDER BY id;

-- update end time and other info of the inserted log
-- update with log id 1
SELECT polar_advisor.action_db_log_end(id => 1, acted_relation => 3, age_after => 101, db_size_after => 1001, cluster_age_after => 500);
-- update with log id 2
SELECT polar_advisor.action_db_log_end(id => 2, acted_relation => 5, age_after => 201, db_size_after => 2001, cluster_age_after => 300);
-- ok even if every thing is null
SELECT polar_advisor.action_db_log_end(id => 3, acted_relation => NULL, age_after => NULL, db_size_after => NULL, cluster_age_after => NULL);
-- update 0 row because id 4 doesn't exists
SELECT polar_advisor.action_db_log_end(id => 4, acted_relation => NULL, age_after => NULL, db_size_after => NULL, cluster_age_after => NULL, end_time => NULL);

SELECT id, exec_id, db_name, event_type, total_relation, acted_relation, age_before, age_after, others,
    now() - start_time < INTERVAL '10s' AS "inserted just now", now() - end_time < INTERVAL '5s' AS "updated just now"
FROM polar_advisor.db_level_advisor_log ORDER BY id;


--
-- Universe Explorer collector SQL
--
SELECT polar_advisor.report_exception('test exception');
SELECT
  w.enabled::INT AS rt_window_enabled,
  w.last_error_detail AS dim_last_error_detail,
  w.others AS dim_window_others,
  l.exec_id AS rt_exec_id,
  EXTRACT(EPOCH FROM l.start_time)::FLOAT > 0 AS rt_start_time,
  EXTRACT(EPOCH FROM l.end_time)::FLOAT > 0 AS rt_end_time,
  EXTRACT(EPOCH FROM l.db_start_time)::FLOAT > 0 AS rt_db_start_time,
  EXTRACT(EPOCH FROM l.db_end_time)::FLOAT > 0 AS rt_db_end_time,
  l.db_name AS dim_db_name,
  l.schema_name AS dim_schema_name,
  l.relation_name AS dim_relation_name,
  l.event_type AS dim_event_type,
  l.others::TEXT AS dim_others,
  l.db_others::TEXT AS dim_db_others,
  l.total_relation AS rt_total_relation,
  l.acted_relation AS rt_acted_relation,
  l.age_before AS rt_age_before,
  l.age_after AS rt_age_after,
  l.tuples_deleted AS rt_tuples_deleted,
  l.tuples_dead_now AS rt_tuples_dead_now,
  l.tuples_now AS rt_tuples_now,
  l.pages_scanned AS rt_pages_scanned,
  l.pages_pinned AS rt_pages_pinned,
  l.pages_frozen_now AS rt_pages_frozen_now,
  l.pages_truncated AS rt_pages_truncated,
  l.pages_now AS rt_pages_now,
  l.idx_tuples_deleted AS rt_idx_tuples_deleted,
  l.idx_tuples_now AS rt_idx_tuples_now,
  l.idx_pages_now AS rt_idx_pages_now,
  l.idx_pages_deleted AS rt_idx_pages_deleted,
  l.idx_pages_reusable AS rt_idx_pages_reusable,
  l.size_before AS rt_size_before,
  l.size_now AS rt_size_now
FROM
  polar_advisor.advisor_window w
LEFT JOIN (
  SELECT
    a.*,
    d.start_time AS db_start_time,
    d.end_time AS db_end_time,
    d.total_relation,
    d.acted_relation,
    d.age_before,
    d.age_after,
    d.others AS db_others
  FROM
    polar_advisor.advisor_log a,
    polar_advisor.db_level_advisor_log d
  WHERE
    d.exec_id = a.exec_id
    AND a.db_name = d.db_name
    AND a.event_type = d.event_type
    AND now() - a.end_time < INTERVAL '10 min'
) l ON true;


--
-- Delete old log
--
-- Delete old log before 1 hour, 0 row deleted
SELECT polar_advisor.delete_old_log(INTERVAL '1 hour');
SELECT COUNT(1) FROM polar_advisor.advisor_log;

-- Delete old log before 1ms, all rows deleted
SELECT polar_advisor.delete_old_log(INTERVAL '1 ms');
SELECT * FROM polar_advisor.advisor_log;

-- Delete old log before 1 hour, 0 row deleted
SELECT polar_advisor.delete_old_db_log(INTERVAL '1 hour');
SELECT COUNT(1) FROM polar_advisor.db_level_advisor_log;

-- Delete old log before 1ms, all rows deleted
SELECT polar_advisor.delete_old_db_log(INTERVAL '1 ms');
SELECT * FROM polar_advisor.db_level_advisor_log;
