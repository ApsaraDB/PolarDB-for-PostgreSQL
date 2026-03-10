/*
 * external/polar_advisor/sql/window_blacklist_vacuum.sql
 * Test the blacklist of vacuum. This case should run before window_vacuum.sql
 * because window_vacuum.sql will do vacuum and update the last_vacuum time,
 * but we need to use the time here.
 */

CREATE EXTENSION IF NOT EXISTS polar_advisor;

-- ignore the notice printed by polar_advisor.get_relations_to_vacuum_analyze() function
-- which could make the test case unstable
SET client_min_messages TO WARNING;

--
-- show blacklist, empty by default
--
SELECT * FROM polar_advisor.blacklist_relations;
SELECT * FROM polar_advisor.get_vacuum_analyze_blacklist();


--
-- Get relations to vacuum before setting blacklist, all tables in it
--
SELECT schema_name, rel_name, vacuum_cmd, age >= 0 FROM polar_advisor.get_relations_to_vacuum_analyze (
    result_rows => 1000, sleep_interval => INTERVAL '5 hours',
    table_age_min_limit => -1, table_dead_tup_min_limit => -1
) WHERE schema_name = 'test_polar_advisor' ORDER BY rel_name;


--
-- Add normal table to blacklist
--
SELECT polar_advisor.add_relation_to_vacuum_analyze_blacklist('test_polar_advisor', 'normal');
SELECT * FROM polar_advisor.get_vacuum_analyze_blacklist();
SELECT polar_advisor.is_relation_in_vacuum_analyze_blacklist('test_polar_advisor', 'normal');

-- get relations to vacuum again, no normal table in it now since it's in blacklist.
-- other tables still in blacklist
SELECT schema_name, rel_name, vacuum_cmd, age >= 0 FROM polar_advisor.get_relations_to_vacuum_analyze (
    result_rows => 1000, sleep_interval => INTERVAL '5 hours',
    table_age_min_limit => -1, table_dead_tup_min_limit => -1
) WHERE schema_name = 'test_polar_advisor' ORDER BY rel_name;

-- remove normal table from blacklist
SELECT polar_advisor.delete_relation_from_vacuum_analyze_blacklist('test_polar_advisor', 'normal');
SELECT * FROM polar_advisor.get_vacuum_analyze_blacklist();
SELECT polar_advisor.is_relation_in_vacuum_analyze_blacklist('test_polar_advisor', 'normal');

-- get relations to vacuum again, normal table in it now since it's not in blacklist now.
SELECT schema_name, rel_name, vacuum_cmd, age >= 0 FROM polar_advisor.get_relations_to_vacuum_analyze (
    result_rows => 1000, sleep_interval => INTERVAL '5 hours',
    table_age_min_limit => -1, table_dead_tup_min_limit => -1
) WHERE schema_name = 'test_polar_advisor' ORDER BY rel_name;


--
-- Add partitioned table with gi to blacklist
--
SELECT polar_advisor.add_relation_to_vacuum_analyze_blacklist('test_polar_advisor', 'prt_gi');
SELECT * FROM polar_advisor.get_vacuum_analyze_blacklist();
SELECT polar_advisor.is_relation_in_vacuum_analyze_blacklist('test_polar_advisor', 'prt_gi');

-- get relations to vacuum again, no prt_gi in it now since it's in blacklist.
-- it's children also not in list.
SELECT schema_name, rel_name, vacuum_cmd, age >= 0 FROM polar_advisor.get_relations_to_vacuum_analyze (
    result_rows => 1000, sleep_interval => INTERVAL '5 hours',
    table_age_min_limit => -1, table_dead_tup_min_limit => -1
) WHERE schema_name = 'test_polar_advisor' ORDER BY rel_name;

-- remove table from blacklist
SELECT polar_advisor.delete_relation_from_vacuum_analyze_blacklist('test_polar_advisor', 'prt_gi');
SELECT * FROM polar_advisor.get_vacuum_analyze_blacklist();
SELECT polar_advisor.is_relation_in_vacuum_analyze_blacklist('test_polar_advisor', 'prt_gi');

-- get relations to vacuum again, table in it now since it's not in blacklist now.
SELECT schema_name, rel_name, vacuum_cmd, age >= 0 FROM polar_advisor.get_relations_to_vacuum_analyze (
    result_rows => 1000, sleep_interval => INTERVAL '5 hours',
    table_age_min_limit => -1, table_dead_tup_min_limit => -1
) WHERE schema_name = 'test_polar_advisor' ORDER BY rel_name;


--
-- Add partitioned partition to blacklist
--
SELECT polar_advisor.add_relation_to_vacuum_analyze_blacklist('test_polar_advisor', 'prt_no_gi_p_0');
SELECT * FROM polar_advisor.get_vacuum_analyze_blacklist();
SELECT polar_advisor.is_relation_in_vacuum_analyze_blacklist('test_polar_advisor', 'prt_no_gi_p_0');

-- get relations to vacuum again, no prt_no_gi_p_0 in it now since it's in blacklist.
-- it's children and parent also not in list.
SELECT schema_name, rel_name, vacuum_cmd, age >= 0 FROM polar_advisor.get_relations_to_vacuum_analyze (
    result_rows => 1000, sleep_interval => INTERVAL '5 hours',
    table_age_min_limit => -1, table_dead_tup_min_limit => -1
) WHERE schema_name = 'test_polar_advisor' ORDER BY rel_name;

-- remove table from blacklist
SELECT polar_advisor.delete_relation_from_vacuum_analyze_blacklist('test_polar_advisor', 'prt_no_gi_p_0');
SELECT * FROM polar_advisor.get_vacuum_analyze_blacklist();
SELECT polar_advisor.is_relation_in_vacuum_analyze_blacklist('test_polar_advisor', 'prt_no_gi_p_0');

-- get relations to vacuum again, table in it now since it's not in blacklist now.
SELECT schema_name, rel_name, vacuum_cmd, age >= 0 FROM polar_advisor.get_relations_to_vacuum_analyze (
    result_rows => 1000, sleep_interval => INTERVAL '5 hours',
    table_age_min_limit => -1, table_dead_tup_min_limit => -1
) WHERE schema_name = 'test_polar_advisor' ORDER BY rel_name;


--
-- Add sub partition to blacklist
--
SELECT polar_advisor.add_relation_to_vacuum_analyze_blacklist('test_polar_advisor', 'prt_no_gi_p_0_p_1');
SELECT * FROM polar_advisor.get_vacuum_analyze_blacklist();
SELECT polar_advisor.is_relation_in_vacuum_analyze_blacklist('test_polar_advisor', 'prt_no_gi_p_0_p_1');

-- get relations to vacuum again, no prt_no_gi_p_0_p_1 in it now since it's in blacklist.
-- it's parent also not in list.
SELECT schema_name, rel_name, vacuum_cmd, age >= 0 FROM polar_advisor.get_relations_to_vacuum_analyze (
    result_rows => 1000, sleep_interval => INTERVAL '5 hours',
    table_age_min_limit => -1, table_dead_tup_min_limit => -1
) WHERE schema_name = 'test_polar_advisor' ORDER BY rel_name;

-- remove table from blacklist
SELECT polar_advisor.delete_relation_from_vacuum_analyze_blacklist('test_polar_advisor', 'prt_no_gi_p_0_p_1');
SELECT * FROM polar_advisor.get_vacuum_analyze_blacklist();
SELECT polar_advisor.is_relation_in_vacuum_analyze_blacklist('test_polar_advisor', 'prt_no_gi_p_0_p_1');

-- get relations to vacuum again, table in it now since it's not in blacklist now.
SELECT schema_name, rel_name, vacuum_cmd, age >= 0 FROM polar_advisor.get_relations_to_vacuum_analyze (
    result_rows => 1000, sleep_interval => INTERVAL '5 hours',
    table_age_min_limit => -1, table_dead_tup_min_limit => -1
) WHERE schema_name = 'test_polar_advisor' ORDER BY rel_name;


--
-- Add partitioned table without gi to blacklist
--
SELECT polar_advisor.add_relation_to_vacuum_analyze_blacklist('test_polar_advisor', 'prt_no_gi');
SELECT * FROM polar_advisor.get_vacuum_analyze_blacklist();
SELECT polar_advisor.is_relation_in_vacuum_analyze_blacklist('test_polar_advisor', 'prt_no_gi');

-- get relations to vacuum again, no prt_no_gi in it now since it's in blacklist.
-- it's children also not in list.
SELECT schema_name, rel_name, vacuum_cmd, age >= 0 FROM polar_advisor.get_relations_to_vacuum_analyze (
    result_rows => 1000, sleep_interval => INTERVAL '5 hours',
    table_age_min_limit => -1, table_dead_tup_min_limit => -1
) WHERE schema_name = 'test_polar_advisor' ORDER BY rel_name;

-- remove table from blacklist
SELECT polar_advisor.delete_relation_from_vacuum_analyze_blacklist('test_polar_advisor', 'prt_no_gi');
SELECT * FROM polar_advisor.get_vacuum_analyze_blacklist();
SELECT polar_advisor.is_relation_in_vacuum_analyze_blacklist('test_polar_advisor', 'prt_no_gi');

-- get relations to vacuum again, table in it now since it's not in blacklist now.
SELECT schema_name, rel_name, vacuum_cmd, age >= 0 FROM polar_advisor.get_relations_to_vacuum_analyze (
    result_rows => 1000, sleep_interval => INTERVAL '5 hours',
    table_age_min_limit => -1, table_dead_tup_min_limit => -1
) WHERE schema_name = 'test_polar_advisor' ORDER BY rel_name;


--
-- Add all tables to blacklist
--
SELECT polar_advisor.add_relation_to_vacuum_analyze_blacklist('test_polar_advisor', 'normal');
SELECT polar_advisor.add_relation_to_vacuum_analyze_blacklist('test_polar_advisor', 'mv_normal');
SELECT polar_advisor.add_relation_to_vacuum_analyze_blacklist('test_polar_advisor', 'prt_gi');
SELECT polar_advisor.add_relation_to_vacuum_analyze_blacklist('test_polar_advisor', 'prt_no_gi');
SELECT * FROM polar_advisor.get_vacuum_analyze_blacklist();
SELECT polar_advisor.is_relation_in_vacuum_analyze_blacklist('test_polar_advisor', 'normal');
SELECT polar_advisor.is_relation_in_vacuum_analyze_blacklist('test_polar_advisor', 'mv_normal');
SELECT polar_advisor.is_relation_in_vacuum_analyze_blacklist('test_polar_advisor', 'prt_gi');
SELECT polar_advisor.is_relation_in_vacuum_analyze_blacklist('test_polar_advisor', 'prt_no_gi');

-- get relations to vacuum again, no table in it now since they are all in blacklist.
SELECT schema_name, rel_name, vacuum_cmd, age >= 0 FROM polar_advisor.get_relations_to_vacuum_analyze (
    result_rows => 1000, sleep_interval => INTERVAL '5 hours',
    table_age_min_limit => -1, table_dead_tup_min_limit => -1
) WHERE schema_name = 'test_polar_advisor' ORDER BY rel_name;

-- clear blacklist for later test
DELETE FROM polar_advisor.blacklist_relations;
