/*
 * external/polar_advisor/sql/action_command.sql
 * Test generating the vacuum/analyze/reindex command prefix.
 */

CREATE EXTENSION IF NOT EXISTS polar_advisor;

-- ignore the notice printed by polar_advisor.get_relations_to_vacuum_analyze() function
-- which could make the test case unstable
SET client_min_messages TO WARNING;

--
-- Get command prefix
--
SELECT polar_advisor.get_vacuum_cmd_prefix();
SELECT polar_advisor.get_analyze_cmd_prefix();

--
-- Get tables to vacuum, the command prefix is in the full command
--
SELECT schema_name, rel_name, vacuum_cmd, age >= 0 FROM polar_advisor.get_relations_to_vacuum_analyze (
    result_rows => 1000, sleep_interval => INTERVAL '1s',
    table_age_min_limit => 0, table_dead_tup_min_limit => 0
) WHERE schema_name = 'test_polar_advisor' ORDER BY rel_name;

-- we cannot execute the command here because there's VERBOSE option in it and it makes
-- regression test unstable. We test it in super_pg client regression.