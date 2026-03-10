/*
 * external/polar_advisor/sql/window_vacuum.sql
 * Test getting tables to vacuum
 */

CREATE EXTENSION IF NOT EXISTS polar_advisor;

SET search_path TO test_polar_advisor;

-- ignore the notice printed by polar_advisor.get_relations_to_vacuum_analyze() function
-- which could make the test case unstable
SET client_min_messages TO WARNING;

--
-- Get tables to vacuum with no stats limit, all the tables should be returned
--

-- normal table in list
-- partitioned table with global index in list because we should vacuum the parent table for the table with global index
-- partitions with global index not in vacuum list because their parent already in it
-- partitions without global index in vacuum list because their parent is only logical
-- partitioned table without global index not in vacuum list because it's partitions in it
-- partitioned table without global index in analyze list because we don't do vacuum analyze for it
-- view not in list
-- mat view in list
-- index not in list
SELECT schema_name, rel_name, vacuum_cmd, age >= 0 FROM polar_advisor.get_relations_to_vacuum_analyze (
    result_rows => 1000, sleep_interval => INTERVAL '5 hours',
    table_age_min_limit => 0, table_dead_tup_min_limit => 0
)
-- do not show the pg_catalog tables because they are too many
WHERE schema_name = 'test_polar_advisor' ORDER BY rel_name;

-- there are some catalog tables returned
SELECT COUNT(1) > 1 AS "catalog tables returned" FROM polar_advisor.get_relations_to_vacuum_analyze (
    result_rows => 1000, sleep_interval => INTERVAL '1s',
    table_age_min_limit => 0, table_dead_tup_min_limit => 0
) WHERE schema_name = 'pg_catalog';

--
-- Get tables to vacuum with age and dead tuples limit.
-- Only 1 limit is useless because the table will be returned if it has many dead tuples
-- or it's age is big.
--
-- no vacuum result because the limit it too big.
-- but there will be analyze result because we cannot skip it except by last_analyze time
SELECT schema_name, rel_name, vacuum_cmd, age >= 0 FROM polar_advisor.get_relations_to_vacuum_analyze (
    result_rows => 1000, sleep_interval => INTERVAL '1s',
    table_age_min_limit => 10000000, table_dead_tup_min_limit => 10000000
) ORDER BY rel_name;

-- smaller age limit, some rows returned
SELECT COUNT(1) > 1 AS "some tables returned" FROM polar_advisor.get_relations_to_vacuum_analyze (
    result_rows => 1000, sleep_interval => INTERVAL '1s',
    table_age_min_limit => 10, table_dead_tup_min_limit => 10
);

-- make dead tuples in tables
DELETE FROM normal;
DELETE FROM prt_gi;
DELETE FROM prt_no_gi;
-- wait for the dead tuples updated in pg_stat_all_tables
SELECT pg_sleep(0.2);
DELETE FROM normal;
DELETE FROM prt_gi;
DELETE FROM prt_no_gi;
-- wait for the dead tuples updated in pg_stat_all_tables
SELECT pg_sleep(0.2);
DELETE FROM normal;
DELETE FROM prt_gi;
DELETE FROM prt_no_gi;
-- wait for the dead tuples updated in pg_stat_all_tables
SELECT pg_sleep(0.2);
-- smaller dead tuples limit, some rows returned
SELECT schema_name, rel_name, vacuum_cmd, age >= 0 FROM polar_advisor.get_relations_to_vacuum_analyze (
    result_rows => 1000, sleep_interval => INTERVAL '1s',
    table_age_min_limit => 10, table_dead_tup_min_limit => 10
) WHERE schema_name = 'test_polar_advisor' ORDER BY rel_name;

--
-- skip latest vacuumed/analyzed table
--
-- all tables in list when no limit set
SELECT schema_name, rel_name, vacuum_cmd, age >= 0 FROM polar_advisor.get_relations_to_vacuum_analyze (
    result_rows => 1000, sleep_interval => INTERVAL '1s',
    table_age_min_limit => -1, table_dead_tup_min_limit => -1
) WHERE schema_name = 'test_polar_advisor' ORDER BY rel_name;
-- analyze table
ANALYZE prt_no_gi;
SELECT pg_sleep(0.2);
ANALYZE prt_no_gi;
SELECT pg_sleep(0.2);
ANALYZE prt_no_gi;
-- wait for the last_analyze time updated
SELECT pg_sleep(0.2);
SELECT last_analyze IS NOT NULL FROM pg_stat_user_tables WHERE relname = 'prt_no_gi';
-- table not in analyze list because it's analyzed just now
SELECT schema_name, rel_name, vacuum_cmd, age >= 0 FROM polar_advisor.get_relations_to_vacuum_analyze (
    result_rows => 1000, sleep_interval => INTERVAL '5 min',
    table_age_min_limit => -1, table_dead_tup_min_limit => -1
) WHERE schema_name = 'test_polar_advisor' ORDER BY rel_name;

-- vacuum normal table
VACUUM ANALYZE normal;
SELECT pg_sleep(0.2);
VACUUM ANALYZE normal;
SELECT pg_sleep(0.2);
-- sometimes the stats not updated and vacuum again
VACUUM ANALYZE normal;
-- wait for the last_vacuum time updated
SELECT pg_sleep(0.2);
SELECT last_vacuum IS NOT NULL FROM pg_stat_user_tables WHERE relname = 'normal';
-- normal table not in list because it's vacuumed just now
SELECT schema_name, rel_name, vacuum_cmd, age >= 0 FROM polar_advisor.get_relations_to_vacuum_analyze (
    result_rows => 1000, sleep_interval => INTERVAL '5 min',
    table_age_min_limit => -1, table_dead_tup_min_limit => -1
) WHERE schema_name = 'test_polar_advisor' ORDER BY rel_name;

-- the same for partitioned table
-- vacuum partitioned table
VACUUM prt_gi;
VACUUM prt_no_gi;
SELECT pg_sleep(0.2);
VACUUM prt_gi;
VACUUM prt_no_gi;
SELECT pg_sleep(0.2);
-- sometimes the stats not updated and vacuum again
VACUUM prt_gi;
VACUUM prt_no_gi;
SELECT pg_sleep(0.2);
VACUUM prt_gi;
VACUUM prt_no_gi;
-- wait for the last_vacuum time updated
SELECT pg_sleep(0.2);
-- partitioned table not in list because it's vacuumed just now
SELECT schema_name, rel_name, vacuum_cmd, age >= 0 FROM polar_advisor.get_relations_to_vacuum_analyze (
    result_rows => 1000, sleep_interval => INTERVAL '5 min',
    table_age_min_limit => -1, table_dead_tup_min_limit => -1
) WHERE schema_name = 'test_polar_advisor' ORDER BY rel_name;

-- the same for mat view
-- vacuum mat view
VACUUM ANALYZE mv_normal;
SELECT pg_sleep(0.2);
VACUUM ANALYZE mv_normal;
SELECT pg_sleep(0.2);
-- sometimes the stats not updated and vacuum again
VACUUM ANALYZE mv_normal;
-- wait for the last_vacuum time updated
SELECT pg_sleep(0.2);
SELECT last_vacuum IS NOT NULL FROM pg_stat_user_tables WHERE relname = 'mv_normal';
-- mat view not in list because it's vacuumed just now
SELECT schema_name, rel_name, vacuum_cmd, age >= 0 FROM polar_advisor.get_relations_to_vacuum_analyze (
    result_rows => 1000, sleep_interval => INTERVAL '5 min',
    table_age_min_limit => -1, table_dead_tup_min_limit => -1
) WHERE schema_name = 'test_polar_advisor' ORDER BY rel_name;
