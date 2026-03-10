/*
 * external/polar_advisor/sql/window_stats.sql
 * Test the stats of maintenance window
 */

CREATE EXTENSION IF NOT EXISTS polar_advisor;

--
-- Get cluster stats
--
SELECT polar_advisor.get_cluster_age() > 0;


--
-- Get db stats
--

-- db age and size
SELECT db_size > 0, age > 0 FROM polar_advisor.get_db_stats();
SELECT db_size > 0, age > 0 FROM polar_advisor.get_db_stats('template1');
-- NULL
SELECT db_size > 0, age > 0 FROM polar_advisor.get_db_stats('xxxxx');

--
-- Stats for vacuum
--

-- get total relation num that can be vacuumed in current db
SELECT polar_advisor.get_total_relation_num_for_vacuum() > 0 AS total_relation_num_ok;

-- get stats related to vacuum for single relation
-- normal table
SELECT total_relation_size > 0, age > 0 FROM polar_advisor.get_vacuum_stats_of_relation('test_polar_advisor', 'normal');

-- partitioned table with gi
SELECT total_relation_size > 0, age > 0 FROM polar_advisor.get_vacuum_stats_of_relation('test_polar_advisor', 'prt_gi');
SELECT total_relation_size > 0, age > 0 FROM polar_advisor.get_vacuum_stats_of_relation('test_polar_advisor', 'prt_gi_p_def');
SELECT total_relation_size > 0, age > 0 FROM polar_advisor.get_vacuum_stats_of_relation('test_polar_advisor', 'prt_gi_p_0');
SELECT total_relation_size > 0, age > 0 FROM polar_advisor.get_vacuum_stats_of_relation('test_polar_advisor', 'prt_gi_p_0_p_1');

-- partitioned table without gi
SELECT total_relation_size > 0, age > 0 FROM polar_advisor.get_vacuum_stats_of_relation('test_polar_advisor', 'prt_no_gi');
SELECT total_relation_size > 0, age > 0 FROM polar_advisor.get_vacuum_stats_of_relation('test_polar_advisor', 'prt_no_gi_p_def');
SELECT total_relation_size > 0, age > 0 FROM polar_advisor.get_vacuum_stats_of_relation('test_polar_advisor', 'prt_no_gi_p_0');
SELECT total_relation_size > 0, age > 0 FROM polar_advisor.get_vacuum_stats_of_relation('test_polar_advisor', 'prt_no_gi_p_0_p_1');

-- NULL because the relation is invalid
SELECT total_relation_size > 0, age > 0 FROM polar_advisor.get_vacuum_stats_of_relation('test_polar_advisor', 'xxxxx');
