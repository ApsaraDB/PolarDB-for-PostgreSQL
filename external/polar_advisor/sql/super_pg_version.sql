/*
 * external/polar_advisor/sql/super_pg_version.sql
 * Test the functions to set and get super_pg client version.
 */

CREATE EXTENSION IF NOT EXISTS polar_advisor;

-- Empty by default
SELECT others, others ->> 'super_pg_version' AS version, others ->> 'super_pg_release_date' AS release_date FROM polar_advisor.advisor_window;
SELECT polar_advisor.get_super_pg_version();
SELECT polar_advisor.get_super_pg_release_date();

-- Set it
SELECT polar_advisor.set_super_pg_client_version_release_date(super_pg_version => '1.0', super_pg_release_date => '20240630');

-- Get again
SELECT others, others ->> 'super_pg_version' AS version, others ->> 'super_pg_release_date' AS release_date FROM polar_advisor.advisor_window;
SELECT polar_advisor.get_super_pg_version();
SELECT polar_advisor.get_super_pg_release_date();

-- Update it
SELECT polar_advisor.set_super_pg_client_version_release_date(super_pg_version => '2.0', super_pg_release_date => '20240730');

-- Get again
SELECT others, others ->> 'super_pg_version' AS version, others ->> 'super_pg_release_date' AS release_date FROM polar_advisor.advisor_window;
SELECT polar_advisor.get_super_pg_version();
SELECT polar_advisor.get_super_pg_release_date();
