/*
 * external/polar_advisor/sql/window_whitelist.sql
 * Test the whitelist for maintenance window.
 * Whitelist is useless now. Maybe we can use it to do dangerous actions like VACUUM FULL.
 */

CREATE EXTENSION IF NOT EXISTS polar_advisor;

\d+ polar_advisor.whitelist_relations

--
-- show whitelist, empty by default
--
SELECT * FROM polar_advisor.whitelist_relations;

--
-- add a relation to whitelist
--
-- error if table does not exist
SELECT polar_advisor.add_relation_to_whitelist_internal('public', 'xxxxxxxx', 'VACUUM');
-- error if action type does not exist
SELECT polar_advisor.add_relation_to_whitelist_internal('public', 'xxxxxxxx', 'WRONG TYPE');
-- ok
SELECT polar_advisor.add_relation_to_whitelist_internal('pg_catalog', 'pg_class', 'VACUUM');

-- show whitelist, should have one entry
SELECT * FROM polar_advisor.whitelist_relations;

--
-- check table in whitelist
--
-- in it
SELECT polar_advisor.is_relation_in_whitelist_internal('pg_catalog', 'pg_class', 'VACUUM');
-- another table not in it
SELECT polar_advisor.is_relation_in_whitelist_internal('pg_catalog', 'pg_index', 'VACUUM');
-- invalid table not in it
SELECT polar_advisor.is_relation_in_whitelist_internal('pg_catalog', 'xxxxx', 'VACUUM');
-- invalid action type not in it
SELECT polar_advisor.is_relation_in_whitelist_internal('pg_catalog', 'pg_class', 'WRONG TYPE');

--
-- remove a relation from whitelist
--
-- return false if table does not exist
SELECT polar_advisor.delete_relation_from_whitelist_internal('public', 'xxxxxxxx', 'VACUUM');
-- error if action type does not exist
SELECT polar_advisor.delete_relation_from_whitelist_internal('pg_catalog', 'pg_class', 'WRONG TYPE');
-- ok
SELECT polar_advisor.delete_relation_from_whitelist_internal('pg_catalog', 'pg_class', 'VACUUM');
-- whitelist is empty now
SELECT * FROM polar_advisor.whitelist_relations;
-- table not in it now
SELECT polar_advisor.is_relation_in_whitelist_internal('pg_catalog', 'pg_class', 'VACUUM');
