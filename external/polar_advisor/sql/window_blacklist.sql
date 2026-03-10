/*
 * external/polar_advisor/sql/window_blacklist.sql
 * Test the blacklist for maintenance window. super_pg client never operate the relations in the blacklist.
 * We only test get/set blacklist here and the vacuum/reindex behavior of the blacklist is tested in
 * blacklist_vacuum.sql and blacklist_reindex.sql.
 */

CREATE EXTENSION IF NOT EXISTS polar_advisor;

\d+ polar_advisor.blacklist_relations

--
-- show blacklist, empty by default
--
SELECT * FROM polar_advisor.blacklist_relations;
SELECT * FROM polar_advisor.get_vacuum_analyze_blacklist();
SELECT * FROM polar_advisor.get_reindex_blacklist();

--
-- add a relation to blacklist
--
-- error if table does not exist
SELECT polar_advisor.add_relation_to_vacuum_analyze_blacklist('public', 'xxxxxxxx');
-- ok
SELECT polar_advisor.add_relation_to_vacuum_analyze_blacklist('pg_catalog', 'pg_class');
SELECT polar_advisor.add_relation_to_vacuum_analyze_blacklist('pg_catalog', 'pg_database');
-- error if index does not exist
SELECT polar_advisor.add_relation_to_reindex_blacklist('public', 'xxxxxxxx');
-- ok
SELECT polar_advisor.add_relation_to_reindex_blacklist('pg_catalog', 'pg_class_relname_nsp_index');
SELECT polar_advisor.add_relation_to_reindex_blacklist('pg_catalog', 'pg_database_datname_index');

-- show blacklist, should have some rows
SELECT * FROM polar_advisor.blacklist_relations;
SELECT * FROM polar_advisor.get_vacuum_analyze_blacklist();
SELECT * FROM polar_advisor.get_reindex_blacklist();

--
-- check table in blacklist
--
-- in it
SELECT polar_advisor.is_relation_in_vacuum_analyze_blacklist('pg_catalog', 'pg_class');
SELECT polar_advisor.is_relation_in_vacuum_analyze_blacklist('pg_catalog', 'pg_database');
-- another table not in it
SELECT polar_advisor.is_relation_in_vacuum_analyze_blacklist('pg_catalog', 'pg_index');
-- index not in it
SELECT polar_advisor.is_relation_in_vacuum_analyze_blacklist('pg_catalog', 'pg_class_relname_nsp_index');
-- invalid table not in it
SELECT polar_advisor.is_relation_in_vacuum_analyze_blacklist('pg_catalog', 'xxxxx');

-- in it
SELECT polar_advisor.is_relation_in_reindex_blacklist('pg_catalog', 'pg_class_relname_nsp_index');
SELECT polar_advisor.is_relation_in_reindex_blacklist('pg_catalog', 'pg_database_datname_index');
-- another index not in it
SELECT polar_advisor.is_relation_in_reindex_blacklist('pg_catalog', 'pg_class_oid_index');
-- table not in it
SELECT polar_advisor.is_relation_in_reindex_blacklist('pg_catalog', 'pg_class');
-- invalid index not in it
SELECT polar_advisor.is_relation_in_reindex_blacklist('pg_catalog', 'xxxxx');


--
-- remove a relation from blacklist
--
-- return false if table does not exist
SELECT polar_advisor.delete_relation_from_vacuum_analyze_blacklist('public', 'xxxxxxxx');
-- ok
SELECT polar_advisor.delete_relation_from_vacuum_analyze_blacklist('pg_catalog', 'pg_class');
-- return false if index does not exist
SELECT polar_advisor.delete_relation_from_reindex_blacklist('public', 'xxxxxxxx');
-- ok
SELECT polar_advisor.delete_relation_from_reindex_blacklist('pg_catalog', 'pg_class_relname_nsp_index');

-- less rows in blacklist now
SELECT * FROM polar_advisor.blacklist_relations;
SELECT * FROM polar_advisor.get_vacuum_analyze_blacklist();
SELECT * FROM polar_advisor.get_reindex_blacklist();

-- table not in it now
SELECT polar_advisor.is_relation_in_vacuum_analyze_blacklist('pg_catalog', 'pg_class');
-- index not in it now
SELECT polar_advisor.is_relation_in_reindex_blacklist('pg_catalog', 'pg_class_relname_nsp_index');
-- another table still in it now
SELECT polar_advisor.is_relation_in_vacuum_analyze_blacklist('pg_catalog', 'pg_database');
-- another index still in it now
SELECT polar_advisor.is_relation_in_reindex_blacklist('pg_catalog', 'pg_database_datname_index');

--
-- clear blacklist
--
DELETE FROM polar_advisor.blacklist_relations;
