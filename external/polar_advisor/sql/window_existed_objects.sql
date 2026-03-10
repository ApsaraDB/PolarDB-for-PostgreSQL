/*
 * external/polar_advisor/sql/window_existed_objects.sql
 * Test whether we can create extension when there are existed objects created by super_pg client.
 * This could happen in some old version instances. The extension should add these
 * objects into the extension if they exist.
 */

--
-- Create extension with no existed objects
--
-- must be owner of extension polar_advisor to drop it
\c - postgres
DROP EXTENSION IF EXISTS polar_advisor;
-- switch back to test user of the extension
\c - polar_polar_advisor
CREATE EXTENSION polar_advisor;
-- show the objects
\dx polar_advisor
\dn polar_advisor
\dt polar_advisor.*
-- we cannot use \di because it shows polar_rowid_<oid>_index and the oid is different every time
SELECT relname FROM pg_class WHERE relkind IN ('i', 'I') AND relname NOT LIKE '%rowid%'
    AND relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'polar_advisor') ORDER BY relname;
\dT polar_advisor.*
\ds polar_advisor.*
\df polar_advisor.*

-- Drop extension
DROP EXTENSION polar_advisor;
-- no objects existed
\dx polar_advisor
\dn polar_advisor
\dt polar_advisor.*
\di polar_advisor.*
\dT polar_advisor.*
\ds polar_advisor.*
\df polar_advisor.*


--
-- Create extension with existed objects
--
DROP EXTENSION IF EXISTS polar_advisor;
DROP SCHEMA IF EXISTS polar_advisor;
-- create objects
\i sql/init_window_objects.sql
-- all objects existed except the extension
\dx polar_advisor
\dn polar_advisor
\dt polar_advisor.*
-- we cannot use \di because it shows polar_rowid_<oid>_index and the oid is different every time
SELECT relname FROM pg_class WHERE relkind IN ('i', 'I') AND relname NOT LIKE '%rowid%'
    AND relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'polar_advisor') ORDER BY relname;
\dT polar_advisor.*
\ds polar_advisor.*
\df polar_advisor.*

-- create extension with existed objects
CREATE EXTENSION polar_advisor;
-- show the objects after creating extension.
-- all objects are added to the extension
\dx polar_advisor
\dn polar_advisor
\dt polar_advisor.*
-- we cannot use \di because it shows polar_rowid_<oid>_index and the oid is different every time
SELECT relname FROM pg_class WHERE relkind IN ('i', 'I') AND relname NOT LIKE '%rowid%'
    AND relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'polar_advisor') ORDER BY relname;
\dT polar_advisor.*
\ds polar_advisor.*
\df polar_advisor.*

-- Drop extension
DROP EXTENSION polar_advisor;
-- no objects existed because they were dropped together with the extension
\dx polar_advisor
\dn polar_advisor
\dt polar_advisor.*
\di polar_advisor.*
\dT polar_advisor.*
\ds polar_advisor.*
\df polar_advisor.*
