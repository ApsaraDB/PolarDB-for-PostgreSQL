--
-- Test the LOCK statement
--

-- Setup
CREATE SCHEMA lock_schema1;
SET search_path = lock_schema1;
CREATE TABLE lock_tbl1 (a BIGINT);
CREATE VIEW lock_view1 AS SELECT 1;
CREATE ROLE regress_rol_lock1;
ALTER ROLE regress_rol_lock1 SET search_path = lock_schema1;
GRANT USAGE ON SCHEMA lock_schema1 TO regress_rol_lock1;

-- Try all valid lock options; also try omitting the optional TABLE keyword.
BEGIN TRANSACTION;
LOCK TABLE lock_tbl1 IN ACCESS SHARE MODE;
LOCK lock_tbl1 IN ROW SHARE MODE;
LOCK TABLE lock_tbl1 IN ROW EXCLUSIVE MODE;
LOCK TABLE lock_tbl1 IN SHARE UPDATE EXCLUSIVE MODE;
LOCK TABLE lock_tbl1 IN SHARE MODE;
LOCK lock_tbl1 IN SHARE ROW EXCLUSIVE MODE;
LOCK TABLE lock_tbl1 IN EXCLUSIVE MODE;
LOCK TABLE lock_tbl1 IN ACCESS EXCLUSIVE MODE;
ROLLBACK;

-- Try using NOWAIT along with valid options.
BEGIN TRANSACTION;
LOCK TABLE lock_tbl1 IN ACCESS SHARE MODE NOWAIT;
LOCK TABLE lock_tbl1 IN ROW SHARE MODE NOWAIT;
LOCK TABLE lock_tbl1 IN ROW EXCLUSIVE MODE NOWAIT;
LOCK TABLE lock_tbl1 IN SHARE UPDATE EXCLUSIVE MODE NOWAIT;
LOCK TABLE lock_tbl1 IN SHARE MODE NOWAIT;
LOCK TABLE lock_tbl1 IN SHARE ROW EXCLUSIVE MODE NOWAIT;
LOCK TABLE lock_tbl1 IN EXCLUSIVE MODE NOWAIT;
LOCK TABLE lock_tbl1 IN ACCESS EXCLUSIVE MODE NOWAIT;
LOCK TABLE lock_view1 IN EXCLUSIVE MODE;   -- Will fail; can't lock a non-table
ROLLBACK;

-- Verify that we can lock a table with inheritance children.
CREATE TABLE lock_tbl2 (b BIGINT) INHERITS (lock_tbl1);
CREATE TABLE lock_tbl3 () INHERITS (lock_tbl2);
BEGIN TRANSACTION;
LOCK TABLE lock_tbl1 * IN ACCESS EXCLUSIVE MODE;
ROLLBACK;

-- Verify that we can't lock a child table just because we have permission
-- on the parent, but that we can lock the parent only.
GRANT UPDATE ON TABLE lock_tbl1 TO regress_rol_lock1;
SET ROLE regress_rol_lock1;
BEGIN;
LOCK TABLE lock_tbl1 * IN ACCESS EXCLUSIVE MODE;
ROLLBACK;
BEGIN;
LOCK TABLE ONLY lock_tbl1;
ROLLBACK;
RESET ROLE;

--
-- Clean up
--
DROP VIEW lock_view1;
DROP TABLE lock_tbl3;
DROP TABLE lock_tbl2;
DROP TABLE lock_tbl1;
DROP SCHEMA lock_schema1 CASCADE;
DROP ROLE regress_rol_lock1;


-- atomic ops tests
RESET search_path;
SELECT test_atomic_ops();
