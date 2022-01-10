--
-- This file contains tests for const expressions in nested loops.
--

/*--EXPLAIN_QUERY_BEGIN*/
CREATE TABLE foo_tbl (a int, b text);
-- Create an index for foo_tbl(a), so that we are able to generate
-- a plan with NestedLoop.
CREATE UNIQUE INDEX foo_index_a ON foo_tbl(a);
CREATE INDEX foo_index_b ON foo_tbl(b);

INSERT INTO foo_tbl VALUES (1, 'aaa');
INSERT INTO foo_tbl VALUES (2, 'bbb');
INSERT INTO foo_tbl VALUES (3, 'ccc');
INSERT INTO foo_tbl VALUES (4, 'ddd');
INSERT INTO foo_tbl VALUES (5, 'eee');

SELECT * FROM foo_tbl
  WHERE a=(SELECT 1);

SELECT * FROM foo_tbl
  WHERE b=(SELECT 'aaa');

SELECT * FROM foo_tbl
  WHERE a=(SELECT 1) AND b=(SELECT 'aaa');

SELECT * FROM foo_tbl
  WHERE a=(SELECT 1) OR b=(SELECT 'aaa');

DROP TABLE foo_tbl;
