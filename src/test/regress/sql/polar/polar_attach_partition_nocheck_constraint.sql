-------------------------------------------------
-- init
-------------------------------------------------
CREATE SCHEMA test_attach_partition_nocheck_constraint;
SET search_path TO test_attach_partition_nocheck_constraint;

-- range partition
CREATE TABLE t(a int, b varchar(10), c char(10)) PARTITION BY RANGE (a);
CREATE INDEX on t(a);
\d+ t


-------------------------------------------------
-- test
-------------------------------------------------
CREATE TABLE t_p1(a int, b varchar(10), c char(10));
ALTER TABLE t_p1 REPLICA IDENTITY DEFAULT;
INSERT INTO t_p1 VALUES(1, '1', '1');
ALTER TABLE t ATTACH PARTITION t_p1 FOR VALUES FROM (0) TO (100);
\d+ t
\d+ t_p1
SELECT * FROM t WHERE a = 1;

CREATE TABLE t_p2(a int, b varchar(10), c char(10));
ALTER TABLE t_p2 REPLICA IDENTITY DEFAULT;
INSERT INTO t_p2 VALUES(101, '101', '101');
-- disable check constraint, error should be raised
SET polar_enable_skip_partition_constraint_check TO off;
ALTER TABLE t ATTACH PARTITION t_p2 FOR VALUES FROM (100) TO (200) NOCHECK_CONSTRAINT;
\d+ t
\d+ t_p2
SELECT * FROM t WHERE a = 101;
-- enable check constraint
SET polar_enable_skip_partition_constraint_check TO on;
ALTER TABLE t ATTACH PARTITION t_p2 FOR VALUES FROM (100) TO (200) NOCHECK_CONSTRAINT;
\d+ t
\d+ t_p2
SELECT * FROM t WHERE a = 101;

CREATE TABLE t_p3(a int, b varchar(10), c char(10));
ALTER TABLE t_p3 REPLICA IDENTITY DEFAULT;
INSERT INTO t_p3 VALUES(201, '201', '201');
ALTER TABLE t ATTACH PARTITION t_p3 FOR VALUES FROM (200) TO (300) NOCHECK_CONSTRAINT;
\d+ t
\d+ t_p3
SELECT * FROM t WHERE a = 201;

CREATE TABLE t_default(a int, b varchar(10), c char(10));
ALTER TABLE t_default REPLICA IDENTITY DEFAULT;
INSERT INTO t_default VALUES(-1, '-1', '-1');
ALTER TABLE t ATTACH PARTITION t_default DEFAULT;
\d+ t
\d+ t_default
SELECT * FROM t WHERE a = -1;


DROP TABLE t;

-- 2 level hash partition
CREATE TABLE t(a int, b varchar(10), c char(10)) PARTITION BY HASH (a);
CREATE INDEX on t(a);
\d+ t

CREATE TABLE t_p1(a int, b varchar(10), c char(10));
ALTER TABLE t_p1 REPLICA IDENTITY DEFAULT;
INSERT INTO t_p1 VALUES(0, '0', '0');
ALTER TABLE t ATTACH PARTITION t_p1 FOR VALUES WITH (MODULUS 4, REMAINDER 0) NOCHECK_CONSTRAINT ;
SELECT * FROM t WHERE a = 0;

CREATE TABLE t_p2(a int, b varchar(10), c char(10)) PARTITION BY HASH (a);
CREATE TABLE t_p2_p1(a int, b varchar(10), c char(10));
ALTER TABLE t_p2_p1 REPLICA IDENTITY DEFAULT;
CREATE TABLE t_p2_p2(a int, b varchar(10), c char(10));
ALTER TABLE t_p2_p2 REPLICA IDENTITY DEFAULT;
INSERT INTO t_p2_p1 VALUES(1, '1', '1');
INSERT INTO t_p2_p2 VALUES(5, '5', '5');

ALTER TABLE t_p2 ATTACH PARTITION t_p2_p1 FOR VALUES WITH (MODULUS 8, REMAINDER 1) NOCHECK_CONSTRAINT;
ALTER TABLE t_p2 ATTACH PARTITION t_p2_p2 FOR VALUES WITH (MODULUS 8, REMAINDER 5) NOCHECK_CONSTRAINT;
ALTER TABLE t ATTACH PARTITION t_p2 FOR VALUES WITH (MODULUS 4, REMAINDER 1) NOCHECK_CONSTRAINT;

\d+ t

SELECT * FROM t;

ALTER TABLE t ALTER COLUMN b TYPE varchar(15) NOCHECK_CONSTRAINT;

DROP TABLE t;

-- There will be data that does not satisfy the partition constraints.
CREATE TABLE t(a int, b varchar(10), c char(10)) PARTITION BY RANGE (a);
CREATE TABLE t_p1(a int, b varchar(10), c char(10));
ALTER TABLE t_p1 REPLICA IDENTITY DEFAULT;
INSERT INTO t_p1 VALUES(200, '200', '200');
ALTER TABLE t ATTACH PARTITION t_p1 FOR VALUES FROM (0) TO (100);
ALTER TABLE t ATTACH PARTITION t_p1 FOR VALUES FROM (0) TO (100) NOCHECK_CONSTRAINT;
CREATE INDEX ON t(a);
SELECT * FROM t;


-------------------------------------------------
-- clear
-------------------------------------------------
SET client_min_messages TO error;
DROP TABLE t;
DROP SCHEMA test_attach_partition_nocheck_constraint CASCADE;
