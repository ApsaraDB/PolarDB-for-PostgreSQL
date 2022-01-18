/*--EXPLAIN_QUERY_BEGIN*/
set enable_partitionwise_join =1;

--range
drop table t1 cascade;
CREATE TABLE t1 (c1 int, c2 int, c3 varchar) PARTITION BY RANGE(c1);
CREATE TABLE t1_p3 PARTITION OF t1 FOR VALUES FROM (500) TO (600);
CREATE TABLE t1_p1 PARTITION OF t1 FOR VALUES FROM (0) TO (250);
CREATE TABLE t1_p2 PARTITION OF t1 FOR VALUES FROM (250) TO (500);
INSERT INTO t1 SELECT i, i % 25, to_char(i, 'FM0000') FROM generate_series(0, 599) i WHERE i % 2 = 0;
ANALYZE t1;

drop table t2 cascade;
CREATE TABLE t2 (c1 int, c2 int, c3 varchar) PARTITION BY RANGE(c1);
CREATE TABLE t2_p1 PARTITION OF t2 FOR VALUES FROM (0) TO (250);
CREATE TABLE t2_p2 PARTITION OF t2 FOR VALUES FROM (250) TO (500);
CREATE TABLE t2_p3 PARTITION OF t2 FOR VALUES FROM (500) TO (600);
INSERT INTO t2 SELECT i, i % 25, to_char(i, 'FM0000') FROM generate_series(0, 599) i WHERE i % 3 = 0;
ANALYZE t2;

\i sql/polar-px-dev/polar_partitionwise_join_query.sql

CREATE INDEX idx_t1 ON t1 USING btree (c1);
CREATE INDEX idx_t2 ON t2 USING btree (c1);
ANALYZE t1;
ANALYZE t2;

\i sql/polar-px-dev/polar_partitionwise_join_query.sql


--hash
drop table t1 cascade;
CREATE TABLE t1 (c1 int, c2 int, c3 varchar) PARTITION BY HASH(c1);
CREATE TABLE t1_p3 PARTITION OF t1 FOR VALUES WITH (modulus 3, remainder 0);
CREATE TABLE t1_p1 PARTITION OF t1 FOR VALUES WITH (modulus 3, remainder 1);
CREATE TABLE t1_p2 PARTITION OF t1 FOR VALUES WITH (modulus 3, remainder 2);
INSERT INTO t1 SELECT i, i % 25, to_char(i, 'FM0000') FROM generate_series(0, 599) i WHERE i % 2 = 0;
ANALYZE t1;

drop table t2 cascade;
CREATE TABLE t2 (c1 int, c2 int, c3 varchar) PARTITION BY HASH(c1);
CREATE TABLE t2_p1 PARTITION OF t2 FOR VALUES WITH (modulus 3, remainder 1);
CREATE TABLE t2_p2 PARTITION OF t2 FOR VALUES WITH (modulus 3, remainder 2);
CREATE TABLE t2_p3 PARTITION OF t2 FOR VALUES WITH (modulus 3, remainder 0);
INSERT INTO t2 SELECT i, i % 25, to_char(i, 'FM0000') FROM generate_series(0, 599) i WHERE i % 3 = 0;
ANALYZE t2;

\i sql/polar-px-dev/polar_partitionwise_join_query.sql

CREATE INDEX idx_t1 ON t1 USING btree (c1);
CREATE INDEX idx_t2 ON t2 USING btree (c1);
ANALYZE t1;
ANALYZE t2;

\i sql/polar-px-dev/polar_partitionwise_join_query.sql


drop table t1 cascade;
drop table t2 cascade;
--list
drop table t1 cascade;
CREATE TABLE t1 (c1 int, c2 int, c3 varchar) PARTITION BY LIST(c1);
CREATE TABLE t1_p3 PARTITION OF t1 DEFAULT;
CREATE TABLE t1_p1 PARTITION OF t1 FOR VALUES IN (10, 11, 12, 13, 14, 15, 16, 17, 18, 19);
CREATE TABLE t1_p2 PARTITION OF t1 FOR VALUES IN (0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
INSERT INTO t1 SELECT i % 30, i % 25, to_char(i, 'FM0000') FROM generate_series(0, 599) i WHERE i % 2 = 0;
ANALYZE t1;

drop table t2 cascade;
CREATE TABLE t2 (c1 int, c2 int, c3 varchar) PARTITION BY LIST(c1);
CREATE TABLE t2_p1 PARTITION OF t2 DEFAULT;
CREATE TABLE t2_p2 PARTITION OF t2 FOR VALUES IN (10, 11, 12, 13, 14, 15, 16, 17, 18, 19);
CREATE TABLE t2_p3 PARTITION OF t2 FOR VALUES IN (0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
INSERT INTO t2 SELECT i % 30, i % 25, to_char(i, 'FM0000') FROM generate_series(0, 599) i WHERE i % 3 = 0;
ANALYZE t2;

\i sql/polar-px-dev/polar_partitionwise_join_query.sql

CREATE INDEX idx_t1 ON t1 USING btree (c1);
CREATE INDEX idx_t2 ON t2 USING btree (c1);
ANALYZE t1;
ANALYZE t2;

\i sql/polar-px-dev/polar_partitionwise_join_query.sql

