CREATE TABLE t_point_sort (id int, pt point);
INSERT INTO  t_point_sort SELECT generate_series(1,1000000),point(random(), random());

SET polar_enable_gist_sort=on;
-- work with factor
CREATE INDEX idx_tmp_1 ON t_point_sort USING gist(pt) WITH (fillfactor=50);
DROP INDEX idx_tmp_1;
-- work with buffering
CREATE INDEX idx_tmp_2 ON t_point_sort USING gist(pt) WITH (buffering=on);
CREATE INDEX idx_tmp_3 ON t_point_sort USING gist(pt) WITH (buffering=off);
CREATE INDEX idx_tmp_4 ON t_point_sort USING gist(pt) WITH (buffering=auto);
DROP INDEX idx_tmp_2,idx_tmp_3,idx_tmp_4;
-- parallel work
SET max_parallel_maintenance_workers = 0;
CREATE INDEX idx_tmp_1 ON t_point_sort USING gist(pt);
SELECT relpages FROM pg_class WHERE relname = 'idx_tmp_1';
DROP INDEX idx_tmp_1;
SET max_parallel_maintenance_workers = 4;
CREATE INDEX idx_tmp_1 ON t_point_sort USING gist(pt);
SELECT relpages FROM pg_class WHERE relname = 'idx_tmp_1';
-- dml after index
INSERT INTO t_point_sort SELECT generate_series(1,100),point(random(), random());
DELETE FROM t_point_sort WHERE id < 100;
-- reset gist sort
DROP TABLE t_point_sort;
RESET max_parallel_maintenance_workers;
RESET polar_enable_gist_sort;

-- compare query result
CREATE TABLE point_gist_tbl(f1 point);
INSERT INTO point_gist_tbl SELECT '(0,0)' FROM generate_series(0,1000);
INSERT INTO point_gist_tbl VALUES ('(0.0000009,0.0000009)');
SET enable_seqscan TO true;
SET enable_indexscan TO false;
SET enable_bitmapscan TO false;
SELECT COUNT(*) FROM point_gist_tbl WHERE f1 ~= '(0.0000009,0.0000009)'::point;
SELECT COUNT(*) FROM point_gist_tbl WHERE f1 <@ '(0.0000009,0.0000009),(0.0000009,0.0000009)'::box;
SELECT COUNT(*) FROM point_gist_tbl WHERE f1 ~= '(0.0000018,0.0000018)'::point;
SET enable_seqscan TO false;
SET enable_indexscan TO true;
SET enable_bitmapscan TO true;
-- create gist default index
SET polar_enable_gist_sort=off;
CREATE INDEX point_gist_tbl_index ON point_gist_tbl USING gist (f1);
SELECT COUNT(*) FROM point_gist_tbl WHERE f1 ~= '(0.0000009,0.0000009)'::point;
SELECT COUNT(*) FROM point_gist_tbl WHERE f1 <@ '(0.0000009,0.0000009),(0.0000009,0.0000009)'::box;
SELECT COUNT(*) FROM point_gist_tbl WHERE f1 ~= '(0.0000018,0.0000018)'::point;
DROP INDEX point_gist_tbl_index;
-- create gist sort index
SET polar_enable_gist_sort=on;
CREATE INDEX point_gist_tbl_index ON point_gist_tbl USING gist (f1);
SELECT COUNT(*) FROM point_gist_tbl WHERE f1 ~= '(0.0000009,0.0000009)'::point;
SELECT COUNT(*) FROM point_gist_tbl WHERE f1 <@ '(0.0000009,0.0000009),(0.0000009,0.0000009)'::box;
SELECT COUNT(*) FROM point_gist_tbl WHERE f1 ~= '(0.0000018,0.0000018)'::point;
RESET enable_seqscan;
RESET enable_indexscan;
RESET enable_bitmapscan;
RESET polar_enable_gist_sort;
