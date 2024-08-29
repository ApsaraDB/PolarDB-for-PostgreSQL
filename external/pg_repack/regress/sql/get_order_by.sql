-- set role to superuser
SET ROLE polar_repack_superuser;

--
-- pg_repack issue #3
--
CREATE TABLE issue3_1 (col1 int NOT NULL, col2 text NOT NULL);
CREATE UNIQUE INDEX issue3_1_idx ON issue3_1 (col1, col2 DESC);
SELECT repack.get_order_by('issue3_1_idx'::regclass::oid, 'issue3_1'::regclass::oid);
\! pg_repack -T 3600 -k -U polar_repack_superuser --dbname=contrib_regression_pg_repack --table=issue3_1

CREATE TABLE issue3_2 (col1 int NOT NULL, col2 text NOT NULL);
CREATE UNIQUE INDEX issue3_2_idx ON issue3_2 (col1 DESC, col2 text_pattern_ops);
SELECT repack.get_order_by('issue3_2_idx'::regclass::oid, 'issue3_2'::regclass::oid);
\! pg_repack -T 3600 -k -U polar_repack_superuser --dbname=contrib_regression_pg_repack --table=issue3_2

CREATE TABLE issue3_3 (col1 int NOT NULL, col2 text NOT NULL);
CREATE UNIQUE INDEX issue3_3_idx ON issue3_3 (col1 DESC, col2 DESC);
SELECT repack.get_order_by('issue3_3_idx'::regclass::oid, 'issue3_3'::regclass::oid);
\! pg_repack -T 3600 -k -U polar_repack_superuser --dbname=contrib_regression_pg_repack --table=issue3_3

CREATE TABLE issue3_4 (col1 int NOT NULL, col2 text NOT NULL);
CREATE UNIQUE INDEX issue3_4_idx ON issue3_4 (col1 NULLS FIRST, col2 text_pattern_ops DESC NULLS LAST);
SELECT repack.get_order_by('issue3_4_idx'::regclass::oid, 'issue3_4'::regclass::oid);
\! pg_repack -T 3600 -k -U polar_repack_superuser --dbname=contrib_regression_pg_repack --table=issue3_4

CREATE TABLE issue3_5 (col1 int NOT NULL, col2 text NOT NULL);
CREATE UNIQUE INDEX issue3_5_idx ON issue3_5 (col1 DESC NULLS FIRST, col2 COLLATE "POSIX" DESC);
SELECT repack.get_order_by('issue3_5_idx'::regclass::oid, 'issue3_5'::regclass::oid);
\! pg_repack -T 3600 -k -U polar_repack_superuser --dbname=contrib_regression_pg_repack --table=issue3_5

--
-- pg_repack issue #321
--
CREATE TABLE issue321 (col1 int NOT NULL, col2 text NOT NULL);
CREATE UNIQUE INDEX issue321_idx ON issue321 (col1);
SELECT repack.get_order_by('issue321_idx'::regclass::oid, 1);
SELECT repack.get_order_by(1, 1);
