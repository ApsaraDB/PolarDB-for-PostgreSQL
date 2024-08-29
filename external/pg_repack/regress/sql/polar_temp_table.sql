-- temp table is not supported since:
-- 1. we do not know which schema is the temp table in
-- 2. pg_repack only support tables with pg_class.relpersistence = 'p'
SET ROLE polar_repack_superuser;
CREATE TEMP TABLE pg_repack_temp (col1 int NOT NULL, col2 int NOT NULL);
-- ERROR
\! pg_repack -T 3600 -k -U polar_repack_superuser --dbname=contrib_regression_pg_repack --table=pg_repack_temp