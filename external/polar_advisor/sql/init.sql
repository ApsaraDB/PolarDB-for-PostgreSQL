/*
 * external/polar_advisor/sql/init.sql
 * Initialize the objects needed by later test.
 */

DROP EXTENSION IF EXISTS polar_advisor;
DROP SCHEMA IF EXISTS polar_advisor;
CREATE EXTENSION IF NOT EXISTS polar_advisor;
CREATE SCHEMA test_polar_advisor;
SET search_path TO test_polar_advisor;

--
-- Init tables
--
-- init normal table
CREATE TABLE IF NOT EXISTS normal (id INT, a TEXT) WITH (autovacuum_enabled=FALSE);
CREATE INDEX normal_id_idx ON normal(id);
INSERT INTO normal SELECT i, random() FROM generate_series(0, 10000) i;

-- init partitioned table with global index
CREATE TABLE IF NOT EXISTS prt_gi (id INT, a TEXT) PARTITION BY RANGE (id);
DO $$
BEGIN
    FOR i IN 0..1 LOOP
        EXECUTE 'CREATE TABLE IF NOT EXISTS prt_gi_p_' || i || 
            ' PARTITION OF prt_gi FOR VALUES FROM (' || i * 10000 || ') TO (' || (i + 1) * 10000 || ')' || ' PARTITION BY RANGE (id);';
        FOR j IN 0..1 LOOP
            EXECUTE 'CREATE TABLE IF NOT EXISTS prt_gi_p_' || i || '_p_' || j || ' PARTITION OF prt_gi_p_' || i ||
                ' FOR VALUES FROM (' || i * 10000 + j * 5000 || ') TO (' || i * 10000 + (j + 1) * 5000 || ') WITH (autovacuum_enabled=FALSE);';
        END LOOP;
    END LOOP;
END $$;
CREATE TABLE IF NOT EXISTS prt_gi_p_def PARTITION OF prt_gi DEFAULT WITH (autovacuum_enabled=FALSE);
CREATE INDEX IF NOT EXISTS global_idx_a on prt_gi USING btree (a) GLOBAL; -- not supported now
CREATE INDEX prt_id_idx ON prt_gi(id);
INSERT INTO prt_gi SELECT i, random() FROM generate_series(0, 10000) i;

-- init partitioned table without global index
CREATE TABLE IF NOT EXISTS prt_no_gi (id INT, a TEXT) PARTITION BY RANGE (id);
DO $$
BEGIN
    FOR i IN 0..1 LOOP
        EXECUTE 'CREATE TABLE IF NOT EXISTS prt_no_gi_p_' || i || 
            ' PARTITION OF prt_no_gi FOR VALUES FROM (' || i * 10000 || ') TO (' || (i + 1) * 10000 || ')' || ' PARTITION BY RANGE (id);';
        FOR j IN 0..1 LOOP
            EXECUTE 'CREATE TABLE IF NOT EXISTS prt_no_gi_p_' || i || '_p_' || j || ' PARTITION OF prt_no_gi_p_' || i || 
                ' FOR VALUES FROM (' || i * 10000 + j * 5000 || ') TO (' || i * 10000 + (j + 1) * 5000 || ') WITH (autovacuum_enabled=FALSE);';
        END LOOP;
    END LOOP;
END $$;
CREATE TABLE IF NOT EXISTS prt_no_gi_p_def PARTITION OF prt_no_gi DEFAULT WITH (autovacuum_enabled=FALSE);
CREATE INDEX prt_no_gi_id_idx ON prt_no_gi(id);
INSERT INTO prt_no_gi SELECT i, random() FROM generate_series(0, 10000) i;

-- init view
CREATE VIEW v_normal AS SELECT * FROM normal;

-- init mat view
CREATE MATERIALIZED VIEW mv_normal AS SELECT * FROM normal;

\d+ normal
\d+ prt_gi
\d+ prt_gi_p_0
\d+ prt_gi_p_0_p_0
\d+ prt_gi_p_def
\d+ prt_no_gi
\d+ prt_no_gi_p_0
\d+ prt_no_gi_p_0_p_1
\d+ prt_no_gi_p_def
\d+ v_normal
\d+ mv_normal
