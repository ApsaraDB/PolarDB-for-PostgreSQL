select pg_switch_wal();

CREATE TABLE hash_split_heap (keycol INT);
INSERT INTO hash_split_heap SELECT a FROM generate_series(1, 500) a;
CREATE INDEX hash_split_index on hash_split_heap USING HASH (keycol);
INSERT INTO hash_split_heap SELECT a FROM generate_series(1, 5000) a;

SELECT count(*) FROM hash_split_heap WHERE keycol < 1000;
SELECT * FROM hash_split_heap WHERE keycol < 1000 limit 3;

DROP TABLE hash_split_heap;

