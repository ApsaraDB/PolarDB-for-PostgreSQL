\set aid random(1, 100 * :scale)
BEGIN;
INSERT INTO dccheck_hash_heap (keycol) SELECT a % 3 FROM generate_series (1, :aid) a;
DELETE FROM dccheck_hash_heap WHERE keycol % 2 = 1;
END;
