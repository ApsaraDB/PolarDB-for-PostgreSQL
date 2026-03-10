\set aid random(1, 5)
BEGIN;
SET enable_seqscan = OFF;
SET enable_bitmapscan = OFF;

DECLARE c CURSOR FOR SELECT * from dccheck_hash_heap WHERE keycol = :aid;
MOVE FORWARD ALL FROM c;
MOVE BACKWARD 10000 FROM c;
MOVE BACKWARD ALL FROM c;
CLOSE c;
END;
