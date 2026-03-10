\set mid random(1, 200)
BEGIN;
SET enable_seqscan = OFF;
SET enable_bitmapscan = OFF;

SELECT count(*) FROM dccheck_gist where c4 <@ box(point(:mid, :mid), point(:mid + 40, :mid + 40));
END;

