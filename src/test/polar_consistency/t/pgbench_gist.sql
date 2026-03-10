\set mid random(1, 100)
BEGIN;

INSERT INTO dccheck_gist SELECT x, 2*x, 3*x, box(point(x,x+1),point(2*x,2*x+1)) FROM generate_series(:mid, :mid + 20) AS x;
DELETE FROM dccheck_gist WHERE c1 % 6 = 0;

END;

