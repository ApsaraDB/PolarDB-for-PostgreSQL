-- ----------------------------------------------------------------
-- Regression tests for disabling hash aggregation
-- ----------------------------------------------------------------

-- Since we get different results for 3 different PG version sets, add following
-- queries to specify version of the output easier.
SHOW server_version_num \gset
SELECT :'server_version_num' >= 90600 as version_above_nine_five;
SELECT :'server_version_num' >= 90500 AND :'server_version_num' < 90600 as version_nine_five;
SELECT :'server_version_num' >= 90400 AND :'server_version_num' < 90500 as version_nine_four;

SELECT hll_set_output_version(1);

CREATE TABLE tt1(id int, col_1 int, sd hll);
INSERT INTO tt1 values(1,1, hll_empty());
INSERT INTO tt1 values(1,2, hll_empty());
UPDATE tt1 SET sd = hll_add(sd, hll_hash_integer(111)) WHERE id = 1 and col_1 = 1;
UPDATE tt1 SET sd = hll_add(sd, hll_hash_integer(222)) WHERE id = 1 and col_1 = 2;

INSERT INTO tt1 values(2,1, hll_empty());
INSERT INTO tt1 values(2,2, hll_empty());
UPDATE tt1 SET sd = hll_add(sd, hll_hash_integer(333)) WHERE id = 2 and col_1 = 1;
UPDATE tt1 SET sd = hll_add(sd, hll_hash_integer(444)) WHERE id = 2 and col_1 = 2;

-- Test with hll_union_agg
SET hll.force_groupagg to OFF;
EXPLAIN(COSTS OFF)
SELECT 
	id, hll_union_agg(sd) 
FROM 
	tt1 
GROUP BY(id);

SET hll.force_groupagg to ON;
EXPLAIN(COSTS OFF)
SELECT 
	id, hll_union_agg(sd) 
FROM 
	tt1 
GROUP BY(id);

-- Test with operator over aggregate
SET hll.force_groupagg to OFF;
EXPLAIN(COSTS OFF)
SELECT 
	id, hll_union_agg(sd) || hll_union_agg(sd)
FROM 
	tt1 
GROUP BY(id);

SET hll.force_groupagg to ON;
EXPLAIN(COSTS OFF)
SELECT 
	id, hll_union_agg(sd) || hll_union_agg(sd)
FROM 
	tt1 
GROUP BY(id);

-- Test with function over aggregate
SET hll.force_groupagg to OFF;
EXPLAIN(COSTS OFF)
SELECT 
	id, hll_cardinality(hll_union_agg(sd))
FROM 
	tt1 
GROUP BY(id);

SET hll.force_groupagg to ON;
EXPLAIN(COSTS OFF)
SELECT 
	id, hll_cardinality(hll_union_agg(sd))
FROM 
	tt1 
GROUP BY(id);

-- Test with having clause
SET hll.force_groupagg to OFF;
EXPLAIN(COSTS OFF)
SELECT 
	id, hll_cardinality(hll_union_agg(sd))
FROM 
	tt1 
GROUP BY(id)
HAVING hll_cardinality(hll_union_agg(sd)) > 1;

SET hll.force_groupagg to ON;
EXPLAIN(COSTS OFF)
SELECT 
	id, hll_cardinality(hll_union_agg(sd))
FROM 
	tt1 
GROUP BY(id)
HAVING hll_cardinality(hll_union_agg(sd)) > 1;

-- Test hll_add_agg, first set work_mem to 256MB in order to use hash aggregate
-- before forcing group aggregate
SET work_mem TO '256MB';

CREATE TABLE add_test_table(c1 bigint, c2 bigint);
INSERT INTO add_test_table SELECT g, g FROM generate_series(1, 1000) AS g;

-- Test with different hll_add_agg signatures
SET hll.force_groupagg TO OFF;
EXPLAIN(COSTS OFF) 
SELECT 
	c1, hll_add_agg(hll_hash_bigint(c2)) 
FROM 
	add_test_table 
GROUP BY 1;

SET hll.force_groupagg TO ON;
EXPLAIN(COSTS OFF) 
SELECT 
	c1, hll_add_agg(hll_hash_bigint(c2)) 
FROM 
	add_test_table 
GROUP BY 1;

SET hll.force_groupagg TO OFF;
EXPLAIN(COSTS OFF) 
SELECT 
	c1, hll_add_agg(hll_hash_bigint(c2), 10) 
FROM 
	add_test_table 
GROUP BY 1;

SET hll.force_groupagg TO ON;
EXPLAIN(COSTS OFF) 
SELECT 
	c1, hll_add_agg(hll_hash_bigint(c2), 10) 
FROM 
	add_test_table 
GROUP BY 1;

SET hll.force_groupagg TO OFF;
EXPLAIN(COSTS OFF) 
SELECT 
	c1, hll_add_agg(hll_hash_bigint(c2), 10, 4) 
FROM 
	add_test_table 
GROUP BY 1;

SET hll.force_groupagg TO ON;
EXPLAIN(COSTS OFF) 
SELECT 
	c1, hll_add_agg(hll_hash_bigint(c2), 10, 4) 
FROM 
	add_test_table 
GROUP BY 1;

SET hll.force_groupagg TO OFF;
EXPLAIN(COSTS OFF) 
SELECT 
	c1, hll_add_agg(hll_hash_bigint(c2), 10, 4, 512) 
FROM 
	add_test_table 
GROUP BY 1;

SET hll.force_groupagg TO ON;
EXPLAIN(COSTS OFF) 
SELECT 
	c1, hll_add_agg(hll_hash_bigint(c2), 10, 4, 512) 
FROM 
	add_test_table 
GROUP BY 1;

SET hll.force_groupagg TO OFF;
EXPLAIN(COSTS OFF) 
SELECT 
	c1, hll_add_agg(hll_hash_bigint(c2), 10, 4, 512, 0) 
FROM 
	add_test_table 
GROUP BY 1;

SET hll.force_groupagg TO ON;
EXPLAIN(COSTS OFF) 
SELECT 
	c1, hll_add_agg(hll_hash_bigint(c2), 10, 4, 512, 0) 
FROM 
	add_test_table 
GROUP BY 1;

RESET work_mem;
DROP TABLE tt1;
DROP TABLE add_test_table;
