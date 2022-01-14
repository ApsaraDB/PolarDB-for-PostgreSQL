/*--EXPLAIN_QUERY_BEGIN*/
CREATE SCHEMA partition_subquery;
SET SEARCH_PATH=partition_subquery;

-- Given a partition table
CREATE TABLE pt1(id int) PARTITION BY RANGE (id);

CREATE TABLE pt1_1 PARTITION of pt1 default;

-- When I run a query, outermost query, and it is selecting FROM a subquery
-- And that subquery, subquery 1, contains another subquery, subquery 2
-- And the outermost query aggregates over a column from an inherited table
-- And the subquery 1 is prevented from being pulled up into a join
SELECT id FROM (
	SELECT id, sum(id) OVER() as sum_id FROM (
		SELECT id FROM pt1
	) as sq1
) as sq2 GROUP BY id;
-- Then, the query executes successfully

--start_ignore
DROP TABLE IF EXISTS pt1;
--end_ignore
