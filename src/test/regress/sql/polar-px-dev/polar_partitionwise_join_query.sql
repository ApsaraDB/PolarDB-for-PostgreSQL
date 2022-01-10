


-- join
SELECT t1.c1, t1.c2, t1.c3, t2.c2, t2.c3
FROM t1, t2 
WHERE t1.c1 = t2.c1 AND t1.c2 = 0;

SELECT t1.c1, t1.c2, t1.c3, t2.c2, t2.c3
FROM t1 
join t2
on t1.c1 = t2.c1 
WHERE t1.c2 = 0;

SELECT t1.c1, t1.c2, t1.c3, t2.c2, t2.c3
FROM t1 
left join t2
on t1.c1 = t2.c1 
WHERE t1.c2 = 0;

SELECT t1.c1, t1.c2, t1.c3, t2.c2, t2.c3
FROM t1 
right join t2
on t1.c1 = t2.c1 
WHERE t1.c2 = 0;

SELECT t1.c1, t1.c2, t1.c3, t2.c2, t2.c3
FROM t1 
full join t2
on t1.c1 = t2.c1 
WHERE t1.c2 = 0;

-- join with order
SELECT t1.c1, t1.c2, t1.c3, t2.c2, t2.c3
FROM t1 
join t2
on t1.c1 = t2.c1 
WHERE t1.c2 = 0
order by t1.c1 desc, t1.c2, t1.c3 desc;

SELECT t1.c1, t1.c2, t1.c3, t2.c2, t2.c3
FROM t1 
left join t2
on t1.c1 = t2.c1 
WHERE t1.c2 = 0
order by t1.c1 desc, t1.c2, t1.c3 desc;

-- join with agg
SELECT t1.c1, max(t1.c2), min(t1.c3), max(t2.c2), min(t2.c3)
FROM t1 
join t2
on t1.c1 = t2.c1 
WHERE t1.c2 = 0
group by t1.c1;

SELECT t1.c1, max(t1.c2), min(t1.c3), max(t2.c2), min(t2.c3)
FROM t1 
left join t2
on t1.c1 = t2.c1 
WHERE t1.c2 = 0
group by t1.c1;

SELECT t1.c1, max(t1.c2), min(t1.c3), max(t2.c2), min(t2.c3)
FROM t1 
join t2
on t1.c1 = t2.c1 
WHERE t1.c2 = 0
group by t1.c1
order by 1,2,3;

SELECT t1.c1, max(t1.c2), min(t1.c3), max(t2.c2), min(t2.c3)
FROM t1 
left join t2
on t1.c1 = t2.c1 
WHERE t1.c2 = 0
group by t1.c1
order by 1,2,3;

-- join with winfunc
SELECT t1.c1, 
max(t1.c2) OVER (PARTITION BY t1.c1), 
min(t1.c3) OVER (PARTITION BY t1.c1 order by t1.c1), 
max(t2.c2) OVER (PARTITION BY t1.c1 order by t1.c1 range between current row and unbounded following), 
min(t2.c3) OVER (PARTITION BY t1.c1 order by t1.c1 rows between 2 preceding and 2 following)
FROM t1 
join t2
on t1.c1 = t2.c1 
WHERE t1.c2 = 0
;

SELECT t1.c1, 
max(t1.c2) OVER (PARTITION BY t1.c1), 
min(t1.c3) OVER (PARTITION BY t1.c1 order by t1.c1), 
max(t2.c2) OVER (PARTITION BY t1.c1 order by t1.c1 range between current row and unbounded following), 
min(t2.c3) OVER (PARTITION BY t1.c1 order by t1.c1 rows between 2 preceding and 2 following)
FROM t1 
left join t2
on t1.c1 = t2.c1 
WHERE t1.c2 = 0
;

SELECT t1.c1, 
max(t1.c2) OVER (PARTITION BY t1.c1), 
min(t1.c3) OVER (PARTITION BY t1.c1 order by t1.c1), 
max(t2.c2) OVER (PARTITION BY t1.c1 order by t1.c1 range between current row and unbounded following), 
min(t2.c3) OVER (PARTITION BY t1.c1 order by t1.c1 rows between 2 preceding and 2 following)
FROM t1 
join t2
on t1.c1 = t2.c1 
WHERE t1.c2 = 0
order by 1,2,3;

SELECT t1.c1, 
max(t1.c2) OVER (PARTITION BY t1.c1), 
min(t1.c3) OVER (PARTITION BY t1.c1 order by t1.c1), 
max(t2.c2) OVER (PARTITION BY t1.c1 order by t1.c1 range between current row and unbounded following), 
min(t2.c3) OVER (PARTITION BY t1.c1 order by t1.c1 rows between 2 preceding and 2 following)
FROM t1 
left join t2
on t1.c1 = t2.c1 
WHERE t1.c2 = 0
order by 1,2,3;

-- join with cte
with cte as (
SELECT t1.c1 c01, t1.c2 c02, t1.c3 c03, t2.c2 c12, t2.c3 c13
FROM t1 
join t2
on t1.c1 = t2.c1 
WHERE t1.c2 = 0
order by t1.c1 desc, t1.c2, t1.c3 desc
)
select c01, max(c02), min(c03), max(c12), min(c13)
from cte
group by c01
order by 1, 2, 3;

with cte as (
SELECT t1.c1 c01, t1.c2 c02, t1.c3 c03, t2.c2 c12, t2.c3 c13
FROM t1 
join t2
on t1.c1 = t2.c1 
WHERE t1.c2 = 0
order by t1.c1 desc, t1.c2, t1.c3 desc
)
select *
from cte t1, cte t2
where t1.c01=t2.c01;

with cte as (
SELECT t1.c1 c01, t1.c2 c02, t1.c3 c03, t2.c2 c12, t2.c3 c13
FROM t1 
join t2
on t1.c1 = t2.c1 
WHERE t1.c2 = 0
order by t1.c1 desc, t1.c2, t1.c3 desc
)
select *
from cte t1, cte t2
where t1.c01=t2.c01
order by 1,2,3,4;

with cte as (
SELECT t1.c1 c01, t1.c2 c02, t1.c3 c03, t2.c2 c12, t2.c3 c13
FROM t1 
join t2
on t1.c1 = t2.c1 
WHERE t1.c2 = 0
order by t1.c1 desc, t1.c2, t1.c3 desc
)
select t1.c01, count(*)
from cte t1, cte t2
where t1.c01=t2.c01
group by t1.c01
order by 1,2;

-- join with subquery
SELECT t1.c1, t1.c2, t1.c3, t2.c2, t2.c3
FROM 
(select * from t1 where c1=9) t1
join 
(select * from t2 where c1=9) t2
on t1.c1 = t2.c1 
order by t1.c1 desc, t1.c2, t1.c3 desc;

SELECT t1.c1, t1.c2, t1.c3, t2.c2, t2.c3
FROM 
(select * from t1 where c1=9) t1
left join 
(select * from t2 where c1=9) t2
on t1.c1 = t2.c1 
order by t1.c1 desc, t1.c2, t1.c3 desc;


with cte as (
SELECT t1.c1 c01, t1.c2 c02, t1.c3 c03, t2.c1 c11, t2.c2 c12, t2.c3 c13
FROM 
(select * from t1 where c1=9) t1
left join 
(select * from t2 where c1=9) t2
on t1.c1 = t2.c1 
order by t1.c1 desc, t1.c2, t1.c3 desc
)
select t1.c01, count(*)
from cte t1, cte t2
where t1.c01=t2.c01
group by t1.c01
order by 1,2;


--not support case
-- join
SELECT t1.c1, t1.c2, t1.c3, t2.c2, t2.c3
FROM t1, t2 
WHERE t1.c1 - t2.c1 = 0 AND t1.c2 = 0;

SELECT t1.c1, t1.c2, t1.c3, t2.c2, t2.c3
FROM t1, t2 
WHERE t1.c1  + 1 = t2.c1 + 1 AND t1.c2 = 0;
