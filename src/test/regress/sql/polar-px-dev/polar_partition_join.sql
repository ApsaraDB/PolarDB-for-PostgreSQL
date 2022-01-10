
--hash join hash
/*--EXPLAIN_QUERY_BEGIN*/
select t1.id, t1.value, t2.id, t2.value
from t0_hash t1
left join t0_hash t2
on t1.id=t2.id
order by t1.id, t2.id, t1.value, t2.value
limit 20;

select t1.id, t1.value, t2.id, t2.value
from t0_hash t1
join t0_hash t2
on t1.id=t2.id
order by t1.id, t2.id, t1.value, t2.value
limit 20;


select t1.id, t1.value, t2.id, t2.value
from t0_hash t1
right join t0_hash t2
on t1.id=t2.id
order by t1.id, t2.id, t1.value, t2.value
limit 20;

select t1.id, t1.value, t2.id, t2.value
from t0_hash t1
full outer join t0_hash t2
on t1.id=t2.id
order by t1.id, t2.id, t1.value, t2.value
limit 20;

--range join range
select t1.id, t1.value, t2.id, t2.value
from t1_range t1
left join t1_range t2
on t1.id=t2.id
order by t1.id, t2.id, t1.value, t2.value
limit 20;

select t1.id, t1.value, t2.id, t2.value
from t1_range t1
join t1_range t2
on t1.id=t2.id
order by t1.id, t2.id, t1.value, t2.value
limit 20;

select t1.id, t1.value, t2.id, t2.value
from t1_range t1
right join t1_range t2
on t1.id=t2.id
order by t1.id, t2.id, t1.value, t2.value
limit 20;

select t1.id, t1.value, t2.id, t2.value
from t1_range t1
full outer join t1_range t2
on t1.id=t2.id
order by t1.id, t2.id, t1.value, t2.value
limit 20;

--list join list
select t1.id, t1.value, t2.id, t2.value
from t2_list t1
left join t2_list t2
on t1.id=t2.id
order by t1.id, t2.id, t1.value, t2.value
limit 20;

select t1.id, t1.value, t2.id, t2.value
from t2_list t1
join t2_list t2
on t1.id=t2.id
order by t1.id, t2.id, t1.value, t2.value
limit 20;

select t1.id, t1.value, t2.id, t2.value
from t2_list t1
right join t2_list t2
on t1.id=t2.id
order by t1.id, t2.id, t1.value, t2.value
limit 20;

select t1.id, t1.value, t2.id, t2.value
from t2_list t1
full outer join t2_list t2
on t1.id=t2.id
order by t1.id, t2.id, t1.value, t2.value
limit 20;

--hash join range
select t1.id, t1.value, t2.id, t2.value
from t0_hash t1
left join t1_range t2
on t1.id=t2.id
order by t1.id, t2.id, t1.value, t2.value
limit 20;

select t1.id, t1.value, t2.id, t2.value
from t0_hash t1
join t1_range t2
on t1.id=t2.id
order by t1.id, t2.id, t1.value, t2.value
limit 20;

select t1.id, t1.value, t2.id, t2.value
from t0_hash t1
right join t1_range t2
on t1.id=t2.id
order by t1.id, t2.id, t1.value, t2.value
limit 20;

select t1.id, t1.value, t2.id, t2.value
from t0_hash t1
full outer join t1_range t2
on t1.id=t2.id
order by t1.id, t2.id, t1.value, t2.value
limit 20;


--hash join list
select t1.id, t1.value, t2.id, t2.value
from t0_hash t1
left join t2_list t2
on t1.id=t2.id
order by t1.id, t2.id, t1.value, t2.value
limit 20;

select t1.id, t1.value, t2.id, t2.value
from t0_hash t1
join t2_list t2
on t1.id=t2.id
order by t1.id, t2.id, t1.value, t2.value
limit 20;

select t1.id, t1.value, t2.id, t2.value
from t0_hash t1
right join t2_list t2
on t1.id=t2.id
order by t1.id, t2.id, t1.value, t2.value
limit 20;

select t1.id, t1.value, t2.id, t2.value
from t0_hash t1
full outer join t2_list t2
on t1.id=t2.id
order by t1.id, t2.id, t1.value, t2.value
limit 20;

--range join list
select t1.id, t1.value, t2.id, t2.value
from t1_range t1
left join t2_list t2
on t1.id=t2.id
order by t1.id, t2.id, t1.value, t2.value
limit 20;

select t1.id, t1.value, t2.id, t2.value
from t1_range t1
join t2_list t2
on t1.id=t2.id
order by t1.id, t2.id, t1.value, t2.value
limit 20;

select t1.id, t1.value, t2.id, t2.value
from t1_range t1
right join t2_list t2
on t1.id=t2.id
order by t1.id, t2.id, t1.value, t2.value
limit 20;

select t1.id, t1.value, t2.id, t2.value
from t1_range t1
full outer join t2_list t2
on t1.id=t2.id
order by t1.id, t2.id, t1.value, t2.value
limit 20;

--range join normal
select t1.id, t1.value, t2.id, t2.value
from t1_range t1
left join t0 t2
on t1.id=t2.id
order by t1.id, t2.id, t1.value, t2.value
limit 20;

select t1.id, t1.value, t2.id, t2.value
from t1_range t1
join t0 t2
on t1.id=t2.id
order by t1.id, t2.id, t1.value, t2.value
limit 20;

select t1.id, t1.value, t2.id, t2.value
from t1_range t1
right join t0 t2
on t1.id=t2.id
order by t1.id, t2.id, t1.value, t2.value
limit 20;

select t1.id, t1.value, t2.id, t2.value
from t1_range t1
full outer join t0 t2
on t1.id=t2.id
order by t1.id, t2.id, t1.value, t2.value
limit 20;

--list join normal
select t1.id, t1.value, t2.id, t2.value
from t2_list t1
left join t0 t2
on t1.id=t2.id
order by t1.id, t2.id, t1.value, t2.value
limit 20;

select t1.id, t1.value, t2.id, t2.value
from t2_list t1
join t0 t2
on t1.id=t2.id
order by t1.id, t2.id, t1.value, t2.value
limit 20;

select t1.id, t1.value, t2.id, t2.value
from t2_list t1
right join t0 t2
on t1.id=t2.id
order by t1.id, t2.id, t1.value, t2.value
limit 20;

select t1.id, t1.value, t2.id, t2.value
from t2_list t1
full outer join t0 t2
on t1.id=t2.id
order by t1.id, t2.id, t1.value, t2.value
limit 20;

--hash join normal
select t1.id, t1.value, t2.id, t2.value
from t0_hash t1
left join t0 t2
on t1.id=t2.id
order by t1.id, t2.id, t1.value, t2.value
limit 20;

select t1.id, t1.value, t2.id, t2.value
from t0_hash t1
join t0 t2
on t1.id=t2.id
order by t1.id, t2.id, t1.value, t2.value
limit 20;

select t1.id, t1.value, t2.id, t2.value
from t0_hash t1
right join t0 t2
on t1.id=t2.id
order by t1.id, t2.id, t1.value, t2.value
limit 20;

select t1.id, t1.value, t2.id, t2.value
from t0_hash t1
full outer join t0 t2
on t1.id=t2.id
order by t1.id, t2.id, t1.value, t2.value
limit 20;

--hash, range join list
select t1.id, t1.value, t2.id, t2.value, t3.id, t3.value
from t0_hash t1
left join t2_list t2
on t1.id=t2.id
left join t1_range t3
on t3.id=t2.id
order by t1.id, t2.id, t1.value, t2.value, t3.id, t3.value
limit 20;

select t1.id, t1.value, t2.id, t2.value, t3.id, t3.value
from t0_hash t1
 join t2_list t2
on t1.id=t2.id
 join t1_range t3
on t3.id=t2.id
order by t1.id, t2.id, t1.value, t2.value, t3.id, t3.value
limit 20;

select t1.id, t1.value, t2.id, t2.value, t3.id, t3.value
from t0_hash t1
right join t2_list t2
on t1.id=t2.id
right join t1_range t3
on t3.id=t2.id
order by t1.id, t2.id, t1.value, t2.value, t3.id, t3.value
limit 20;

select t1.id, t1.value, t2.id, t2.value, t3.id, t3.value
from t0_hash t1
full outer join t2_list t2
on t1.id=t2.id
right join t1_range t3
on t3.id=t2.id
order by t1.id, t2.id, t1.value, t2.value, t3.id, t3.value
limit 20;

--
explain (costs off)
select * from t1_range, t2_list;

explain (costs off)
select * from t0_hash, t1_range;

explain (costs off)
select * from t0_hash, t2_list;

explain (costs off)
select * from t0_hash, t1_range, t2_list;
