------------------------------------------------------------------------
--hash partition
--Partition constraint: satisfies_hash_partition('16384'::oid, 3, 1, id)

--one table
select * from  t0_hash where id =1 ORDER BY 1, 2 limit 20;
select * from  t0_hash where id =2 ORDER BY 1, 2 limit 20;
select * from  t0_hash where id !=1 ORDER BY 1, 2 limit 20;
select * from  t0_hash where id in (1, 2) ORDER BY 1, 2 limit 20;
select * from  t0_hash where id not in (1, 3) ORDER BY 1, 2 limit 20;
select * from  t0_hash where id =1 or id = 2 ORDER BY 1, 2 limit 20;
select * from  t0_hash where id !=1 or id != 2 ORDER BY 1, 2 limit 20;
select * from  t0_hash where id =1 and id = 2 ORDER BY 1, 2 limit 20;
select * from  t0_hash where id !=1 and id != 2 ORDER BY 1, 2 limit 20;
select * from  t0_hash where id is null ORDER BY 1, 2 limit 20;
select * from  t0_hash where id is not null ORDER BY 1, 2 limit 20;
select * from  t0_hash where id > 1 and id < 3 ORDER BY 1, 2 limit 20;
select * from  t0_hash where id > 1 and id < 5 ORDER BY 1, 2 limit 20;
select * from  t0_hash where id > 5 ORDER BY 1, 2 limit 20;

------------------------------------------------------------------------
--range partition
--Partition constraint: ((x IS NOT NULL) AND (x >= 10) AND (x < 20))

--one table
select * from  t1_range where id =1 ORDER BY 1, 2 limit 20;
select * from  t1_range where id =2 ORDER BY 1, 2 limit 20;
select * from  t1_range where id !=1 ORDER BY 1, 2 limit 20;
select * from  t1_range where id in (1, 2) ORDER BY 1, 2 limit 20;
select * from  t1_range where id not in (1, 3) ORDER BY 1, 2 limit 20;
select * from  t1_range where id =1 or id = 2 ORDER BY 1, 2 limit 20;
select * from  t1_range where id !=1 or id != 2 ORDER BY 1, 2 limit 20;
select * from  t1_range where id =1 and id = 2 ORDER BY 1, 2 limit 20;
select * from  t1_range where id !=1 and id != 2 ORDER BY 1, 2 limit 20;
select * from  t1_range where id is null ORDER BY 1, 2 limit 20;
select * from  t1_range where id is not null ORDER BY 1, 2 limit 20;
select * from  t1_range where id > 1 and id < 3 ORDER BY 1, 2 limit 20;
select * from  t1_range where id > 1 and id < 5 ORDER BY 1, 2 limit 20;
select * from  t1_range where id > 5 ORDER BY 1, 2 limit 20;


------------------------------------------------------------------------
--list
--Partition constraint: ((b IS NOT NULL) AND (b = ANY (ARRAY[1, 3])))

--one table
select * from  t2_list where id =1 ORDER BY 1, 2 limit 20;
select * from  t2_list where id =2 ORDER BY 1, 2 limit 20;
select * from  t2_list where id !=1 ORDER BY 1, 2 limit 20;
select * from  t2_list where id in (1, 2) ORDER BY 1, 2 limit 20;
select * from  t2_list where id not in (1, 3) ORDER BY 1, 2 limit 20;
select * from  t2_list where id =1 or id = 2 ORDER BY 1, 2 limit 20;
select * from  t2_list where id !=1 or id != 2 ORDER BY 1, 2 limit 20;
select * from  t2_list where id =1 and id = 2 ORDER BY 1, 2 limit 20;
select * from  t2_list where id !=1 and id != 2 ORDER BY 1, 2 limit 20;
select * from  t2_list where id is null ORDER BY 1, 2 limit 20;
select * from  t2_list where id is not null ORDER BY 1, 2 limit 20;
select * from  t2_list where id > 1 and id < 3 ORDER BY 1, 2 limit 20;
select * from  t2_list where id > 1 and id < 5 ORDER BY 1, 2 limit 20;
select * from  t2_list where id > 5 ORDER BY 1, 2 limit 20;
