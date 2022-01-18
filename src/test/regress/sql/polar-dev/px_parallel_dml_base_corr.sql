
-- Insert
insert into px_parallel_dml_corr_t1 select generate_series(1,60),generate_series(1,60);
select count(*) from px_parallel_dml_corr_t1;
select count(*) from px_parallel_dml_corr_t2;
insert into px_parallel_dml_corr_t1 select c2, c1 from px_parallel_dml_corr_t1;
insert into px_parallel_dml_corr_t2 select c2, c1 from px_parallel_dml_corr_t1;
select count(*) from px_parallel_dml_corr_t1;
select count(*) from px_parallel_dml_corr_t2;
insert into px_parallel_dml_corr_t1 select c2, c1 from px_parallel_dml_corr_t2 where c1 = '1';
insert into px_parallel_dml_corr_t2 select c2, c1 from px_parallel_dml_corr_t1 where c1 = '1';
select count(*) from px_parallel_dml_corr_t1;
select count(*) from px_parallel_dml_corr_t2;

-- Not Use PX Insert
insert into px_parallel_dml_corr_t1 VALUES(100,100);
insert into px_parallel_dml_corr_t2 VALUES(200,200);
insert into px_parallel_dml_corr_t1 select generate_series(101,150),generate_series(101,150);
insert into px_parallel_dml_corr_t2 select generate_series(101,150),generate_series(101,150);
insert into px_parallel_dml_corr_t1 select 1,2;
insert into px_parallel_dml_corr_t2 select 3,4;
insert into px_parallel_dml_corr_t1 select generate_series(201,250), c2 from px_parallel_dml_corr_t2;
insert into px_parallel_dml_corr_t1 select generate_series(301,350),generate_series(301,350) union select * from px_parallel_dml_corr_t3;

-- Use PX Insert
insert into px_parallel_dml_corr_t1 select * from px_parallel_dml_corr_t2;




--Insert into normal table select * from partition table
insert into px_parallel_dml_corr_t0_insert select px_parallel_dml_corr_t0_hash.id,px_parallel_dml_corr_t1_range.value from px_parallel_dml_corr_t0_hash,px_parallel_dml_corr_t1_range where px_parallel_dml_corr_t0_hash.id = px_parallel_dml_corr_t1_range.id and px_parallel_dml_corr_t1_range.id < 2000;
insert into px_parallel_dml_corr_t0_insert select * from px_parallel_dml_corr_t0_hash where id < 2000;
insert into px_parallel_dml_corr_t0_insert select px_parallel_dml_corr_t0_hash.id,px_parallel_dml_corr_t1_range.value from px_parallel_dml_corr_t0_hash,px_parallel_dml_corr_t1_range where px_parallel_dml_corr_t0_hash.id = px_parallel_dml_corr_t1_range.id and px_parallel_dml_corr_t1_range.id > 28000;
insert into px_parallel_dml_corr_t0_insert select px_parallel_dml_corr_t0_hash.id,px_parallel_dml_corr_t1_range.value from px_parallel_dml_corr_t0_hash,px_parallel_dml_corr_t1_range where px_parallel_dml_corr_t0_hash.id = px_parallel_dml_corr_t1_range.id and px_parallel_dml_corr_t1_range.id > 28000;
insert into px_parallel_dml_corr_t0_insert select * from  px_parallel_dml_corr_t1_range where id > 29000;
insert into px_parallel_dml_corr_t0_insert select * from px_parallel_dml_corr_t2_list where job='student';
insert into px_parallel_dml_corr_t0_insert select 100000,pvalue from px_parallel_dml_corr_t2_list where job='student';
insert into px_parallel_dml_corr_t0_insert select id, px_parallel_dml_corr_t2_list.pvalue from px_parallel_dml_corr_t2_list,px_parallel_dml_corr_t0_hash where px_parallel_dml_corr_t2_list.job='student' and px_parallel_dml_corr_t0_hash.id < 5000 LIMIT 5000;

select count(*)from px_parallel_dml_corr_t0_insert;

-- Open the px_enable_insert_partition_table
set polar_px_enable_insert_partition_table='on';
--Insert into partition table select * from normal table
insert into px_parallel_dml_corr_t0_hash select * from px_parallel_dml_corr_t0_select_table order by px_parallel_dml_corr_t0_select_table.id limit 1000;
insert into px_parallel_dml_corr_t1_range select * from px_parallel_dml_corr_t0_select_table order by px_parallel_dml_corr_t0_select_table.id limit 1000;
-- With Index
insert into px_parallel_dml_corr_t1_range_index select * from px_parallel_dml_corr_t0_select_table order by px_parallel_dml_corr_t0_select_table.id limit 1000;
insert into px_parallel_dml_corr_t2_list select 'student',px_parallel_dml_corr_t0_select_table.id from px_parallel_dml_corr_t0_select_table order by px_parallel_dml_corr_t0_select_table.id LIMIT 1000;
insert into px_parallel_dml_corr_t2_list select * from px_parallel_dml_corr_t0_select_table order by px_parallel_dml_corr_t0_select_table.id limit 1000;

select count(*) from px_parallel_dml_corr_t0_hash;
select count(*) from px_parallel_dml_corr_t1_range;
select count(*) from px_parallel_dml_corr_t2_list;


--Insert into partition table select * from partition table
insert into px_parallel_dml_corr_t0_hash select * from px_parallel_dml_corr_t1_range where id < 2000;
insert into px_parallel_dml_corr_t0_hash select * from px_parallel_dml_corr_t2_list where job='teachar' and pvalue < 12000;
insert into px_parallel_dml_corr_t0_hash select 100000,pvalue from px_parallel_dml_corr_t2_list where job='teachar' and pvalue < 12000;

insert into px_parallel_dml_corr_t1_range select * from px_parallel_dml_corr_t0_hash where id > 28000;
insert into px_parallel_dml_corr_t1_range select * from px_parallel_dml_corr_t2_list where job='teachar' and pvalue < 12000;
insert into px_parallel_dml_corr_t1_range select 100000,pvalue from px_parallel_dml_corr_t2_list where job='teachar' and pvalue < 12000;

insert into px_parallel_dml_corr_t2_list select 'student',px_parallel_dml_corr_t0_hash.id from px_parallel_dml_corr_t0_hash where id > 28000;
insert into px_parallel_dml_corr_t2_list select * from px_parallel_dml_corr_t0_hash where id > 28000;
insert into px_parallel_dml_corr_t2_list select * from px_parallel_dml_corr_t1_range where id < 2000;

select count(*) from px_parallel_dml_corr_t0_hash;
select count(*) from px_parallel_dml_corr_t1_range;
select count(*) from px_parallel_dml_corr_t2_list;

-- Close the px_enable_insert_partition_table
set polar_px_enable_insert_partition_table='off';
--Insert into partition table select * from normal table
insert into px_parallel_dml_corr_t0_hash select * from px_parallel_dml_corr_t0_select_table order by px_parallel_dml_corr_t0_select_table.id limit 1000;
insert into px_parallel_dml_corr_t1_range select * from px_parallel_dml_corr_t0_select_table order by px_parallel_dml_corr_t0_select_table.id limit 1000;
-- With Index
insert into px_parallel_dml_corr_t1_range_index select * from px_parallel_dml_corr_t0_select_table order by px_parallel_dml_corr_t0_select_table.id limit 1000;
insert into px_parallel_dml_corr_t2_list select 'student',px_parallel_dml_corr_t0_select_table.id from px_parallel_dml_corr_t0_select_table order by px_parallel_dml_corr_t0_select_table.id limit 1000;
insert into px_parallel_dml_corr_t2_list select * from px_parallel_dml_corr_t0_select_table order by px_parallel_dml_corr_t0_select_table.id limit 1000;


select count(*) from px_parallel_dml_corr_t0_hash;
select count(*) from px_parallel_dml_corr_t1_range;
select count(*) from px_parallel_dml_corr_t2_list;

--Insert into partition table select * from partition table
insert into px_parallel_dml_corr_t0_hash select * from px_parallel_dml_corr_t1_range where id < 2000;
insert into px_parallel_dml_corr_t0_hash select * from px_parallel_dml_corr_t2_list where job='teachar' and pvalue < 12000;
insert into px_parallel_dml_corr_t0_hash select 100000,pvalue from px_parallel_dml_corr_t2_list where job='teachar' and pvalue < 12000;

insert into px_parallel_dml_corr_t1_range select * from px_parallel_dml_corr_t0_hash where id > 28000;
insert into px_parallel_dml_corr_t1_range select * from px_parallel_dml_corr_t2_list where job='teachar' and pvalue < 12000;
insert into px_parallel_dml_corr_t1_range select 100000,pvalue from px_parallel_dml_corr_t2_list where job='teachar' and pvalue < 12000;

insert into px_parallel_dml_corr_t2_list select * from 'student',px_parallel_dml_corr_t0_hash.id where id > 28000;
insert into px_parallel_dml_corr_t2_list select * from px_parallel_dml_corr_t0_hash where id > 28000;
insert into px_parallel_dml_corr_t2_list select * from px_parallel_dml_corr_t1_range where id < 2000;

select count(*) from px_parallel_dml_corr_t0_hash;
select count(*) from px_parallel_dml_corr_t1_range;
select count(*) from px_parallel_dml_corr_t2_list;


------------------------------------------------------------------------
--Insert into ... select from index
insert into px_parallel_dml_corr_t3 select * from px_parallel_dml_corr_t4 where c1 < 500;
insert into px_parallel_dml_corr_t3 select * from px_parallel_dml_corr_t4 where c2 < 500;

select count(*) from px_parallel_dml_corr_t3;

------------------------------------------------------------------------
-- Select Join
-- Innser join
insert into px_parallel_dml_corr_t1 select c1,px_parallel_dml_corr_t3.c2 from px_parallel_dml_corr_t2 LEFT OUTER JOIN px_parallel_dml_corr_t3 using(c1);
insert into px_parallel_dml_corr_t1 select c1,px_parallel_dml_corr_t3.c2 from px_parallel_dml_corr_t2 RIGHT OUTER JOIN px_parallel_dml_corr_t3 using(c1);
insert into px_parallel_dml_corr_t1 select c1,px_parallel_dml_corr_t3.c2 from px_parallel_dml_corr_t2 FULL OUTER JOIN px_parallel_dml_corr_t3 using(c1);
insert into px_parallel_dml_corr_t1 select px_parallel_dml_corr_t2.c1,px_parallel_dml_corr_t3.c2 from px_parallel_dml_corr_t2 CROSS JOIN px_parallel_dml_corr_t3 LIMIT 5000;
insert into px_parallel_dml_corr_t1 select c1,px_parallel_dml_corr_t3.c2 from px_parallel_dml_corr_t2 NATURAL JOIN px_parallel_dml_corr_t3;

select count(*) from px_parallel_dml_corr_t1;

------------------------------------------------------------------------
-- Group BY
insert into px_parallel_dml_corr_t1 select * from px_parallel_dml_corr_t2 group by c1,c2;

select count(*) from px_parallel_dml_corr_t1;


----------------------------------------------------------------------------
---- Insert into table with default num
insert into px_parallel_dml_corr_t8 select c1 from px_parallel_dml_corr_t9;
select * from px_parallel_dml_corr_t8 ORDER BY c1 ASC;

