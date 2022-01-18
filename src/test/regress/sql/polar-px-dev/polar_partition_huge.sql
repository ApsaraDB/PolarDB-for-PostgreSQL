-- partition by range 
/*--EXPLAIN_QUERY_BEGIN*/
select 'partition by range' as current_query;
DROP TABLE sales cascade;
CREATE OR REPLACE FUNCTION create_huge_partitions()
RETURNS VOID AS $$
DECLARE
	partition_num   INT := 1000;
	i INT := 0;
	last_num INT := 0;
BEGIN
	EXECUTE 'CREATE TABLE sales (id int, date date, amt decimal(10,2)) PARTITION BY RANGE (id)';

	LOOP
		EXIT WHEN i = partition_num;
		i := i + 1;
		EXECUTE format('CREATE TABLE sales_part_%s PARTITION of sales for values FROM (%s) to (%s)', i - 1, last_num, last_num + 100);
		last_num := last_num + 100;
	END LOOP;

	EXECUTE format('create table sales_part_default partition of sales  DEFAULT');

	insert into sales select x, '2021-04-27', 111.1  from  generate_series(1,999999) x;
END;
$$ LANGUAGE plpgsql;

select create_huge_partitions();


\d+ sales
select count(*) from sales where id > 890 and id < 1000;
select count(*) from sales t1, sales t2 where t1.id = t2.id and t1.id > 890 and t1.id < 1000;
select count(*) from sales where id > 999100 and id < 10000000;


-- partition by list 
select 'partition by list' as current_query;
DROP TABLE sales cascade;
CREATE OR REPLACE FUNCTION create_huge_partitions()
RETURNS VOID AS $$
DECLARE
	partition_num   INT := 1000;
	i INT := 0;
BEGIN
	EXECUTE 'CREATE TABLE sales (id int, date date, amt decimal(10,2)) PARTITION BY list (id)';

	LOOP
		EXIT WHEN i = partition_num;
		i := i + 1;
		EXECUTE format('CREATE TABLE sales_part_%s PARTITION of sales for values IN (%s)', i - 1, i - 1);
	END LOOP;

	EXECUTE format('create table sales_part_default partition of sales  DEFAULT');

	insert into sales select x, '2021-04-27', 111.1  from  generate_series(1,999999) x;
END;
$$ LANGUAGE plpgsql;


select create_huge_partitions();

\d+ sales
select count(*) from sales where id > 890 and id < 1000;
select count(*) from sales t1, sales t2 where t1.id = t2.id and t1.id > 890 and t1.id < 1000;
select count(*) from sales where id > 999100 and id < 10000000;



-- partition by hash 
select 'partition by hash' as current_query;
DROP TABLE sales cascade;
CREATE OR REPLACE FUNCTION create_huge_partitions()
RETURNS VOID AS $$
DECLARE
	partition_num   INT := 1000;
	i INT := 0;
BEGIN
	EXECUTE 'CREATE TABLE sales (id int, date date, amt decimal(10,2)) PARTITION BY hash (id)';

	LOOP
		EXIT WHEN i = partition_num;
		i := i + 1;
		EXECUTE format('CREATE TABLE sales_part_%s PARTITION of sales for values WITH (modulus %s, remainder %s)', 
			i - 1, partition_num, i - 1);
	END LOOP;

	insert into sales select x, '2021-04-27', 111.1  from  generate_series(1,999999) x;
END;
$$ LANGUAGE plpgsql;


select create_huge_partitions();

\d+ sales
select count(*) from sales where id = 890;
select count(*) from sales where id in (999100, 10000000);
select count(*) from sales t1, sales t2 where t1.id = t2.id;

