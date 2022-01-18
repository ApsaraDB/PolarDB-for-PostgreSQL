--
-- PX ignore function
--

show polar_enable_px;
show polar_px_ignore_function;

/*--EXPLAIN_QUERY_BEGIN*/
create sequence polar_px_testseq;
create table px_test(id int);
insert into px_test values(generate_series(1,10000));
analyze px_test;

select nextval('polar_px_testseq');

/*--POLAR_ENABLE_PX*/
set polar_enable_px = on;
reset polar_px_ignore_function;
explain select count(*) from px_test where currval('polar_px_testseq') = 1;
select count(*) from px_test where currval('polar_px_testseq') = 1;
explain select nextval('polar_px_testseq'), id from px_test order by id limit 10;
select nextval('polar_px_testseq'), id from px_test order by id limit 10;

set polar_enable_px = on;
set polar_px_ignore_function='';
explain select count(*) from px_test where currval('polar_px_testseq') = 1;
select count(*) from px_test where currval('polar_px_testseq') = 1;
explain select nextval('polar_px_testseq'), id from px_test order by id limit 10;
select nextval('polar_px_testseq'), id from px_test order by id limit 10;

/*--POLAR_DISABLE_PX*/
set polar_enable_px = off;
explain select count(*) from px_test where currval('polar_px_testseq') = 1;
select count(*) from px_test where currval('polar_px_testseq') = 1;
explain select nextval('polar_px_testseq'), id from px_test order by id limit 10;
select nextval('polar_px_testseq'), id from px_test order by id limit 10;

reset polar_px_ignore_function;
reset polar_enable_px;

set polar_px_ignore_function='12345,54221';
show polar_px_ignore_function;

set polar_px_ignore_function='12345, 54321';
show polar_px_ignore_function;

set polar_px_ignore_function='abcd';
set polar_px_ignore_function='$%^&';
set polar_px_ignore_function='12345,abcd';
set polar_px_ignore_function='12345,-54221';

reset polar_enable_px;
reset polar_px_ignore_function;
