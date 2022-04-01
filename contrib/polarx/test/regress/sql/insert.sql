--
-- insert with DEFAULT in the target_list
--
create table inserttest (col1 int4, col2 int4 NOT NULL, col3 text default 'testing');
insert into inserttest (col1, col2, col3) values (DEFAULT, DEFAULT, DEFAULT);
insert into inserttest (col2, col3) values (3, DEFAULT);
insert into inserttest (col1, col2, col3) values (DEFAULT, 5, DEFAULT);
insert into inserttest values (DEFAULT, 5, 'test');
insert into inserttest values (DEFAULT, 7);

select * from inserttest;

--
-- insert with similar expression / target_list values (all fail)
--
insert into inserttest (col1, col2, col3) values (DEFAULT, DEFAULT);
insert into inserttest (col1, col2, col3) values (1, 2);
insert into inserttest (col1) values (1, 2);
insert into inserttest (col1) values (DEFAULT, DEFAULT);

select * from inserttest;

--
-- VALUES test
--
insert into inserttest values(10, 20, '40'), (-1, 2, DEFAULT),
    ((select 2), (select i from (values(3)) as foo (i)), 'values are fun!');

select * from inserttest order by 1,2,3;

--
-- TOASTed value test
--
insert into inserttest values(30, 50, repeat('x', 10000));

select col1, col2, char_length(col3) from inserttest order by 1,2,3;

drop table inserttest;

--
-- check indirection (field/array assignment), cf bug #14265
--
-- these tests are aware that transformInsertStmt has 3 separate code paths
--

create type insert_test_type as (if1 int, if2 text[]);

create table inserttest (f1 int, f2 int[],
                         f3 insert_test_type, f4 insert_test_type[]);

insert into inserttest (f2[1], f2[2]) values (1,2);
insert into inserttest (f2[1], f2[2]) values (3,4), (5,6);
insert into inserttest (f2[1], f2[2]) select 7,8;
insert into inserttest (f2[1], f2[2]) values (1,default);  -- not supported

insert into inserttest (f3.if1, f3.if2) values (1,array['foo']);
insert into inserttest (f3.if1, f3.if2) values (1,'{foo}'), (2,'{bar}');
insert into inserttest (f3.if1, f3.if2) select 3, '{baz,quux}';
insert into inserttest (f3.if1, f3.if2) values (1,default);  -- not supported

insert into inserttest (f3.if2[1], f3.if2[2]) values ('foo', 'bar');
insert into inserttest (f3.if2[1], f3.if2[2]) values ('foo', 'bar'), ('baz', 'quux');
insert into inserttest (f3.if2[1], f3.if2[2]) select 'bear', 'beer';

insert into inserttest (f4[1].if2[1], f4[1].if2[2]) values ('foo', 'bar');
insert into inserttest (f4[1].if2[1], f4[1].if2[2]) values ('foo', 'bar'), ('baz', 'quux');
insert into inserttest (f4[1].if2[1], f4[1].if2[2]) select 'bear', 'beer';

select * from inserttest;

-- also check reverse-listing
create table inserttest2 (f1 bigint, f2 text);
create rule irule1 as on insert to inserttest2 do also
  insert into inserttest (f3.if2[1], f3.if2[2])
  values (new.f1,new.f2);
create rule irule2 as on insert to inserttest2 do also
  insert into inserttest (f4[1].if1, f4[1].if2[2])
  values (1,'fool'),(new.f1,new.f2);
create rule irule3 as on insert to inserttest2 do also
  insert into inserttest (f4[1].if1, f4[1].if2[2])
  select new.f1, new.f2;
\d+ inserttest2

drop table inserttest2;
drop table inserttest;
drop type insert_test_type;

-- direct partition inserts should check partition bound constraint
create table range_parted (
	a text,
	b int
) partition by range (a, (b+0));
create table part1 partition of range_parted for values from ('a', 1) to ('a', 10);
create table part2 partition of range_parted for values from ('a', 10) to ('a', 20);
create table part3 partition of range_parted for values from ('b', 1) to ('b', 10);
create table part4 partition of range_parted for values from ('b', 10) to ('b', 20);

-- fail
insert into part1 values ('a', 11);
insert into part1 values ('b', 1);
-- ok
insert into part1 values ('a', 1);
-- fail
insert into part4 values ('b', 21);
insert into part4 values ('a', 10);
-- ok
insert into part4 values ('b', 10);

-- fail (partition key a has a NOT NULL constraint)
insert into part1 values (null);
-- fail (expression key (b+0) cannot be null either)
insert into part1 values (1);

create table list_parted (
	a text,
	b int
) partition by list (lower(a));
create table part_aa_bb partition of list_parted FOR VALUES IN ('aa', 'bb');
create table part_cc_dd partition of list_parted FOR VALUES IN ('cc', 'dd');
create table part_null partition of list_parted FOR VALUES IN (null);

-- fail
insert into part_aa_bb values ('cc', 1);
insert into part_aa_bb values ('AAa', 1);
insert into part_aa_bb values (null);
-- ok
insert into part_cc_dd values ('cC', 1);
insert into part_null values (null, 0);

-- check in case of multi-level partitioned table
create table part_ee_ff partition of list_parted for values in ('ee', 'ff') partition by range (b);
create table part_ee_ff1 partition of part_ee_ff for values from (1) to (10);
create table part_ee_ff2 partition of part_ee_ff for values from (10) to (20);

-- fail
insert into part_ee_ff1 values ('EE', 11);
-- fail (even the parent's, ie, part_ee_ff's partition constraint applies)
insert into part_ee_ff1 values ('cc', 1);
-- ok
insert into part_ee_ff1 values ('ff', 1);
insert into part_ee_ff2 values ('ff', 11);

-- Check tuple routing for partitioned tables

-- fail
insert into range_parted values ('a', 0);
-- ok
insert into range_parted values ('a', 1);
insert into range_parted values ('a', 10);
-- fail
insert into range_parted values ('a', 20);
-- ok
insert into range_parted values ('b', 1);
insert into range_parted values ('b', 10);
-- fail (partition key (b+0) is null)
insert into range_parted values ('a');
select tableoid::regclass, * from range_parted order by 1, 2, 3;

-- ok
insert into list_parted values (null, 1);
insert into list_parted (a) values ('aA');
-- fail (partition of part_ee_ff not found in both cases)
insert into list_parted values ('EE', 0);
insert into part_ee_ff values ('EE', 0);
-- ok
insert into list_parted values ('EE', 1);
insert into part_ee_ff values ('EE', 10);
select tableoid::regclass, * from list_parted order by tableoid;

-- some more tests to exercise tuple-routing with multi-level partitioning
create table part_gg partition of list_parted for values in ('gg') partition by range (b);
create table part_gg1 partition of part_gg for values from (minvalue) to (1);
create table part_gg2 partition of part_gg for values from (1) to (10) partition by range (b);
create table part_gg2_1 partition of part_gg2 for values from (1) to (5);
create table part_gg2_2 partition of part_gg2 for values from (5) to (10);

create table part_ee_ff3 partition of part_ee_ff for values from (20) to (30) partition by range (b);
create table part_ee_ff3_1 partition of part_ee_ff3 for values from (20) to (25);
create table part_ee_ff3_2 partition of part_ee_ff3 for values from (25) to (30);

truncate list_parted;
insert into list_parted values ('aa'), ('cc');
insert into list_parted select 'Ff', s.a from generate_series(1, 29) s(a);
insert into list_parted select 'gg', s.a from generate_series(1, 9) s(a);
insert into list_parted (b) values (1);
select tableoid::regclass::text, a, min(b) as min_b, max(b) as max_b from list_parted group by 1, 2 order by 1;

-- cleanup
drop table range_parted, list_parted;

-- more tests for certain multi-level partitioning scenarios
create table mlparted (a int, b int) partition by range (a, b);
create table mlparted1 (a int not null, b int not null) partition by range ((b+0));
create table mlparted11 (like mlparted1);
-- attnum for key attribute 'a' is different in mlparted, mlparted1, and mlparted11
select attrelid::regclass, attname, attnum
from pg_attribute
where attname = 'a'
 and (attrelid = 'mlparted'::regclass
   or attrelid = 'mlparted1'::regclass
   or attrelid = 'mlparted11'::regclass)
order by attrelid::regclass::text;

alter table mlparted1 attach partition mlparted11 for values from (2) to (5);
alter table mlparted attach partition mlparted1 for values from (1, 2) to (1, 10);

-- check that "(1, 2)" is correctly routed to mlparted11.
insert into mlparted values (1, 2);
select tableoid::regclass, * from mlparted;

-- check that proper message is shown after failure to route through mlparted1
insert into mlparted (a, b) values (1, 5);

truncate mlparted;
alter table mlparted add constraint check_b check (b = 3);

-- have a BR trigger modify the row such that the check_b is violated
create function mlparted11_trig_fn()
returns trigger AS
$$
begin
  NEW.b := 4;
  return NEW;
end;
$$
language plpgsql;
create trigger mlparted11_trig before insert ON mlparted11
  for each row execute procedure mlparted11_trig_fn();

-- check that the correct row is shown when constraint check_b fails after
-- "(1, 2)" is routed to mlparted11 (actually "(1, 4)" would be shown due
-- to the BR trigger mlparted11_trig_fn)
-- XXX since trigger are not supported in XL, "(1, 2)" would be shown
insert into mlparted values (1, 2);
drop trigger mlparted11_trig on mlparted11;
drop function mlparted11_trig_fn();

-- check that inserting into an internal partition successfully results in
-- checking its partition constraint before inserting into the leaf partition
-- selected by tuple-routing
insert into mlparted1 (a, b) values (2, 3);

-- check routing error through a list partitioned table when the key is null
create table lparted_nonullpart (a int, b char) partition by list (b);
create table lparted_nonullpart_a partition of lparted_nonullpart for values in ('a');
insert into lparted_nonullpart values (1);
drop table lparted_nonullpart;

-- check that RETURNING works correctly with tuple-routing
alter table mlparted drop constraint check_b;
create table mlparted12 partition of mlparted1 for values from (5) to (10);
create table mlparted2 (a int not null, b int not null);
alter table mlparted attach partition mlparted2 for values from (1, 10) to (1, 20);
create table mlparted3 partition of mlparted for values from (1, 20) to (1, 30);
create table mlparted4 (like mlparted);
-- alter table mlparted4 drop a;
-- alter table mlparted4 add a int not null;
alter table mlparted attach partition mlparted4 for values from (1, 30) to (1, 40);
insert into mlparted (b, a) select s.a, 1 from generate_series(2, 39) s(a) returning tableoid::regclass, *;

alter table mlparted add c text;
create table mlparted5 (a int not null, b int not null, c text) partition by list (c);
create table mlparted5a (a int not null, b int not null, c text);
alter table mlparted5 attach partition mlparted5a for values in ('a');
alter table mlparted attach partition mlparted5 for values from (1, 40) to (1, 50);
alter table mlparted add constraint check_b check (a = 1 and b < 45);
insert into mlparted values (1, 45, 'a');
create function mlparted5abrtrig_func() returns trigger as $$ begin new.c = 'b'; return new; end; $$ language plpgsql;
create trigger mlparted5abrtrig before insert on mlparted5a for each row execute procedure mlparted5abrtrig_func();
insert into mlparted5 (a, b, c) values (1, 40, 'a');
drop table mlparted5;
\d+ mlparted
drop table mlparted;
\d+ mlparted

-- check that message shown after failure to find a partition shows the
-- appropriate key description (or none) in various situations
create table key_desc (a int, b int) partition by list ((a+0));
create table key_desc_1 partition of key_desc for values in (1) partition by range (b);

create user regress_insert_other_user;
grant select (a) on key_desc_1 to regress_insert_other_user;
grant insert on key_desc to regress_insert_other_user;

set role regress_insert_other_user;
-- no key description is shown
insert into key_desc values (1, 1);

reset role;
grant select (b) on key_desc_1 to regress_insert_other_user;
set role regress_insert_other_user;
-- key description (b)=(1) is now shown
insert into key_desc values (1, 1);

-- key description is not shown if key contains expression
insert into key_desc values (2, 1);
reset role;
revoke all on key_desc from regress_insert_other_user;
revoke all on key_desc_1 from regress_insert_other_user;
drop role regress_insert_other_user;
drop table key_desc, key_desc_1;

-- test minvalue/maxvalue restrictions
create table mcrparted (a int, b int, c int) partition by range (a, abs(b), c);
create table mcrparted0 partition of mcrparted for values from (minvalue, 0, 0) to (1, maxvalue, maxvalue);
create table mcrparted2 partition of mcrparted for values from (10, 6, minvalue) to (10, maxvalue, minvalue);
create table mcrparted4 partition of mcrparted for values from (21, minvalue, 0) to (30, 20, minvalue);

-- check multi-column range partitioning expression enforces the same
-- constraint as what tuple-routing would determine it to be
create table mcrparted0 partition of mcrparted for values from (minvalue, minvalue, minvalue) to (1, maxvalue, maxvalue);
create table mcrparted1 partition of mcrparted for values from (2, 1, minvalue) to (10, 5, 10);
create table mcrparted2 partition of mcrparted for values from (10, 6, minvalue) to (10, maxvalue, maxvalue);
create table mcrparted3 partition of mcrparted for values from (11, 1, 1) to (20, 10, 10);
create table mcrparted4 partition of mcrparted for values from (21, minvalue, minvalue) to (30, 20, maxvalue);
create table mcrparted5 partition of mcrparted for values from (30, 21, 20) to (maxvalue, maxvalue, maxvalue);

-- routed to mcrparted0
insert into mcrparted values (0, 1, 1);
insert into mcrparted0 values (0, 1, 1);

-- routed to mcparted1
insert into mcrparted values (9, 1000, 1);
insert into mcrparted1 values (9, 1000, 1);
insert into mcrparted values (10, 5, -1);
insert into mcrparted1 values (10, 5, -1);
insert into mcrparted values (2, 1, 0);
insert into mcrparted1 values (2, 1, 0);

-- routed to mcparted2
insert into mcrparted values (10, 6, 1000);
insert into mcrparted2 values (10, 6, 1000);
insert into mcrparted values (10, 1000, 1000);
insert into mcrparted2 values (10, 1000, 1000);

-- no partition exists, nor does mcrparted3 accept it
insert into mcrparted values (11, 1, -1);
insert into mcrparted3 values (11, 1, -1);

-- routed to mcrparted5
insert into mcrparted values (30, 21, 20);
insert into mcrparted5 values (30, 21, 20);
insert into mcrparted4 values (30, 21, 20);	-- error

-- check rows
select tableoid::regclass::text, * from mcrparted order by 1, 2, 3, 4;

-- cleanup
drop table mcrparted;

-- check that a BR constraint can't make partition contain violating rows
create table brtrigpartcon (a int, b text) partition by list (a);
create table brtrigpartcon1 partition of brtrigpartcon for values in (1);
create or replace function brtrigpartcon1trigf() returns trigger as $$begin new.a := 2; return new; end$$ language plpgsql;
create trigger brtrigpartcon1trig before insert on brtrigpartcon1 for each row execute procedure brtrigpartcon1trigf();
insert into brtrigpartcon values (1, 'hi there');
insert into brtrigpartcon1 values (1, 'hi there');

-- check that the message shows the appropriate column description in a
-- situation where the partitioned table is not the primary ModifyTable node
create table inserttest3 (f1 text default 'foo', f2 text default 'bar', f3 int);
create role regress_coldesc_role;
grant insert on inserttest3 to regress_coldesc_role;
grant insert on brtrigpartcon to regress_coldesc_role;
revoke select on brtrigpartcon from regress_coldesc_role;
set role regress_coldesc_role;
with result as (insert into brtrigpartcon values (1, 'hi there') returning 1)
  insert into inserttest3 (f3) select * from result;
reset role;

-- cleanup
revoke all on inserttest3 from regress_coldesc_role;
revoke all on brtrigpartcon from regress_coldesc_role;
drop role regress_coldesc_role;
drop table inserttest3;
drop table brtrigpartcon;
drop function brtrigpartcon1trigf();

-- check that "do nothing" BR triggers work with tuple-routing (this checks
-- that estate->es_result_relation_info is appropriately set/reset for each
-- routed tuple)
create table donothingbrtrig_test (a int, b text) partition by list (a) with(dist_type=hash, dist_col=a);
create table donothingbrtrig_test1 (a int, b text) with(dist_type=hash, dist_col=a);
create table donothingbrtrig_test2 (a int, b text, c text) with(dist_type=hash, dist_col=a);
alter table donothingbrtrig_test2 drop column c;
create or replace function donothingbrtrig_func() returns trigger as $$begin raise notice 'b: %', new.b; return NULL; end$$ language plpgsql;
create trigger donothingbrtrig1 before insert on donothingbrtrig_test1 for each row execute procedure donothingbrtrig_func();
create trigger donothingbrtrig2 before insert on donothingbrtrig_test2 for each row execute procedure donothingbrtrig_func();
alter table donothingbrtrig_test attach partition donothingbrtrig_test1 for values in (1);
alter table donothingbrtrig_test attach partition donothingbrtrig_test2 for values in (2);
insert into donothingbrtrig_test values (1, 'foo'), (2, 'bar');
copy donothingbrtrig_test from stdout;
1	baz
2	qux
\.
select tableoid::regclass, * from donothingbrtrig_test;

-- cleanup
drop table donothingbrtrig_test;
drop function donothingbrtrig_func();

-- check multi-column range partitioning with minvalue/maxvalue constraints
create table mcrparted (a text, b int) partition by range(a, b);
create table mcrparted1_lt_b partition of mcrparted for values from (minvalue, minvalue) to ('b', minvalue);
create table mcrparted2_b partition of mcrparted for values from ('b', minvalue) to ('c', minvalue);
create table mcrparted3_c_to_common partition of mcrparted for values from ('c', minvalue) to ('common', minvalue);
create table mcrparted4_common_lt_0 partition of mcrparted for values from ('common', minvalue) to ('common', 0);
create table mcrparted5_common_0_to_10 partition of mcrparted for values from ('common', 0) to ('common', 10);
create table mcrparted6_common_ge_10 partition of mcrparted for values from ('common', 10) to ('common', maxvalue);
create table mcrparted7_gt_common_lt_d partition of mcrparted for values from ('common', maxvalue) to ('d', minvalue);
create table mcrparted8_ge_d partition of mcrparted for values from ('d', minvalue) to (maxvalue, maxvalue);

\d+ mcrparted
\d+ mcrparted1_lt_b
\d+ mcrparted2_b
\d+ mcrparted3_c_to_common
\d+ mcrparted4_common_lt_0
\d+ mcrparted5_common_0_to_10
\d+ mcrparted6_common_ge_10
\d+ mcrparted7_gt_common_lt_d
\d+ mcrparted8_ge_d

insert into mcrparted values ('aaa', 0), ('b', 0), ('bz', 10), ('c', -10),
    ('comm', -10), ('common', -10), ('common', 0), ('common', 10),
    ('commons', 0), ('d', -10), ('e', 0);
select tableoid::regclass, * from mcrparted order by a, b;
drop table mcrparted;

-- check that wholerow vars in the RETURNING list work with partitioned tables
create table returningwrtest (a int) partition by list (a);
create table returningwrtest1 partition of returningwrtest for values in (1);
insert into returningwrtest values (1) returning returningwrtest;

-- check also that the wholerow vars in RETURNING list are converted as needed
alter table returningwrtest add b text;
create table returningwrtest2 (a int, b text, c int);
alter table returningwrtest2 drop c;
alter table returningwrtest attach partition returningwrtest2 for values in (2);
insert into returningwrtest values (2, 'foo') returning returningwrtest;
drop table returningwrtest;
