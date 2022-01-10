/*--EXPLAIN_QUERY_BEGIN*/

drop table if exists d;
drop table if exists c;
drop table if exists b;
drop table if exists a;

-- basic list partition

create table ggg (a char(1), b char(2), d char(3))
partition by LIST (b);

CREATE TABLE ggg_aa PARTITION OF ggg FOR VALUES IN ('a','b','c','d');
CREATE TABLE ggg_bb PARTITION OF ggg FOR VALUES IN ('e','f','g');

insert into ggg values ('x', 'a');
insert into ggg values ('x', 'b');
insert into ggg values ('x', 'c');
insert into ggg values ('x', 'd');
insert into ggg values ('x', 'e');
insert into ggg values ('x', 'f');
insert into ggg values ('x', 'g');
insert into ggg values ('x', 'a');
insert into ggg values ('x', 'b');
insert into ggg values ('x', 'c');
insert into ggg values ('x', 'd');
insert into ggg values ('x', 'e');
insert into ggg values ('x', 'f');
insert into ggg values ('x', 'g');

set polar_px_optimizer_multilevel_partitioning=1;

select * from ggg order by 1, 2;

-- ok
select * from ggg_aa order by 1, 2;
select * from ggg_bb order by 1, 2;

drop table ggg cascade;

-- documentation example - partition by list and range
CREATE TABLE rank (id int, rank int, year date, gender
char(1)) 
partition by list (gender);

CREATE TABLE rank_m PARTITION OF rank FOR VALUES IN ('M') PARTITION BY RANGE (year);
CREATE TABLE rank_f PARTITION OF rank FOR VALUES IN ('F') PARTITION BY RANGE (year);

CREATE TABLE rank_m_year_1 PARTITION OF rank_m FOR VALUES from ('2001-01-01') TO ('2002-01-01') ;
CREATE TABLE rank_m_year_2 PARTITION OF rank_m FOR VALUES from ('2002-01-01') TO ('2003-01-01') ;
CREATE TABLE rank_m_year_3 PARTITION OF rank_m FOR VALUES from ('2003-01-01') TO ('2004-01-01') ;
CREATE TABLE rank_m_year_4 PARTITION OF rank_m FOR VALUES from ('2004-01-01') TO ('2005-01-01') ;
CREATE TABLE rank_m_year_default PARTITION OF rank_m DEFAULT;

CREATE TABLE rank_f_year_1 PARTITION OF rank_f FOR VALUES from ('2001-01-01') TO ('2002-01-01') ;
CREATE TABLE rank_f_year_2 PARTITION OF rank_f FOR VALUES from ('2002-01-01') TO ('2003-01-01') ;
CREATE TABLE rank_f_year_3 PARTITION OF rank_f FOR VALUES from ('2003-01-01') TO ('2004-01-01') ;
CREATE TABLE rank_f_year_4 PARTITION OF rank_f FOR VALUES from ('2004-01-01') TO ('2005-01-01') ;
CREATE TABLE rank_f_year_default PARTITION OF rank_f DEFAULT;

insert into rank values (1, 1, date '2001-01-15', 'M');
insert into rank values (2, 1, date '2002-02-15', 'M');
insert into rank values (3, 1, date '2003-03-15', 'M');
insert into rank values (4, 1, date '2004-04-15', 'M');
insert into rank values (5, 1, date '2005-05-15', 'M');
insert into rank values (6, 1, date '2001-01-15', 'F');
insert into rank values (7, 1, date '2002-02-15', 'F');
insert into rank values (8, 1, date '2003-03-15', 'F');
insert into rank values (9, 1, date '2004-04-15', 'F');
insert into rank values (10, 1, date '2005-05-15', 'F');

select * from rank order by 1, 2, 3, 4;
select * from rank_m order by 1, 2, 3, 4;
select * from rank_f order by 1, 2, 3, 4;
select * from rank_m_year_1 order by 1, 2, 3, 4;
select * from rank_f_year_1 order by 1, 2, 3, 4;

drop table rank cascade;


-- range list combo
create table ggg (a char(1), b date, d char(3), e numeric)
partition by range (b);
CREATE TABLE gg_1 PARTITION OF ggg FOR VALUES from ('2007-01-01') TO ('2008-01-01') PARTITION BY LIST (d);
CREATE TABLE gg_2 PARTITION OF ggg FOR VALUES from ('2008-01-01') TO ('2009-01-01') PARTITION BY LIST (d);
CREATE TABLE gg_3 PARTITION OF ggg default PARTITION BY LIST (d);

CREATE TABLE gg_1 PARTITION OF ggg FOR VALUES from ('2007-01-01') TO ('2008-01-01') PARTITION BY LIST (d);
CREATE TABLE gg_2 PARTITION OF ggg FOR VALUES from ('2008-01-01') TO ('2009-01-01') PARTITION BY LIST (d);
CREATE TABLE gg_3 PARTITION OF ggg default PARTITION BY LIST (d);

CREATE TABLE gg_1_1 PARTITION OF gg_1 FOR VALUES IN ('1','2','3') ;
CREATE TABLE gg_1_2 PARTITION OF gg_1 FOR VALUES IN ('4','5','6') ;
CREATE TABLE gg_1_3 PARTITION OF gg_1 default;

CREATE TABLE gg_2_1 PARTITION OF gg_2 FOR VALUES IN ('1','2','3') ;
CREATE TABLE gg_2_2 PARTITION OF gg_2 FOR VALUES IN ('4','5','6') ;
CREATE TABLE gg_2_3 PARTITION OF gg_2 default;

CREATE TABLE gg_3_1 PARTITION OF gg_3 FOR VALUES IN ('1','2','3') ;
CREATE TABLE gg_3_2 PARTITION OF gg_3 FOR VALUES IN ('4','5','6') ;
CREATE TABLE gg_3_3 PARTITION OF gg_3 default;

drop table ggg cascade;


-- demo ends here


-- LIST validation

-- duplicate partition name
CREATE TABLE rank (id int, rank int, year date, gender
char(1))
partition by list (gender);

CREATE TABLE rank_1 PARTITION OF rank FOR VALUES IN ('M') ;
CREATE TABLE rank_2 PARTITION OF rank FOR VALUES IN ('a') ;
CREATE TABLE rank_3 PARTITION OF rank FOR VALUES IN ('b') ;
CREATE TABLE rank_4 PARTITION OF rank FOR VALUES IN ('c') ;
CREATE TABLE rank_5 PARTITION OF rank FOR VALUES IN ('d') ;
CREATE TABLE rank_6 PARTITION OF rank FOR VALUES IN ('e') ;
CREATE TABLE rank_1 PARTITION OF rank FOR VALUES IN ('M') ;

drop table rank cascade;

-- EVERY

--  the documentation example, rewritten with EVERY in a template
CREATE TABLE rank (id int,
rank int, year date, gender char(1))
partition by list (gender);

CREATE TABLE rank_m PARTITION OF rank FOR VALUES IN ('M') PARTITION BY RANGE (year);
CREATE TABLE rank_f PARTITION OF rank FOR VALUES IN ('F') PARTITION BY RANGE (year);

CREATE TABLE rank_m_year_1 PARTITION OF rank_m FOR VALUES from ('2001-01-01') TO ('2002-01-01') ;
CREATE TABLE rank_m_year_2 PARTITION OF rank_m FOR VALUES from ('2002-01-01') TO ('2003-01-01') ;
CREATE TABLE rank_m_year_3 PARTITION OF rank_m FOR VALUES from ('2003-01-01') TO ('2004-01-01') ;
CREATE TABLE rank_m_year_4 PARTITION OF rank_m FOR VALUES from ('2004-01-01') TO ('2005-01-01') ;
CREATE TABLE rank_m_year_5 PARTITION OF rank_m FOR VALUES from ('2005-01-01') TO ('2006-01-01') ;
CREATE TABLE rank_m_year_default PARTITION OF rank_m DEFAULT;

CREATE TABLE rank_f_year_1 PARTITION OF rank_f FOR VALUES from ('2001-01-01') TO ('2002-01-01') ;
CREATE TABLE rank_f_year_2 PARTITION OF rank_f FOR VALUES from ('2002-01-01') TO ('2003-01-01') ;
CREATE TABLE rank_f_year_3 PARTITION OF rank_f FOR VALUES from ('2003-01-01') TO ('2004-01-01') ;
CREATE TABLE rank_f_year_4 PARTITION OF rank_f FOR VALUES from ('2004-01-01') TO ('2005-01-01') ;
CREATE TABLE rank_f_year_5 PARTITION OF rank_f FOR VALUES from ('2005-01-01') TO ('2006-01-01') ;
CREATE TABLE rank_f_year_default PARTITION OF rank_f DEFAULT;

insert into rank values (1, 1, date '2001-01-15', 'M');
insert into rank values (2, 1, date '2002-02-15', 'M');
insert into rank values (3, 1, date '2003-03-15', 'M');
insert into rank values (4, 1, date '2004-04-15', 'M');
insert into rank values (5, 1, date '2005-05-15', 'M');
insert into rank values (6, 1, date '2001-01-15', 'F');
insert into rank values (7, 1, date '2002-02-15', 'F');
insert into rank values (8, 1, date '2003-03-15', 'F');
insert into rank values (9, 1, date '2004-04-15', 'F');
insert into rank values (10, 1, date '2005-05-15', 'F');

select * from rank order by 1, 2, 3, 4;
select * from rank_m order by 1, 2, 3, 4;
select * from rank_f order by 1, 2, 3, 4;
select * from rank_m_year_1 order by 1, 2, 3, 4;
select * from rank_f_year_1 order by 1, 2, 3, 4;

drop table rank cascade;

-- copy test
create table foz (i int, d date) distributed by (i)
partition by range (d) (start (date '2001-01-01') end (date '2005-01-01')
every(interval '1 year'));

create table foz (i int, d date)
partition by range (d);

CREATE TABLE foz_1 PARTITION OF foz FOR VALUES from ('2001-01-01') TO ('2002-01-01');
CREATE TABLE foz_2 PARTITION OF foz FOR VALUES from ('2002-01-01') TO ('2003-01-01');
CREATE TABLE foz_3 PARTITION OF foz FOR VALUES from ('2003-01-01') TO ('2004-01-01');
CREATE TABLE foz_4 PARTITION OF foz FOR VALUES from ('2004-01-01') TO ('2005-01-01');
CREATE TABLE foz_5 PARTITION OF foz default;

COPY foz FROM stdin DELIMITER '|';
1|2001-01-2
2|2001-10-10
3|2002-10-30
4|2003-01-01
5|2004-05-05
\.
select * from foz_1;
select * from foz_2;
select * from foz_3;
select * from foz_4;

-- Check behaviour of key for which there is no partition
COPY foz FROM stdin DELIMITER '|';
6|2010-01-01
\.
drop table foz cascade;

--range
-- Reject NULL values
create table d (i int,  j int) partition by range(j);
CREATE TABLE d_a PARTITION OF d FOR VALUES from (1) TO (10);
CREATE TABLE d_b PARTITION OF d FOR VALUES from (11) TO (20);

insert into d values (1, 1);
insert into d values (1, 2);
insert into d values (1, NULL);
drop table  d cascade;

-- allow NULLs into the default partition
create table d (i int,  j int) partition by range(j);
CREATE TABLE d_a PARTITION OF d FOR VALUES from (1) TO (10);
CREATE TABLE d_b PARTITION OF d FOR VALUES from (11) TO (20);
CREATE TABLE d_c PARTITION OF d default;

insert into d values (1, 1);
insert into d values (1, 2);
insert into d values (1, NULL);
select * from d_c;
drop table  d cascade;

-- SETUP
-- start_ignore
drop table if exists s1;
drop table if exists s2;

-- setup two partitioned tables s1 and s2

create table s1 (d1 int, p1 int)
partition by list (p1);
CREATE TABLE s1_1 PARTITION OF s1 FOR VALUES IN (0);
CREATE TABLE s1_2 PARTITION OF s1 FOR VALUES IN (1);


create table s2 (d2 int, p2 int)
partition by list (d2);
CREATE TABLE s2_1 PARTITION OF s2 FOR VALUES IN (0);
CREATE TABLE s2_2 PARTITION OF s2 FOR VALUES IN (1);


-- VERIFY
-- expect GPOPT fall back to Postgres query optimizer
-- since GPOPT don't support partition elimination through full outer joins
select * from s1 full outer join s2 on s1.d1 = s2.d2 and s1.p1 = s2.p2 where s1.p1 = 1;

-- CLEANUP
-- start_ignore
drop table if exists s1;
drop table if exists s2;
-- end_ignore

create table mpp10847_pkeyconstraints(
  pkid serial,
  option1 int,
  option2 int,
  option3 int,
  primary key(pkid, option3))
partition by range (option3);

CREATE TABLE mpp10847_pkeyconstraints_1 PARTITION OF mpp10847_pkeyconstraints FOR VALUES from (1) TO (100);
CREATE TABLE mpp10847_pkeyconstraints_2 PARTITION OF mpp10847_pkeyconstraints FOR VALUES from (100) TO (200);
CREATE TABLE mpp10847_pkeyconstraints_3 PARTITION OF mpp10847_pkeyconstraints FOR VALUES from (200) TO (300);

insert into mpp10847_pkeyconstraints values (10000, 50, 50, 102);
-- This is supposed to fail as you're not supposed to be able to use the same
-- primary key in the same table. But GPDB cannot currently enforce that.
insert into mpp10847_pkeyconstraints values (10000, 50, 50, 5);

select * from mpp10847_pkeyconstraints;

drop table mpp10847_pkeyconstraints;


-- Test that ADD/EXCHANGE/SPLIT PARTITION works, even when there are partial or expression
-- indexes on the table. (MPP-13750)
create table dcl_messaging_test
(
        message_create_date     timestamp(3) not null,
        trace_socket            varchar(1024) null,
        trace_count             varchar(1024) null,
        variable_10             varchar(1024) null,
        variable_11             varchar(1024) null,
        variable_12             varchar(1024) null,
        variable_13             varchar(1024) default('-1'),
        variable_14             varchar(1024) null,
        variable_15             varchar(1024) null,
        variable_16             varchar(1024) null,
        variable_17             varchar(1024) null,
        variable_18             varchar(1024) null,
        variable_19             varchar(1024) null,
        variable_20             varchar(1024) null
)
partition by range (message_create_date);

CREATE TABLE dcl_messaging_test_1 PARTITION OF dcl_messaging_test FOR VALUES from ('2011-09-01') TO ('2011-09-02') ;
CREATE TABLE dcl_messaging_test_2 PARTITION OF dcl_messaging_test FOR VALUES from ('2011-09-02') TO ('2011-09-03') ;
CREATE TABLE dcl_messaging_test_3 PARTITION OF dcl_messaging_test FOR VALUES from ('2011-09-03') TO ('2011-09-04') ;
CREATE TABLE dcl_messaging_test_4 PARTITION OF dcl_messaging_test FOR VALUES from ('2011-09-04') TO ('2011-09-05') ;
CREATE TABLE dcl_messaging_test_5 PARTITION OF dcl_messaging_test FOR VALUES from ('2011-09-05') TO ('2011-09-06') ;
CREATE TABLE dcl_messaging_test_6 PARTITION OF dcl_messaging_test FOR VALUES from ('2011-09-06') TO ('2011-09-07') ;
CREATE TABLE dcl_messaging_test_7 PARTITION OF dcl_messaging_test FOR VALUES from ('2011-09-07') TO ('2011-09-08') ;
CREATE TABLE dcl_messaging_test_8 PARTITION OF dcl_messaging_test FOR VALUES from ('2011-09-08') TO ('2011-09-09') ;
CREATE TABLE dcl_messaging_test_9 PARTITION OF dcl_messaging_test FOR VALUES from ('2011-09-09') TO ('2011-09-10') ;
CREATE TABLE dcl_messaging_test_default PARTITION OF dcl_messaging_test default ;

-- partial index
create index dcl_messaging_test_index13 on dcl_messaging_test(variable_13) where message_create_date > '2011-09-02';
-- expression index
create index dcl_messaging_test_index16 on dcl_messaging_test(upper(variable_16));


--
-- Test handling of NULL values in SPLIT PARTITION.
--
CREATE TABLE mpp7863 (id int, dat char(8))
PARTITION BY RANGE (dat);
CREATE TABLE mpp7863_1 PARTITION OF mpp7863 FOR VALUES from (200910) TO (200911);
CREATE TABLE mpp7863_2 PARTITION OF mpp7863 FOR VALUES from (200911) TO (200912);
CREATE TABLE mpp7863_3 PARTITION OF mpp7863 FOR VALUES from (200912) TO (201001);
CREATE TABLE mpp7863_4 PARTITION OF mpp7863 default;

insert into mpp7863 values(generate_series(1, 100),'200910');
insert into mpp7863 values(generate_series(101, 200),'200911');
insert into mpp7863 values(generate_series(201, 300),'200912');
insert into mpp7863 values(generate_series(301, 30300),'');
insert into mpp7863 (id) values(generate_series(30301, 60300));
insert into mpp7863 values(generate_series(60301, 60400),'201001');

select count(*) from mpp7863_4;
select count(*) from mpp7863_4 where dat is null;
select count(*) from mpp7863_4 where dat ='';
select count(*) from mpp7863;
set polar_px_optimizer_multilevel_partitioning=0;