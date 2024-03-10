/*--EXPLAIN_QUERY_BEGIN*/
create schema bfv_aggregate;
set search_path=bfv_aggregate;

--
-- Window function with outer references in PARTITION BY/ORDER BY clause
--

-- SETUP
create table x_outer (a int, b int, c int);
create table y_inner (d int, e int);
insert into x_outer select i%3, i, i from generate_series(1,10) i;
insert into y_inner select i%3, i from generate_series(1,10) i;
analyze x_outer;
analyze y_inner;

-- TEST
select * from x_outer where a in (select row_number() over(partition by a) from y_inner) order by 1, 2;

select * from x_outer where a in (select rank() over(order by a) from y_inner) order by 1, 2;

select * from x_outer where a not in (select rank() over(order by a) from y_inner) order by 1, 2;

select * from x_outer where exists (select rank() over(order by a) from y_inner where d = a) order by 1, 2;

select * from x_outer where not exists (select rank() over(order by a) from y_inner where d = a) order by 1, 2;

select * from x_outer where a in (select last_value(d) over(partition by b order by e rows between e preceding and e+1 following) from y_inner) order by 1, 2;

--
-- Testing aggregation in a query
--

-- SETUP
create table d (col1 timestamp, col2 int);
insert into d select to_date('2014-01-01', 'YYYY-DD-MM'), generate_series(1,100);

-- TEST
select 1, to_char(col1, 'YYYY'), median(col2) from d group by 1, 2;

--
-- Testing if aggregate derived window function produces incorrect results
--

-- SETUP
create table toy(id,val) as select i,i from generate_series(1,5) i;
create aggregate mysum1(int4) (sfunc = int4_sum, combinefunc=int8pl, stype=bigint);
create aggregate mysum2(int4) (sfunc = int4_sum, stype=bigint);

-- TEST
select
   id, val,
   sum(val) over (w),
   mysum1(val) over (w),
   mysum2(val) over (w)
from toy
window w as (order by id rows 2 preceding);

--
-- Error executing for aggregate with anyarray as return type
--

-- SETUP
CREATE OR REPLACE FUNCTION tfp(anyarray,anyelement) RETURNS anyarray AS
'select $1 || $2' LANGUAGE SQL;

CREATE OR REPLACE FUNCTION ffp(anyarray) RETURNS anyarray AS
'select $1' LANGUAGE SQL;

CREATE AGGREGATE myaggp20a(BASETYPE = anyelement, SFUNC = tfp,
  STYPE = anyarray, FINALFUNC = ffp, INITCOND = '{}');

-- Adding a sql function to sory the array
CREATE OR REPLACE FUNCTION array_sort (ANYARRAY)
RETURNS ANYARRAY LANGUAGE SQL
AS $$
SELECT ARRAY(SELECT unnest($1) ORDER BY 1)
$$;

create temp table t(f1 int, f2 int[], f3 text);

-- TEST
insert into t values(1,array[1],'a');
insert into t values(1,array[11],'b');
insert into t values(1,array[111],'c');
insert into t values(2,array[2],'a');
insert into t values(2,array[22],'b');
insert into t values(2,array[222],'c');
insert into t values(3,array[3],'a');
insert into t values(3,array[3],'b');

select f3, array_sort(myaggp20a(f1)) from t group by f3 order by f3;

-- start_ignore
create language plpython3u;
-- end_ignore
create or replace function count_operator(query text, operator text) returns int as
$$
rv = plpy.execute('EXPLAIN ' + query)
search_text = operator
result = 0
for i in range(len(rv)):
    cur_line = rv[i]['QUERY PLAN']
    if search_text.lower() in cur_line.lower():
        result = result+1
return result
$$
language plpython3u;

--
-- Testing adding a traceflag to favor multi-stage aggregation
--

-- SETUP
create table multi_stage_test(a int, b int);
insert into multi_stage_test select i, i%4 from generate_series(1,10) i;
analyze multi_stage_test;

-- TEST
set polar_px_optimizer_segments=2;
set polar_px_optimizer_force_multistage_agg = on;
select count_operator('select count(*) from multi_stage_test group by b;','GroupAggregate');

set polar_px_optimizer_force_multistage_agg = off;
select count_operator('select count(*) from multi_stage_test group by b;','GroupAggregate');

--CLEANUP
reset polar_px_optimizer_segments;
reset polar_px_optimizer_force_multistage_agg;

--
-- Testing not picking HashAgg for aggregates without combine functions
--
-- POLARDB_12_MERGE_FIXME: The reason that we tested that a HashAggregate is
-- not chosen when an aggregate is missing combine functions is that in
-- GPDB Hybrid Hash Agg, a HashAggregate can spill to disk, and it requires
-- the combine function for that. But that got reverted with the v12 merge.
-- Currently, this does choose a Hash Agg, and as long as we don't do the
-- spilling like we used to, that's OK. But if we resurrect the Hybrid Hash
-- Agg spilling, then this test becomes relevant again.

-- SETUP
set polar_px_optimizer_print_missing_stats = off;
CREATE TABLE attribute_table (product_id integer, attribute_id integer,attribute text, attribute2 text,attribute_ref_lists text,short_name text,attribute6 text,attribute5 text,measure double precision,unit character varying(60)) ;
-- create the transition function
CREATE OR REPLACE FUNCTION do_concat(text,text)
RETURNS text
--concatenates 2 strings
AS 'SELECT CASE WHEN $1 IS NULL THEN $2
WHEN $2 IS NULL THEN $1
ELSE $1 || $2 END;'
     LANGUAGE SQL
     IMMUTABLE
     RETURNS NULL ON NULL INPUT;
-- UDA definition. No COMBINEFUNC exists
CREATE AGGREGATE concat(text) (
   --text/string concatenation
   SFUNC = do_concat, --Function to call for each string that builds the aggregate
   STYPE = text,--FINALFUNC=final_func, --Function to call after everything has been aggregated
   INITCOND = '' --Initialize as an empty string when starting
);

-- TEST
-- cook some stats
set allow_system_table_mods=true;
UPDATE pg_class set reltuples=524592::real, relpages=2708::integer where oid = 'attribute_table'::regclass;
select count_operator('select product_id,concat(E''#attribute_''||attribute_id::varchar||E'':''||attribute) as attr FROM attribute_table GROUP BY product_id;','HashAggregate');

-- CLEANUP

--
-- Testing fallback to planner when the agg used in window does not have
-- a combine function.
--

-- SETUP
create table foo(a int, b text) ;

-- TEST
insert into foo values (1,'aaa'), (2,'bbb'), (3,'ccc');
-- should fall back
select string_agg(b, '') over (partition by a) from foo order by 1;
select string_agg(b, '') over (partition by a,b) from foo order by 1;
-- should not fall back
select max(b) over (partition by a) from foo order by 1;
select count_operator('select max(b) over (partition by a) from foo order by 1;', 'Pivotal Optimizer (GPORCA)');
-- fall back
select string_agg(b, '') over (partition by a+1) from foo order by 1;
select string_agg(b || 'txt', '') over (partition by a) from foo order by 1;
select string_agg(b || 'txt', '') over (partition by a+1) from foo order by 1;
-- fall back
select string_agg(b, '') over (partition by a order by a) from foo order by 1;
select string_agg(b || 'txt', '') over (partition by a,b order by a,b) from foo order by 1;
select '1' || string_agg(b, '') over (partition by a+1 order by a+1) from foo;


-- Test for a bug in memtuple compute_null_save() function, where the result value
-- was a 16-bit integer, which overflowed with a tuple with enough NULL attributes.
-- That bug wasn't related to aggregates per se, but this is one way to construct
-- such a memtuple, and seems useful to test a query that has a huge number of
-- aggregates anyway.
create temporary table mtup1(
c0 text,
c1 text,
c3 int,
c4 int
)
;

insert into mtup1 values
  ('foo', '2015-09-1.1', 1),
  ('foo', '2015-09-1.1', 2),
  ('foo', '2015-09-1.1', 3),
  ('foo', '2015-09-1.1', 4),
  ('foo', '2015-09-1.1', 5),
  ('foo', '2015-09-1.1', 6),
  ('foo', '2015-09-1.1', 7),
  ('foo', '2015-09-1.1', 8),
  ('foo', '2015-09-1.1', 9);

-- The number of SUMs is a chosen so that it it's enough to trigger the bug with NULL
-- attributes, but not much more than that. There's some O(n^2) code in ExecInitAgg
-- to detect duplicate AggRefs, so this starts to get really slow as you add more
-- aggregates.

-- POLARDB_12_MERGE_FIXME: we use MinimalTuples in Motions now, and a MinimalTuple
-- has a limit of 1600 columns. With the default plan, you now get an error from
-- exceeding that limit. Are we OK with that limitation? Does the single-phase
-- plan exercise the original bug?
set gp_enable_multiphase_agg=off;


select c0, c1, array_length(ARRAY[
 SUM(c4 % 2), SUM(c4 % 3), SUM(c4 % 4),
 SUM(c4 % 5), SUM(c4 % 6), SUM(c4 % 7), SUM(c4 % 8), SUM(c4 % 9),
 SUM(c4 % 10), SUM(c4 % 11), SUM(c4 % 12), SUM(c4 % 13), SUM(c4 % 14),
 SUM(c4 % 15), SUM(c4 % 16), SUM(c4 % 17), SUM(c4 % 18), SUM(c4 % 19),
 SUM(c4 % 20), SUM(c4 % 21), SUM(c4 % 22), SUM(c4 % 23), SUM(c4 % 24),
 SUM(c4 % 25), SUM(c4 % 26), SUM(c4 % 27), SUM(c4 % 28), SUM(c4 % 29),
 SUM(c4 % 30), SUM(c4 % 31), SUM(c4 % 32), SUM(c4 % 33), SUM(c4 % 34),
 SUM(c4 % 35), SUM(c4 % 36), SUM(c4 % 37), SUM(c4 % 38), SUM(c4 % 39),
 SUM(c4 % 40), SUM(c4 % 41), SUM(c4 % 42), SUM(c4 % 43), SUM(c4 % 44),
 SUM(c4 % 45), SUM(c4 % 46), SUM(c4 % 47), SUM(c4 % 48), SUM(c4 % 49),
 SUM(c4 % 50), SUM(c4 % 51), SUM(c4 % 52), SUM(c4 % 53), SUM(c4 % 54),
 SUM(c4 % 55), SUM(c4 % 56), SUM(c4 % 57), SUM(c4 % 58), SUM(c4 % 59),
 SUM(c4 % 60), SUM(c4 % 61), SUM(c4 % 62), SUM(c4 % 63), SUM(c4 % 64),
 SUM(c4 % 65), SUM(c4 % 66), SUM(c4 % 67), SUM(c4 % 68), SUM(c4 % 69),
 SUM(c4 % 70), SUM(c4 % 71), SUM(c4 % 72), SUM(c4 % 73), SUM(c4 % 74),
 SUM(c4 % 75), SUM(c4 % 76), SUM(c4 % 77), SUM(c4 % 78), SUM(c4 % 79),
 SUM(c4 % 80), SUM(c4 % 81), SUM(c4 % 82), SUM(c4 % 83), SUM(c4 % 84),
 SUM(c4 % 85), SUM(c4 % 86), SUM(c4 % 87), SUM(c4 % 88), SUM(c4 % 89),
 SUM(c4 % 90), SUM(c4 % 91), SUM(c4 % 92), SUM(c4 % 93), SUM(c4 % 94),
 SUM(c4 % 95), SUM(c4 % 96), SUM(c4 % 97), SUM(c4 % 98), SUM(c4 % 99),
 SUM(c4 % 100), SUM(c4 % 101), SUM(c4 % 102), SUM(c4 % 103), SUM(c4 % 104),
 SUM(c4 % 105), SUM(c4 % 106), SUM(c4 % 107), SUM(c4 % 108), SUM(c4 % 109),
 SUM(c4 % 110), SUM(c4 % 111), SUM(c4 % 112), SUM(c4 % 113), SUM(c4 % 114),
 SUM(c4 % 115), SUM(c4 % 116), SUM(c4 % 117), SUM(c4 % 118), SUM(c4 % 119),
 SUM(c4 % 120), SUM(c4 % 121), SUM(c4 % 122), SUM(c4 % 123), SUM(c4 % 124),
 SUM(c4 % 125), SUM(c4 % 126), SUM(c4 % 127), SUM(c4 % 128), SUM(c4 % 129),
 SUM(c4 % 130), SUM(c4 % 131), SUM(c4 % 132), SUM(c4 % 133), SUM(c4 % 134),
 SUM(c4 % 135), SUM(c4 % 136), SUM(c4 % 137), SUM(c4 % 138), SUM(c4 % 139),
 SUM(c4 % 140), SUM(c4 % 141), SUM(c4 % 142), SUM(c4 % 143), SUM(c4 % 144),
 SUM(c4 % 145), SUM(c4 % 146), SUM(c4 % 147), SUM(c4 % 148), SUM(c4 % 149),
 SUM(c4 % 150), SUM(c4 % 151), SUM(c4 % 152), SUM(c4 % 153), SUM(c4 % 154),
 SUM(c4 % 155), SUM(c4 % 156), SUM(c4 % 157), SUM(c4 % 158), SUM(c4 % 159),
 SUM(c4 % 160), SUM(c4 % 161), SUM(c4 % 162), SUM(c4 % 163), SUM(c4 % 164),
 SUM(c4 % 165), SUM(c4 % 166), SUM(c4 % 167), SUM(c4 % 168), SUM(c4 % 169),
 SUM(c4 % 170), SUM(c4 % 171), SUM(c4 % 172), SUM(c4 % 173), SUM(c4 % 174),
 SUM(c4 % 175), SUM(c4 % 176), SUM(c4 % 177), SUM(c4 % 178), SUM(c4 % 179),
 SUM(c4 % 180), SUM(c4 % 181), SUM(c4 % 182), SUM(c4 % 183), SUM(c4 % 184),
 SUM(c4 % 185), SUM(c4 % 186), SUM(c4 % 187), SUM(c4 % 188), SUM(c4 % 189),
 SUM(c4 % 190), SUM(c4 % 191), SUM(c4 % 192), SUM(c4 % 193), SUM(c4 % 194),
 SUM(c4 % 195), SUM(c4 % 196), SUM(c4 % 197), SUM(c4 % 198), SUM(c4 % 199),
 SUM(c4 % 200), SUM(c4 % 201), SUM(c4 % 202), SUM(c4 % 203), SUM(c4 % 204),
 SUM(c4 % 205), SUM(c4 % 206), SUM(c4 % 207), SUM(c4 % 208), SUM(c4 % 209),
 SUM(c4 % 210), SUM(c4 % 211), SUM(c4 % 212), SUM(c4 % 213), SUM(c4 % 214),
 SUM(c4 % 215), SUM(c4 % 216), SUM(c4 % 217), SUM(c4 % 218), SUM(c4 % 219),
 SUM(c4 % 220), SUM(c4 % 221), SUM(c4 % 222), SUM(c4 % 223), SUM(c4 % 224),
 SUM(c4 % 225), SUM(c4 % 226), SUM(c4 % 227), SUM(c4 % 228), SUM(c4 % 229),
 SUM(c4 % 230), SUM(c4 % 231), SUM(c4 % 232), SUM(c4 % 233), SUM(c4 % 234),
 SUM(c4 % 235), SUM(c4 % 236), SUM(c4 % 237), SUM(c4 % 238), SUM(c4 % 239),
 SUM(c4 % 240), SUM(c4 % 241), SUM(c4 % 242), SUM(c4 % 243), SUM(c4 % 244),
 SUM(c4 % 245), SUM(c4 % 246), SUM(c4 % 247), SUM(c4 % 248), SUM(c4 % 249),
 SUM(c4 % 250), SUM(c4 % 251), SUM(c4 % 252), SUM(c4 % 253), SUM(c4 % 254),
 SUM(c4 % 255), SUM(c4 % 256), SUM(c4 % 257), SUM(c4 % 258), SUM(c4 % 259),
 SUM(c4 % 260), SUM(c4 % 261), SUM(c4 % 262), SUM(c4 % 263), SUM(c4 % 264),
 SUM(c4 % 265), SUM(c4 % 266), SUM(c4 % 267), SUM(c4 % 268), SUM(c4 % 269),
 SUM(c4 % 270), SUM(c4 % 271), SUM(c4 % 272), SUM(c4 % 273), SUM(c4 % 274),
 SUM(c4 % 275), SUM(c4 % 276), SUM(c4 % 277), SUM(c4 % 278), SUM(c4 % 279),
 SUM(c4 % 280), SUM(c4 % 281), SUM(c4 % 282), SUM(c4 % 283), SUM(c4 % 284),
 SUM(c4 % 285), SUM(c4 % 286), SUM(c4 % 287), SUM(c4 % 288), SUM(c4 % 289),
 SUM(c4 % 290), SUM(c4 % 291), SUM(c4 % 292), SUM(c4 % 293), SUM(c4 % 294),
 SUM(c4 % 295), SUM(c4 % 296), SUM(c4 % 297), SUM(c4 % 298), SUM(c4 % 299),
 SUM(c4 % 300), SUM(c4 % 301), SUM(c4 % 302), SUM(c4 % 303), SUM(c4 % 304),
 SUM(c4 % 305), SUM(c4 % 306), SUM(c4 % 307), SUM(c4 % 308), SUM(c4 % 309),
 SUM(c4 % 310), SUM(c4 % 311), SUM(c4 % 312), SUM(c4 % 313), SUM(c4 % 314),
 SUM(c4 % 315), SUM(c4 % 316), SUM(c4 % 317), SUM(c4 % 318), SUM(c4 % 319),
 SUM(c4 % 320), SUM(c4 % 321), SUM(c4 % 322), SUM(c4 % 323), SUM(c4 % 324),
 SUM(c4 % 325), SUM(c4 % 326), SUM(c4 % 327), SUM(c4 % 328), SUM(c4 % 329),
 SUM(c4 % 330), SUM(c4 % 331), SUM(c4 % 332), SUM(c4 % 333), SUM(c4 % 334),
 SUM(c4 % 335), SUM(c4 % 336), SUM(c4 % 337), SUM(c4 % 338), SUM(c4 % 339),
 SUM(c4 % 340), SUM(c4 % 341), SUM(c4 % 342), SUM(c4 % 343), SUM(c4 % 344),
 SUM(c4 % 345), SUM(c4 % 346), SUM(c4 % 347), SUM(c4 % 348), SUM(c4 % 349),
 SUM(c4 % 350), SUM(c4 % 351), SUM(c4 % 352), SUM(c4 % 353), SUM(c4 % 354),
 SUM(c4 % 355), SUM(c4 % 356), SUM(c4 % 357), SUM(c4 % 358), SUM(c4 % 359),
 SUM(c4 % 360), SUM(c4 % 361), SUM(c4 % 362), SUM(c4 % 363), SUM(c4 % 364),
 SUM(c4 % 365), SUM(c4 % 366), SUM(c4 % 367), SUM(c4 % 368), SUM(c4 % 369),
 SUM(c4 % 370), SUM(c4 % 371), SUM(c4 % 372), SUM(c4 % 373), SUM(c4 % 374),
 SUM(c4 % 375), SUM(c4 % 376), SUM(c4 % 377), SUM(c4 % 378), SUM(c4 % 379),
 SUM(c4 % 380), SUM(c4 % 381), SUM(c4 % 382), SUM(c4 % 383), SUM(c4 % 384),
 SUM(c4 % 385), SUM(c4 % 386), SUM(c4 % 387), SUM(c4 % 388), SUM(c4 % 389),
 SUM(c4 % 390), SUM(c4 % 391), SUM(c4 % 392), SUM(c4 % 393), SUM(c4 % 394),
 SUM(c4 % 395), SUM(c4 % 396), SUM(c4 % 397), SUM(c4 % 398), SUM(c4 % 399),
 SUM(c4 % 400), SUM(c4 % 401), SUM(c4 % 402), SUM(c4 % 403), SUM(c4 % 404),
 SUM(c4 % 405), SUM(c4 % 406), SUM(c4 % 407), SUM(c4 % 408), SUM(c4 % 409),
 SUM(c4 % 410), SUM(c4 % 411), SUM(c4 % 412), SUM(c4 % 413), SUM(c4 % 414),
 SUM(c4 % 415), SUM(c4 % 416), SUM(c4 % 417), SUM(c4 % 418), SUM(c4 % 419),
 SUM(c4 % 420), SUM(c4 % 421), SUM(c4 % 422), SUM(c4 % 423), SUM(c4 % 424),
 SUM(c4 % 425), SUM(c4 % 426), SUM(c4 % 427), SUM(c4 % 428), SUM(c4 % 429),
 SUM(c4 % 430), SUM(c4 % 431), SUM(c4 % 432), SUM(c4 % 433), SUM(c4 % 434),
 SUM(c4 % 435), SUM(c4 % 436), SUM(c4 % 437), SUM(c4 % 438), SUM(c4 % 439),
 SUM(c4 % 440), SUM(c4 % 441), SUM(c4 % 442), SUM(c4 % 443), SUM(c4 % 444),
 SUM(c4 % 445), SUM(c4 % 446), SUM(c4 % 447), SUM(c4 % 448), SUM(c4 % 449),
 SUM(c4 % 450), SUM(c4 % 451), SUM(c4 % 452), SUM(c4 % 453), SUM(c4 % 454),
 SUM(c4 % 455), SUM(c4 % 456), SUM(c4 % 457), SUM(c4 % 458), SUM(c4 % 459),
 SUM(c4 % 460), SUM(c4 % 461), SUM(c4 % 462), SUM(c4 % 463), SUM(c4 % 464),
 SUM(c4 % 465), SUM(c4 % 466), SUM(c4 % 467), SUM(c4 % 468), SUM(c4 % 469),
 SUM(c4 % 470), SUM(c4 % 471), SUM(c4 % 472), SUM(c4 % 473), SUM(c4 % 474),
 SUM(c4 % 475), SUM(c4 % 476), SUM(c4 % 477), SUM(c4 % 478), SUM(c4 % 479),
 SUM(c4 % 480), SUM(c4 % 481), SUM(c4 % 482), SUM(c4 % 483), SUM(c4 % 484),
 SUM(c4 % 485), SUM(c4 % 486), SUM(c4 % 487), SUM(c4 % 488), SUM(c4 % 489),
 SUM(c4 % 490), SUM(c4 % 491), SUM(c4 % 492), SUM(c4 % 493), SUM(c4 % 494),
 SUM(c4 % 495), SUM(c4 % 496), SUM(c4 % 497), SUM(c4 % 498), SUM(c4 % 499),
 SUM(c4 % 500), SUM(c4 % 501), SUM(c4 % 502), SUM(c4 % 503), SUM(c4 % 504),
 SUM(c4 % 505), SUM(c4 % 506), SUM(c4 % 507), SUM(c4 % 508), SUM(c4 % 509),
 SUM(c4 % 510), SUM(c4 % 511), SUM(c4 % 512), SUM(c4 % 513), SUM(c4 % 514),
 SUM(c4 % 515), SUM(c4 % 516), SUM(c4 % 517), SUM(c4 % 518), SUM(c4 % 519),
 SUM(c4 % 520), SUM(c4 % 521), SUM(c4 % 522), SUM(c4 % 523), SUM(c4 % 524),
 SUM(c4 % 525), SUM(c4 % 526), SUM(c4 % 527), SUM(c4 % 528), SUM(c4 % 529),
 SUM(c4 % 530), SUM(c4 % 531), SUM(c4 % 532), SUM(c4 % 533), SUM(c4 % 534),
 SUM(c4 % 535), SUM(c4 % 536), SUM(c4 % 537), SUM(c4 % 538), SUM(c4 % 539),
 SUM(c4 % 540), SUM(c4 % 541), SUM(c4 % 542), SUM(c4 % 543), SUM(c4 % 544),
 SUM(c4 % 545), SUM(c4 % 546), SUM(c4 % 547), SUM(c4 % 548), SUM(c4 % 549),
 SUM(c4 % 550), SUM(c4 % 551), SUM(c4 % 552), SUM(c4 % 553), SUM(c4 % 554),
 SUM(c4 % 555), SUM(c4 % 556), SUM(c4 % 557), SUM(c4 % 558), SUM(c4 % 559),
 SUM(c4 % 560), SUM(c4 % 561), SUM(c4 % 562), SUM(c4 % 563), SUM(c4 % 564),
 SUM(c4 % 565), SUM(c4 % 566), SUM(c4 % 567), SUM(c4 % 568), SUM(c4 % 569),
 SUM(c4 % 570), SUM(c4 % 571), SUM(c4 % 572), SUM(c4 % 573), SUM(c4 % 574),
 SUM(c4 % 575), SUM(c4 % 576), SUM(c4 % 577), SUM(c4 % 578), SUM(c4 % 579),
 SUM(c4 % 580), SUM(c4 % 581), SUM(c4 % 582), SUM(c4 % 583), SUM(c4 % 584),
 SUM(c4 % 585), SUM(c4 % 586), SUM(c4 % 587), SUM(c4 % 588), SUM(c4 % 589),
 SUM(c4 % 590), SUM(c4 % 591), SUM(c4 % 592), SUM(c4 % 593), SUM(c4 % 594),
 SUM(c4 % 595), SUM(c4 % 596), SUM(c4 % 597), SUM(c4 % 598), SUM(c4 % 599),
 SUM(c4 % 600), SUM(c4 % 601), SUM(c4 % 602), SUM(c4 % 603), SUM(c4 % 604),
 SUM(c4 % 605), SUM(c4 % 606), SUM(c4 % 607), SUM(c4 % 608), SUM(c4 % 609),
 SUM(c4 % 610), SUM(c4 % 611), SUM(c4 % 612), SUM(c4 % 613), SUM(c4 % 614),
 SUM(c4 % 615), SUM(c4 % 616), SUM(c4 % 617), SUM(c4 % 618), SUM(c4 % 619),
 SUM(c4 % 620), SUM(c4 % 621), SUM(c4 % 622), SUM(c4 % 623), SUM(c4 % 624),
 SUM(c4 % 625), SUM(c4 % 626), SUM(c4 % 627), SUM(c4 % 628), SUM(c4 % 629),
 SUM(c4 % 630), SUM(c4 % 631), SUM(c4 % 632), SUM(c4 % 633), SUM(c4 % 634),
 SUM(c4 % 635), SUM(c4 % 636), SUM(c4 % 637), SUM(c4 % 638), SUM(c4 % 639),
 SUM(c4 % 640), SUM(c4 % 641), SUM(c4 % 642), SUM(c4 % 643), SUM(c4 % 644),
 SUM(c4 % 645), SUM(c4 % 646), SUM(c4 % 647), SUM(c4 % 648), SUM(c4 % 649),
 SUM(c4 % 650), SUM(c4 % 651), SUM(c4 % 652), SUM(c4 % 653), SUM(c4 % 654),
 SUM(c4 % 655), SUM(c4 % 656), SUM(c4 % 657), SUM(c4 % 658), SUM(c4 % 659),
 SUM(c4 % 660), SUM(c4 % 661), SUM(c4 % 662), SUM(c4 % 663), SUM(c4 % 664),
 SUM(c4 % 665), SUM(c4 % 666), SUM(c4 % 667), SUM(c4 % 668), SUM(c4 % 669),
 SUM(c4 % 670), SUM(c4 % 671), SUM(c4 % 672), SUM(c4 % 673), SUM(c4 % 674),
 SUM(c4 % 675), SUM(c4 % 676), SUM(c4 % 677), SUM(c4 % 678), SUM(c4 % 679),
 SUM(c4 % 680), SUM(c4 % 681), SUM(c4 % 682), SUM(c4 % 683), SUM(c4 % 684),
 SUM(c4 % 685), SUM(c4 % 686), SUM(c4 % 687), SUM(c4 % 688), SUM(c4 % 689),
 SUM(c4 % 690), SUM(c4 % 691), SUM(c4 % 692), SUM(c4 % 693), SUM(c4 % 694),
 SUM(c4 % 695), SUM(c4 % 696), SUM(c4 % 697), SUM(c4 % 698), SUM(c4 % 699),
 SUM(c4 % 700), SUM(c4 % 701), SUM(c4 % 702), SUM(c4 % 703), SUM(c4 % 704),
 SUM(c4 % 705), SUM(c4 % 706), SUM(c4 % 707), SUM(c4 % 708), SUM(c4 % 709),
 SUM(c4 % 710), SUM(c4 % 711), SUM(c4 % 712), SUM(c4 % 713), SUM(c4 % 714),
 SUM(c4 % 715), SUM(c4 % 716), SUM(c4 % 717), SUM(c4 % 718), SUM(c4 % 719),
 SUM(c4 % 720), SUM(c4 % 721), SUM(c4 % 722), SUM(c4 % 723), SUM(c4 % 724),
 SUM(c4 % 725), SUM(c4 % 726), SUM(c4 % 727), SUM(c4 % 728), SUM(c4 % 729),
 SUM(c4 % 730), SUM(c4 % 731), SUM(c4 % 732), SUM(c4 % 733), SUM(c4 % 734),
 SUM(c4 % 735), SUM(c4 % 736), SUM(c4 % 737), SUM(c4 % 738), SUM(c4 % 739),
 SUM(c4 % 740), SUM(c4 % 741), SUM(c4 % 742), SUM(c4 % 743), SUM(c4 % 744),
 SUM(c4 % 745), SUM(c4 % 746), SUM(c4 % 747), SUM(c4 % 748), SUM(c4 % 749),
 SUM(c4 % 750), SUM(c4 % 751), SUM(c4 % 752), SUM(c4 % 753), SUM(c4 % 754),
 SUM(c4 % 755), SUM(c4 % 756), SUM(c4 % 757), SUM(c4 % 758), SUM(c4 % 759),
 SUM(c4 % 760), SUM(c4 % 761), SUM(c4 % 762), SUM(c4 % 763), SUM(c4 % 764),
 SUM(c4 % 765), SUM(c4 % 766), SUM(c4 % 767), SUM(c4 % 768), SUM(c4 % 769),
 SUM(c4 % 770), SUM(c4 % 771), SUM(c4 % 772), SUM(c4 % 773), SUM(c4 % 774),
 SUM(c4 % 775), SUM(c4 % 776), SUM(c4 % 777), SUM(c4 % 778), SUM(c4 % 779),
 SUM(c4 % 780), SUM(c4 % 781), SUM(c4 % 782), SUM(c4 % 783), SUM(c4 % 784),
 SUM(c4 % 785), SUM(c4 % 786), SUM(c4 % 787), SUM(c4 % 788), SUM(c4 % 789),
 SUM(c4 % 790), SUM(c4 % 791), SUM(c4 % 792), SUM(c4 % 793), SUM(c4 % 794),
 SUM(c4 % 795), SUM(c4 % 796), SUM(c4 % 797), SUM(c4 % 798), SUM(c4 % 799),
 SUM(c4 % 800), SUM(c4 % 801), SUM(c4 % 802), SUM(c4 % 803), SUM(c4 % 804),
 SUM(c4 % 805), SUM(c4 % 806), SUM(c4 % 807), SUM(c4 % 808), SUM(c4 % 809),
 SUM(c4 % 810), SUM(c4 % 811), SUM(c4 % 812), SUM(c4 % 813), SUM(c4 % 814),
 SUM(c4 % 815), SUM(c4 % 816), SUM(c4 % 817), SUM(c4 % 818), SUM(c4 % 819),
 SUM(c4 % 820), SUM(c4 % 821), SUM(c4 % 822), SUM(c4 % 823), SUM(c4 % 824),
 SUM(c4 % 825), SUM(c4 % 826), SUM(c4 % 827), SUM(c4 % 828), SUM(c4 % 829),
 SUM(c4 % 830), SUM(c4 % 831), SUM(c4 % 832), SUM(c4 % 833), SUM(c4 % 834),
 SUM(c4 % 835), SUM(c4 % 836), SUM(c4 % 837), SUM(c4 % 838), SUM(c4 % 839),
 SUM(c4 % 840), SUM(c4 % 841), SUM(c4 % 842), SUM(c4 % 843), SUM(c4 % 844),
 SUM(c4 % 845), SUM(c4 % 846), SUM(c4 % 847), SUM(c4 % 848), SUM(c4 % 849),
 SUM(c4 % 850), SUM(c4 % 851), SUM(c4 % 852), SUM(c4 % 853), SUM(c4 % 854),
 SUM(c4 % 855), SUM(c4 % 856), SUM(c4 % 857), SUM(c4 % 858), SUM(c4 % 859),
 SUM(c4 % 860), SUM(c4 % 861), SUM(c4 % 862), SUM(c4 % 863), SUM(c4 % 864),
 SUM(c4 % 865), SUM(c4 % 866), SUM(c4 % 867), SUM(c4 % 868), SUM(c4 % 869),
 SUM(c4 % 870), SUM(c4 % 871), SUM(c4 % 872), SUM(c4 % 873), SUM(c4 % 874),
 SUM(c4 % 875), SUM(c4 % 876), SUM(c4 % 877), SUM(c4 % 878), SUM(c4 % 879),
 SUM(c4 % 880), SUM(c4 % 881), SUM(c4 % 882), SUM(c4 % 883), SUM(c4 % 884),
 SUM(c4 % 885), SUM(c4 % 886), SUM(c4 % 887), SUM(c4 % 888), SUM(c4 % 889),
 SUM(c4 % 890), SUM(c4 % 891), SUM(c4 % 892), SUM(c4 % 893), SUM(c4 % 894),
 SUM(c4 % 895), SUM(c4 % 896), SUM(c4 % 897), SUM(c4 % 898), SUM(c4 % 899),
 SUM(c4 % 900), SUM(c4 % 901), SUM(c4 % 902), SUM(c4 % 903), SUM(c4 % 904),
 SUM(c4 % 905), SUM(c4 % 906), SUM(c4 % 907), SUM(c4 % 908), SUM(c4 % 909),
 SUM(c4 % 910), SUM(c4 % 911), SUM(c4 % 912), SUM(c4 % 913), SUM(c4 % 914),
 SUM(c4 % 915), SUM(c4 % 916), SUM(c4 % 917), SUM(c4 % 918), SUM(c4 % 919),
 SUM(c4 % 920), SUM(c4 % 921), SUM(c4 % 922), SUM(c4 % 923), SUM(c4 % 924),
 SUM(c4 % 925), SUM(c4 % 926), SUM(c4 % 927), SUM(c4 % 928), SUM(c4 % 929),
 SUM(c4 % 930), SUM(c4 % 931), SUM(c4 % 932), SUM(c4 % 933), SUM(c4 % 934),
 SUM(c4 % 935), SUM(c4 % 936), SUM(c4 % 937), SUM(c4 % 938), SUM(c4 % 939),
 SUM(c4 % 940), SUM(c4 % 941), SUM(c4 % 942), SUM(c4 % 943), SUM(c4 % 944),
 SUM(c4 % 945), SUM(c4 % 946), SUM(c4 % 947), SUM(c4 % 948), SUM(c4 % 949),
 SUM(c4 % 950), SUM(c4 % 951), SUM(c4 % 952), SUM(c4 % 953), SUM(c4 % 954),
 SUM(c4 % 955), SUM(c4 % 956), SUM(c4 % 957), SUM(c4 % 958), SUM(c4 % 959),
 SUM(c4 % 960), SUM(c4 % 961), SUM(c4 % 962), SUM(c4 % 963), SUM(c4 % 964),
 SUM(c4 % 965), SUM(c4 % 966), SUM(c4 % 967), SUM(c4 % 968), SUM(c4 % 969),
 SUM(c4 % 970), SUM(c4 % 971), SUM(c4 % 972), SUM(c4 % 973), SUM(c4 % 974),
 SUM(c4 % 975), SUM(c4 % 976), SUM(c4 % 977), SUM(c4 % 978), SUM(c4 % 979),
 SUM(c4 % 980), SUM(c4 % 981), SUM(c4 % 982), SUM(c4 % 983), SUM(c4 % 984),
 SUM(c4 % 985), SUM(c4 % 986), SUM(c4 % 987), SUM(c4 % 988), SUM(c4 % 989),
 SUM(c4 % 990), SUM(c4 % 991), SUM(c4 % 992), SUM(c4 % 993), SUM(c4 % 994),
 SUM(c4 % 995), SUM(c4 % 996), SUM(c4 % 997), SUM(c4 % 998), SUM(c4 % 999),
 SUM(c4 % 1000), SUM(c4 % 1001), SUM(c4 % 1002), SUM(c4 % 1003), SUM(c4 % 1004),
 SUM(c4 % 1005), SUM(c4 % 1006), SUM(c4 % 1007), SUM(c4 % 1008), SUM(c4 % 1009),
 SUM(c4 % 1010), SUM(c4 % 1011), SUM(c4 % 1012), SUM(c4 % 1013), SUM(c4 % 1014),
 SUM(c4 % 1015), SUM(c4 % 1016), SUM(c4 % 1017), SUM(c4 % 1018), SUM(c4 % 1019),
 SUM(c4 % 1020), SUM(c4 % 1021), SUM(c4 % 1022), SUM(c4 % 1023), SUM(c4 % 1024),
 SUM(c4 % 1025), SUM(c4 % 1026), SUM(c4 % 1027), SUM(c4 % 1028), SUM(c4 % 1029),
 SUM(c4 % 1030), SUM(c4 % 1031), SUM(c4 % 1032), SUM(c4 % 1033), SUM(c4 % 1034),
 SUM(c4 % 1035), SUM(c4 % 1036), SUM(c4 % 1037), SUM(c4 % 1038), SUM(c4 % 1039),
 SUM(c4 % 1040), SUM(c4 % 1041), SUM(c4 % 1042), SUM(c4 % 1043), SUM(c4 % 1044),
 SUM(c4 % 1045), SUM(c4 % 1046), SUM(c4 % 1047), SUM(c4 % 1048), SUM(c4 % 1049),
 SUM(c4 % 1050), SUM(c4 % 1051), SUM(c4 % 1052), SUM(c4 % 1053), SUM(c4 % 1054),
 SUM(c4 % 1055), SUM(c4 % 1056), SUM(c4 % 1057), SUM(c4 % 1058), SUM(c4 % 1059),
 SUM(c4 % 1060), SUM(c4 % 1061), SUM(c4 % 1062), SUM(c4 % 1063), SUM(c4 % 1064),
 SUM(c4 % 1065), SUM(c4 % 1066), SUM(c4 % 1067), SUM(c4 % 1068), SUM(c4 % 1069),
 SUM(c4 % 1070), SUM(c4 % 1071), SUM(c4 % 1072), SUM(c4 % 1073), SUM(c4 % 1074),
 SUM(c4 % 1075), SUM(c4 % 1076), SUM(c4 % 1077), SUM(c4 % 1078), SUM(c4 % 1079),
 SUM(c4 % 1080), SUM(c4 % 1081), SUM(c4 % 1082), SUM(c4 % 1083), SUM(c4 % 1084),
 SUM(c4 % 1085), SUM(c4 % 1086), SUM(c4 % 1087), SUM(c4 % 1088), SUM(c4 % 1089),
 SUM(c4 % 1090), SUM(c4 % 1091), SUM(c4 % 1092), SUM(c4 % 1093), SUM(c4 % 1094),
 SUM(c4 % 1095), SUM(c4 % 1096), SUM(c4 % 1097), SUM(c4 % 1098), SUM(c4 % 1099),
 SUM(c4 % 1100), SUM(c4 % 1101), SUM(c4 % 1102), SUM(c4 % 1103), SUM(c4 % 1104),
 SUM(c4 % 1105), SUM(c4 % 1106), SUM(c4 % 1107), SUM(c4 % 1108), SUM(c4 % 1109),
 SUM(c4 % 1110), SUM(c4 % 1111), SUM(c4 % 1112), SUM(c4 % 1113), SUM(c4 % 1114),
 SUM(c4 % 1115), SUM(c4 % 1116), SUM(c4 % 1117), SUM(c4 % 1118), SUM(c4 % 1119),
 SUM(c4 % 1120), SUM(c4 % 1121), SUM(c4 % 1122), SUM(c4 % 1123), SUM(c4 % 1124),
 SUM(c4 % 1125), SUM(c4 % 1126), SUM(c4 % 1127), SUM(c4 % 1128), SUM(c4 % 1129),
 SUM(c4 % 1130), SUM(c4 % 1131), SUM(c4 % 1132), SUM(c4 % 1133), SUM(c4 % 1134),
 SUM(c4 % 1135), SUM(c4 % 1136), SUM(c4 % 1137), SUM(c4 % 1138), SUM(c4 % 1139),
 SUM(c4 % 1140), SUM(c4 % 1141), SUM(c4 % 1142), SUM(c4 % 1143), SUM(c4 % 1144),
 SUM(c4 % 1145), SUM(c4 % 1146), SUM(c4 % 1147), SUM(c4 % 1148), SUM(c4 % 1149),
 SUM(c4 % 1150), SUM(c4 % 1151), SUM(c4 % 1152), SUM(c4 % 1153), SUM(c4 % 1154),
 SUM(c4 % 1155), SUM(c4 % 1156), SUM(c4 % 1157), SUM(c4 % 1158), SUM(c4 % 1159),
 SUM(c4 % 1160), SUM(c4 % 1161), SUM(c4 % 1162), SUM(c4 % 1163), SUM(c4 % 1164),
 SUM(c4 % 1165), SUM(c4 % 1166), SUM(c4 % 1167), SUM(c4 % 1168), SUM(c4 % 1169),
 SUM(c4 % 1170), SUM(c4 % 1171), SUM(c4 % 1172), SUM(c4 % 1173), SUM(c4 % 1174),
 SUM(c4 % 1175), SUM(c4 % 1176), SUM(c4 % 1177), SUM(c4 % 1178), SUM(c4 % 1179),
 SUM(c4 % 1180), SUM(c4 % 1181), SUM(c4 % 1182), SUM(c4 % 1183), SUM(c4 % 1184),
 SUM(c4 % 1185), SUM(c4 % 1186), SUM(c4 % 1187), SUM(c4 % 1188), SUM(c4 % 1189),
 SUM(c4 % 1190), SUM(c4 % 1191), SUM(c4 % 1192), SUM(c4 % 1193), SUM(c4 % 1194),
 SUM(c4 % 1195), SUM(c4 % 1196), SUM(c4 % 1197), SUM(c4 % 1198), SUM(c4 % 1199),
 SUM(c4 % 1200), SUM(c4 % 1201), SUM(c4 % 1202), SUM(c4 % 1203), SUM(c4 % 1204),
 SUM(c4 % 1205), SUM(c4 % 1206), SUM(c4 % 1207), SUM(c4 % 1208), SUM(c4 % 1209),
 SUM(c4 % 1210), SUM(c4 % 1211), SUM(c4 % 1212), SUM(c4 % 1213), SUM(c4 % 1214),
 SUM(c4 % 1215), SUM(c4 % 1216), SUM(c4 % 1217), SUM(c4 % 1218), SUM(c4 % 1219),
 SUM(c4 % 1220), SUM(c4 % 1221), SUM(c4 % 1222), SUM(c4 % 1223), SUM(c4 % 1224),
 SUM(c4 % 1225), SUM(c4 % 1226), SUM(c4 % 1227), SUM(c4 % 1228), SUM(c4 % 1229),
 SUM(c4 % 1230), SUM(c4 % 1231), SUM(c4 % 1232), SUM(c4 % 1233), SUM(c4 % 1234),
 SUM(c4 % 1235), SUM(c4 % 1236), SUM(c4 % 1237), SUM(c4 % 1238), SUM(c4 % 1239),
 SUM(c4 % 1240), SUM(c4 % 1241), SUM(c4 % 1242), SUM(c4 % 1243), SUM(c4 % 1244),
 SUM(c4 % 1245), SUM(c4 % 1246), SUM(c4 % 1247), SUM(c4 % 1248), SUM(c4 % 1249),
 SUM(c4 % 1250), SUM(c4 % 1251), SUM(c4 % 1252), SUM(c4 % 1253), SUM(c4 % 1254),
 SUM(c4 % 1255), SUM(c4 % 1256), SUM(c4 % 1257), SUM(c4 % 1258), SUM(c4 % 1259),
 SUM(c4 % 1260), SUM(c4 % 1261), SUM(c4 % 1262), SUM(c4 % 1263), SUM(c4 % 1264),
 SUM(c4 % 1265), SUM(c4 % 1266), SUM(c4 % 1267), SUM(c4 % 1268), SUM(c4 % 1269),
 SUM(c4 % 1270), SUM(c4 % 1271), SUM(c4 % 1272), SUM(c4 % 1273), SUM(c4 % 1274),
 SUM(c4 % 1275), SUM(c4 % 1276), SUM(c4 % 1277), SUM(c4 % 1278), SUM(c4 % 1279),
 SUM(c4 % 1280), SUM(c4 % 1281), SUM(c4 % 1282), SUM(c4 % 1283), SUM(c4 % 1284),
 SUM(c4 % 1285), SUM(c4 % 1286), SUM(c4 % 1287), SUM(c4 % 1288), SUM(c4 % 1289),
 SUM(c4 % 1290), SUM(c4 % 1291), SUM(c4 % 1292), SUM(c4 % 1293), SUM(c4 % 1294),
 SUM(c4 % 1295), SUM(c4 % 1296), SUM(c4 % 1297), SUM(c4 % 1298), SUM(c4 % 1299),
 SUM(c4 % 1300), SUM(c4 % 1301), SUM(c4 % 1302), SUM(c4 % 1303), SUM(c4 % 1304),
 SUM(c4 % 1305), SUM(c4 % 1306), SUM(c4 % 1307), SUM(c4 % 1308), SUM(c4 % 1309),
 SUM(c4 % 1310), SUM(c4 % 1311), SUM(c4 % 1312), SUM(c4 % 1313), SUM(c4 % 1314),
 SUM(c4 % 1315), SUM(c4 % 1316), SUM(c4 % 1317), SUM(c4 % 1318), SUM(c4 % 1319),
 SUM(c4 % 1320), SUM(c4 % 1321), SUM(c4 % 1322), SUM(c4 % 1323), SUM(c4 % 1324),
 SUM(c4 % 1325), SUM(c4 % 1326), SUM(c4 % 1327), SUM(c4 % 1328), SUM(c4 % 1329),
 SUM(c4 % 1330), SUM(c4 % 1331), SUM(c4 % 1332), SUM(c4 % 1333), SUM(c4 % 1334),
 SUM(c4 % 1335), SUM(c4 % 1336), SUM(c4 % 1337), SUM(c4 % 1338), SUM(c4 % 1339),
 SUM(c4 % 1340), SUM(c4 % 1341), SUM(c4 % 1342), SUM(c4 % 1343), SUM(c4 % 1344),
 SUM(c4 % 1345), SUM(c4 % 1346), SUM(c4 % 1347), SUM(c4 % 1348), SUM(c4 % 1349),
 SUM(c4 % 1350), SUM(c4 % 1351), SUM(c4 % 1352), SUM(c4 % 1353), SUM(c4 % 1354),
 SUM(c4 % 1355), SUM(c4 % 1356), SUM(c4 % 1357), SUM(c4 % 1358), SUM(c4 % 1359),
 SUM(c4 % 1360), SUM(c4 % 1361), SUM(c4 % 1362), SUM(c4 % 1363), SUM(c4 % 1364),
 SUM(c4 % 1365), SUM(c4 % 1366), SUM(c4 % 1367), SUM(c4 % 1368), SUM(c4 % 1369),
 SUM(c4 % 1370), SUM(c4 % 1371), SUM(c4 % 1372), SUM(c4 % 1373), SUM(c4 % 1374),
 SUM(c4 % 1375), SUM(c4 % 1376), SUM(c4 % 1377), SUM(c4 % 1378), SUM(c4 % 1379),
 SUM(c4 % 1380), SUM(c4 % 1381), SUM(c4 % 1382), SUM(c4 % 1383), SUM(c4 % 1384),
 SUM(c4 % 1385), SUM(c4 % 1386), SUM(c4 % 1387), SUM(c4 % 1388), SUM(c4 % 1389),
 SUM(c4 % 1390), SUM(c4 % 1391), SUM(c4 % 1392), SUM(c4 % 1393), SUM(c4 % 1394),
 SUM(c4 % 1395), SUM(c4 % 1396), SUM(c4 % 1397), SUM(c4 % 1398), SUM(c4 % 1399),
 SUM(c4 % 1400), SUM(c4 % 1401), SUM(c4 % 1402), SUM(c4 % 1403), SUM(c4 % 1404),
 SUM(c4 % 1405), SUM(c4 % 1406), SUM(c4 % 1407), SUM(c4 % 1408), SUM(c4 % 1409),
 SUM(c4 % 1410), SUM(c4 % 1411), SUM(c4 % 1412), SUM(c4 % 1413), SUM(c4 % 1414),
 SUM(c4 % 1415), SUM(c4 % 1416), SUM(c4 % 1417), SUM(c4 % 1418), SUM(c4 % 1419),
 SUM(c4 % 1420), SUM(c4 % 1421), SUM(c4 % 1422), SUM(c4 % 1423), SUM(c4 % 1424),
 SUM(c4 % 1425), SUM(c4 % 1426), SUM(c4 % 1427), SUM(c4 % 1428), SUM(c4 % 1429),
 SUM(c4 % 1430), SUM(c4 % 1431), SUM(c4 % 1432), SUM(c4 % 1433), SUM(c4 % 1434),
 SUM(c4 % 1435), SUM(c4 % 1436), SUM(c4 % 1437), SUM(c4 % 1438), SUM(c4 % 1439),
 SUM(c4 % 1440), SUM(c4 % 1441), SUM(c4 % 1442), SUM(c4 % 1443), SUM(c4 % 1444),
 SUM(c4 % 1445), SUM(c4 % 1446), SUM(c4 % 1447), SUM(c4 % 1448), SUM(c4 % 1449),
 SUM(c4 % 1450), SUM(c4 % 1451), SUM(c4 % 1452), SUM(c4 % 1453), SUM(c4 % 1454),
 SUM(c4 % 1455), SUM(c4 % 1456), SUM(c4 % 1457), SUM(c4 % 1458), SUM(c4 % 1459),
 SUM(c4 % 1460), SUM(c4 % 1461), SUM(c4 % 1462), SUM(c4 % 1463), SUM(c4 % 1464),
 SUM(c4 % 1465), SUM(c4 % 1466), SUM(c4 % 1467), SUM(c4 % 1468), SUM(c4 % 1469),
 SUM(c4 % 1470), SUM(c4 % 1471), SUM(c4 % 1472), SUM(c4 % 1473), SUM(c4 % 1474),
 SUM(c4 % 1475), SUM(c4 % 1476), SUM(c4 % 1477), SUM(c4 % 1478), SUM(c4 % 1479),
 SUM(c4 % 1480), SUM(c4 % 1481), SUM(c4 % 1482), SUM(c4 % 1483), SUM(c4 % 1484),
 SUM(c4 % 1485), SUM(c4 % 1486), SUM(c4 % 1487), SUM(c4 % 1488), SUM(c4 % 1489),
 SUM(c4 % 1490), SUM(c4 % 1491), SUM(c4 % 1492), SUM(c4 % 1493), SUM(c4 % 1494),
 SUM(c4 % 1495), SUM(c4 % 1496), SUM(c4 % 1497), SUM(c4 % 1498), SUM(c4 % 1499),
 SUM(c4 % 1500), SUM(c4 % 1501), SUM(c4 % 1502), SUM(c4 % 1503), SUM(c4 % 1504),
 SUM(c4 % 1505), SUM(c4 % 1506), SUM(c4 % 1507), SUM(c4 % 1508), SUM(c4 % 1509),
 SUM(c4 % 1510), SUM(c4 % 1511), SUM(c4 % 1512), SUM(c4 % 1513), SUM(c4 % 1514),
 SUM(c4 % 1515), SUM(c4 % 1516), SUM(c4 % 1517), SUM(c4 % 1518), SUM(c4 % 1519),
 SUM(c4 % 1520), SUM(c4 % 1521), SUM(c4 % 1522), SUM(c4 % 1523), SUM(c4 % 1524),
 SUM(c4 % 1525), SUM(c4 % 1526), SUM(c4 % 1527), SUM(c4 % 1528), SUM(c4 % 1529),
 SUM(c4 % 1530), SUM(c4 % 1531), SUM(c4 % 1532), SUM(c4 % 1533), SUM(c4 % 1534),
 SUM(c4 % 1535), SUM(c4 % 1536), SUM(c4 % 1537), SUM(c4 % 1538), SUM(c4 % 1539),
 SUM(c4 % 1540), SUM(c4 % 1541), SUM(c4 % 1542), SUM(c4 % 1543), SUM(c4 % 1544),
 SUM(c4 % 1545), SUM(c4 % 1546), SUM(c4 % 1547), SUM(c4 % 1548), SUM(c4 % 1549),
 SUM(c4 % 1550), SUM(c4 % 1551), SUM(c4 % 1552), SUM(c4 % 1553), SUM(c4 % 1554),
 SUM(c4 % 1555), SUM(c4 % 1556), SUM(c4 % 1557), SUM(c4 % 1558), SUM(c4 % 1559),
 SUM(c4 % 1560), SUM(c4 % 1561), SUM(c4 % 1562), SUM(c4 % 1563), SUM(c4 % 1564),
 SUM(c4 % 1565), SUM(c4 % 1566), SUM(c4 % 1567), SUM(c4 % 1568), SUM(c4 % 1569),
 SUM(c4 % 1570), SUM(c4 % 1571), SUM(c4 % 1572), SUM(c4 % 1573), SUM(c4 % 1574),
 SUM(c4 % 1575), SUM(c4 % 1576), SUM(c4 % 1577), SUM(c4 % 1578), SUM(c4 % 1579),
 SUM(c4 % 1580), SUM(c4 % 1581), SUM(c4 % 1582), SUM(c4 % 1583), SUM(c4 % 1584),
 SUM(c4 % 1585), SUM(c4 % 1586), SUM(c4 % 1587), SUM(c4 % 1588), SUM(c4 % 1589),
 SUM(c4 % 1590), SUM(c4 % 1591), SUM(c4 % 1592), SUM(c4 % 1593), SUM(c4 % 1594),
 SUM(c4 % 1595), SUM(c4 % 1596), SUM(c4 % 1597), SUM(c4 % 1598), SUM(c4 % 1599),
 SUM(c4 % 1600), SUM(c4 % 1601), SUM(c4 % 1602), SUM(c4 % 1603), SUM(c4 % 1604),
 SUM(c4 % 1605), SUM(c4 % 1606), SUM(c4 % 1607), SUM(c4 % 1608), SUM(c4 % 1609),
 SUM(c4 % 1610), SUM(c4 % 1611), SUM(c4 % 1612), SUM(c4 % 1613), SUM(c4 % 1614),
 SUM(c4 % 1615), SUM(c4 % 1616), SUM(c4 % 1617), SUM(c4 % 1618), SUM(c4 % 1619),
 SUM(c4 % 1620), SUM(c4 % 1621), SUM(c4 % 1622), SUM(c4 % 1623), SUM(c4 % 1624),
 SUM(c4 % 1625), SUM(c4 % 1626), SUM(c4 % 1627), SUM(c4 % 1628), SUM(c4 % 1629),
 SUM(c4 % 1630), SUM(c4 % 1631), SUM(c4 % 1632), SUM(c4 % 1633), SUM(c4 % 1634),
 SUM(c4 % 1635), SUM(c4 % 1636), SUM(c4 % 1637), SUM(c4 % 1638), SUM(c4 % 1639),
 SUM(c4 % 1640), SUM(c4 % 1641), SUM(c4 % 1642), SUM(c4 % 1643), SUM(c4 % 1644),
 SUM(c4 % 1645), SUM(c4 % 1646), SUM(c4 % 1647), SUM(c4 % 1648), SUM(c4 % 1649),
 SUM(c4 % 1650), SUM(c4 % 1651), SUM(c4 % 1652), SUM(c4 % 1653), SUM(c4 % 1654),
 SUM(c4 % 1655), SUM(c4 % 1656), SUM(c4 % 1657), SUM(c4 % 1658), SUM(c4 % 1659),
 SUM(c4 % 1660), SUM(c4 % 1661), SUM(c4 % 1662), SUM(c4 % 1663), SUM(c4 % 1664),
 SUM(c4 % 1665), SUM(c4 % 1666), SUM(c4 % 1667), SUM(c4 % 1668), SUM(c4 % 1669),
 SUM(c4 % 1670), SUM(c4 % 1671), SUM(c4 % 1672), SUM(c4 % 1673), SUM(c4 % 1674),
 SUM(c4 % 1675), SUM(c4 % 1676), SUM(c4 % 1677), SUM(c4 % 1678), SUM(c4 % 1679),
 SUM(c4 % 1680), SUM(c4 % 1681), SUM(c4 % 1682), SUM(c4 % 1683), SUM(c4 % 1684),
 SUM(c4 % 1685), SUM(c4 % 1686), SUM(c4 % 1687), SUM(c4 % 1688), SUM(c4 % 1689),
 SUM(c4 % 1690), SUM(c4 % 1691), SUM(c4 % 1692), SUM(c4 % 1693), SUM(c4 % 1694),
 SUM(c4 % 1695), SUM(c4 % 1696), SUM(c4 % 1697), SUM(c4 % 1698), SUM(c4 % 1699),
 SUM(c4 % 1700), SUM(c4 % 1701), SUM(c4 % 1702), SUM(c4 % 1703), SUM(c4 % 1704),
 SUM(c4 % 1705), SUM(c4 % 1706), SUM(c4 % 1707), SUM(c4 % 1708), SUM(c4 % 1709),
 SUM(c4 % 1710), SUM(c4 % 1711), SUM(c4 % 1712), SUM(c4 % 1713), SUM(c4 % 1714),
 SUM(c4 % 1715), SUM(c4 % 1716), SUM(c4 % 1717), SUM(c4 % 1718), SUM(c4 % 1719),
 SUM(c4 % 1720), SUM(c4 % 1721), SUM(c4 % 1722), SUM(c4 % 1723), SUM(c4 % 1724),
 SUM(c4 % 1725), SUM(c4 % 1726), SUM(c4 % 1727), SUM(c4 % 1728), SUM(c4 % 1729),
 SUM(c4 % 1730), SUM(c4 % 1731), SUM(c4 % 1732), SUM(c4 % 1733), SUM(c4 % 1734),
 SUM(c4 % 1735), SUM(c4 % 1736), SUM(c4 % 1737), SUM(c4 % 1738), SUM(c4 % 1739),
 SUM(c4 % 1740), SUM(c4 % 1741), SUM(c4 % 1742), SUM(c4 % 1743), SUM(c4 % 1744),
 SUM(c4 % 1745), SUM(c4 % 1746), SUM(c4 % 1747), SUM(c4 % 1748), SUM(c4 % 1749),
 SUM(c4 % 1750), SUM(c4 % 1751), SUM(c4 % 1752), SUM(c4 % 1753), SUM(c4 % 1754),
 SUM(c4 % 1755), SUM(c4 % 1756), SUM(c4 % 1757), SUM(c4 % 1758), SUM(c4 % 1759),
 SUM(c4 % 1760), SUM(c4 % 1761), SUM(c4 % 1762), SUM(c4 % 1763), SUM(c4 % 1764),
 SUM(c4 % 1765), SUM(c4 % 1766), SUM(c4 % 1767), SUM(c4 % 1768), SUM(c4 % 1769),
 SUM(c4 % 1770), SUM(c4 % 1771), SUM(c4 % 1772), SUM(c4 % 1773), SUM(c4 % 1774),
 SUM(c4 % 1775), SUM(c4 % 1776), SUM(c4 % 1777), SUM(c4 % 1778), SUM(c4 % 1779),
 SUM(c4 % 1780), SUM(c4 % 1781), SUM(c4 % 1782), SUM(c4 % 1783), SUM(c4 % 1784),
 SUM(c4 % 1785), SUM(c4 % 1786), SUM(c4 % 1787), SUM(c4 % 1788), SUM(c4 % 1789),
 SUM(c4 % 1790), SUM(c4 % 1791), SUM(c4 % 1792), SUM(c4 % 1793), SUM(c4 % 1794),
 SUM(c4 % 1795), SUM(c4 % 1796), SUM(c4 % 1797), SUM(c4 % 1798), SUM(c4 % 1799),
 SUM(c4 % 1800), SUM(c4 % 1801), SUM(c4 % 1802), SUM(c4 % 1803), SUM(c4 % 1804),
 SUM(c4 % 1805), SUM(c4 % 1806), SUM(c4 % 1807), SUM(c4 % 1808), SUM(c4 % 1809),
 SUM(c4 % 1810), SUM(c4 % 1811), SUM(c4 % 1812), SUM(c4 % 1813), SUM(c4 % 1814),
 SUM(c4 % 1815), SUM(c4 % 1816), SUM(c4 % 1817), SUM(c4 % 1818), SUM(c4 % 1819),
 SUM(c4 % 1820), SUM(c4 % 1821), SUM(c4 % 1822), SUM(c4 % 1823), SUM(c4 % 1824),
 SUM(c4 % 1825), SUM(c4 % 1826), SUM(c4 % 1827), SUM(c4 % 1828), SUM(c4 % 1829),
 SUM(c4 % 1830), SUM(c4 % 1831), SUM(c4 % 1832), SUM(c4 % 1833), SUM(c4 % 1834),
 SUM(c4 % 1835), SUM(c4 % 1836), SUM(c4 % 1837), SUM(c4 % 1838), SUM(c4 % 1839),
 SUM(c4 % 1840), SUM(c4 % 1841), SUM(c4 % 1842), SUM(c4 % 1843), SUM(c4 % 1844),
 SUM(c4 % 1845), SUM(c4 % 1846), SUM(c4 % 1847), SUM(c4 % 1848), SUM(c4 % 1849),
 SUM(c4 % 1850), SUM(c4 % 1851), SUM(c4 % 1852), SUM(c4 % 1853), SUM(c4 % 1854),
 SUM(c4 % 1855), SUM(c4 % 1856), SUM(c4 % 1857), SUM(c4 % 1858), SUM(c4 % 1859),
 SUM(c4 % 1860), SUM(c4 % 1861), SUM(c4 % 1862), SUM(c4 % 1863), SUM(c4 % 1864),
 SUM(c4 % 1865), SUM(c4 % 1866), SUM(c4 % 1867), SUM(c4 % 1868), SUM(c4 % 1869),
 SUM(c4 % 1870), SUM(c4 % 1871), SUM(c4 % 1872), SUM(c4 % 1873), SUM(c4 % 1874),
 SUM(c4 % 1875), SUM(c4 % 1876), SUM(c4 % 1877), SUM(c4 % 1878), SUM(c4 % 1879),
 SUM(c4 % 1880), SUM(c4 % 1881), SUM(c4 % 1882), SUM(c4 % 1883), SUM(c4 % 1884),
 SUM(c4 % 1885), SUM(c4 % 1886), SUM(c4 % 1887), SUM(c4 % 1888), SUM(c4 % 1889),
 SUM(c4 % 1890), SUM(c4 % 1891), SUM(c4 % 1892), SUM(c4 % 1893), SUM(c4 % 1894),
 SUM(c4 % 1895), SUM(c4 % 1896), SUM(c4 % 1897), SUM(c4 % 1898), SUM(c4 % 1899),
 SUM(c4 % 1900), SUM(c4 % 1901), SUM(c4 % 1902), SUM(c4 % 1903), SUM(c4 % 1904),
 SUM(c4 % 1905), SUM(c4 % 1906), SUM(c4 % 1907), SUM(c4 % 1908), SUM(c4 % 1909),
 SUM(c4 % 1910), SUM(c4 % 1911), SUM(c4 % 1912), SUM(c4 % 1913), SUM(c4 % 1914),
 SUM(c4 % 1915), SUM(c4 % 1916), SUM(c4 % 1917), SUM(c4 % 1918), SUM(c4 % 1919),
 SUM(c4 % 1920), SUM(c4 % 1921), SUM(c4 % 1922), SUM(c4 % 1923), SUM(c4 % 1924),
 SUM(c4 % 1925), SUM(c4 % 1926), SUM(c4 % 1927), SUM(c4 % 1928), SUM(c4 % 1929),
 SUM(c4 % 1930), SUM(c4 % 1931), SUM(c4 % 1932), SUM(c4 % 1933), SUM(c4 % 1934),
 SUM(c4 % 1935), SUM(c4 % 1936), SUM(c4 % 1937), SUM(c4 % 1938), SUM(c4 % 1939),
 SUM(c4 % 1940), SUM(c4 % 1941), SUM(c4 % 1942), SUM(c4 % 1943), SUM(c4 % 1944),
 SUM(c4 % 1945), SUM(c4 % 1946), SUM(c4 % 1947), SUM(c4 % 1948), SUM(c4 % 1949),
 SUM(c4 % 1950), SUM(c4 % 1951), SUM(c4 % 1952), SUM(c4 % 1953), SUM(c4 % 1954),
 SUM(c4 % 1955), SUM(c4 % 1956), SUM(c4 % 1957), SUM(c4 % 1958), SUM(c4 % 1959),
 SUM(c4 % 1960), SUM(c4 % 1961), SUM(c4 % 1962), SUM(c4 % 1963), SUM(c4 % 1964),
 SUM(c4 % 1965), SUM(c4 % 1966), SUM(c4 % 1967), SUM(c4 % 1968), SUM(c4 % 1969),
 SUM(c4 % 1970), SUM(c4 % 1971), SUM(c4 % 1972), SUM(c4 % 1973), SUM(c4 % 1974),
 SUM(c4 % 1975), SUM(c4 % 1976), SUM(c4 % 1977), SUM(c4 % 1978), SUM(c4 % 1979),
 SUM(c4 % 1980), SUM(c4 % 1981), SUM(c4 % 1982), SUM(c4 % 1983), SUM(c4 % 1984),
 SUM(c4 % 1985), SUM(c4 % 1986), SUM(c4 % 1987), SUM(c4 % 1988), SUM(c4 % 1989),
 SUM(c4 % 1990), SUM(c4 % 1991), SUM(c4 % 1992), SUM(c4 % 1993), SUM(c4 % 1994),
 SUM(c4 % 1995), SUM(c4 % 1996), SUM(c4 % 1997), SUM(c4 % 1998), SUM(c4 % 1999),
 SUM(c4 % 2000), SUM(c4 % 2001), SUM(c4 % 2002), SUM(c4 % 2003), SUM(c4 % 2004),
 SUM(c4 % 2005), SUM(c4 % 2006), SUM(c4 % 2007), SUM(c4 % 2008), SUM(c4 % 2009),
 SUM(c4 % 2010), SUM(c4 % 2011), SUM(c4 % 2012), SUM(c4 % 2013), SUM(c4 % 2014),
 SUM(c4 % 2015), SUM(c4 % 2016), SUM(c4 % 2017), SUM(c4 % 2018), SUM(c4 % 2019),
 SUM(c4 % 2020), SUM(c4 % 2021), SUM(c4 % 2022), SUM(c4 % 2023), SUM(c4 % 2024),
 SUM(c4 % 2025), SUM(c4 % 2026), SUM(c4 % 2027), SUM(c4 % 2028), SUM(c4 % 2029),
 SUM(c4 % 2030), SUM(c4 % 2031), SUM(c4 % 2032), SUM(c4 % 2033), SUM(c4 % 2034),
 SUM(c4 % 2035), SUM(c4 % 2036), SUM(c4 % 2037), SUM(c4 % 2038), SUM(c4 % 2039),
 SUM(c4 % 2040), SUM(c4 % 2041), SUM(c4 % 2042), SUM(c4 % 2043), SUM(c4 % 2044),
 SUM(c4 % 2045), SUM(c4 % 2046), SUM(c4 % 2047), SUM(c4 % 2048), SUM(c4 % 2049),
 SUM(c4 % 2050), SUM(c4 % 2051), SUM(c4 % 2052), SUM(c4 % 2053), SUM(c4 % 2054),
 SUM(c4 % 2055), SUM(c4 % 2056), SUM(c4 % 2057), SUM(c4 % 2058), SUM(c4 % 2059),
 SUM(c4 % 2060), SUM(c4 % 2061), SUM(c4 % 2062), SUM(c4 % 2063), SUM(c4 % 2064),
 SUM(c4 % 2065), SUM(c4 % 2066), SUM(c4 % 2067), SUM(c4 % 2068), SUM(c4 % 2069),
 SUM(c4 % 2070), SUM(c4 % 2071), SUM(c4 % 2072), SUM(c4 % 2073), SUM(c4 % 2074),
 SUM(c4 % 2075), SUM(c4 % 2076), SUM(c4 % 2077), SUM(c4 % 2078), SUM(c4 % 2079),
 SUM(c4 % 2080), SUM(c4 % 2081), SUM(c4 % 2082), SUM(c4 % 2083), SUM(c4 % 2084),
 SUM(c4 % 2085), SUM(c4 % 2086), SUM(c4 % 2087), SUM(c4 % 2088), SUM(c4 % 2089),
 SUM(c4 % 2090), SUM(c4 % 2091), SUM(c4 % 2092), SUM(c4 % 2093), SUM(c4 % 2094),
 SUM(c4 % 2095), SUM(c4 % 2096), SUM(c4 % 2097), SUM(c4 % 2098), SUM(c4 % 2099),
 SUM(c4 % 2100), SUM(c4 % 2101), SUM(c4 % 2102), SUM(c4 % 2103), SUM(c4 % 2104),
 SUM(c4 % 2105), SUM(c4 % 2106), SUM(c4 % 2107), SUM(c4 % 2108), SUM(c4 % 2109),
 SUM(c4 % 2110), SUM(c4 % 2111), SUM(c4 % 2112), SUM(c4 % 2113), SUM(c4 % 2114),
 SUM(c4 % 2115), SUM(c4 % 2116), SUM(c4 % 2117), SUM(c4 % 2118), SUM(c4 % 2119),
 SUM(c4 % 2120), SUM(c4 % 2121), SUM(c4 % 2122), SUM(c4 % 2123), SUM(c4 % 2124),
 SUM(c4 % 2125), SUM(c4 % 2126), SUM(c4 % 2127), SUM(c4 % 2128), SUM(c4 % 2129),
 SUM(c4 % 2130), SUM(c4 % 2131), SUM(c4 % 2132), SUM(c4 % 2133), SUM(c4 % 2134),
 SUM(c4 % 2135), SUM(c4 % 2136), SUM(c4 % 2137), SUM(c4 % 2138), SUM(c4 % 2139),
 SUM(c4 % 2140), SUM(c4 % 2141), SUM(c4 % 2142), SUM(c4 % 2143), SUM(c4 % 2144),
 SUM(c4 % 2145), SUM(c4 % 2146), SUM(c4 % 2147), SUM(c4 % 2148), SUM(c4 % 2149),
 SUM(c4 % 2150), SUM(c4 % 2151), SUM(c4 % 2152), SUM(c4 % 2153), SUM(c4 % 2154),
 SUM(c4 % 2155), SUM(c4 % 2156), SUM(c4 % 2157), SUM(c4 % 2158), SUM(c4 % 2159),
 SUM(c4 % 2160), SUM(c4 % 2161), SUM(c4 % 2162), SUM(c4 % 2163), SUM(c4 % 2164),
 SUM(c4 % 2165), SUM(c4 % 2166), SUM(c4 % 2167), SUM(c4 % 2168), SUM(c4 % 2169),
 SUM(c4 % 2170), SUM(c4 % 2171), SUM(c4 % 2172), SUM(c4 % 2173), SUM(c4 % 2174),
 SUM(c4 % 2175), SUM(c4 % 2176), SUM(c4 % 2177), SUM(c4 % 2178), SUM(c4 % 2179),
 SUM(c4 % 2180), SUM(c4 % 2181), SUM(c4 % 2182), SUM(c4 % 2183), SUM(c4 % 2184),
 SUM(c4 % 2185), SUM(c4 % 2186), SUM(c4 % 2187), SUM(c4 % 2188), SUM(c4 % 2189),
 SUM(c4 % 2190), SUM(c4 % 2191), SUM(c4 % 2192), SUM(c4 % 2193), SUM(c4 % 2194),
 SUM(c4 % 2195), SUM(c4 % 2196), SUM(c4 % 2197), SUM(c4 % 2198), SUM(c4 % 2199),
 SUM(c4 % 2200), SUM(c4 % 2201), SUM(c4 % 2202), SUM(c4 % 2203), SUM(c4 % 2204),
 SUM(c4 % 2205), SUM(c4 % 2206), SUM(c4 % 2207), SUM(c4 % 2208), SUM(c4 % 2209),
 SUM(c4 % 2210), SUM(c4 % 2211), SUM(c4 % 2212), SUM(c4 % 2213), SUM(c4 % 2214),
 SUM(c4 % 2215), SUM(c4 % 2216), SUM(c4 % 2217), SUM(c4 % 2218), SUM(c4 % 2219),
 SUM(c4 % 2220), SUM(c4 % 2221), SUM(c4 % 2222), SUM(c4 % 2223), SUM(c4 % 2224),
 SUM(c4 % 2225), SUM(c4 % 2226), SUM(c4 % 2227), SUM(c4 % 2228), SUM(c4 % 2229),
 SUM(c4 % 2230), SUM(c4 % 2231), SUM(c4 % 2232), SUM(c4 % 2233), SUM(c4 % 2234),
 SUM(c4 % 2235), SUM(c4 % 2236), SUM(c4 % 2237), SUM(c4 % 2238), SUM(c4 % 2239),
 SUM(c4 % 2240), SUM(c4 % 2241), SUM(c4 % 2242), SUM(c4 % 2243), SUM(c4 % 2244),
 SUM(c4 % 2245), SUM(c4 % 2246), SUM(c4 % 2247), SUM(c4 % 2248), SUM(c4 % 2249),
 SUM(c4 % 2250), SUM(c4 % 2251), SUM(c4 % 2252), SUM(c4 % 2253), SUM(c4 % 2254),
 SUM(c4 % 2255), SUM(c4 % 2256), SUM(c4 % 2257), SUM(c4 % 2258), SUM(c4 % 2259),
 SUM(c4 % 2260), SUM(c4 % 2261), SUM(c4 % 2262), SUM(c4 % 2263), SUM(c4 % 2264),
 SUM(c4 % 2265), SUM(c4 % 2266), SUM(c4 % 2267), SUM(c4 % 2268), SUM(c4 % 2269),
 SUM(c4 % 2270), SUM(c4 % 2271), SUM(c4 % 2272), SUM(c4 % 2273), SUM(c4 % 2274),
 SUM(c4 % 2275), SUM(c4 % 2276), SUM(c4 % 2277), SUM(c4 % 2278), SUM(c4 % 2279),
 SUM(c4 % 2280), SUM(c4 % 2281), SUM(c4 % 2282), SUM(c4 % 2283), SUM(c4 % 2284),
 SUM(c4 % 2285), SUM(c4 % 2286), SUM(c4 % 2287), SUM(c4 % 2288), SUM(c4 % 2289),
 SUM(c4 % 2290), SUM(c4 % 2291), SUM(c4 % 2292), SUM(c4 % 2293), SUM(c4 % 2294),
 SUM(c4 % 2295), SUM(c4 % 2296), SUM(c4 % 2297), SUM(c4 % 2298), SUM(c4 % 2299),
 SUM(c4 % 2300), SUM(c4 % 2301), SUM(c4 % 2302), SUM(c4 % 2303), SUM(c4 % 2304),
 SUM(c4 % 2305), SUM(c4 % 2306), SUM(c4 % 2307), SUM(c4 % 2308), SUM(c4 % 2309),
 SUM(c4 % 2310), SUM(c4 % 2311), SUM(c4 % 2312), SUM(c4 % 2313), SUM(c4 % 2314),
 SUM(c4 % 2315), SUM(c4 % 2316), SUM(c4 % 2317), SUM(c4 % 2318), SUM(c4 % 2319),
 SUM(c4 % 2320), SUM(c4 % 2321), SUM(c4 % 2322), SUM(c4 % 2323), SUM(c4 % 2324),
 SUM(c4 % 2325), SUM(c4 % 2326), SUM(c4 % 2327), SUM(c4 % 2328), SUM(c4 % 2329),
 SUM(c4 % 2330), SUM(c4 % 2331), SUM(c4 % 2332), SUM(c4 % 2333), SUM(c4 % 2334),
 SUM(c4 % 2335), SUM(c4 % 2336), SUM(c4 % 2337), SUM(c4 % 2338), SUM(c4 % 2339),
 SUM(c4 % 2340), SUM(c4 % 2341), SUM(c4 % 2342), SUM(c4 % 2343), SUM(c4 % 2344),
 SUM(c4 % 2345), SUM(c4 % 2346), SUM(c4 % 2347), SUM(c4 % 2348), SUM(c4 % 2349),
 SUM(c4 % 2350), SUM(c4 % 2351), SUM(c4 % 2352), SUM(c4 % 2353), SUM(c4 % 2354),
 SUM(c4 % 2355), SUM(c4 % 2356), SUM(c4 % 2357), SUM(c4 % 2358), SUM(c4 % 2359),
 SUM(c4 % 2360), SUM(c4 % 2361), SUM(c4 % 2362), SUM(c4 % 2363), SUM(c4 % 2364),
 SUM(c4 % 2365), SUM(c4 % 2366), SUM(c4 % 2367), SUM(c4 % 2368), SUM(c4 % 2369),
 SUM(c4 % 2370), SUM(c4 % 2371), SUM(c4 % 2372), SUM(c4 % 2373), SUM(c4 % 2374),
 SUM(c4 % 2375), SUM(c4 % 2376), SUM(c4 % 2377), SUM(c4 % 2378), SUM(c4 % 2379),
 SUM(c4 % 2380), SUM(c4 % 2381), SUM(c4 % 2382), SUM(c4 % 2383), SUM(c4 % 2384),
 SUM(c4 % 2385), SUM(c4 % 2386), SUM(c4 % 2387), SUM(c4 % 2388), SUM(c4 % 2389),
 SUM(c4 % 2390), SUM(c4 % 2391), SUM(c4 % 2392), SUM(c4 % 2393), SUM(c4 % 2394),
 SUM(c4 % 2395), SUM(c4 % 2396), SUM(c4 % 2397), SUM(c4 % 2398), SUM(c4 % 2399),
 SUM(c4 % 2400), SUM(c4 % 2401), SUM(c4 % 2402), SUM(c4 % 2403), SUM(c4 % 2404),
 SUM(c4 % 2405), SUM(c4 % 2406), SUM(c4 % 2407), SUM(c4 % 2408), SUM(c4 % 2409),
 SUM(c4 % 2410), SUM(c4 % 2411), SUM(c4 % 2412), SUM(c4 % 2413), SUM(c4 % 2414),
 SUM(c4 % 2415), SUM(c4 % 2416), SUM(c4 % 2417), SUM(c4 % 2418), SUM(c4 % 2419),
 SUM(c4 % 2420), SUM(c4 % 2421), SUM(c4 % 2422), SUM(c4 % 2423), SUM(c4 % 2424),
 SUM(c4 % 2425), SUM(c4 % 2426), SUM(c4 % 2427), SUM(c4 % 2428), SUM(c4 % 2429),
 SUM(c4 % 2430), SUM(c4 % 2431), SUM(c4 % 2432), SUM(c4 % 2433), SUM(c4 % 2434),
 SUM(c4 % 2435), SUM(c4 % 2436), SUM(c4 % 2437), SUM(c4 % 2438), SUM(c4 % 2439),
 SUM(c4 % 2440), SUM(c4 % 2441), SUM(c4 % 2442), SUM(c4 % 2443), SUM(c4 % 2444),
 SUM(c4 % 2445), SUM(c4 % 2446), SUM(c4 % 2447), SUM(c4 % 2448), SUM(c4 % 2449),
 SUM(c4 % 2450), SUM(c4 % 2451), SUM(c4 % 2452), SUM(c4 % 2453), SUM(c4 % 2454),
 SUM(c4 % 2455), SUM(c4 % 2456), SUM(c4 % 2457), SUM(c4 % 2458), SUM(c4 % 2459),
 SUM(c4 % 2460), SUM(c4 % 2461), SUM(c4 % 2462), SUM(c4 % 2463), SUM(c4 % 2464),
 SUM(c4 % 2465), SUM(c4 % 2466), SUM(c4 % 2467), SUM(c4 % 2468), SUM(c4 % 2469),
 SUM(c4 % 2470), SUM(c4 % 2471), SUM(c4 % 2472), SUM(c4 % 2473), SUM(c4 % 2474),
 SUM(c4 % 2475), SUM(c4 % 2476), SUM(c4 % 2477), SUM(c4 % 2478), SUM(c4 % 2479),
 SUM(c4 % 2480), SUM(c4 % 2481), SUM(c4 % 2482), SUM(c4 % 2483), SUM(c4 % 2484),
 SUM(c4 % 2485), SUM(c4 % 2486), SUM(c4 % 2487), SUM(c4 % 2488), SUM(c4 % 2489),
 SUM(c4 % 2490), SUM(c4 % 2491), SUM(c4 % 2492), SUM(c4 % 2493), SUM(c4 % 2494),
 SUM(c4 % 2495), SUM(c4 % 2496), SUM(c4 % 2497), SUM(c4 % 2498), SUM(c4 % 2499),
 SUM(c4 % 2500), SUM(c4 % 2501), SUM(c4 % 2502), SUM(c4 % 2503), SUM(c4 % 2504),
 SUM(c4 % 2505), SUM(c4 % 2506), SUM(c4 % 2507), SUM(c4 % 2508), SUM(c4 % 2509),
 SUM(c4 % 2510), SUM(c4 % 2511), SUM(c4 % 2512), SUM(c4 % 2513), SUM(c4 % 2514),
 SUM(c4 % 2515), SUM(c4 % 2516), SUM(c4 % 2517), SUM(c4 % 2518), SUM(c4 % 2519),
 SUM(c4 % 2520), SUM(c4 % 2521), SUM(c4 % 2522), SUM(c4 % 2523), SUM(c4 % 2524),
 SUM(c4 % 2525), SUM(c4 % 2526), SUM(c4 % 2527), SUM(c4 % 2528), SUM(c4 % 2529),
 SUM(c4 % 2530), SUM(c4 % 2531), SUM(c4 % 2532), SUM(c4 % 2533), SUM(c4 % 2534),
 SUM(c4 % 2535), SUM(c4 % 2536), SUM(c4 % 2537), SUM(c4 % 2538), SUM(c4 % 2539),
 SUM(c4 % 2540), SUM(c4 % 2541), SUM(c4 % 2542), SUM(c4 % 2543), SUM(c4 % 2544),
 SUM(c4 % 2545), SUM(c4 % 2546), SUM(c4 % 2547), SUM(c4 % 2548), SUM(c4 % 2549),
 SUM(c4 % 2550), SUM(c4 % 2551), SUM(c4 % 2552), SUM(c4 % 2553), SUM(c4 % 2554),
 SUM(c4 % 2555), SUM(c4 % 2556), SUM(c4 % 2557), SUM(c4 % 2558), SUM(c4 % 2559),
 SUM(c4 % 2560), SUM(c4 % 2561), SUM(c4 % 2562), SUM(c4 % 2563), SUM(c4 % 2564),
 SUM(c4 % 2565), SUM(c4 % 2566), SUM(c4 % 2567), SUM(c4 % 2568), SUM(c4 % 2569),
 SUM(c4 % 2570), SUM(c4 % 2571), SUM(c4 % 2572), SUM(c4 % 2573), SUM(c4 % 2574),
 SUM(c4 % 2575), SUM(c4 % 2576), SUM(c4 % 2577), SUM(c4 % 2578), SUM(c4 % 2579),
 SUM(c4 % 2580), SUM(c4 % 2581), SUM(c4 % 2582), SUM(c4 % 2583), SUM(c4 % 2584),
 SUM(c4 % 2585), SUM(c4 % 2586), SUM(c4 % 2587), SUM(c4 % 2588), SUM(c4 % 2589),
 SUM(c4 % 2590), SUM(c4 % 2591), SUM(c4 % 2592), SUM(c4 % 2593), SUM(c4 % 2594),
 SUM(c4 % 2595), SUM(c4 % 2596), SUM(c4 % 2597), SUM(c4 % 2598), SUM(c4 % 2599),
 SUM(c4 % 2600), SUM(c4 % 2601), SUM(c4 % 2602), SUM(c4 % 2603), SUM(c4 % 2604),
 SUM(c4 % 2605), SUM(c4 % 2606), SUM(c4 % 2607), SUM(c4 % 2608), SUM(c4 % 2609),
 SUM(c4 % 2610), SUM(c4 % 2611), SUM(c4 % 2612), SUM(c4 % 2613), SUM(c4 % 2614),
 SUM(c4 % 2615), SUM(c4 % 2616), SUM(c4 % 2617), SUM(c4 % 2618), SUM(c4 % 2619),
 SUM(c4 % 2620), SUM(c4 % 2621), SUM(c4 % 2622), SUM(c4 % 2623), SUM(c4 % 2624),
 SUM(c4 % 2625), SUM(c4 % 2626), SUM(c4 % 2627), SUM(c4 % 2628), SUM(c4 % 2629),
 SUM(c4 % 2630), SUM(c4 % 2631), SUM(c4 % 2632), SUM(c4 % 2633), SUM(c4 % 2634),
 SUM(c4 % 2635), SUM(c4 % 2636), SUM(c4 % 2637), SUM(c4 % 2638), SUM(c4 % 2639),
 SUM(c4 % 2640), SUM(c4 % 2641), SUM(c4 % 2642), SUM(c4 % 2643), SUM(c4 % 2644),
 SUM(c4 % 2645), SUM(c4 % 2646), SUM(c4 % 2647), SUM(c4 % 2648), SUM(c4 % 2649),
 SUM(c4 % 2650), SUM(c4 % 2651), SUM(c4 % 2652), SUM(c4 % 2653), SUM(c4 % 2654),
 SUM(c4 % 2655), SUM(c4 % 2656), SUM(c4 % 2657), SUM(c4 % 2658), SUM(c4 % 2659),
 SUM(c4 % 2660), SUM(c4 % 2661), SUM(c4 % 2662), SUM(c4 % 2663), SUM(c4 % 2664),
 SUM(c4 % 2665), SUM(c4 % 2666), SUM(c4 % 2667), SUM(c4 % 2668), SUM(c4 % 2669),
 SUM(c4 % 2670), SUM(c4 % 2671), SUM(c4 % 2672), SUM(c4 % 2673), SUM(c4 % 2674),
 SUM(c4 % 2675), SUM(c4 % 2676), SUM(c4 % 2677), SUM(c4 % 2678), SUM(c4 % 2679),
 SUM(c4 % 2680), SUM(c4 % 2681), SUM(c4 % 2682), SUM(c4 % 2683), SUM(c4 % 2684),
 SUM(c4 % 2685), SUM(c4 % 2686), SUM(c4 % 2687), SUM(c4 % 2688), SUM(c4 % 2689),
 SUM(c4 % 2690), SUM(c4 % 2691), SUM(c4 % 2692), SUM(c4 % 2693), SUM(c4 % 2694),
 SUM(c4 % 2695), SUM(c4 % 2696), SUM(c4 % 2697), SUM(c4 % 2698), SUM(c4 % 2699),
 SUM(c4 % 2700), SUM(c4 % 2701), SUM(c4 % 2702), SUM(c4 % 2703), SUM(c4 % 2704),
 SUM(c4 % 2705), SUM(c4 % 2706), SUM(c4 % 2707), SUM(c4 % 2708), SUM(c4 % 2709),
 SUM(c4 % 2710), SUM(c4 % 2711), SUM(c4 % 2712), SUM(c4 % 2713), SUM(c4 % 2714),
 SUM(c4 % 2715), SUM(c4 % 2716), SUM(c4 % 2717), SUM(c4 % 2718), SUM(c4 % 2719),
 SUM(c4 % 2720), SUM(c4 % 2721), SUM(c4 % 2722), SUM(c4 % 2723), SUM(c4 % 2724),
 SUM(c4 % 2725), SUM(c4 % 2726), SUM(c4 % 2727), SUM(c4 % 2728), SUM(c4 % 2729),
 SUM(c4 % 2730), SUM(c4 % 2731), SUM(c4 % 2732), SUM(c4 % 2733), SUM(c4 % 2734),
 SUM(c4 % 2735), SUM(c4 % 2736), SUM(c4 % 2737), SUM(c4 % 2738), SUM(c4 % 2739),
 SUM(c4 % 2740), SUM(c4 % 2741), SUM(c4 % 2742), SUM(c4 % 2743), SUM(c4 % 2744),
 SUM(c4 % 2745), SUM(c4 % 2746), SUM(c4 % 2747), SUM(c4 % 2748), SUM(c4 % 2749),
 SUM(c4 % 2750), SUM(c4 % 2751), SUM(c4 % 2752), SUM(c4 % 2753), SUM(c4 % 2754),
 SUM(c4 % 2755), SUM(c4 % 2756), SUM(c4 % 2757), SUM(c4 % 2758), SUM(c4 % 2759),
 SUM(c4 % 2760), SUM(c4 % 2761), SUM(c4 % 2762), SUM(c4 % 2763), SUM(c4 % 2764),
 SUM(c4 % 2765), SUM(c4 % 2766), SUM(c4 % 2767), SUM(c4 % 2768), SUM(c4 % 2769),
 SUM(c4 % 2770), SUM(c4 % 2771), SUM(c4 % 2772), SUM(c4 % 2773), SUM(c4 % 2774),
 SUM(c4 % 2775), SUM(c4 % 2776), SUM(c4 % 2777), SUM(c4 % 2778), SUM(c4 % 2779),
 SUM(c4 % 2780), SUM(c4 % 2781), SUM(c4 % 2782), SUM(c4 % 2783), SUM(c4 % 2784),
 SUM(c4 % 2785), SUM(c4 % 2786), SUM(c4 % 2787), SUM(c4 % 2788), SUM(c4 % 2789),
 SUM(c4 % 2790), SUM(c4 % 2791), SUM(c4 % 2792), SUM(c4 % 2793), SUM(c4 % 2794),
 SUM(c4 % 2795), SUM(c4 % 2796), SUM(c4 % 2797), SUM(c4 % 2798), SUM(c4 % 2799),
 SUM(c4 % 2800), SUM(c4 % 2801), SUM(c4 % 2802), SUM(c4 % 2803), SUM(c4 % 2804),
 SUM(c4 % 2805), SUM(c4 % 2806), SUM(c4 % 2807), SUM(c4 % 2808), SUM(c4 % 2809),
 SUM(c4 % 2810), SUM(c4 % 2811), SUM(c4 % 2812), SUM(c4 % 2813), SUM(c4 % 2814),
 SUM(c4 % 2815), SUM(c4 % 2816), SUM(c4 % 2817), SUM(c4 % 2818), SUM(c4 % 2819),
 SUM(c4 % 2820), SUM(c4 % 2821), SUM(c4 % 2822), SUM(c4 % 2823), SUM(c4 % 2824),
 SUM(c4 % 2825), SUM(c4 % 2826), SUM(c4 % 2827), SUM(c4 % 2828), SUM(c4 % 2829),
 SUM(c4 % 2830), SUM(c4 % 2831), SUM(c4 % 2832), SUM(c4 % 2833), SUM(c4 % 2834),
 SUM(c4 % 2835), SUM(c4 % 2836), SUM(c4 % 2837), SUM(c4 % 2838), SUM(c4 % 2839),
 SUM(c4 % 2840), SUM(c4 % 2841), SUM(c4 % 2842), SUM(c4 % 2843), SUM(c4 % 2844),
 SUM(c4 % 2845), SUM(c4 % 2846), SUM(c4 % 2847), SUM(c4 % 2848), SUM(c4 % 2849),
 SUM(c4 % 2850), SUM(c4 % 2851), SUM(c4 % 2852), SUM(c4 % 2853), SUM(c4 % 2854),
 SUM(c4 % 2855), SUM(c4 % 2856), SUM(c4 % 2857), SUM(c4 % 2858), SUM(c4 % 2859),
 SUM(c4 % 2860), SUM(c4 % 2861), SUM(c4 % 2862), SUM(c4 % 2863), SUM(c4 % 2864),
 SUM(c4 % 2865), SUM(c4 % 2866), SUM(c4 % 2867), SUM(c4 % 2868), SUM(c4 % 2869),
 SUM(c4 % 2870), SUM(c4 % 2871), SUM(c4 % 2872), SUM(c4 % 2873), SUM(c4 % 2874),
 SUM(c4 % 2875), SUM(c4 % 2876), SUM(c4 % 2877), SUM(c4 % 2878), SUM(c4 % 2879),
 SUM(c4 % 2880), SUM(c4 % 2881), SUM(c4 % 2882), SUM(c4 % 2883), SUM(c4 % 2884),
 SUM(c4 % 2885), SUM(c4 % 2886), SUM(c4 % 2887), SUM(c4 % 2888), SUM(c4 % 2889),
 SUM(c4 % 2890), SUM(c4 % 2891), SUM(c4 % 2892), SUM(c4 % 2893), SUM(c4 % 2894),
 SUM(c4 % 2895), SUM(c4 % 2896), SUM(c4 % 2897), SUM(c4 % 2898), SUM(c4 % 2899),
 SUM(c4 % 2900), SUM(c4 % 2901), SUM(c4 % 2902), SUM(c4 % 2903), SUM(c4 % 2904),
 SUM(c4 % 2905), SUM(c4 % 2906), SUM(c4 % 2907), SUM(c4 % 2908), SUM(c4 % 2909),
 SUM(c4 % 2910), SUM(c4 % 2911), SUM(c4 % 2912), SUM(c4 % 2913), SUM(c4 % 2914),
 SUM(c4 % 2915), SUM(c4 % 2916), SUM(c4 % 2917), SUM(c4 % 2918), SUM(c4 % 2919),
 SUM(c4 % 2920), SUM(c4 % 2921), SUM(c4 % 2922), SUM(c4 % 2923), SUM(c4 % 2924),
 SUM(c4 % 2925), SUM(c4 % 2926), SUM(c4 % 2927), SUM(c4 % 2928), SUM(c4 % 2929),
 SUM(c4 % 2930), SUM(c4 % 2931), SUM(c4 % 2932), SUM(c4 % 2933), SUM(c4 % 2934),
 SUM(c4 % 2935), SUM(c4 % 2936), SUM(c4 % 2937), SUM(c4 % 2938), SUM(c4 % 2939),
 SUM(c4 % 2940), SUM(c4 % 2941), SUM(c4 % 2942), SUM(c4 % 2943), SUM(c4 % 2944),
 SUM(c4 % 2945), SUM(c4 % 2946), SUM(c4 % 2947), SUM(c4 % 2948), SUM(c4 % 2949),
 SUM(c4 % 2950), SUM(c4 % 2951), SUM(c4 % 2952), SUM(c4 % 2953), SUM(c4 % 2954),
 SUM(c4 % 2955), SUM(c4 % 2956), SUM(c4 % 2957), SUM(c4 % 2958), SUM(c4 % 2959),
 SUM(c4 % 2960), SUM(c4 % 2961), SUM(c4 % 2962), SUM(c4 % 2963), SUM(c4 % 2964),
 SUM(c4 % 2965), SUM(c4 % 2966), SUM(c4 % 2967), SUM(c4 % 2968), SUM(c4 % 2969),
 SUM(c4 % 2970), SUM(c4 % 2971), SUM(c4 % 2972), SUM(c4 % 2973), SUM(c4 % 2974),
 SUM(c4 % 2975), SUM(c4 % 2976), SUM(c4 % 2977), SUM(c4 % 2978), SUM(c4 % 2979),
 SUM(c4 % 2980), SUM(c4 % 2981), SUM(c4 % 2982), SUM(c4 % 2983), SUM(c4 % 2984),
 SUM(c4 % 2985), SUM(c4 % 2986), SUM(c4 % 2987), SUM(c4 % 2988), SUM(c4 % 2989),
 SUM(c4 % 2990), SUM(c4 % 2991), SUM(c4 % 2992), SUM(c4 % 2993), SUM(c4 % 2994),
 SUM(c4 % 2995), SUM(c4 % 2996), SUM(c4 % 2997), SUM(c4 % 2998), SUM(c4 % 2999),
 SUM(c4 % 3000), SUM(c4 % 3001), SUM(c4 % 3002), SUM(c4 % 3003), SUM(c4 % 3004),
 SUM(c4 % 3005), SUM(c4 % 3006), SUM(c4 % 3007), SUM(c4 % 3008), SUM(c4 % 3009),
 SUM(c4 % 3010), SUM(c4 % 3011), SUM(c4 % 3012), SUM(c4 % 3013), SUM(c4 % 3014),
 SUM(c4 % 3015), SUM(c4 % 3016), SUM(c4 % 3017), SUM(c4 % 3018), SUM(c4 % 3019),
 SUM(c4 % 3020), SUM(c4 % 3021), SUM(c4 % 3022), SUM(c4 % 3023), SUM(c4 % 3024),
 SUM(c4 % 3025), SUM(c4 % 3026), SUM(c4 % 3027), SUM(c4 % 3028), SUM(c4 % 3029),
 SUM(c4 % 3030), SUM(c4 % 3031), SUM(c4 % 3032), SUM(c4 % 3033), SUM(c4 % 3034),
 SUM(c4 % 3035), SUM(c4 % 3036), SUM(c4 % 3037), SUM(c4 % 3038), SUM(c4 % 3039),
 SUM(c4 % 3040), SUM(c4 % 3041), SUM(c4 % 3042), SUM(c4 % 3043), SUM(c4 % 3044),
 SUM(c4 % 3045), SUM(c4 % 3046), SUM(c4 % 3047), SUM(c4 % 3048), SUM(c4 % 3049),
 SUM(c4 % 3050), SUM(c4 % 3051), SUM(c4 % 3052), SUM(c4 % 3053), SUM(c4 % 3054),
 SUM(c4 % 3055), SUM(c4 % 3056), SUM(c4 % 3057), SUM(c4 % 3058), SUM(c4 % 3059),
 SUM(c4 % 3060), SUM(c4 % 3061), SUM(c4 % 3062), SUM(c4 % 3063), SUM(c4 % 3064),
 SUM(c4 % 3065), SUM(c4 % 3066), SUM(c4 % 3067), SUM(c4 % 3068), SUM(c4 % 3069),
 SUM(c4 % 3070), SUM(c4 % 3071), SUM(c4 % 3072), SUM(c4 % 3073), SUM(c4 % 3074),
 SUM(c4 % 3075), SUM(c4 % 3076), SUM(c4 % 3077), SUM(c4 % 3078), SUM(c4 % 3079),
 SUM(c4 % 3080), SUM(c4 % 3081), SUM(c4 % 3082), SUM(c4 % 3083), SUM(c4 % 3084),
 SUM(c4 % 3085), SUM(c4 % 3086), SUM(c4 % 3087), SUM(c4 % 3088), SUM(c4 % 3089),
 SUM(c4 % 3090), SUM(c4 % 3091), SUM(c4 % 3092), SUM(c4 % 3093), SUM(c4 % 3094),
 SUM(c4 % 3095), SUM(c4 % 3096), SUM(c4 % 3097), SUM(c4 % 3098), SUM(c4 % 3099),
 SUM(c4 % 3100), SUM(c4 % 3101), SUM(c4 % 3102), SUM(c4 % 3103), SUM(c4 % 3104),
 SUM(c4 % 3105), SUM(c4 % 3106), SUM(c4 % 3107), SUM(c4 % 3108), SUM(c4 % 3109),
 SUM(c4 % 3110), SUM(c4 % 3111), SUM(c4 % 3112), SUM(c4 % 3113), SUM(c4 % 3114),
 SUM(c4 % 3115), SUM(c4 % 3116), SUM(c4 % 3117), SUM(c4 % 3118), SUM(c4 % 3119),
 SUM(c4 % 3120), SUM(c4 % 3121), SUM(c4 % 3122), SUM(c4 % 3123), SUM(c4 % 3124),
 SUM(c4 % 3125), SUM(c4 % 3126), SUM(c4 % 3127), SUM(c4 % 3128), SUM(c4 % 3129),
 SUM(c4 % 3130), SUM(c4 % 3131), SUM(c4 % 3132), SUM(c4 % 3133), SUM(c4 % 3134),
 SUM(c4 % 3135), SUM(c4 % 3136), SUM(c4 % 3137), SUM(c4 % 3138), SUM(c4 % 3139),
 SUM(c4 % 3140), SUM(c4 % 3141), SUM(c4 % 3142), SUM(c4 % 3143), SUM(c4 % 3144),
 SUM(c4 % 3145), SUM(c4 % 3146), SUM(c4 % 3147), SUM(c4 % 3148), SUM(c4 % 3149),
 SUM(c4 % 3150), SUM(c4 % 3151), SUM(c4 % 3152), SUM(c4 % 3153), SUM(c4 % 3154),
 SUM(c4 % 3155), SUM(c4 % 3156), SUM(c4 % 3157), SUM(c4 % 3158), SUM(c4 % 3159),
 SUM(c4 % 3160), SUM(c4 % 3161), SUM(c4 % 3162), SUM(c4 % 3163), SUM(c4 % 3164),
 SUM(c4 % 3165), SUM(c4 % 3166), SUM(c4 % 3167), SUM(c4 % 3168), SUM(c4 % 3169),
 SUM(c4 % 3170), SUM(c4 % 3171), SUM(c4 % 3172), SUM(c4 % 3173), SUM(c4 % 3174),
 SUM(c4 % 3175), SUM(c4 % 3176), SUM(c4 % 3177), SUM(c4 % 3178), SUM(c4 % 3179),
 SUM(c4 % 3180), SUM(c4 % 3181), SUM(c4 % 3182), SUM(c4 % 3183), SUM(c4 % 3184),
 SUM(c4 % 3185), SUM(c4 % 3186), SUM(c4 % 3187), SUM(c4 % 3188), SUM(c4 % 3189),
 SUM(c4 % 3190), SUM(c4 % 3191), SUM(c4 % 3192), SUM(c4 % 3193), SUM(c4 % 3194),
 SUM(c4 % 3195), SUM(c4 % 3196), SUM(c4 % 3197), SUM(c4 % 3198), SUM(c4 % 3199),
 SUM(c4 % 3200), SUM(c4 % 3201), SUM(c4 % 3202), SUM(c4 % 3203), SUM(c4 % 3204),
 SUM(c4 % 3205), SUM(c4 % 3206), SUM(c4 % 3207), SUM(c4 % 3208), SUM(c4 % 3209),
 SUM(c4 % 3210), SUM(c4 % 3211), SUM(c4 % 3212), SUM(c4 % 3213), SUM(c4 % 3214),
 SUM(c4 % 3215), SUM(c4 % 3216), SUM(c4 % 3217), SUM(c4 % 3218), SUM(c4 % 3219),
 SUM(c4 % 3220), SUM(c4 % 3221), SUM(c4 % 3222), SUM(c4 % 3223), SUM(c4 % 3224),
 SUM(c4 % 3225), SUM(c4 % 3226), SUM(c4 % 3227), SUM(c4 % 3228), SUM(c4 % 3229),
 SUM(c4 % 3230), SUM(c4 % 3231), SUM(c4 % 3232), SUM(c4 % 3233), SUM(c4 % 3234),
 SUM(c4 % 3235), SUM(c4 % 3236), SUM(c4 % 3237), SUM(c4 % 3238), SUM(c4 % 3239),
 SUM(c4 % 3240), SUM(c4 % 3241), SUM(c4 % 3242), SUM(c4 % 3243), SUM(c4 % 3244),
 SUM(c4 % 3245), SUM(c4 % 3246), SUM(c4 % 3247), SUM(c4 % 3248), SUM(c4 % 3249),
 SUM(c4 % 3250), SUM(c4 % 3251), SUM(c4 % 3252), SUM(c4 % 3253), SUM(c4 % 3254),
 SUM(c4 % 3255), SUM(c4 % 3256), SUM(c4 % 3257), SUM(c4 % 3258), SUM(c4 % 3259),
 SUM(c4 % 3260), SUM(c4 % 3261), SUM(c4 % 3262), SUM(c4 % 3263), SUM(c4 % 3264),
 SUM(c4 % 3265), SUM(c4 % 3266), SUM(c4 % 3267), SUM(c4 % 3268), SUM(c4 % 3269),
 SUM(c4 % 3270), SUM(c4 % 3271), SUM(c4 % 3272), SUM(c4 % 3273), SUM(c4 % 3274),
 SUM(c4 % 3275), SUM(c4 % 3276), SUM(c4 % 3277), SUM(c4 % 3278), SUM(c4 % 3279),
 SUM(c4 % 3280), SUM(c4 % 3281), SUM(c4 % 3282), SUM(c4 % 3283), SUM(c4 % 3284),
 SUM(c4 % 3285), SUM(c4 % 3286), SUM(c4 % 3287), SUM(c4 % 3288), SUM(c4 % 3289),
 SUM(c4 % 3290), SUM(c4 % 3291), SUM(c4 % 3292), SUM(c4 % 3293), SUM(c4 % 3294),
 SUM(c4 % 3295), SUM(c4 % 3296), SUM(c4 % 3297), SUM(c4 % 3298), SUM(c4 % 3299),
 SUM(c4 % 3300), SUM(c4 % 3301), SUM(c4 % 3302), SUM(c4 % 3303), SUM(c4 % 3304),
 SUM(c4 % 3305), SUM(c4 % 3306), SUM(c4 % 3307), SUM(c4 % 3308), SUM(c4 % 3309),
 SUM(c4 % 3310), SUM(c4 % 3311), SUM(c4 % 3312), SUM(c4 % 3313), SUM(c4 % 3314),
 SUM(c4 % 3315), SUM(c4 % 3316), SUM(c4 % 3317), SUM(c4 % 3318), SUM(c4 % 3319),
 SUM(c4 % 3320), SUM(c4 % 3321), SUM(c4 % 3322), SUM(c4 % 3323), SUM(c4 % 3324),
 SUM(c4 % 3325), SUM(c4 % 3326), SUM(c4 % 3327), SUM(c4 % 3328), SUM(c4 % 3329),
 SUM(c4 % 3330), SUM(c4 % 3331), SUM(c4 % 3332), SUM(c4 % 3333), SUM(c4 % 3334),
 SUM(c4 % 3335), SUM(c4 % 3336), SUM(c4 % 3337), SUM(c4 % 3338), SUM(c4 % 3339),
 SUM(c4 % 3340), SUM(c4 % 3341), SUM(c4 % 3342), SUM(c4 % 3343), SUM(c4 % 3344),
 SUM(c4 % 3345), SUM(c4 % 3346), SUM(c4 % 3347), SUM(c4 % 3348), SUM(c4 % 3349),
 SUM(c4 % 3350), SUM(c4 % 3351), SUM(c4 % 3352), SUM(c4 % 3353), SUM(c4 % 3354),
 SUM(c4 % 3355), SUM(c4 % 3356), SUM(c4 % 3357), SUM(c4 % 3358), SUM(c4 % 3359),
 SUM(c4 % 3360), SUM(c4 % 3361), SUM(c4 % 3362), SUM(c4 % 3363), SUM(c4 % 3364),
 SUM(c4 % 3365), SUM(c4 % 3366), SUM(c4 % 3367), SUM(c4 % 3368), SUM(c4 % 3369),
 SUM(c4 % 3370), SUM(c4 % 3371), SUM(c4 % 3372), SUM(c4 % 3373), SUM(c4 % 3374),
 SUM(c4 % 3375), SUM(c4 % 3376), SUM(c4 % 3377), SUM(c4 % 3378), SUM(c4 % 3379),
 SUM(c4 % 3380), SUM(c4 % 3381), SUM(c4 % 3382), SUM(c4 % 3383), SUM(c4 % 3384),
 SUM(c4 % 3385), SUM(c4 % 3386), SUM(c4 % 3387), SUM(c4 % 3388), SUM(c4 % 3389),
 SUM(c4 % 3390), SUM(c4 % 3391), SUM(c4 % 3392), SUM(c4 % 3393), SUM(c4 % 3394),
 SUM(c4 % 3395), SUM(c4 % 3396), SUM(c4 % 3397), SUM(c4 % 3398), SUM(c4 % 3399),
 SUM(c4 % 3400), SUM(c4 % 3401), SUM(c4 % 3402), SUM(c4 % 3403), SUM(c4 % 3404),
 SUM(c4 % 3405), SUM(c4 % 3406), SUM(c4 % 3407), SUM(c4 % 3408), SUM(c4 % 3409),
 SUM(c4 % 3410), SUM(c4 % 3411), SUM(c4 % 3412), SUM(c4 % 3413), SUM(c4 % 3414),
 SUM(c4 % 3415), SUM(c4 % 3416), SUM(c4 % 3417), SUM(c4 % 3418), SUM(c4 % 3419),
 SUM(c4 % 3420), SUM(c4 % 3421), SUM(c4 % 3422), SUM(c4 % 3423), SUM(c4 % 3424),
 SUM(c4 % 3425), SUM(c4 % 3426), SUM(c4 % 3427), SUM(c4 % 3428), SUM(c4 % 3429),
 SUM(c4 % 3430), SUM(c4 % 3431), SUM(c4 % 3432), SUM(c4 % 3433), SUM(c4 % 3434),
 SUM(c4 % 3435), SUM(c4 % 3436), SUM(c4 % 3437), SUM(c4 % 3438), SUM(c4 % 3439),
 SUM(c4 % 3440), SUM(c4 % 3441), SUM(c4 % 3442), SUM(c4 % 3443), SUM(c4 % 3444),
 SUM(c4 % 3445), SUM(c4 % 3446), SUM(c4 % 3447), SUM(c4 % 3448), SUM(c4 % 3449),
 SUM(c4 % 3450), SUM(c4 % 3451), SUM(c4 % 3452), SUM(c4 % 3453), SUM(c4 % 3454),
 SUM(c4 % 3455), SUM(c4 % 3456), SUM(c4 % 3457), SUM(c4 % 3458), SUM(c4 % 3459),
 SUM(c4 % 3460), SUM(c4 % 3461), SUM(c4 % 3462), SUM(c4 % 3463), SUM(c4 % 3464),
 SUM(c4 % 3465), SUM(c4 % 3466), SUM(c4 % 3467), SUM(c4 % 3468), SUM(c4 % 3469),
 SUM(c4 % 3470), SUM(c4 % 3471), SUM(c4 % 3472), SUM(c4 % 3473), SUM(c4 % 3474),
 SUM(c4 % 3475), SUM(c4 % 3476), SUM(c4 % 3477), SUM(c4 % 3478), SUM(c4 % 3479),
 SUM(c4 % 3480), SUM(c4 % 3481), SUM(c4 % 3482), SUM(c4 % 3483), SUM(c4 % 3484),
 SUM(c4 % 3485), SUM(c4 % 3486), SUM(c4 % 3487), SUM(c4 % 3488), SUM(c4 % 3489),
 SUM(c4 % 3490), SUM(c4 % 3491), SUM(c4 % 3492), SUM(c4 % 3493), SUM(c4 % 3494),
 SUM(c4 % 3495), SUM(c4 % 3496), SUM(c4 % 3497), SUM(c4 % 3498), SUM(c4 % 3499),
 SUM(c4 % 3500), SUM(c4 % 3501), SUM(c4 % 3502), SUM(c4 % 3503), SUM(c4 % 3504),
 SUM(c4 % 3505), SUM(c4 % 3506), SUM(c4 % 3507), SUM(c4 % 3508), SUM(c4 % 3509),
 SUM(c4 % 3510), SUM(c4 % 3511), SUM(c4 % 3512), SUM(c4 % 3513), SUM(c4 % 3514),
 SUM(c4 % 3515), SUM(c4 % 3516), SUM(c4 % 3517), SUM(c4 % 3518), SUM(c4 % 3519),
 SUM(c4 % 3520), SUM(c4 % 3521), SUM(c4 % 3522), SUM(c4 % 3523), SUM(c4 % 3524),
 SUM(c4 % 3525), SUM(c4 % 3526), SUM(c4 % 3527), SUM(c4 % 3528), SUM(c4 % 3529),
 SUM(c4 % 3530), SUM(c4 % 3531), SUM(c4 % 3532), SUM(c4 % 3533), SUM(c4 % 3534),
 SUM(c4 % 3535), SUM(c4 % 3536), SUM(c4 % 3537), SUM(c4 % 3538), SUM(c4 % 3539),
 SUM(c4 % 3540), SUM(c4 % 3541), SUM(c4 % 3542), SUM(c4 % 3543), SUM(c4 % 3544),
 SUM(c4 % 3545), SUM(c4 % 3546), SUM(c4 % 3547), SUM(c4 % 3548), SUM(c4 % 3549),
 SUM(c4 % 3550), SUM(c4 % 3551), SUM(c4 % 3552), SUM(c4 % 3553), SUM(c4 % 3554),
 SUM(c4 % 3555), SUM(c4 % 3556), SUM(c4 % 3557), SUM(c4 % 3558), SUM(c4 % 3559),
 SUM(c4 % 3560), SUM(c4 % 3561), SUM(c4 % 3562), SUM(c4 % 3563), SUM(c4 % 3564),
 SUM(c4 % 3565), SUM(c4 % 3566), SUM(c4 % 3567), SUM(c4 % 3568), SUM(c4 % 3569),
 SUM(c4 % 3570), SUM(c4 % 3571), SUM(c4 % 3572), SUM(c4 % 3573), SUM(c4 % 3574),
 SUM(c4 % 3575), SUM(c4 % 3576), SUM(c4 % 3577), SUM(c4 % 3578), SUM(c4 % 3579),
 SUM(c4 % 3580), SUM(c4 % 3581), SUM(c4 % 3582), SUM(c4 % 3583), SUM(c4 % 3584),
 SUM(c4 % 3585), SUM(c4 % 3586), SUM(c4 % 3587), SUM(c4 % 3588), SUM(c4 % 3589),
 SUM(c4 % 3590), SUM(c4 % 3591), SUM(c4 % 3592), SUM(c4 % 3593), SUM(c4 % 3594),
 SUM(c4 % 3595), SUM(c4 % 3596), SUM(c4 % 3597), SUM(c4 % 3598), SUM(c4 % 3599),
 SUM(c4 % 3600), SUM(c4 % 3601), SUM(c4 % 3602), SUM(c4 % 3603), SUM(c4 % 3604),
 SUM(c4 % 3605), SUM(c4 % 3606), SUM(c4 % 3607), SUM(c4 % 3608), SUM(c4 % 3609),
 SUM(c4 % 3610), SUM(c4 % 3611), SUM(c4 % 3612), SUM(c4 % 3613), SUM(c4 % 3614),
 SUM(c4 % 3615), SUM(c4 % 3616), SUM(c4 % 3617), SUM(c4 % 3618), SUM(c4 % 3619),
 SUM(c4 % 3620), SUM(c4 % 3621), SUM(c4 % 3622), SUM(c4 % 3623), SUM(c4 % 3624),
 SUM(c4 % 3625), SUM(c4 % 3626), SUM(c4 % 3627), SUM(c4 % 3628), SUM(c4 % 3629),
 SUM(c4 % 3630), SUM(c4 % 3631), SUM(c4 % 3632), SUM(c4 % 3633), SUM(c4 % 3634),
 SUM(c4 % 3635), SUM(c4 % 3636), SUM(c4 % 3637), SUM(c4 % 3638), SUM(c4 % 3639),
 SUM(c4 % 3640), SUM(c4 % 3641), SUM(c4 % 3642), SUM(c4 % 3643), SUM(c4 % 3644),
 SUM(c4 % 3645), SUM(c4 % 3646), SUM(c4 % 3647), SUM(c4 % 3648), SUM(c4 % 3649),
 SUM(c4 % 3650), SUM(c4 % 3651), SUM(c4 % 3652), SUM(c4 % 3653), SUM(c4 % 3654),
 SUM(c4 % 3655), SUM(c4 % 3656), SUM(c4 % 3657), SUM(c4 % 3658), SUM(c4 % 3659),
 SUM(c4 % 3660), SUM(c4 % 3661), SUM(c4 % 3662), SUM(c4 % 3663), SUM(c4 % 3664),
 SUM(c4 % 3665), SUM(c4 % 3666), SUM(c4 % 3667), SUM(c4 % 3668), SUM(c4 % 3669),
 SUM(c4 % 3670), SUM(c4 % 3671), SUM(c4 % 3672), SUM(c4 % 3673), SUM(c4 % 3674),
 SUM(c4 % 3675), SUM(c4 % 3676), SUM(c4 % 3677), SUM(c4 % 3678), SUM(c4 % 3679),
 SUM(c4 % 3680), SUM(c4 % 3681), SUM(c4 % 3682), SUM(c4 % 3683), SUM(c4 % 3684),
 SUM(c4 % 3685), SUM(c4 % 3686), SUM(c4 % 3687), SUM(c4 % 3688), SUM(c4 % 3689),
 SUM(c4 % 3690), SUM(c4 % 3691), SUM(c4 % 3692), SUM(c4 % 3693), SUM(c4 % 3694),
 SUM(c4 % 3695), SUM(c4 % 3696), SUM(c4 % 3697), SUM(c4 % 3698), SUM(c4 % 3699),
 SUM(c4 % 3700), SUM(c4 % 3701), SUM(c4 % 3702), SUM(c4 % 3703), SUM(c4 % 3704),
 SUM(c4 % 3705), SUM(c4 % 3706), SUM(c4 % 3707), SUM(c4 % 3708), SUM(c4 % 3709),
 SUM(c4 % 3710), SUM(c4 % 3711), SUM(c4 % 3712), SUM(c4 % 3713), SUM(c4 % 3714),
 SUM(c4 % 3715), SUM(c4 % 3716), SUM(c4 % 3717), SUM(c4 % 3718), SUM(c4 % 3719),
 SUM(c4 % 3720), SUM(c4 % 3721), SUM(c4 % 3722), SUM(c4 % 3723), SUM(c4 % 3724),
 SUM(c4 % 3725), SUM(c4 % 3726), SUM(c4 % 3727), SUM(c4 % 3728), SUM(c4 % 3729),
 SUM(c4 % 3730), SUM(c4 % 3731), SUM(c4 % 3732), SUM(c4 % 3733), SUM(c4 % 3734),
 SUM(c4 % 3735), SUM(c4 % 3736), SUM(c4 % 3737), SUM(c4 % 3738), SUM(c4 % 3739),
 SUM(c4 % 3740), SUM(c4 % 3741), SUM(c4 % 3742), SUM(c4 % 3743), SUM(c4 % 3744),
 SUM(c4 % 3745), SUM(c4 % 3746), SUM(c4 % 3747), SUM(c4 % 3748), SUM(c4 % 3749),
 SUM(c4 % 3750), SUM(c4 % 3751), SUM(c4 % 3752), SUM(c4 % 3753), SUM(c4 % 3754),
 SUM(c4 % 3755), SUM(c4 % 3756), SUM(c4 % 3757), SUM(c4 % 3758), SUM(c4 % 3759),
 SUM(c4 % 3760), SUM(c4 % 3761), SUM(c4 % 3762), SUM(c4 % 3763), SUM(c4 % 3764),
 SUM(c4 % 3765), SUM(c4 % 3766), SUM(c4 % 3767), SUM(c4 % 3768), SUM(c4 % 3769),
 SUM(c4 % 3770), SUM(c4 % 3771), SUM(c4 % 3772), SUM(c4 % 3773), SUM(c4 % 3774),
 SUM(c4 % 3775), SUM(c4 % 3776), SUM(c4 % 3777), SUM(c4 % 3778), SUM(c4 % 3779),
 SUM(c4 % 3780), SUM(c4 % 3781), SUM(c4 % 3782), SUM(c4 % 3783), SUM(c4 % 3784),
 SUM(c4 % 3785), SUM(c4 % 3786), SUM(c4 % 3787), SUM(c4 % 3788), SUM(c4 % 3789),
 SUM(c4 % 3790), SUM(c4 % 3791), SUM(c4 % 3792), SUM(c4 % 3793), SUM(c4 % 3794),
 SUM(c4 % 3795), SUM(c4 % 3796), SUM(c4 % 3797), SUM(c4 % 3798), SUM(c4 % 3799),
 SUM(c4 % 3800), SUM(c4 % 3801), SUM(c4 % 3802), SUM(c4 % 3803), SUM(c4 % 3804),
 SUM(c4 % 3805), SUM(c4 % 3806), SUM(c4 % 3807), SUM(c4 % 3808), SUM(c4 % 3809),
 SUM(c4 % 3810), SUM(c4 % 3811), SUM(c4 % 3812), SUM(c4 % 3813), SUM(c4 % 3814),
 SUM(c4 % 3815), SUM(c4 % 3816), SUM(c4 % 3817), SUM(c4 % 3818), SUM(c4 % 3819),
 SUM(c4 % 3820), SUM(c4 % 3821), SUM(c4 % 3822), SUM(c4 % 3823), SUM(c4 % 3824),
 SUM(c4 % 3825), SUM(c4 % 3826), SUM(c4 % 3827), SUM(c4 % 3828), SUM(c4 % 3829),
 SUM(c4 % 3830), SUM(c4 % 3831), SUM(c4 % 3832), SUM(c4 % 3833), SUM(c4 % 3834),
 SUM(c4 % 3835), SUM(c4 % 3836), SUM(c4 % 3837), SUM(c4 % 3838), SUM(c4 % 3839),
 SUM(c4 % 3840), SUM(c4 % 3841), SUM(c4 % 3842), SUM(c4 % 3843), SUM(c4 % 3844),
 SUM(c4 % 3845), SUM(c4 % 3846), SUM(c4 % 3847), SUM(c4 % 3848), SUM(c4 % 3849),
 SUM(c4 % 3850), SUM(c4 % 3851), SUM(c4 % 3852), SUM(c4 % 3853), SUM(c4 % 3854),
 SUM(c4 % 3855), SUM(c4 % 3856), SUM(c4 % 3857), SUM(c4 % 3858), SUM(c4 % 3859),
 SUM(c4 % 3860), SUM(c4 % 3861), SUM(c4 % 3862), SUM(c4 % 3863), SUM(c4 % 3864),
 SUM(c4 % 3865), SUM(c4 % 3866), SUM(c4 % 3867), SUM(c4 % 3868), SUM(c4 % 3869),
 SUM(c4 % 3870), SUM(c4 % 3871), SUM(c4 % 3872), SUM(c4 % 3873), SUM(c4 % 3874),
 SUM(c4 % 3875), SUM(c4 % 3876), SUM(c4 % 3877), SUM(c4 % 3878), SUM(c4 % 3879),
 SUM(c4 % 3880), SUM(c4 % 3881), SUM(c4 % 3882), SUM(c4 % 3883), SUM(c4 % 3884),
 SUM(c4 % 3885), SUM(c4 % 3886), SUM(c4 % 3887), SUM(c4 % 3888), SUM(c4 % 3889),
 SUM(c4 % 3890), SUM(c4 % 3891), SUM(c4 % 3892), SUM(c4 % 3893), SUM(c4 % 3894),
 SUM(c4 % 3895), SUM(c4 % 3896), SUM(c4 % 3897), SUM(c4 % 3898), SUM(c4 % 3899),
 SUM(c4 % 3900), SUM(c4 % 3901), SUM(c4 % 3902), SUM(c4 % 3903), SUM(c4 % 3904),
 SUM(c4 % 3905), SUM(c4 % 3906), SUM(c4 % 3907), SUM(c4 % 3908), SUM(c4 % 3909),
 SUM(c4 % 3910), SUM(c4 % 3911), SUM(c4 % 3912), SUM(c4 % 3913), SUM(c4 % 3914),
 SUM(c4 % 3915), SUM(c4 % 3916), SUM(c4 % 3917), SUM(c4 % 3918), SUM(c4 % 3919),
 SUM(c4 % 3920), SUM(c4 % 3921), SUM(c4 % 3922), SUM(c4 % 3923), SUM(c4 % 3924),
 SUM(c4 % 3925), SUM(c4 % 3926), SUM(c4 % 3927), SUM(c4 % 3928), SUM(c4 % 3929),
 SUM(c4 % 3930), SUM(c4 % 3931), SUM(c4 % 3932), SUM(c4 % 3933), SUM(c4 % 3934),
 SUM(c4 % 3935), SUM(c4 % 3936), SUM(c4 % 3937), SUM(c4 % 3938), SUM(c4 % 3939),
 SUM(c4 % 3940), SUM(c4 % 3941), SUM(c4 % 3942), SUM(c4 % 3943), SUM(c4 % 3944),
 SUM(c4 % 3945), SUM(c4 % 3946), SUM(c4 % 3947), SUM(c4 % 3948), SUM(c4 % 3949),
 SUM(c4 % 3950), SUM(c4 % 3951), SUM(c4 % 3952), SUM(c4 % 3953), SUM(c4 % 3954),
 SUM(c4 % 3955), SUM(c4 % 3956), SUM(c4 % 3957), SUM(c4 % 3958), SUM(c4 % 3959),
 SUM(c4 % 3960), SUM(c4 % 3961), SUM(c4 % 3962), SUM(c4 % 3963), SUM(c4 % 3964),
 SUM(c4 % 3965), SUM(c4 % 3966), SUM(c4 % 3967), SUM(c4 % 3968), SUM(c4 % 3969),
 SUM(c4 % 3970), SUM(c4 % 3971), SUM(c4 % 3972), SUM(c4 % 3973), SUM(c4 % 3974),
 SUM(c4 % 3975), SUM(c4 % 3976), SUM(c4 % 3977), SUM(c4 % 3978), SUM(c4 % 3979),
 SUM(c4 % 3980), SUM(c4 % 3981), SUM(c4 % 3982), SUM(c4 % 3983), SUM(c4 % 3984),
 SUM(c4 % 3985), SUM(c4 % 3986), SUM(c4 % 3987), SUM(c4 % 3988), SUM(c4 % 3989),
 SUM(c4 % 3990), SUM(c4 % 3991), SUM(c4 % 3992), SUM(c4 % 3993), SUM(c4 % 3994),
 SUM(c4 % 3995), SUM(c4 % 3996), SUM(c4 % 3997), SUM(c4 % 3998), SUM(c4 % 3999),
 SUM(c4 % 4000), SUM(c4 % 4001), SUM(c4 % 4002), SUM(c4 % 4003), SUM(c4 % 4004),
 SUM(c4 % 4005), SUM(c4 % 4006), SUM(c4 % 4007), SUM(c4 % 4008), SUM(c4 % 4009),
 SUM(c4 % 4010), SUM(c4 % 4011), SUM(c4 % 4012), SUM(c4 % 4013), SUM(c4 % 4014),
 SUM(c4 % 4015), SUM(c4 % 4016), SUM(c4 % 4017), SUM(c4 % 4018), SUM(c4 % 4019),
 SUM(c4 % 4020), SUM(c4 % 4021), SUM(c4 % 4022), SUM(c4 % 4023), SUM(c4 % 4024),
 SUM(c4 % 4025), SUM(c4 % 4026), SUM(c4 % 4027), SUM(c4 % 4028), SUM(c4 % 4029),
 SUM(c4 % 4030), SUM(c4 % 4031), SUM(c4 % 4032), SUM(c4 % 4033), SUM(c4 % 4034),
 SUM(c4 % 4035), SUM(c4 % 4036), SUM(c4 % 4037), SUM(c4 % 4038), SUM(c4 % 4039),
 SUM(c4 % 4040), SUM(c4 % 4041), SUM(c4 % 4042), SUM(c4 % 4043), SUM(c4 % 4044),
 SUM(c4 % 4045), SUM(c4 % 4046), SUM(c4 % 4047), SUM(c4 % 4048), SUM(c4 % 4049),
 SUM(c4 % 4050), SUM(c4 % 4051), SUM(c4 % 4052), SUM(c4 % 4053), SUM(c4 % 4054),
 SUM(c4 % 4055), SUM(c4 % 4056), SUM(c4 % 4057), SUM(c4 % 4058), SUM(c4 % 4059),
 SUM(c4 % 4060), SUM(c4 % 4061), SUM(c4 % 4062), SUM(c4 % 4063), SUM(c4 % 4064),
 SUM(c4 % 4065), SUM(c4 % 4066), SUM(c4 % 4067), SUM(c4 % 4068), SUM(c4 % 4069),
 SUM(c4 % 4070), SUM(c4 % 4071), SUM(c4 % 4072), SUM(c4 % 4073), SUM(c4 % 4074),
 SUM(c4 % 4075), SUM(c4 % 4076), SUM(c4 % 4077), SUM(c4 % 4078), SUM(c4 % 4079),
 SUM(c4 % 4080), SUM(c4 % 4081), SUM(c4 % 4082), SUM(c4 % 4083), SUM(c4 % 4084),
 SUM(c4 % 4085), SUM(c4 % 4086), SUM(c4 % 4087), SUM(c4 % 4088), SUM(c4 % 4089),
 SUM(c4 % 4090), SUM(c4 % 4091), SUM(c4 % 4092), SUM(c4 % 4093), SUM(c4 % 4094),
 SUM(c4 % 4095), SUM(c4 % 4096), SUM(c4 % 4097), SUM(c4 % 4098), SUM(c4 % 4099),
 SUM(c4 % 4100), SUM(c4 % 4101), SUM(c4 % 4102), SUM(c4 % 4103), SUM(c4 % 4104),
 SUM(c4 % 4105), SUM(c4 % 4106), SUM(c4 % 4107), SUM(c4 % 4108), SUM(c4 % 4109),
 SUM(c4 % 4110), SUM(c4 % 4111), SUM(c4 % 4112), SUM(c4 % 4113), SUM(c4 % 4114),
 SUM(c4 % 4115), SUM(c4 % 4116), SUM(c4 % 4117), SUM(c4 % 4118), SUM(c4 % 4119),
 SUM(c4 % 4120), SUM(c4 % 4121), SUM(c4 % 4122), SUM(c4 % 4123), SUM(c4 % 4124),
 SUM(c4 % 4125), SUM(c4 % 4126), SUM(c4 % 4127), SUM(c4 % 4128), SUM(c4 % 4129),
 SUM(c4 % 4130), SUM(c4 % 4131), SUM(c4 % 4132), SUM(c4 % 4133), SUM(c4 % 4134),
 SUM(c4 % 4135), SUM(c4 % 4136), SUM(c4 % 4137), SUM(c4 % 4138), SUM(c4 % 4139),
 SUM(c4 % 4140), SUM(c4 % 4141), SUM(c4 % 4142), SUM(c4 % 4143), SUM(c4 % 4144),
 SUM(c4 % 4145), SUM(c4 % 4146), SUM(c4 % 4147), SUM(c4 % 4148), SUM(c4 % 4149),
 SUM(c4 % 4150), SUM(c4 % 4151), SUM(c4 % 4152), SUM(c4 % 4153), SUM(c4 % 4154),
 SUM(c4 % 4155), SUM(c4 % 4156), SUM(c4 % 4157), SUM(c4 % 4158), SUM(c4 % 4159),
 SUM(c4 % 4160), SUM(c4 % 4161), SUM(c4 % 4162), SUM(c4 % 4163), SUM(c4 % 4164),
 SUM(c4 % 4165), SUM(c4 % 4166), SUM(c4 % 4167), SUM(c4 % 4168), SUM(c4 % 4169),
 SUM(c4 % 4170), SUM(c4 % 4171), SUM(c4 % 4172), SUM(c4 % 4173), SUM(c4 % 4174),
 SUM(c4 % 4175), SUM(c4 % 4176), SUM(c4 % 4177), SUM(c4 % 4178), SUM(c4 % 4179),
 SUM(c4 % 4180), SUM(c4 % 4181), SUM(c4 % 4182), SUM(c4 % 4183), SUM(c4 % 4184),
 SUM(c4 % 4185), SUM(c4 % 4186), SUM(c4 % 4187), SUM(c4 % 4188), SUM(c4 % 4189),
 SUM(c4 % 4190), SUM(c4 % 4191), SUM(c4 % 4192), SUM(c4 % 4193), SUM(c4 % 4194),
 SUM(c4 % 4195), SUM(c4 % 4196), SUM(c4 % 4197), SUM(c4 % 4198), SUM(c4 % 4199),
 SUM(c4 % 4200), SUM(c4 % 4201), SUM(c4 % 4202), SUM(c4 % 4203), SUM(c4 % 4204),
 SUM(c4 % 4205), SUM(c4 % 4206), SUM(c4 % 4207), SUM(c4 % 4208), SUM(c4 % 4209),
 SUM(c4 % 4210), SUM(c4 % 4211), SUM(c4 % 4212), SUM(c4 % 4213), SUM(c4 % 4214),
 SUM(c4 % 4215), SUM(c4 % 4216), SUM(c4 % 4217), SUM(c4 % 4218), SUM(c4 % 4219),
 SUM(c4 % 4220), SUM(c4 % 4221), SUM(c4 % 4222), SUM(c4 % 4223), SUM(c4 % 4224),
 SUM(c4 % 4225), SUM(c4 % 4226), SUM(c4 % 4227), SUM(c4 % 4228), SUM(c4 % 4229),
 SUM(c4 % 4230), SUM(c4 % 4231), SUM(c4 % 4232), SUM(c4 % 4233), SUM(c4 % 4234),
 SUM(c4 % 4235), SUM(c4 % 4236), SUM(c4 % 4237), SUM(c4 % 4238), SUM(c4 % 4239),
 SUM(c4 % 4240), SUM(c4 % 4241), SUM(c4 % 4242), SUM(c4 % 4243), SUM(c4 % 4244),
 SUM(c4 % 4245), SUM(c4 % 4246), SUM(c4 % 4247), SUM(c4 % 4248), SUM(c4 % 4249),
 SUM(c4 % 4250), SUM(c4 % 4251), SUM(c4 % 4252), SUM(c4 % 4253), SUM(c4 % 4254),
 SUM(c4 % 4255), SUM(c4 % 4256), SUM(c4 % 4257), SUM(c4 % 4258), SUM(c4 % 4259),
 SUM(c4 % 4260), SUM(c4 % 4261), SUM(c4 % 4262), SUM(c4 % 4263), SUM(c4 % 4264),
 SUM(c4 % 4265), SUM(c4 % 4266), SUM(c4 % 4267), SUM(c4 % 4268), SUM(c4 % 4269),
 SUM(c4 % 4270), SUM(c4 % 4271), SUM(c4 % 4272), SUM(c4 % 4273), SUM(c4 % 4274),
 SUM(c4 % 4275), SUM(c4 % 4276), SUM(c4 % 4277), SUM(c4 % 4278), SUM(c4 % 4279),
 SUM(c4 % 4280), SUM(c4 % 4281), SUM(c4 % 4282), SUM(c4 % 4283), SUM(c4 % 4284),
 SUM(c4 % 4285), SUM(c4 % 4286), SUM(c4 % 4287), SUM(c4 % 4288), SUM(c4 % 4289),
 SUM(c4 % 4290), SUM(c4 % 4291), SUM(c4 % 4292), SUM(c4 % 4293), SUM(c4 % 4294),
 SUM(c4 % 4295), SUM(c4 % 4296), SUM(c4 % 4297), SUM(c4 % 4298), SUM(c4 % 4299),
 SUM(c4 % 4300), SUM(c4 % 4301), SUM(c4 % 4302), SUM(c4 % 4303), SUM(c4 % 4304),
 SUM(c4 % 4305), SUM(c4 % 4306), SUM(c4 % 4307), SUM(c4 % 4308), SUM(c4 % 4309),
 SUM(c4 % 4310), SUM(c4 % 4311), SUM(c4 % 4312), SUM(c4 % 4313), SUM(c4 % 4314),
 SUM(c4 % 4315), SUM(c4 % 4316), SUM(c4 % 4317), SUM(c4 % 4318), SUM(c4 % 4319),
 SUM(c4 % 4320), SUM(c4 % 4321), SUM(c4 % 4322), SUM(c4 % 4323), SUM(c4 % 4324),
 SUM(c4 % 4325), SUM(c4 % 4326), SUM(c4 % 4327), SUM(c4 % 4328), SUM(c4 % 4329),
 SUM(c4 % 4330), SUM(c4 % 4331), SUM(c4 % 4332), SUM(c4 % 4333), SUM(c4 % 4334),
 SUM(c4 % 4335), SUM(c4 % 4336), SUM(c4 % 4337), SUM(c4 % 4338), SUM(c4 % 4339),
 SUM(c4 % 4340), SUM(c4 % 4341), SUM(c4 % 4342), SUM(c4 % 4343), SUM(c4 % 4344),
 SUM(c4 % 4345), SUM(c4 % 4346), SUM(c4 % 4347), SUM(c4 % 4348), SUM(c4 % 4349),
 SUM(c4 % 4350), SUM(c4 % 4351), SUM(c4 % 4352), SUM(c4 % 4353), SUM(c4 % 4354),
 SUM(c4 % 4355), SUM(c4 % 4356), SUM(c4 % 4357), SUM(c4 % 4358), SUM(c4 % 4359),
 SUM(c4 % 4360), SUM(c4 % 4361), SUM(c4 % 4362), SUM(c4 % 4363), SUM(c4 % 4364),
 SUM(c4 % 4365), SUM(c4 % 4366), SUM(c4 % 4367), SUM(c4 % 4368), SUM(c4 % 4369),
 SUM(c4 % 4370), SUM(c4 % 4371), SUM(c4 % 4372), SUM(c4 % 4373), SUM(c4 % 4374),
 SUM(c4 % 4375), SUM(c4 % 4376), SUM(c4 % 4377), SUM(c4 % 4378), SUM(c4 % 4379),
 SUM(c4 % 4380), SUM(c4 % 4381), SUM(c4 % 4382), SUM(c4 % 4383), SUM(c4 % 4384),
 SUM(c4 % 4385), SUM(c4 % 4386), SUM(c4 % 4387), SUM(c4 % 4388), SUM(c4 % 4389),
 SUM(c4 % 4390), SUM(c4 % 4391), SUM(c4 % 4392), SUM(c4 % 4393), SUM(c4 % 4394),
 SUM(c4 % 4395), SUM(c4 % 4396), SUM(c4 % 4397), SUM(c4 % 4398), SUM(c4 % 4399),
 SUM(c4 % 4400), SUM(c4 % 4401), SUM(c4 % 4402), SUM(c4 % 4403), SUM(c4 % 4404),
 SUM(c4 % 4405), SUM(c4 % 4406), SUM(c4 % 4407), SUM(c4 % 4408), SUM(c4 % 4409),
 SUM(c4 % 4410), SUM(c4 % 4411), SUM(c4 % 4412), SUM(c4 % 4413), SUM(c4 % 4414),
 SUM(c4 % 4415), SUM(c4 % 4416), SUM(c4 % 4417), SUM(c4 % 4418), SUM(c4 % 4419),
 SUM(c4 % 4420), SUM(c4 % 4421), SUM(c4 % 4422), SUM(c4 % 4423), SUM(c4 % 4424),
 SUM(c4 % 4425), SUM(c4 % 4426), SUM(c4 % 4427), SUM(c4 % 4428), SUM(c4 % 4429),
 SUM(c4 % 4430), SUM(c4 % 4431), SUM(c4 % 4432), SUM(c4 % 4433), SUM(c4 % 4434),
 SUM(c4 % 4435), SUM(c4 % 4436), SUM(c4 % 4437), SUM(c4 % 4438), SUM(c4 % 4439),
 SUM(c4 % 4440), SUM(c4 % 4441), SUM(c4 % 4442), SUM(c4 % 4443), SUM(c4 % 4444),
 SUM(c4 % 4445), SUM(c4 % 4446), SUM(c4 % 4447), SUM(c4 % 4448), SUM(c4 % 4449),
 SUM(c4 % 4450), SUM(c4 % 4451), SUM(c4 % 4452), SUM(c4 % 4453), SUM(c4 % 4454),
 SUM(c4 % 4455), SUM(c4 % 4456), SUM(c4 % 4457), SUM(c4 % 4458), SUM(c4 % 4459),
 SUM(c4 % 4460), SUM(c4 % 4461), SUM(c4 % 4462), SUM(c4 % 4463), SUM(c4 % 4464),
 SUM(c4 % 4465), SUM(c4 % 4466), SUM(c4 % 4467), SUM(c4 % 4468), SUM(c4 % 4469),
 SUM(c4 % 4470), SUM(c4 % 4471), SUM(c4 % 4472), SUM(c4 % 4473), SUM(c4 % 4474),
 SUM(c4 % 4475), SUM(c4 % 4476), SUM(c4 % 4477), SUM(c4 % 4478), SUM(c4 % 4479),
 SUM(c4 % 4480), SUM(c4 % 4481), SUM(c4 % 4482), SUM(c4 % 4483), SUM(c4 % 4484),
 SUM(c4 % 4485), SUM(c4 % 4486), SUM(c4 % 4487), SUM(c4 % 4488), SUM(c4 % 4489),
 SUM(c4 % 4490), SUM(c4 % 4491), SUM(c4 % 4492), SUM(c4 % 4493), SUM(c4 % 4494),
 SUM(c4 % 4495), SUM(c4 % 4496), SUM(c4 % 4497), SUM(c4 % 4498), SUM(c4 % 4499),
 SUM(c4 % 4500), SUM(c4 % 4501), SUM(c4 % 4502), SUM(c4 % 4503), SUM(c4 % 4504),
 SUM(c4 % 4505), SUM(c4 % 4506), SUM(c4 % 4507), SUM(c4 % 4508), SUM(c4 % 4509),
 SUM(c4 % 4510), SUM(c4 % 4511), SUM(c4 % 4512), SUM(c4 % 4513), SUM(c4 % 4514),
 SUM(c4 % 4515), SUM(c4 % 4516), SUM(c4 % 4517), SUM(c4 % 4518), SUM(c4 % 4519),
 SUM(c4 % 4520), SUM(c4 % 4521), SUM(c4 % 4522), SUM(c4 % 4523), SUM(c4 % 4524),
 SUM(c4 % 4525), SUM(c4 % 4526), SUM(c4 % 4527), SUM(c4 % 4528), SUM(c4 % 4529),
 SUM(c4 % 4530), SUM(c4 % 4531), SUM(c4 % 4532), SUM(c4 % 4533), SUM(c4 % 4534),
 SUM(c4 % 4535), SUM(c4 % 4536), SUM(c4 % 4537), SUM(c4 % 4538), SUM(c4 % 4539),
 SUM(c4 % 4540), SUM(c4 % 4541), SUM(c4 % 4542), SUM(c4 % 4543), SUM(c4 % 4544),
 SUM(c4 % 4545), SUM(c4 % 4546), SUM(c4 % 4547), SUM(c4 % 4548), SUM(c4 % 4549),
 SUM(c4 % 4550), SUM(c4 % 4551), SUM(c4 % 4552), SUM(c4 % 4553), SUM(c4 % 4554),
 SUM(c4 % 4555), SUM(c4 % 4556), SUM(c4 % 4557), SUM(c4 % 4558), SUM(c4 % 4559),
 SUM(c4 % 4560), SUM(c4 % 4561), SUM(c4 % 4562), SUM(c4 % 4563), SUM(c4 % 4564),
 SUM(c4 % 4565), SUM(c4 % 4566), SUM(c4 % 4567), SUM(c4 % 4568), SUM(c4 % 4569),
 SUM(c4 % 4570), SUM(c4 % 4571), SUM(c4 % 4572), SUM(c4 % 4573), SUM(c4 % 4574),
 SUM(c4 % 4575), SUM(c4 % 4576), SUM(c4 % 4577), SUM(c4 % 4578), SUM(c4 % 4579),
 SUM(c4 % 4580), SUM(c4 % 4581), SUM(c4 % 4582), SUM(c4 % 4583), SUM(c4 % 4584),
 SUM(c4 % 4585), SUM(c4 % 4586), SUM(c4 % 4587), SUM(c4 % 4588), SUM(c4 % 4589),
 SUM(c4 % 4590), SUM(c4 % 4591), SUM(c4 % 4592), SUM(c4 % 4593), SUM(c4 % 4594),
 SUM(c4 % 4595), SUM(c4 % 4596), SUM(c4 % 4597), SUM(c4 % 4598), SUM(c4 % 4599),
 SUM(c4 % 4600), SUM(c4 % 4601), SUM(c4 % 4602), SUM(c4 % 4603), SUM(c4 % 4604),
 SUM(c4 % 4605), SUM(c4 % 4606), SUM(c4 % 4607), SUM(c4 % 4608), SUM(c4 % 4609),
 SUM(c4 % 4610), SUM(c4 % 4611), SUM(c4 % 4612), SUM(c4 % 4613), SUM(c4 % 4614),
 SUM(c4 % 4615), SUM(c4 % 4616), SUM(c4 % 4617), SUM(c4 % 4618), SUM(c4 % 4619),
 SUM(c4 % 4620), SUM(c4 % 4621), SUM(c4 % 4622), SUM(c4 % 4623), SUM(c4 % 4624),
 SUM(c4 % 4625), SUM(c4 % 4626), SUM(c4 % 4627), SUM(c4 % 4628), SUM(c4 % 4629),
 SUM(c4 % 4630), SUM(c4 % 4631), SUM(c4 % 4632), SUM(c4 % 4633), SUM(c4 % 4634),
 SUM(c4 % 4635), SUM(c4 % 4636), SUM(c4 % 4637), SUM(c4 % 4638), SUM(c4 % 4639),
 SUM(c4 % 4640), SUM(c4 % 4641), SUM(c4 % 4642), SUM(c4 % 4643), SUM(c4 % 4644),
 SUM(c4 % 4645), SUM(c4 % 4646), SUM(c4 % 4647), SUM(c4 % 4648), SUM(c4 % 4649),
 SUM(c4 % 4650), SUM(c4 % 4651), SUM(c4 % 4652), SUM(c4 % 4653), SUM(c4 % 4654),
 SUM(c4 % 4655), SUM(c4 % 4656), SUM(c4 % 4657), SUM(c4 % 4658), SUM(c4 % 4659),
 SUM(c4 % 4660), SUM(c4 % 4661), SUM(c4 % 4662), SUM(c4 % 4663), SUM(c4 % 4664),
 SUM(c4 % 4665), SUM(c4 % 4666), SUM(c4 % 4667), SUM(c4 % 4668), SUM(c4 % 4669),
 SUM(c4 % 4670), SUM(c4 % 4671), SUM(c4 % 4672), SUM(c4 % 4673), SUM(c4 % 4674),
 SUM(c4 % 4675), SUM(c4 % 4676), SUM(c4 % 4677), SUM(c4 % 4678), SUM(c4 % 4679),
 SUM(c4 % 4680), SUM(c4 % 4681), SUM(c4 % 4682), SUM(c4 % 4683), SUM(c4 % 4684),
 SUM(c4 % 4685), SUM(c4 % 4686), SUM(c4 % 4687), SUM(c4 % 4688), SUM(c4 % 4689),
 SUM(c4 % 4690), SUM(c4 % 4691), SUM(c4 % 4692), SUM(c4 % 4693), SUM(c4 % 4694),
 SUM(c4 % 4695), SUM(c4 % 4696), SUM(c4 % 4697), SUM(c4 % 4698), SUM(c4 % 4699),
 SUM(c4 % 4700), SUM(c4 % 4701), SUM(c4 % 4702), SUM(c4 % 4703), SUM(c4 % 4704),
 SUM(c4 % 4705), SUM(c4 % 4706), SUM(c4 % 4707), SUM(c4 % 4708), SUM(c4 % 4709),
 SUM(c4 % 4710), SUM(c4 % 4711), SUM(c4 % 4712), SUM(c4 % 4713), SUM(c4 % 4714),
 SUM(c4 % 4715), SUM(c4 % 4716), SUM(c4 % 4717), SUM(c4 % 4718), SUM(c4 % 4719),
 SUM(c4 % 4720), SUM(c4 % 4721), SUM(c4 % 4722), SUM(c4 % 4723), SUM(c4 % 4724),
 SUM(c4 % 4725), SUM(c4 % 4726), SUM(c4 % 4727), SUM(c4 % 4728), SUM(c4 % 4729),
 SUM(c4 % 4730), SUM(c4 % 4731), SUM(c4 % 4732), SUM(c4 % 4733), SUM(c4 % 4734),
 SUM(c4 % 4735), SUM(c4 % 4736), SUM(c4 % 4737), SUM(c4 % 4738), SUM(c4 % 4739),
 SUM(c4 % 4740), SUM(c4 % 4741), SUM(c4 % 4742), SUM(c4 % 4743), SUM(c4 % 4744),
 SUM(c4 % 4745), SUM(c4 % 4746), SUM(c4 % 4747), SUM(c4 % 4748), SUM(c4 % 4749),
 SUM(c4 % 4750), SUM(c4 % 4751), SUM(c4 % 4752), SUM(c4 % 4753), SUM(c4 % 4754),
 SUM(c4 % 4755), SUM(c4 % 4756), SUM(c4 % 4757), SUM(c4 % 4758), SUM(c4 % 4759),
 SUM(c4 % 4760), SUM(c4 % 4761), SUM(c4 % 4762), SUM(c4 % 4763), SUM(c4 % 4764),
 SUM(c4 % 4765), SUM(c4 % 4766), SUM(c4 % 4767), SUM(c4 % 4768), SUM(c4 % 4769),
 SUM(c4 % 4770), SUM(c4 % 4771), SUM(c4 % 4772), SUM(c4 % 4773), SUM(c4 % 4774),
 SUM(c4 % 4775), SUM(c4 % 4776), SUM(c4 % 4777), SUM(c4 % 4778), SUM(c4 % 4779),
 SUM(c4 % 4780), SUM(c4 % 4781), SUM(c4 % 4782), SUM(c4 % 4783), SUM(c4 % 4784),
 SUM(c4 % 4785), SUM(c4 % 4786), SUM(c4 % 4787), SUM(c4 % 4788), SUM(c4 % 4789),
 SUM(c4 % 4790), SUM(c4 % 4791), SUM(c4 % 4792), SUM(c4 % 4793), SUM(c4 % 4794),
 SUM(c4 % 4795), SUM(c4 % 4796), SUM(c4 % 4797), SUM(c4 % 4798), SUM(c4 % 4799),
 SUM(c4 % 4800), SUM(c4 % 4801), SUM(c4 % 4802), SUM(c4 % 4803), SUM(c4 % 4804),
 SUM(c4 % 4805), SUM(c4 % 4806), SUM(c4 % 4807), SUM(c4 % 4808), SUM(c4 % 4809),
 SUM(c4 % 4810), SUM(c4 % 4811), SUM(c4 % 4812), SUM(c4 % 4813), SUM(c4 % 4814),
 SUM(c4 % 4815), SUM(c4 % 4816), SUM(c4 % 4817), SUM(c4 % 4818), SUM(c4 % 4819),
 SUM(c4 % 4820), SUM(c4 % 4821), SUM(c4 % 4822), SUM(c4 % 4823), SUM(c4 % 4824),
 SUM(c4 % 4825), SUM(c4 % 4826), SUM(c4 % 4827), SUM(c4 % 4828), SUM(c4 % 4829),
 SUM(c4 % 4830), SUM(c4 % 4831), SUM(c4 % 4832), SUM(c4 % 4833), SUM(c4 % 4834),
 SUM(c4 % 4835), SUM(c4 % 4836), SUM(c4 % 4837), SUM(c4 % 4838), SUM(c4 % 4839),
 SUM(c4 % 4840), SUM(c4 % 4841), SUM(c4 % 4842), SUM(c4 % 4843), SUM(c4 % 4844),
 SUM(c4 % 4845), SUM(c4 % 4846), SUM(c4 % 4847), SUM(c4 % 4848), SUM(c4 % 4849),
 SUM(c4 % 4850), SUM(c4 % 4851), SUM(c4 % 4852), SUM(c4 % 4853), SUM(c4 % 4854),
 SUM(c4 % 4855), SUM(c4 % 4856), SUM(c4 % 4857), SUM(c4 % 4858), SUM(c4 % 4859),
 SUM(c4 % 4860), SUM(c4 % 4861), SUM(c4 % 4862), SUM(c4 % 4863), SUM(c4 % 4864),
 SUM(c4 % 4865), SUM(c4 % 4866), SUM(c4 % 4867), SUM(c4 % 4868), SUM(c4 % 4869),
 SUM(c4 % 4870), SUM(c4 % 4871), SUM(c4 % 4872), SUM(c4 % 4873), SUM(c4 % 4874),
 SUM(c4 % 4875), SUM(c4 % 4876), SUM(c4 % 4877), SUM(c4 % 4878), SUM(c4 % 4879),
 SUM(c4 % 4880), SUM(c4 % 4881), SUM(c4 % 4882), SUM(c4 % 4883), SUM(c4 % 4884),
 SUM(c4 % 4885), SUM(c4 % 4886), SUM(c4 % 4887), SUM(c4 % 4888), SUM(c4 % 4889),
 SUM(c4 % 4890), SUM(c4 % 4891), SUM(c4 % 4892), SUM(c4 % 4893), SUM(c4 % 4894),
 SUM(c4 % 4895), SUM(c4 % 4896), SUM(c4 % 4897), SUM(c4 % 4898), SUM(c4 % 4899),
 SUM(c4 % 4900), SUM(c4 % 4901), SUM(c4 % 4902), SUM(c4 % 4903), SUM(c4 % 4904),
 SUM(c4 % 4905), SUM(c4 % 4906), SUM(c4 % 4907), SUM(c4 % 4908), SUM(c4 % 4909),
 SUM(c4 % 4910), SUM(c4 % 4911), SUM(c4 % 4912), SUM(c4 % 4913), SUM(c4 % 4914),
 SUM(c4 % 4915), SUM(c4 % 4916), SUM(c4 % 4917), SUM(c4 % 4918), SUM(c4 % 4919),
 SUM(c4 % 4920), SUM(c4 % 4921), SUM(c4 % 4922), SUM(c4 % 4923), SUM(c4 % 4924),
 SUM(c4 % 4925), SUM(c4 % 4926), SUM(c4 % 4927), SUM(c4 % 4928), SUM(c4 % 4929),
 SUM(c4 % 4930), SUM(c4 % 4931), SUM(c4 % 4932), SUM(c4 % 4933), SUM(c4 % 4934),
 SUM(c4 % 4935), SUM(c4 % 4936), SUM(c4 % 4937), SUM(c4 % 4938), SUM(c4 % 4939),
 SUM(c4 % 4940), SUM(c4 % 4941), SUM(c4 % 4942), SUM(c4 % 4943), SUM(c4 % 4944),
 SUM(c4 % 4945), SUM(c4 % 4946), SUM(c4 % 4947), SUM(c4 % 4948), SUM(c4 % 4949),
 SUM(c4 % 4950), SUM(c4 % 4951), SUM(c4 % 4952), SUM(c4 % 4953), SUM(c4 % 4954),
 SUM(c4 % 4955), SUM(c4 % 4956), SUM(c4 % 4957), SUM(c4 % 4958), SUM(c4 % 4959),
 SUM(c4 % 4960), SUM(c4 % 4961), SUM(c4 % 4962), SUM(c4 % 4963), SUM(c4 % 4964),
 SUM(c4 % 4965), SUM(c4 % 4966), SUM(c4 % 4967), SUM(c4 % 4968), SUM(c4 % 4969),
 SUM(c4 % 4970), SUM(c4 % 4971), SUM(c4 % 4972), SUM(c4 % 4973), SUM(c4 % 4974),
 SUM(c4 % 4975), SUM(c4 % 4976), SUM(c4 % 4977), SUM(c4 % 4978), SUM(c4 % 4979),
 SUM(c4 % 4980), SUM(c4 % 4981), SUM(c4 % 4982), SUM(c4 % 4983), SUM(c4 % 4984),
 SUM(c4 % 4985), SUM(c4 % 4986), SUM(c4 % 4987), SUM(c4 % 4988), SUM(c4 % 4989),
 SUM(c4 % 4990), SUM(c4 % 4991), SUM(c4 % 4992), SUM(c4 % 4993), SUM(c4 % 4994),
 SUM(c4 % 4995), SUM(c4 % 4996), SUM(c4 % 4997), SUM(c4 % 4998), SUM(c4 % 4999),
 SUM(c4 % 5000), SUM(c4 % 5001), SUM(c4 % 5002), SUM(c4 % 5003), SUM(c4 % 5004),
 SUM(c4 % 5005), SUM(c4 % 5006), SUM(c4 % 5007), SUM(c4 % 5008), SUM(c4 % 5009),
 SUM(c4 % 5010), SUM(c4 % 5011), SUM(c4 % 5012), SUM(c4 % 5013), SUM(c4 % 5014),
 SUM(c4 % 5015), SUM(c4 % 5016), SUM(c4 % 5017), SUM(c4 % 5018), SUM(c4 % 5019),
 SUM(c4 % 5020), SUM(c4 % 5021), SUM(c4 % 5022), SUM(c4 % 5023), SUM(c4 % 5024),
 SUM(c4 % 5025), SUM(c4 % 5026), SUM(c4 % 5027), SUM(c4 % 5028), SUM(c4 % 5029),
 SUM(c4 % 5030), SUM(c4 % 5031), SUM(c4 % 5032), SUM(c4 % 5033), SUM(c4 % 5034),
 SUM(c4 % 5035), SUM(c4 % 5036), SUM(c4 % 5037), SUM(c4 % 5038), SUM(c4 % 5039),
 SUM(c4 % 5040), SUM(c4 % 5041), SUM(c4 % 5042), SUM(c4 % 5043), SUM(c4 % 5044),
 SUM(c4 % 5045), SUM(c4 % 5046), SUM(c4 % 5047), SUM(c4 % 5048), SUM(c4 % 5049),
 SUM(c4 % 5050), SUM(c4 % 5051), SUM(c4 % 5052), SUM(c4 % 5053), SUM(c4 % 5054),
 SUM(c4 % 5055), SUM(c4 % 5056), SUM(c4 % 5057), SUM(c4 % 5058), SUM(c4 % 5059),
 SUM(c4 % 5060), SUM(c4 % 5061), SUM(c4 % 5062), SUM(c4 % 5063), SUM(c4 % 5064),
 SUM(c4 % 5065), SUM(c4 % 5066), SUM(c4 % 5067), SUM(c4 % 5068), SUM(c4 % 5069),
 SUM(c4 % 5070), SUM(c4 % 5071), SUM(c4 % 5072), SUM(c4 % 5073), SUM(c4 % 5074),
 SUM(c4 % 5075), SUM(c4 % 5076), SUM(c4 % 5077), SUM(c4 % 5078), SUM(c4 % 5079),
 SUM(c4 % 5080), SUM(c4 % 5081), SUM(c4 % 5082), SUM(c4 % 5083), SUM(c4 % 5084),
 SUM(c4 % 5085), SUM(c4 % 5086), SUM(c4 % 5087), SUM(c4 % 5088), SUM(c4 % 5089),
 SUM(c4 % 5090), SUM(c4 % 5091), SUM(c4 % 5092), SUM(c4 % 5093), SUM(c4 % 5094),
 SUM(c4 % 5095), SUM(c4 % 5096), SUM(c4 % 5097), SUM(c4 % 5098), SUM(c4 % 5099),
 SUM(c4 % 5100), SUM(c4 % 5101), SUM(c4 % 5102), SUM(c4 % 5103), SUM(c4 % 5104),
 SUM(c4 % 5105), SUM(c4 % 5106), SUM(c4 % 5107), SUM(c4 % 5108), SUM(c4 % 5109),
 SUM(c4 % 5110), SUM(c4 % 5111), SUM(c4 % 5112), SUM(c4 % 5113), SUM(c4 % 5114),
 SUM(c4 % 5115), SUM(c4 % 5116), SUM(c4 % 5117), SUM(c4 % 5118), SUM(c4 % 5119),
 SUM(c4 % 5120), SUM(c4 % 5121), SUM(c4 % 5122), SUM(c4 % 5123), SUM(c4 % 5124),
 SUM(c4 % 5125), SUM(c4 % 5126), SUM(c4 % 5127), SUM(c4 % 5128), SUM(c4 % 5129),
 SUM(c4 % 5130), SUM(c4 % 5131), SUM(c4 % 5132), SUM(c4 % 5133), SUM(c4 % 5134),
 SUM(c4 % 5135), SUM(c4 % 5136), SUM(c4 % 5137), SUM(c4 % 5138), SUM(c4 % 5139),
 SUM(c4 % 5140), SUM(c4 % 5141), SUM(c4 % 5142), SUM(c4 % 5143), SUM(c4 % 5144),
 SUM(c4 % 5145), SUM(c4 % 5146), SUM(c4 % 5147), SUM(c4 % 5148), SUM(c4 % 5149),
 SUM(c4 % 5150), SUM(c4 % 5151), SUM(c4 % 5152), SUM(c4 % 5153), SUM(c4 % 5154),
 SUM(c4 % 5155), SUM(c4 % 5156), SUM(c4 % 5157), SUM(c4 % 5158), SUM(c4 % 5159),
 SUM(c4 % 5160), SUM(c4 % 5161), SUM(c4 % 5162), SUM(c4 % 5163), SUM(c4 % 5164),
 SUM(c4 % 5165), SUM(c4 % 5166), SUM(c4 % 5167), SUM(c4 % 5168), SUM(c4 % 5169),
 SUM(c4 % 5170), SUM(c4 % 5171), SUM(c4 % 5172), SUM(c4 % 5173), SUM(c4 % 5174),
 SUM(c4 % 5175), SUM(c4 % 5176), SUM(c4 % 5177), SUM(c4 % 5178), SUM(c4 % 5179),
 SUM(c4 % 5180), SUM(c4 % 5181), SUM(c4 % 5182), SUM(c4 % 5183), SUM(c4 % 5184),
 SUM(c4 % 5185), SUM(c4 % 5186), SUM(c4 % 5187), SUM(c4 % 5188), SUM(c4 % 5189),
 SUM(c4 % 5190), SUM(c4 % 5191), SUM(c4 % 5192), SUM(c4 % 5193), SUM(c4 % 5194),
 SUM(c4 % 5195), SUM(c4 % 5196), SUM(c4 % 5197), SUM(c4 % 5198), SUM(c4 % 5199),
 SUM(c4 % 5200), SUM(c4 % 5201), SUM(c4 % 5202), SUM(c4 % 5203), SUM(c4 % 5204),
 SUM(c4 % 5205), SUM(c4 % 5206), SUM(c4 % 5207), SUM(c4 % 5208), SUM(c4 % 5209),
 SUM(c4 % 5210), SUM(c4 % 5211), SUM(c4 % 5212), SUM(c4 % 5213), SUM(c4 % 5214),
 SUM(c4 % 5215), SUM(c4 % 5216), SUM(c4 % 5217), SUM(c4 % 5218), SUM(c4 % 5219),
 SUM(c4 % 5220), SUM(c4 % 5221), SUM(c4 % 5222), SUM(c4 % 5223), SUM(c4 % 5224),
 SUM(c4 % 5225), SUM(c4 % 5226), SUM(c4 % 5227), SUM(c4 % 5228), SUM(c4 % 5229),
 SUM(c4 % 5230), SUM(c4 % 5231), SUM(c4 % 5232), SUM(c4 % 5233), SUM(c4 % 5234),
 SUM(c4 % 5235), SUM(c4 % 5236), SUM(c4 % 5237), SUM(c4 % 5238), SUM(c4 % 5239),
 SUM(c4 % 5240), SUM(c4 % 5241), SUM(c4 % 5242), SUM(c4 % 5243), SUM(c4 % 5244),
 SUM(c4 % 5245), SUM(c4 % 5246), SUM(c4 % 5247), SUM(c4 % 5248), SUM(c4 % 5249),
 SUM(c4 % 5250), SUM(c4 % 5251), SUM(c4 % 5252), SUM(c4 % 5253), SUM(c4 % 5254),
 SUM(c4 % 5255), SUM(c4 % 5256), SUM(c4 % 5257), SUM(c4 % 5258), SUM(c4 % 5259),
 SUM(c4 % 5260), SUM(c4 % 5261), SUM(c4 % 5262), SUM(c4 % 5263), SUM(c4 % 5264),
 SUM(c4 % 5265), SUM(c4 % 5266), SUM(c4 % 5267), SUM(c4 % 5268), SUM(c4 % 5269),
 SUM(c4 % 5270), SUM(c4 % 5271), SUM(c4 % 5272), SUM(c4 % 5273), SUM(c4 % 5274),
 SUM(c4 % 5275), SUM(c4 % 5276), SUM(c4 % 5277), SUM(c4 % 5278), SUM(c4 % 5279),
 SUM(c4 % 5280), SUM(c4 % 5281), SUM(c4 % 5282), SUM(c4 % 5283), SUM(c4 % 5284),
 SUM(c4 % 5285), SUM(c4 % 5286), SUM(c4 % 5287), SUM(c4 % 5288), SUM(c4 % 5289),
 SUM(c4 % 5290), SUM(c4 % 5291), SUM(c4 % 5292), SUM(c4 % 5293), SUM(c4 % 5294),
 SUM(c4 % 5295), SUM(c4 % 5296), SUM(c4 % 5297), SUM(c4 % 5298), SUM(c4 % 5299),
 SUM(c4 % 5300), SUM(c4 % 5301), SUM(c4 % 5302), SUM(c4 % 5303), SUM(c4 % 5304),
 SUM(c4 % 5305), SUM(c4 % 5306), SUM(c4 % 5307), SUM(c4 % 5308), SUM(c4 % 5309),
 SUM(c4 % 5310), SUM(c4 % 5311), SUM(c4 % 5312), SUM(c4 % 5313), SUM(c4 % 5314),
 SUM(c4 % 5315), SUM(c4 % 5316), SUM(c4 % 5317), SUM(c4 % 5318), SUM(c4 % 5319),
 SUM(c4 % 5320), SUM(c4 % 5321), SUM(c4 % 5322), SUM(c4 % 5323), SUM(c4 % 5324),
 SUM(c4 % 5325), SUM(c4 % 5326), SUM(c4 % 5327), SUM(c4 % 5328), SUM(c4 % 5329),
 SUM(c4 % 5330), SUM(c4 % 5331), SUM(c4 % 5332), SUM(c4 % 5333), SUM(c4 % 5334),
 SUM(c4 % 5335), SUM(c4 % 5336), SUM(c4 % 5337), SUM(c4 % 5338), SUM(c4 % 5339),
 SUM(c4 % 5340), SUM(c4 % 5341), SUM(c4 % 5342), SUM(c4 % 5343), SUM(c4 % 5344),
 SUM(c4 % 5345), SUM(c4 % 5346), SUM(c4 % 5347), SUM(c4 % 5348), SUM(c4 % 5349),
 SUM(c4 % 5350), SUM(c4 % 5351), SUM(c4 % 5352), SUM(c4 % 5353), SUM(c4 % 5354),
 SUM(c4 % 5355), SUM(c4 % 5356), SUM(c4 % 5357), SUM(c4 % 5358), SUM(c4 % 5359),
 SUM(c4 % 5360), SUM(c4 % 5361), SUM(c4 % 5362), SUM(c4 % 5363), SUM(c4 % 5364),
 SUM(c4 % 5365), SUM(c4 % 5366), SUM(c4 % 5367), SUM(c4 % 5368), SUM(c4 % 5369),
 SUM(c4 % 5370), SUM(c4 % 5371), SUM(c4 % 5372), SUM(c4 % 5373), SUM(c4 % 5374),
 SUM(c4 % 5375), SUM(c4 % 5376), SUM(c4 % 5377), SUM(c4 % 5378), SUM(c4 % 5379),
 SUM(c4 % 5380), SUM(c4 % 5381), SUM(c4 % 5382), SUM(c4 % 5383), SUM(c4 % 5384),
 SUM(c4 % 5385), SUM(c4 % 5386), SUM(c4 % 5387), SUM(c4 % 5388), SUM(c4 % 5389),
 SUM(c4 % 5390), SUM(c4 % 5391), SUM(c4 % 5392), SUM(c4 % 5393), SUM(c4 % 5394),
 SUM(c4 % 5395), SUM(c4 % 5396), SUM(c4 % 5397), SUM(c4 % 5398), SUM(c4 % 5399),
 SUM(c4 % 5400), SUM(c4 % 5401), SUM(c4 % 5402), SUM(c4 % 5403), SUM(c4 % 5404),
 SUM(c4 % 5405), SUM(c4 % 5406), SUM(c4 % 5407), SUM(c4 % 5408), SUM(c4 % 5409),
 SUM(c4 % 5410), SUM(c4 % 5411), SUM(c4 % 5412), SUM(c4 % 5413), SUM(c4 % 5414),
 SUM(c4 % 5415), SUM(c4 % 5416), SUM(c4 % 5417), SUM(c4 % 5418), SUM(c4 % 5419),
 SUM(c4 % 5420), SUM(c4 % 5421), SUM(c4 % 5422), SUM(c4 % 5423), SUM(c4 % 5424),
 SUM(c4 % 5425), SUM(c4 % 5426), SUM(c4 % 5427), SUM(c4 % 5428), SUM(c4 % 5429),
 SUM(c4 % 5430), SUM(c4 % 5431), SUM(c4 % 5432), SUM(c4 % 5433), SUM(c4 % 5434),
 SUM(c4 % 5435), SUM(c4 % 5436), SUM(c4 % 5437), SUM(c4 % 5438), SUM(c4 % 5439),
 SUM(c4 % 5440), SUM(c4 % 5441), SUM(c4 % 5442), SUM(c4 % 5443), SUM(c4 % 5444),
 SUM(c4 % 5445), SUM(c4 % 5446), SUM(c4 % 5447), SUM(c4 % 5448), SUM(c4 % 5449),
 SUM(c4 % 5450), SUM(c4 % 5451), SUM(c4 % 5452), SUM(c4 % 5453), SUM(c4 % 5454),
 SUM(c4 % 5455), SUM(c4 % 5456), SUM(c4 % 5457), SUM(c4 % 5458), SUM(c4 % 5459),
 SUM(c4 % 5460), SUM(c4 % 5461), SUM(c4 % 5462), SUM(c4 % 5463), SUM(c4 % 5464),
 SUM(c4 % 5465), SUM(c4 % 5466), SUM(c4 % 5467), SUM(c4 % 5468), SUM(c4 % 5469),
 SUM(c4 % 5470), SUM(c4 % 5471), SUM(c4 % 5472), SUM(c4 % 5473), SUM(c4 % 5474),
 SUM(c4 % 5475), SUM(c4 % 5476), SUM(c4 % 5477), SUM(c4 % 5478), SUM(c4 % 5479),
 SUM(c4 % 5480), SUM(c4 % 5481), SUM(c4 % 5482), SUM(c4 % 5483), SUM(c4 % 5484),
 SUM(c4 % 5485), SUM(c4 % 5486), SUM(c4 % 5487), SUM(c4 % 5488), SUM(c4 % 5489),
 SUM(c4 % 5490), SUM(c4 % 5491), SUM(c4 % 5492), SUM(c4 % 5493), SUM(c4 % 5494),
 SUM(c4 % 5495), SUM(c4 % 5496), SUM(c4 % 5497), SUM(c4 % 5498), SUM(c4 % 5499),
 SUM(c4 % 5500), SUM(c4 % 5501), SUM(c4 % 5502), SUM(c4 % 5503), SUM(c4 % 5504),
 SUM(c4 % 5505), SUM(c4 % 5506), SUM(c4 % 5507), SUM(c4 % 5508), SUM(c4 % 5509),
 SUM(c4 % 5510), SUM(c4 % 5511), SUM(c4 % 5512), SUM(c4 % 5513), SUM(c4 % 5514),
 SUM(c4 % 5515), SUM(c4 % 5516), SUM(c4 % 5517), SUM(c4 % 5518), SUM(c4 % 5519),
 SUM(c4 % 5520), SUM(c4 % 5521), SUM(c4 % 5522), SUM(c4 % 5523), SUM(c4 % 5524),
 SUM(c4 % 5525), SUM(c4 % 5526), SUM(c4 % 5527), SUM(c4 % 5528), SUM(c4 % 5529),
 SUM(c4 % 5530), SUM(c4 % 5531), SUM(c4 % 5532), SUM(c4 % 5533), SUM(c4 % 5534),
 SUM(c4 % 5535), SUM(c4 % 5536), SUM(c4 % 5537), SUM(c4 % 5538), SUM(c4 % 5539),
 SUM(c4 % 5540), SUM(c4 % 5541), SUM(c4 % 5542), SUM(c4 % 5543), SUM(c4 % 5544),
 SUM(c4 % 5545), SUM(c4 % 5546), SUM(c4 % 5547), SUM(c4 % 5548), SUM(c4 % 5549),
 SUM(c4 % 5550), SUM(c4 % 5551), SUM(c4 % 5552), SUM(c4 % 5553), SUM(c4 % 5554),
 SUM(c4 % 5555), SUM(c4 % 5556), SUM(c4 % 5557), SUM(c4 % 5558), SUM(c4 % 5559),
 SUM(c4 % 5560), SUM(c4 % 5561), SUM(c4 % 5562), SUM(c4 % 5563), SUM(c4 % 5564),
 SUM(c4 % 5565), SUM(c4 % 5566), SUM(c4 % 5567), SUM(c4 % 5568), SUM(c4 % 5569),
 SUM(c4 % 5570), SUM(c4 % 5571), SUM(c4 % 5572), SUM(c4 % 5573), SUM(c4 % 5574),
 SUM(c4 % 5575), SUM(c4 % 5576), SUM(c4 % 5577), SUM(c4 % 5578), SUM(c4 % 5579),
 SUM(c4 % 5580), SUM(c4 % 5581), SUM(c4 % 5582), SUM(c4 % 5583), SUM(c4 % 5584),
 SUM(c4 % 5585), SUM(c4 % 5586), SUM(c4 % 5587), SUM(c4 % 5588), SUM(c4 % 5589),
 SUM(c4 % 5590), SUM(c4 % 5591), SUM(c4 % 5592), SUM(c4 % 5593), SUM(c4 % 5594),
 SUM(c4 % 5595), SUM(c4 % 5596), SUM(c4 % 5597), SUM(c4 % 5598), SUM(c4 % 5599),
 SUM(c4 % 5600), SUM(c4 % 5601), SUM(c4 % 5602), SUM(c4 % 5603), SUM(c4 % 5604),
 SUM(c4 % 5605), SUM(c4 % 5606), SUM(c4 % 5607), SUM(c4 % 5608), SUM(c4 % 5609),
 SUM(c4 % 5610), SUM(c4 % 5611), SUM(c4 % 5612), SUM(c4 % 5613), SUM(c4 % 5614),
 SUM(c4 % 5615), SUM(c4 % 5616), SUM(c4 % 5617), SUM(c4 % 5618), SUM(c4 % 5619),
 SUM(c4 % 5620), SUM(c4 % 5621), SUM(c4 % 5622), SUM(c4 % 5623), SUM(c4 % 5624),
 SUM(c4 % 5625), SUM(c4 % 5626), SUM(c4 % 5627), SUM(c4 % 5628), SUM(c4 % 5629),
 SUM(c4 % 5630), SUM(c4 % 5631), SUM(c4 % 5632), SUM(c4 % 5633), SUM(c4 % 5634),
 SUM(c4 % 5635), SUM(c4 % 5636), SUM(c4 % 5637), SUM(c4 % 5638), SUM(c4 % 5639),
 SUM(c4 % 5640), SUM(c4 % 5641), SUM(c4 % 5642), SUM(c4 % 5643), SUM(c4 % 5644),
 SUM(c4 % 5645), SUM(c4 % 5646), SUM(c4 % 5647), SUM(c4 % 5648), SUM(c4 % 5649),
 SUM(c4 % 5650), SUM(c4 % 5651), SUM(c4 % 5652), SUM(c4 % 5653), SUM(c4 % 5654),
 SUM(c4 % 5655), SUM(c4 % 5656), SUM(c4 % 5657), SUM(c4 % 5658), SUM(c4 % 5659),
 SUM(c4 % 5660), SUM(c4 % 5661), SUM(c4 % 5662), SUM(c4 % 5663), SUM(c4 % 5664),
 SUM(c4 % 5665), SUM(c4 % 5666), SUM(c4 % 5667), SUM(c4 % 5668), SUM(c4 % 5669),
 SUM(c4 % 5670), SUM(c4 % 5671)], 1)
from mtup1 where c0 = 'foo' group by c0, c1 limit 10;

reset gp_enable_multiphase_agg;

-- MPP-29042 Multistage aggregation plans should have consistent targetlists in
-- case of same column aliases and grouping on them.
DROP TABLE IF EXISTS t1;
CREATE TABLE t1 (a varchar, b character varying) DISTRIBUTED RANDOMLY;
INSERT INTO t1 VALUES ('aaaaaaa', 'cccccccccc');
INSERT INTO t1 VALUES ('aaaaaaa', 'ddddd');
INSERT INTO t1 VALUES ('bbbbbbb', 'eeee');
INSERT INTO t1 VALUES ('bbbbbbb', 'eeef');
INSERT INTO t1 VALUES ('bbbbb', 'dfafa');
SELECT substr(a, 1) as a FROM (SELECT ('-'||a)::varchar as a FROM (SELECT a FROM t1) t2) t3 GROUP BY a ORDER BY a;
SELECT array_agg(f ORDER BY f)  FROM (SELECT b::text as f FROM t1 GROUP BY b ORDER BY b) q;


-- Check that ORDER BY NULLS FIRST/LAST in an aggregate is respected (these are
-- variants of similar query in PostgreSQL's aggregates test)
create temporary table aggordertest (a int4, b int4) ;
insert into aggordertest values (1,1), (2,2), (1,3), (3,4), (null,5), (2,null);

select array_agg(a order by a nulls first) from aggordertest;
select array_agg(a order by a nulls last) from aggordertest;
select array_agg(a order by a desc nulls first) from aggordertest;
select array_agg(a order by a desc nulls last) from aggordertest;
select array_agg(a order by b nulls first) from aggordertest;
select array_agg(a order by b nulls last) from aggordertest;
select array_agg(a order by b desc nulls first) from aggordertest;
select array_agg(a order by b desc nulls last) from aggordertest;

-- begin MPP-14125: if combine function is missing, do not choose hash agg.
-- POLARDB_12_MERGE_FIXME: Like in the 'attribute_table' and 'concat' test earlier in this
-- file, a Hash Agg is currently OK, since we lost the Hybrid Hash Agg spilling
-- code in the merge.
create temp table mpp14125 as select repeat('a', a) a, a % 10 b from generate_series(1, 100)a;
explain (costs off) select string_agg(a, '') from mpp14125 group by b;
-- end MPP-14125

-- Test that integer AVG() aggregate is accurate with large values. We used to
-- use float8 to hold the running sums, which did not have enough precision
-- for this.
select avg('1000000000000000000'::int8) from generate_series(1, 100000);

-- Test cases where the planner would like to distribute on a column, to implement
-- grouping or distinct, but can't because the datatype isn't GPDB-hashable.
-- These are all variants of the same issue; all of these used to miss the
-- check on whether the column is GPDB_hashble, producing an assertion failure.
create table int2vectortab (distkey int, t int2vector,t2 int2vector) ;
insert into int2vectortab values
  (1, '1', '1'),
  (2, '1 2', '1 2'),
  (3, '1 2 3', '1 2 3'),
  (22,'22', '1 2 3 4'),
  (22,'1 2', '1 2 3 4 5');

select distinct t from int2vectortab group by distkey, t;
select t from int2vectortab union select t from int2vectortab;
select count(*) over (partition by t) from int2vectortab;
select count(distinct t) from int2vectortab;
select count(distinct t), count(distinct t2) from int2vectortab;

--
-- Testing aggregate above FULL JOIN
--

-- SETUP
CREATE TABLE pagg_tab1(x int, y int) ;
CREATE TABLE pagg_tab2(x int, y int) ;

INSERT INTO pagg_tab1 SELECT i % 30, i % 20 FROM generate_series(0, 299, 2) i;
INSERT INTO pagg_tab2 SELECT i % 20, i % 30 FROM generate_series(0, 299, 3) i;

ANALYZE pagg_tab1;
ANALYZE pagg_tab2;

-- TEST
-- should have Redistribute Motion above the FULL JOIN
EXPLAIN (COSTS OFF)
SELECT a.x, sum(b.x) FROM pagg_tab1 a FULL OUTER JOIN pagg_tab2 b ON a.x = b.y GROUP BY a.x ORDER BY 1 NULLS LAST;
SELECT a.x, sum(b.x) FROM pagg_tab1 a FULL OUTER JOIN pagg_tab2 b ON a.x = b.y GROUP BY a.x ORDER BY 1 NULLS LAST;


EXPLAIN (COSTS OFF)
SELECT a.x, b.y, count(*) FROM pagg_tab1 a FULL JOIN pagg_tab2 b ON a.x = b.y GROUP BY a.x, b.y;
SELECT a.x, b.y, count(*) FROM pagg_tab1 a FULL JOIN pagg_tab2 b ON a.x = b.y GROUP BY a.x, b.y;

--
-- Test GROUP BY with a constant
--
create temp table group_by_const (col1 int, col2 int);
insert into group_by_const select i from generate_series(1, 1000) i;

explain (costs off)
select 1, sum(col1) from group_by_const group by 1;
select 1, sum(col1) from group_by_const group by 1;

-- Same, but using an aggregate that doesn't have a combine function, so
-- that you get a one-phase aggregate plan.
explain (costs off)
select 1, median(col1) from group_by_const group by 1;
select 1, median(col1) from group_by_const group by 1;

-- CLEANUP
set client_min_messages='warning';
drop schema bfv_aggregate cascade;
