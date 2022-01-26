
--User defined functions have several limitations 
--
-- PLPGSQL

-- Table xl_room default distributed by Hash(roomno) on all nodes. 
create table xl_Room (
    roomno	char(8),
    comment	text
);

create unique index xl_Room_rno on xl_Room using btree (roomno bpchar_ops);

-- Table xl_wslot default distributed by Hash(slotname) on all nodes.
create table xl_WSlot (
    slotname	char(20),
    roomno	char(8),
    slotlink	char(20),
    backlink	char(20)
);

create unique index xl_WSlot_name on xl_WSlot using btree (slotname bpchar_ops);

--default distributed by HASH(slotname)
create table xl_PLine (
    slotname	char(20),
    phonenumber	char(20),
    comment	text,
    backlink	char(20)
);

create unique index xl_PLine_name on xl_PLine using btree (slotname bpchar_ops);


-- ************************************************************
-- *
-- * Trigger procedures and functions for the patchfield
-- * test of PL/pgSQL
-- *
-- ************************************************************


-- ************************************************************
-- * AFTER UPDATE on Room
-- *	- If room no changes let wall slots follow
-- ************************************************************
create function xl_room_au() returns trigger as '
begin
    if new.roomno != old.roomno then
        update xl_WSlot set roomno = new.roomno where roomno = old.roomno;
    end if;
    return new;
end;
' language plpgsql;

--BEFORE/AFTER TRIGGERs are not supported in Postgres-XL - below would fail

create trigger tg_room_bu before update
    on xl_Room for each row execute procedure xl_room_au();

create trigger xl_room_au after update
    on xl_Room for each row execute procedure xl_room_au();


--
-- Test error trapping
--
--Internal subtransactions (exception clause within a function)
-- calling below function would fail.
create function xl_trap_zero_divide(int) returns int as $$
declare x int;
	sx smallint;
begin
	begin	-- start a subtransaction
		raise notice 'should see this';
		x := 100 / $1;
		raise notice 'should see this only if % <> 0', $1;
		sx := $1;
		raise notice 'should see this only if % fits in smallint', $1;
		if $1 < 0 then
			raise exception '% is less than zero', $1;
		end if;
	exception
		when division_by_zero then
			raise notice 'caught division_by_zero';
			x := -1;
		when NUMERIC_VALUE_OUT_OF_RANGE then
			raise notice 'caught numeric_value_out_of_range';
			x := -2;
	end;
	return x;
end$$ language plpgsql;

select xl_trap_zero_divide(50);


--SERIALIZABLE TRANSACTIONs are not supported. Isolation level SERIALIZABLE is converted to REPEATABLE READ internally silently. 

-- ALTER OPERATOR FAMILY ... ADD/DROP

-- Should work. Textbook case of CREATE / ALTER ADD / ALTER DROP / DROP
BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE;
CREATE OPERATOR FAMILY alt_opf4 USING btree;
ALTER OPERATOR FAMILY alt_opf4 USING btree ADD
  -- int4 vs int2
  OPERATOR 1 < (int4, int2) ,
  OPERATOR 2 <= (int4, int2) ,
  OPERATOR 3 = (int4, int2) ,
  OPERATOR 4 >= (int4, int2) ,
  OPERATOR 5 > (int4, int2) ,
  FUNCTION 1 btint42cmp(int4, int2);

ALTER OPERATOR FAMILY alt_opf4 USING btree DROP
  -- int4 vs int2
  OPERATOR 1 (int4, int2) ,
  OPERATOR 2 (int4, int2) ,
  OPERATOR 3 (int4, int2) ,
  OPERATOR 4 (int4, int2) ,
  OPERATOR 5 (int4, int2) ,
  FUNCTION 1 (int4, int2) ;
DROP OPERATOR FAMILY alt_opf4 USING btree;
ROLLBACK;

create function xl_nodename_from_id(integer) returns name as $$
declare 
	n name;
BEGIN
	select node_name into n from pgxc_node where node_id = $1;
	RETURN n;
END;$$ language plpgsql;

--insert Plines
insert into  xl_PLine values ('PL.001', '-0', 'Central call', 'PS.base.ta1');
insert into  xl_PLine values ('PL.002', '-101', '', 'PS.base.ta2');
insert into  xl_PLine values ('PL.003', '-102', '', 'PS.base.ta3');
insert into  xl_PLine values ('PL.004', '-103', '', 'PS.base.ta5');
insert into  xl_PLine values ('PL.005', '-104', '', 'PS.base.ta6');
insert into  xl_PLine values ('PL.006', '-106', '', 'PS.base.tb2');
insert into  xl_PLine values ('PL.007', '-108', '', 'PS.base.tb3');
insert into  xl_PLine values ('PL.008', '-109', '', 'PS.base.tb4');
insert into  xl_PLine values ('PL.009', '-121', '', 'PS.base.tb5');
insert into  xl_PLine values ('PL.010', '-122', '', 'PS.base.tb6');
insert into  xl_PLine values ('PL.011', '-122', '', 'PS.base.tb6');
insert into  xl_PLine values ('PL.012', '-122', '', 'PS.base.tb6');
insert into  xl_PLine values ('PL.013', '-122', '', 'PS.base.tb6');
insert into  xl_PLine values ('PL.014', '-122', '', 'PS.base.tb6');
insert into  xl_PLine values ('PL.015', '-134', '', 'PS.first.ta1');
insert into  xl_PLine values ('PL.016', '-137', '', 'PS.first.ta3');
insert into  xl_PLine values ('PL.017', '-139', '', 'PS.first.ta4');
insert into  xl_PLine values ('PL.018', '-362', '', 'PS.first.tb1');
insert into  xl_PLine values ('PL.019', '-363', '', 'PS.first.tb2');
insert into  xl_PLine values ('PL.020', '-364', '', 'PS.first.tb3');
insert into  xl_PLine values ('PL.021', '-365', '', 'PS.first.tb5');
insert into  xl_PLine values ('PL.022', '-367', '', 'PS.first.tb6');
insert into  xl_PLine values ('PL.023', '-367', '', 'PS.first.tb6');
insert into  xl_PLine values ('PL.024', '-367', '', 'PS.first.tb6');
insert into  xl_PLine values ('PL.025', '-367', '', 'PS.first.tb6');
insert into  xl_PLine values ('PL.026', '-367', '', 'PS.first.tb6');
insert into  xl_PLine values ('PL.027', '-367', '', 'PS.first.tb6');
insert into  xl_PLine values ('PL.028', '-501', 'Fax entrance', 'PS.base.ta2');
insert into  xl_PLine values ('PL.029', '-502', 'Fax first floor', 'PS.first.ta1');


--INSENSITIVE/SCROLL/WITH HOLD cursors are not supported
-- if INSENSITIVE is not allowed then what is the default for Postgres-XL for cursor ?

-- May also check over here - FETCH and WHERE CURRENT OF conditions
-- WHERE CURRENT OF is not supported 
--Many forms of FETCH are not supported such as those involving PRIOR, FIRST, LAST, ABSOLUTE, BACKWARD
--Basic test with data spannig on multiple nodes is working fine.
--scroll cursor would be insensitive to updates happening to the table
BEGIN;

declare xl_scroll_cursor SCROLL CURSOR for select * from xl_Pline order by slotname;

FETCH ALL FROM xl_scroll_cursor;
select xl_nodename_from_id(xc_node_id), * from xl_Pline order by slotname;
FETCH FIRST xl_scroll_cursor;
FETCH LAST xl_scroll_cursor;
insert into  xl_PLine values ('PL.030', '-367', '', 'PS.first.tb6');
insert into  xl_PLine values ('PL.031', '-367', '', 'PS.first.tb6');
insert into  xl_PLine values ('PL.032', '-367', '', 'PS.first.tb6');
insert into  xl_PLine values ('PL.033', '-367', '', 'PS.first.tb6');
insert into  xl_PLine values ('PL.034', '-367', '', 'PS.first.tb6');
FETCH LAST xl_scroll_cursor;
select xl_nodename_from_id(xc_node_id), * from xl_Pline where slotname in ('PL.030', 'PL.031', 'PL.032','PL.033', 'PL.034') order by slotname;
delete from xl_Pline where slotname in ('PL.030', 'PL.031', 'PL.032','PL.033', 'PL.034');
FETCH LAST xl_scroll_cursor;
delete from xl_Pline where slotname in ('PL.029');
FETCH LAST xl_scroll_cursor;
insert into  xl_PLine values ('PL.029', '-367', '', 'PS.first.tb6');
FETCH PRIOR xl_scroll_cursor;
FETCH ABSOLUTE 5 xl_scroll_cursor;
FETCH BACKWARD 2 xl_scroll_cursor;
DELETE FROM xl_Pline WHERE CURRENT OF xl_scroll_cursor;
COMMIT;


--scroll cursor would be insensitive to updates happening to the table
BEGIN;

declare xl_scroll_cursor1 SCROLL CURSOR for select * from xl_Pline order by slotname desc;

FETCH ALL FROM xl_scroll_cursor1;
select xl_nodename_from_id(xc_node_id), * from xl_Pline order by slotname desc;
FETCH FIRST xl_scroll_cursor1;
insert into  xl_PLine values ('PL.030', '-367', '', 'PS.first.tb6');
insert into  xl_PLine values ('PL.031', '-367', '', 'PS.first.tb6');
insert into  xl_PLine values ('PL.032', '-367', '', 'PS.first.tb6');
insert into  xl_PLine values ('PL.033', '-367', '', 'PS.first.tb6');
insert into  xl_PLine values ('PL.034', '-367', '', 'PS.first.tb6');
FETCH FIRST xl_scroll_cursor1;
select xl_nodename_from_id(xc_node_id), * from xl_Pline where slotname in ('PL.030', 'PL.031', 'PL.032','PL.033', 'PL.034') order by slotname;
delete from xl_Pline where slotname in ('PL.030', 'PL.031', 'PL.032','PL.033', 'PL.034');
FETCH FIRST xl_scroll_cursor1;
delete from xl_Pline where slotname in ('PL.029');
FETCH FIRST xl_scroll_cursor1;
FETCH LAST xl_scroll_cursor1;
insert into  xl_PLine values ('PL.029', '-367', '', 'PS.first.tb6');
FETCH PRIOR xl_scroll_cursor1;
FETCH ABSOLUTE 5 xl_scroll_cursor1;
FETCH BACKWARD 2 xl_scroll_cursor1;
DELETE FROM xl_Pline WHERE CURRENT OF xl_scroll_cursor1;
COMMIT;

--with hold cursor wold be available after the transaction that successfully committed it is gone
BEGIN;

declare xl_with_hold_cursor CURSOR WITH HOLD for select * from xl_Pline order by slotname;

FETCH FIRST xl_with_hold_cursor;
FETCH ABSOLUTE 5 xl_with_hold_cursor;
FETCH ALL FROM xl_with_hold_cursor;
COMMIT;
FETCH FIRST xl_with_hold_cursor;

--SAVEPOINTs are not supported
begin;
  set constraints all deferred;
  savepoint x;
    set constraints all immediate; -- fails
  rollback to x;
commit;				-- still fails

--Correlated UPDATE/DELETE is not supported. 
-- distributed by default ==> xl_t by HASH(no), xl_t1 by HASH(no1)
CREATE TABLE xl_t("no" integer,"name" character varying);
CREATE TABLE xl_t1(no1 integer,name1 character varying);
create table xl_names("name" character varying, "name1" character varying);

INSERT INTO xl_t("no", "name")VALUES (1, 'A');
INSERT INTO xl_t("no", "name")VALUES (2, 'B');
INSERT INTO xl_t("no", "name")VALUES (3, 'C');
INSERT INTO xl_t("no", "name")VALUES (4, 'D');

INSERT INTO xl_t1("no1", "name1")VALUES (1, 'Z');
INSERT INTO xl_t1("no1", "name1")VALUES (2, 'Y');
INSERT INTO xl_t1("no1", "name1")VALUES (3, 'X');
INSERT INTO xl_t1("no1", "name1")VALUES (4, 'W');

INSERT INTO xl_names("name", "name1")VALUES ('A', 'A1');
INSERT INTO xl_names("name", "name1")VALUES ('B', 'B1');
INSERT INTO xl_names("name", "name1")VALUES ('C', 'C1');
INSERT INTO xl_names("name", "name1")VALUES ('D', 'D1');
INSERT INTO xl_names("name", "name1")VALUES ('W', 'W1');
INSERT INTO xl_names("name", "name1")VALUES ('X', 'X1');
INSERT INTO xl_names("name", "name1")VALUES ('Y', 'Y1');
INSERT INTO xl_names("name", "name1")VALUES ('Z', 'Z1');

select xl_nodename_from_id(xc_node_id), * from xl_t;

select xl_nodename_from_id(xc_node_id), * from xl_t1;

select xl_nodename_from_id(xc_node_id), * from xl_names order by name;

update xl_t  set name = T1.name1 
from (select no1,name1 from xl_t1) T1 
where xl_t.no = T1.no1;

--correlated update fails
update xl_t1  set name1 = T1.name1 
from (select name,name1 from xl_names) T1 
where xl_t1.name1 = T1.name;

select xl_nodename_from_id(xc_node_id), * from xl_t;
select xl_nodename_from_id(xc_node_id), * from xl_t1;

--testing correlated delete:
delete from xl_t 
where xl_t.no in (select no1 from xl_t1 where name1 in ('Z', 'X'))
;

--correlated delete fails
delete from xl_t1  
where xl_t1.name1 in (select name1 from xl_names where name in ('Z', 'X'))
;

select xl_nodename_from_id(xc_node_id), * from xl_t;

select xl_nodename_from_id(xc_node_id), * from xl_t1;

drop table xl_t;
drop table xl_t1;
drop table xl_names;

--EXCLUSION CONSTRAINTS are not supported
--The constraint is enforced when both rows map to the same datanode. But if they go into different datanodes, the constraint is not enforced
CREATE TABLE xl_circles (
    c circle,
    EXCLUDE USING gist (c WITH &&)
);

CREATE TABLE xl_cons_hash (a int, c circle, EXCLUDE USING gist (c WITH &&)) DISTRIBUTE BY HASH(a); 
CREATE TABLE xl_cons_modulo (a int, c circle, EXCLUDE USING gist (c WITH &&)) DISTRIBUTE BY MODULO(a);  error
CREATE TABLE xl_cons_rr (c circle, EXCLUDE USING gist (c WITH &&)) DISTRIBUTE BY ROUNDROBIN;
CREATE TABLE xl_cons_hash2 (a int, b int, c circle, EXCLUDE USING btree (a WITH = , b WITH =)) DISTRIBUTE BY HASH(a); 
CREATE TABLE xl_cons_modulo2 (a int, b int, c circle, EXCLUDE USING btree (a WITH =, b WITH =)) DISTRIBUTE BY MODULO(a); 

-- xl_test1 is distributed by default on HASH(a)
CREATE TABLE xl_test1 (a int, b int);
INSERT INTO xl_test1 VALUES (1,2);
INSERT INTO xl_test1 VALUES (2,2);
INSERT INTO xl_test1 VALUES (3,2);
INSERT INTO xl_test1 VALUES (4,2);
INSERT INTO xl_test1 VALUES (5,2);
INSERT INTO xl_test1 VALUES (6,2);
INSERT INTO xl_test1 VALUES (7,2);

select xl_nodename_from_id(xc_node_id),* from xl_test1 order by a;
-- this is to see how hash of integers key distributes among data nodes.

-- xl_test is distributed by default on HASH(a)
CREATE TABLE xl_test (a int, b int, EXCLUDE USING btree (b WITH =));
INSERT INTO xl_test VALUES (1,2);
INSERT INTO xl_test VALUES (2,2);
INSERT INTO xl_test VALUES (3,2);
INSERT INTO xl_test VALUES (4,2);
INSERT INTO xl_test VALUES (5,2);
INSERT INTO xl_test VALUES (6,2);
INSERT INTO xl_test VALUES (7,2);

--the constraint is enforced when both rows map to the same datanode, e.g. when a=1, a=2
--But if they go into different datanodes, the constraint is not enforced, e.g. when a=3

drop table xl_circles;
drop table xl_cons_hash;
drop table xl_cons_modulo;
drop table xl_cons_rr;
drop table xl_test;
drop table xl_test1;

drop table xl_Room;
drop table xl_WSlot;
drop table xl_PLine;


-- CREATE INDEX CONCURRENTLY is not supported
--
-- Try some concurrent index builds
--
-- Unfortunately this only tests about half the code paths because there are
-- no concurrent updates happening to the table at the same time.

CREATE TABLE xl_concur_heap (f1 text, f2 text);
-- empty table
CREATE INDEX CONCURRENTLY xl_concur_index1 ON xl_concur_heap(f2,f1);
CREATE INDEX CONCURRENTLY IF NOT EXISTS xl_concur_index1 ON xl_concur_heap(f2,f1);
INSERT INTO xl_concur_heap VALUES  ('a','b');
INSERT INTO xl_concur_heap VALUES  ('b','b');

drop index concurrently xl_concur_index1;
drop table xl_concur_heap;


--Large objects are not supported

CREATE TABLE lotest_stash_values (loid oid, junk integer, fd integer);
-- lo_creat(mode integer) returns oid
-- The mode arg to lo_creat is unused, some vestigal holdover from ancient times
-- returns the large object id
INSERT INTO lotest_stash_values (loid) VALUES( lo_creat(42) );

drop table lotest_stash_values;

--EVENT TRIGGERs are not supported
CREATE FUNCTION xl_event_trigger_for_drops()
        RETURNS event_trigger LANGUAGE plpgsql AS $$
DECLARE
    obj record;
BEGIN
    FOR obj IN SELECT * FROM pg_event_trigger_dropped_objects()
    LOOP
        RAISE NOTICE '% dropped object: % %.% %',
                     tg_tag,
                     obj.object_type,
                     obj.schema_name,
                     obj.object_name,
                     obj.object_identity;
    END LOOP;
END
$$;
CREATE EVENT TRIGGER xl_event_trigger_for_drops
   ON sql_drop
   EXECUTE PROCEDURE xl_event_trigger_for_drops();

drop function xl_event_trigger_for_drops();
drop EVENT TRIGGER xl_event_trigger_for_drops;

--Recursive queries work now to some extent
--when replicated tables are used or when query can be fully 
--shipped to a single node
WITH RECURSIVE t(n) AS (
    VALUES (1)
  UNION ALL
    SELECT n+1 FROM t WHERE n < 100
)
SELECT sum(n) FROM t;

--FDWs are not supported

CREATE FOREIGN DATA WRAPPER xl_foo; -- ERROR
RESET ROLE;
CREATE FOREIGN DATA WRAPPER xl_foo VALIDATOR postgresql_fdw_validator;

--LISTEN/NOTIFY is not supported. Looks like they are supported now. 
--We would obviously have issues with LISTEN/NOTIFY if clients are connected to different coordinators. Need to test that manually as it is difficult via regression.
--LISTEN notify_async1; 
-- commenting LISTEN as PIDs shown here would never match in regression.
SELECT pg_notify('notify_async1','sample message1');
SELECT pg_notify('notify_async1','');
SELECT pg_notify('notify_async1',NULL);

-- Should fail. Send a valid message via an invalid channel name
SELECT pg_notify('','sample message1');
SELECT pg_notify(NULL,'sample message1');
SELECT pg_notify('notify_async_channel_name_too_long______________________________','sample_message1');

--Should work. Valid NOTIFY/LISTEN/UNLISTEN commands
NOTIFY notify_async2;
LISTEN notify_async2;
UNLISTEN notify_async2;
UNLISTEN *;

--LISTEN virtual;
NOTIFY virtual;

NOTIFY virtual, 'This is the payload';


--LISTEN foo;
SELECT pg_notify('fo' || 'o', 'pay' || 'load');

drop function xl_room_au();
drop function xl_trap_zero_divide(int);
drop function xl_nodename_from_id(integer);

-- TRUNCATE ... RESTART IDENTITY
CREATE SEQUENCE truncate_a_id1 START WITH 33;
CREATE TABLE truncate_a (id serial,
                         id1 integer default nextval('truncate_a_id1'));
ALTER SEQUENCE truncate_a_id1 OWNED BY truncate_a.id1;
TRUNCATE truncate_a RESTART IDENTITY;
DROP TABLE truncate_a;
