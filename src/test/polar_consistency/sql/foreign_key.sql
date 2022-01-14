--
-- FOREIGN KEY
--

-- MATCH FULL
--
-- First test, check and cascade
--
CREATE TABLE PKTABLE ( ptest1 int PRIMARY KEY, ptest2 text );
CREATE TABLE FKTABLE ( ftest1 int REFERENCES PKTABLE MATCH FULL ON DELETE CASCADE ON UPDATE CASCADE, ftest2 int );

-- Insert test data into PKTABLE
INSERT INTO PKTABLE VALUES (1, 'Test1');
INSERT INTO PKTABLE VALUES (2, 'Test2');
INSERT INTO PKTABLE VALUES (3, 'Test3');
INSERT INTO PKTABLE VALUES (4, 'Test4');
INSERT INTO PKTABLE VALUES (5, 'Test5');

-- Insert successful rows into FK TABLE
INSERT INTO FKTABLE VALUES (1, 2);
INSERT INTO FKTABLE VALUES (2, 3);
INSERT INTO FKTABLE VALUES (3, 4);
INSERT INTO FKTABLE VALUES (NULL, 1);

-- Insert a failed row into FK TABLE
INSERT INTO FKTABLE VALUES (100, 2);

-- Check FKTABLE
SELECT * FROM FKTABLE;

-- Delete a row from PK TABLE
DELETE FROM PKTABLE WHERE ptest1=1;

-- Check FKTABLE for removal of matched row
SELECT * FROM FKTABLE;

-- Update a row from PK TABLE
UPDATE PKTABLE SET ptest1=1 WHERE ptest1=2;

-- Check FKTABLE for update of matched row
SELECT * FROM FKTABLE;

DROP TABLE FKTABLE;
DROP TABLE PKTABLE;

--
-- check set NULL and table constraint on multiple columns
--
CREATE TABLE PKTABLE ( ptest1 int, ptest2 int, ptest3 text, PRIMARY KEY(ptest1, ptest2) );
CREATE TABLE FKTABLE ( ftest1 int, ftest2 int, ftest3 int, CONSTRAINT constrname FOREIGN KEY(ftest1, ftest2)
                       REFERENCES PKTABLE MATCH FULL ON DELETE SET NULL ON UPDATE SET NULL);

-- Test comments
COMMENT ON CONSTRAINT constrname_wrong ON FKTABLE IS 'fk constraint comment';
COMMENT ON CONSTRAINT constrname ON FKTABLE IS 'fk constraint comment';
COMMENT ON CONSTRAINT constrname ON FKTABLE IS NULL;

-- Insert test data into PKTABLE
INSERT INTO PKTABLE VALUES (1, 2, 'Test1');
INSERT INTO PKTABLE VALUES (1, 3, 'Test1-2');
INSERT INTO PKTABLE VALUES (2, 4, 'Test2');
INSERT INTO PKTABLE VALUES (3, 6, 'Test3');
INSERT INTO PKTABLE VALUES (4, 8, 'Test4');
INSERT INTO PKTABLE VALUES (5, 10, 'Test5');

-- Insert successful rows into FK TABLE
INSERT INTO FKTABLE VALUES (1, 2, 4);
INSERT INTO FKTABLE VALUES (1, 3, 5);
INSERT INTO FKTABLE VALUES (2, 4, 8);
INSERT INTO FKTABLE VALUES (3, 6, 12);
INSERT INTO FKTABLE VALUES (NULL, NULL, 0);

-- Insert failed rows into FK TABLE
INSERT INTO FKTABLE VALUES (100, 2, 4);
INSERT INTO FKTABLE VALUES (2, 2, 4);
INSERT INTO FKTABLE VALUES (NULL, 2, 4);
INSERT INTO FKTABLE VALUES (1, NULL, 4);

-- Check FKTABLE
SELECT * FROM FKTABLE;

-- Delete a row from PK TABLE
DELETE FROM PKTABLE WHERE ptest1=1 and ptest2=2;

-- Check FKTABLE for removal of matched row
SELECT * FROM FKTABLE;

-- Delete another row from PK TABLE
DELETE FROM PKTABLE WHERE ptest1=5 and ptest2=10;

-- Check FKTABLE (should be no change)
SELECT * FROM FKTABLE;

-- Update a row from PK TABLE
UPDATE PKTABLE SET ptest1=1 WHERE ptest1=2;

-- Check FKTABLE for update of matched row
SELECT * FROM FKTABLE;

-- Try altering the column type where foreign keys are involved
ALTER TABLE PKTABLE ALTER COLUMN ptest1 TYPE bigint;
ALTER TABLE FKTABLE ALTER COLUMN ftest1 TYPE bigint;
SELECT * FROM PKTABLE;
SELECT * FROM FKTABLE;

DROP TABLE PKTABLE CASCADE;
DROP TABLE FKTABLE;

--
-- check set default and table constraint on multiple columns
--
CREATE TABLE PKTABLE ( ptest1 int, ptest2 int, ptest3 text, PRIMARY KEY(ptest1, ptest2) );
CREATE TABLE FKTABLE ( ftest1 int DEFAULT -1, ftest2 int DEFAULT -2, ftest3 int, CONSTRAINT constrname2 FOREIGN KEY(ftest1, ftest2)
                       REFERENCES PKTABLE MATCH FULL ON DELETE SET DEFAULT ON UPDATE SET DEFAULT);

-- Insert a value in PKTABLE for default
INSERT INTO PKTABLE VALUES (-1, -2, 'The Default!');

-- Insert test data into PKTABLE
INSERT INTO PKTABLE VALUES (1, 2, 'Test1');
INSERT INTO PKTABLE VALUES (1, 3, 'Test1-2');
INSERT INTO PKTABLE VALUES (2, 4, 'Test2');
INSERT INTO PKTABLE VALUES (3, 6, 'Test3');
INSERT INTO PKTABLE VALUES (4, 8, 'Test4');
INSERT INTO PKTABLE VALUES (5, 10, 'Test5');

-- Insert successful rows into FK TABLE
INSERT INTO FKTABLE VALUES (1, 2, 4);
INSERT INTO FKTABLE VALUES (1, 3, 5);
INSERT INTO FKTABLE VALUES (2, 4, 8);
INSERT INTO FKTABLE VALUES (3, 6, 12);
INSERT INTO FKTABLE VALUES (NULL, NULL, 0);

-- Insert failed rows into FK TABLE
INSERT INTO FKTABLE VALUES (100, 2, 4);
INSERT INTO FKTABLE VALUES (2, 2, 4);
INSERT INTO FKTABLE VALUES (NULL, 2, 4);
INSERT INTO FKTABLE VALUES (1, NULL, 4);

-- Check FKTABLE
SELECT * FROM FKTABLE;

-- Delete a row from PK TABLE
DELETE FROM PKTABLE WHERE ptest1=1 and ptest2=2;

-- Check FKTABLE to check for removal
SELECT * FROM FKTABLE;

-- Delete another row from PK TABLE
DELETE FROM PKTABLE WHERE ptest1=5 and ptest2=10;

-- Check FKTABLE (should be no change)
SELECT * FROM FKTABLE;

-- Update a row from PK TABLE
UPDATE PKTABLE SET ptest1=1 WHERE ptest1=2;

-- Check FKTABLE for update of matched row
SELECT * FROM FKTABLE;

-- this should fail for lack of CASCADE
DROP TABLE PKTABLE;
DROP TABLE PKTABLE CASCADE;
DROP TABLE FKTABLE;


--
-- First test, check with no on delete or on update
--
CREATE TABLE PKTABLE ( ptest1 int PRIMARY KEY, ptest2 text );
CREATE TABLE FKTABLE ( ftest1 int REFERENCES PKTABLE MATCH FULL, ftest2 int );

-- Insert test data into PKTABLE
INSERT INTO PKTABLE VALUES (1, 'Test1');
INSERT INTO PKTABLE VALUES (2, 'Test2');
INSERT INTO PKTABLE VALUES (3, 'Test3');
INSERT INTO PKTABLE VALUES (4, 'Test4');
INSERT INTO PKTABLE VALUES (5, 'Test5');

-- Insert successful rows into FK TABLE
INSERT INTO FKTABLE VALUES (1, 2);
INSERT INTO FKTABLE VALUES (2, 3);
INSERT INTO FKTABLE VALUES (3, 4);
INSERT INTO FKTABLE VALUES (NULL, 1);

-- Insert a failed row into FK TABLE
INSERT INTO FKTABLE VALUES (100, 2);

-- Check FKTABLE
SELECT * FROM FKTABLE;

-- Check PKTABLE
SELECT * FROM PKTABLE;

-- Delete a row from PK TABLE (should fail)
DELETE FROM PKTABLE WHERE ptest1=1;

-- Delete a row from PK TABLE (should succeed)
DELETE FROM PKTABLE WHERE ptest1=5;

-- Check PKTABLE for deletes
SELECT * FROM PKTABLE;

-- Update a row from PK TABLE (should fail)
UPDATE PKTABLE SET ptest1=0 WHERE ptest1=2;

-- Update a row from PK TABLE (should succeed)
UPDATE PKTABLE SET ptest1=0 WHERE ptest1=4;

-- Check PKTABLE for updates
SELECT * FROM PKTABLE;

DROP TABLE FKTABLE;
DROP TABLE PKTABLE;


-- MATCH SIMPLE

-- Base test restricting update/delete
CREATE TABLE PKTABLE ( ptest1 int, ptest2 int, ptest3 int, ptest4 text, PRIMARY KEY(ptest1, ptest2, ptest3) );
CREATE TABLE FKTABLE ( ftest1 int, ftest2 int, ftest3 int, ftest4 int,  CONSTRAINT constrname3
			FOREIGN KEY(ftest1, ftest2, ftest3) REFERENCES PKTABLE);

-- Insert Primary Key values
INSERT INTO PKTABLE VALUES (1, 2, 3, 'test1');
INSERT INTO PKTABLE VALUES (1, 3, 3, 'test2');
INSERT INTO PKTABLE VALUES (2, 3, 4, 'test3');
INSERT INTO PKTABLE VALUES (2, 4, 5, 'test4');

-- Insert Foreign Key values
INSERT INTO FKTABLE VALUES (1, 2, 3, 1);
INSERT INTO FKTABLE VALUES (NULL, 2, 3, 2);
INSERT INTO FKTABLE VALUES (2, NULL, 3, 3);
INSERT INTO FKTABLE VALUES (NULL, 2, 7, 4);
INSERT INTO FKTABLE VALUES (NULL, 3, 4, 5);

-- Insert a failed values
INSERT INTO FKTABLE VALUES (1, 2, 7, 6);

-- Show FKTABLE
SELECT * from FKTABLE;

-- Try to update something that should fail
UPDATE PKTABLE set ptest2=5 where ptest2=2;

-- Try to update something that should succeed
UPDATE PKTABLE set ptest1=1 WHERE ptest2=3;

-- Try to delete something that should fail
DELETE FROM PKTABLE where ptest1=1 and ptest2=2 and ptest3=3;

-- Try to delete something that should work
DELETE FROM PKTABLE where ptest1=2;

-- Show PKTABLE and FKTABLE
SELECT * from PKTABLE;

SELECT * from FKTABLE;

DROP TABLE FKTABLE;
DROP TABLE PKTABLE;

-- cascade update/delete
CREATE TABLE PKTABLE ( ptest1 int, ptest2 int, ptest3 int, ptest4 text, PRIMARY KEY(ptest1, ptest2, ptest3) );
CREATE TABLE FKTABLE ( ftest1 int, ftest2 int, ftest3 int, ftest4 int,  CONSTRAINT constrname3
			FOREIGN KEY(ftest1, ftest2, ftest3) REFERENCES PKTABLE
			ON DELETE CASCADE ON UPDATE CASCADE);

-- Insert Primary Key values
INSERT INTO PKTABLE VALUES (1, 2, 3, 'test1');
INSERT INTO PKTABLE VALUES (1, 3, 3, 'test2');
INSERT INTO PKTABLE VALUES (2, 3, 4, 'test3');
INSERT INTO PKTABLE VALUES (2, 4, 5, 'test4');

-- Insert Foreign Key values
INSERT INTO FKTABLE VALUES (1, 2, 3, 1);
INSERT INTO FKTABLE VALUES (NULL, 2, 3, 2);
INSERT INTO FKTABLE VALUES (2, NULL, 3, 3);
INSERT INTO FKTABLE VALUES (NULL, 2, 7, 4);
INSERT INTO FKTABLE VALUES (NULL, 3, 4, 5);

-- Insert a failed values
INSERT INTO FKTABLE VALUES (1, 2, 7, 6);

-- Show FKTABLE
SELECT * from FKTABLE;

-- Try to update something that will cascade
UPDATE PKTABLE set ptest2=5 where ptest2=2;

-- Try to update something that should not cascade
UPDATE PKTABLE set ptest1=1 WHERE ptest2=3;

-- Show PKTABLE and FKTABLE
SELECT * from PKTABLE;
SELECT * from FKTABLE;

-- Try to delete something that should cascade
DELETE FROM PKTABLE where ptest1=1 and ptest2=5 and ptest3=3;

-- Show PKTABLE and FKTABLE
SELECT * from PKTABLE;
SELECT * from FKTABLE;

-- Try to delete something that should not have a cascade
DELETE FROM PKTABLE where ptest1=2;

-- Show PKTABLE and FKTABLE
SELECT * from PKTABLE;
SELECT * from FKTABLE;

DROP TABLE FKTABLE;
DROP TABLE PKTABLE;

-- set null update / set default delete
CREATE TABLE PKTABLE ( ptest1 int, ptest2 int, ptest3 int, ptest4 text, PRIMARY KEY(ptest1, ptest2, ptest3) );
CREATE TABLE FKTABLE ( ftest1 int DEFAULT 0, ftest2 int, ftest3 int, ftest4 int,  CONSTRAINT constrname3
			FOREIGN KEY(ftest1, ftest2, ftest3) REFERENCES PKTABLE
			ON DELETE SET DEFAULT ON UPDATE SET NULL);

-- Insert Primary Key values
INSERT INTO PKTABLE VALUES (1, 2, 3, 'test1');
INSERT INTO PKTABLE VALUES (1, 3, 3, 'test2');
INSERT INTO PKTABLE VALUES (2, 3, 4, 'test3');
INSERT INTO PKTABLE VALUES (2, 4, 5, 'test4');

-- Insert Foreign Key values
INSERT INTO FKTABLE VALUES (1, 2, 3, 1);
INSERT INTO FKTABLE VALUES (2, 3, 4, 1);
INSERT INTO FKTABLE VALUES (NULL, 2, 3, 2);
INSERT INTO FKTABLE VALUES (2, NULL, 3, 3);
INSERT INTO FKTABLE VALUES (NULL, 2, 7, 4);
INSERT INTO FKTABLE VALUES (NULL, 3, 4, 5);

-- Insert a failed values
INSERT INTO FKTABLE VALUES (1, 2, 7, 6);

-- Show FKTABLE
SELECT * from FKTABLE;

-- Try to update something that will set null
UPDATE PKTABLE set ptest2=5 where ptest2=2;

-- Try to update something that should not set null
UPDATE PKTABLE set ptest2=2 WHERE ptest2=3 and ptest1=1;

-- Show PKTABLE and FKTABLE
SELECT * from PKTABLE;
SELECT * from FKTABLE;

-- Try to delete something that should set default
DELETE FROM PKTABLE where ptest1=2 and ptest2=3 and ptest3=4;

-- Show PKTABLE and FKTABLE
SELECT * from PKTABLE;
SELECT * from FKTABLE;

-- Try to delete something that should not set default
DELETE FROM PKTABLE where ptest2=5;

-- Show PKTABLE and FKTABLE
SELECT * from PKTABLE;
SELECT * from FKTABLE;

DROP TABLE FKTABLE;
DROP TABLE PKTABLE;

-- set default update / set null delete
CREATE TABLE PKTABLE ( ptest1 int, ptest2 int, ptest3 int, ptest4 text, PRIMARY KEY(ptest1, ptest2, ptest3) );
CREATE TABLE FKTABLE ( ftest1 int DEFAULT 0, ftest2 int DEFAULT -1, ftest3 int DEFAULT -2, ftest4 int, CONSTRAINT constrname3
			FOREIGN KEY(ftest1, ftest2, ftest3) REFERENCES PKTABLE
			ON DELETE SET NULL ON UPDATE SET DEFAULT);

-- Insert Primary Key values
INSERT INTO PKTABLE VALUES (1, 2, 3, 'test1');
INSERT INTO PKTABLE VALUES (1, 3, 3, 'test2');
INSERT INTO PKTABLE VALUES (2, 3, 4, 'test3');
INSERT INTO PKTABLE VALUES (2, 4, 5, 'test4');
INSERT INTO PKTABLE VALUES (2, -1, 5, 'test5');

-- Insert Foreign Key values
INSERT INTO FKTABLE VALUES (1, 2, 3, 1);
INSERT INTO FKTABLE VALUES (2, 3, 4, 1);
INSERT INTO FKTABLE VALUES (2, 4, 5, 1);
INSERT INTO FKTABLE VALUES (NULL, 2, 3, 2);
INSERT INTO FKTABLE VALUES (2, NULL, 3, 3);
INSERT INTO FKTABLE VALUES (NULL, 2, 7, 4);
INSERT INTO FKTABLE VALUES (NULL, 3, 4, 5);

-- Insert a failed values
INSERT INTO FKTABLE VALUES (1, 2, 7, 6);

-- Show FKTABLE
SELECT * from FKTABLE;

-- Try to update something that will fail
UPDATE PKTABLE set ptest2=5 where ptest2=2;

-- Try to update something that will set default
UPDATE PKTABLE set ptest1=0, ptest2=-1, ptest3=-2 where ptest2=2;
UPDATE PKTABLE set ptest2=10 where ptest2=4;

-- Try to update something that should not set default
UPDATE PKTABLE set ptest2=2 WHERE ptest2=3 and ptest1=1;

-- Show PKTABLE and FKTABLE
SELECT * from PKTABLE;
SELECT * from FKTABLE;

-- Try to delete something that should set null
DELETE FROM PKTABLE where ptest1=2 and ptest2=3 and ptest3=4;

-- Show PKTABLE and FKTABLE
SELECT * from PKTABLE;
SELECT * from FKTABLE;

-- Try to delete something that should not set null
DELETE FROM PKTABLE where ptest2=-1 and ptest3=5;

-- Show PKTABLE and FKTABLE
SELECT * from PKTABLE;
SELECT * from FKTABLE;

DROP TABLE FKTABLE;
DROP TABLE PKTABLE;

CREATE TABLE PKTABLE (ptest1 int PRIMARY KEY);
CREATE TABLE FKTABLE_FAIL1 ( ftest1 int, CONSTRAINT fkfail1 FOREIGN KEY (ftest2) REFERENCES PKTABLE);
CREATE TABLE FKTABLE_FAIL2 ( ftest1 int, CONSTRAINT fkfail1 FOREIGN KEY (ftest1) REFERENCES PKTABLE(ptest2));

DROP TABLE FKTABLE_FAIL1;
DROP TABLE FKTABLE_FAIL2;
DROP TABLE PKTABLE;

-- Test for referencing column number smaller than referenced constraint
CREATE TABLE PKTABLE (ptest1 int, ptest2 int, UNIQUE(ptest1, ptest2));
CREATE TABLE FKTABLE_FAIL1 (ftest1 int REFERENCES pktable(ptest1));

DROP TABLE FKTABLE_FAIL1;
DROP TABLE PKTABLE;

--
-- Tests for mismatched types
--
-- Basic one column, two table setup
CREATE TABLE PKTABLE (ptest1 int PRIMARY KEY);
INSERT INTO PKTABLE VALUES(42);
-- This next should fail, because int=inet does not exist
CREATE TABLE FKTABLE (ftest1 inet REFERENCES pktable);
-- This should also fail for the same reason, but here we
-- give the column name
CREATE TABLE FKTABLE (ftest1 inet REFERENCES pktable(ptest1));
-- This should succeed, even though they are different types,
-- because int=int8 exists and is a member of the integer opfamily
CREATE TABLE FKTABLE (ftest1 int8 REFERENCES pktable);
-- Check it actually works
INSERT INTO FKTABLE VALUES(42);		-- should succeed
INSERT INTO FKTABLE VALUES(43);		-- should fail
UPDATE FKTABLE SET ftest1 = ftest1;	-- should succeed
UPDATE FKTABLE SET ftest1 = ftest1 + 1;	-- should fail
DROP TABLE FKTABLE;
-- This should fail, because we'd have to cast numeric to int which is
-- not an implicit coercion (or use numeric=numeric, but that's not part
-- of the integer opfamily)
CREATE TABLE FKTABLE (ftest1 numeric REFERENCES pktable);
DROP TABLE PKTABLE;
-- On the other hand, this should work because int implicitly promotes to
-- numeric, and we allow promotion on the FK side
CREATE TABLE PKTABLE (ptest1 numeric PRIMARY KEY);
INSERT INTO PKTABLE VALUES(42);
CREATE TABLE FKTABLE (ftest1 int REFERENCES pktable);
-- Check it actually works
INSERT INTO FKTABLE VALUES(42);		-- should succeed
INSERT INTO FKTABLE VALUES(43);		-- should fail
UPDATE FKTABLE SET ftest1 = ftest1;	-- should succeed
UPDATE FKTABLE SET ftest1 = ftest1 + 1;	-- should fail
DROP TABLE FKTABLE;
DROP TABLE PKTABLE;

-- Two columns, two tables
CREATE TABLE PKTABLE (ptest1 int, ptest2 inet, PRIMARY KEY(ptest1, ptest2));
-- This should fail, because we just chose really odd types
CREATE TABLE FKTABLE (ftest1 cidr, ftest2 timestamp, FOREIGN KEY(ftest1, ftest2) REFERENCES pktable);
-- Again, so should this...
CREATE TABLE FKTABLE (ftest1 cidr, ftest2 timestamp, FOREIGN KEY(ftest1, ftest2) REFERENCES pktable(ptest1, ptest2));
-- This fails because we mixed up the column ordering
CREATE TABLE FKTABLE (ftest1 int, ftest2 inet, FOREIGN KEY(ftest2, ftest1) REFERENCES pktable);
-- As does this...
CREATE TABLE FKTABLE (ftest1 int, ftest2 inet, FOREIGN KEY(ftest2, ftest1) REFERENCES pktable(ptest1, ptest2));
-- And again..
CREATE TABLE FKTABLE (ftest1 int, ftest2 inet, FOREIGN KEY(ftest1, ftest2) REFERENCES pktable(ptest2, ptest1));
-- This works...
CREATE TABLE FKTABLE (ftest1 int, ftest2 inet, FOREIGN KEY(ftest2, ftest1) REFERENCES pktable(ptest2, ptest1));
DROP TABLE FKTABLE;
-- As does this
CREATE TABLE FKTABLE (ftest1 int, ftest2 inet, FOREIGN KEY(ftest1, ftest2) REFERENCES pktable(ptest1, ptest2));
DROP TABLE FKTABLE;
DROP TABLE PKTABLE;

-- Two columns, same table
-- Make sure this still works...
CREATE TABLE PKTABLE (ptest1 int, ptest2 inet, ptest3 int, ptest4 inet, PRIMARY KEY(ptest1, ptest2), FOREIGN KEY(ptest3,
ptest4) REFERENCES pktable(ptest1, ptest2));
DROP TABLE PKTABLE;
-- And this,
CREATE TABLE PKTABLE (ptest1 int, ptest2 inet, ptest3 int, ptest4 inet, PRIMARY KEY(ptest1, ptest2), FOREIGN KEY(ptest3,
ptest4) REFERENCES pktable);
DROP TABLE PKTABLE;
-- This shouldn't (mixed up columns)
CREATE TABLE PKTABLE (ptest1 int, ptest2 inet, ptest3 int, ptest4 inet, PRIMARY KEY(ptest1, ptest2), FOREIGN KEY(ptest3,
ptest4) REFERENCES pktable(ptest2, ptest1));
-- Nor should this... (same reason, we have 4,3 referencing 1,2 which mismatches types
CREATE TABLE PKTABLE (ptest1 int, ptest2 inet, ptest3 int, ptest4 inet, PRIMARY KEY(ptest1, ptest2), FOREIGN KEY(ptest4,
ptest3) REFERENCES pktable(ptest1, ptest2));
-- Not this one either... Same as the last one except we didn't defined the columns being referenced.
CREATE TABLE PKTABLE (ptest1 int, ptest2 inet, ptest3 int, ptest4 inet, PRIMARY KEY(ptest1, ptest2), FOREIGN KEY(ptest4,
ptest3) REFERENCES pktable);

--
-- Now some cases with inheritance
-- Basic 2 table case: 1 column of matching types.
create table pktable_base (base1 int not null);
create table pktable (ptest1 int, primary key(base1), unique(base1, ptest1)) inherits (pktable_base);
create table fktable (ftest1 int references pktable(base1));
-- now some ins, upd, del
insert into pktable(base1) values (1);
insert into pktable(base1) values (2);
--  let's insert a non-existent fktable value
insert into fktable(ftest1) values (3);
--  let's make a valid row for that
insert into pktable(base1) values (3);
insert into fktable(ftest1) values (3);
-- let's try removing a row that should fail from pktable
delete from pktable where base1>2;
-- okay, let's try updating all of the base1 values to *4
-- which should fail.
update pktable set base1=base1*4;
-- okay, let's try an update that should work.
update pktable set base1=base1*4 where base1<3;
-- and a delete that should work
delete from pktable where base1>3;
-- cleanup
drop table fktable;
delete from pktable;

-- Now 2 columns 2 tables, matching types
create table fktable (ftest1 int, ftest2 int, foreign key(ftest1, ftest2) references pktable(base1, ptest1));
-- now some ins, upd, del
insert into pktable(base1, ptest1) values (1, 1);
insert into pktable(base1, ptest1) values (2, 2);
--  let's insert a non-existent fktable value
insert into fktable(ftest1, ftest2) values (3, 1);
--  let's make a valid row for that
insert into pktable(base1,ptest1) values (3, 1);
insert into fktable(ftest1, ftest2) values (3, 1);
-- let's try removing a row that should fail from pktable
delete from pktable where base1>2;
-- okay, let's try updating all of the base1 values to *4
-- which should fail.
update pktable set base1=base1*4;
-- okay, let's try an update that should work.
update pktable set base1=base1*4 where base1<3;
-- and a delete that should work
delete from pktable where base1>3;
-- cleanup
drop table fktable;
drop table pktable;
drop table pktable_base;

-- Now we'll do one all in 1 table with 2 columns of matching types
create table pktable_base(base1 int not null, base2 int);
create table pktable(ptest1 int, ptest2 int, primary key(base1, ptest1), foreign key(base2, ptest2) references
                                             pktable(base1, ptest1)) inherits (pktable_base);
insert into pktable (base1, ptest1, base2, ptest2) values (1, 1, 1, 1);
insert into pktable (base1, ptest1, base2, ptest2) values (2, 1, 1, 1);
insert into pktable (base1, ptest1, base2, ptest2) values (2, 2, 2, 1);
insert into pktable (base1, ptest1, base2, ptest2) values (1, 3, 2, 2);
-- fails (3,2) isn't in base1, ptest1
insert into pktable (base1, ptest1, base2, ptest2) values (2, 3, 3, 2);
-- fails (2,2) is being referenced
delete from pktable where base1=2;
-- fails (1,1) is being referenced (twice)
update pktable set base1=3 where base1=1;
-- this sequence of two deletes will work, since after the first there will be no (2,*) references
delete from pktable where base2=2;
delete from pktable where base1=2;
drop table pktable;
drop table pktable_base;

-- 2 columns (2 tables), mismatched types
create table pktable_base(base1 int not null);
create table pktable(ptest1 inet, primary key(base1, ptest1)) inherits (pktable_base);
-- just generally bad types (with and without column references on the referenced table)
create table fktable(ftest1 cidr, ftest2 int[], foreign key (ftest1, ftest2) references pktable);
create table fktable(ftest1 cidr, ftest2 int[], foreign key (ftest1, ftest2) references pktable(base1, ptest1));
-- let's mix up which columns reference which
create table fktable(ftest1 int, ftest2 inet, foreign key(ftest2, ftest1) references pktable);
create table fktable(ftest1 int, ftest2 inet, foreign key(ftest2, ftest1) references pktable(base1, ptest1));
create table fktable(ftest1 int, ftest2 inet, foreign key(ftest1, ftest2) references pktable(ptest1, base1));
drop table pktable;
drop table pktable_base;

-- 2 columns (1 table), mismatched types
create table pktable_base(base1 int not null, base2 int);
create table pktable(ptest1 inet, ptest2 inet[], primary key(base1, ptest1), foreign key(base2, ptest2) references
                                             pktable(base1, ptest1)) inherits (pktable_base);
create table pktable(ptest1 inet, ptest2 inet, primary key(base1, ptest1), foreign key(base2, ptest2) references
                                             pktable(ptest1, base1)) inherits (pktable_base);
create table pktable(ptest1 inet, ptest2 inet, primary key(base1, ptest1), foreign key(ptest2, base2) references
                                             pktable(base1, ptest1)) inherits (pktable_base);
create table pktable(ptest1 inet, ptest2 inet, primary key(base1, ptest1), foreign key(ptest2, base2) references
                                             pktable(base1, ptest1)) inherits (pktable_base);
drop table pktable;
drop table pktable_base;

--
-- Deferrable constraints
--		(right now, only FOREIGN KEY constraints can be deferred)
--

-- deferrable, explicitly deferred
CREATE TABLE pktable (
	id		INT4 PRIMARY KEY,
	other	INT4
);

CREATE TABLE fktable (
	id		INT4 PRIMARY KEY,
	fk		INT4 REFERENCES pktable DEFERRABLE
);

-- default to immediate: should fail
INSERT INTO fktable VALUES (5, 10);

-- explicitly defer the constraint
BEGIN;

SET CONSTRAINTS ALL DEFERRED;

INSERT INTO fktable VALUES (10, 15);
INSERT INTO pktable VALUES (15, 0); -- make the FK insert valid

COMMIT;

DROP TABLE fktable, pktable;

-- deferrable, initially deferred
CREATE TABLE pktable (
	id		INT4 PRIMARY KEY,
	other	INT4
);

CREATE TABLE fktable (
	id		INT4 PRIMARY KEY,
	fk		INT4 REFERENCES pktable DEFERRABLE INITIALLY DEFERRED
);

-- default to deferred, should succeed
BEGIN;

INSERT INTO fktable VALUES (100, 200);
INSERT INTO pktable VALUES (200, 500); -- make the FK insert valid

COMMIT;

-- default to deferred, explicitly make immediate
BEGIN;

SET CONSTRAINTS ALL IMMEDIATE;

-- should fail
INSERT INTO fktable VALUES (500, 1000);

COMMIT;

DROP TABLE fktable, pktable;

-- tricky behavior: according to SQL99, if a deferred constraint is set
-- to 'immediate' mode, it should be checked for validity *immediately*,
-- not when the current transaction commits (i.e. the mode change applies
-- retroactively)
CREATE TABLE pktable (
	id		INT4 PRIMARY KEY,
	other	INT4
);

CREATE TABLE fktable (
	id		INT4 PRIMARY KEY,
	fk		INT4 REFERENCES pktable DEFERRABLE
);

BEGIN;

SET CONSTRAINTS ALL DEFERRED;

-- should succeed, for now
INSERT INTO fktable VALUES (1000, 2000);

-- should cause transaction abort, due to preceding error
SET CONSTRAINTS ALL IMMEDIATE;

INSERT INTO pktable VALUES (2000, 3); -- too late

COMMIT;

DROP TABLE fktable, pktable;

-- deferrable, initially deferred
CREATE TABLE pktable (
	id		INT4 PRIMARY KEY,
	other	INT4
);

CREATE TABLE fktable (
	id		INT4 PRIMARY KEY,
	fk		INT4 REFERENCES pktable DEFERRABLE INITIALLY DEFERRED
);

BEGIN;

-- no error here
INSERT INTO fktable VALUES (100, 200);

-- error here on commit
COMMIT;

DROP TABLE pktable, fktable;

-- test notice about expensive referential integrity checks,
-- where the index cannot be used because of type incompatibilities.

CREATE TABLE pktable (
        id1     INT4 PRIMARY KEY,
        id2     VARCHAR(4) UNIQUE,
        id3     REAL UNIQUE,
        UNIQUE(id1, id2, id3)
);

CREATE TABLE fktable (
        x1      INT4 REFERENCES pktable(id1),
        x2      VARCHAR(4) REFERENCES pktable(id2),
        x3      REAL REFERENCES pktable(id3),
        x4      TEXT,
        x5      INT2
);

-- check individual constraints with alter table.

-- should fail

-- varchar does not promote to real
ALTER TABLE fktable ADD CONSTRAINT fk_2_3
FOREIGN KEY (x2) REFERENCES pktable(id3);

-- nor to int4
ALTER TABLE fktable ADD CONSTRAINT fk_2_1
FOREIGN KEY (x2) REFERENCES pktable(id1);

-- real does not promote to int4
ALTER TABLE fktable ADD CONSTRAINT fk_3_1
FOREIGN KEY (x3) REFERENCES pktable(id1);

-- int4 does not promote to text
ALTER TABLE fktable ADD CONSTRAINT fk_1_2
FOREIGN KEY (x1) REFERENCES pktable(id2);

-- should succeed

-- int4 promotes to real
ALTER TABLE fktable ADD CONSTRAINT fk_1_3
FOREIGN KEY (x1) REFERENCES pktable(id3);

-- text is compatible with varchar
ALTER TABLE fktable ADD CONSTRAINT fk_4_2
FOREIGN KEY (x4) REFERENCES pktable(id2);

-- int2 is part of integer opfamily as of 8.0
ALTER TABLE fktable ADD CONSTRAINT fk_5_1
FOREIGN KEY (x5) REFERENCES pktable(id1);

-- check multikey cases, especially out-of-order column lists

-- these should work

ALTER TABLE fktable ADD CONSTRAINT fk_123_123
FOREIGN KEY (x1,x2,x3) REFERENCES pktable(id1,id2,id3);

ALTER TABLE fktable ADD CONSTRAINT fk_213_213
FOREIGN KEY (x2,x1,x3) REFERENCES pktable(id2,id1,id3);

ALTER TABLE fktable ADD CONSTRAINT fk_253_213
FOREIGN KEY (x2,x5,x3) REFERENCES pktable(id2,id1,id3);

-- these should fail

ALTER TABLE fktable ADD CONSTRAINT fk_123_231
FOREIGN KEY (x1,x2,x3) REFERENCES pktable(id2,id3,id1);

ALTER TABLE fktable ADD CONSTRAINT fk_241_132
FOREIGN KEY (x2,x4,x1) REFERENCES pktable(id1,id3,id2);

DROP TABLE pktable, fktable;

-- test a tricky case: we can elide firing the FK check trigger during
-- an UPDATE if the UPDATE did not change the foreign key
-- field. However, we can't do this if our transaction was the one that
-- created the updated row and the trigger is deferred, since our UPDATE
-- will have invalidated the original newly-inserted tuple, and therefore
-- cause the on-INSERT RI trigger not to be fired.

CREATE TABLE pktable (
    id int primary key,
    other int
);

CREATE TABLE fktable (
    id int primary key,
    fk int references pktable deferrable initially deferred
);

INSERT INTO pktable VALUES (5, 10);

BEGIN;

-- doesn't match PK, but no error yet
INSERT INTO fktable VALUES (0, 20);

-- don't change FK
UPDATE fktable SET id = id + 1;

-- should catch error from initial INSERT
COMMIT;

-- check same case when insert is in a different subtransaction than update

BEGIN;

-- doesn't match PK, but no error yet
INSERT INTO fktable VALUES (0, 20);

-- UPDATE will be in a subxact
SAVEPOINT savept1;

-- don't change FK
UPDATE fktable SET id = id + 1;

-- should catch error from initial INSERT
COMMIT;

BEGIN;

-- INSERT will be in a subxact
SAVEPOINT savept1;

-- doesn't match PK, but no error yet
INSERT INTO fktable VALUES (0, 20);

RELEASE SAVEPOINT savept1;

-- don't change FK
UPDATE fktable SET id = id + 1;

-- should catch error from initial INSERT
COMMIT;

BEGIN;

-- doesn't match PK, but no error yet
INSERT INTO fktable VALUES (0, 20);

-- UPDATE will be in a subxact
SAVEPOINT savept1;

-- don't change FK
UPDATE fktable SET id = id + 1;

-- Roll back the UPDATE
ROLLBACK TO savept1;

-- should catch error from initial INSERT
COMMIT;

--
-- check ALTER CONSTRAINT
--

INSERT INTO fktable VALUES (1, 5);

ALTER TABLE fktable ALTER CONSTRAINT fktable_fk_fkey DEFERRABLE INITIALLY IMMEDIATE;

BEGIN;

-- doesn't match FK, should throw error now
UPDATE pktable SET id = 10 WHERE id = 5;

COMMIT;

BEGIN;

-- doesn't match PK, should throw error now
INSERT INTO fktable VALUES (0, 20);

COMMIT;

-- try additional syntax
ALTER TABLE fktable ALTER CONSTRAINT fktable_fk_fkey NOT DEFERRABLE;
-- illegal option
ALTER TABLE fktable ALTER CONSTRAINT fktable_fk_fkey NOT DEFERRABLE INITIALLY DEFERRED;

-- POLAR_TAG: EQUAL
BEGIN;
SELECT * FROM pktable;
SELECT * FROM fktable;
END;

DROP TABLE pktable, fktable;

-- test order of firing of FK triggers when several RI-induced changes need to
-- be made to the same row.  This was broken by subtransaction-related
-- changes in 8.0.

CREATE TABLE users (
  id INT PRIMARY KEY,
  name VARCHAR NOT NULL
);

INSERT INTO users VALUES (1, 'Jozko');
INSERT INTO users VALUES (2, 'Ferko');
INSERT INTO users VALUES (3, 'Samko');

CREATE TABLE tasks (
  id INT PRIMARY KEY,
  owner INT REFERENCES users ON UPDATE CASCADE ON DELETE SET NULL,
  worker INT REFERENCES users ON UPDATE CASCADE ON DELETE SET NULL,
  checked_by INT REFERENCES users ON UPDATE CASCADE ON DELETE SET NULL
);

INSERT INTO tasks VALUES (1,1,NULL,NULL);
INSERT INTO tasks VALUES (2,2,2,NULL);
INSERT INTO tasks VALUES (3,3,3,3);

SELECT * FROM tasks;

UPDATE users SET id = 4 WHERE id = 3;

SELECT * FROM tasks;

DELETE FROM users WHERE id = 4;

SELECT * FROM tasks;

-- could fail with only 2 changes to make, if row was already updated
BEGIN;
UPDATE tasks set id=id WHERE id=2;
SELECT * FROM tasks;
DELETE FROM users WHERE id = 2;
SELECT * FROM tasks;
COMMIT;

-- POLAR_TAG: EQUAL
BEGIN;
SELECT * FROM tasks;
SELECT * FROM users;
END;

DROP TABLE tasks, users;

--
-- Test self-referential FK with CASCADE (bug #6268)
--
create table selfref (
    a int primary key,
    b int,
    foreign key (b) references selfref (a)
        on update cascade on delete cascade
);

insert into selfref (a, b)
values
    (0, 0),
    (1, 1);

begin;
    update selfref set a = 123 where a = 0;
    select a, b from selfref;
    update selfref set a = 456 where a = 123;
    select a, b from selfref;
commit;

select * from selfref;
DROP TABLE selfref;

--
-- Test that SET DEFAULT actions recognize updates to default values
--
create table defp (f1 int primary key);
create table defc (f1 int default 0
                        references defp on delete set default);
insert into defp values (0), (1), (2);
insert into defc values (2);
select * from defc;
delete from defp where f1 = 2;
select * from defc;
delete from defp where f1 = 0; -- fail
alter table defc alter column f1 set default 1;
delete from defp where f1 = 0;
select * from defc;
delete from defp where f1 = 1; -- fail

drop table defc;
drop table defp;

--
-- Test the difference between NO ACTION and RESTRICT
--
create table pp (f1 int primary key);
create table cc (f1 int references pp on update no action);
insert into pp values(12);
insert into pp values(11);
update pp set f1=f1+1;
insert into cc values(13);
update pp set f1=f1+1;
update pp set f1=f1+1; -- fail
select * from pp;
select * from cc;
drop table pp, cc;

create table pp (f1 int primary key);
create table cc (f1 int references pp on update restrict);
insert into pp values(12);
insert into pp values(11);
update pp set f1=f1+1;
insert into cc values(13);
update pp set f1=f1+1; -- fail
select * from pp;
select * from cc;
drop table pp, cc;
-- POLAR_SESSION_BEGIN
-- POLAR_TAG: REPLICA_ERR
--
-- Test interaction of foreign-key optimization with rules (bug #14219)
--
create temp table t1 (a integer primary key, b text);
create temp table t2 (a integer primary key, b integer references t1);
create rule r1 as on delete to t1 do delete from t2 where t2.b = old.a;

-- explain (costs off) delete from t1 where a = 1;
delete from t1 where a = 1;
-- POLAR_SESSION_END
-- Test a primary key with attributes located in later attnum positions
-- compared to the fk attributes.
create table pktable2 (a int, b int, c int, d int, e int, primary key (d, e));
create table fktable2 (d int, e int, foreign key (d, e) references pktable2);
insert into pktable2 values (1, 2, 3, 4, 5);
insert into fktable2 values (4, 5);
delete from pktable2;
update pktable2 set d = 5;
drop table pktable2, fktable2;

--
-- Test deferred FK check on a tuple deleted by a rolled-back subtransaction
--
create table pktable2(f1 int primary key);
create table fktable2(f1 int references pktable2 deferrable initially deferred);
insert into pktable2 values(1);

begin;
insert into fktable2 values(1);
savepoint x;
delete from fktable2;
rollback to x;
commit;

begin;
insert into fktable2 values(2);
savepoint x;
delete from fktable2;
rollback to x;
commit; -- fail

--
-- Test that we prevent dropping FK constraint with pending trigger events
--
begin;
insert into fktable2 values(2);
alter table fktable2 drop constraint fktable2_f1_fkey;
commit;

begin;
delete from pktable2 where f1 = 1;
alter table fktable2 drop constraint fktable2_f1_fkey;
commit;

drop table pktable2, fktable2;


--
-- Foreign keys and partitioned tables
--

-- partitioned table in the referenced side are not allowed
CREATE TABLE fk_partitioned_pk (a int, b int, primary key (a, b))
  PARTITION BY RANGE (a, b);
-- verify with create table first ...
CREATE TABLE fk_notpartitioned_fk (a int, b int,
  FOREIGN KEY (a, b) REFERENCES fk_partitioned_pk);
-- and then with alter table.
CREATE TABLE fk_notpartitioned_fk_2 (a int, b int);
ALTER TABLE fk_notpartitioned_fk_2 ADD FOREIGN KEY (a, b)
  REFERENCES fk_partitioned_pk;
DROP TABLE fk_partitioned_pk, fk_notpartitioned_fk_2;

-- Creation of a partitioned hierarchy with irregular definitions
CREATE TABLE fk_notpartitioned_pk (fdrop1 int, a int, fdrop2 int, b int,
  PRIMARY KEY (a, b));
ALTER TABLE fk_notpartitioned_pk DROP COLUMN fdrop1, DROP COLUMN fdrop2;
CREATE TABLE fk_partitioned_fk (b int, fdrop1 int, a int) PARTITION BY RANGE (a, b);
ALTER TABLE fk_partitioned_fk DROP COLUMN fdrop1;
CREATE TABLE fk_partitioned_fk_1 (fdrop1 int, fdrop2 int, a int, fdrop3 int, b int);
ALTER TABLE fk_partitioned_fk_1 DROP COLUMN fdrop1, DROP COLUMN fdrop2, DROP COLUMN fdrop3;
ALTER TABLE fk_partitioned_fk ATTACH PARTITION fk_partitioned_fk_1 FOR VALUES FROM (0,0) TO (1000,1000);
ALTER TABLE fk_partitioned_fk ADD FOREIGN KEY (a, b) REFERENCES fk_notpartitioned_pk;
CREATE TABLE fk_partitioned_fk_2 (b int, fdrop1 int, fdrop2 int, a int);
ALTER TABLE fk_partitioned_fk_2 DROP COLUMN fdrop1, DROP COLUMN fdrop2;
ALTER TABLE fk_partitioned_fk ATTACH PARTITION fk_partitioned_fk_2 FOR VALUES FROM (1000,1000) TO (2000,2000);

CREATE TABLE fk_partitioned_fk_3 (fdrop1 int, fdrop2 int, fdrop3 int, fdrop4 int, b int, a int)
  PARTITION BY HASH (a);
ALTER TABLE fk_partitioned_fk_3 DROP COLUMN fdrop1, DROP COLUMN fdrop2,
	DROP COLUMN fdrop3, DROP COLUMN fdrop4;
CREATE TABLE fk_partitioned_fk_3_0 PARTITION OF fk_partitioned_fk_3 FOR VALUES WITH (MODULUS 5, REMAINDER 0);
CREATE TABLE fk_partitioned_fk_3_1 PARTITION OF fk_partitioned_fk_3 FOR VALUES WITH (MODULUS 5, REMAINDER 1);
ALTER TABLE fk_partitioned_fk ATTACH PARTITION fk_partitioned_fk_3
  FOR VALUES FROM (2000,2000) TO (3000,3000);

-- Creating a foreign key with ONLY on a partitioned table referencing
-- a non-partitioned table fails.
ALTER TABLE ONLY fk_partitioned_fk ADD FOREIGN KEY (a, b)
  REFERENCES fk_notpartitioned_pk;
-- Adding a NOT VALID foreign key on a partitioned table referencing
-- a non-partitioned table fails.
ALTER TABLE fk_partitioned_fk ADD FOREIGN KEY (a, b)
  REFERENCES fk_notpartitioned_pk NOT VALID;

-- these inserts, targetting both the partition directly as well as the
-- partitioned table, should all fail
INSERT INTO fk_partitioned_fk (a,b) VALUES (500, 501);
INSERT INTO fk_partitioned_fk_1 (a,b) VALUES (500, 501);
INSERT INTO fk_partitioned_fk (a,b) VALUES (1500, 1501);
INSERT INTO fk_partitioned_fk_2 (a,b) VALUES (1500, 1501);
INSERT INTO fk_partitioned_fk (a,b) VALUES (2500, 2502);
INSERT INTO fk_partitioned_fk_3 (a,b) VALUES (2500, 2502);
INSERT INTO fk_partitioned_fk (a,b) VALUES (2501, 2503);
INSERT INTO fk_partitioned_fk_3 (a,b) VALUES (2501, 2503);

-- but if we insert the values that make them valid, then they work
INSERT INTO fk_notpartitioned_pk VALUES (500, 501), (1500, 1501),
  (2500, 2502), (2501, 2503);
INSERT INTO fk_partitioned_fk (a,b) VALUES (500, 501);
INSERT INTO fk_partitioned_fk (a,b) VALUES (1500, 1501);
INSERT INTO fk_partitioned_fk (a,b) VALUES (2500, 2502);
INSERT INTO fk_partitioned_fk (a,b) VALUES (2501, 2503);

-- this update fails because there is no referenced row
UPDATE fk_partitioned_fk SET a = a + 1 WHERE a = 2501;
-- but we can fix it thusly:
INSERT INTO fk_notpartitioned_pk (a,b) VALUES (2502, 2503);
UPDATE fk_partitioned_fk SET a = a + 1 WHERE a = 2501;

-- these updates would leave lingering rows in the referencing table; disallow
UPDATE fk_notpartitioned_pk SET b = 502 WHERE a = 500;
UPDATE fk_notpartitioned_pk SET b = 1502 WHERE a = 1500;
UPDATE fk_notpartitioned_pk SET b = 2504 WHERE a = 2500;
ALTER TABLE fk_partitioned_fk DROP CONSTRAINT fk_partitioned_fk_a_fkey;
-- done.
DROP TABLE fk_notpartitioned_pk, fk_partitioned_fk;

-- Altering a type referenced by a foreign key needs to drop/recreate the FK.
-- Ensure that works.
CREATE TABLE fk_notpartitioned_pk (a INT, PRIMARY KEY(a), CHECK (a > 0));
CREATE TABLE fk_partitioned_fk (a INT REFERENCES fk_notpartitioned_pk(a) PRIMARY KEY) PARTITION BY RANGE(a);
CREATE TABLE fk_partitioned_fk_1 PARTITION OF fk_partitioned_fk FOR VALUES FROM (MINVALUE) TO (MAXVALUE);
INSERT INTO fk_notpartitioned_pk VALUES (1);
INSERT INTO fk_partitioned_fk VALUES (1);
ALTER TABLE fk_notpartitioned_pk ALTER COLUMN a TYPE bigint;
DELETE FROM fk_notpartitioned_pk WHERE a = 1;
DROP TABLE fk_notpartitioned_pk, fk_partitioned_fk;

-- Test some other exotic foreign key features: MATCH SIMPLE, ON UPDATE/DELETE
-- actions
CREATE TABLE fk_notpartitioned_pk (a int, b int, primary key (a, b));
CREATE TABLE fk_partitioned_fk (a int default 2501, b int default 142857) PARTITION BY LIST (a);
CREATE TABLE fk_partitioned_fk_1 PARTITION OF fk_partitioned_fk FOR VALUES IN (NULL,500,501,502);
ALTER TABLE fk_partitioned_fk ADD FOREIGN KEY (a, b)
  REFERENCES fk_notpartitioned_pk MATCH SIMPLE
  ON DELETE SET NULL ON UPDATE SET NULL;
CREATE TABLE fk_partitioned_fk_2 PARTITION OF fk_partitioned_fk FOR VALUES IN (1500,1502);
CREATE TABLE fk_partitioned_fk_3 (a int, b int);
ALTER TABLE fk_partitioned_fk ATTACH PARTITION fk_partitioned_fk_3 FOR VALUES IN (2500,2501,2502,2503);

-- this insert fails
INSERT INTO fk_partitioned_fk (a, b) VALUES (2502, 2503);
INSERT INTO fk_partitioned_fk_3 (a, b) VALUES (2502, 2503);
-- but since the FK is MATCH SIMPLE, this one doesn't
INSERT INTO fk_partitioned_fk_3 (a, b) VALUES (2502, NULL);
-- now create the referenced row ...
INSERT INTO fk_notpartitioned_pk VALUES (2502, 2503);
--- and now the same insert work
INSERT INTO fk_partitioned_fk_3 (a, b) VALUES (2502, 2503);
-- this always works
INSERT INTO fk_partitioned_fk (a,b) VALUES (NULL, NULL);

-- ON UPDATE SET NULL
SELECT tableoid::regclass, a, b FROM fk_partitioned_fk WHERE b IS NULL ORDER BY a;
UPDATE fk_notpartitioned_pk SET a = a + 1 WHERE a = 2502;
SELECT tableoid::regclass, a, b FROM fk_partitioned_fk WHERE b IS NULL ORDER BY a;

-- ON DELETE SET NULL
INSERT INTO fk_partitioned_fk VALUES (2503, 2503);
SELECT count(*) FROM fk_partitioned_fk WHERE a IS NULL;
DELETE FROM fk_notpartitioned_pk;
SELECT count(*) FROM fk_partitioned_fk WHERE a IS NULL;

-- ON UPDATE/DELETE SET DEFAULT
ALTER TABLE fk_partitioned_fk DROP CONSTRAINT fk_partitioned_fk_a_fkey;
ALTER TABLE fk_partitioned_fk ADD FOREIGN KEY (a, b)
  REFERENCES fk_notpartitioned_pk
  ON DELETE SET DEFAULT ON UPDATE SET DEFAULT;
INSERT INTO fk_notpartitioned_pk VALUES (2502, 2503);
INSERT INTO fk_partitioned_fk_3 (a, b) VALUES (2502, 2503);
-- this fails, because the defaults for the referencing table are not present
-- in the referenced table:
UPDATE fk_notpartitioned_pk SET a = 1500 WHERE a = 2502;
-- but inserting the row we can make it work:
INSERT INTO fk_notpartitioned_pk VALUES (2501, 142857);
UPDATE fk_notpartitioned_pk SET a = 1500 WHERE a = 2502;
SELECT * FROM fk_partitioned_fk WHERE b = 142857;

-- ON UPDATE/DELETE CASCADE
ALTER TABLE fk_partitioned_fk DROP CONSTRAINT fk_partitioned_fk_a_fkey;
ALTER TABLE fk_partitioned_fk ADD FOREIGN KEY (a, b)
  REFERENCES fk_notpartitioned_pk
  ON DELETE CASCADE ON UPDATE CASCADE;
UPDATE fk_notpartitioned_pk SET a = 2502 WHERE a = 2501;
SELECT * FROM fk_partitioned_fk WHERE b = 142857;

-- Now you see it ...
SELECT * FROM fk_partitioned_fk WHERE b = 142857;
DELETE FROM fk_notpartitioned_pk WHERE b = 142857;
-- now you don't.
SELECT * FROM fk_partitioned_fk WHERE a = 142857;

-- verify that DROP works
DROP TABLE fk_partitioned_fk_2;

-- Test behavior of the constraint together with attaching and detaching
-- partitions.
CREATE TABLE fk_partitioned_fk_2 PARTITION OF fk_partitioned_fk FOR VALUES IN (1500,1502);
ALTER TABLE fk_partitioned_fk DETACH PARTITION fk_partitioned_fk_2;
BEGIN;
DROP TABLE fk_partitioned_fk;
-- constraint should still be there
\d fk_partitioned_fk_2;
ROLLBACK;
ALTER TABLE fk_partitioned_fk ATTACH PARTITION fk_partitioned_fk_2 FOR VALUES IN (1500,1502);
DROP TABLE fk_partitioned_fk_2;
CREATE TABLE fk_partitioned_fk_2 (b int, c text, a int,
	FOREIGN KEY (a, b) REFERENCES fk_notpartitioned_pk ON UPDATE CASCADE ON DELETE CASCADE);
ALTER TABLE fk_partitioned_fk_2 DROP COLUMN c;
ALTER TABLE fk_partitioned_fk ATTACH PARTITION fk_partitioned_fk_2 FOR VALUES IN (1500,1502);
-- should have only one constraint
\d fk_partitioned_fk_2
DROP TABLE fk_partitioned_fk_2;

CREATE TABLE fk_partitioned_fk_4 (a int, b int, FOREIGN KEY (a, b) REFERENCES fk_notpartitioned_pk(a, b) ON UPDATE CASCADE ON DELETE CASCADE) PARTITION BY RANGE (b, a);
CREATE TABLE fk_partitioned_fk_4_1 PARTITION OF fk_partitioned_fk_4 FOR VALUES FROM (1,1) TO (100,100);
CREATE TABLE fk_partitioned_fk_4_2 (a int, b int, FOREIGN KEY (a, b) REFERENCES fk_notpartitioned_pk(a, b) ON UPDATE SET NULL);
ALTER TABLE fk_partitioned_fk_4 ATTACH PARTITION fk_partitioned_fk_4_2 FOR VALUES FROM (100,100) TO (1000,1000);
ALTER TABLE fk_partitioned_fk ATTACH PARTITION fk_partitioned_fk_4 FOR VALUES IN (3500,3502);
ALTER TABLE fk_partitioned_fk DETACH PARTITION fk_partitioned_fk_4;
ALTER TABLE fk_partitioned_fk ATTACH PARTITION fk_partitioned_fk_4 FOR VALUES IN (3500,3502);
-- should only have one constraint
\d fk_partitioned_fk_4
\d fk_partitioned_fk_4_1
-- this one has an FK with mismatched properties
\d fk_partitioned_fk_4_2

CREATE TABLE fk_partitioned_fk_5 (a int, b int,
	FOREIGN KEY (a, b) REFERENCES fk_notpartitioned_pk(a, b) ON UPDATE CASCADE ON DELETE CASCADE DEFERRABLE,
	FOREIGN KEY (a, b) REFERENCES fk_notpartitioned_pk(a, b) MATCH FULL ON UPDATE CASCADE ON DELETE CASCADE)
  PARTITION BY RANGE (a);
CREATE TABLE fk_partitioned_fk_5_1 (a int, b int, FOREIGN KEY (a, b) REFERENCES fk_notpartitioned_pk);
ALTER TABLE fk_partitioned_fk ATTACH PARTITION fk_partitioned_fk_5 FOR VALUES IN (4500);
ALTER TABLE fk_partitioned_fk_5 ATTACH PARTITION fk_partitioned_fk_5_1 FOR VALUES FROM (0) TO (10);
ALTER TABLE fk_partitioned_fk DETACH PARTITION fk_partitioned_fk_5;
ALTER TABLE fk_partitioned_fk ATTACH PARTITION fk_partitioned_fk_5 FOR VALUES IN (4500);
-- this one has two constraints, similar but not quite the one in the parent,
-- so it gets a new one
\d fk_partitioned_fk_5
-- verify that it works to reattaching a child with multiple candidate
-- constraints
ALTER TABLE fk_partitioned_fk_5 DETACH PARTITION fk_partitioned_fk_5_1;
ALTER TABLE fk_partitioned_fk_5 ATTACH PARTITION fk_partitioned_fk_5_1 FOR VALUES FROM (0) TO (10);
\d fk_partitioned_fk_5_1

-- verify that attaching a table checks that the existing data satisfies the
-- constraint
CREATE TABLE fk_partitioned_fk_2 (a int, b int) PARTITION BY RANGE (b);
CREATE TABLE fk_partitioned_fk_2_1 PARTITION OF fk_partitioned_fk_2 FOR VALUES FROM (0) TO (1000);
CREATE TABLE fk_partitioned_fk_2_2 PARTITION OF fk_partitioned_fk_2 FOR VALUES FROM (1000) TO (2000);
INSERT INTO fk_partitioned_fk_2 VALUES (1600, 601), (1600, 1601);
ALTER TABLE fk_partitioned_fk ATTACH PARTITION fk_partitioned_fk_2
  FOR VALUES IN (1600);
INSERT INTO fk_notpartitioned_pk VALUES (1600, 601), (1600, 1601);
ALTER TABLE fk_partitioned_fk ATTACH PARTITION fk_partitioned_fk_2
  FOR VALUES IN (1600);

-- leave these tables around intentionally

-- Test creating a constraint at the parent that already exists in partitions.
-- There should be no duplicated constraints, and attempts to drop the
-- constraint in partitions should raise appropriate errors.
create schema fkpart0
  create table pkey (a int primary key)
  create table fk_part (a int) partition by list (a)
  create table fk_part_1 partition of fk_part
      (foreign key (a) references fkpart0.pkey) for values in (1)
  create table fk_part_23 partition of fk_part
      (foreign key (a) references fkpart0.pkey) for values in (2, 3)
      partition by list (a)
  create table fk_part_23_2 partition of fk_part_23 for values in (2);

alter table fkpart0.fk_part add foreign key (a) references fkpart0.pkey;
\d fkpart0.fk_part_1	\\ -- should have only one FK
alter table fkpart0.fk_part_1 drop constraint fk_part_1_a_fkey;

\d fkpart0.fk_part_23	\\ -- should have only one FK
\d fkpart0.fk_part_23_2	\\ -- should have only one FK
alter table fkpart0.fk_part_23 drop constraint fk_part_23_a_fkey;
alter table fkpart0.fk_part_23_2 drop constraint fk_part_23_a_fkey;

create table fkpart0.fk_part_4 partition of fkpart0.fk_part for values in (4);
\d fkpart0.fk_part_4
alter table fkpart0.fk_part_4 drop constraint fk_part_a_fkey;

create table fkpart0.fk_part_56 partition of fkpart0.fk_part
    for values in (5,6) partition by list (a);
create table fkpart0.fk_part_56_5 partition of fkpart0.fk_part_56
    for values in (5);
\d fkpart0.fk_part_56
alter table fkpart0.fk_part_56 drop constraint fk_part_a_fkey;
alter table fkpart0.fk_part_56_5 drop constraint fk_part_a_fkey;

-- verify that attaching and detaching partitions maintains the right set of
-- triggers
create schema fkpart1
  create table pkey (a int primary key)
  create table fk_part (a int) partition by list (a)
  create table fk_part_1 partition of fk_part for values in (1) partition by list (a)
  create table fk_part_1_1 partition of fk_part_1 for values in (1);
alter table fkpart1.fk_part add foreign key (a) references fkpart1.pkey;
insert into fkpart1.fk_part values (1);		-- should fail
insert into fkpart1.pkey values (1);
insert into fkpart1.fk_part values (1);
delete from fkpart1.pkey where a = 1;		-- should fail
alter table fkpart1.fk_part detach partition fkpart1.fk_part_1;
create table fkpart1.fk_part_1_2 partition of fkpart1.fk_part_1 for values in (2);
insert into fkpart1.fk_part_1 values (2);	-- should fail
delete from fkpart1.pkey where a = 1;

-- verify that attaching and detaching partitions manipulates the inheritance
-- properties of their FK constraints correctly
create schema fkpart2
  create table pkey (a int primary key)
  create table fk_part (a int, constraint fkey foreign key (a) references fkpart2.pkey) partition by list (a)
  create table fk_part_1 partition of fkpart2.fk_part for values in (1) partition by list (a)
  create table fk_part_1_1 (a int, constraint my_fkey foreign key (a) references fkpart2.pkey);
alter table fkpart2.fk_part_1 attach partition fkpart2.fk_part_1_1 for values in (1);
alter table fkpart2.fk_part_1 drop constraint fkey;	-- should fail
alter table fkpart2.fk_part_1_1 drop constraint my_fkey;	-- should fail
alter table fkpart2.fk_part detach partition fkpart2.fk_part_1;
alter table fkpart2.fk_part_1 drop constraint fkey;	-- ok
alter table fkpart2.fk_part_1_1 drop constraint my_fkey;	-- doesn't exist

\set VERBOSITY terse	\\ -- suppress cascade details
drop schema fkpart0, fkpart1, fkpart2 cascade;
\set VERBOSITY default
