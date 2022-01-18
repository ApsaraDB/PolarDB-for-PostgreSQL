--
-- Test for polar_superuser kind role or user to execute grant statements.
--

-- Clean up in case a prior regression run failed
SET client_min_messages TO 'warning';

DROP DATABASE IF EXISTS rds_test_db;
DROP ROLE IF EXISTS granter_rs_superuser;
DROP ROLE IF EXISTS granter_test;
DROP ROLE IF EXISTS granter_test1;
DROP ROLE IF EXISTS grantee_test_another;
DROP ROLE IF EXISTS postgres;

RESET client_min_messages;

CREATE DATABASE rds_test_db;
\c rds_test_db

CREATE ROLE granter_rs_superuser POLAR_SUPERUSER LOGIN;
CREATE ROLE granter_test LOGIN;
CREATE ROLE granter_test1 LOGIN;
CREATE ROLE grantee_test_another LOGIN;
CREATE ROLE postgres SUPERUSER LOGIN;



-- Test case related with issue 385. Here we use this scenario also verify if the privilege is really granted to the grantee.
-- For other object grant related statement, the logic of implement is same. 
\c - postgres
DROP SCHEMA IF EXISTS schema_test;
DROP SCHEMA IF EXISTS schema_test1;

CREATE SCHEMA schema_test1 AUTHORIZATION postgres;
create schema schema_test authorization granter_test;

-- 1 Granter_rs_superuser is the polar_superuser, could grant all privileges of schema that not belong to superuser.
\c - grantee_test_another
create table schema_test.a ( a integer, b integer);

\c - granter_rs_superuser
GRANT ALL ON schema schema_test TO grantee_test_another;

--Check if the related privilege is granted successful.
\c - grantee_test_another
create table schema_test.a ( a integer, b integer);


-- 2 Schema schema_test1 is belong to superuser postgres, so polar_superuser could not grant privileges to others before he own the privilege.
GRANT ALL ON SCHEMA schema_test1 TO grantee_test_another;

-- 3 None polar_superuser could not grant privileges that he did not own to others.
\c - granter_test1  
GRANT ALL ON SCHEMA schema_test to grantee_test_another;

-- 4 When polar_superuser has been granted correlated privilege, then he could grant the privilege to others, if grant options has been assigned.

\c - grantee_test_another
create table schema_test1.a ( a integer, b integer);

\c - postgres
GRANT CREATE ON SCHEMA schema_test1 TO granter_rs_superuser;

\c - granter_rs_superuser;

GRANT CREATE ON SCHEMA schema_test1 TO grantee_test_another;

\c - postgres
GRANT CREATE ON SCHEMA schema_test1 TO granter_rs_superuser WITH GRANT OPTION;

\c - granter_rs_superuser;

GRANT CREATE ON SCHEMA schema_test1 TO grantee_test_another;
--Check if the related privilege is granted successful.
\c - grantee_test_another
create table schema_test1.a ( a integer, b integer);


--5 When none polar_superuser has been granted correlated privilege, then he could grant the privilege to others, if grant options has been assigned. 

\c - postgres
REVOKE ALL ON SCHEMA schema_test1 to grantee_test_another;
GRANT CREATE ON SCHEMA schema_test1 TO granter_test1 WITH GRANT OPTION;
\c - granter_test1
GRANT USAGE ON SCHEMA schema_test1 To grantee_test_another;
GRANT CREATE ON SCHEMA schema_test1 To grantee_test_another;

\c - grantee_test_another
create table schema_test1.b ( a integer, b integer);

\c - postgres
DROP SCHEMA IF EXISTS schema_test;
DROP SCHEMA IF EXISTS schema_test1;


-- Grant statement for table and column;
\c - postgres
DROP TABLE IF EXISTS table_a;
DROP TABLE IF EXISTS table_test_a;

create table table_a(a integer, b integer);
\c - granter_test
create table table_test_a(a integer, b integer);

--1 & 2 Granter_rs_superuser is the polar_superuser, could grant all privileges of table and column that not belong to superuser.
-- table_a is belong to superuser postgres, so polar_superuser could not grant privileges to others before he own the privilege.
\c - granter_rs_superuser
GRANT ALL ON table_test_a to grantee_test_another;
GRANT ALL ON table_a to grantee_test_another;
GRANT ALL (a) ON table_test_a to grantee_test_another;
GRANT ALL (a) ON table_a to grantee_test_another;

--3 None polar_superuser could not grant privileges that he did not own to others.
\c - granter_test1
GRANT ALL ON table_test_a to grantee_test_another;
GRANT ALL (a) ON table_test_a to grantee_test_another;

--4 When polar_superuser has been granted correlated privilege, then he could grant the privilege to others, if grant options has been assigned.
\c - postgres
GRANT SELECT ON table_a TO granter_rs_superuser;
GRANT SELECT (a) ON table_a TO granter_rs_superuser;

\c - granter_rs_superuser;

GRANT SELECT ON table_a TO grantee_test_another;
GRANT SELECT (a) ON table_a TO grantee_test_another;

\c - postgres
GRANT SELECT ON table_a TO granter_rs_superuser WITH GRANT OPTION;
GRANT SELECT (a) ON table_a TO granter_rs_superuser WITH GRANT OPTION;

\c - granter_rs_superuser;

GRANT SELECT ON table_a TO grantee_test_another;
GRANT SELECT (a) ON table_a TO grantee_test_another;

--5 When none polar_superuser has been granted correlated privilege, then he could grant the privilege to others, if grant options has been assigned. 
\c - postgres
GRANT INSERT ON table_a TO granter_test1 WITH GRANT OPTION;
GRANT INSERT (a) ON table_a TO granter_test1 WITH GRANT OPTION;
\c - granter_test1
GRANT UPDATE ON table_a To grantee_test_another;
GRANT INSERT ON table_a To grantee_test_another;
GRANT UPDATE (a) ON table_a To grantee_test_another;
GRANT INSERT (a) ON table_a To grantee_test_another;

\c - postgres
DROP TABLE IF EXISTS table_a;
DROP TABLE IF EXISTS table_test_a;


-- Grant statement for sequence;
\c - postgres
DROP SEQUENCE IF EXISTS sequence_a;
DROP SEQUENCE IF EXISTS sequence_test_a;

CREATE SEQUENCE sequence_a START 1;

\c - granter_test
CREATE SEQUENCE sequence_test_a START 1;

-- 1 & 2 Granter_rs_superuser is the polar_superuser, could grant all privileges of sequence that not belong to superuser.
-- sequence_a is belong to superuser postgres, so polar_superuser could not grant privileges to others before he own the privilege.
\c - granter_rs_superuser
GRANT ALL ON sequence_test_a to grantee_test_another;
GRANT ALL ON sequence_a to grantee_test_another;

-- 3 None polar_superuser could not grant privileges that he did not own to others.
\c - granter_test1
GRANT ALL ON sequence_test_a to grantee_test_another;

-- 4 When polar_superuser has been granted correlated privilege, then he could grant the privilege to others, if grant options has been assigned.
\c - postgres
GRANT SELECT ON sequence_a TO granter_rs_superuser;

\c - granter_rs_superuser;

GRANT SELECT ON sequence_a TO grantee_test_another;

\c - postgres
GRANT SELECT ON sequence_a TO granter_rs_superuser WITH GRANT OPTION;

\c - granter_rs_superuser;

GRANT SELECT ON sequence_a TO grantee_test_another;

-- 5 When none polar_superuser has been granted correlated privilege, then he could grant the privilege to others, if grant options has been assigned. 
\c - postgres
GRANT UPDATE ON sequence_a TO granter_test1 WITH GRANT OPTION;
\c - granter_test1
GRANT SELECT ON sequence_a To grantee_test_another;
GRANT UPDATE ON sequence_a To grantee_test_another;

\c - postgres
DROP SEQUENCE IF EXISTS sequence_a;
DROP SEQUENCE IF EXISTS sequence_test_a;

-- Grant statement for database;
\c - postgres 
DROP DATABASE IF EXISTS database_a;
DROP DATABASE IF EXISTS database_test_a;
CREATE DATABASE database_a;
CREATE DATABASE database_test_a WITH OWNER = granter_test;



-- 1 & 2 Granter_rs_superuser is the polar_superuser, could grant all privileges of database that not belong to superuser.
-- database_a is belong to superuser postgres, so polar_superuser could not grant privileges to others before he own the privilege.
\c - granter_rs_superuser
GRANT ALL ON DATABASE database_a to grantee_test_another;
GRANT ALL ON DATABASE database_test_a to grantee_test_another;

--3 None polar_superuser could not grant privileges that he did not own to others.
\c - granter_test1
GRANT ALL ON DATABASE database_test_a to grantee_test_another;

--4 When polar_superuser has been granted correlated privilege, then he could grant the privilege to others, if grant options has been assigned.
\c - postgres
GRANT CREATE ON DATABASE database_a TO granter_rs_superuser;

\c - granter_rs_superuser;

GRANT CREATE ON DATABASE database_a TO grantee_test_another;

\c - postgres
GRANT CREATE ON DATABASE database_a TO granter_rs_superuser WITH GRANT OPTION;

\c - granter_rs_superuser;

GRANT CREATE ON DATABASE database_a TO grantee_test_another;

--5 When none polar_superuser has been granted correlated privilege, then he could grant the privilege to others, if grant options has been assigned. 
\c - postgres
GRANT CONNECT ON DATABASE database_a TO granter_test1 WITH GRANT OPTION;
\c - granter_test1
GRANT CREATE ON DATABASE database_a To grantee_test_another;
GRANT CONNECT ON DATABASE database_a To grantee_test_another;

 
-- Grant statement for domain;
\c - postgres
DROP DOMAIN IF EXISTS domain_a;
DROP DOMAIN IF EXISTS domain_test_a;

CREATE DOMAIN domain_a AS TEXT
CHECK(
   VALUE ~ '^\d{5}$'
OR VALUE ~ '^\d{5}-\d{4}$'
);

\c - granter_test
CREATE DOMAIN domain_test_a AS TEXT
CHECK(
   VALUE ~ '^\d{5}$'
OR VALUE ~ '^\d{5}-\d{4}$'
);

-- 1 & 2 Granter_rs_superuser is the polar_superuser, could grant all privileges of domain that not belong to superuser.
-- domain_a is belong to superuser postgres, so polar_superuser could not grant privileges to others before he own the privilege.
\c - granter_rs_superuser
GRANT ALL ON DOMAIN domain_test_a to grantee_test_another;
GRANT ALL ON DOMAIN domain_a to grantee_test_another;

-- 3 None polar_superuser could not grant privileges that he did not own to others.
\c - granter_test1
GRANT ALL ON DOMAIN domain_test_a to grantee_test_another;

-- 4 When polar_superuser has been granted correlated privilege, then he could grant the privilege to others, if grant options has been assigned.
\c - postgres
GRANT ALL ON DOMAIN domain_a TO granter_rs_superuser;

\c - granter_rs_superuser;

GRANT ALL ON DOMAIN domain_a TO grantee_test_another;

\c - postgres
GRANT USAGE ON DOMAIN domain_a TO granter_rs_superuser WITH GRANT OPTION;

\c - granter_rs_superuser;

GRANT USAGE ON DOMAIN domain_a TO grantee_test_another;

-- 5 When none polar_superuser has been granted correlated privilege, then he could grant the privilege to others, if grant options has been assigned. 
\c - postgres
GRANT USAGE ON DOMAIN domain_a TO granter_test1 WITH GRANT OPTION;
\c - granter_test1
-- DOMAIN object only has USAGE privileges, so Grant ALL = Grant USAGE.
GRANT ALL ON DOMAIN domain_a To grantee_test_another;
GRANT USAGE ON DOMAIN domain_a To grantee_test_another;

\c - postgres
DROP DOMAIN IF EXISTS domain_a;
DROP DOMAIN IF EXISTS domain_test_a;


-- Grant statement for foreign data wrapper. Because the foreign data wrapper 
-- could only be created by superuser, so the testing scenario will be re-design.

-- 1 superuser create one foreign data wrapper
\c - postgres
DROP FOREIGN DATA WRAPPER IF EXISTS fdw_a;

CREATE FOREIGN DATA WRAPPER fdw_a
    OPTIONS (debug 'true');

GRANT USAGE ON FOREIGN DATA WRAPPER fdw_a to granter_test1;

\c - granter_rs_superuser
GRANT ALL ON FOREIGN DATA WRAPPER fdw_a to grantee_test_another;

-- 2 When polar_superuser was granted privileges, then he could also grant to others.
\c - postgres
GRANT ALL ON FOREIGN DATA WRAPPER fdw_a TO granter_rs_superuser;

\c - granter_rs_superuser;

GRANT ALL ON FOREIGN DATA WRAPPER fdw_a TO grantee_test_another;

\c - postgres
GRANT USAGE ON FOREIGN DATA WRAPPER fdw_a TO granter_rs_superuser WITH GRANT OPTION;

\c - granter_rs_superuser;

GRANT USAGE ON FOREIGN DATA WRAPPER fdw_a TO grantee_test_another;



-- Grant statement for foreign server;
\c - postgres
DROP SERVER IF EXISTS fs_a;
DROP SERVER IF EXISTS fs_test_a;

CREATE SERVER fs_a FOREIGN DATA WRAPPER fdw_a OPTIONS (host 'foo', dbname 'foodb', port '5432');
GRANT USAGE ON FOREIGN DATA WRAPPER fdw_a TO granter_test;

\c - granter_test
CREATE SERVER fs_test_a FOREIGN DATA WRAPPER fdw_a OPTIONS (host 'fooo', dbname 'footdb', port '5433');

--1 & 2 Granter_rs_superuser is the polar_superuser, could grant all privileges of foreign server that not belong to superuser.
-- fs_a is belong to superuser postgres, so polar_superuser could not grant privileges to others before he own the privilege.
\c - granter_rs_superuser
GRANT ALL ON FOREIGN SERVER fs_test_a to grantee_test_another;
GRANT ALL ON FOREIGN SERVER fs_a to grantee_test_another;

-- 3 None polar_superuser could not grant privileges that he did not own to others.
\c - granter_test1
GRANT ALL ON FOREIGN SERVER fs_test_a to grantee_test_another;

-- 4 When polar_superuser has been granted correlated privilege, then he could grant the privilege to others, if grant options has been assigned.
\c - postgres
GRANT ALL ON FOREIGN SERVER fs_a TO granter_rs_superuser;

\c - granter_rs_superuser;

GRANT ALL ON FOREIGN SERVER fs_a TO grantee_test_another;

\c - postgres
GRANT USAGE ON FOREIGN SERVER fs_a TO granter_rs_superuser WITH GRANT OPTION;

\c - granter_rs_superuser;

GRANT USAGE ON FOREIGN SERVER fs_a TO grantee_test_another;

--5 When none polar_superuser has been granted correlated privilege, then he could grant the privilege to others, if grant options has been assigned. 
\c - postgres
GRANT USAGE ON FOREIGN SERVER fs_a TO granter_test1 WITH GRANT OPTION;
\c - granter_test1
GRANT ALL ON FOREIGN SERVER fs_a To grantee_test_another;
GRANT USAGE ON FOREIGN SERVER fs_a To grantee_test_another;

\c - postgres
DROP SERVER IF EXISTS fs_a;
DROP SERVER IF EXISTS fs_test_a;
DROP FOREIGN DATA WRAPPER IF EXISTS fdw_a;

-- Grant statement for functions;
\c - postgres
DROP FUNCTION IF EXISTS function_a(integer, integer);
DROP FUNCTION IF EXISTS function_test_a(integer, integer);

CREATE FUNCTION function_a(integer, integer) RETURNS integer
    AS 'select $1 + $2;'
    LANGUAGE SQL
    IMMUTABLE
    RETURNS NULL ON NULL INPUT;

\c - granter_test
CREATE FUNCTION function_test_a(integer, integer) RETURNS integer
    AS 'select $1 + $2;'
    LANGUAGE SQL
    IMMUTABLE
    RETURNS NULL ON NULL INPUT;

--1 & 2 Granter_rs_superuser is the polar_superuser, could grant all privileges of function that not belong to superuser.
-- function_a is belong to superuser postgres, so polar_superuser could not grant privileges to others before he own the privilege.
\c - granter_rs_superuser
GRANT ALL ON FUNCTION function_test_a(integer, integer) to grantee_test_another;
GRANT ALL ON FUNCTION function_a(integer, integer) to grantee_test_another;

-- 3 None polar_superuser could not grant privileges that he did not own to others.
\c - granter_test1
GRANT ALL ON FUNCTION function_test_a(integer, integer) to grantee_test_another;

--4 When polar_superuser has been granted correlated privilege, then he could grant the privilege to others, if grant options has been assigned.
\c - postgres
GRANT ALL ON FUNCTION function_a(integer, integer) TO granter_rs_superuser;

\c - granter_rs_superuser;

GRANT ALL ON FUNCTION function_a(integer, integer) TO grantee_test_another;

\c - postgres
GRANT EXECUTE ON FUNCTION function_a (integer, integer) TO granter_rs_superuser WITH GRANT OPTION;

\c - granter_rs_superuser;

GRANT EXECUTE ON FUNCTION function_a (integer, integer) TO grantee_test_another;

--5 When none polar_superuser has been granted correlated privilege, then he could grant the privilege to others, if grant options has been assigned. 
\c - postgres
GRANT EXECUTE ON FUNCTION function_a (integer, integer) TO granter_test1 WITH GRANT OPTION;
\c - granter_test1
GRANT ALL ON FUNCTION function_a (integer, integer) To grantee_test_another;
GRANT EXECUTE ON FUNCTION function_a (integer, integer) To grantee_test_another;

\c - postgres
DROP FUNCTION IF EXISTS function_a (integer, integer);
DROP FUNCTION IF EXISTS function_test_a (integer, integer);

-- Grant statement for tablespace;
\c rds_test_db
\c - postgres
DROP TABLESPACE IF EXISTS tablespace_a;
DROP TABLESPACE IF EXISTS tablespace_test_a;
\! rm -rf /tmp/tablespace_a
\! rm -rf /tmp/tablespace_test_a

\! mkdir /tmp/tablespace_a
\! mkdir /tmp/tablespace_test_a
CREATE TABLESPACE tablespace_a OWNER postgres LOCATION '/tmp/tablespace_a';

CREATE TABLESPACE tablespace_test_a OWNER granter_test LOCATION '/tmp/tablespace_test_a';

--1 & 2 Granter_rs_superuser is the polar_superuser, could grant all privileges of tablespace that not belong to superuser.
-- tablespace_a is belong to superuser postgres, so polar_superuser could not grant privileges to others before he own the privilege.
\c - granter_rs_superuser
GRANT ALL ON TABLESPACE tablespace_test_a to grantee_test_another;
GRANT ALL ON TABLESPACE tablespace_a to grantee_test_another;

--3 None polar_superuser could not grant privileges that he did not own to others.
\c - granter_test1
GRANT ALL ON TABLESPACE tablespace_test_a to grantee_test_another;

--4 When polar_superuser has been granted correlated privilege, then he could grant the privilege to others, if grant options has been assigned.
\c - postgres
GRANT ALL ON TABLESPACE tablespace_a TO granter_rs_superuser;

\c - granter_rs_superuser;

GRANT ALL ON TABLESPACE tablespace_a TO grantee_test_another;

\c - postgres
GRANT CREATE ON TABLESPACE tablespace_a TO granter_rs_superuser WITH GRANT OPTION;

\c - granter_rs_superuser;

GRANT CREATE ON TABLESPACE tablespace_a TO grantee_test_another;

--5 When none polar_superuser has been granted correlated privilege, then he could grant the privilege to others, if grant options has been assigned. 
\c - postgres
GRANT CREATE ON TABLESPACE tablespace_a TO granter_test1 WITH GRANT OPTION;
\c - granter_test1
GRANT ALL ON TABLESPACE tablespace_a To grantee_test_another;
GRANT CREATE ON TABLESPACE tablespace_a To grantee_test_another;

\c - postgres
DROP TABLESPACE IF EXISTS tablespace_a;
DROP TABLESPACE IF EXISTS tablespace_test_a;
\! rm -rf /tmp/tablespace_a
\! rm -rf /tmp/tablespace_test_a

-- Grant statement for type
\c - postgres
DROP TYPE IF EXISTS type_a;
DROP TYPE IF EXISTS type_test_a;

CREATE TYPE type_a AS (f1 int, f2 text);

\c - granter_test
CREATE TYPE type_test_a AS (f1 int, f2 text);

--1 & 2 Granter_rs_superuser is the polar_superuser, could grant all privileges of type that not belong to superuser.
-- type_a is belong to superuser postgres, so polar_superuser could not grant privileges to others before he own the privilege.
\c - granter_rs_superuser
GRANT ALL ON TYPE type_test_a to grantee_test_another;
GRANT ALL ON TYPE type_a to grantee_test_another;

--3 None polar_superuser could not grant privileges that he did not own to others.
\c - granter_test1
GRANT ALL ON TYPE type_test_a to grantee_test_another;

--4 When polar_superuser has been granted correlated privilege, then he could grant the privilege to others, if grant options has been assigned.
\c - postgres
GRANT ALL ON TYPE type_a TO granter_rs_superuser;

\c - granter_rs_superuser;

GRANT ALL ON TYPE type_a TO grantee_test_another;

\c - postgres

GRANT USAGE ON TYPE type_a TO granter_rs_superuser WITH GRANT OPTION;

\c - granter_rs_superuser;

GRANT USAGE ON TYPE type_a TO grantee_test_another;

--5 When none polar_superuser has been granted correlated privilege, then he could grant the privilege to others, if grant options has been assigned. 
\c - postgres
GRANT USAGE ON TYPE type_a TO granter_test1 WITH GRANT OPTION;
\c - granter_test1
GRANT ALL ON TYPE type_a To grantee_test_another;
GRANT USAGE ON TYPE type_a To grantee_test_another;

\c - postgres
DROP TYPE IF EXISTS type_a;
DROP TYPE IF EXISTS type_test_a;

DROP DATABASE IF EXISTS database_a;
DROP DATABASE IF EXISTS database_test_a;
DROP SCHEMA IF EXISTS schema_test1 CASCADE;
DROP ROLE granter_rs_superuser;
