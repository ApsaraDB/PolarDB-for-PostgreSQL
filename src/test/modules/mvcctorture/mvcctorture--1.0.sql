/* src/test/modules/mvcctorture/mvcctorture--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION mvcctorture" to load this file. \quit

CREATE FUNCTION populate_mvcc_test_table(int4, bool)
RETURNS void
AS 'MODULE_PATHNAME', 'populate_mvcc_test_table'
LANGUAGE C STRICT;
