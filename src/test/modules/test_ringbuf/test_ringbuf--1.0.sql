/* src/test/modules/test_ringbuf/test_ringbuf--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION test_ringbuf" to load this file. \quit

CREATE FUNCTION test_ringbuf()
RETURNS pg_catalog.void STRICT
AS 'MODULE_PATHNAME' LANGUAGE C;
