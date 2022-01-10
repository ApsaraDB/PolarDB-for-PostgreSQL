/* src/test/modules/test_checkpoint_ringbuf/test_checkpoint_ringbuf--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION test_checkpoint_ringbuf" to load this file. \quit

CREATE FUNCTION test_checkpoint_ringbuf()
RETURNS pg_catalog.void STRICT
AS 'MODULE_PATHNAME' LANGUAGE C;
