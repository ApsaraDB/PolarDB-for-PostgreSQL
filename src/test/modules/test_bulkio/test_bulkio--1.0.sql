/* src/test/modules/test_bulkio/test_bulkio--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION test_bulkio" to load this file. \quit

CREATE FUNCTION test_bulkio()
RETURNS VOID STRICT
AS 'MODULE_PATHNAME' LANGUAGE C;
