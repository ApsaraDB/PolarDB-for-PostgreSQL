
-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION polar_sys_stats" to load this file. \quit

CREATE FUNCTION polar_random_page_cost(numeric, numeric, numeric)
RETURNS float8
AS 'MODULE_PATHNAME','polar_random_page_cost'
LANGUAGE C STRICT;
