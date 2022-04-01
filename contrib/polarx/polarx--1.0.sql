/* contrib/polarx/polarx--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION polarx" to load this file. \quit

CREATE FUNCTION polarx_fdw_handler()
RETURNS fdw_handler
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FUNCTION polarx_fdw_validator(text[], oid)
RETURNS void
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FUNCTION self_node_inx()
RETURNS int
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FUNCTION pgxc_node_str()
RETURNS text
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;


CREATE FUNCTION clean_db_connections(text)
RETURNS void 
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FOREIGN DATA WRAPPER polarx
  HANDLER polarx_fdw_handler
  VALIDATOR polarx_fdw_validator;
