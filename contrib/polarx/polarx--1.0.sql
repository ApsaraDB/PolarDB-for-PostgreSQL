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

CREATE TABLE sql_prepared (
    sql_hash    int NOT NULL,
    version     int NOT NULL,
    self_vs     int NOT NULL,
    sql_md5     text NOT NULL,
    reloids     oid[],
    sql         text NOT NULL
);

CREATE INDEX sql_prepared_hash_idx ON sql_prepared (sql_hash);
CREATE INDEX sql_prepared_query_idx ON sql_prepared (sql_md5);
CREATE INDEX sql_prepared_oids_idx ON sql_prepared USING gin(reloids);

CREATE FUNCTION sql_prepared_invalid_table() RETURNS event_trigger
LANGUAGE plpgsql AS $$
DECLARE
obj      record;
BEGIN
    FOR obj IN SELECT * FROM pg_event_trigger_dropped_objects()
        WHERE object_type = 'table' OR object_type = 'table column'
    LOOP
        UPDATE @extschema@.sql_prepared SET self_vs = self_vs + 1, reloids = NULL WHERE reloids @> ARRAY[obj.objid];
    END LOOP;
END
$$;

CREATE FUNCTION sql_prepared_invalid_rewirte_table() RETURNS event_trigger
LANGUAGE plpgsql AS $$
BEGIN
    UPDATE @extschema@.sql_prepared SET self_vs = self_vs + 1, reloids = NULL WHERE reloids @> ARRAY[pg_event_trigger_table_rewrite_oid()];
END
$$;


CREATE EVENT TRIGGER sql_prepared_invalid_table ON sql_drop 
    EXECUTE PROCEDURE sql_prepared_invalid_table();
CREATE EVENT TRIGGER sql_prepared_invalid_rewirte_table ON table_rewrite 
    EXECUTE PROCEDURE sql_prepared_invalid_rewirte_table();


CREATE FOREIGN DATA WRAPPER polarx
  HANDLER polarx_fdw_handler
  VALIDATOR polarx_fdw_validator;

CREATE SCHEMA polarx;
CREATE TABLE polarx.pg_shard_map(
    shardid int8 NOT NULL,
    nodeoid int,
    shardminvalue int8,
    shardmaxvalue int8
);
ALTER TABLE polarx.pg_shard_map SET SCHEMA pg_catalog;
GRANT SELECT ON pg_catalog.pg_shard_map TO public;

CREATE FUNCTION polardbx_build_shard_map(int4)
    RETURNS bool
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FUNCTION shard_map_cache_invalidate()
    RETURNS trigger
    LANGUAGE C
    AS 'MODULE_PATHNAME', $$shard_map_cache_invalidate$$;
COMMENT ON FUNCTION shard_map_cache_invalidate()
    IS 'register relcache invalidation for changed rows';

-- CREATE TRIGGER shard_map_cache_invalidate
--     AFTER INSERT OR UPDATE OR DELETE
--     ON pg_catalog.pg_shard_map
--     FOR EACH ROW EXECUTE PROCEDURE shard_map_cache_invalidate();

CREATE FUNCTION polardbx_update_shard_meta(shardid int[], nodename text)
    RETURNS VOID
    LANGUAGE C STRICT
	AS 'MODULE_PATHNAME', $$polardbx_update_shard_meta$$;
COMMENT ON FUNCTION polardbx_update_shard_meta(shardid int[], nodename text)
    IS 'polardbx_update_shard_meta';