/* external/polar_advisor/polar_advisor--1.0--1.1.sql */
-- complain if script is sourced in psql, rather than via ALTER EXTENSION
\echo Use "ALTER EXTENSION polar_advisor UPDATE TO '1.1'" to load this file. \quit

-- Replace the function to remove SECURITY DEFINER option
-- Function to get indexes to reindex.
CREATE OR REPLACE FUNCTION polar_advisor.get_indexes_to_reindex (
    result_rows INT DEFAULT 1000,
    reindex_prefix TEXT DEFAULT 'REINDEX (VERBOSE) INDEX CONCURRENTLY',
    min_index_page_num INT DEFAULT 12800, -- 12800 * 8KB = 100MB
    min_bloat_ratio NUMERIC DEFAULT 0.5
    -- new parameters should be added to the last because we use the id of param in sql
)
RETURNS TABLE (
    schema_name         NAME,
    index_name          NAME,
    reindex_cmd         TEXT,
    bloat_ratio         FLOAT,
    index_size          BIGINT
)
AS $$
BEGIN
    RETURN QUERY
    -- Level 5, get index name and bloat ratio
    WITH RECURSIVE grand_children_of_blacklist AS (
        -- the parent
        SELECT C.oid AS child_oid, N.nspname AS schema_name, C.relname AS rel_name
        FROM pg_catalog.pg_class C
        JOIN pg_catalog.pg_namespace N ON C.relnamespace = N.oid
        JOIN polar_advisor.blacklist_relations B ON B.schema_name = N.nspname AND B.relation_name = C.relname
            AND B.action_type = 'REINDEX'
        UNION
        -- the children and grand children
        SELECT C.oid AS child_oid, N.nspname AS schema_name, C.relname AS rel_name
        FROM pg_catalog.pg_inherits INH
        JOIN pg_catalog.pg_class C ON INH.inhrelid = C.oid
        JOIN pg_catalog.pg_namespace N ON C.relnamespace = N.oid
        JOIN grand_children_of_blacklist PT ON INH.inhparent = PT.child_oid
    ),
    grand_parents_of_blacklist AS (
        -- the son
        SELECT C.oid AS parent_oid, N.nspname AS schema_name, C.relname AS rel_name
        FROM pg_catalog.pg_class C
        JOIN pg_catalog.pg_namespace N ON C.relnamespace = N.oid
        JOIN polar_advisor.blacklist_relations B ON B.schema_name = N.nspname AND B.relation_name = C.relname
            AND B.action_type = 'REINDEX'
        UNION
        -- the parents and grand parents
        SELECT C.oid AS parent_oid, N.nspname AS schema_name, C.relname AS rel_name
        FROM pg_catalog.pg_inherits INH
        JOIN pg_catalog.pg_class C ON INH.inhparent = C.oid
        JOIN pg_catalog.pg_namespace N ON C.relnamespace = N.oid
        JOIN grand_parents_of_blacklist PT ON INH.inhrelid = PT.parent_oid
    )
    SELECT n.nspname AS schema_name, page_stats.index_name,
        $2 || ' ' || pg_catalog.quote_ident(n.nspname) || '.' || pg_catalog.quote_ident(page_stats.index_name) AS reindex_cmd,
        (actual_page_num - full_page_num) / actual_page_num AS bloat_ratio,
        pg_catalog.pg_relation_size(index_id) AS index_size
    FROM (
        -- Level 4, get estimated full page number
        SELECT block_size, index_id, tuple_stats.index_name, schema_id, actual_page_num,
            coalesce(tuple_num * index_tuple_size / (index_fillfactor / 100.0 * block_data_size), 0) AS full_page_num
        FROM (
            -- Level 3, get index tuple size by index tuple header size
            SELECT index_id, rows_tuple_stats.index_name, schema_id, tuple_num, actual_page_num,
                index_fillfactor, block_data_size, block_size,
                (
                    -- what is 4???
                    4 + index_tuple_header_size +
                    -- Add padding to the index tuple header
                    max_align - CASE
                        WHEN index_tuple_header_size % max_align = 0 THEN max_align
                        ELSE index_tuple_header_size % max_align
                    END
                    -- Add padding to the index tuple data
                    + index_tuple_data_size + max_align - CASE
                        WHEN index_tuple_data_size = 0 THEN 0
                        WHEN cast(index_tuple_data_size AS INT) % max_align = 0 THEN max_align
                        ELSE cast(index_tuple_data_size AS INT) % max_align
                    END
                )::NUMERIC AS index_tuple_size
            FROM (
                -- Level 2, get index tuple header size, block size, block data size
                SELECT i.index_id, i.index_name, i.schema_id, i.tuple_num, i.actual_page_num, i.index_fillfactor,
                    8 AS max_align, -- 8 on 64 bits system
                    -- per tuple header
                    CASE WHEN MAX(coalesce(s.stanullfrac, 0)) = 0
                        THEN 8 -- IndexTupleData size
                        ELSE 8 + (32 + 8 - 1) / 8 -- add IndexAttributeBitMapData size if any column is null-able
                    END AS index_tuple_header_size,
                    -- data width without null values
                    SUM((1 - coalesce(s.stanullfrac, 0)) * coalesce(s.stawidth, 1024)) AS index_tuple_data_size,
                    pg_catalog.current_setting('block_size')::NUMERIC AS block_size,
                    -- per page header is 24, per page btree opaque data is 16
                    (pg_catalog.current_setting('block_size')::NUMERIC - 24 - 16) AS block_data_size
                FROM (
                    -- Level 1, add attnum of the index column from pg_attribute
                    SELECT ic.index_name, ic.schema_id, ic.col_id, ic.tuple_num, ic.actual_page_num, ic.orig_table_id, ic.index_id, ic.index_fillfactor,
                        coalesce(a1.attnum, a2.attnum) AS attnum
                    FROM (
                        -- Level 0, raw index info from pg_index and pg_class
                        SELECT ci.reltuples AS tuple_num, ci.relpages AS actual_page_num, i.indrelid AS orig_table_id,
                            i.indexrelid AS index_id, ci.relname AS index_name, ci.relnamespace AS schema_id,
                            pg_catalog.generate_series(1, i.indnatts) AS col_id,
                            pg_catalog.string_to_array(
                                pg_catalog.textin(pg_catalog.int2vectorout(i.indkey)), ' '
                            )::INT[] AS index_key,
                            coalesce(
                                substring(pg_catalog.array_to_string(ci.reloptions, ' ') FROM 'fillfactor=([0-9]+)')::SMALLINT,
                                -- Default fillfactor is 90 for btree index
                                90
                            ) AS index_fillfactor
                        FROM pg_catalog.pg_index i
                        JOIN pg_catalog.pg_class ci ON ci.oid = i.indexrelid
                        JOIN pg_catalog.pg_namespace n ON n.oid = ci.relnamespace
                        -- Ignore small indexes
                        WHERE ci.relpages >= $3
                        -- Only btree index
                        AND ci.relam = (SELECT oid FROM pg_catalog.pg_am WHERE amname = 'btree')
                        -- Only normal index, ignore partitioned index
                        AND ci.relkind = 'i'
                        -- Ignore global index
                        AND (ci.reloptions IS NULL OR NOT ('global_index=true' = ANY (ci.reloptions)))
                        -- Ignore system and toast indexes since:
                        -- a) pg_catalog index doesn't support reindex concurrently
                        -- b) it's dangerous to operate system index
                        -- c) pg_class.reltuples of pg_toast index is always 0
                        AND n.nspname NOT IN ('pg_catalog', 'polar_catalog', 'sys', 'information_schema', 'pg_toast')
                        -- Ignore the schema of extensions because some extensions like dbms_xxx are
                        -- created by system and do not operate them. It's ok because these tables are often
                        -- small.
                        AND n.nspname NOT IN (
                            SELECT n.nspname
                            FROM pg_catalog.pg_namespace n, pg_catalog.pg_depend d, pg_catalog.pg_extension e
                            WHERE d.deptype = 'e' AND d.refobjid = e.oid AND d.objid = n.oid
                        )
                    ) AS ic
                    LEFT JOIN pg_catalog.pg_attribute a1 ON
                        a1.attrelid = ic.orig_table_id AND
                        a1.attnum = ic.index_key[ic.col_id] AND
                        -- Non-0 means normal index
                        ic.index_key[ic.col_id] <> 0
                    LEFT JOIN pg_catalog.pg_attribute a2 ON
                        a2.attrelid = ic.index_id AND
                        a2.attnum = ic.col_id AND
                        -- 0 means expression index
                        ic.index_key[ic.col_id] = 0
                ) i
                -- XXX: only superuser can see the stats
                JOIN pg_catalog.pg_statistic s
                ON s.starelid = i.orig_table_id
                AND s.staattnum = i.attnum
                GROUP BY i.index_id, i.index_name, i.schema_id, i.tuple_num, i.actual_page_num, i.index_fillfactor
            ) AS rows_tuple_stats
        ) AS tuple_stats
    ) AS page_stats
    JOIN pg_catalog.pg_namespace n ON n.oid = page_stats.schema_id
    WHERE 1.0 * (actual_page_num - full_page_num) / actual_page_num >= $4
        -- not in blacklist
        AND (n.nspname, page_stats.index_name) NOT IN (
            SELECT DISTINCT b.schema_name, b.rel_name FROM grand_children_of_blacklist b ORDER BY schema_name, rel_name)
        AND (n.nspname, page_stats.index_name) NOT IN (
            SELECT DISTINCT b.schema_name, b.rel_name FROM grand_parents_of_blacklist b ORDER BY schema_name, rel_name)
    ORDER BY bloat_ratio DESC
    LIMIT $1;
END;
$$ LANGUAGE PLpgSQL;
