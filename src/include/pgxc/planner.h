/*-------------------------------------------------------------------------
 *
 * planner.h
 *        Externally declared locator functions
 *
 *
 * Portions Copyright (c) 2012-2014, TransLattice, Inc.
 * Portions Copyright (c) 1996-2011, PostgreSQL Global Development Group
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 * src/include/pgxc/planner.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PGXCPLANNER_H
#define PGXCPLANNER_H

#include "fmgr.h"
#include "lib/stringinfo.h"
#include "nodes/params.h"
#include "nodes/parsenodes.h"
#include "nodes/plannodes.h"
#include "nodes/primnodes.h"
#include "pgxc/locator.h"
#include "tcop/dest.h"
#include "nodes/relation.h"


typedef enum
{
    COMBINE_TYPE_NONE,            /* it is known that no row count, do not parse */
    COMBINE_TYPE_SUM,            /* sum row counts (partitioned, round robin) */
    COMBINE_TYPE_SAME            /* expect all row counts to be the same (replicated write) */
}    CombineType;

/* For sorting within RemoteQuery handling */
/*
 * It is pretty much like Sort, but without Plan. We may use Sort later.
 */
typedef struct
{
    NodeTag        type;
    int            numCols;        /* number of sort-key columns */
    AttrNumber *sortColIdx;        /* their indexes in the target list */
    Oid           *sortOperators;    /* OIDs of operators to sort them by */
    Oid           *sortCollations;
    bool       *nullsFirst;        /* NULLS FIRST/LAST directions */
} SimpleSort;

/*
 * Determines if query has to be launched
 * on Coordinators only (SEQUENCE DDL),
 * on Datanodes (normal Remote Queries),
 * or on all Postgres-XC nodes (Utilities and DDL).
 */
typedef enum
{
    EXEC_ON_CURRENT,
    EXEC_ON_DATANODES,
    EXEC_ON_COORDS,
    EXEC_ON_ALL_NODES,
    EXEC_ON_NONE
} RemoteQueryExecType;

typedef enum
{
    EXEC_DIRECT_NONE,
    EXEC_DIRECT_LOCAL,
    EXEC_DIRECT_LOCAL_UTILITY,
    EXEC_DIRECT_UTILITY,
    EXEC_DIRECT_SELECT,
    EXEC_DIRECT_INSERT,
    EXEC_DIRECT_UPDATE,
    EXEC_DIRECT_DELETE
} ExecDirectType;

typedef enum UPSERT_ACTION
{
    UPSERT_NONE,
    UPSERT_SELECT,
    UPSERT_INSERT,
    UPSERT_UPDATE
} UPSERT_ACTION;
/*
 * Contains instructions on processing a step of a query.
 * In the prototype this will be simple, but it will eventually
 * evolve into a GridSQL-style QueryStep.
 */
typedef struct
{
    Scan            scan;
    ExecDirectType        exec_direct_type;    /* track if remote query is execute direct and what type it is */
    char            *sql_statement;
    ExecNodes        *exec_nodes;        /* List of Datanodes where to launch query */
    CombineType        combine_type;
    SimpleSort        *sort;
    bool            read_only;        /* do not use 2PC when committing read only steps */
    bool            force_autocommit;    /* some commands like VACUUM require autocommit mode */
    char            *statement;        /* if specified use it as a PreparedStatement name on Datanodes */
    char            *cursor;        /* if specified use it as a Portal name on Datanodes */
    int             rq_num_params;      /* number of parameters present in
                                           remote statement */
    Oid             *rq_param_types;    /* parameter types for the remote
                                           statement */
    RemoteQueryExecType    exec_type;
    int            reduce_level;        /* in case of reduced JOIN, it's level    */
    char            *outer_alias;
    char            *inner_alias;
    int            outer_reduce_level;
    int            inner_reduce_level;
    Relids            outer_relids;
    Relids            inner_relids;
    char            *inner_statement;
    char            *outer_statement;
    char            *join_condition;
    bool            has_row_marks;        /* Did SELECT had FOR UPDATE/SHARE? */
    bool            has_ins_child_sel_parent;    /* This node is part of an INSERT SELECT that
                                 * inserts into child by selecting from its parent */

    bool            rq_finalise_aggs;   /* Aggregates should be finalised at
                                           the 
                                         * Datanode */
    bool            rq_sortgroup_colno; /* Use resno for sort group references
                                         * instead of expressions */
    Query           *remote_query;  /* Query structure representing the query
                                       to be
                                     * sent to the datanodes */
    List            *base_tlist;    /* the targetlist representing the result
                                       of 
                                     * the query to be sent to the datanode */

    /*
     * Reference targetlist of Vars to match the Vars in the plan nodes on
     * coordinator to the corresponding Vars in the remote_query.  These
     * targetlists are used to while replacing/adding targetlist and quals in
     * the remote_query.
     */ 
    List            *coord_var_tlist;
    List            *query_var_tlist;
    bool            is_temp;
    /*
      * This part is used for 'insert...on onconflict do update' while the target
      * relation has unshippable triggers, we have to do the UPSERT on coordinator with
      * triggers. In order to make triggers work, we separate UPSERT into INSERT and
      * UPDATE.
         */
    char            *sql_select;      /* select statement */
    char            *sql_select_base;
    bool            forUpadte;
    int             ss_num_params;
    Oid             *ss_param_types;
    char            *select_cursor;
    char            *sql_update;      /* update statement */
    int             su_num_params;
    Oid             *su_param_types;
    char            *update_cursor;
    UPSERT_ACTION   action;
    bool            dml_on_coordinator;
    AttrNumber        jf_ctid;
    AttrNumber        jf_xc_node_id;
    AttrNumber        jf_xc_wholerow;
    Bitmapset       *conflict_cols;

    Node            *parsetree;  /* to recognise subtxn cmds(savepoint,rollback to,release savepoint) */

} RemoteQuery;

#endif   /* PGXCPLANNER_H */
