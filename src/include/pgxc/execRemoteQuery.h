/*-------------------------------------------------------------------------
 *
 * execRemoteQuery.h
 *
 *      Functions to execute commands on multiple Datanodes
 *
 *
 * Portions Copyright (c) 2012-2014, TransLattice, Inc.
 * Portions Copyright (c) 1996-2011, PostgreSQL Global Development Group
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 * src/include/pgxc/execRemoteQuery.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef EXECREMOTE_H
#define EXECREMOTE_H
#include "locator.h"
#include "nodes/nodes.h"
#include "pgxcnode.h"
#include "planner.h"
#include "squeue.h"
#include "access/tupdesc.h"
#include "executor/tuptable.h"
#include "nodes/execnodes.h"
#include "nodes/pg_list.h"
#include "tcop/dest.h"
#include "tcop/pquery.h"
#include "utils/snapshot.h"
#include "access/parallel.h"
#include "access/xact.h"
#include "pgxc/recvRemote.h"

typedef struct RemoteQueryState
{
    ResponseCombiner combiner;            /* see ResponseCombiner struct */
    bool        query_Done;                /* query has been sent down to Datanodes */
    /*
     * While we are not supporting grouping use this flag to indicate we need
     * to initialize collecting of aggregates from the DNs
     */
    bool        initAggregates;
    /* Simple DISTINCT support */
    FmgrInfo   *eqfunctions;             /* functions to compare tuples */
    MemoryContext tmp_ctx;                /* separate context is needed to compare tuples */
    /* Support for parameters */
    char       *paramval_data;        /* parameter data, format is like in BIND */
    int            paramval_len;        /* length of parameter values data */
    Oid           *rqs_param_types;    /* Types of the remote params */
    int            rqs_num_params;

    int            eflags;            /* capability flags to pass to tuplestore */
    bool        eof_underlying; /* reached end of underlying plan? */

    /* parameters for insert...on conflict do update */
    char       *ss_paramval_data;        
    int            ss_paramval_len;        
    Oid           *ss_param_types;
    int            ss_num_params;

    char       *su_paramval_data;        
    int            su_paramval_len;        
    Oid           *su_param_types;    
    int            su_num_params;
}    RemoteQueryState;

typedef struct RemoteParam
{
    ParamKind     paramkind;        /* kind of parameter */
    int            paramid;        /* numeric ID for parameter */
    Oid            paramtype;        /* pg_type OID of parameter's datatype */
    int            paramused;        /* is param used */
} RemoteParam;


/*
 * Execution state of a RemoteSubplan node
 */
typedef struct RemoteSubplanState
{
    ResponseCombiner combiner;            /* see ResponseCombiner struct */
    char       *subplanstr;                /* subplan encoded as a string */
    bool        bound;                    /* subplan is sent down to the nodes */
    bool        local_exec;             /* execute subplan on this datanode */
    Locator    *locator;                /* determine destination of tuples of
                                         * locally executed plan */
    int        *dest_nodes;                /* allocate once */
    List       *execNodes;                /* where to execute subplan */
    /* should query be executed on all (true) or any (false) node specified
     * in the execNodes list */
    bool         execOnAll;
    int            nParamRemote;    /* number of params sent from the master node */
    RemoteParam *remoteparams;  /* parameter descriptors */
    
    bool        finish_init;
    int32       eflags;                       /* estate flag. */
} RemoteSubplanState;


/*
 * Data needed to set up a PreparedStatement on the remote node and other data
 * for the remote executor
 */
typedef struct RemoteStmt
{
    NodeTag        type;

    CmdType        commandType;    /* select|insert|update|delete */

    bool        hasReturning;    /* is it insert|update|delete RETURNING? */

    bool        parallelModeNeeded;     /* is parallel needed? */
    bool        parallelWorkerSendTuple;/* can parallel workers send tuples to remote? */

    struct Plan *planTree;                /* tree of Plan nodes */

    List       *rtable;                    /* list of RangeTblEntry nodes */

    /* rtable indexes of target relations for INSERT/UPDATE/DELETE */
    List       *resultRelations;    /* integer list of RT indexes, or NIL */

    List       *subplans;        /* Plan trees for SubPlan expressions */

    int            nParamExec;        /* number of PARAM_EXEC Params used */

    int            nParamRemote;    /* number of params sent from the master node */

    RemoteParam *remoteparams;  /* parameter descriptors */

    List       *rowMarks;

    char        distributionType;

    AttrNumber    distributionKey;

    List       *distributionNodes;

    List       *distributionRestrict;
} RemoteStmt;


extern void ExecCloseRemoteStatement(const char *stmt_name, List *nodelist);

#endif
