/*-------------------------------------------------------------------------
 *
 * execRemoteQuery.h
 *
 *      Functions to execute commands on multiple polarx Datanodes
 *
 *
 * Copyright (c) 2021, Alibaba Group Holding Limited
 * Licensed under the Apache License, Version 2.0 (the "License");
 * Portions Copyright (c) 2012-2014, TransLattice, Inc.
 * Portions Copyright (c) 1996-2011, PostgreSQL Global Development Group
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 * IDENTIFICATION
 *        contrib/polarx/include/executor/execRemoteQuery.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef EXECREMOTE_H
#define EXECREMOTE_H
#include "pgxc/locator.h"
#include "nodes/nodes.h"
#include "pgxc/pgxcnode.h"
#include "plan/polarx_planner.h"
#include "pgxc/squeue.h"
#include "access/tupdesc.h"
#include "executor/tuptable.h"
#include "nodes/execnodes.h"
#include "nodes/pg_list.h"
#include "nodes/extensible.h"
#include "tcop/dest.h"
#include "tcop/pquery.h"
#include "utils/snapshot.h"
#include "access/parallel.h"
#include "access/xact.h"
#include "executor/recvRemote.h"


#define get_rqs_addr(ptr, type, member)   ({                        \
        const typeof( ((type *)0)->member ) *__mptr = (ptr);        \
        (type *)( (char *)__mptr - offsetof(type,member) );})

typedef struct RemoteQueryState
{
    PolarxNode type;
    ResponseCombiner combiner;            /* see ResponseCombiner struct */
    RemoteQuery *remote_query;
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

typedef struct FastShipQueryState 
{
    CustomScanState customScanState;
    RemoteQueryState *remoteQueryState;
} FastShipQueryState;

extern CustomScanMethods FastShipQueryExecutorCustomScanMethods;
extern void ExecRemoteUtility(RemoteQuery *node);
extern void ExecCloseRemoteStatement(const char *stmt_name, List *nodelist);
extern void pgxc_all_success_nodes(ExecNodes **d_nodes, ExecNodes **c_nodes, char **failednodes_msg);
extern void RegisterPolarxFastShipQueryMethods(void);
#endif
