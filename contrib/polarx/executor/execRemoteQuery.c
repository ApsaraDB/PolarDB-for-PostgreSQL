/*-------------------------------------------------------------------------
 *
 * execRemoteQuery.c
 *
 *      Functions to execute commands for fast ship query in polarx
 *
 *
 * Copyright (c) 2021, Alibaba Group Holding Limited
 * Licensed under the Apache License, Version 2.0 (the "License");
 * Portions Copyright (c) 2012-2014, TransLattice, Inc.
 * Portions Copyright (c) 1996-2011, PostgreSQL Global Development Group
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 * IDENTIFICATION
 *        contrib/polarx/executor/execRemoteQuery.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "polarx.h"
#include "funcapi.h"

#include <time.h>

#include "access/relscan.h"
#include "access/sysattr.h"
#include "access/transam.h"
#include "access/twophase.h"
#include "access/xact.h"
#include "catalog/pg_type.h"
#include "commands/prepare.h"
#include "executor/executor.h"
#include "executor/nodeSubplan.h"
#include "libpq/libpq.h"
#include "miscadmin.h"
#include "nodes/nodeFuncs.h"
#include "nodes/nodes.h"
#include "nodes/params.h"
#include "optimizer/var.h"
#include "parser/parse_type.h"
#include "parser/parsetree.h"
#include "pgstat.h"
#include "executor/execRemoteQuery.h"
#include "pgxc/locator.h"
#include "pgxc/nodemgr.h"
#include "pgxc/pgxcnode.h"
#include "utils/datarowstore.h"
#include "executor/recvRemote.h"
#include "distribute_transaction/txn.h"
#include "storage/ipc.h"
#include "storage/proc.h"
#include "tcop/tcopprot.h"
#include "utils/builtins.h"
#include "utils/datum.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/pg_rusage.h"
#include "utils/snapmgr.h"
#include "utils/tuplesort.h"
#include "utils/ruleutils.h"
#include "utils/typcache.h"
#include "commands/explain.h"
#include "nodes/makefuncs.h"
#include "metadata/cache.h"
#include "catalog/pg_class.h"
#include "utils/syscache.h"
#include "deparse/deparse_fqs.h"
#include "utils/fmgroids.h"
#include "utils/relcache.h"
#include "catalog/pg_collation.h"
#include "utils/rel.h"
#include "catalog/namespace.h"
#include "parser/parser.h"
#include "nodes/polarx_node.h"
#include "pool/poolnodes.h"
#include "catalog/dependency.h"
#include "catalog/indexing.h"
#include "catalog/pg_inherits.h"

static PGXCNodeAllHandles *get_exec_connections(RemoteQueryState *planstate,
                                                ExecNodes *exec_nodes,
                                                RemoteQueryExecType exec_type,
                                                bool is_global_session);
static bool pgxc_start_command_on_connection(PGXCNodeHandle *connection,
                                             RemoteQueryState *remotestate,
                                             Snapshot snapshot,
                                             bool is_prepared);
static ExecNodes *get_success_nodes(int node_count, PGXCNodeHandle **handles,
                                    char node_type, StringInfo failednodes);
static TupleTableSlot *RemoteQueryNext(RemoteQueryState *node);
static bool RemoteQueryRecheck(ScanState *node, TupleTableSlot *slot);

static RemoteQueryState *ExecInitRemoteQuery(RemoteQuery *node, EState *estate, int eflags);
static TupleTableSlot *ExecRemoteQuery(PlanState *pstate);
static void ExecEndRemoteQuery(RemoteQueryState *node);
static void ExecReScanRemoteQuery(RemoteQueryState *node);

static void show_expression(Node *node, const char *qlabel,
                            PlanState *planstate, List *ancestors,
                            bool useprefix, ExplainState *es);
static Node *polarxFQSExecutorCreateRemoteState(CustomScan *scan);
static void PolarxFQSBeginScan(CustomScanState *pstate, EState *estate, int eflags);
static TupleTableSlot *PolarxFQSExecScan(CustomScanState *pstate);
static void PolarxFQSReScan(CustomScanState *pstate);
static void PolarxFQSEndScan(CustomScanState *pstate);
static void PolarxFQSExplainScan(CustomScanState *pstate, List *ancestors, struct ExplainState *es);
static void deform_datarow_into_slot(RemoteDataRow datarow, TupleTableSlot *slot);
static void SetDataRowForExtParams(ParamListInfo paraminfo, RemoteQueryState *rq_state);
static int prepare_fqs_sql(const char *p_name, const char *query,
                            PGXCNodeHandle **connections, int conn_num);
static char* GenDNCreatePartitionTableCmd(Oid relationId,char* sql_statement, char* schema_name, char* tbl_name, 
                                          char* dist_col_name, char* hash_func_name, bool with_oids);
static char *generate_relation_name(Oid relid, List *namespaces);
static char* GetTableCreateCommand(Oid relationId, char *dist_col_name, char* hash_func_name, bool with_oids);
static char *flatten_reloptions(Oid relid);
static void simple_quote_literal(StringInfo buf, const char *val);
static const char* get_dn_partition_key_postfix_name(char* hashfuncname);

CustomScanMethods FastShipQueryExecutorCustomScanMethods = {
    "Polarx Fast Ship Query",
    polarxFQSExecutorCreateRemoteState};

static CustomExecMethods FastShipQueryExecutorCustomExecMethods = {
    .CustomName = "FastShipQueryExecutorScan",
    .BeginCustomScan = PolarxFQSBeginScan,
    .ExecCustomScan = PolarxFQSExecScan,
    .EndCustomScan = PolarxFQSEndScan,
    .ReScanCustomScan = PolarxFQSReScan,
    .ExplainCustomScan = PolarxFQSExplainScan};

void RegisterPolarxFastShipQueryMethods(void)
{
    RegisterCustomScanMethods(&FastShipQueryExecutorCustomScanMethods);
}

/*
 * Get Node connections depending on the connection type:
 * Datanodes Only, Coordinators only or both types
 */
static PGXCNodeAllHandles *
get_exec_connections(RemoteQueryState *planstate,
                     ExecNodes *exec_nodes,
                     RemoteQueryExecType exec_type,
                     bool is_global_session)
{ // #lizard forgives
    List *nodelist = NIL;
    List *primarynode = NIL;
    List *coordlist = NIL;
    PGXCNodeHandle *primaryconnection;
    int co_conn_count, dn_conn_count;
    bool is_query_coord_only = false;
    PGXCNodeAllHandles *pgxc_handles = NULL;


    /*
     * If query is launched only on Coordinators, we have to inform get_handles
     * not to ask for Datanode connections even if list of Datanodes is NIL.
     */
    if (exec_type == EXEC_ON_COORDS)
        is_query_coord_only = true;

    if (exec_type == EXEC_ON_CURRENT)
        return get_current_handles();

    if (exec_nodes)
    {
        if (exec_nodes->en_expr)
        {
            /* execution time determining of target Datanodes */
            bool isnull;
            ExecNodes *nodes;

            ExprState *estate = ExecInitExpr(exec_nodes->en_expr,
                                             (PlanState *)(&(planstate->combiner)));
            Datum partvalue = ExecEvalExpr(estate,
                                           planstate->combiner.ss.ps.ps_ExprContext,
                                           &isnull);
            RelationLocInfo *rel_loc_info = GetRelationLocInfo(exec_nodes->en_relid);

            /* PGXCTODO what is the type of partvalue here */
            nodes = GetRelationNodes(rel_loc_info,
                                     partvalue,
                                     isnull,
                                     exec_nodes->accesstype);
            /*
             * en_expr is set by pgxc_set_en_expr only for distributed
             * relations while planning DMLs, hence a select for update
             * on a replicated table here is an assertion
             */
            Assert(!(exec_nodes->accesstype == RELATION_ACCESS_READ_FOR_UPDATE &&
                     IsRelationReplicated(rel_loc_info)));

            if (nodes)
            {
                nodelist = nodes->nodeList;
                primarynode = nodes->primarynodelist;
                pfree(nodes);
            }
            //FreeRelationLocInfo(rel_loc_info);
        }
        else if (OidIsValid(exec_nodes->en_relid))
        {
            RelationLocInfo *rel_loc_info = GetRelationLocInfo(exec_nodes->en_relid);
            ExecNodes *nodes = GetRelationNodes(rel_loc_info, 0, true, exec_nodes->accesstype);
            /*
             * en_relid is set only for DMLs, hence a select for update on a
             * replicated table here is an assertion
             */
            Assert(!(exec_nodes->accesstype == RELATION_ACCESS_READ_FOR_UPDATE &&
                     IsRelationReplicated(rel_loc_info)));

            /* Use the obtained list for given table */
            if (nodes)
                nodelist = nodes->nodeList;

            /*
             * Special handling for ROUND ROBIN distributed tables. The target
             * node must be determined at the execution time
             */
            if (rel_loc_info->locatorType == LOCATOR_TYPE_RROBIN && nodes)
            {
                nodelist = nodes->nodeList;
                primarynode = nodes->primarynodelist;
            }
            else if (nodes)
            {
                if (exec_type == EXEC_ON_DATANODES || exec_type == EXEC_ON_ALL_NODES)
                {
                    nodelist = exec_nodes->nodeList;
                    primarynode = exec_nodes->primarynodelist;
                }
            }

            if (nodes)
                pfree(nodes);
            //FreeRelationLocInfo(rel_loc_info);
        }
        else
        {
            if (exec_type == EXEC_ON_DATANODES || exec_type == EXEC_ON_ALL_NODES)
                nodelist = exec_nodes->nodeList;
            else if (exec_type == EXEC_ON_COORDS)
                coordlist = exec_nodes->nodeList;

            primarynode = exec_nodes->primarynodelist;
        }
    }

    /* Set node list and DN number */
    if (list_length(nodelist) == 0 &&
        (exec_type == EXEC_ON_ALL_NODES ||
         exec_type == EXEC_ON_DATANODES))
    {
        /* Primary connection is included in this number of connections if it exists */
        dn_conn_count = NumDataNodes;
    }
    else
    {
        if (exec_type == EXEC_ON_DATANODES || exec_type == EXEC_ON_ALL_NODES)
        {
            if (primarynode)
                dn_conn_count = list_length(nodelist) + 1;
            else
                dn_conn_count = list_length(nodelist);
        }
        else
            dn_conn_count = 0;
    }

    /* Set Coordinator list and Coordinator number */
    if ((list_length(nodelist) == 0 && exec_type == EXEC_ON_ALL_NODES) ||
        (list_length(coordlist) == 0 && exec_type == EXEC_ON_COORDS))
    {
        coordlist = GetAllCoordNodes();
        co_conn_count = list_length(coordlist);
    }
    else
    {
        if (exec_type == EXEC_ON_COORDS)
            co_conn_count = list_length(coordlist);
        else
            co_conn_count = 0;
    }

    /* Get other connections (non-primary) */
    pgxc_handles = get_handles(nodelist, coordlist, is_query_coord_only, is_global_session);
    if (!pgxc_handles)
        ereport(ERROR,
                (errcode(ERRCODE_INTERNAL_ERROR),
                 errmsg("Could not obtain connection from pool")));

    /* Get connection for primary node, if used */
    if (primarynode)
    {
        /* Let's assume primary connection is always a Datanode connection for the moment */
        PGXCNodeAllHandles *pgxc_conn_res;
        pgxc_conn_res = get_handles(primarynode, NULL, false, is_global_session);

        /* primary connection is unique */
        primaryconnection = pgxc_conn_res->datanode_handles[0];

        pfree(pgxc_conn_res);

        if (!primaryconnection)
            ereport(ERROR,
                    (errcode(ERRCODE_INTERNAL_ERROR),
                     errmsg("Could not obtain connection from pool")));
        pgxc_handles->primary_handle = primaryconnection;
    }

    /* Depending on the execution type, we still need to save the initial node counts */
    pgxc_handles->dn_conn_count = dn_conn_count;
    pgxc_handles->co_conn_count = co_conn_count;

    return pgxc_handles;
}

static bool
pgxc_start_command_on_connection(PGXCNodeHandle *connection,
                                 RemoteQueryState *remotestate,
                                 Snapshot snapshot,
                                 bool is_prepared)
{ // #lizard forgives
    CommandId cid;
    ResponseCombiner *combiner = (ResponseCombiner *)(&(remotestate->combiner));
    RemoteQuery *step = (RemoteQuery *)(remotestate->remote_query);
    elog(DEBUG5, "pgxc_start_command_on_connection - node %s, state %d",
         connection->nodename, connection->state);

    /*
     * Scan descriptor would be valid and would contain a valid snapshot
     * in cases when we need to send out of order command id to data node
     * e.g. in case of a fetch
     */
    if (snapshot)
    {
        cid = snapshot->curcid;

        if (cid == InvalidCommandId)
        {
            elog(LOG, "commandId in snapshot is invalid.");
            cid = GetCurrentCommandId(false);
        }
    }
    else
    {
        cid = GetCurrentCommandId(false);
    }

    if (step->statement || step->cursor || remotestate->rqs_num_params)
    {
        /* need to use Extended Query Protocol */
        int fetch = 0;
        bool send_desc = false;
        char *prep_name = NULL;

        if(step->statement)
            GetPrepStmtNameAndSelfVersion(step->statement, &prep_name);

        if (step->base_tlist != NULL ||
            step->exec_nodes->accesstype == RELATION_ACCESS_READ ||
            step->read_only ||
            step->has_row_marks)
            send_desc = true;

        /*
         * execute and fetch rows only if they will be consumed
         * immediately by the sorter
         */
        if (step->cursor)
        {
            /* we need all rows one time */
            if (step->dml_on_coordinator)
                fetch = 0;
            else
                fetch = 1;
        }

        combiner->extended_query = true;
#ifdef PATCH_ENABLE_DISTRIBUTED_TRANSACTION
        if (pgxc_node_send_cmd_id(connection, cid) < 0 )
            return false;
#endif
        if (pgxc_node_send_query_extended(connection,
                                          is_prepared ? NULL : step->sql_statement,
                                          prep_name,
                                          step->cursor,
                                          remotestate->rqs_num_params,
                                          remotestate->rqs_param_types,
                                          remotestate->paramval_len,
                                          remotestate->paramval_data,
                                          send_desc,
                                          fetch) != 0)
            return false;
    }
    else
    {
        combiner->extended_query = false;
#ifdef PATCH_ENABLE_DISTRIBUTED_TRANSACTION
        if (pgxc_node_send_cmd_id(connection, cid) < 0 )
            return false;
#endif
        if (pgxc_node_send_query(connection, step->sql_statement) != 0)
            return false;
    }
    return true;
}

static Node *
polarxFQSExecutorCreateRemoteState(CustomScan *scan)
{
    FastShipQueryState *fsp_state = palloc0(sizeof(FastShipQueryState));

    fsp_state->customScanState.ss.ps.type = T_CustomScanState;
    fsp_state->customScanState.methods = &FastShipQueryExecutorCustomExecMethods;
    fsp_state->remoteQueryState = polarxMakeNode(RemoteQueryState);

    return (Node *)fsp_state;
}
static RemoteQueryState *
ExecInitRemoteQuery(RemoteQuery *node, EState *estate, int eflags)
{
    RemoteQueryState *remotestate;
    ResponseCombiner *combiner;
    TupleDesc scan_type;

    remotestate = polarxMakeNode(RemoteQueryState);
    remotestate->remote_query = node;
    combiner = (ResponseCombiner *)(&(remotestate->combiner));
    InitResponseCombiner(combiner, 0, node->combine_type);
    combiner->ss.ps.plan = (Plan *)(&(node->scan));
    combiner->ss.ps.state = estate;
    combiner->ss.ps.ExecProcNode = ExecRemoteQuery;

    ExecAssignExprContext(estate, &combiner->ss.ps);
    combiner->ss.ps.qual =
        ExecInitQual(node->scan.plan.qual, (PlanState *)combiner);

    combiner->request_type = REQUEST_TYPE_QUERY;

    ExecInitResultTupleSlotTL(estate, &combiner->ss.ps);
    ExecInitScanTupleSlot(estate, &combiner->ss, NULL);

    if (node->base_tlist)
        scan_type = ExecTypeFromTL(node->base_tlist, false);
    else
        scan_type = ExecTypeFromTL(node->scan.plan.targetlist, false);

    ExecAssignScanType(&combiner->ss, scan_type);

    if(estate->es_param_list_info)
        SetDataRowForExtParams(estate->es_param_list_info, remotestate);

    if (node->base_tlist)
        ExecAssignScanProjectionInfo(&combiner->ss);
    return remotestate;
}
/*****************************************************************************
 *
 * Simplified versions of PolarxFQSBeginScan 
 *
 *****************************************************************************/
static void
PolarxFQSBeginScan(CustomScanState *pstate, EState *estate, int eflags)
{
    FastShipQueryState *fsp_state = (FastShipQueryState *)pstate;
    CustomScan *scan = (CustomScan *)fsp_state->customScanState.ss.ps.plan;
    RemoteQuery *node = (RemoteQuery *)linitial(scan->custom_private);
    fsp_state->remoteQueryState = ExecInitRemoteQuery(node, estate, eflags);
}

/*
 * Execute step of Polarx FSP plan.
 * The step specifies a command to be executed on specified nodes.
 * On first invocation connections to the data nodes are initialized and
 * command is executed. Further, as well as within subsequent invocations,
 * responses are received until step is completed or there is a tuple to emit.
 * If there is a tuple it is returned, otherwise returned NULL. The NULL result
 * from the function indicates completed step.
 * The function returns at most one tuple per invocation.
 */
static TupleTableSlot *
RemoteQueryNext(RemoteQueryState *node)
{ // #lizard forgives

    node = get_rqs_addr((ResponseCombiner *)node, RemoteQueryState, combiner);
    ResponseCombiner *combiner = (ResponseCombiner *)(&(node->combiner));
    RemoteQuery *step = (RemoteQuery *)(node->remote_query);
    TupleTableSlot *slot = combiner->ss.ss_ScanTupleSlot;;
    RemoteDataRow datarow = NULL;

    if (!node->query_Done)
    {
        Snapshot snapshot = GetActiveSnapshot();
        PGXCNodeHandle **connections = NULL;
        int i;
        int regular_conn_count = 0;
        int total_conn_count = 0;
        bool need_tran_block;
        PGXCNodeAllHandles *pgxc_connections;

        /*
         * Get connections for Datanodes only, utilities and DDLs
         * are launched in ExecRemoteUtility
         */
        pgxc_connections = get_exec_connections(node, step->exec_nodes,
                                                step->exec_type,
                                                true);

        if (step->exec_type == EXEC_ON_DATANODES)
        {
            connections = pgxc_connections->datanode_handles;
            total_conn_count = regular_conn_count = pgxc_connections->dn_conn_count;
        }
        else if (step->exec_type == EXEC_ON_COORDS)
        {
            connections = pgxc_connections->coord_handles;
            total_conn_count = regular_conn_count = pgxc_connections->co_conn_count;
        }

        /* initialize */
        combiner->recv_node_count = regular_conn_count;
        combiner->recv_tuples = 0;
        combiner->recv_total_time = -1;
        combiner->recv_datarows = 0;

        /*
         * We save only regular connections, at the time we exit the function
         * we finish with the primary connection and deal only with regular
         * connections on subsequent invocations
         */
        combiner->node_count = regular_conn_count;

        /*
         * Start transaction on data nodes if we are in explicit transaction
         * or going to use extended query protocol or write to multiple nodes
         */
        if (step->force_autocommit)
            need_tran_block = false;
        else
            need_tran_block = step->cursor ||
                              (!step->read_only && total_conn_count > 1) ||
                              (TransactionBlockStatusCode() == 'T');

        if (regular_conn_count > 0)
        {
            combiner->connections = connections;
            combiner->conn_count = regular_conn_count;
            combiner->current_conn = 0;
        }

        if(pgxc_node_begin(regular_conn_count, connections, 0,
                        need_tran_block, step->read_only, PGXC_NODE_DATANODE))
            ereport(ERROR,
                    (errcode(ERRCODE_INTERNAL_ERROR),
                     errmsg("Could not begin transaction on data node.")));
        if(!node->is_prepared && step->statement)
        {
            prepare_fqs_sql(step->statement,step->sql_statement,connections, regular_conn_count); 
            node->is_prepared = true;
        }

        for (i = 0; i < regular_conn_count; i++)
        {
            //connections[i]->read_only = true;
            connections[i]->recv_datarows = 0;

            /* If explicit transaction is needed gxid is already sent */
            if (!pgxc_start_command_on_connection(connections[i], node, snapshot, node->is_prepared))
            {
                pfree_pgxc_all_handles(pgxc_connections);
                ereport(ERROR,
                        (errcode(ERRCODE_INTERNAL_ERROR),
                         errmsg("Failed to send command to data nodes")));
            }
            connections[i]->combiner = combiner;
        }

        node->query_Done = true;
    }

    datarow = FetchDatarow(combiner);
    if (datarow)
    {
        ExecClearTuple(slot);
        deform_datarow_into_slot(datarow, slot);
        pfree(datarow);
        return slot;
    }

    if (combiner->errorMessage)
        pgxc_node_report_error(combiner);

    return NULL;
}

static TupleTableSlot *
ExecRemoteQuery(PlanState *pstate)
{
    ResponseCombiner *combiner = (ResponseCombiner *) pstate;
    /*
     * As there may be unshippable qual which we need to evaluate on CN,
     * We should call ExecScan to handle such cases.
     */

    return ExecScan(&(combiner->ss),
                    (ExecScanAccessMtd)RemoteQueryNext,
                    (ExecScanRecheckMtd)RemoteQueryRecheck);
}
/*
 * PolarxFQSExecScan 
 * Wrapper around the main RemoteQueryNext() function. This
 * wrapper provides materialization of the result returned by
 * RemoteQueryNext
 */

static TupleTableSlot *
PolarxFQSExecScan(CustomScanState *pstate)
{
    FastShipQueryState *fsp_state = (FastShipQueryState *)pstate;
    return ExecRemoteQuery((PlanState *)(&(fsp_state->remoteQueryState->combiner)));
}

/*
 * RemoteQueryRecheck -- remote query routine to recheck a tuple in EvalPlanQual
 */
static bool
RemoteQueryRecheck(ScanState *node, TupleTableSlot *slot)
{
    /*
	 * Note that unlike IndexScan, RemoteQueryScan never use keys in heap_beginscan
	 * (and this is very bad) - so, here we do not check are keys ok or not.
	 */
    return true;
}

static void
ExecReScanRemoteQuery(RemoteQueryState *node)
{
    ResponseCombiner *combiner = (ResponseCombiner *)(&(node->combiner));

    /*
     * If we haven't queried remote nodes yet, just return. If outerplan'
     * chgParam is not NULL then it will be re-scanned by ExecProcNode,
     * else - no reason to re-scan it at all.
     */
    if (!node->query_Done)
        return;

    /*
     * If we execute locally rescan local copy of the plan
     */
    if (outerPlanState(combiner))
        ExecReScan(outerPlanState(combiner));

    /*
     * Consume any possible pending input
     */
    pgxc_connections_cleanup(combiner);

    /* misc cleanup */
    combiner->command_complete_count = 0;
    combiner->description_count = 0;

    /*
     * Force query is re-bound with new parameters
     */
    node->query_Done = false;
}
/* ----------------------------------------------------------------
 *       PolarxFQSReScan 
 * ----------------------------------------------------------------
 */
static void
PolarxFQSReScan(CustomScanState *pstate)
{
    FastShipQueryState *fsp_state = (FastShipQueryState *)pstate;
    RemoteQueryState *node = fsp_state->remoteQueryState;
    ExecReScanRemoteQuery(node);
}
static void
ExecEndRemoteQuery(RemoteQueryState *node)
{
    ResponseCombiner *combiner = (ResponseCombiner *)(&(node->combiner));

    /*
     * Clean up remote connections
     */
    pgxc_connections_cleanup(combiner);

    /*
     * Clean up parameters if they were set, since plan may be reused
     */
    if (node->paramval_data)
    {
        pfree(node->paramval_data);
        node->paramval_data = NULL;
        node->paramval_len = 0;
    }

    CloseCombiner(combiner);
    pfree(node);
}
/*
 * End the remote query
 */
static void
PolarxFQSEndScan(CustomScanState *pstate)
{
    FastShipQueryState *fsp_state = (FastShipQueryState *)pstate;
    RemoteQueryState *node = fsp_state->remoteQueryState;
    ExecEndRemoteQuery(node);
}

/*
 * get_success_nodes:
 * Currently called to print a user-friendly message about
 * which nodes the query failed.
 * Gets all the nodes where no 'E' (error) messages were received; i.e. where the
 * query ran successfully.
 */
static ExecNodes *
get_success_nodes(int node_count, PGXCNodeHandle **handles, char node_type, StringInfo failednodes)
{
    ExecNodes *success_nodes = NULL;
    int i;

    for (i = 0; i < node_count; i++)
    {
        PGXCNodeHandle *handle = handles[i];
        int nodenum = PGXCNodeGetNodeId(handle->nodeoid, &node_type);

        if (handle->error[0])
        {
            if (!success_nodes)
                success_nodes = polarxMakeNode(ExecNodes);
            success_nodes->nodeList = lappend_int(success_nodes->nodeList, nodenum);
        }
        else
        {
            if (failednodes->len == 0)
                appendStringInfo(failednodes, "Error message received from nodes:");
            appendStringInfo(failednodes, " %s#%d",
                             (node_type == PGXC_NODE_COORDINATOR ? "coordinator" : "datanode"),
                             nodenum + 1);
        }
    }
    return success_nodes;
}

/*
 * pgxc_all_success_nodes: Uses get_success_nodes() to collect the
 * user-friendly message from coordinator as well as datanode.
 */
void pgxc_all_success_nodes(ExecNodes **d_nodes, ExecNodes **c_nodes, char **failednodes_msg)
{
    PGXCNodeAllHandles *connections = get_exec_connections(NULL, NULL, EXEC_ON_ALL_NODES, true);
    StringInfoData failednodes;
    initStringInfo(&failednodes);

    *d_nodes = get_success_nodes(connections->dn_conn_count,
                                 connections->datanode_handles,
                                 PGXC_NODE_DATANODE,
                                 &failednodes);

    *c_nodes = get_success_nodes(connections->co_conn_count,
                                 connections->coord_handles,
                                 PGXC_NODE_COORDINATOR,
                                 &failednodes);

    if (failednodes.len == 0)
        *failednodes_msg = NULL;
    else
        *failednodes_msg = failednodes.data;

    pfree_pgxc_all_handles(connections);
}

/*
 * Execute utility statement on multiple Datanodes
 * It does approximately the same as
 *
 * But does not need an Estate instance and does not do some unnecessary work,
 * like allocating tuple slots.
 */
void ExecRemoteUtility(RemoteQuery *node)
{ // #lizard forgives
    RemoteQueryState *remotestate;
    ResponseCombiner *combiner;
    bool force_autocommit = node->force_autocommit;
    RemoteQueryExecType exec_type = node->exec_type;
    PGXCNodeAllHandles *pgxc_connections;
    int co_conn_count;
    int dn_conn_count;
    bool need_tran_block;
    ExecDirectType exec_direct_type = node->exec_direct_type;
    int i;

    if (!force_autocommit)
    {
        if (!g_in_set_config_option)
            RegisterTransactionLocalNode(true);
    }

    remotestate = polarxMakeNode(RemoteQueryState);
    combiner = (ResponseCombiner *)(&(remotestate->combiner));
    InitResponseCombiner(combiner, 0, node->combine_type);

    /*
     * Do not set global_session if it is a utility statement.
     * Avoids CREATE NODE error on cluster configuration.
     */
    pgxc_connections = get_exec_connections(NULL, node->exec_nodes, exec_type,
                                            exec_direct_type != EXEC_DIRECT_UTILITY);

    dn_conn_count = pgxc_connections->dn_conn_count;
    co_conn_count = pgxc_connections->co_conn_count;
    /* exit right away if no nodes to run command on */
    if (dn_conn_count == 0 && co_conn_count == 0)
    {
        pfree_pgxc_all_handles(pgxc_connections);
        return;
    }

    if (force_autocommit)
        need_tran_block = false;
    else
        need_tran_block = true;

    /* Commands launched through EXECUTE DIRECT do not need start a transaction */
    if (exec_direct_type == EXEC_DIRECT_UTILITY)
    {
        need_tran_block = false;

        /* This check is not done when analyzing to limit dependencies */
        if (IsTransactionBlock())
            ereport(ERROR,
                    (errcode(ERRCODE_ACTIVE_SQL_TRANSACTION),
                     errmsg("cannot run EXECUTE DIRECT with utility inside a transaction block")));
    }

    /* send command */
    {
        if (pgxc_node_begin(dn_conn_count, pgxc_connections->datanode_handles,
                            0, need_tran_block, g_in_set_config_option ? true : false, PGXC_NODE_DATANODE))
            ereport(ERROR,
                    (errcode(ERRCODE_INTERNAL_ERROR),
                     errmsg("Could not begin transaction on Datanodes")));
        for (i = 0; i < dn_conn_count; i++)
        {
            PGXCNodeHandle *conn = pgxc_connections->datanode_handles[i];

            if (conn->state == DN_CONNECTION_STATE_QUERY)
                BufferConnection(conn);
			conn->combiner = NULL;
			/* To support shard table, replace original create table statement to partition table */
            char* sql_string = node->sql_statement;
            if (node->need_dn_create_partition_table_cmd)
                sql_string = GenDNCreatePartitionTableCmd( node->relationId,
                                                           node->sql_statement, 
                                                           node->schema_name,
                                                           node->tbl_name,
                                                           node->dist_col_name,
                                                           node->hash_func_name,
                                                           node->with_oids);
            if (pgxc_node_send_query(conn, sql_string) != 0)
            {
                ereport(ERROR,
                        (errcode(ERRCODE_INTERNAL_ERROR),
                         errmsg("Failed to send command to Datanodes")));
            }
        }
    }
    {
        if (pgxc_node_begin(co_conn_count, pgxc_connections->coord_handles,
                            0, need_tran_block, g_in_set_config_option ? true : false, PGXC_NODE_COORDINATOR))
            ereport(ERROR,
                    (errcode(ERRCODE_INTERNAL_ERROR),
                     errmsg("Could not begin transaction on coordinators")));
        /* Now send it to Coordinators if necessary */
        for (i = 0; i < co_conn_count; i++)
        {
            PGXCNodeHandle *conn = pgxc_connections->coord_handles[i];
            if (conn->state == DN_CONNECTION_STATE_QUERY)
                BufferConnection(conn);
            conn->combiner = NULL;
            if (pgxc_node_send_query(conn, node->sql_statement) != 0)
            {
                ereport(ERROR,
                        (errcode(ERRCODE_INTERNAL_ERROR),
                         errmsg("Failed to send command to coordinators")));
            }
        }
    }

    /*
     * Stop if all commands are completed or we got a data row and
     * initialized state node for subsequent invocations
     */
    while (dn_conn_count > 0)
    {
        int i = 0;

        if (pgxc_node_receive(dn_conn_count, pgxc_connections->datanode_handles, NULL))
            break;
        /*
            * Handle input from the Datanodes.
            * We do not expect Datanodes returning tuples when running utility
            * command.
            * If we got EOF, move to the next connection, will receive more
            * data on the next iteration.
            */
        while (i < dn_conn_count)
        {
            PGXCNodeHandle *conn = pgxc_connections->datanode_handles[i];
            int res = handle_response(conn, combiner);
            if (res == RESPONSE_EOF)
            {
                i++;
            }
            else if (res == RESPONSE_COMPLETE)
            {
                /* Ignore, wait for ReadyForQuery */
                if (conn->state == DN_CONNECTION_STATE_ERROR_FATAL)
                {
                    ereport(ERROR,
                            (errcode(ERRCODE_INTERNAL_ERROR),
                             errmsg("Unexpected FATAL ERROR on Connection to Datanode %s pid %d",
                                    conn->nodename, conn->backend_pid)));
                }
            }
            else if (res == RESPONSE_ERROR)
            {
                /* Ignore, wait for ReadyForQuery */
            }
            else if (res == RESPONSE_READY)
            {
                if (i < --dn_conn_count)
                    pgxc_connections->datanode_handles[i] =
                        pgxc_connections->datanode_handles[dn_conn_count];
            }
            else if (res == RESPONSE_TUPDESC)
            {
            }
            else if (res == RESPONSE_DATAROW)
            {
            }
        }
    }

    /* Make the same for Coordinators */

    while (co_conn_count > 0)
    {
        int i = 0;

        if (pgxc_node_receive(co_conn_count, pgxc_connections->coord_handles, NULL))
            break;

        while (i < co_conn_count)
        {
            int res = handle_response(pgxc_connections->coord_handles[i], combiner);
            if (res == RESPONSE_EOF)
            {
                i++;
            }
            else if (res == RESPONSE_COMPLETE)
            {
                /* Ignore, wait for ReadyForQuery */
                if (pgxc_connections->coord_handles[i]->state == DN_CONNECTION_STATE_ERROR_FATAL)
                {
                    ereport(ERROR,
                            (errcode(ERRCODE_INTERNAL_ERROR),
                             errmsg("Unexpected FATAL ERROR on Connection to Coordinator %s pid %d",
                                    pgxc_connections->coord_handles[i]->nodename, pgxc_connections->coord_handles[i]->backend_pid)));
                }
            }
            else if (res == RESPONSE_ERROR)
            {
                /* Ignore, wait for ReadyForQuery */
            }
            else if (res == RESPONSE_READY)
            {
                if (i < --co_conn_count)
                    pgxc_connections->coord_handles[i] =
                        pgxc_connections->coord_handles[co_conn_count];
            }
            else if (res == RESPONSE_TUPDESC)
            {
                ereport(ERROR,
                        (errcode(ERRCODE_INTERNAL_ERROR),
                         errmsg("Unexpected response from coordinator")));
            }
            else if (res == RESPONSE_DATAROW)
            {
                ereport(ERROR,
                        (errcode(ERRCODE_INTERNAL_ERROR),
                         errmsg("Unexpected response from coordinator")));
            }
        }
    }

    /*
     * We have processed all responses from nodes and if we have
     * error message pending we can report it. All connections should be in
     * consistent state now and so they can be released to the pool after ROLLBACK.
     */
    pfree_pgxc_all_handles(pgxc_connections);
    pgxc_node_report_error(combiner);
}

void ExecCloseRemoteStatement(const char *stmt_name, List *nodelist)
{ // #lizard forgives
    PGXCNodeAllHandles *all_handles;
    PGXCNodeHandle **connections;
    ResponseCombiner combiner;
    int conn_count;
    int i;

    /* Exit if nodelist is empty */
    if (list_length(nodelist) == 0)
        return;

    /* get needed Datanode connections */
    all_handles = get_handles(nodelist, NIL, false, true);
    conn_count = all_handles->dn_conn_count;
    connections = all_handles->datanode_handles;

    for (i = 0; i < conn_count; i++)
    {
        if (connections[i]->state == DN_CONNECTION_STATE_QUERY)
            BufferConnection(connections[i]);
        connections[i]->combiner = NULL;
        if (pgxc_node_send_close(connections[i], true, stmt_name) != 0)
        {
            /*
             * statements are not affected by statement end, so consider
             * unclosed statement on the Datanode as a fatal issue and
             * force connection is discarded
             */
            PGXCNodeSetConnectionState(connections[i],
                                       DN_CONNECTION_STATE_ERROR_FATAL);
            ereport(WARNING,
                    (errcode(ERRCODE_INTERNAL_ERROR),
                     errmsg("Failed to close Datanode statemrnt")));
        }
        if (pgxc_node_send_sync(connections[i]) != 0)
        {
            PGXCNodeSetConnectionState(connections[i],
                                       DN_CONNECTION_STATE_ERROR_FATAL);
            ereport(WARNING,
                    (errcode(ERRCODE_INTERNAL_ERROR),
                     errmsg("Failed to close Datanode statement")));

            ereport(LOG,
                    (errcode(ERRCODE_INTERNAL_ERROR),
                     errmsg("Failed to sync msg to node %s backend_pid:%d", connections[i]->nodename, connections[i]->backend_pid)));
        }
        PGXCNodeSetConnectionState(connections[i], DN_CONNECTION_STATE_CLOSE);
    }

    InitResponseCombiner(&combiner, conn_count, COMBINE_TYPE_NONE);
    /*
     * Make sure there are zeroes in unused fields
     */
    memset(&combiner, 0, sizeof(ScanState));

    while (conn_count > 0)
    {
        if (pgxc_node_receive(conn_count, connections, NULL))
        {
            for (i = 0; i < conn_count; i++)
                PGXCNodeSetConnectionState(connections[i],
                                           DN_CONNECTION_STATE_ERROR_FATAL);

            ereport(ERROR,
                    (errcode(ERRCODE_INTERNAL_ERROR),
                     errmsg("Failed to close Datanode statement")));
        }
        i = 0;
        while (i < conn_count)
        {
            int res = handle_response(connections[i], &combiner);
            if (res == RESPONSE_EOF)
            {
                i++;
            }
            else if (res == RESPONSE_READY ||
                     connections[i]->state == DN_CONNECTION_STATE_ERROR_FATAL)
            {
                if (--conn_count > i)
                    connections[i] = connections[conn_count];
            }
        }
    }

    ValidateAndCloseCombiner(&combiner);
    pfree_pgxc_all_handles(all_handles);
}

static void
show_expression(Node *node, const char *qlabel,
                PlanState *planstate, List *ancestors,
                bool useprefix, ExplainState *es)
{
    List *context;
    char *exprstr;

    /* Set up deparsing context */
    context = set_deparse_context_planstate(es->deparse_cxt,
                                            (Node *)planstate,
                                            ancestors);

    /* Deparse the expression */
    exprstr = deparse_expression(node, context, useprefix, false);

    /* And add to es->str */
    ExplainPropertyText(qlabel, exprstr, es);
}
static void
PolarxFQSExplainScan(CustomScanState *pstate, List *ancestors, struct ExplainState *es)
{
    FastShipQueryState *fsp_state = (FastShipQueryState *)pstate;
    PlanState *planstate = (PlanState *)(&(fsp_state->remoteQueryState->combiner));
    RemoteQuery *plan = (RemoteQuery *)(fsp_state->remoteQueryState->remote_query);
    ExecNodes *en = plan->exec_nodes;
    /* add names of the nodes if they exist */
    if (en)
    {
        StringInfo node_names = makeStringInfo();
        ListCell *lcell;
        char *sep;
        int node_no;
        if (en->primarynodelist)
        {
            sep = "";
            foreach (lcell, en->primarynodelist)
            {
                node_no = lfirst_int(lcell);
                appendStringInfo(node_names, "%s%s", sep,
                                 get_pgxc_nodename(PGXCNodeGetNodeOid(node_no, PGXC_NODE_DATANODE)));
                sep = ", ";
            }
            ExplainPropertyText("Primary node/s", node_names->data, es);
        }
        if (en->nodeList)
        {
            resetStringInfo(node_names);
            sep = "";
            foreach (lcell, en->nodeList)
            {
                node_no = lfirst_int(lcell);
                appendStringInfo(node_names, "%s%s", sep,
                                 get_pgxc_nodename(PGXCNodeGetNodeOid(node_no, PGXC_NODE_DATANODE)));

                sep = ", ";
            }
            //ExplainPropertyText("Node/s", node_names->data, es);
        }
    }

    if (en && en->en_expr)
        show_expression((Node *)en->en_expr, "Node expr", planstate, ancestors,
                        es->verbose, es);

    /* Remote query statement */
    if (es->verbose)
        ExplainPropertyText("Remote query", plan->sql_statement, es);

    if (!es->analyze)
    {
        RemoteQuery *step = polarxMakeNode(RemoteQuery);
        StringInfoData explainQuery;
        StringInfoData explainResult;
        EState *estate;
        RemoteQueryState *node;
        Var *dummy;
        MemoryContext oldcontext;
        TupleTableSlot *result;
        bool firstline = true;

        initStringInfo(&explainQuery);
        initStringInfo(&explainResult);

        if (es->format == EXPLAIN_FORMAT_TEXT)
        {
            if (es->indent)
            {
                appendStringInfoSpaces(es->str, es->indent * 2);
                appendStringInfoString(es->str, "->  ");
                es->indent += 2;
            }
        }

        appendStringInfo(&explainQuery, "EXPLAIN (");
        switch (es->format)
        {
        case EXPLAIN_FORMAT_TEXT:
            appendStringInfo(&explainQuery, "FORMAT TEXT");
            break;
        case EXPLAIN_FORMAT_YAML:
            appendStringInfo(&explainQuery, "FORMAT YAML");
            break;
        case EXPLAIN_FORMAT_JSON:
            appendStringInfo(&explainQuery, "FORMAT JSON");
            break;
        case EXPLAIN_FORMAT_XML:
            appendStringInfo(&explainQuery, "FORMAT XML");
            break;
        }
        appendStringInfo(&explainQuery, ", VERBOSE %s", es->verbose ? "ON" : "OFF");
        appendStringInfo(&explainQuery, ", COSTS %s", es->costs ? "ON" : "OFF");
        appendStringInfo(&explainQuery, ", TIMING %s", es->timing ? "ON" : "OFF");
        appendStringInfo(&explainQuery, ", BUFFERS %s", es->buffers ? "ON" : "OFF");
        appendStringInfo(&explainQuery, ") %s", plan->sql_statement);

        step->sql_statement = explainQuery.data;
        step->combine_type = COMBINE_TYPE_NONE;
        step->exec_nodes = copyObject(plan->exec_nodes);
        step->exec_nodes->primarynodelist = NIL;

        if (plan->exec_nodes && plan->exec_nodes->nodeList)
            step->exec_nodes->nodeList =
                list_make1_int(linitial_int(plan->exec_nodes->nodeList));

        step->force_autocommit = true;
        step->exec_type = EXEC_ON_DATANODES;

        dummy = makeVar(1, 1, TEXTOID, -1, InvalidOid, 0);
        step->scan.plan.targetlist = lappend(step->scan.plan.targetlist,
                                             makeTargetEntry((Expr *)dummy, 1, "QUERY PLAN", false));
        estate = planstate->state;
        oldcontext = MemoryContextSwitchTo(estate->es_query_cxt);

        node = ExecInitRemoteQuery(step, estate, 0);
        MemoryContextSwitchTo(oldcontext);
        result = ExecRemoteQuery((PlanState *)(&(node->combiner)));
        while (result != NULL && !TupIsNull(result))
        {
            Datum value;
            bool isnull;
            value = slot_getattr(result, 1, &isnull);
            if (!isnull)
            {
                if (!firstline)
                    appendStringInfoSpaces(&explainResult, 2 * es->indent);
                appendStringInfo(&explainResult, "%s\n", TextDatumGetCString(value));
                firstline = false;
            }

            /* fetch next */
            result = ExecRemoteQuery((PlanState *)(&(node->combiner)));
        }
        ExecEndRemoteQuery(node);

        if (es->format == EXPLAIN_FORMAT_TEXT)
            appendStringInfo(es->str, "%s", explainResult.data);
        else
            ExplainPropertyText("Remote plan", explainResult.data, es);
    }
}
static void
deform_datarow_into_slot(RemoteDataRow datarow, TupleTableSlot *slot)
{// #lizard forgives
    int natts;
    int i;
    int         col_count;
    char       *cur = datarow->msg;
    StringInfo  buffer;
    uint16        n16;
    uint32        n32;
    MemoryContext oldcontext;
    AttInMetadata *tts_attinmeta = NULL;

    Assert(slot->tts_tupleDescriptor != NULL);
    Assert(datarow != NULL);

    natts = slot->tts_tupleDescriptor->natts;

    /* fastpath: exit if values already extracted */
    if (slot->tts_nvalid == natts)
        return;

    memcpy(&n16, cur, 2);
    cur += 2;
    col_count = ntohs(n16);

    if (col_count != natts)
        ereport(ERROR,
                (errcode(ERRCODE_DATA_CORRUPTED),
                 errmsg("Tuple does not match the descriptor")));

    /*
     * Ensure info about input functions is available as long as slot lives
     */
    oldcontext = MemoryContextSwitchTo(slot->tts_mcxt);
    tts_attinmeta = TupleDescGetAttInMetadata(slot->tts_tupleDescriptor);
    MemoryContextSwitchTo(oldcontext);

    buffer = makeStringInfo();
    for (i = 0; i < natts; i++)
    {
        Form_pg_attribute attr = TupleDescAttr(slot->tts_tupleDescriptor, i);
        int len;

        /* get size */
        memcpy(&n32, cur, 4);
        cur += 4;
        len = ntohl(n32);

        /* get data */
        if (len == -1)
        {
            slot->tts_values[i] = (Datum) 0;
            slot->tts_isnull[i] = true;
        }
        else if (len == -2)
        {
            /* composite type */
            TupleDesc tupDesc;

            memcpy(&n32, cur, 4);
            cur += 4;
            len = ntohl(n32);

            appendBinaryStringInfo(buffer, cur, len);

            ignore_describe_response(buffer->data, len);
            tupDesc = slot->tts_tupleDescriptor;

            assign_record_type_typmod(tupDesc);

            resetStringInfo(buffer);

            cur += len;

            memcpy(&n32, cur, 4);
            cur += 4;
            len = ntohl(n32);

            appendBinaryStringInfo(buffer, cur, len);
            cur += len;

            slot->tts_values[i] = InputFunctionCall(tts_attinmeta->attinfuncs + i,
                    buffer->data,
                    tts_attinmeta->attioparams[i],
                    tupDesc->tdtypmod);
            slot->tts_isnull[i] = false;

            resetStringInfo(buffer);

            if (!attr->attbyval)
            {
                Pointer        val = DatumGetPointer(slot->tts_values[i]);
                Size        data_length;
                void       *data;

                if (attr->attlen == -1)
                {
                    /* varlena */
                    data_length = VARSIZE_ANY(val);
                }
                else if (attr->attlen == -2)
                {
                    /* cstring */
                    data_length = strlen(val) + 1;
                }
                else
                {
                    /* fixed-length pass-by-reference */
                    data_length = attr->attlen;
                }
                data = MemoryContextAlloc(slot->tts_mcxt, data_length);
                memcpy(data, val, data_length);

                pfree(val);

                slot->tts_values[i] = PointerGetDatum(data);
            }
        }
        else
        {
            appendBinaryStringInfo(buffer, cur, len);
            cur += len;

            slot->tts_values[i] = InputFunctionCall(tts_attinmeta->attinfuncs + i,
                    buffer->data,
                    tts_attinmeta->attioparams[i],
                    tts_attinmeta->atttypmods[i]);
            slot->tts_isnull[i] = false;

            resetStringInfo(buffer);

            /*
             * The input function was executed in caller's memory context,
             * because it may be allocating working memory, and caller may
             * want to clean it up.
             * However returned Datums need to be in the special context, so
             * if attribute is pass-by-reference, copy it.
             */
            if (!attr->attbyval)
            {
                Pointer        val = DatumGetPointer(slot->tts_values[i]);
                Size        data_length;
                void       *data;

                if (attr->attlen == -1)
                {
                    /* varlena */
                    data_length = VARSIZE_ANY(val);
                }
                else if (attr->attlen == -2)
                {
                    /* cstring */
                    data_length = strlen(val) + 1;
                }
                else
                {
                    /* fixed-length pass-by-reference */
                    data_length = attr->attlen;
                }
                data = MemoryContextAlloc(slot->tts_mcxt, data_length);
                memcpy(data, val, data_length);

                pfree(val);

                slot->tts_values[i] = PointerGetDatum(data);
            }
        }
    }
    pfree(buffer->data);
    pfree(buffer);

    slot->tts_nvalid = natts;
    slot->tts_isempty = false;
    slot->tts_shouldFree = false;
    slot->tts_shouldFreeMin = false;
}
/*
   Encode parameter values to format of DataRow message (the same format is
 * used in Bind) to prepare for sending down to Datanodes.
 * The data row is copied to RemoteQueryState.paramval_data.
 */
static void
SetDataRowForExtParams(ParamListInfo paraminfo, RemoteQueryState *rq_state)
{
    StringInfoData buf;
    uint16 n16;
    int i;
    int real_num_params = paraminfo->numParams;
    RemoteQuery *node = (RemoteQuery*) rq_state->remote_query;

    /* If there are no parameters, there is no data to BIND. */
    if (!paraminfo)
        return;

    Assert(!rq_state->paramval_data);

    /*
     * If there are no parameters available, simply leave.
     * This is possible in the case of a query called through SPI
     * and using no parameters.
     */
    if (real_num_params == 0)
    {
        rq_state->paramval_data = NULL;
        rq_state->paramval_len = 0;
        return;
    }

    initStringInfo(&buf);

    /* Number of parameter values */
    n16 = htons(real_num_params);
    appendBinaryStringInfo(&buf, (char *) &n16, 2);

    /* Parameter values */
    for (i = 0; i < real_num_params; i++)
    {
        ParamExternData *param = &paraminfo->params[i];
        uint32 n32;

        /*
         * Parameters with no types are considered as NULL and treated as integer
         * The same trick is used for dropped columns for remote DML generation.
         */
        if (param->isnull || !OidIsValid(param->ptype))
        {
            n32 = htonl(-1);
            appendBinaryStringInfo(&buf, (char *) &n32, 4);
        }
        else
        {
            Oid     typOutput;
            bool    typIsVarlena;
            Datum   pval;
            char   *pstring;
            int     len;

            /* Get info needed to output the value */
            getTypeOutputInfo(param->ptype, &typOutput, &typIsVarlena);

            /*
             * If we have a toasted datum, forcibly detoast it here to avoid
             * memory leakage inside the type's output routine.
             */
            if (typIsVarlena)
                pval = PointerGetDatum(PG_DETOAST_DATUM(param->value));
            else
                pval = param->value;

            /* Convert Datum to string */
            pstring = OidOutputFunctionCall(typOutput, pval);

            /* copy data to the buffer */
            len = strlen(pstring);
            n32 = htonl(len);
            appendBinaryStringInfo(&buf, (char *) &n32, 4);
            appendBinaryStringInfo(&buf, pstring, len);
        }
    }


    /*
     * If parameter types are not already set, infer them from
     * the paraminfo.
     */
    if (node->rq_num_params > 0)
    {
        /*
         * Use the already known param types for BIND. Parameter types
         * can be already known when the same plan is executed multiple
         * times.
         */
        if (node->rq_num_params != real_num_params)
            elog(ERROR, "Number of user-supplied parameters do not match "
                    "the number of remote parameters");
        rq_state->rqs_num_params = node->rq_num_params;
        rq_state->rqs_param_types = node->rq_param_types;
    }
    else
    {
        rq_state->rqs_num_params = real_num_params;
        rq_state->rqs_param_types = (Oid *) palloc(sizeof(Oid) * real_num_params);
        for (i = 0; i < real_num_params; i++)
            rq_state->rqs_param_types[i] = paraminfo->params[i].ptype;
    }

    /* Assign the newly allocated data row to paramval */
    rq_state->paramval_data = buf.data;
    rq_state->paramval_len = buf.len;
}

static int
prepare_fqs_sql(const char *p_name, const char *query,
                 PGXCNodeHandle **connections, int conn_num)
{
    int     i = 0;
    int     j = 0;
    PGXCNodeHandle      *res_conns[conn_num];
    ResponseCombiner    combiner;
    int         complete_num = 0;
    char *prep_name;
    int self_version;

    if(conn_num == 0)
        return 0;

    self_version = GetPrepStmtNameAndSelfVersion(p_name, &prep_name);
    for (j = 0; j < conn_num; j++)
    {
        bool is_expired = false; 
        bool is_prepared = false; 
       
        is_prepared = IsStmtPrepared(prep_name,
                                        PGXCNodeGetNodeId(connections[j]->nodeoid, NULL),
                                        self_version,
                                        &is_expired);
        if(is_prepared && is_expired == false)
        {
            continue;
        }
        if (connections[j]->state == DN_CONNECTION_STATE_QUERY)
            BufferConnection(connections[j]);

        if(is_expired)
        {
            pgxc_node_send_close(connections[j], true, prep_name);
        }
        if (pgxc_node_send_parse(connections[j], prep_name, query, 0, NULL) != 0)
        {
            PGXCNodeSetConnectionState(connections[j],
                    DN_CONNECTION_STATE_ERROR_FATAL);
            ereport(WARNING,
                    (errcode(ERRCODE_INTERNAL_ERROR),
                     errmsg("Failed to prepare statement: %s, in node: %d", query, connections[j]->nodeoid)));
        }
        if (pgxc_node_send_sync(connections[j]) != 0)
        {
            PGXCNodeSetConnectionState(connections[j],
                    DN_CONNECTION_STATE_ERROR_FATAL);
            ereport(WARNING,
                    (errcode(ERRCODE_INTERNAL_ERROR),
                     errmsg("Failed to close Datanode cursor")));
        }
        PGXCNodeSetConnectionState(connections[j], DN_CONNECTION_STATE_QUERY);
        res_conns[i++] = connections[j];
    }
    if(i == 0)
        return 0;
    conn_num = i;
    j = i;
    InitResponseCombiner(&combiner, conn_num, COMBINE_TYPE_NONE);
    memset(&combiner, 0, sizeof(ScanState));
    combiner.request_type = REQUEST_TYPE_QUERY;
    if(p_name)
    {
        combiner.prep_name = pstrdup(p_name);
    }

    while (conn_num > 0)
    {
        if (pgxc_node_receive(conn_num, res_conns, NULL))
            ereport(ERROR,
                    (errcode(ERRCODE_INTERNAL_ERROR),
                     errmsg("Failed to send prepared in receive result")));
        i = 0;
        while (i < conn_num)
        {
            int res = handle_response(res_conns[i], &combiner);
            if (res == RESPONSE_EOF)
            {
                i++;
            }
            else if (res == RESPONSE_READY || res_conns[i]->state == DN_CONNECTION_STATE_ERROR_FATAL)
            {
                if(res == RESPONSE_READY)
                {
                    complete_num++;
                }
                if (--conn_num > i)
                {
                    res_conns[i] = res_conns[conn_num];
                }
            }
            else
            {
            }
        }
    }

    if(complete_num != j)
    {
        ereport(ERROR,
                (errcode(ERRCODE_INTERNAL_ERROR),
                 errmsg("Failed to send prepared due to not complete all")));
    }

    CloseCombiner(&combiner);
    return complete_num;
}

static char *
generate_relation_name(Oid relid, List *namespaces)
{
    HeapTuple	tp;
    Form_pg_class reltup;
    bool		need_qual;
    ListCell   *nslist;
    char	   *relname;
    char	   *nspname;
    char	   *result;

    tp = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
    if (!HeapTupleIsValid(tp))
        elog(ERROR, "cache lookup failed for relation %u", relid);
    reltup = (Form_pg_class) GETSTRUCT(tp);
    relname = NameStr(reltup->relname);

    /* Check for conflicting CTE name */
    need_qual = false;
    foreach(nslist, namespaces)
    {
        deparse_namespace *dpns = (deparse_namespace *) lfirst(nslist);
        ListCell   *ctlist;

        foreach(ctlist, dpns->ctes)
        {
            CommonTableExpr *cte = (CommonTableExpr *) lfirst(ctlist);

            if (strcmp(cte->ctename, relname) == 0)
            {
                need_qual = true;
                break;
            }
        }
        if (need_qual)
            break;
    }

    /* Otherwise, qualify the name if not visible in search path */
    if (!need_qual)
        need_qual = !RelationIsVisible(relid);

    if (need_qual)
        nspname = get_namespace_name(reltup->relnamespace);
    else
        nspname = NULL;

    result = quote_qualified_identifier(nspname, relname);

    ReleaseSysCache(tp);

    return result;
}

static bool contain_nextval_expression_walker(Node *node, void *context)
{
    if (node == NULL)
    {
        return false;
    }

    if (IsA(node, FuncExpr))
    {
        FuncExpr *funcExpr = (FuncExpr *) node;

        if (funcExpr->funcid == F_NEXTVAL_OID)
        {
            return true;
        }
    }
    return expression_tree_walker(node, contain_nextval_expression_walker, context);
}

/*
 * Generate a C string representing a relation's reloptions, or NULL if none.
 */
static char *
flatten_reloptions(Oid relid)
{
    char	   *result = NULL;
    HeapTuple	tuple;
    Datum		reloptions;
    bool		isnull;

    tuple = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
    if (!HeapTupleIsValid(tuple))
        elog(ERROR, "cache lookup failed for relation %u", relid);

    reloptions = SysCacheGetAttr(RELOID, tuple,
                                 Anum_pg_class_reloptions, &isnull);
    if (!isnull)
    {
        StringInfoData buf;
        Datum	   *options;
        int			noptions;
        int			i;

        initStringInfo(&buf);

        deconstruct_array(DatumGetArrayTypeP(reloptions),
                          TEXTOID, -1, false, 'i',
                          &options, NULL, &noptions);

        for (i = 0; i < noptions; i++)
        {
            char	   *option = TextDatumGetCString(options[i]);
            char	   *name;
            char	   *separator;
            char	   *value;

            /*
             * Each array element should have the form name=value.  If the "="
             * is missing for some reason, treat it like an empty value.
             */
            name = option;
            separator = strchr(option, '=');
            if (separator)
            {
                *separator = '\0';
                value = separator + 1;
            }
            else
                value = "";

            if (i > 0)
                appendStringInfoString(&buf, ", ");
            appendStringInfo(&buf, "%s=", quote_identifier(name));

            /*
             * In general we need to quote the value; but to avoid unnecessary
             * clutter, do not quote if it is an identifier that would not
             * need quoting.  (We could also allow numbers, but that is a bit
             * trickier than it looks --- for example, are leading zeroes
             * significant?  We don't want to assume very much here about what
             * custom reloptions might mean.)
             */
            if (quote_identifier(value) == value)
                appendStringInfoString(&buf, value);
            else
                simple_quote_literal(&buf, value);

            pfree(option);
        }

        result = buf.data;
    }

    ReleaseSysCache(tuple);

    return result;
}

/*
 * simple_quote_literal - Format a string as a SQL literal, append to buf
 */
static void
simple_quote_literal(StringInfo buf, const char *val)
{
    const char *valptr;

    /*
     * We form the string literal according to the prevailing setting of
     * standard_conforming_strings; we never use E''. User is responsible for
     * making sure result is used correctly.
     */
    appendStringInfoChar(buf, '\'');
    for (valptr = val; *valptr; valptr++)
    {
        char		ch = *valptr;

        if (SQL_STR_DOUBLE(ch, !standard_conforming_strings))
            appendStringInfoChar(buf, ch);
        appendStringInfoChar(buf, ch);
    }
    appendStringInfoChar(buf, '\'');
}


static char* GetTableCreateCommand(Oid relationId, char *dist_col_name, char* hash_func_name, bool with_oids)
{
    int attIndex = 0;
    bool firstAttributePrinted = false;
    AttrNumber defaultValueIndex = 0;
    AttrNumber constraintIndex = 0;
    AttrNumber constraintCount = 0;
    StringInfoData buffer = { NULL, 0, 0, 0 };
    const char* postfix_name = NULL;
    
    Relation relation = relation_open(relationId, NoLock);
    char *relationName = generate_relation_name(relationId, NIL);

    initStringInfo(&buffer);
    
    appendStringInfoString(&buffer, "CREATE ");

    if (relation->rd_rel->relpersistence == RELPERSISTENCE_UNLOGGED)
    {
        appendStringInfoString(&buffer, "UNLOGGED ");
    }

    appendStringInfo(&buffer, "TABLE %s (", relationName);
    
    TupleDesc tupleDescriptor = RelationGetDescr(relation);
    TupleConstr *tupleConstraints = tupleDescriptor->constr;

    for (attIndex = 0; attIndex < tupleDescriptor->natts; attIndex++)
    {
        Form_pg_attribute attributeForm = TupleDescAttr(tupleDescriptor, attIndex);
        
        if (!attributeForm->attisdropped)
        {
            if (firstAttributePrinted)
            {
                appendStringInfoString(&buffer, ", ");
            }
            firstAttributePrinted = true;

            const char *attributeName = NameStr(attributeForm->attname);
            appendStringInfo(&buffer, "%s ", quote_identifier(attributeName));

            const char *attributeTypeName = format_type_with_typemod(
                    attributeForm->atttypid,
                    attributeForm->
                            atttypmod);
            appendStringInfoString(&buffer, attributeTypeName);

            /* if this column has a default value, append the default value */
            if (attributeForm->atthasdef)
            {
                List *defaultContext = NULL;
                char *defaultString = NULL;

                Assert(tupleConstraints != NULL);

                AttrDefault *defaultValueList = tupleConstraints->defval;
                Assert(defaultValueList != NULL);

                AttrDefault *defaultValue = &(defaultValueList[defaultValueIndex]);
                defaultValueIndex++;

                Assert(defaultValue->adnum == (attIndex + 1));
                Assert(defaultValueIndex <= tupleConstraints->num_defval);

                /* convert expression to node tree, and prepare deparse context */
                Node *defaultNode = (Node *) stringToNode(defaultValue->adbin);

                /*
                 * if column default value is explicitly requested, or it is
                 * not set from a sequence then we include DEFAULT clause for
                 * this column.
                 */
                if (!contain_nextval_expression_walker(defaultNode, NULL))
                {
                    defaultContext = deparse_context_for(relationName, relationId);

                    /* deparse default value string */
                    defaultString = deparse_expression(defaultNode, defaultContext,
                                                       false, false);
                    
                    appendStringInfo(&buffer, " DEFAULT %s", defaultString);
                }
            }

            /* if this column has a not null constraint, append the constraint */
            if (attributeForm->attnotnull)
            {
                appendStringInfoString(&buffer, " NOT NULL");
            }

            if (attributeForm->attcollation != InvalidOid &&
                attributeForm->attcollation != DEFAULT_COLLATION_OID)
            {
                appendStringInfo(&buffer, " COLLATE %s", generate_collation_name(
                        attributeForm->attcollation));
            }
        }
    }

    /*
     * Now check if the table has any constraints. If it does, set the number of
     * check constraints here. Then iterate over all check constraints and print
     * them.
     */
    if (tupleConstraints != NULL)
    {
        constraintCount = tupleConstraints->num_check;
    }

    for (constraintIndex = 0; constraintIndex < constraintCount; constraintIndex++)
    {
        ConstrCheck *checkConstraintList = tupleConstraints->check;
        ConstrCheck *checkConstraint = &(checkConstraintList[constraintIndex]);


        /* if an attribute or constraint has been printed, format properly */
        if (firstAttributePrinted || constraintIndex > 0)
        {
            appendStringInfoString(&buffer, ", ");
        }

        appendStringInfo(&buffer, "CONSTRAINT %s CHECK ",
                         quote_identifier(checkConstraint->ccname));

        /* convert expression to node tree, and prepare deparse context */
        Node *checkNode = (Node *) stringToNode(checkConstraint->ccbin);
        List *checkContext = deparse_context_for(relationName, relationId);

        /* deparse check constraint string */
        char *checkString = deparse_expression(checkNode, checkContext, false, false);

        appendStringInfoString(&buffer, checkString);
    }

    /* close create table's outer parentheses */
    appendStringInfoString(&buffer, ")");
    
    appendStringInfo(&buffer, " PARTITION BY RANGE(%s(%s", hash_func_name, dist_col_name);
    postfix_name = get_dn_partition_key_postfix_name(hash_func_name);
    if (postfix_name)
        appendStringInfo(&buffer, "::%s", postfix_name);
    appendStringInfo(&buffer, ")) ");
    /*
     * Add any reloptions (storage parameters) defined on the table in a WITH
     * clause.
     */
    {
        char *reloptions = flatten_reloptions(relationId);
        if (reloptions)
        {
            appendStringInfo(&buffer, " WITH (%s", reloptions);
            pfree(reloptions);
			if (with_oids)
				appendStringInfo(&buffer, " ,OIDS)");
			else
				appendStringInfo(&buffer, ")");
        }
        else
		{
        	if (with_oids)
				appendStringInfo(&buffer, " WITH OIDS");
		}
    }

    relation_close(relation, NoLock);
    appendStringInfoString(&buffer, ";");
    return (buffer.data);
}

static char* pg_get_indexclusterdef_string(Oid indexRelationId)
{
	StringInfoData buffer = { NULL, 0, 0, 0 };

	HeapTuple indexTuple = SearchSysCache(INDEXRELID, ObjectIdGetDatum(indexRelationId),
										  0, 0, 0);
	if (!HeapTupleIsValid(indexTuple))
	{
		ereport(ERROR, (errmsg("cache lookup failed for index %u", indexRelationId)));
	}

	Form_pg_index indexForm = (Form_pg_index) GETSTRUCT(indexTuple);
	Oid tableRelationId = indexForm->indrelid;

	/* check if the table is clustered on this index */
	if (indexForm->indisclustered)
	{
		char *tableName = generate_relation_name(tableRelationId, NIL);
		char *indexName = get_rel_name(indexRelationId); /* needs to be quoted */

		initStringInfo(&buffer);
		appendStringInfo(&buffer, "ALTER TABLE %s CLUSTER ON %s",
						 tableName, quote_identifier(indexName));
	}

	ReleaseSysCache(indexTuple);

	return (buffer.data);
}

static char *GetTableIndexAndConstraintCommands(Oid relationId)
{
	StringInfoData buffer = { NULL, 0, 0, 0 };
	ScanKeyData scanKey[1];
	int scanKeyCount = 1;

	initStringInfo(&buffer);
	
	/*
	 * Set search_path to NIL so that all objects outside of pg_catalog will be
	 * schema-prefixed. pg_catalog will be added automatically when we call
	 * PushOverrideSearchPath(), since we set addCatalog to true;
	 */
	OverrideSearchPath *overridePath = GetOverrideSearchPath(CurrentMemoryContext);
	overridePath->schemas = NIL;
	overridePath->addCatalog = true;
	PushOverrideSearchPath(overridePath);

	/* open system catalog and scan all indexes that belong to this table */
	Relation pgIndex = heap_open(IndexRelationId, AccessShareLock);

	ScanKeyInit(&scanKey[0], Anum_pg_index_indrelid,
				BTEqualStrategyNumber, F_OIDEQ, relationId);

	SysScanDesc scanDescriptor = systable_beginscan(pgIndex,
													IndexIndrelidIndexId, true, /* indexOK */
													NULL, scanKeyCount, scanKey);

	HeapTuple heapTuple = systable_getnext(scanDescriptor);
	while (HeapTupleIsValid(heapTuple))
	{
		Form_pg_index indexForm = (Form_pg_index) GETSTRUCT(heapTuple);
		Oid indexId = indexForm->indexrelid;
		bool isConstraint = false;
		char *statementDef = NULL;

		/*
		 * A primary key index is always created by a constraint statement.
		 * A unique key index or exclusion index is created by a constraint
		 * if and only if the index has a corresponding constraint entry in pg_depend.
		 * Any other index form is never associated with a constraint.
		 */
		if (indexForm->indisprimary)
		{
			isConstraint = true;
		}
		else if (indexForm->indisunique || indexForm->indisexclusion)
		{
			Oid constraintId = get_index_constraint(indexId);
			isConstraint = OidIsValid(constraintId);
		}
		else
		{
			isConstraint = false;
		}

		/* get the corresponding constraint or index statement */
		if (isConstraint)
		{
			Oid constraintId = get_index_constraint(indexId);
			Assert(constraintId != InvalidOid);

			statementDef = pg_get_constraintdef_command(constraintId);
		}
		else
		{
			statementDef = pg_get_indexdef_string(indexId);
		}

		/* append found constraint or index definition to the list */
		appendStringInfo(&buffer, "%s;", statementDef);

		/* if table is clustered on this index, append definition to the list */
		if (indexForm->indisclustered)
		{
			char *clusteredDef = pg_get_indexclusterdef_string(indexId);
			Assert(clusteredDef != NULL);
			
			appendStringInfo(&buffer, "%s;", clusteredDef);
		}

		heapTuple = systable_getnext(scanDescriptor);
	}

	/* clean up scan and close system catalog */
	systable_endscan(scanDescriptor);
	heap_close(pgIndex, AccessShareLock);

	/* revert back to original search_path */
	PopOverrideSearchPath();

	return buffer.data;
}


static char* GenDNCreatePartitionTableCmd(Oid relationId, char* sql_statement, char* schema_name, char* tbl_name, 
										  char* dist_col_name, char* hash_func_name, bool with_oids)
{
#define MAX_STR_LEN 512
    char str[MAX_STR_LEN] = {0};
    int i = 0;

    InitializeMetadataCache();
    ShardMapCacheData *cache = GetShardMapCache();
    
    MemoryContext oldcontext;
    StringInfoData cmd;
    oldcontext = MemoryContextSwitchTo(TopTransactionContext);
    initStringInfo(&cmd);

    char* table_create_cmd = GetTableCreateCommand(relationId, dist_col_name, hash_func_name, with_oids);
    appendStringInfoString(&cmd, table_create_cmd);
    char* index_cmd = GetTableIndexAndConstraintCommands(relationId);
	appendStringInfoString(&cmd, index_cmd);
	
    /* sql like: create table test_1 partition of test for values from (range1) to (range2);*/
    Assert(ShardCount != 0);
    for (i = 0; i < ShardCount; i++)
    {
        memset(str, 0, MAX_STR_LEN);
        if (i == ShardCount -1)
		{
			snprintf(str, MAX_STR_LEN, "create table %s.%s_%d partition of %s.%s DEFAULT;",
					 schema_name, tbl_name, i+1, schema_name, tbl_name);
		}
        else
		{
			snprintf(str, MAX_STR_LEN, "create table %s.%s_%d partition of %s.%s for values from (%d) to (%d);",
					 schema_name, tbl_name, i+1, schema_name, tbl_name, cache->items[i].shardMinValue, cache->items[i].shardMaxValue);
		}
        
        appendStringInfoString(&cmd, str);
    }
    
    MemoryContextSwitchTo(oldcontext);
    elog(DEBUG1, "GenDNCreatePartitionTableCmd: %s", cmd.data);
    return cmd.data;
}

/*
 * polardbx_execute_on_nodes
 * Execute 'query' on all the nodes in 'nodelist', and returns int64 datum
 * which has the sum of all the results. If multiples nodes are involved, it
 * assumes that the query returns exactly one row with one attribute of type
 * int64. If there is a single node, it just returns the datum as-is without
 * checking the type of the returned value.
 *
 * Note: nodelist should either have all coordinators or all datanodes in it.
 * Mixing both will result an error being thrown
 */
Datum
polardbx_execute_on_nodes(int numnodes, Oid *nodelist, char *query)
{
    int             i;
    int64           total_size = 0;
    int64           size = 0;
    Datum            datum = (Datum) 0;
    bool        isnull = false;

    EState           *estate;
    MemoryContext    oldcontext;
    RemoteQuery       *plan;
    RemoteQueryState   *pstate;
    TupleTableSlot       *result = NULL;
    Var               *dummy;

    /*
     * Make up RemoteQuery plan node
     */
    plan = makeNode(RemoteQuery);
    plan->combine_type = COMBINE_TYPE_NONE;
    plan->exec_nodes = makeNode(ExecNodes);
    plan->exec_type = EXEC_ON_NONE;

    for (i = 0; i < numnodes; i++)
    {
        char ntype = PGXC_NODE_NONE;
        plan->exec_nodes->nodeList = lappend_int(plan->exec_nodes->nodeList,
                                                 PGXCNodeGetNodeId(nodelist[i], &ntype));
        if (ntype == PGXC_NODE_NONE)
            ereport(ERROR,
                    (errcode(ERRCODE_INTERNAL_ERROR),
                            errmsg("Unknown node Oid: %u", nodelist[i])));
        else if (ntype == PGXC_NODE_COORDINATOR)
        {
            if (plan->exec_type == EXEC_ON_DATANODES)
                ereport(ERROR,
                        (errcode(ERRCODE_INTERNAL_ERROR),
                                errmsg("Cannot mix datanodes and coordinators")));
            plan->exec_type = EXEC_ON_COORDS;
        }
        else
        {
            if (plan->exec_type == EXEC_ON_COORDS)
                ereport(ERROR,
                        (errcode(ERRCODE_INTERNAL_ERROR),
                                errmsg("Cannot mix datanodes and coordinators")));
            plan->exec_type = EXEC_ON_DATANODES;
        }

    }
    plan->sql_statement = query;
    plan->force_autocommit = false;
    /*
     * We only need the target entry to determine result data type.
     * So create dummy even if real expression is a function.
     */
    dummy = makeVar(1, 1, INT8OID, 0, InvalidOid, 0);
    plan->scan.plan.targetlist = lappend(plan->scan.plan.targetlist,
                                         makeTargetEntry((Expr *) dummy, 1, NULL, false));
    /* prepare to execute */
    estate = CreateExecutorState();
    oldcontext = MemoryContextSwitchTo(estate->es_query_cxt);
    estate->es_snapshot = GetActiveSnapshot();
    pstate = ExecInitRemoteQuery(plan, estate, 0);
    MemoryContextSwitchTo(oldcontext);

    result = ExecRemoteQuery((PlanState *) pstate);
    while (result != NULL && !TupIsNull(result))
    {
        datum = slot_getattr(result, 1, &isnull);
        result = ExecRemoteQuery((PlanState *) pstate);

        /* For single node, don't assume the type of datum. It can be bool also. */
        if (numnodes == 1)
            continue;
        /* We should not cast a null into an int */
        if (isnull)
        {
            ereport(ERROR,
                    (errcode(ERRCODE_INTERNAL_ERROR),
                            errmsg("Expected Int64 but got null instead "
                                   "while executing query '%s'",
                                   query)));
            break;
        }

        size = DatumGetInt64(datum);
        total_size += size;
    }
    ExecEndRemoteQuery(pstate);


    if (numnodes == 1)
    {
        /* 
         * if result is NULL, so no need to check isnull value, just return 0
         * isnull also needs explict asignment.
         */
        if (isnull)
            ereport(ERROR,
                    (errcode(ERRCODE_INTERNAL_ERROR),
                            errmsg("Expected datum but got null instead "
                                   "while executing query '%s'",
                                   query)));
        PG_RETURN_DATUM(datum);
    }
    else
        PG_RETURN_INT64(total_size);
}

void exec_query_on_nodes(int numnodes, PGXCNodeHandle** conns, char *query)
{
    int i = 0;
    int conn_count = numnodes;
    ResponseCombiner combiner;
    InitResponseCombiner(&combiner, 0, COMBINE_TYPE_NONE);
    for (i = 0; i < numnodes; i++)
    {
        PGXCNodeHandle *conn = conns[i];
        
        if (conn->state == DN_CONNECTION_STATE_QUERY)
            BufferConnection(conn);

        if (pgxc_node_send_query(conn, query) != 0)
        {
            ereport(ERROR,
                    (errcode(ERRCODE_INTERNAL_ERROR),
                            errmsg("Failed to send command to node")));
        }
    }

    /*
     * Stop if all commands are completed or we got a data row and
     * initialized state node for subsequent invocations
     */
    while (conn_count > 0)
    {
        i = 0;

        if (pgxc_node_receive(conn_count, conns, NULL))
            break;
        
        while (i < conn_count)
        {
            PGXCNodeHandle *conn = conns[i];
            int res = handle_response(conn, &combiner);
            if (res == RESPONSE_EOF)
            {
                i++;
            }
            else if (res == RESPONSE_COMPLETE)
            {
                /* Ignore, wait for ReadyForQuery */
                if (conn->state == DN_CONNECTION_STATE_ERROR_FATAL)
                {
                    ereport(ERROR,
                            (errcode(ERRCODE_INTERNAL_ERROR),
                                    errmsg("Unexpected FATAL ERROR on Connection to node %s pid %d",
                                           conn->nodename, conn->backend_pid)));
                }
            }
            else if (res == RESPONSE_ERROR)
            {
                /* Ignore, wait for ReadyForQuery */
            }
            else if (res == RESPONSE_READY)
            {
                if (i < --conn_count)
                    conns[i] = conns[conn_count];
            }
            else if (res == RESPONSE_TUPDESC)
            {
                ereport(ERROR,
                        (errcode(ERRCODE_INTERNAL_ERROR),
                                errmsg("Unexpected response from Datanode")));
            }
            else if (res == RESPONSE_DATAROW)
            {
                ereport(ERROR,
                        (errcode(ERRCODE_INTERNAL_ERROR),
                                errmsg("Unexpected response from Datanode")));
            }
        }
    }
    
    pgxc_node_report_error(&combiner);
}

static const char* get_dn_partition_key_postfix_name(char* hashfuncname)
{ // #lizard forgives
    if (strcmp(hashfuncname, "hashint4") == 0)
    {
        return "integer";
    } 
    else if (strcmp(hashfuncname, "hashint8") == 0)
    {
        return "bigint";
    }

    return NULL;
}
