/*-------------------------------------------------------------------------
 *
 * execRemoteQuery.c
 *
 *      Functions to execute commands for fast ship query in polarx
 *
 *
 * Portions Copyright (c) 2012-2014, TransLattice, Inc.
 * Portions Copyright (c) 1996-2011, PostgreSQL Global Development Group
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 * Copyright (c) 2020, Apache License Version 2.0*
 *
 * IDENTIFICATION
 *        contrib/polarx/executor/execRemoteQuery.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

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
#include "pgxc/pgxc.h"
#include "pgxc/pgxcnode.h"
#include "executor/recvRemote.h"
#include "pgxc/transam/txn_coordinator.h"
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
#include "gtm/gtm_c.h"
#include "utils/ruleutils.h"
#include "commands/explain.h"
#include "nodes/makefuncs.h"

static PGXCNodeAllHandles *get_exec_connections(RemoteQueryState *planstate,
                                                ExecNodes *exec_nodes,
                                                RemoteQueryExecType exec_type,
                                                bool is_global_session);
static bool pgxc_start_command_on_connection(PGXCNodeHandle *connection,
                                             RemoteQueryState *remotestate,
                                             Snapshot snapshot);
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
                                             (PlanState *)planstate);
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
                                 Snapshot snapshot)
{ // #lizard forgives
    CommandId cid;
    ResponseCombiner *combiner = (ResponseCombiner *)remotestate;
    RemoteQuery *step = (RemoteQuery *)combiner->ss.ps.plan;
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
        bool prepared = false;
        char nodetype = PGXC_NODE_DATANODE;
        bool send_desc = false;

        if (step->base_tlist != NULL ||
            step->exec_nodes->accesstype == RELATION_ACCESS_READ ||
            step->read_only ||
            step->has_row_marks)
            send_desc = true;

        /* if prepared statement is referenced see if it is already
         * exist */

        if (step->statement)
            prepared =
                ActivateDatanodeStatementOnNode(step->statement,
                                                PGXCNodeGetNodeId(connection->nodeoid,
                                                                  &nodetype));

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

        if (pgxc_node_send_query_extended(connection,
                                          step->sql_statement,
                                          NULL,
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
    fsp_state->remoteQueryState = makeNode(RemoteQueryState);

    return (Node *)fsp_state;
}
static RemoteQueryState *
ExecInitRemoteQuery(RemoteQuery *node, EState *estate, int eflags)
{
    RemoteQueryState *remotestate;
    ResponseCombiner *combiner;
    TupleDesc scan_type;

    remotestate = makeNode(RemoteQueryState);
    combiner = (ResponseCombiner *)remotestate;
    InitResponseCombiner(combiner, 0, node->combine_type);
    combiner->ss.ps.plan = (Plan *)node;
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
    ResponseCombiner *combiner = (ResponseCombiner *)node;
    RemoteQuery *step = (RemoteQuery *)combiner->ss.ps.plan;
    TupleTableSlot *slot;

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

        for (i = 0; i < regular_conn_count; i++)
        {
            BeginTxnIfNecessary(connections[i]);
        }

        for (i = 0; i < regular_conn_count; i++)
        {
            //connections[i]->read_only = true;
            connections[i]->recv_datarows = 0;
            GlobalTransactionId gxid = InvalidGlobalTransactionId;
            if (pgxc_node_begin(1, &connections[i], gxid, need_tran_block,
                                step->read_only, PGXC_NODE_DATANODE))
                ereport(ERROR,
                        (errcode(ERRCODE_INTERNAL_ERROR),
                         errmsg("Could not begin transaction on data node:%s.", connections[i]->nodename)));
            /* If explicit transaction is needed gxid is already sent */
            if (!pgxc_start_command_on_connection(connections[i], node, snapshot))
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

    slot = FetchTuple(combiner);
    if (!TupIsNull(slot))
        return slot;

    if (combiner->errorMessage)
        pgxc_node_report_error(combiner);

    return NULL;
}

static TupleTableSlot *
ExecRemoteQuery(PlanState *pstate)
{
    RemoteQueryState *node = castNode(RemoteQueryState, pstate);
    ResponseCombiner *combiner = (ResponseCombiner *)node;
    /*
     *      * As there may be unshippable qual which we need to evaluate on CN,
     *           * We should call ExecScan to handle such cases.
     *                */

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
    return ExecRemoteQuery((PlanState *)fsp_state->remoteQueryState);
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
    ResponseCombiner *combiner = (ResponseCombiner *)node;

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
    if (outerPlanState(node))
        ExecReScan(outerPlanState(node));

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
    ResponseCombiner *combiner = (ResponseCombiner *)node;

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
                success_nodes = makeNode(ExecNodes);
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
    GlobalTransactionId gxid = InvalidGlobalTransactionId;
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

    remotestate = makeNode(RemoteQueryState);
    combiner = (ResponseCombiner *)remotestate;
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
                            gxid, need_tran_block, g_in_set_config_option ? true : false, PGXC_NODE_DATANODE))
            ereport(ERROR,
                    (errcode(ERRCODE_INTERNAL_ERROR),
                     errmsg("Could not begin transaction on Datanodes")));
        for (i = 0; i < dn_conn_count; i++)
        {
            PGXCNodeHandle *conn = pgxc_connections->datanode_handles[i];

            if (conn->state == DN_CONNECTION_STATE_QUERY)
                BufferConnection(conn);
            if (pgxc_node_send_query(conn, node->sql_statement) != 0)
            {
                ereport(ERROR,
                        (errcode(ERRCODE_INTERNAL_ERROR),
                         errmsg("Failed to send command to Datanodes")));
            }
        }
    }
    {
        if (pgxc_node_begin(co_conn_count, pgxc_connections->coord_handles,
                            gxid, need_tran_block, g_in_set_config_option ? true : false, PGXC_NODE_COORDINATOR))
            ereport(ERROR,
                    (errcode(ERRCODE_INTERNAL_ERROR),
                     errmsg("Could not begin transaction on coordinators")));
        /* Now send it to Coordinators if necessary */
        for (i = 0; i < co_conn_count; i++)
        {
            if (pgxc_node_send_query(pgxc_connections->coord_handles[i], node->sql_statement) != 0)
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
    PlanState *planstate = (PlanState *)fsp_state->remoteQueryState;
    RemoteQuery *plan = (RemoteQuery *)planstate->plan;
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
        RemoteQuery *step = makeNode(RemoteQuery);
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
        result = ExecRemoteQuery((PlanState *)node);
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
            result = ExecRemoteQuery((PlanState *)node);
        }
        ExecEndRemoteQuery(node);

        if (es->format == EXPLAIN_FORMAT_TEXT)
            appendStringInfo(es->str, "%s", explainResult.data);
        else
            ExplainPropertyText("Remote plan", explainResult.data, es);
    }
}
