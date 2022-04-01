/*-------------------------------------------------------------------------
 * polarx_execdirect.c
 *   The implementation of the EXECUTE polarx_direct command in polarx
 *
 * Copyright (c) 2021, Alibaba Group Holding Limited
 * Licensed under the Apache License, Version 2.0 (the "License");
 *
 * IDENTIFICATION
 *        contrib/polarx/commands/polarx_execdirect.c
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "polarx.h"
#include "commands/polarx_execdirect.h"
#include "access/reloptions.h"
#include "access/htup_details.h"
#include "access/sysattr.h"
#include "access/xact.h"
#include "access/xlog.h"
#include "catalog/namespace.h"
#include "catalog/toasting.h"
#include "commands/createas.h"
#include "commands/matview.h"
#include "commands/prepare.h"
#include "commands/tablecmds.h"
#include "commands/view.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "parser/parse_clause.h"
#include "rewrite/rewriteHandler.h"
#include "storage/smgr.h"
#include "tcop/tcopprot.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/rls.h"
#include "utils/snapmgr.h"
#include "commands/defrem.h"
#include "deparse/deparse_fqs.h"
#include "utils/portal.h"
#include "tcop/pquery.h"
#include "executor/execRemoteQuery.h"
#include "plan/polarx_planner.h"
#include "nodes/polarx_node.h"

static void ExecDirect(ExecuteStmt *stmt, DestReceiver *dest, char *completionTag);
static PlannedStmt *polarx_direct_planner(Query *query);
static Query *transformExecDirect(ExecuteStmt *stmt, char **query_string);

void
ExecDirectPre(ExecuteStmt *stmt, DestReceiver *dest, char *completionTag, bool *all_done)
{
    if(stmt->params != NIL && list_length(stmt->params) == 2
            && strncmp(stmt->name, "polarx_direct", 13) == 0)
    {
        ExecDirect(stmt, dest, completionTag);
        if(all_done)
            *all_done = true;
    }
}

static void
ExecDirect(ExecuteStmt *stmt, DestReceiver *dest, char *completionTag)
{
    List       *plan_list = NIL;
    Portal      portal;
    char       *query_string;
    Query       *query;
    PlannedStmt *pstmt;
    char        *remote_query_str = NULL;


    query = transformExecDirect(stmt, &remote_query_str);
    /* Create a new portal to run the query in */
    portal = CreateNewPortal();
    /* Don't display the portal in pg_cursors, it is for internal use only */
    portal->visible = false;

    if(remote_query_str && *remote_query_str != '\0')
        query_string = MemoryContextStrdup(portal->portalContext,
                remote_query_str);
    else
        elog(ERROR, "execute polarx_direct param sql is NULL");

    if(query->utilityStmt)
    {
        pstmt = polarx_direct_planner(query);
        plan_list = lappend(plan_list, pstmt);
    }
    else
    {
        List *query_list = NIL; 

        query_list = lappend(query_list, query);
        plan_list = pg_plan_queries(query_list, CURSOR_OPT_PARALLEL_OK, NULL);
    }

    PortalDefineQuery(portal,
            NULL,
            query_string,
            "Execute polarx_direct",
            plan_list,
            NULL);

    /*
     * Run the portal as appropriate.
     */
    PortalStart(portal, NULL, 0, GetActiveSnapshot());

    (void) PortalRun(portal, FETCH_ALL, false, true, dest, dest, completionTag);

    PortalDrop(portal, false);
}

static PlannedStmt *
polarx_direct_planner(Query *query)
{
    PlannedStmt *result;
    RemoteQuery *query_step = NULL;
    List *tlist = query->targetList;
    CustomScan *customScan = makeNode(CustomScan);

    result = makeNode(PlannedStmt);

    result->commandType = query->commandType;
    result->canSetTag = query->canSetTag;
    result->utilityStmt = query->utilityStmt;
    result->rtable = query->rtable;


    /* EXECUTE DIRECT statements have their RemoteQuery node already built when analyzing */
    if (query->utilityStmt
            && polarxIsA(query->utilityStmt, RemoteQuery))
    {
        RemoteQuery *stmt = (RemoteQuery *) query->utilityStmt;
        if (stmt->exec_direct_type != EXEC_DIRECT_NONE)
        {
            query_step = stmt;
            query->utilityStmt = NULL;
        }
    }
    Assert(query_step);

    query_step->scan.plan.targetlist = tlist;
    query_step->read_only = query->commandType == CMD_SELECT;
    customScan->methods = &FastShipQueryExecutorCustomScanMethods;
    customScan->custom_private = list_make1((Node *)query_step);
    customScan->custom_scan_tlist = tlist;
    customScan->scan.plan.targetlist = makeCustomScanVarTargetlistBasedTargetEntryList(tlist);

    result->planTree = (Plan *) customScan;

    return result;
}
static Query *
transformExecDirect(ExecuteStmt *stmt, char **query_string)
{// #lizard forgives
    Query        *result = makeNode(Query);
    List        *params = stmt->params;
    RemoteQuery    *step = polarxMakeNode(RemoteQuery);
    bool        is_local = false;
    List        *raw_parsetree_list;
    ListCell    *raw_parsetree_item;
    char        *nodename = NULL;
    int            nodeIndex;
    char        nodetype;
    ListCell   *l;
    int         i = 0;
    char        *query = NULL;

    /* Support not available on Datanodes */
    if (IS_PGXC_DATANODE)
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                 errmsg("EXECUTE polarx_direct cannot be executed on a Datanode")));

    Assert(list_length(params) == 2);
    Assert(IS_PGXC_COORDINATOR);

    foreach(l, params)
    {
        Node       *expr = lfirst(l);

        if(IsA(expr, A_Const) && IsA(&(((A_Const *)expr)->val), String))
        {
            if(i == 0)
                nodename = strVal(&(((A_Const *)expr)->val));
            else if(i == 1)
                query = strVal(&(((A_Const *)expr)->val));
        }
        else
            ereport(ERROR,
                    (errcode(ERRCODE_DATATYPE_MISMATCH),
                     errmsg("EXECUTE polarx_direct params is not expected type")));
        i++;
    }

    /* There is a single element here */
    nodetype = PGXC_NODE_NONE;
    if(nodename)
        nodeIndex = PGXCNodeGetNodeIdFromName(nodename, &nodetype);
    if (nodetype == PGXC_NODE_NONE)
        ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_OBJECT),
                 errmsg("Polarx Node %s: object not defined",
                     nodename)));

    if(query_string)
        *query_string = query;
    /* Check if node is requested is the self-node or not */
    if (nodetype == PGXC_NODE_COORDINATOR && nodeIndex == PGXCNodeId - 1)
        is_local = true;

    /* Transform the query into a raw parse list */
    raw_parsetree_list = pg_parse_query(query);

    /* EXECUTE polarx_direct can just be executed with a single query */
    if (list_length(raw_parsetree_list) > 1)
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                 errmsg("EXECUTE polarx_direct cannot execute multiple queries")));

    /*
     * Analyze the Raw parse tree
     * EXECUTE polarx_direct is restricted to one-step usage
     */
    foreach(raw_parsetree_item, raw_parsetree_list)
    {
        RawStmt   *parsetree = lfirst_node(RawStmt, raw_parsetree_item);
        List *result_list = pg_analyze_and_rewrite(parsetree, query, NULL, 0, NULL);
        result = linitial_node(Query, result_list);
    }

    /* Default list of parameters to set */
    step->sql_statement = NULL;
    step->exec_nodes = polarxMakeNode(ExecNodes);
    step->combine_type = COMBINE_TYPE_NONE;
    step->sort = NULL;
    step->read_only = true;
    step->force_autocommit = false;
    step->cursor = NULL;

    /* This is needed by executor */
    step->sql_statement = pstrdup(query);
    if (nodetype == PGXC_NODE_COORDINATOR)
        step->exec_type = EXEC_ON_COORDS;
    else
        step->exec_type = EXEC_ON_DATANODES;

    step->reduce_level = 0;
    step->base_tlist = NIL;
    step->outer_alias = NULL;
    step->inner_alias = NULL;
    step->outer_reduce_level = 0;
    step->inner_reduce_level = 0;
    step->outer_relids = NULL;
    step->inner_relids = NULL;
    step->inner_statement = NULL;
    step->outer_statement = NULL;
    step->join_condition = NULL;

    /* Change the list of nodes that will be executed for the query and others */
    step->force_autocommit = false;
    step->combine_type = COMBINE_TYPE_SAME;
    step->read_only = true;
    step->exec_direct_type = EXEC_DIRECT_NONE;

    /* Set up EXECUTE DIRECT flag */
    if (is_local)
    {
        if (result->commandType == CMD_UTILITY)
            step->exec_direct_type = EXEC_DIRECT_LOCAL_UTILITY;
        else
            step->exec_direct_type = EXEC_DIRECT_LOCAL;
    }
    else
    {
        switch(result->commandType)
        {
            case CMD_UTILITY:
                step->exec_direct_type = EXEC_DIRECT_UTILITY;
                break;
            case CMD_SELECT:
                step->exec_direct_type = EXEC_DIRECT_SELECT;
                break;
            case CMD_INSERT:
                step->exec_direct_type = EXEC_DIRECT_INSERT;
                break;
            case CMD_UPDATE:
                step->exec_direct_type = EXEC_DIRECT_UPDATE;
                break;
            case CMD_DELETE:
                step->exec_direct_type = EXEC_DIRECT_DELETE;
                break;
            default:
                Assert(0);
        }
    }

    /* Build Execute Node list, there is a unique node for the time being */
    step->exec_nodes->nodeList = lappend_int(step->exec_nodes->nodeList, nodeIndex);

    if (!is_local)
        result->utilityStmt = (Node *) step;

    /*
     * Reset the queryId since the caller would do that anyways.
     */
    result->queryId = 0;

    return result;
}

