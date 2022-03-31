/*-------------------------------------------------------------------------
 *
 * polarx_init.c
 *		  Foreign-data wrapper for poalrx distributed cluster
 *
 * Copyright (c) 2021, Alibaba Group Holding Limited
 * Licensed under the Apache License, Version 2.0 (the "License");
 * Portions Copyright (c) 2012-2018, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		  contrib/polarx/polarx_init.c
 *
 *-------------------------------------------------------------------------
 */
#include <math.h>
#include "postgres.h"
#include "polarx.h"

#include "polarx/polarx_fdw.h"

#include "pgxc/connpool.h"
#include "miscadmin.h"
#include "optimizer/planner.h"
#include "executor/execRemoteQuery.h"
#include "plan/polarx_planner.h"
#include "utils/fdwplanner_utils.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"
#include "commands/polarx_utility.h"
#include "catalog/objectaccess.h"
#include "catalog/dependency.h"
#include "catalog/pg_foreign_table.h"
#include "catalog/indexing.h"
#include "catalog/pg_class.h"
#include "nodes/polarx_nodesdef.h"
#include "distribute_transaction/txn.h"
#include "pgxc/nodemgr.h"
#include "pgxc/connpool.h"

PG_MODULE_MAGIC;

void    _PG_init(void);

char    *PGXCNodeName = NULL;
char    *PGXCClusterName = NULL;
char    *PGXCMainClusterName = NULL;
bool    IsPGXCMainCluster = false;
int     PGXCNodeId = 0;
bool    isPGXCCoordinator = false;
bool    isPGXCDataNode = false;
int     remoteConnType = REMOTE_CONN_APP;

static const struct config_enum_entry pgxc_conn_types[] = {
    {"application", REMOTE_CONN_APP, false},
    {"coordinator", REMOTE_CONN_COORD, false},
    {"datanode", REMOTE_CONN_DATANODE, false},
    {"gtm", REMOTE_CONN_GTM, false},
    {"gtmproxy", REMOTE_CONN_GTM_PROXY, false},
    {NULL, 0, false}
};
static object_access_hook_type next_object_access_hook = NULL;
/*
 * SQL functions
 */
PG_FUNCTION_INFO_V1(polarx_fdw_handler);
PG_FUNCTION_INFO_V1(self_node_inx);
PG_FUNCTION_INFO_V1(clean_db_connections);
PG_FUNCTION_INFO_V1(pgxc_node_str);

/*
 * FDW callback routines
 */
static void routerGetForeignRelSize(PlannerInfo *root,
						  RelOptInfo *baserel,
						  Oid foreigntableid);
static void routerGetForeignPaths(PlannerInfo *root,
						RelOptInfo *baserel,
						Oid foreigntableid);
static ForeignScan *routerGetForeignPlan(PlannerInfo *root,
					   RelOptInfo *foreignrel,
					   Oid foreigntableid,
					   ForeignPath *best_path,
					   List *tlist,
					   List *scan_clauses,
					   Plan *outer_plan);
static void routerBeginForeignScan(ForeignScanState *node, int eflags);
static TupleTableSlot *routerIterateForeignScan(ForeignScanState *node);
static void routerReScanForeignScan(ForeignScanState *node);
static void routerEndForeignScan(ForeignScanState *node);
static void routerAddForeignUpdateTargets(Query *parsetree,
								RangeTblEntry *target_rte,
								Relation target_relation);
static List *routerPlanForeignModify(PlannerInfo *root,
						  ModifyTable *plan,
						  Index resultRelation,
						  int subplan_index);
static void routerBeginForeignModify(ModifyTableState *mtstate,
						   ResultRelInfo *resultRelInfo,
						   List *fdw_private,
						   int subplan_index,
						   int eflags);
static TupleTableSlot *routerExecForeignInsert(EState *estate,
						  ResultRelInfo *resultRelInfo,
						  TupleTableSlot *slot,
						  TupleTableSlot *planSlot);
static TupleTableSlot *routerExecForeignUpdate(EState *estate,
						  ResultRelInfo *resultRelInfo,
						  TupleTableSlot *slot,
						  TupleTableSlot *planSlot);
static TupleTableSlot *routerExecForeignDelete(EState *estate,
						  ResultRelInfo *resultRelInfo,
						  TupleTableSlot *slot,
						  TupleTableSlot *planSlot);
static void routerEndForeignModify(EState *estate,
						 ResultRelInfo *resultRelInfo);
static void routerBeginForeignInsert(ModifyTableState *mtstate,
						   ResultRelInfo *resultRelInfo);
static void routerEndForeignInsert(EState *estate,
						 ResultRelInfo *resultRelInfo);
static int	routerIsForeignRelUpdatable(Relation rel);
static bool routerPlanDirectModify(PlannerInfo *root,
						 ModifyTable *plan,
						 Index resultRelation,
						 int subplan_index);
static void routerBeginDirectModify(ForeignScanState *node, int eflags);
static TupleTableSlot *routerIterateDirectModify(ForeignScanState *node);
static void routerEndDirectModify(ForeignScanState *node);
static void routerExplainForeignScan(ForeignScanState *node,
						   ExplainState *es);
static void routerExplainForeignModify(ModifyTableState *mtstate,
							 ResultRelInfo *rinfo,
							 List *fdw_private,
							 int subplan_index,
							 ExplainState *es);
static void routerExplainDirectModify(ForeignScanState *node,
							ExplainState *es);
static bool routerAnalyzeForeignTable(Relation relation,
							AcquireSampleRowsFunc *func,
							BlockNumber *totalpages);
static void routerGetForeignJoinPaths(PlannerInfo *root,
							RelOptInfo *joinrel,
							RelOptInfo *outerrel,
							RelOptInfo *innerrel,
							JoinType jointype,
							JoinPathExtraData *extra);
static bool routerRecheckForeignScan(ForeignScanState *node,
						   TupleTableSlot *slot);
static void routerGetForeignUpperPaths(PlannerInfo *root,
							 UpperRelationKind stage,
							 RelOptInfo *input_rel,
							 RelOptInfo *output_rel,
							 void *extra);

static void polarx_object_access(ObjectAccessType access, Oid classId, Oid objectId, int subId, void *arg);
static void polarxExecutorStart(QueryDesc *queryDesc, int eflags);
static void polarxExplainOneQuery(Query *query, int cursorOptions,
                                    IntoClause *into, ExplainState *es,
                                    const char *queryString, ParamListInfo params,
                                    QueryEnvironment *queryEnv);
static void RegisterPolarxConfigVariables(void);

Datum
self_node_inx(PG_FUNCTION_ARGS)
{
	PG_RETURN_INT32(PGXCNodeId - 1);
}

Datum
clean_db_connections(PG_FUNCTION_ARGS)
{
    text *dbname_text = PG_GETARG_TEXT_P(0);
    char *dbname = text_to_cstring(dbname_text);

    DropDBCleanConnection(dbname);
    PG_RETURN_VOID();
}
/*
 * pgxc_node_str
 *
 * get the name of the node
 */
Datum
pgxc_node_str(PG_FUNCTION_ARGS)
{
    PG_RETURN_TEXT_P(cstring_to_text(PGXCNodeName));
}

void
_PG_init(void)
{

    if (!process_shared_preload_libraries_in_progress)
    {
        ereport(ERROR, (errmsg("polarx can only be loaded via shared_preload_libraries"),
                    errhint("Add polarx to shared_preload_libraries configuration "
                        "variable in postgresql.conf in master and workers. Note "
                        "that polarx should be at the beginning of "
                        "shared_preload_libraries.")));
    }
    if (planner_hook != NULL || ProcessUtility_hook != NULL
            || ExecutorStart_hook != NULL || ExplainOneQuery_hook != NULL)
    {
        ereport(ERROR, (errmsg("polarx has to be loaded first"),
                    errhint("Place polarx at the beginning of "
                        "shared_preload_libraries.")));
    }

    RegisterPolarxConfigVariables();
    RegisterPolarxFastShipQueryMethods();
    RegisterPolarxNodes();
    planner_hook = polarx_planner;
    ProcessUtility_hook = polarx_ProcessUtility;
    next_object_access_hook = object_access_hook;
    object_access_hook = polarx_object_access;
    ExecutorStart_hook = polarxExecutorStart;
    ExplainOneQuery_hook = polarxExplainOneQuery;
    InitializeNodeTablesShmemStruct();
    InitializePoolerShmemStruct();
    
    RegisterDistributeTxnCallback();
}
/*
 * Foreign-data wrapper handler function: return a struct with pointers
 * to my callback routines.
 */
Datum
polarx_fdw_handler(PG_FUNCTION_ARGS)
{
	FdwRoutine *routine = makeNode(FdwRoutine);

	/* Functions for scanning foreign tables */
	routine->GetForeignRelSize = routerGetForeignRelSize;
	routine->GetForeignPaths = routerGetForeignPaths;
	routine->GetForeignPlan = routerGetForeignPlan;
	routine->BeginForeignScan = routerBeginForeignScan;
	routine->IterateForeignScan = routerIterateForeignScan;
	routine->ReScanForeignScan = routerReScanForeignScan;
	routine->EndForeignScan = routerEndForeignScan;

	/* Functions for updating foreign tables */
	routine->AddForeignUpdateTargets = routerAddForeignUpdateTargets;
	routine->PlanForeignModify = routerPlanForeignModify;
	routine->BeginForeignModify = routerBeginForeignModify;
	routine->ExecForeignInsert = routerExecForeignInsert;
	routine->ExecForeignUpdate = routerExecForeignUpdate;
	routine->ExecForeignDelete = routerExecForeignDelete;
	routine->EndForeignModify = routerEndForeignModify;
	routine->BeginForeignInsert = routerBeginForeignInsert;
	routine->EndForeignInsert = routerEndForeignInsert;
	routine->IsForeignRelUpdatable = routerIsForeignRelUpdatable;
	routine->PlanDirectModify = routerPlanDirectModify;
	routine->BeginDirectModify = routerBeginDirectModify;
	routine->IterateDirectModify = routerIterateDirectModify;
	routine->EndDirectModify = routerEndDirectModify;

	/* Function for EvalPlanQual rechecks */
	routine->RecheckForeignScan = routerRecheckForeignScan;
	/* Support functions for EXPLAIN */
	routine->ExplainForeignScan = routerExplainForeignScan;
	routine->ExplainForeignModify = routerExplainForeignModify;
	routine->ExplainDirectModify = routerExplainDirectModify;

	/* Support functions for ANALYZE */
	routine->AnalyzeForeignTable = routerAnalyzeForeignTable;

	/* Support functions for IMPORT FOREIGN SCHEMA */
	routine->ImportForeignSchema = NULL;

	/* Support functions for join push-down */
	routine->GetForeignJoinPaths = routerGetForeignJoinPaths;

	/* Support functions for upper relation push-down */
	routine->GetForeignUpperPaths = routerGetForeignUpperPaths;

	PG_RETURN_POINTER(routine);
}

/*
 * routerGetForeignRelSize
 *		Estimate # of rows and width of the result of the scan
 *
 * We should consider the effect of all baserestrictinfo clauses here, but
 * not any join clauses.
 */
static void
routerGetForeignRelSize(PlannerInfo *root,
						  RelOptInfo *baserel,
						  Oid foreigntableid)
{
    polarxGetForeignRelSize(root, baserel, foreigntableid);
}

/*
 * routerGetForeignPaths
 *		Create possible scan paths for a scan on the foreign table
 */
static void
routerGetForeignPaths(PlannerInfo *root,
						RelOptInfo *baserel,
						Oid foreigntableid)
{
    polarxGetForeignPaths(root, baserel, foreigntableid);
}

/*
 * routerGetForeignPlan
 *		Create ForeignScan plan node which implements selected best path
 */
static ForeignScan *
routerGetForeignPlan(PlannerInfo *root,
					   RelOptInfo *foreignrel,
					   Oid foreigntableid,
					   ForeignPath *best_path,
					   List *tlist,
					   List *scan_clauses,
					   Plan *outer_plan)
{
    return polarxGetForeignPlan(root, foreignrel, foreigntableid, best_path,
			        		   tlist, scan_clauses, outer_plan);
}

/*
 * routerBeginForeignScan
 *		Initiate an executor scan of a foreign PostgreSQL table.
 */
static void
routerBeginForeignScan(ForeignScanState *node, int eflags)
{
    polarxBeginForeignScan(node, eflags);
}

/*
 * routerIterateForeignScan
 *		Retrieve next row from the result set, or clear tuple slot to indicate
 *		EOF.
 */
static TupleTableSlot *
routerIterateForeignScan(ForeignScanState *node)
{
    return polarxIterateForeignScan(node);
}

/*
 * routerReScanForeignScan
 *		Restart the scan.
 */
static void
routerReScanForeignScan(ForeignScanState *node)
{
    polarxReScanForeignScan(node);
}

/*
 * routerEndForeignScan
 *		Finish scanning foreign table and dispose objects used for this scan
 */
static void
routerEndForeignScan(ForeignScanState *node)
{
    polarxEndForeignScan(node);
}

/*
 * routerAddForeignUpdateTargets
 *		Add resjunk column(s) needed for update/delete on a foreign table
 */
static void
routerAddForeignUpdateTargets(Query *parsetree,
								RangeTblEntry *target_rte,
								Relation target_relation)
{
    polarxAddForeignUpdateTargets(parsetree, target_rte, target_relation);
}

/*
 * routerPlanForeignModify
 *		Plan an insert/update/delete operation on a foreign table
 */
static List *
routerPlanForeignModify(PlannerInfo *root,
						  ModifyTable *plan,
						  Index resultRelation,
						  int subplan_index)
{
    return polarxPlanForeignModify(root, plan, resultRelation, subplan_index);
}

/*
 * routerBeginForeignModify
 *		Begin an insert/update/delete operation on a foreign table
 */
static void
routerBeginForeignModify(ModifyTableState *mtstate,
						   ResultRelInfo *resultRelInfo,
						   List *fdw_private,
						   int subplan_index,
						   int eflags)
{
    polarxBeginForeignModify(mtstate, resultRelInfo, fdw_private, subplan_index,
						    eflags);
}

/*
 * routerExecForeignInsert
 *		Insert one row into a foreign table
 */
static TupleTableSlot *
routerExecForeignInsert(EState *estate,
						  ResultRelInfo *resultRelInfo,
						  TupleTableSlot *slot,
						  TupleTableSlot *planSlot)
{
    return polarxExecForeignInsert(estate, resultRelInfo, slot, planSlot);
}

/*
 * routerExecForeignUpdate
 *		Update one row in a foreign table
 */
static TupleTableSlot *
routerExecForeignUpdate(EState *estate,
						  ResultRelInfo *resultRelInfo,
						  TupleTableSlot *slot,
						  TupleTableSlot *planSlot)
{
    return polarxExecForeignUpdate(estate, resultRelInfo, slot, planSlot);
}

/*
 * routerExecForeignDelete
 *		Delete one row from a foreign table
 */
static TupleTableSlot *
routerExecForeignDelete(EState *estate,
						  ResultRelInfo *resultRelInfo,
						  TupleTableSlot *slot,
						  TupleTableSlot *planSlot)
{
    return polarxExecForeignDelete(estate, resultRelInfo, slot, planSlot);
}

/*
 * routerEndForeignModify
 *		Finish an insert/update/delete operation on a foreign table
 */
static void
routerEndForeignModify(EState *estate,
						 ResultRelInfo *resultRelInfo)
{
    polarxEndForeignModify(estate, resultRelInfo);
}

/*
 * routerBeginForeignInsert
 *		Begin an insert operation on a foreign table
 */
static void
routerBeginForeignInsert(ModifyTableState *mtstate,
						   ResultRelInfo *resultRelInfo)
{
    polarxBeginForeignInsert(mtstate, resultRelInfo);
}

/*
 * routerEndForeignInsert
 *		Finish an insert operation on a foreign table
 */
static void
routerEndForeignInsert(EState *estate,
						 ResultRelInfo *resultRelInfo)
{
    polarxEndForeignInsert(estate, resultRelInfo);
}

/*
 * routerIsForeignRelUpdatable
 *		Determine whether a foreign table supports INSERT, UPDATE and/or
 *		DELETE.
 */
static int
routerIsForeignRelUpdatable(Relation rel)
{
    return polarxIsForeignRelUpdatable(rel);
}

/*
 * routerRecheckForeignScan
 *		Execute a local join execution plan for a foreign join
 */
static bool
routerRecheckForeignScan(ForeignScanState *node, TupleTableSlot *slot)
{
    return polarxRecheckForeignScan(node, slot);
}

/*
 * routerPlanDirectModify
 *		Consider a direct foreign table modification
 *
 * Decide whether it is safe to modify a foreign table directly, and if so,
 * rewrite subplan accordingly.
 */
static bool
routerPlanDirectModify(PlannerInfo *root,
						 ModifyTable *plan,
						 Index resultRelation,
						 int subplan_index)
{
    return polarxPlanDirectModify(root, plan, resultRelation, subplan_index);
}

/*
 * routerBeginDirectModify
 *		Prepare a direct foreign table modification
 */
static void
routerBeginDirectModify(ForeignScanState *node, int eflags)
{
    polarxBeginDirectModify(node, eflags);
}

/*
 * routerIterateDirectModify
 *		Execute a direct foreign table modification
 */
static TupleTableSlot *
routerIterateDirectModify(ForeignScanState *node)
{
    return polarxIterateDirectModify(node);
}

/*
 * routerEndDirectModify
 *		Finish a direct foreign table modification
 */
static void
routerEndDirectModify(ForeignScanState *node)
{
    polarxEndDirectModify(node);
}

/*
 * routerExplainForeignScan
 *		Produce extra output for EXPLAIN of a ForeignScan on a foreign table
 */
static void
routerExplainForeignScan(ForeignScanState *node, ExplainState *es)
{
    polarxExplainForeignScan(node, es);
}

/*
 * routerExplainForeignModify
 *		Produce extra output for EXPLAIN of a ModifyTable on a foreign table
 */
static void
routerExplainForeignModify(ModifyTableState *mtstate,
							 ResultRelInfo *rinfo,
							 List *fdw_private,
							 int subplan_index,
							 ExplainState *es)
{
    polarxExplainForeignModify(mtstate, rinfo, fdw_private, subplan_index, es);
}

/*
 * routerExplainDirectModify
 *		Produce extra output for EXPLAIN of a ForeignScan that modifies a
 *		foreign table directly
 */
static void
routerExplainDirectModify(ForeignScanState *node, ExplainState *es)
{
    polarxExplainDirectModify(node, es);
}


/*
 * routerGetForeignJoinPaths
 *		Add possible ForeignPath to joinrel, if join is safe to push down.
 */
static void
routerGetForeignJoinPaths(PlannerInfo *root,
							RelOptInfo *joinrel,
							RelOptInfo *outerrel,
							RelOptInfo *innerrel,
							JoinType jointype,
							JoinPathExtraData *extra)
{
    polarxGetForeignJoinPaths(root, joinrel, outerrel, innerrel, jointype, extra);
}

static bool
routerAnalyzeForeignTable(Relation relation,
                        AcquireSampleRowsFunc *func,
                        BlockNumber *totalpages)
{
    return polarxAnalyzeForeignTable(relation, func, totalpages);
}


static void
routerGetForeignUpperPaths(PlannerInfo *root,
                            UpperRelationKind stage,
                            RelOptInfo *input_rel,
                            RelOptInfo *output_rel,
                            void *extra)
{
    polarxGetForeignUpperPaths(root, stage, input_rel, output_rel, extra);
}

static void
polarx_object_access(ObjectAccessType access,
                        Oid classId,
                        Oid objectId,
                        int subId,
                        void *arg)
{
    if (next_object_access_hook)
        (*next_object_access_hook) (access, classId, objectId, subId, arg);

    if(subId !=0)
        return;

    switch (access)
    {
        case OAT_POST_CREATE:
            break;
        case OAT_DROP:
            {
                switch (classId)
                {
                    case RelationRelationId:
                        {
                            char        relkind = get_rel_relkind(objectId);

                            if(IS_PGXC_COORDINATOR && (relkind == RELKIND_RELATION
                                                    || relkind == RELKIND_PARTITIONED_TABLE))
                            {
                                Relation    rel;
                                HeapTuple   tuple;

                                rel = heap_open(ForeignTableRelationId, RowExclusiveLock);

                                tuple = SearchSysCache1(FOREIGNTABLEREL, ObjectIdGetDatum(objectId));
                                if (HeapTupleIsValid(tuple))
                                {
                                    CatalogTupleDelete(rel, &tuple->t_self);
                                    ReleaseSysCache(tuple);
                                }

                                heap_close(rel, RowExclusiveLock);
                            }
                        }
                        break;
                    default:
                        /* Ignore unsupported object classes */
                        break;
                }
            }
            break;

        case OAT_POST_ALTER:
        case OAT_NAMESPACE_SEARCH:
        case OAT_FUNCTION_EXECUTE:
            break;

        default:
            elog(ERROR, "unexpected object access type: %d", (int) access);
            break;
    }
}
static void
polarxExecutorStart(QueryDesc *queryDesc, int eflags)
{
    PlannedStmt *plannedStmt = queryDesc->plannedstmt;

    if(plannedStmt && (plannedStmt->commandType == CMD_UPDATE
                || plannedStmt->commandType == CMD_INSERT
                || plannedStmt->commandType == CMD_DELETE
                || plannedStmt->commandType == CMD_SELECT)
                && plannedStmt->rtable)
    {
        bool need_adjust = true;

        if(IsA(plannedStmt->planTree, CustomScan))
        {
            CustomScan *plan = (CustomScan *)plannedStmt->planTree;

            if(plan->custom_private)
            {
                void *ptr = linitial(plan->custom_private);
                if(polarxIsA(ptr, RemoteQuery))
                    need_adjust = false;
            }
        }
        if(need_adjust)
            AdjustRelationToForeignTable(plannedStmt->rtable);
        else 
            AdjustRelationBackToTable(false);
    }

    standard_ExecutorStart(queryDesc, eflags);
}
static void
polarxExplainOneQuery(Query *query, int cursorOptions,
                        IntoClause *into, ExplainState *es,
                        const char *queryString, ParamListInfo params,
                        QueryEnvironment *queryEnv)
{
    if(IS_PGXC_LOCAL_COORDINATOR && into && es->analyze)
        ereport(ERROR, (errmsg("EXPLAIN ANALYZE is currently not supported for INSERT "
                        "... SELECT commands via the coordinator")));
    else
    {
        PlannedStmt *plan;
        instr_time  planstart,
                    planduration;

        INSTR_TIME_SET_CURRENT(planstart);

        /* plan the query */
        plan = pg_plan_query(query, cursorOptions, params);

        INSTR_TIME_SET_CURRENT(planduration);
        INSTR_TIME_SUBTRACT(planduration, planstart);

        /* run it (if needed) and produce output */
        ExplainOnePlan(plan, into, es, queryString, params, queryEnv,
                &planduration);
    }
}
static void
RegisterPolarxConfigVariables(void)
{
    DefineCustomBoolVariable(
            "polarx.set_exec_utility_local",
            gettext_noop("This GUC control whether uitility command should be propgate"),
            gettext_noop("When enabled, utility commands will be execute on local and not propgate"
                "to other nodes"),
            &SetExecUtilityLocal,
            false,
            PGC_USERSET,
            0,
            NULL, NULL, NULL);
    DefineCustomBoolVariable(
            "polarx.enable_fast_query_shipping",
            gettext_noop("This GUC control Fast Query ship"),
            gettext_noop("When enabled, Enables the planner's use of fast query shipping to ship query directly to datanode."),
            &EnableFastQueryShipping,
            true,
            PGC_USERSET,
            0,
            NULL, NULL, NULL);
    DefineCustomBoolVariable(
            "polarx.enable_hlc_transaction",
            gettext_noop("This GUC control HLC transaction"),
            gettext_noop("When enabled, HLC will be used in distribute transaction, need kernel has coresponding support."),
            &EnableHLCTransaction,
            true,
            PGC_USERSET,
            0,
            NULL, NULL, NULL);
    DefineCustomBoolVariable(
            "polarx.enable_txn_debug_print",
            gettext_noop("This GUC control transaction log print"),
            gettext_noop("When enabled, more logs will be printed."),
            &EnableTransactionDebugPrint,
            false,
            PGC_USERSET,
            0,
            NULL, NULL, NULL);
    DefineCustomBoolVariable(
            "polarx.enable_log_remote_query",
            gettext_noop("enable log remote query"),
            NULL,
            &enable_log_remote_query,
            false,
            PGC_USERSET,
            0,
            NULL, NULL, NULL);
    DefineCustomBoolVariable(
            "pooler.persistent_datanode_connections",
            gettext_noop("Session never releases acquired connections."),
            NULL,
            &PersistentConnections,
            false,
            PGC_BACKEND,
            GUC_NOT_IN_SAMPLE,
            check_persistent_connections, NULL, NULL);
    DefineCustomIntVariable(
            "pooler.pool_conn_keepalive",
            gettext_noop("Close connections if they are idle in the pool for that time."),
            gettext_noop("A value of -1 turns autoclose off."),
            &PoolConnKeepAlive,
            60, 60, INT_MAX,
            PGC_SIGHUP,
            GUC_UNIT_S,
            NULL, NULL, NULL);
    DefineCustomIntVariable(
            "pooler.pool_maintenance_timeout",
            gettext_noop("Launch maintenance routine if pooler idle for that time."),
            gettext_noop("A value of -1 turns feature off."),
            &PoolMaintenanceTimeout,
            10, -1, INT_MAX,
            PGC_SIGHUP,
            GUC_UNIT_S,
            NULL, NULL, NULL);
    DefineCustomIntVariable(
            "pooler.max_pool_size",
            gettext_noop("Max pool size."),
            gettext_noop("If number of active connections reaches this value, "
                            "other connection requests will be refused"),
            &MaxPoolSize,
            300, 1, 65535,
            PGC_POSTMASTER,
            0,
            NULL, NULL, NULL);
    DefineCustomIntVariable(
            "pooler.min_free_size",
            gettext_noop("minimal pool free connection number."),
            gettext_noop("When pool need to acquire new connections, we use the number as step "),
            &MinFreeSize,
            5, 1, 65535,
            PGC_POSTMASTER,
            0,
            NULL, NULL, NULL);
    DefineCustomIntVariable(
            "pooler.min_pool_size",
            gettext_noop("Min pool size."),
            gettext_noop("If number of active connections decreased below this value, "
                         "we established new connections for warm user and database"),
            &MinPoolSize,
            5, 1, 65535,
            PGC_POSTMASTER,
            0,
            NULL, NULL, NULL);
    DefineCustomIntVariable(
            "pooler.pool_session_context_check_gap",
            gettext_noop("Gap to check datanode session memory context."),
            gettext_noop("In seconds."),
            &PoolSizeCheckGap,
            120, 10, 7200,
            PGC_SIGHUP,
            GUC_UNIT_S,
            NULL, NULL, NULL);
    DefineCustomIntVariable(
            "pooler.pool_session_max_lifetime",
            gettext_noop("Datanode session max lifetime."),
            gettext_noop("Session will be colsed when expired."),
            &PoolConnMaxLifetime,
            300, 1, INT_MAX,
            PGC_SIGHUP,
            GUC_UNIT_S,
            NULL, NULL, NULL);
    DefineCustomIntVariable(
            "pooler.pool_session_memory_limit",
            gettext_noop("Datanode session max memory context size."),
            gettext_noop("Exceed limit will be closed."),
            &PoolMaxMemoryLimit,
            10, 1, 10000,
            PGC_SIGHUP,
            GUC_UNIT_S,
            NULL, NULL, NULL);
    DefineCustomIntVariable(
            "pooler.pooler_connect_timeout",
            gettext_noop("Pooler connection timeout."),
            gettext_noop("Pooler connection timeout."),
            &PoolConnectTimeOut,
            10, 1, 3600,
            PGC_POSTMASTER,
            GUC_UNIT_S,
            NULL, NULL, NULL);
    DefineCustomIntVariable(
            "pooler.pooler_scale_factor",
            gettext_noop("Pooler scale factor."),
            gettext_noop("Pooler scale factor."),
            &PoolScaleFactor,
            2, 1, 64,
            PGC_POSTMASTER,
            0,
            NULL, NULL, NULL);
    DefineCustomIntVariable(
            "pooler.pooler_dn_set_timeout",
            gettext_noop("Pooler datanode set query timeout."),
            gettext_noop("Pooler datanode set query timeout."),
            &PoolDNSetTimeout,
            10, 1, 3600,
            PGC_SIGHUP,
            GUC_UNIT_S,
            NULL, NULL, NULL);
    DefineCustomIntVariable(
            "pooler.pool_check_slot_timeout",
            gettext_noop("Enable pooler check slot. When slot is using by agent, shouldn't exist in nodepool."),
            gettext_noop("A value of -1 turns feature off."),
            &PoolCheckSlotTimeout,
            5, -1, INT_MAX,
            PGC_SIGHUP,
            GUC_UNIT_S,
            NULL, NULL, NULL);
    DefineCustomIntVariable(
            "pooler.pool_print_stat_timeout",
            gettext_noop("Enable pooler print stat info."),
            gettext_noop("A value of -1 turns feature off."),
            &PoolPrintStatTimeout,
            60, -1, INT_MAX,
            PGC_SIGHUP,
            GUC_UNIT_S,
            NULL, NULL, NULL);
    DefineCustomIntVariable(
            "pooler.port",
            gettext_noop("Port of the Pool Manager."),
            NULL,
            &PoolerPort,
            6667, 1, 65535,
            PGC_POSTMASTER,
            0,
            NULL, NULL, NULL);
    DefineCustomStringVariable(
            "polarx.node_name",
            gettext_noop("The Coordinator or Datanode name."),
            NULL,
            &PGXCNodeName,
            "",
            PGC_POSTMASTER,
            GUC_NO_RESET_ALL | GUC_IS_NAME,
            NULL, NULL, NULL);
    DefineCustomStringVariable(
            "polarx.cluster_name",
            gettext_noop("The Cluster name."),
            NULL,
            &PGXCClusterName,
            "cluster_server",
            PGC_POSTMASTER,
            GUC_NO_RESET_ALL | GUC_IS_NAME,
            NULL, NULL, NULL);
    DefineCustomStringVariable(
            "polarx.main_cluster_name",
            gettext_noop("The Main Cluster name."),
            NULL,
            &PGXCMainClusterName,
            "cluster_server",
            PGC_POSTMASTER,
            GUC_NO_RESET_ALL | GUC_IS_NAME,
            NULL, NULL, NULL);
    DefineCustomEnumVariable(
            "polarx.remotetype",
            gettext_noop("Sets the type of PolarDB-X remote connection"),
            NULL,
            &remoteConnType,
            REMOTE_CONN_APP,
            pgxc_conn_types,
            PGC_BACKEND,
            0,
            NULL, NULL, NULL);
#ifdef POLARDBX_TWO_PHASE_TESTS
    DefineCustomIntVariable(
            "polarx.twophase_exception_case",
            gettext_noop("run tests for twophase transaction"),
            NULL,
            &twophase_exception_case,
            0, 0, INT_MAX,
            PGC_POSTMASTER,
            0,
            NULL, NULL, NULL);
    DefineCustomIntVariable(
            "polarx.twophase_exception_node_exception",
            gettext_noop("run tests for twophase transaction, node crash"),
            NULL,
            &twophase_exception_node_exception,
            0, 0, INT_MAX,
            PGC_POSTMASTER,
            0,
            NULL, NULL, NULL);
#endif

}
