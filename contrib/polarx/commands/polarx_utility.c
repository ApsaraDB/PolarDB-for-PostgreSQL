/*-------------------------------------------------------------------------
 * polarx_utility.c
 *    polarx utility hook and related functionality.
 *
 * The polarx utility hook is used to replace PostprocessUtility function.
 * We use this to implement DDL commands and other utitily commands
 * working on polarx distributed tables.
 *
 * Copyright (c) 2020, Alibaba Inc. and/or its affiliates
 * Copyright (c) 2020, Apache License Version 2.0*
 *
 * IDENTIFICATION
 *        contrib/polarx/commands/polarx_utility.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "miscadmin.h"
#include "access/htup_details.h"
#include "access/reloptions.h"
#include "access/twophase.h"
#include "access/xact.h"
#include "access/xlog.h"
#include "catalog/catalog.h"
#include "catalog/namespace.h"
#include "catalog/pg_inherits.h"
#include "catalog/toasting.h"
#include "parser/parse_utilcmd.h"
#include "postmaster/bgwriter.h"
#include "rewrite/rewriteDefine.h"
#include "rewrite/rewriteRemove.h"
#include "storage/fd.h"
#include "tcop/pquery.h"
#include "tcop/utility.h"
#include "utils/acl.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"
#include "utils/rel.h"
#include "utils/builtins.h"
#include "utils/snapmgr.h"
#include "nodes/nodes.h"
#include "nodes/makefuncs.h"
#include "pgxc/pgxc.h"
#include "pgxc/nodemgr.h"
#include "plan/planner.h"
#include "executor/execRemoteQuery.h"
#include "pgxc/connpool.h"
#include "commands/polarx_utility.h"
#include "commands/alter.h"
#include "commands/async.h"
#include "commands/cluster.h"
#include "commands/comment.h"
#include "commands/collationcmds.h"
#include "commands/conversioncmds.h"
#include "commands/copy.h"
#include "commands/createas.h"
#include "commands/dbcommands.h"
#include "commands/defrem.h"
#include "commands/discard.h"
#include "commands/event_trigger.h"
#include "commands/explain.h"
#include "commands/extension.h"
#include "commands/matview.h"
#include "commands/lockcmds.h"
#include "commands/policy.h"
#include "commands/portalcmds.h"
#include "commands/prepare.h"
#include "commands/proclang.h"
#include "commands/publicationcmds.h"
#include "commands/schemacmds.h"
#include "commands/seclabel.h"
#include "commands/sequence.h"
#include "commands/subscriptioncmds.h"
#include "commands/tablecmds.h"
#include "commands/tablespace.h"
#include "commands/trigger.h"
#include "commands/typecmds.h"
#include "commands/user.h"
#include "commands/vacuum.h"
#include "commands/view.h"
#include "commands/polarx_variable_set.h"
#include "commands/polarx_discard.h"
#include "commands/polarx_createas.h"
#include "commands/polarx_tablecmds.h"
#include "commands/polarx_dbcommands.h"
#include "deparse/deparse_fqs.h"
#include "utils/fdwplanner_utils.h"

bool SetExecUtilityLocal = false;

static bool ProcessUtilityPre(PlannedStmt *pstmt,
                                const char *queryString,
                                ProcessUtilityContext context,
                                QueryEnvironment *queryEnv,
                                bool sentToRemote,
                                char *completionTag);

static void ProcessUtilityPost(PlannedStmt *pstmt,
                                const char *queryString,
                                ProcessUtilityContext context,
                                QueryEnvironment *queryEnv,
                                bool sentToRemote);
static void ProcessUtilityReplace(PlannedStmt *pstmt,
                                const char *queryString,
                                ProcessUtilityContext context,
                                ParamListInfo params,
                                QueryEnvironment *queryEnv,
                                DestReceiver *dest,
                                bool sentToRemote,
                                char *completionTag);
static void ExecUtilityStmtOnNodes(Node* parsetree, const char *queryString,
                                    ExecNodes *nodes, bool sentToRemote,
                                    bool auto_commit,
                                    RemoteQueryExecType exec_type,
                                    bool is_temp, bool add_context);
static RemoteQueryExecType  ExecUtilityFindNodes(ObjectType object_type,
                                                Oid object_id,
                                                bool *is_temp);
static RemoteQueryExecType  ExecUtilityFindNodesRelkind(Oid relid, bool *is_temp);
static RemoteQueryExecType  GetNodesForCommentUtility(CommentStmt *stmt, bool *is_temp);
static RemoteQueryExecType  GetNodesForRulesUtility(RangeVar *relation, bool *is_temp);
static void DropStmtPreTreatment(DropStmt *stmt, const char *queryString, 
                                  bool *is_temp, RemoteQueryExecType *exec_type);
static void ExecUtilityStmtOnNodesInternal(Node* parsetree, const char *queryString,
                                            ExecNodes *nodes, bool sentToRemote,
                                            bool force_autocommit,
                                            RemoteQueryExecType exec_type,
                                            bool is_temp);
static void ExecDropStmt(DropStmt *stmt,
                            const char *queryString,
                            bool isTopLevel);
static void ProcessUtilitySlow(ParseState *pstate, PlannedStmt *pstmt,
                                const char *queryString, ProcessUtilityContext context,
                                ParamListInfo params, QueryEnvironment *queryEnv,
                                DestReceiver *dest, bool sentToRemote, char *completionTag);
static void check_xact_readonly(Node *parsetree);
static void set_exec_utility_local(bool value);
static void polarx_ProcessUtility_internal(PlannedStmt *pstmt,
                                            const char *queryString,
                                            ProcessUtilityContext context,
                                            ParamListInfo params,
                                            struct QueryEnvironment *queryEnv,
                                            DestReceiver *dest,
                                            bool sentToRemote,
                                            char *completionTag);

void
polarx_ProcessUtility(PlannedStmt *pstmt,
        const char *queryString,
        ProcessUtilityContext context,
        ParamListInfo params,
        struct QueryEnvironment *queryEnv,
        DestReceiver *dest,
        char *completionTag)
{
    bool    sentToRemote = SetExecUtilityLocal;

    polarx_ProcessUtility_internal(pstmt,queryString, context,
                                    params, queryEnv, dest, sentToRemote, completionTag);
}

static void
polarx_ProcessUtility_internal(PlannedStmt *pstmt,
        const char *queryString,
        ProcessUtilityContext context,
        ParamListInfo params,
        struct QueryEnvironment *queryEnv,
        DestReceiver *dest,
        bool sentToRemote,
        char *completionTag)
{
    AdjustRelationBackToTable(false);
    if (ProcessUtilityPre(pstmt, queryString, context, queryEnv, sentToRemote, completionTag))
    {
        return;
    }
    ProcessUtilityReplace(pstmt, queryString, context, params, queryEnv, dest, sentToRemote, completionTag);
    ProcessUtilityPost(pstmt, queryString, context, queryEnv, sentToRemote);
    AdjustRelationBackToForeignTable();
}
/*
 * Do the necessary processing before executing the utility command locally on
 * the coordinator.
 */
static bool
ProcessUtilityPre(PlannedStmt *pstmt,
        const char *queryString,
        ProcessUtilityContext context,
        QueryEnvironment *queryEnv,
        bool sentToRemote,
        char *completionTag)
{// #lizard forgives
    Node       *parsetree = pstmt->utilityStmt;
    bool        isTopLevel = (context == PROCESS_UTILITY_TOPLEVEL);
    bool        all_done = false;
    bool        is_temp = false;
    bool        auto_commit = false;
    bool        add_context = false;
    RemoteQueryExecType    exec_type = EXEC_ON_NONE;

    /*
     * auto_commit and is_temp is initialised to false and changed if required.
     *
     * exec_type is initialised to EXEC_ON_NONE and updated iff the command
     * needs remote execution during the preprocessing step.
     */

    switch (nodeTag(parsetree))
    {
        /*
         * ******************** transactions ********************
         */
        case T_TransactionStmt:
            /*
             * Portal (cursor) manipulation
             */
        case T_DeclareCursorStmt:
        case T_ClosePortalStmt:
        case T_FetchStmt:
        case T_DoStmt:
        case T_CreateTableSpaceStmt:
        case T_DropTableSpaceStmt:
        case T_AlterTableSpaceOptionsStmt:
        case T_TruncateStmt:
            break;
        case T_CopyStmt:
            {
                RangeVar *var = ((CopyStmt *) parsetree)->relation;

                if(var)
                    AdjustRelationToForeignTable(list_make1(var));
            }
            break;
        case T_PrepareStmt:
        case T_ExecuteStmt:
            break;
        case T_DeallocateStmt:
        case T_GrantRoleStmt:
        case T_CreatedbStmt:
            break;
        case T_AlterDatabaseStmt:
        case T_AlterDatabaseSetStmt:
            break;

        case T_DropdbStmt:
            {
                /* Clean connections before dropping a database on local node */
                if (IS_PGXC_LOCAL_COORDINATOR)
                {
                    DropdbStmt *stmt = (DropdbStmt *) parsetree;
                    char query[256];

                    DropDBCleanConnection(stmt->dbname);
                    /* Clean also remote Coordinators */
                    sprintf(query, "CLEAN CONNECTION TO ALL FOR DATABASE %s;",
                            quote_identifier(stmt->dbname));
                    ExecUtilityStmtOnNodes(parsetree, query, NULL, sentToRemote, true,
                            EXEC_ON_ALL_NODES, false, false);
                }
            }
            break;

            /* Query-level asynchronous notification */
        case T_NotifyStmt:
        case T_ListenStmt:
        case T_UnlistenStmt:
        case T_LoadStmt:
        case T_ClusterStmt:
            break;

        case T_VacuumStmt:
            {
                VacuumStmt *stmt = (VacuumStmt *) parsetree;

                /* we choose to allow this during "read only" transactions */
                PreventCommandDuringRecovery((stmt->options & VACOPT_VACUUM) ?
                        "VACUUM" : "ANALYZE");
                if((stmt->options & VACOPT_ANALYZE)
                        && !(stmt->options & VACOPT_VACUUM)
                        &&  stmt->rels != NULL)
                {
                    if(IS_PGXC_COORDINATOR)
                    {
                        ListCell    *cell;

                        foreach(cell, stmt->rels)
                        {
                            VacuumRelation *vrel = lfirst_node(VacuumRelation, cell);
                            if(vrel->relation)
                                AdjustRelationToForeignTable(list_make1(vrel->relation));
                        }
                    }
                    if(IS_PGXC_LOCAL_COORDINATOR)
                    {
                        if(IsInTransactionBlock(isTopLevel))
                        {
                            exec_type = EXEC_ON_DATANODES;
                            elog(WARNING, "Analyze will only work in local cordinator due to in a transaction block");
                        }
                        else
                        {
                            ListCell    *cell;
                            int temp_num = 0;
                            foreach(cell, stmt->rels)
                            {
                                VacuumRelation *vrel = lfirst_node(VacuumRelation, cell);
                                if(vrel->relation)
                                {
                                    Oid relid = RangeVarGetRelid(vrel->relation, NoLock, false);
                                    if (relid != InvalidOid && IsTempTable(relid))
                                    {
                                        temp_num++;
                                    }
                                }
                            }

                            if(temp_num > 0)
                            {
                                exec_type = EXEC_ON_DATANODES;
                                if(list_length(stmt->rels) != temp_num)
                                    elog(WARNING, "Analyze will only work in local cordinator due to there is a temp table in target list");
                            }
                            else
                                exec_type = EXEC_ON_ALL_NODES;
                        }
                    }
                }
            }
            break;

        case T_ExplainStmt:
        case T_AlterSystemStmt:
            break;
        case T_VariableSetStmt:
            ExecSetVariableStmtPre((VariableSetStmt *) parsetree);
            break;
        case T_VariableShowStmt:
        case T_DiscardStmt:
            break;

        case T_CreateEventTrigStmt:
            if(!IS_PGXC_SINGLE_NODE)
                ereport(ERROR,            
                        (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                         errmsg("EVENT TRIGGER not yet supported in Postgres-XL")));
            break;

        case T_AlterEventTrigStmt:
            break;

            /*
             * ******************************** ROLE statements ****
             */
        case T_CreateRoleStmt:
        case T_AlterRoleStmt:
        case T_AlterRoleSetStmt:
        case T_DropRoleStmt:
        case T_ReassignOwnedStmt:
        case T_LockStmt:
        case T_ConstraintsSetStmt:
        case T_CheckPointStmt:
        case T_ReindexStmt:
        case T_GrantStmt:
        case T_DropStmt:
            break;
        case T_RenameStmt:
            {
                RenameStmt *stmt = (RenameStmt *) parsetree;

                if (IS_PGXC_LOCAL_COORDINATOR)
                {
                    /*
                     * Get the necessary details about the relation before we
                     * run ExecRenameStmt locally. Otherwise we may not be able
                     * to look-up using the old relation name.
                     */
                    if (stmt->relation)
                    {
                        /*
                         * If the table does not exist, don't send the query to
                         * the remote nodes. The local node will eventually
                         * report an error, which is then sent back to the
                         * client.
                         */
                        Oid relid = RangeVarGetRelid(stmt->relation, NoLock, true);

                        if (OidIsValid(relid))
                            exec_type = ExecUtilityFindNodes(stmt->renameType,
                                    relid,
                                    &is_temp);
                        else
                            exec_type = EXEC_ON_NONE;
                    }
                    else
                        exec_type = ExecUtilityFindNodes(stmt->renameType,
                                InvalidOid,
                                &is_temp);
                    /* clean connections of the old name first. */
                    if (OBJECT_DATABASE == stmt->renameType)
                    {
                        char query[256];

                        DropDBCleanConnection(stmt->subname);
                        /* Clean also remote nodes */
                        sprintf(query, "CLEAN CONNECTION TO ALL FOR DATABASE %s;", stmt->subname);
                        ExecUtilityStmtOnNodes(parsetree, query, NULL, sentToRemote, true,
                                EXEC_ON_ALL_NODES, false, false);
                    }
                    if(OBJECT_FOREIGN_SERVER == stmt->renameType || OBJECT_FDW == stmt->renameType)
                        exec_type = EXEC_ON_NONE;
                }
            }
            break;
        case T_AlterObjectDependsStmt:
        case T_AlterObjectSchemaStmt:
        case T_AlterOwnerStmt:
            break;

        case T_RemoteQuery:
            Assert(IS_PGXC_COORDINATOR);
            /*
             * Do not launch query on Other Datanodes if remote connection is a Coordinator one
             * it will cause a deadlock in the cluster at Datanode levels.
             */
            if (!IsConnFromCoord() && !creating_extension)
                ExecRemoteUtility((RemoteQuery *) parsetree);
            all_done = true;
            break;

        case T_CleanConnStmt:
            /*
             * First send command to other nodes via probably existing
             * connections, then clean local pooler
             */
            if (IS_PGXC_COORDINATOR)
                ExecUtilityStmtOnNodes(parsetree, queryString, NULL, sentToRemote, true,
                        EXEC_ON_ALL_NODES, false, false);
            CleanConnection((CleanConnStmt *) parsetree);
            all_done = true;
            break;

        case T_CommentStmt:
        case T_SecLabelStmt:
        case T_CreateSchemaStmt:
        case T_CreateStmt:
        case T_CreateForeignTableStmt:
        case T_AlterTableStmt:
        case T_AlterDomainStmt:
        case T_DefineStmt:
        case T_IndexStmt:    /* CREATE INDEX */
            break;
        case T_CreateExtensionStmt:
        case T_AlterExtensionStmt:
            set_exec_utility_local(true);
            break;
        case T_AlterExtensionContentsStmt:
        case T_CreateFdwStmt:
        case T_AlterFdwStmt:
        case T_CreateForeignServerStmt:
        case T_AlterForeignServerStmt:
        case T_CreateUserMappingStmt:
        case T_AlterUserMappingStmt:
        case T_DropUserMappingStmt:
        case T_ImportForeignSchemaStmt:
        case T_CompositeTypeStmt:    /* CREATE TYPE (composite) */
        case T_CreateEnumStmt:    /* CREATE TYPE AS ENUM */
        case T_CreateRangeStmt: /* CREATE TYPE AS RANGE */
        case T_AlterEnumStmt:    /* ALTER TYPE (enum) */
        case T_ViewStmt:    /* CREATE VIEW */
        case T_CreateFunctionStmt:    /* CREATE FUNCTION */
        case T_AlterFunctionStmt:    /* ALTER FUNCTION */
        case T_RuleStmt:    /* CREATE RULE */
        case T_CreateSeqStmt:
        case T_AlterSeqStmt:
            break;
        case T_CreateTableAsStmt:
            if(IS_PGXC_LOCAL_COORDINATOR)
            {
                CreateTableAsStmt *stmt = (CreateTableAsStmt *) parsetree;

                exec_type = EXEC_ON_ALL_NODES;
                StringInfoData cquery;
                char *p = NULL;

                if(stmt->is_select_into)
                {
                    initStringInfo(&cquery);
                    polarx_deparse_query((Query *)stmt->query, &cquery, NIL, false, false);
                    p = pstrdup(cquery.data);

                    initStringInfo(&cquery);
                    appendStringInfo(&cquery, "CREATE %s TABLE %s%s%s AS %s %s;",
                            stmt->into->rel->relpersistence == RELPERSISTENCE_UNLOGGED ? "UNLOGGED": "",
                            stmt->into->rel->schemaname ? stmt->into->rel->schemaname : "",
                            stmt->into->rel->schemaname ? "." : "",
                            stmt->into->rel->relname, p , "WITH NO DATA");
                    queryString = cquery.data;
                }
                else if(!stmt->into->skipData)
                {
                    p = strtok(pstrdup(queryString), ";");
                    initStringInfo(&cquery);
                    appendStringInfo(&cquery, "%s %s;", p, "WITH NO DATA");
                    queryString = cquery.data;
                }
            }
            break;

        case T_RefreshMatViewStmt:
            if (IS_PGXC_LOCAL_COORDINATOR)
            {
                RefreshMatViewStmt *stmt = (RefreshMatViewStmt *) parsetree;
                if (stmt->relation->relpersistence != RELPERSISTENCE_TEMP)
                    exec_type = EXEC_ON_COORDS;
            }
            break;
        case T_CreateTrigStmt:
        case T_CreatePLangStmt:
        case T_CreateDomainStmt:
        case T_CreateConversionStmt:
        case T_CreateCastStmt:
        case T_CreateOpClassStmt:
        case T_CreateOpFamilyStmt:
        case T_CreateTransformStmt:
        case T_AlterOpFamilyStmt:
        case T_AlterTSDictionaryStmt:
        case T_AlterTSConfigurationStmt:
        case T_AlterTableMoveAllStmt:
        case T_AlterOperatorStmt:
        case T_DropOwnedStmt:
        case T_AlterDefaultPrivilegesStmt:
        case T_CreatePolicyStmt:    /* CREATE POLICY */
        case T_AlterPolicyStmt: /* ALTER POLICY */
        case T_CreateAmStmt:
            break;

        case T_CreatePublicationStmt:
            if (IS_PGXC_COORDINATOR)
            {
                ereport(ERROR,
                        (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                         errmsg("COORDINATOR does not support CREATE PUBLICATION"),
                         errdetail("The feature is not currently supported")));
            }
            break;
        case T_AlterPublicationStmt:
            break;
        case T_CreateSubscriptionStmt:
            if (IS_PGXC_COORDINATOR)
            {
                ereport(ERROR,
                        (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                         errmsg("COORDINATOR does not support CREATE SUBSCRIPTION"),
                         errdetail("The feature is not currently supported")));
            }
            break;
        case T_AlterSubscriptionStmt:
        case T_DropSubscriptionStmt:
        case T_CreateStatsStmt:
        case T_AlterCollationStmt:
        case T_CallStmt:
            break;
        default:
            elog(ERROR, "unrecognized node type: %d",
                    (int) nodeTag(parsetree));
            break;
    }

    /*
     * Send queryString to remote nodes, if needed.
     */ 
    if (IS_PGXC_LOCAL_COORDINATOR)
        ExecUtilityStmtOnNodes(parsetree, queryString, NULL, sentToRemote, auto_commit,
                exec_type, is_temp, add_context);

    return all_done;
}

static void
ProcessUtilityPost(PlannedStmt *pstmt,
        const char *queryString,
        ProcessUtilityContext context,
        QueryEnvironment *queryEnv,
        bool sentToRemote)
{// #lizard forgives
    Node       *parsetree = pstmt->utilityStmt;
    bool        is_temp = false;
    bool        auto_commit = false;
    bool        add_context = false;
    RemoteQueryExecType    exec_type = EXEC_ON_NONE;

    /*
     * auto_commit and is_temp is initialised to false and changed if required.
     *
     * exec_type is initialised to EXEC_ON_NONE and updated iff the command
     * needs remote execution during the preprocessing step.
     */

    switch (nodeTag(parsetree))
    {
        /*
         * ******************** transactions ********************
         */
        case T_TransactionStmt:
            {
                TransactionStmt *stmt = (TransactionStmt *) parsetree;

                if(stmt->kind == TRANS_STMT_BEGIN ||
                    stmt->kind == TRANS_STMT_START)
                {
                    ListCell   *lc;

                    foreach(lc, stmt->options)
                    {
                        DefElem    *item = (DefElem *) lfirst(lc);

                        if (strcmp(item->defname, "transaction_isolation") == 0)
                            PolarxSetPGVariable("transaction_isolation",
                                    list_make1(item->arg),
                                    true);
                        else if (strcmp(item->defname, "transaction_read_only") == 0)
                            PolarxSetPGVariable("transaction_read_only",
                                    list_make1(item->arg),
                                    true);
                        else if (strcmp(item->defname, "transaction_deferrable") == 0)
                            PolarxSetPGVariable("transaction_deferrable",
                                    list_make1(item->arg),
                                    true);
                    }
                }
                /* execute savepoint on all nodes */
                if (stmt->kind == TRANS_STMT_SAVEPOINT ||
                        stmt->kind == TRANS_STMT_RELEASE ||
                        stmt->kind == TRANS_STMT_ROLLBACK_TO)
                {
                    add_context = true;
                    exec_type = EXEC_ON_ALL_NODES;
                    break;
                }
            }
        case T_DeclareCursorStmt:
        case T_ClosePortalStmt:
        case T_FetchStmt:
        case T_DoStmt:
        case T_CopyStmt:
        case T_PrepareStmt:
        case T_ExecuteStmt:
        case T_DeallocateStmt:
        case T_NotifyStmt:
        case T_ListenStmt:
        case T_UnlistenStmt:
        case T_VacuumStmt:
        case T_ExplainStmt:
        case T_AlterSystemStmt:
            break;
        case T_VariableSetStmt:
            ExecSetVariableStmtPost((VariableSetStmt *) parsetree);
            break;
        case T_VariableShowStmt:
        case T_CreateEventTrigStmt:
        case T_AlterEventTrigStmt:
        case T_DropStmt:
        case T_RenameStmt:
        case T_AlterObjectDependsStmt:
        case T_RemoteQuery:
        case T_CleanConnStmt:
        case T_SecLabelStmt:
        case T_CreateSchemaStmt:
        case T_CreateStmt:
        case T_CreateForeignTableStmt:
            break;
            /* POLARX_TODO: currently we do not support alter table distributed by,
             *     so execute all nodes directly */
            /*case T_AlterTableStmt:*/
        case T_CreateFdwStmt:
        case T_AlterFdwStmt:
        case T_CreateForeignServerStmt:
        case T_AlterForeignServerStmt:
        case T_CreateUserMappingStmt:
        case T_AlterUserMappingStmt:
        case T_DropUserMappingStmt:
            exec_type = EXEC_ON_NONE;
            break;
        case T_ImportForeignSchemaStmt:
        case T_RefreshMatViewStmt:
        case T_CreateTransformStmt:
        case T_AlterOperatorStmt:
        case T_CreatePublicationStmt:
        case T_AlterPublicationStmt:
        case T_CreateSubscriptionStmt:
        case T_AlterSubscriptionStmt:
        case T_DropSubscriptionStmt:
        case T_AlterCollationStmt:
            break;
        case T_CreateTableSpaceStmt:
        case T_CreatedbStmt:
        case T_DropdbStmt:
        case T_DropTableSpaceStmt:
            add_context = true;
            exec_type = EXEC_ON_ALL_NODES;
            auto_commit = true;
            break;
        case T_AlterTableSpaceOptionsStmt:
        case T_GrantRoleStmt:
        case T_AlterDatabaseSetStmt:
        case T_CreateRoleStmt:
        case T_AlterRoleStmt:
        case T_AlterRoleSetStmt:
        case T_DropRoleStmt:
        case T_ReassignOwnedStmt:
        case T_LockStmt:
        case T_AlterOwnerStmt:
        case T_AlterDomainStmt:
        case T_DefineStmt:
        case T_CompositeTypeStmt:    /* CREATE TYPE (composite) */
        case T_CreateEnumStmt:    /* CREATE TYPE AS ENUM */
        case T_CreateRangeStmt: /* CREATE TYPE AS RANGE */
        case T_CreateFunctionStmt:    /* CREATE FUNCTION */
        case T_AlterFunctionStmt:    /* ALTER FUNCTION */
            exec_type = EXEC_ON_ALL_NODES;
            break;
        case T_AlterExtensionContentsStmt:
            exec_type = EXEC_ON_NONE;
            break;
        case T_CreateTrigStmt:
            exec_type = EXEC_ON_COORDS;
            break;
        case T_CreatePLangStmt:
        case T_CreateDomainStmt:
        case T_CreateConversionStmt:
        case T_CreateCastStmt:
        case T_CreateOpClassStmt:
        case T_CreateOpFamilyStmt:
        case T_AlterOpFamilyStmt:
        case T_AlterTSDictionaryStmt:
        case T_AlterTSConfigurationStmt:
        case T_AlterTableMoveAllStmt:
        case T_DropOwnedStmt:
        case T_AlterDefaultPrivilegesStmt:
        case T_CreatePolicyStmt:    /* CREATE POLICY */
        case T_AlterPolicyStmt: /* ALTER POLICY */
        case T_CreateAmStmt:
            exec_type = EXEC_ON_ALL_NODES;
            break;

        case T_CreateExtensionStmt:
        case T_AlterExtensionStmt:
            set_exec_utility_local(false);
            exec_type = EXEC_ON_NONE;
            break;

        case T_TruncateStmt:
            /*
             * Check details of the object being truncated.
             * If at least one temporary table is truncated truncate cannot use 2PC
             * at commit.
             */
            if (IS_PGXC_LOCAL_COORDINATOR)
            {
                ListCell    *cell;
                TruncateStmt *stmt = (TruncateStmt *) parsetree;

                foreach(cell, stmt->relations)
                {
                    Oid relid;
                    RangeVar *rel = (RangeVar *) lfirst(cell);

                    relid = RangeVarGetRelid(rel, NoLock, false);
                    if (IsTempTable(relid))
                    {
                        is_temp = true;
                        break;
                    }
                }
                exec_type = EXEC_ON_DATANODES;
            }
            break;

        case T_AlterDatabaseStmt:
            if (IS_PGXC_LOCAL_COORDINATOR)
            {
                /*
                 * If this is not a SET TABLESPACE statement, just propogate the
                 * cmd as usual.
                 */
                if (IsSetTableSpace((AlterDatabaseStmt*) parsetree))
                    add_context = true;
                exec_type = EXEC_ON_ALL_NODES;
            }
            break;

        case T_LoadStmt:
        case T_CallStmt:
        case T_ConstraintsSetStmt:
            exec_type = EXEC_ON_DATANODES;
            break;

        case T_ClusterStmt:
        case T_CheckPointStmt:
            auto_commit = true;
            exec_type = EXEC_ON_DATANODES;
            break;

        case T_DiscardStmt:
            DiscardCommandPost((DiscardStmt *) parsetree);
            /*
             * Discard objects for all the sessions possible.
             * For example, temporary tables are created on all Datanodes
             * and Coordinators.
             */
            auto_commit = true;
            exec_type = EXEC_ON_ALL_NODES;
            break;

        case T_ReindexStmt: 
            {
                ReindexStmt         *stmt = (ReindexStmt *) parsetree;
                Oid                    relid;

                /* forbidden in parallel mode due to CommandIsReadOnly */
                switch (stmt->kind)
                {
                    case REINDEX_OBJECT_INDEX:
                    case REINDEX_OBJECT_TABLE:
                        relid = RangeVarGetRelid(stmt->relation, NoLock, true);
                        exec_type = ExecUtilityFindNodesRelkind(relid, &is_temp);
                        break;
                    case REINDEX_OBJECT_SCHEMA:
                    case REINDEX_OBJECT_SYSTEM:
                    case REINDEX_OBJECT_DATABASE:
                        exec_type = EXEC_ON_DATANODES;
                        break;
                    default:
                        elog(ERROR, "unrecognized object type: %d",
                                (int) stmt->kind);
                        break;
                }
                if (IS_PGXC_LOCAL_COORDINATOR)
                {
                    auto_commit = (stmt->kind == REINDEX_OBJECT_DATABASE ||
                            stmt->kind == REINDEX_OBJECT_SCHEMA);
                }
            }
            break;
        case T_GrantStmt: 
            {
                GrantStmt  *stmt = (GrantStmt *) parsetree;
                if (IS_PGXC_LOCAL_COORDINATOR)
                {
                    RemoteQueryExecType remoteExecType = EXEC_ON_ALL_NODES;

                    /* Launch GRANT on Coordinator if object is a sequence */
                    if ((stmt->objtype == OBJECT_TABLE &&
                                stmt->targtype == ACL_TARGET_OBJECT))
                    {
                        /*
                         * In case object is a relation, differenciate the case
                         * of a sequence, a view and a table
                         */
                        ListCell   *cell;
                        /* Check the list of objects */
                        bool        first = true;
                        RemoteQueryExecType type_local = remoteExecType;

                        foreach (cell, stmt->objects)
                        {
                            RangeVar   *relvar = (RangeVar *) lfirst(cell);
                            Oid            relid = RangeVarGetRelid(relvar, NoLock, true);

                            /* Skip if object does not exist */
                            if (!OidIsValid(relid))
                                continue;

                            remoteExecType = ExecUtilityFindNodesRelkind(relid, &is_temp);

                            /* Check if object node type corresponds to the first one */
                            if (first)
                            {
                                type_local = remoteExecType;
                                first = false;
                            }
                            else
                            {
                                if (type_local != remoteExecType)
                                    ereport(ERROR,
                                            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                                             errmsg("PGXC does not support GRANT on multiple object types"),
                                             errdetail("Grant VIEW/TABLE with separate queries")));
                            }
                        }
                    }
                    exec_type = remoteExecType;
                }
            }
            break;

        case T_AlterObjectSchemaStmt:
            if (IS_PGXC_LOCAL_COORDINATOR)
            {
                AlterObjectSchemaStmt *stmt = (AlterObjectSchemaStmt *) parsetree;

                /* Try to use the object relation if possible */
                if (stmt->relation)
                {
                    /*
                     * When a relation is defined, it is possible that this object does
                     * not exist but an IF EXISTS clause might be used. So we do not do
                     * any error check here but block the access to remote nodes to
                     * this object as it does not exisy
                     */
                    Oid relid = RangeVarGetRelid(stmt->relation, NoLock, true);

                    if (OidIsValid(relid))
                        exec_type = ExecUtilityFindNodes(stmt->objectType,
                                relid,
                                &is_temp);
                    else
                        exec_type = EXEC_ON_NONE;
                }
                else
                {
                    exec_type = ExecUtilityFindNodes(stmt->objectType,
                            InvalidOid,
                            &is_temp);
                }
            }
            break;

        case T_CommentStmt:
            /* Comment objects depending on their object and temporary types */
            if (IS_PGXC_LOCAL_COORDINATOR)
            {
                CommentStmt *stmt = (CommentStmt *) parsetree;
                exec_type = GetNodesForCommentUtility(stmt, &is_temp);
            }
            break;

        case T_AlterTableStmt: /* ALTER TABLE */
            if (IS_PGXC_LOCAL_COORDINATOR)
            {
                AlterTableStmt *stmt = (AlterTableStmt *)parsetree;
                Oid relid = RangeVarGetRelid(stmt->relation, NoLock, true);

                if (OidIsValid(relid))
                    exec_type = ExecUtilityFindNodes(OBJECT_TABLE, relid, &is_temp);
                else
                    exec_type = EXEC_ON_NONE;
            }
            break;
        case T_IndexStmt:    /* CREATE INDEX */
            if(IS_PGXC_LOCAL_COORDINATOR)
            {
                IndexStmt  *stmt = (IndexStmt *) parsetree;
                Oid            relid;

                /* INDEX on a temporary table cannot use 2PC at commit */
                relid = RangeVarGetRelid(stmt->relation, NoLock, true);
                DefineIndexCheck(relid, stmt);

                if (OidIsValid(relid))
                    exec_type = ExecUtilityFindNodes(OBJECT_INDEX, relid, &is_temp);
                else
                    exec_type = EXEC_ON_NONE;

                auto_commit = stmt->concurrent;
                if (stmt->isconstraint)
                    exec_type = EXEC_ON_NONE;
            }
            break;

        case T_AlterEnumStmt:    /* ALTER TYPE (enum) */
            /*
             * In this case force autocommit, this transaction cannot be launched
             * inside a transaction block.
             */
            if(IS_PGXC_LOCAL_COORDINATOR)
            {
                exec_type = EXEC_ON_ALL_NODES;
                if(!IsTransactionBlock())
                    auto_commit = true;
            }
            break;

        case T_ViewStmt:    /* CREATE VIEW */
            if (IS_PGXC_LOCAL_COORDINATOR)
            {
                ViewStmt *stmt = (ViewStmt *)parsetree;
                Oid relid = RangeVarGetRelid(stmt->view, NoLock, true);

                if (OidIsValid(relid))
                    exec_type = ExecUtilityFindNodes(OBJECT_VIEW, relid, &is_temp);
                else
                    exec_type = EXEC_ON_NONE;
                //exec_type = EXEC_ON_COORDS;
            }
            break;

        case T_RuleStmt:    /* CREATE RULE */
            if (IS_PGXC_LOCAL_COORDINATOR)
            {
                exec_type = GetNodesForRulesUtility(((RuleStmt *) parsetree)->relation,
                        &is_temp);
            }
            break;

        case T_CreateSeqStmt:
            if (IS_PGXC_LOCAL_COORDINATOR)
            {
                CreateSeqStmt *stmt = (CreateSeqStmt *) parsetree;
                Oid relid = RangeVarGetRelid(stmt->sequence, NoLock, true);

                if (OidIsValid(relid))
                    exec_type = ExecUtilityFindNodes(OBJECT_SEQUENCE, relid, &is_temp);
            }
            break;

        case T_AlterSeqStmt:
            if (IS_PGXC_LOCAL_COORDINATOR)
            {
                AlterSeqStmt *stmt = (AlterSeqStmt *) parsetree;
                Oid relid = RangeVarGetRelid(stmt->sequence, NoLock, true);

                if (OidIsValid(relid))
                    exec_type = ExecUtilityFindNodes(OBJECT_SEQUENCE, relid, &is_temp);

            }
            break;

        case T_CreateTableAsStmt:
            break;

        case T_CreateStatsStmt:
            if (IS_PGXC_LOCAL_COORDINATOR)
            {
                CreateStatsStmt *stmt = (CreateStatsStmt *) parsetree;
                RangeVar *rln = linitial(stmt->relations);
                Relation rel = relation_openrv((RangeVar *) rln, ShareUpdateExclusiveLock);

                /*
                 * Get the target nodes to run the CREATE STATISTICS
                 * command. Since the grammar does not tell us about the
                 * underlying object type, we use the other variant to
                 * fetch the nodes. This is ok because the command must
                 * only be even used on some kind of relation.
                 */ 
                exec_type =
                    ExecUtilityFindNodesRelkind(RelationGetRelid(rel), &is_temp);

                relation_close(rel, NoLock);
            }
            break;
        default:
            elog(ERROR, "unrecognized node type: %d",
                    (int) nodeTag(parsetree));
            break;
    }

    if (IS_PGXC_LOCAL_COORDINATOR)
        ExecUtilityStmtOnNodes(parsetree, queryString, NULL, sentToRemote, auto_commit,
                exec_type, is_temp, add_context);
}

static void
ExecUtilityStmtOnNodesInternal(Node* parsetree, const char *queryString, ExecNodes *nodes, bool sentToRemote,
        bool force_autocommit, RemoteQueryExecType exec_type, bool is_temp)
{
    /* Return if query is launched on no nodes */
    if (exec_type == EXEC_ON_NONE)
        return;

    /* Nothing to be done if this statement has been sent to the nodes */
    if (sentToRemote)
        return;

    /* If no Datanodes defined, the query cannot be launched */
    if (NumDataNodes == 0)
        ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_OBJECT),
                 errmsg("No Datanode defined in cluster"),
                 errhint("You need to define at least 1 Datanode with "
                     "CREATE NODE.")));

    if (!IsConnFromCoord())
    {
        RemoteQuery *step = makeNode(RemoteQuery);
        step->combine_type = COMBINE_TYPE_SAME;
        step->exec_nodes = nodes;
        step->sql_statement = pstrdup(queryString);
        step->force_autocommit = force_autocommit;
        step->exec_type = exec_type;
        step->parsetree = parsetree;
        ExecRemoteUtility(step);
        pfree(step->sql_statement);
        pfree(step);
    }
}


/*
 * ExecUtilityStmtOnNodes
 *
 * Execute the query on remote nodes
 * 
 *  queryString is the raw query to be executed.
 *     If nodes is NULL then the list of nodes is computed from exec_type.
 *     If auto_commit is true, then the query is executed without a transaction
 *       block and auto-committed on the remote node.
 *     exec_type is used to compute the list of remote nodes on which the query is
 *       executed.
 *     is_temp is set to true if the query involves a temporary database object.
 *  If add_context is true and if this fails on one of the nodes then add a
 *       context message containing the failed node names.
 *
 *  NB: parsetree is used to identify 3 subtransaction cmd: 
 *      savepoint, rollback to, release savepoint.
 *  Since these commands should not acquire xid
 */
static void
ExecUtilityStmtOnNodes(Node* parsetree, const char *queryString, ExecNodes *nodes,
        bool sentToRemote, bool auto_commit, RemoteQueryExecType exec_type,
        bool is_temp, bool add_context)
{
    PG_TRY();
    {
        ExecUtilityStmtOnNodesInternal(parsetree, queryString, nodes, sentToRemote,
                auto_commit, exec_type, is_temp);
    }
    PG_CATCH();
    {
        /*
         * Some nodes failed. Add context about what all nodes the query
         * failed
         */
        ExecNodes *coord_success_nodes = NULL;
        ExecNodes *data_success_nodes = NULL;
        char *msg_failed_nodes;

        /*
         * If the caller has asked for context information, add that and
         * re-throw the error.
         */
        if (!add_context)
            PG_RE_THROW();

        pgxc_all_success_nodes(&data_success_nodes, &coord_success_nodes, &msg_failed_nodes);
        if (msg_failed_nodes)
            errcontext("%s", msg_failed_nodes);
        PG_RE_THROW();
    }
    PG_END_TRY();


}

/*
 * ExecUtilityFindNodes
 *
 * Determine the list of nodes to launch query on.
 * This depends on temporary nature of object and object type.
 * Return also a flag indicating if relation is temporary.
 *
 * If object is a RULE, the object id sent is that of the object to which the
 * rule is applicable.
 */
static RemoteQueryExecType
ExecUtilityFindNodes(ObjectType object_type,
        Oid object_id,
        bool *is_temp)
{// #lizard forgives
    RemoteQueryExecType exec_type;

    switch (object_type)
    {
        case OBJECT_SEQUENCE:
            *is_temp = IsTempTable(object_id);
            exec_type = EXEC_ON_ALL_NODES;
            break;

        case OBJECT_TABLE:
        case OBJECT_RULE:
        case OBJECT_VIEW:
        case OBJECT_MATVIEW:
        case OBJECT_INDEX:
            exec_type = ExecUtilityFindNodesRelkind(object_id, is_temp);
            break;
        case OBJECT_TRIGGER:
            *is_temp = false;
            exec_type = EXEC_ON_COORDS;
            break;
        case OBJECT_FDW:
        case OBJECT_FOREIGN_SERVER:
            *is_temp = false;
            exec_type = EXEC_ON_NONE;
            break;

        default:
            *is_temp = false;
            exec_type = EXEC_ON_ALL_NODES;
            break;
    }

    return exec_type;
}

/*
 * ExecUtilityFindNodesRelkind
 *
 * Get node execution and temporary type
 * for given relation depending on its relkind
 */
static RemoteQueryExecType
ExecUtilityFindNodesRelkind(Oid relid, bool *is_temp)
{// #lizard forgives
    char relkind_str = get_rel_relkind(relid);
    RemoteQueryExecType exec_type;

    switch (relkind_str)
    {
        case RELKIND_RELATION:
        case RELKIND_PARTITIONED_TABLE:
            {
                Relation    rel = relation_open(relid, NoLock);
                *is_temp = (rel->rd_rel->relpersistence == RELPERSISTENCE_TEMP);
                exec_type = EXEC_ON_ALL_NODES;
                relation_close(rel, NoLock);
            }
            break;
        case RELKIND_INDEX:
            {
                HeapTuple   tuple;
                Oid table_relid = InvalidOid;

                tuple = SearchSysCache1(INDEXRELID, ObjectIdGetDatum(relid));
                if (HeapTupleIsValid(tuple))
                {
                    Form_pg_index index = (Form_pg_index) GETSTRUCT(tuple);
                    table_relid = index->indrelid;

                    /* Release system cache BEFORE looking at the parent table */
                    ReleaseSysCache(tuple);
                    exec_type = ExecUtilityFindNodesRelkind(table_relid, is_temp);
                }
                else
                {
                    exec_type = EXEC_ON_NONE;
                    *is_temp = false;
                }
            }
            break;
        case RELKIND_SEQUENCE:
            *is_temp = IsTempTable(relid);
            exec_type = EXEC_ON_ALL_NODES;
            break;
        case RELKIND_VIEW:
        case RELKIND_MATVIEW:
            *is_temp = IsTempTable(relid);
            exec_type = EXEC_ON_ALL_NODES;
            break;

        default:
            *is_temp = false;
            exec_type = EXEC_ON_ALL_NODES;
            break;
    }

    return exec_type;
}

/*
 * GetCommentObjectId
 * TODO Change to return the nodes to execute the utility on
 *
 * Return Object ID of object commented
 * Note: This function uses portions of the code of CommentObject,
 * even if this code is duplicated this is done like this to facilitate
 * merges with PostgreSQL head.
 */
static RemoteQueryExecType
GetNodesForCommentUtility(CommentStmt *stmt, bool *is_temp)
{// #lizard forgives
    ObjectAddress        address;
    Relation            relation;
    RemoteQueryExecType    exec_type = EXEC_ON_ALL_NODES;    /* By default execute on all nodes */
    Oid                    object_id;

    if (stmt->objtype == OBJECT_DATABASE)
    {
        char       *database = strVal((Value *) stmt->object);
        if (!OidIsValid(get_database_oid(database, true)))
            ereport(WARNING,
                    (errcode(ERRCODE_UNDEFINED_DATABASE),
                     errmsg("database \"%s\" does not exist", database)));
        /* No clue, return the default one */
        return exec_type;
    }

    address = get_object_address(stmt->objtype, stmt->object,
            &relation, ShareUpdateExclusiveLock, false);
    object_id = address.objectId;

    /*
     * If the object being commented is a rule, the nodes are decided by the
     * object to which rule is applicable, so get the that object's oid
     */
    if (stmt->objtype == OBJECT_RULE)
    {
        if (!relation && !OidIsValid(relation->rd_id))
        {
            /* This should not happen, but prepare for the worst */
            char *rulename = strVal(llast(castNode(List, stmt->object)));
            ereport(WARNING,
                    (errcode(ERRCODE_UNDEFINED_OBJECT),
                     errmsg("can not find relation for rule \"%s\" does not exist", rulename)));
            object_id = InvalidOid;
        }
        else
            object_id = RelationGetRelid(relation);
    }

    if (relation != NULL)
        relation_close(relation, NoLock);

    /* Commented object may not have a valid object ID, so move to default */
    if (OidIsValid(object_id))
        exec_type = ExecUtilityFindNodes(stmt->objtype,
                object_id,
                is_temp);
    return exec_type;
}

/*
 * GetNodesForRulesUtility
 * Get the nodes to execute this RULE related utility statement.
 * A rule is expanded on Coordinator itself, and does not need any
 * existence on Datanode. In fact, if it were to exist on Datanode,
 * there is a possibility that it would expand again
 */
static RemoteQueryExecType
GetNodesForRulesUtility(RangeVar *relation, bool *is_temp)
{
    Oid relid = RangeVarGetRelid(relation, NoLock, true);
    RemoteQueryExecType exec_type;

    /* Skip if this Oid does not exist */
    if (!OidIsValid(relid))
        return EXEC_ON_NONE;

    /*
     * PGXCTODO: See if it's a temporary object, do we really need
     * to care about temporary objects here? What about the
     * temporary objects defined inside the rule?
     */
    exec_type = ExecUtilityFindNodes(OBJECT_RULE, relid, is_temp);
    return exec_type;
}

/*
 * TreatDropStmtOnCoord
 * Do a pre-treatment of Drop statement on a remote Coordinator
 */
static void
DropStmtPreTreatment(DropStmt *stmt, const char *queryString, 
        bool *is_temp, RemoteQueryExecType *exec_type)
{// #lizard forgives
    bool        res_is_temp = false;
    RemoteQueryExecType res_exec_type = EXEC_ON_ALL_NODES;

    /* Nothing to do if not local Coordinator */
    if (IS_PGXC_DATANODE || IsConnFromCoord())
        return;

    switch (stmt->removeType)
    {
        case OBJECT_TABLE:
        case OBJECT_SEQUENCE:
        case OBJECT_VIEW:
        case OBJECT_INDEX:
        case OBJECT_MATVIEW:
            {
                /*
                 * Check the list of objects going to be dropped.
                 * XC does not allow yet to mix drop of temporary and
                 * non-temporary objects because this involves to rewrite
                 * query to process for tables.
                 */
                ListCell   *cell;
                bool        is_first = true;

                foreach(cell, stmt->objects)
                {
                    RangeVar   *rel = makeRangeVarFromNameList((List *) lfirst(cell));
                    Oid         relid;

                    /*
                     * Do not print result at all, error is thrown
                     * after if necessary
                     */
                    relid = RangeVarGetRelid(rel, NoLock, true);

                    /* In case of DROP ... IF EXISTS bypass */
                    if (!OidIsValid(relid))
                        continue;

                    if (is_first)
                    {
                        res_exec_type = ExecUtilityFindNodes(stmt->removeType,
                                relid,
                                &res_is_temp);
                        is_first = false;
                    }
                    else
                    {
                        RemoteQueryExecType exec_type_loc;
                        bool is_temp_loc;
                        exec_type_loc = ExecUtilityFindNodes(stmt->removeType,
                                relid,
                                &is_temp_loc);
                        if (exec_type_loc != res_exec_type ||
                                is_temp_loc != res_is_temp)
                            ereport(ERROR,
                                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                                     errmsg("DROP not supported for TEMP and non-TEMP objects"),
                                     errdetail("You should separate TEMP and non-TEMP objects")));
                    }
                }
            }
            break;

        case OBJECT_RULE:
        case OBJECT_TRIGGER:
            {
                /*
                 * In the case of a rule we need to find the object on
                 * which the rule is dependent and define if this rule
                 * has a dependency with a temporary object or not.
                 */
                Node *objname = linitial(stmt->objects);
                Relation    relation = NULL;

                get_object_address(stmt->removeType,
                        objname, /* XXX PG10MERGE: check if this is ok */
                        &relation,
                        AccessExclusiveLock,
                        stmt->missing_ok);

                /* Do nothing if no relation */
                if (relation && OidIsValid(relation->rd_id))
                    res_exec_type = ExecUtilityFindNodes(stmt->removeType,
                            relation->rd_id,
                            &res_is_temp);
                else
                    res_exec_type = EXEC_ON_NONE;

                /* Close relation if necessary */
                if (relation)
                    relation_close(relation, NoLock);
            }
            break;
        case  OBJECT_FDW:
        case  OBJECT_FOREIGN_SERVER:
            res_is_temp = false;
            res_exec_type = EXEC_ON_NONE;
            break;

        default:
            res_is_temp = false;
            res_exec_type = EXEC_ON_ALL_NODES;
            break;
    }

    /* Save results */
    *is_temp = res_is_temp;
    *exec_type = res_exec_type;
}

static void
ExecDropStmt(DropStmt *stmt,
                const char *queryString,
                bool isTopLevel)
{
    switch (stmt->removeType)
    {
        case OBJECT_INDEX:
            if (stmt->concurrent)
                PreventInTransactionBlock(isTopLevel,
                                        "DROP INDEX CONCURRENTLY");
        case OBJECT_TABLE:
        case OBJECT_SEQUENCE:
        case OBJECT_VIEW:
        case OBJECT_MATVIEW:
        case OBJECT_FOREIGN_TABLE:
            {
                bool        is_temp = false;
                RemoteQueryExecType exec_type = EXEC_ON_ALL_NODES;

                /* Check restrictions on objects dropped */
                DropStmtPreTreatment((DropStmt *) stmt, queryString,
                                     &is_temp, &exec_type);
                if(stmt->removeType == OBJECT_FOREIGN_TABLE)
                    exec_type = EXEC_ON_COORDS;
                RemoveRelations(stmt);
                /* DROP is done depending on the object type and its temporary type */
                if (IS_PGXC_LOCAL_COORDINATOR)
                    ExecUtilityStmtOnNodes(NULL, queryString, NULL, FALSE, false,
                                             exec_type, is_temp, false);
            }
            break;
        case OBJECT_EXTENSION:
            {
                RemoveObjects(stmt); 
            }
            break;
        default:
            {
                bool        is_temp = false;
                RemoteQueryExecType exec_type = EXEC_ON_ALL_NODES;

                /* Check restrictions on objects dropped */
                DropStmtPreTreatment((DropStmt *) stmt, queryString,
                                      &is_temp, &exec_type);
                RemoveObjects(stmt);
                if (IS_PGXC_LOCAL_COORDINATOR)
                    ExecUtilityStmtOnNodes(NULL, queryString, NULL, false, false,
                                     exec_type, is_temp, false);
            }
            break;
    }
}

/*
 * The "Slow" variant of ProcessUtility should only receive statements
 * supported by the event triggers facility.  Therefore, we always
 * perform the trigger support calls if the context allows it.
 */
static void
ProcessUtilitySlow(ParseState *pstate,
        PlannedStmt *pstmt,
        const char *queryString,
        ProcessUtilityContext context,
        ParamListInfo params,
        QueryEnvironment *queryEnv,
        DestReceiver *dest,
        bool sentToRemote,
        char *completionTag)
{
    Node       *parsetree = pstmt->utilityStmt;
    bool        isTopLevel = (context == PROCESS_UTILITY_TOPLEVEL);
    bool        isCompleteQuery = (context <= PROCESS_UTILITY_QUERY);
    bool        needCleanup;
    bool        commandCollected = false;
    ObjectAddress address;
    ObjectAddress secondaryObject = InvalidObjectAddress;

    /* All event trigger calls are done only when isCompleteQuery is true */
    needCleanup = isCompleteQuery && EventTriggerBeginCompleteQuery();

    /* PG_TRY block is to ensure we call EventTriggerEndCompleteQuery */
    PG_TRY();
    {
        if (isCompleteQuery)
            EventTriggerDDLCommandStart(parsetree);

        switch (nodeTag(parsetree))
        {
            /*
             * relation and attribute manipulation
             */
            case T_CreateSchemaStmt:
                CreateSchemaCommand((CreateSchemaStmt *) parsetree,
                        queryString,
                        pstmt->stmt_location,
                        pstmt->stmt_len);
                if(IS_PGXC_LOCAL_COORDINATOR && !sentToRemote)
                {
                    RemoteQuery *step = makeNode(RemoteQuery);
                    PlannedStmt *wrapper;

                    step->combine_type = COMBINE_TYPE_SAME;
                    step->sql_statement = (char *) queryString;
                    step->exec_type = EXEC_ON_ALL_NODES;

                    /* need to make a wrapper PlannedStmt */
                    wrapper = makeNode(PlannedStmt);
                    wrapper->commandType = CMD_UTILITY;
                    wrapper->canSetTag = false;
                    wrapper->utilityStmt = (Node *)step;
                    wrapper->stmt_location = pstmt->stmt_location;
                    wrapper->stmt_len = pstmt->stmt_len;

                    /* do this step */
                    ProcessUtility(wrapper,
                            queryString,
                            PROCESS_UTILITY_SUBCOMMAND,
                            NULL,
                            NULL,
                            None_Receiver,
                            NULL);
                    CommandCounterIncrement();
                }

                /*
                 * EventTriggerCollectSimpleCommand called by
                 * CreateSchemaCommand
                 */
                commandCollected = true;
                break;

            case T_CreateStmt:
            case T_CreateForeignTableStmt:
                {
                    List       *stmts;
                    ListCell   *l;
                    bool        is_temp = false;
                    bool        is_distributed = false;
                    bool        is_local = ((CreateStmt *) parsetree)->islocal;

                    /* Run parse analysis ... */
                    stmts = transformCreateStmt((CreateStmt *) parsetree,
                            queryString);

                    if (IS_PGXC_LOCAL_COORDINATOR)
                    {
                        /*
                         * Scan the list of objects.
                         * Temporary tables are created on Datanodes only.
                         * Non-temporary objects are created on all nodes.
                         * In case temporary and non-temporary objects are mized return an error.
                         */
                        bool    is_first = true;

                        foreach(l, stmts)
                        {
                            Node       *stmt = (Node *) lfirst(l);

                            if (IsA(stmt, CreateStmt))
                            {
                                CreateStmt *stmt_loc = (CreateStmt *) stmt;
                                bool is_object_temp = stmt_loc->relation->relpersistence == RELPERSISTENCE_TEMP;
                                bool is_object_distributed = stmt_loc->distributeby;

                                if (is_first)
                                {
                                    is_first = false;
                                    if (is_object_temp)
                                        is_temp = true;
                                    if (is_object_distributed)
                                        is_distributed = true;
                                }
                                else
                                {
                                    if (is_object_temp != is_temp)
                                        ereport(ERROR,
                                                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                                                 errmsg("CREATE not supported for TEMP and non-TEMP objects"),
                                                 errdetail("You should separate TEMP and non-TEMP objects")));

                                    if (is_object_distributed != is_distributed)
                                        ereport(ERROR,
                                                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                                                 errmsg("CREATE not supported for distributed and local objects"),
                                                 errdetail("You should separate distributed and local objects")));

                                }

                            }
                        }
                    }

                    /*
                     * Add a RemoteQuery node for a query at top level on a remote
                     * Coordinator, if not already done so
                     */
                    if (is_distributed)
                        stmts = AddRemoteQueryNode(stmts, queryString, is_local
                          ? EXEC_ON_NONE	 
                               :EXEC_ON_ALL_NODES);
                    
                    /* ... and do it */
                    foreach(l, stmts)
                    {
                        Node       *stmt = (Node *) lfirst(l);

                        if (IsA(stmt, CreateStmt))
                        {
                            Datum       toast_options;
                            static char *validnsps[] = HEAP_RELOPT_NAMESPACES;

                            CreateStmt *createStmt = (CreateStmt *)stmt;

                            /* Set temporary object object flag in pooler */
                            if (is_temp)
                            {
                                PoolManagerSetCommand(NULL, 0, POOL_CMD_TEMP, NULL);
                            }

                            /* Create the table itself */
                            address = DefineRelation((CreateStmt *) stmt,
                                    RELKIND_RELATION,
                                    InvalidOid, NULL,
                                    queryString);
                            if(IS_PGXC_COORDINATOR)
                            {
                                char *use_remote_estimate = "true";
                                char *server_name = PGXCClusterName;
                                char *dist_type;
                                CreateForeignTableStmt  *fstmt = makeNode(CreateForeignTableStmt);

                                fstmt->options = list_make1(makeDefElem("use_remote_estimate", 
                                            (Node *)makeString(pstrdup(use_remote_estimate)),
                                            -1));
                                fstmt->servername = pstrdup(server_name);

                                if(createStmt->distributeby->disttype == 0)
                                    dist_type = "R";
                                else if(createStmt->distributeby->disttype == 1)
                                    dist_type = "H";
                                else if(createStmt->distributeby->disttype == 2)
                                    dist_type = "N";
                                else if(createStmt->distributeby->disttype == 3)
                                    dist_type = "M";
                                else
                                    ereport(ERROR,
                                            (errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                                             errmsg("distribute type is not defined %d", createStmt->distributeby->disttype)));
                                fstmt->options = lappend(fstmt->options,
                                        makeDefElem("locator_type",
                                            (Node *)makeString(pstrdup(dist_type)),
                                            -1));

                                if(createStmt->distributeby->colname != NULL)
                                    fstmt->options = lappend(fstmt->options,
                                            makeDefElem("dist_col_name",
                                                (Node *)makeString(pstrdup(createStmt->distributeby->colname)),
                                                -1));
                                CreateForeignTable(fstmt,
                                        address.objectId);

                            }
                            EventTriggerCollectSimpleCommand(address,
                                    secondaryObject,
                                    stmt);

                            /*
                             * Let NewRelationCreateToastTable decide if this
                             * one needs a secondary relation too.
                             */
                            CommandCounterIncrement();

                            /*
                             * parse and validate reloptions for the toast
                             * table
                             */
                            toast_options = transformRelOptions((Datum) 0,
                                    ((CreateStmt *) stmt)->options,
                                    "toast",
                                    validnsps,
                                    true,
                                    false);
                            (void) heap_reloptions(RELKIND_TOASTVALUE,
                                    toast_options,
                                    true);

                            NewRelationCreateToastTable(address.objectId,
                                    toast_options);
                        }
                        else if (IsA(stmt, CreateForeignTableStmt))
                        {
                            /* Create the table itself */
                            address = DefineRelation((CreateStmt *) stmt,
                                    RELKIND_FOREIGN_TABLE,
                                    InvalidOid, NULL,
                                    queryString);
                            CreateForeignTable((CreateForeignTableStmt *) stmt,
                                    address.objectId);
                            EventTriggerCollectSimpleCommand(address,
                                    secondaryObject,
                                    stmt);
                        }
                        else
                        {
                            /*
                             * Recurse for anything else.  Note the recursive
                             * call will stash the objects so created into our
                             * event trigger context.
                             */
                            PlannedStmt *wrapper;
                            bool sentToRemote = false;

                            wrapper = makeNode(PlannedStmt);
                            wrapper->commandType = CMD_UTILITY;
                            wrapper->canSetTag = false;
                            wrapper->utilityStmt = stmt;
                            wrapper->stmt_location = pstmt->stmt_location;
                            wrapper->stmt_len = pstmt->stmt_len;
                            if(IS_PGXC_COORDINATOR && (!IsA(stmt, RemoteQuery)))
                                sentToRemote = true;

                            polarx_ProcessUtility_internal(wrapper,
                                    queryString,
                                    PROCESS_UTILITY_SUBCOMMAND,
                                    params,
                                    NULL,
                                    None_Receiver,
                                    sentToRemote,
                                    NULL);
                        }

                        /* Need CCI between commands */
                        if (lnext(l) != NULL)
                            CommandCounterIncrement();
                    }

                    /*
                     *                   * The multiple commands generated here are stashed
                     *                                       * individually, so disable collection below.
                     *                                                           */
                    commandCollected = true;
                }
                break;

            case T_AlterTableStmt:
                {
                    AlterTableStmt *atstmt = (AlterTableStmt *) parsetree;
                    Oid         relid;
                    List       *stmts;
                    ListCell   *l;
                    LOCKMODE    lockmode;

                    if(IS_PGXC_COORDINATOR)
                        AlterTablePrepCmds(atstmt);

                    /*
                     * Figure out lock mode, and acquire lock.  This also does
                     * basic permissions checks, so that we won't wait for a
                     * lock on (for example) a relation on which we have no
                     * permissions.
                     */
                    lockmode = AlterTableGetLockLevel(atstmt->cmds);
                    relid = AlterTableLookupRelation(atstmt, lockmode);

                    if (OidIsValid(relid))
                    {
                        /* Run parse analysis ... */
                        stmts = transformAlterTableStmt(relid, atstmt,
                                queryString);

                        /* ... ensure we have an event trigger context ... */
                        EventTriggerAlterTableStart(parsetree);
                        EventTriggerAlterTableRelid(relid);

                        /* ... and do it */
                        foreach(l, stmts)
                        {
                            Node       *stmt = (Node *) lfirst(l);

                            if (IsA(stmt, AlterTableStmt))
                            {
                                /* Do the table alteration proper */
                                AlterTable(relid, lockmode,
                                        (AlterTableStmt *) stmt);
                            }
                            else
                            {
                                /*
                                 * Recurse for anything else.  If we need to
                                 * do so, "close" the current complex-command
                                 * set, and start a new one at the bottom;
                                 * this is needed to ensure the ordering of
                                 * queued commands is consistent with the way
                                 * they are executed here.
                                 */
                                PlannedStmt *wrapper;

                                EventTriggerAlterTableEnd();
                                wrapper = makeNode(PlannedStmt);
                                wrapper->commandType = CMD_UTILITY;
                                wrapper->canSetTag = false;
                                wrapper->utilityStmt = stmt;
                                wrapper->stmt_location = pstmt->stmt_location;
                                wrapper->stmt_len = pstmt->stmt_len;
                                polarx_ProcessUtility_internal(wrapper,
                                        queryString,
                                        PROCESS_UTILITY_SUBCOMMAND,
                                        params,
                                        NULL,
                                        None_Receiver,
                                        true,
                                        NULL);
                                EventTriggerAlterTableStart(parsetree);
                                EventTriggerAlterTableRelid(relid);
                            }

                            /* Need CCI between commands */
                            if (lnext(l) != NULL)
                                CommandCounterIncrement();
                        }

                        /* done */
                        EventTriggerAlterTableEnd();
                    }
                    else
                        ereport(NOTICE,
                                (errmsg("relation \"%s\" does not exist, skipping",
                                        atstmt->relation->relname)));
                }

                /* ALTER TABLE stashes commands internally */
                commandCollected = true;
                break;
            case T_DropStmt:
                ExecDropStmt((DropStmt *) parsetree, queryString, isTopLevel);
                /* no commands stashed for DROP */
                commandCollected = true;
                break;
            case T_CreateTableAsStmt:
                address = ExecCreateTableAsReplace((CreateTableAsStmt *) parsetree,
                                                queryString, params, queryEnv,
                                                completionTag);
                break;
            default:
                break;
        }

        /*
         * Remember the object so that ddl_command_end event triggers have
         * access to it.
         */
        if (!commandCollected)
            EventTriggerCollectSimpleCommand(address, secondaryObject,
                    parsetree);

        if (isCompleteQuery)
        {
            EventTriggerSQLDrop(parsetree);
            EventTriggerDDLCommandEnd(parsetree);
        }
    }
    PG_CATCH();
    {
        if (needCleanup)
            EventTriggerEndCompleteQuery();
        PG_RE_THROW();
    }
    PG_END_TRY();

    if (needCleanup)
        EventTriggerEndCompleteQuery();
}
static void
check_xact_readonly(Node *parsetree)
{
    /* Only perform the check if we have a reason to do so. */
    if (!XactReadOnly && !IsInParallelMode())
        return;

    /*
     * Note: Commands that need to do more complicated checking are handled
     * elsewhere, in particular COPY and plannable statements do their own
     * checking.  However they should all call PreventCommandIfReadOnly or
     * PreventCommandIfParallelMode to actually throw the error.
     */

    switch (nodeTag(parsetree))
    {
        case T_AlterDatabaseStmt:
        case T_AlterDatabaseSetStmt:
        case T_AlterDomainStmt:
        case T_AlterFunctionStmt:
        case T_AlterRoleStmt:
        case T_AlterRoleSetStmt:
        case T_AlterObjectDependsStmt:
        case T_AlterObjectSchemaStmt:
        case T_AlterOwnerStmt:
        case T_AlterOperatorStmt:
        case T_AlterSeqStmt:
        case T_AlterTableMoveAllStmt:
        case T_AlterTableStmt:
        case T_RenameStmt:
        case T_CommentStmt:
        case T_DefineStmt:
        case T_CreateCastStmt:
        case T_CreateEventTrigStmt:
        case T_AlterEventTrigStmt:
        case T_CreateConversionStmt:
        case T_CreatedbStmt:
        case T_CreateDomainStmt:
        case T_CreateFunctionStmt:
        case T_CreateRoleStmt:
        case T_IndexStmt:
        case T_CreatePLangStmt:
        case T_CreateOpClassStmt:
        case T_CreateOpFamilyStmt:
        case T_AlterOpFamilyStmt:
        case T_RuleStmt:
        case T_CreateSchemaStmt:
        case T_CreateSeqStmt:
        case T_CreateStmt:
        case T_CreateTableAsStmt:
        case T_RefreshMatViewStmt:
        case T_CreateTableSpaceStmt:
        case T_CreateTransformStmt:
        case T_CreateTrigStmt:
        case T_CompositeTypeStmt:
        case T_CreateEnumStmt:
        case T_CreateRangeStmt:
        case T_AlterEnumStmt:
        case T_ViewStmt:
        case T_DropStmt:
        case T_DropdbStmt:
        case T_DropTableSpaceStmt:
        case T_DropRoleStmt:
        case T_GrantStmt:
        case T_GrantRoleStmt:
        case T_AlterDefaultPrivilegesStmt:
        case T_TruncateStmt:
        case T_DropOwnedStmt:
        case T_ReassignOwnedStmt:
        case T_AlterTSDictionaryStmt:
        case T_AlterTSConfigurationStmt:
        case T_CreateExtensionStmt:
        case T_AlterExtensionStmt:
        case T_AlterExtensionContentsStmt:
        case T_CreateFdwStmt:
        case T_AlterFdwStmt:
        case T_CreateForeignServerStmt:
        case T_AlterForeignServerStmt:
        case T_CreateUserMappingStmt:
        case T_AlterUserMappingStmt:
        case T_DropUserMappingStmt:
        case T_AlterTableSpaceOptionsStmt:
        case T_CreateForeignTableStmt:
        case T_ImportForeignSchemaStmt:
        case T_SecLabelStmt:
        case T_CreatePublicationStmt:
        case T_AlterPublicationStmt:
        case T_CreateSubscriptionStmt:
        case T_AlterSubscriptionStmt:
        case T_DropSubscriptionStmt:
            PreventCommandIfReadOnly(CreateCommandTag(parsetree));
            PreventCommandIfParallelMode(CreateCommandTag(parsetree));
            break;
        default:
            /* do nothing */
            break;
    }
}

static void
ProcessUtilityReplace(PlannedStmt *pstmt,
        const char *queryString,
        ProcessUtilityContext context,
        ParamListInfo params,
        QueryEnvironment *queryEnv,
        DestReceiver *dest,
        bool sentToRemote,
        char *completionTag)
{
    Node       *parsetree = pstmt->utilityStmt;
    bool        isTopLevel = (context == PROCESS_UTILITY_TOPLEVEL);
    ParseState *pstate;

    /* This can recurse, so check for excessive recursion */
    check_stack_depth();

    check_xact_readonly(parsetree);

    if (completionTag)
        completionTag[0] = '\0';

    pstate = make_parsestate(NULL);
    pstate->p_sourcetext = queryString;

    switch (nodeTag(parsetree))
    {
        /*
         * ******************** transactions ********************
         */
        case T_CreateSchemaStmt:
        case T_CreateStmt:
        case T_CreateForeignTableStmt:
        case T_AlterTableStmt:
            ProcessUtilitySlow(pstate, pstmt, queryString,
                    context, params, queryEnv,
                    dest, sentToRemote, completionTag);
            break;
        case T_DropStmt:
            {
                DropStmt   *stmt = (DropStmt *) parsetree;

                if (EventTriggerSupportsObjectType(stmt->removeType))
                    ProcessUtilitySlow(pstate, pstmt, queryString,
                            context, params, queryEnv,
                            dest, sentToRemote, completionTag);
                else
                    ExecDropStmt(stmt, queryString, isTopLevel);
            }
            break;
        case T_CreateTableAsStmt:
            ProcessUtilitySlow(pstate, pstmt, queryString,
                               context, params, queryEnv,
                                 dest, sentToRemote, completionTag);
            break;
        default:
            standard_ProcessUtility(pstmt, queryString, context,
                    params, queryEnv, dest, completionTag);
    }
    free_parsestate(pstate);
}
void
polarx_fmgr_hook(FmgrHookEventType event,
                                FmgrInfo *flinfo, Datum *arg)
{
    polarx_fmgr_cache *fcache = (polarx_fmgr_cache *)flinfo;
    if(!fcache->proconfig)
        return;

    switch (event)
    {
        case FHET_START:
            PolarxProcessGUCArray(fcache->proconfig, PGC_S_SESSION, GUC_ACTION_SAVE);
            break;
        case FHET_END:
            PolarxSetBackGUCArray(fcache->proconfig, PGC_S_SESSION, GUC_ACTION_SAVE);
            break;
        case FHET_ABORT:
            break;
    }

}

static void
set_exec_utility_local(bool value)
{
    if(value)
        set_config_option("polarx.set_exec_utility_local", "on",
                PGC_USERSET, PGC_S_SESSION,
                GUC_ACTION_LOCAL, true, 0, false);
    else
        set_config_option("polarx.set_exec_utility_local", "off",
                PGC_USERSET, PGC_S_SESSION,
                GUC_ACTION_LOCAL, true, 0, false);
}
