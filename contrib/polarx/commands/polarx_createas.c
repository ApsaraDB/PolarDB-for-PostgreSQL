/*-------------------------------------------------------------------------
 * polarx_createas.c
 *   The implementation of the CREATE TABLE AS command in polarx
 *
 * Copyright (c) 2020, Alibaba Inc. and/or its affiliates
 * Copyright (c) 2020, Apache License Version 2.0*
 *
 * IDENTIFICATION
 *        contrib/polarx/commands/polarx_createas.c
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "commands/polarx_createas.h"
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
#include "pgxc/pgxc.h"
#include "pgxc/planner.h"
#include "commands/defrem.h"
#include "deparse/deparse_fqs.h"

static ObjectAddress create_ctas_foreign_table(List *tlist, IntoClause *into);

void
ExecCreateTableAsPre(CreateTableAsStmt *stmt)
{
}
void
ExecCreateTableAsPost(CreateTableAsStmt *stmt)
{
}

ObjectAddress
ExecCreateTableAsReplace(CreateTableAsStmt *stmt, const char *queryString,
                        ParamListInfo params, QueryEnvironment *queryEnv,
                        char *completionTag)
{
    Query      *query = castNode(Query, stmt->query);
    IntoClause *into = stmt->into;
    bool        is_matview = (into->viewQuery != NULL);
    ObjectAddress address;
    List       *rewritten;
    PlannedStmt *plan;
    QueryDesc  *queryDesc;

    if (stmt->if_not_exists)
    {
        Oid         nspid;

        nspid = RangeVarGetCreationNamespace(stmt->into->rel);

        if (get_relname_relid(stmt->into->rel->relname, nspid))
        {
            ereport(NOTICE,
                    (errcode(ERRCODE_DUPLICATE_TABLE),
                     errmsg("relation \"%s\" already exists, skipping",
                         stmt->into->rel->relname)));
            return InvalidObjectAddress;
        }
    }
    if(IS_PGXC_COORDINATOR && !is_matview)
    {

        address = create_ctas_foreign_table(query->targetList, into);
        CommandCounterIncrement();

        if(!into->skipData && IS_PGXC_LOCAL_COORDINATOR)
        {
            StringInfoData cquery;
            char *selectstr;
            List *raw_parsetree_list;

            initStringInfo(&cquery);
            polarx_deparse_query((Query *)query, &cquery, NIL, false, false);
            selectstr = pstrdup(cquery.data);

            initStringInfo(&cquery);
            appendStringInfo(&cquery, "INSERT INTO %s.%s",
                    quote_identifier(get_namespace_name(RangeVarGetCreationNamespace(into->rel))),
                    quote_identifier(into->rel->relname));
            appendStringInfo(&cquery, " %s", selectstr);
            raw_parsetree_list = pg_parse_query(cquery.data);
            rewritten = pg_analyze_and_rewrite(linitial(raw_parsetree_list), cquery.data, NULL, 0, NULL);
            /* SELECT should never rewrite to more or less than one SELECT query */
            if (list_length(rewritten) != 1)
                elog(ERROR, "unexpected rewrite result for %s", "CREATE TABLE AS SELECT");

            query = linitial_node(Query, rewritten);

            /* plan the query */
            plan = pg_plan_query(query, CURSOR_OPT_PARALLEL_OK, params);

            /*
             * Use a snapshot with an updated command ID to ensure this query sees
             * results of any previously executed queries.  (This could only
             * matter if the planner executed an allegedly-stable function that
             * changed the database contents, but let's do it anyway to be
             * parallel to the EXPLAIN code path.)
             */
            PushCopiedSnapshot(GetActiveSnapshot());
            UpdateActiveSnapshotCommandId();

            /* Create a QueryDesc, redirecting output to our tuple receiver */
            queryDesc = CreateQueryDesc(plan, queryString,
                    GetActiveSnapshot(), InvalidSnapshot,
                    None_Receiver, params, queryEnv, 0);

            /* call ExecutorStart to prepare the plan for execution */
            ExecutorStart(queryDesc, GetIntoRelEFlags(into));

            /* run the plan to completion */
            ExecutorRun(queryDesc, ForwardScanDirection, 0L, true);

            /* save the rowcount if we're given a completionTag to fill */
            if (completionTag)
                snprintf(completionTag, COMPLETION_TAG_BUFSIZE,
                        "SELECT " UINT64_FORMAT,
                        queryDesc->estate->es_processed);

            /* and clean up */
            ExecutorFinish(queryDesc);
            ExecutorEnd(queryDesc);

            FreeQueryDesc(queryDesc);

            PopActiveSnapshot();
        }
    }
    else
        address = ExecCreateTableAs(stmt, queryString,
                                    params, queryEnv, completionTag);
    return address;
}

static ObjectAddress
create_ctas_foreign_table(List *tlist, IntoClause *into)
{
    List       *attrList;
    ListCell   *t, *lc;
    CreateForeignTableStmt *fstmt = NULL;
    char *dist_type;
    char *use_remote_estimate = "true";
    char *server_name = PGXCClusterName;
    char *dist_name = NULL;
    ObjectAddress intoRelationAddr;


    /*
     * Build list of ColumnDefs from non-junk elements of the tlist.  If a
     * column name list was specified in CREATE TABLE AS, override the column
     * names in the query.  (Too few column names are OK, too many are not.)
     */
    attrList = NIL;
    lc = list_head(into->colNames);
    foreach(t, tlist)
    {
        TargetEntry *tle = (TargetEntry *) lfirst(t);

        if (!tle->resjunk)
        {
            ColumnDef  *col;
            char       *colname;

            if (lc)
            {
                colname = strVal(lfirst(lc));
                lc = lnext(lc);
            }
            else
                colname = tle->resname;

            col = makeColumnDef(colname,
                    exprType((Node *) tle->expr),
                    exprTypmod((Node *) tle->expr),
                    exprCollation((Node *) tle->expr));

            /*
             * It's possible that the column is of a collatable type but the
             * collation could not be resolved, so double-check.  (We must
             * check this here because DefineRelation would adopt the type's
             * default collation rather than complaining.)
             */
            if (!OidIsValid(col->collOid) &&
                    type_is_collatable(col->typeName->typeOid))
                ereport(ERROR,
                        (errcode(ERRCODE_INDETERMINATE_COLLATION),
                         errmsg("no collation was derived for column \"%s\" with collatable type %s",
                             col->colname,
                             format_type_be(col->typeName->typeOid)),
                         errhint("Use the COLLATE clause to set the collation explicitly.")));

            attrList = lappend(attrList, col);
        }
    }

    if (lc != NULL)
        ereport(ERROR,
                (errcode(ERRCODE_SYNTAX_ERROR),
                 errmsg("too many column names were specified")));
    fstmt = makeNode(CreateForeignTableStmt);

    fstmt->servername = pstrdup(server_name);
    fstmt->options = list_make1(
            makeDefElem("use_remote_estimate", (Node *)makeString(pstrdup(use_remote_estimate)), -1));

    if(into->distributeby)
    {
        if(into->distributeby->disttype == 0)
            dist_type = "R";
        else if(into->distributeby->disttype == 1)
            dist_type = "H";
        else if(into->distributeby->disttype == 2)
            dist_type = "N";
        else if(into->distributeby->disttype == 3)
            dist_type = "M";
        else
            ereport(ERROR,
                    (errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                     errmsg("distribute type is not defined %d", into->distributeby->disttype)));

        if(into->distributeby->colname != NULL)
            dist_name = into->distributeby->colname;
    }
    else
    {
        lc = NULL;

        foreach(lc, attrList)
        {
            ColumnDef  *col = (ColumnDef *)lfirst(lc);
            if(IsTypeHashDistributable(col->typeName->typeOid))
            {
                dist_type = "H";
                dist_name = col->colname;
                break;
            }
        }
        if(lc == NULL)
        {
            dist_type = "N";
            dist_name = NULL;
        }
    }
    fstmt->options = lappend(fstmt->options,
            makeDefElem("locator_type", (Node *)makeString(pstrdup(dist_type)), -1));

    if(dist_name != NULL)
        fstmt->options = lappend(fstmt->options,
                makeDefElem("dist_col_name", (Node *)makeString(pstrdup(dist_name)), -1));
    {
        CreateStmt *create = makeNode(CreateStmt);
        bool        is_matview;
        char        relkind;
        Datum       toast_options;
        static char *validnsps[] = HEAP_RELOPT_NAMESPACES;

        /* This code supports both CREATE TABLE AS and CREATE MATERIALIZED VIEW */
        is_matview = (into->viewQuery != NULL);
        relkind = is_matview ? RELKIND_MATVIEW : RELKIND_RELATION;

        /*
         * Create the target relation by faking up a CREATE TABLE parsetree and
         * passing it to DefineRelation.
         */
        create->relation = into->rel;
        create->tableElts = attrList;
        create->inhRelations = NIL;
        create->ofTypename = NULL;
        create->constraints = NIL;
        create->options = into->options;
        create->oncommit = into->onCommit;
        create->tablespacename = into->tableSpaceName;
        create->if_not_exists = false;
        create->distributeby = makeNode(DistributeBy);
        if(into->distributeby)
            create->distributeby = copyObject(into->distributeby);
        else
        {
            if(*dist_type == 'H')
            {
                create->distributeby->disttype = DISTTYPE_HASH;
                create->distributeby->colname = pstrdup(dist_name);
            }
            else
            {
                create->distributeby->disttype = DISTTYPE_ROUNDROBIN;
                create->distributeby->colname = NULL;
            }
        }

        /*
         * Create the relation.  (This will error out if there's an existing view,
         * so we don't need more code to complain if "replace" is false.)
         */
        intoRelationAddr = DefineRelation(create, relkind, InvalidOid, NULL, NULL);

        /*
         * If necessary, create a TOAST table for the target table.  Note that
         * NewRelationCreateToastTable ends with CommandCounterIncrement(), so
         * that the TOAST table will be visible for insertion.
         */
        CommandCounterIncrement();

        /* parse and validate reloptions for the toast table */
        toast_options = transformRelOptions((Datum) 0,
                create->options,
                "toast",
                validnsps,
                true, false);

        (void) heap_reloptions(RELKIND_TOASTVALUE, toast_options, true);

        NewRelationCreateToastTable(intoRelationAddr.objectId, toast_options);

        /* Create the "view" part of a materialized view. */
        if (is_matview)
        {
            /* StoreViewQuery scribbles on tree, so make a copy */
            Query      *query = (Query *) copyObject(into->viewQuery);

            StoreViewQuery(intoRelationAddr.objectId, query, false);
            CommandCounterIncrement();
        }
    }
    CreateForeignTable((CreateForeignTableStmt *) fstmt, intoRelationAddr.objectId);


    /* Create the relation definition using the ColumnDef list */
    return intoRelationAddr;
}

