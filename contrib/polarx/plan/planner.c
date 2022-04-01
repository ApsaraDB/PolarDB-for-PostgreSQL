/*-------------------------------------------------------------------------
 *
 * planner.c
 *
 *      Functions for generating a PGXC style plan.
 *
 * Copyright (c) 2021, Alibaba Group Holding Limited
 * Licensed under the Apache License, Version 2.0 (the "License");
 * Portions Copyright (c) 2012-2014, TransLattice, Inc.
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 * IDENTIFICATION
 *        contrib/polarx/plan/planner.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "polarx.h"
#include "miscadmin.h"
#include "access/transam.h"
#include "catalog/pg_aggregate.h"
#include "catalog/pg_class.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_type.h"
#include "commands/prepare.h"
#include "executor/executor.h"
#include "lib/stringinfo.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "nodes/nodes.h"
#include "nodes/parsenodes.h"
#include "optimizer/clauses.h"
#include "optimizer/cost.h"
#include "optimizer/planmain.h"
#include "optimizer/planner.h"
#include "optimizer/tlist.h"
#include "parser/parse_agg.h"
#include "parser/parse_func.h"
#include "parser/parse_relation.h"
#include "parser/parsetree.h"
#include "parser/parse_oper.h"
#include "parser/analyze.h"
#include "pgxc/locator.h"
#include "pgxc/nodemgr.h"
#include "plan/polarx_planner.h"
#include "tcop/pquery.h"
#include "utils/acl.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/portal.h"
#include "utils/syscache.h"
#include "utils/numeric.h"
#include "utils/memutils.h"
#include "access/hash.h"
#include "commands/polarx_tablecmds.h"
#include "utils/timestamp.h"
#include "utils/date.h"
#include "access/sysattr.h"
#include "catalog/pg_attribute.h"
#include "optimizer/var.h"
#include "access/htup_details.h"
#include "optimizer/prep.h"
#include "plan/pgxcship.h"
#include "optimizer/restrictinfo.h"
#include "utils/datum.h"
#include "optimizer/clauses.h"
#include <float.h>
#include "executor/execRemoteQuery.h"
#include "deparse/deparse_fqs.h"
#include "utils/fdwplanner_utils.h"
#include "nodes/polarx_node.h"

bool EnableFastQueryShipping = false;

static bool contains_temp_tables(List *rtable);
static PlannedStmt *pgxc_FQS_planner(Query *query, int cursorOptions,
                                     ParamListInfo boundParams);
static RemoteQuery *pgxc_FQS_create_remote_plan(Query *query,
                                                ExecNodes *exec_nodes,
                                                bool is_exec_direct);
static CombineType get_plan_combine_type(CmdType commandType, char baselocatortype);

static void fqs_preprocess_rowmarks(PlannerInfo *root);
static List *extractResjunkTarget(List *src_list);




/*
 * Returns true if at least one temporary table is in use
 * in query (and its subqueries)
 */
static bool
contains_temp_tables(List *rtable)
{
    ListCell *item;

    foreach(item, rtable)
    {
        RangeTblEntry *rte = (RangeTblEntry *) lfirst(item);

        if (rte->rtekind == RTE_RELATION)
        {
            if (IsTempTable(rte->relid))
                return true;
        }
        else if (rte->rtekind == RTE_SUBQUERY &&
                 contains_temp_tables(rte->subquery->rtable))
            return true;
    }

    return false;
}

/*
 * get_plan_combine_type - determine combine type
 *
 * COMBINE_TYPE_SAME - for replicated updates
 * COMBINE_TYPE_SUM - for hash and round robin updates
 * COMBINE_TYPE_NONE - for operations where row_count is not applicable
 *
 * return NULL if it is not safe to be done in a single step.
 */
static CombineType
get_plan_combine_type(CmdType commandType, char baselocatortype)
{

    switch (commandType)
    {
        case CMD_INSERT:
        case CMD_UPDATE:
        case CMD_DELETE:
            return baselocatortype == LOCATOR_TYPE_REPLICATED ?
                    COMBINE_TYPE_SAME : COMBINE_TYPE_SUM;

        default:
            return COMBINE_TYPE_NONE;
    }
    /* quiet compiler warning */
    return COMBINE_TYPE_NONE;
}


/*
 * Subroutine for eval_const_expressions: check for non-Const nodes.
 *
 * We can abort recursion immediately on finding a non-Const node.  This is
 * critical for performance, else eval_const_expressions_mutator would take
 * O(N^2) time on non-simplifiable trees.  However, we do need to descend
 * into List nodes since expression_tree_walker sometimes invokes the walker
 * function directly on List subtrees.
 */
static bool
contain_non_const_walker(Node *node, void *context)
{
	if (node == NULL)
		return false;
	if (IsA(node, Const))
		return false;
	if (IsA(node, List))
		return expression_tree_walker(node, contain_non_const_walker, context);
	/* Otherwise, abort the tree traversal and return true */
	return true;
}

/*
 * evaluate_expr: pre-evaluate a constant expression
 *
 * We use the executor's routine ExecEvalExpr() to avoid duplication of
 * code and ensure we get the same result as the executor would get.
 */
static Expr *
polardb_evaluate_expr(Expr *expr, Oid result_type, int32 result_typmod,
			  Oid result_collation)
{
	EState	   *estate;
	ExprState  *exprstate;
	MemoryContext oldcontext;
	Datum		const_val;
	bool		const_is_null;
	int16		resultTypLen;
	bool		resultTypByVal;

	/*
	 * To use the executor, we need an EState.
	 */
	estate = CreateExecutorState();

	/* We can use the estate's working context to avoid memory leaks. */
	oldcontext = MemoryContextSwitchTo(estate->es_query_cxt);

	/* Make sure any opfuncids are filled in. */
	fix_opfuncids((Node *) expr);

	/*
	 * Prepare expr for execution.  (Note: we can't use ExecPrepareExpr
	 * because it'd result in recursively invoking eval_const_expressions.)
	 */
	exprstate = ExecInitExpr(expr, NULL);

	/*
	 * And evaluate it.
	 *
	 * It is OK to use a default econtext because none of the ExecEvalExpr()
	 * code used in this situation will use econtext.  That might seem
	 * fortuitous, but it's not so unreasonable --- a constant expression does
	 * not depend on context, by definition, n'est ce pas?
	 */
	const_val = ExecEvalExprSwitchContext(exprstate,
										  GetPerTupleExprContext(estate),
										  &const_is_null);

	/* Get info needed about result datatype */
	get_typlenbyval(result_type, &resultTypLen, &resultTypByVal);

	/* Get back to outer memory context */
	MemoryContextSwitchTo(oldcontext);

	/*
	 * Must copy result out of sub-context used by expression eval.
	 *
	 * Also, if it's varlena, forcibly detoast it.  This protects us against
	 * storing TOAST pointers into plans that might outlive the referenced
	 * data.  (makeConst would handle detoasting anyway, but it's worth a few
	 * extra lines here so that we can do the copy and detoast in one step.)
	 */
	if (!const_is_null)
	{
		if (resultTypLen == -1)
			const_val = PointerGetDatum(PG_DETOAST_DATUM_COPY(const_val));
		else
			const_val = datumCopy(const_val, resultTypByVal, resultTypLen);
	}

	/* Release all the junk we just created */
	FreeExecutorState(estate);

	/*
	 * Make the constant result node.
	 */
	return (Expr *) makeConst(result_type, result_typmod, result_collation,
							  resultTypLen,
							  const_val, const_is_null,
							  resultTypByVal);
}


/*
 * Check whether all arguments of the given node were reduced to Consts.
 * By going directly to expression_tree_walker, contain_non_const_walker
 * is not applied to the node itself, only to its children.
 */
#define epe_all_arguments_const(node) \
	(!expression_tree_walker((Node *) (node), contain_non_const_walker, NULL))


/* Generic macro for applying evaluate_expr */
#define epe_evaluate_expr(node) \
	((Node *) polardb_evaluate_expr((Expr *) (node), \
							exprType((Node *) (node)), \
							exprTypmod((Node *) (node)), \
							exprCollation((Node *) (node))))

#define epe_generic_processing(node) \
	expression_tree_mutator((Node *) (node), eval_param_expressions_mutator, \
							(void *) context)

typedef struct
{
	ParamListInfo boundParams;
	bool hasUndeterminedParams;
} eval_param_expressions_context;

static Node *
eval_param_expressions_mutator(Node *node, eval_param_expressions_context *context);

static Node *
eval_param_expressions_mutator(Node *node, eval_param_expressions_context *context)
{

	if (node == NULL)
	{
		return NULL;
	}

	switch (nodeTag(node))
	{

		case T_Param:
			{
				Param *param = (Param *) node;
				ParamListInfo paramLI = context->boundParams;

				if (param->paramkind == PARAM_EXTERN &&
							paramLI != NULL &&
							param->paramid > 0 &&
							param->paramid <= paramLI->numParams)
				{
					ParamExternData *prm;
					ParamExternData prmdata;

					/*
					* Give hook a chance in case parameter is dynamic.  Tell
					* it that this fetch is speculative, so it should avoid
					* erroring out if parameter is unavailable.
					*/
					if (paramLI->paramFetch != NULL)
						prm = paramLI->paramFetch(paramLI, param->paramid,
													true, &prmdata);
					else
						prm = &paramLI->params[param->paramid - 1];

					/*
					* We don't just check OidIsValid, but insist that the
					* fetched type match the Param, just in case the hook did
					* something unexpected.  No need to throw an error here
					* though; leave that for runtime.
					*/
					if (OidIsValid(prm->ptype) &&
						prm->ptype == param->paramtype)
					{
						/* OK to substitute parameter value? */
						if (prm->pflags & PARAM_FLAG_CONST)
						{
							/*
							* Return a Const representing the param value.
							* Must copy pass-by-ref datatypes, since the
							* Param might be in a memory context
							* shorter-lived than our output plan should be.
							*/
							int16		typLen;
							bool		typByVal;
							Datum		pval;

							get_typlenbyval(param->paramtype,
											&typLen, &typByVal);
							if (prm->isnull || typByVal)
								pval = prm->value;
							else
								pval = datumCopy(prm->value, typByVal, typLen);
							return (Node *) makeConst(param->paramtype,
														param->paramtypmod,
														param->paramcollid,
														(int) typLen,
														pval,
														prm->isnull,
														typByVal);
						}
					}
				}
                if (param->paramkind == PARAM_EXTERN)
				    context->hasUndeterminedParams = true;
				return (Node *) copyObject(param);
				
			}
			break;
		case T_Query:
			{
				return (Node *) query_tree_mutator((Query *) node, eval_param_expressions_mutator,
												context, 0);
			}
			break;
		case T_ArrayRef:
		case T_ArrayExpr:
		case T_RowExpr:
			{
				/*
					* Generic handling for node types whose own processing is
					* known to be immutable, and for which we need no smarts
					* beyond "simplify if all inputs are constants".
					*/

				/* Copy the node and const-simplify its arguments */
				node = epe_generic_processing(node);
				/* If all arguments are Consts, we can fold to a constant */
				if (epe_all_arguments_const(node))
					return epe_evaluate_expr(node);
				return node;
			}
			break;
		default:
			break;
	}

	return expression_tree_mutator(node, eval_param_expressions_mutator, context);
} 

static Query * replace_param_for_query(Query *query, eval_param_expressions_context *context)
{
	return (Query *) eval_param_expressions_mutator((Node *)query, context);
}

/*
 * This function is consistent with exec_simple_check_plan
 */ 
static bool
IsSimpleQuery(Query *query)
{

	if (query->rtable)
		return false;

	/*
	 * Can't have any subplans, aggregates, qual clauses either.  (These
	 * tests should generally match what inline_function() checks before
	 * inlining a SQL function; otherwise, inlining could change our
	 * conclusion about whether an expression is simple, which we don't want.)
	 */
	if (query->hasAggs ||
		query->hasWindowFuncs ||
		query->hasTargetSRFs ||
		query->hasSubLinks ||
		query->cteList ||
		query->jointree->fromlist ||
		query->jointree->quals ||
		query->groupClause ||
		query->groupingSets ||
		query->havingQual ||
		query->windowClause ||
		query->distinctClause ||
		query->sortClause ||
		query->limitOffset ||
		query->limitCount ||
		query->setOperations)
		return false;

	/*
	 * The query must have a single attribute as result
	 */
	if (list_length(query->targetList) != 1)
		return false;
	
	return true;
}


/*
 * Build up a QueryPlan to execute on.
 *
 * This functions tries to find out whether
 * 1. The statement can be shipped to the Datanode and Coordinator is needed
 *    only as a proxy - in which case, it creates a single node plan.
 * 2. The statement can be evaluated on the Coordinator completely - thus no
 *    query shipping is involved and standard_planner() is invoked to plan the
 *    statement
 * 3. The statement needs Coordinator as well as Datanode for evaluation -
 *    again we use standard_planner() to plan the statement.
 *
 * The plan generated in either of the above cases is returned.
 */
#define  MAX_QUERIES_PER_STMT  (1 << 20)    
Cost	 generic_plan_cost = (FLT_MAX/MAX_QUERIES_PER_STMT);

PlannedStmt *
polarx_planner(Query *query, int cursorOptions, ParamListInfo boundParams)
{
    PlannedStmt *result;
    List *rangeTableList = NULL;
    bool    isGenericPlan = false;

    InitMultinodeExecutor(false);
    if(IS_PGXC_LOCAL_COORDINATOR)
    {
        eval_param_expressions_context context;	

        context.boundParams = boundParams;
        context.hasUndeterminedParams = false;
        /* replace extern params with values first to simplfy distributed planning */
        query = replace_param_for_query(query, &context);
        /*
         * First check params to find whether it is to generate a generic plan
         * of which boundParams would be set to NULL while extern params exist.
         * We set planTree->total_cost to an extremely large value to 
         * make generic plan always not choosen in choose_custom_plan,
         * so as to support distributed stored procedures and extended protocols.
         */ 
        if (!boundParams && context.hasUndeterminedParams)
            isGenericPlan = true;
        
        if(EnableFastQueryShipping && !IsSimpleQuery(query))
        {
            if (!context.hasUndeterminedParams)
            {
                 /* see if can ship the query completely */
                result = pgxc_FQS_planner(query, cursorOptions, boundParams);
                if(result != NULL)
                    return result;
            }
        }
    }

    ExtractRelationRangeTableList((Node *)query, &rangeTableList);
    if(rangeTableList)
        AdjustRelationToForeignTable(rangeTableList);
    result = standard_planner(query, cursorOptions, boundParams);

    if (isGenericPlan)
        result->planTree->total_cost = generic_plan_cost;

    return result;
}

/*
 * pgxc_FQS_planner
 * The routine tries to see if the statement can be completely evaluated on the
 * Datanodes. In such cases Coordinator is not needed to evaluate the statement,
 * and just acts as a proxy. A statement can be completely shipped to the remote
 * node if every row of the result can be evaluated on a single Datanode.
 * For example:
 *
 * 1. SELECT * FROM tab1; where tab1 is a distributed table - Every row of the
 * result set can be evaluated at a single Datanode. Hence this statement is
 * completely shippable even though many Datanodes are involved in evaluating
 * complete result set. In such case Coordinator will be able to gather rows
 * arisign from individual Datanodes and proxy the result to the client.
 *
 * 2. SELECT count(*) FROM tab1; where tab1 is a distributed table - there is
 * only one row in the result but it needs input from all the Datanodes. Hence
 * this is not completely shippable.
 *
 * 3. SELECT count(*) FROM tab1; where tab1 is replicated table - since result
 * can be obtained from a single Datanode, this is a completely shippable
 * statement.
 *
 * fqs in the name of function is acronym for fast query shipping.
 */
static PlannedStmt *
pgxc_FQS_planner(Query *query, int cursorOptions, ParamListInfo boundParams)
{// #lizard forgives
    PlannedStmt        *result;
    PlannerGlobal    *glob;
    PlannerInfo        *root;
    ExecNodes        *exec_nodes;
    Plan            *top_plan;
    List            *tlist = query->targetList;
    CustomScan *customScan = makeNode(CustomScan);
    RemoteQuery *query_remote;

    /* Try by-passing standard planner, if fast query shipping is enabled */
    if (!EnableFastQueryShipping)
        return NULL;

    /* Do not FQS cursor statements that require backward scrolling */
    if (cursorOptions & CURSOR_OPT_SCROLL)
        return NULL;
    
    if ((MyXactFlags & XACT_FLAGS_ACCESSEDTEMPREL))
        return NULL;

    /* Do not FQS EXEC DIRECT statements */
    if (query->utilityStmt && polarxIsA(query->utilityStmt, RemoteQuery))
    {
        RemoteQuery *stmt = (RemoteQuery *) query->utilityStmt;
        if (stmt->exec_direct_type != EXEC_DIRECT_NONE)
            return NULL;
    }

    /*
     * If the query can not be or need not be shipped to the Datanodes, don't
     * create any plan here. standard_planner() will take care of it.
     */
 
    exec_nodes = polarx_is_query_shippable(query, 0);
    if (exec_nodes == NULL)
        return NULL;

    glob = makeNode(PlannerGlobal);
    glob->boundParams = boundParams;
    /* Create a PlannerInfo data structure, usually it is done for a subquery */
    root = makeNode(PlannerInfo);
    root->parse = query;
    root->glob = glob;
    root->query_level = 1;
    root->planner_cxt = CurrentMemoryContext;
    tlist = preprocess_targetlist(root);
    fqs_preprocess_rowmarks(root);

    /*
     * We decided to ship the query to the Datanode/s, create a RemoteQuery node
     * for the same.
     */
    query_remote = pgxc_FQS_create_remote_plan(query, exec_nodes, false);
    top_plan = (Plan *)(&(query_remote->scan));
    top_plan->targetlist = extractResjunkTarget(query->targetList);
    /*
     * Just before creating the PlannedStmt, do some final cleanup
     * We need to save plan dependencies, so that dropping objects will
     * invalidate the cached plan if it depends on those objects. Table
     * dependencies are available in glob->relationOids and all other
     * dependencies are in glob->invalItems. These fields can be retrieved
     * through set_plan_references().
     */
    set_plan_references(root, NULL);

    customScan->methods = &FastShipQueryExecutorCustomScanMethods;
    customScan->custom_private = list_make1((Node *)query_remote);
    customScan->custom_scan_tlist = tlist;
    customScan->scan.plan.targetlist = makeCustomScanVarTargetlistBasedTargetEntryList(tlist);
    /* build the PlannedStmt result */
    result = makeNode(PlannedStmt);
    /* Try and set what we can, rest must have been zeroed out by makeNode() */
    result->commandType = query->commandType;
    result->canSetTag = query->canSetTag;
    result->utilityStmt = query->utilityStmt;

    /* Set result relations */
    if (query->commandType != CMD_SELECT)
        result->resultRelations = list_make1_int(query->resultRelation);
    result->planTree = (Plan *)customScan;
    result->rtable = query->rtable;
    result->queryId = query->queryId;
    result->relationOids = glob->relationOids;
    result->invalItems = glob->invalItems;
    result->rowMarks = glob->finalrowmarks;

    return result;
}

static RemoteQuery *
pgxc_FQS_create_remote_plan(Query *query, ExecNodes *exec_nodes, bool is_exec_direct)
{
    RemoteQuery *query_step;
    StringInfoData buf;

    /* EXECUTE DIRECT statements have their RemoteQuery node already built when analyzing */
    if (is_exec_direct)
    {
        Assert(polarxIsA(query->utilityStmt, RemoteQuery));
        query_step = (RemoteQuery *)query->utilityStmt;
        query->utilityStmt = NULL;
    }
    else
    {
        query_step = polarxMakeNode(RemoteQuery);
        query_step->combine_type = COMBINE_TYPE_NONE;
        query_step->exec_type = EXEC_ON_DATANODES;
        query_step->exec_direct_type = EXEC_DIRECT_NONE;
        query_step->exec_nodes = exec_nodes;
    }

    Assert(query_step->exec_nodes);

    /* Deparse query tree to get step query. */
    if (query_step->sql_statement == NULL)
    {
        initStringInfo(&buf);
        /*
         * We always finalise aggregates on datanodes for FQS.
         * Use the expressions for ORDER BY or GROUP BY clauses.
         */
        polarx_deparse_query(query, &buf, NIL, true, false);
        query_step->sql_statement = pstrdup(buf.data);
        pfree(buf.data);
    }

    /* Optimize multi-node handling */
    query_step->read_only = (query->commandType == CMD_SELECT && !query->hasForUpdate);
    query_step->has_row_marks = query->hasForUpdate;

    /* Check if temporary tables are in use in query */
    /* PGXC_FQS_TODO: scanning the rtable again for the queries should not be
     * needed. We should be able to find out if the query has a temporary object
     * while finding nodes for the objects. But there is no way we can convey
     * that information here. Till such a connection is available, this is it.
     */
    if (contains_temp_tables(query->rtable))
        query_step->is_temp = true;

    /*
     * We need to evaluate some expressions like the ExecNodes->en_expr at
     * Coordinator, prepare those for evaluation. Ideally we should call
     * preprocess_expression, but it needs PlannerInfo structure for the same
     */
    fix_opfuncids((Node *)(query_step->exec_nodes->en_expr));
    /*
     * PGXCTODO
     * When Postgres runs insert into t (a) values (1); against table
     * defined as create table t (a int, b int); the plan is looking
     * like insert into t (a,b) values (1,null);
     * Later executor is verifying plan, to make sure table has not
     * been altered since plan has been created and comparing table
     * definition with plan target list and output error if they do
     * not match.
     * I could not find better way to generate targetList for pgxc plan
     * then call standard planner and take targetList from the plan
     * generated by Postgres.
     */
    query_step->combine_type = get_plan_combine_type(
                query->commandType, query_step->exec_nodes->baselocatortype);

    query_step->scan.plan.targetlist = query->targetList;
    #if 0
	query_step->base_tlist = query->targetList;
	#endif
    return query_step;
}

List *
makeCustomScanVarTargetlistBasedTargetEntryList(List *targetEntryList)
{
    List *customScanTlist = NIL;
    const int rangeTableIndex = INDEX_VAR;
    Var *newVar = NULL;
    TargetEntry *newTle = NULL;
    ListCell   *l;

    foreach(l, targetEntryList)
    {
        TargetEntry *tle = lfirst(l);

        if (tle->resjunk)
        {
            continue;
        }

        newVar = makeVarFromTargetEntry(rangeTableIndex, tle);


        newTle = flatCopyTargetEntry(tle);
        newTle->expr = (Expr *) newVar;
        customScanTlist = lappend(customScanTlist, newTle);
    }

    return customScanTlist;
}

static void
fqs_preprocess_rowmarks(PlannerInfo *root)
{
    Query      *parse = root->parse;
    Bitmapset  *rels;
    List       *prowmarks;
    ListCell   *l;
    int         i;

    if (parse->rowMarks)
    {
        /*
         * We've got trouble if FOR [KEY] UPDATE/SHARE appears inside
         * grouping, since grouping renders a reference to individual tuple
         * CTIDs invalid.  This is also checked at parse time, but that's
         * insufficient because of rule substitution, query pullup, etc.
         */
        CheckSelectLocking(parse, linitial_node(RowMarkClause,
                    parse->rowMarks)->strength);
    }
    else
    {
        /*
         * We only need rowmarks for UPDATE, DELETE, or FOR [KEY]
         * UPDATE/SHARE.
         */
        if (parse->commandType != CMD_UPDATE &&
                parse->commandType != CMD_DELETE)
            return;
    }

    /*
     * We need to have rowmarks for all base relations except the target. We
     * make a bitmapset of all base rels and then remove the items we don't
     * need or have FOR [KEY] UPDATE/SHARE marks for.
     */
    rels = get_relids_in_jointree((Node *) parse->jointree, false);
    if (parse->resultRelation)
        rels = bms_del_member(rels, parse->resultRelation);

    /*
     * Convert RowMarkClauses to PlanRowMark representation.
     */
    prowmarks = NIL;
    foreach(l, parse->rowMarks)
    {
        RowMarkClause *rc = lfirst_node(RowMarkClause, l);
        RangeTblEntry *rte = rt_fetch(rc->rti, parse->rtable);
        PlanRowMark *newrc;

        /*
         * Currently, it is syntactically impossible to have FOR UPDATE et al
         * applied to an update/delete target rel.  If that ever becomes
         * possible, we should drop the target from the PlanRowMark list.
         */
        Assert(rc->rti != parse->resultRelation);

        /*
         * Ignore RowMarkClauses for subqueries; they aren't real tables and
         * can't support true locking.  Subqueries that got flattened into the
         * main query should be ignored completely.  Any that didn't will get
         * ROW_MARK_COPY items in the next loop.
         */
        if (rte->rtekind != RTE_RELATION)
            continue;

        rels = bms_del_member(rels, rc->rti);

        newrc = makeNode(PlanRowMark);
        newrc->rti = newrc->prti = rc->rti;
        newrc->rowmarkId = ++(root->glob->lastRowMarkId);
        newrc->markType = select_rowmark_type(rte, rc->strength);
        newrc->allMarkTypes = (1 << newrc->markType);
        newrc->strength = rc->strength;
        newrc->waitPolicy = rc->waitPolicy;
        newrc->isParent = false;

        prowmarks = lappend(prowmarks, newrc);
    }

    /*
     *   * Now, add rowmarks for any non-target, non-locked base relations.
     *       */
    i = 0;
    foreach(l, parse->rtable)
    {
        RangeTblEntry *rte = lfirst_node(RangeTblEntry, l);
        PlanRowMark *newrc;

        i++;
        if (!bms_is_member(i, rels))
            continue;

        newrc = makeNode(PlanRowMark);
        newrc->rti = newrc->prti = i;
        newrc->rowmarkId = ++(root->glob->lastRowMarkId);
        newrc->markType = select_rowmark_type(rte, LCS_NONE);
        newrc->allMarkTypes = (1 << newrc->markType);
        newrc->strength = LCS_NONE;
        newrc->waitPolicy = LockWaitBlock;  /* doesn't matter */
        newrc->isParent = false;

        prowmarks = lappend(prowmarks, newrc);
    }

    root->rowMarks = prowmarks;
}

List *
AddRemoteQueryNode(List *stmts, const char *queryString, RemoteQueryExecType remoteExecType)
{
    List *result = stmts;

    /* If node is appplied on EXEC_ON_NONE, simply return the list unchanged */
    if (remoteExecType == EXEC_ON_NONE)
        return result;

    /* Only a remote Coordinator is allowed to send a query to backend nodes */
    if (remoteExecType == EXEC_ON_CURRENT ||
            (IS_PGXC_LOCAL_COORDINATOR))
    {
        RemoteQuery *step = polarxMakeNode(RemoteQuery);
        step->combine_type = COMBINE_TYPE_SAME;
        step->sql_statement = (char *) queryString;
        step->exec_type = remoteExecType;
        result = lappend(result, step);
    }

    return result;
}

static List *
extractResjunkTarget(List *src_list)
{
    ListCell   *lc;
    List *res = NIL;
    foreach(lc, src_list)
    {
        TargetEntry *tar = lfirst_node(TargetEntry, lc);

        if(!tar->resjunk)
            res = lappend(res, (void *)tar);
    }
    return res;
}
