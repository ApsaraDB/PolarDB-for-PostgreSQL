/*-------------------------------------------------------------------------
 *
 * explain.c
 *	  Explain query execution plans
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994-5, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/commands/explain.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/xact.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_type.h"
#include "commands/createas.h"
#include "commands/defrem.h"
#include "commands/prepare.h"
#include "executor/nodeHash.h"
#include "foreign/fdwapi.h"
#include "jit/jit.h"
#include "nodes/extensible.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/clauses.h"
#include "optimizer/planmain.h"
#include "parser/parsetree.h"
#include "rewrite/rewriteHandler.h"
#include "storage/bufmgr.h"
#include "tcop/tcopprot.h"
#include "utils/builtins.h"
#include "utils/json.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/ruleutils.h"
#include "utils/snapmgr.h"
#include "utils/tuplesort.h"
#include "utils/typcache.h"
#include "utils/xml.h"

/* POLAR px */
#include "parser/parse_clause.h"
#include "px/px_vars.h"
#include "px/px_hash.h"
#include "px/px_gang.h"
#include "px/px_adaptive_paging.h"
#include "px/px_dispatchresult.h"
#include "executor/execUtils_px.h"
#include "explain_px.c"
/* POLAR end */

/* Hook for plugins to get control in ExplainOneQuery() */
ExplainOneQuery_hook_type ExplainOneQuery_hook = NULL;

/* Hook for plugins to get control in explain_get_index_name() */
explain_get_index_name_hook_type explain_get_index_name_hook = NULL;


/* OR-able flags for ExplainXMLTag() */
#define X_OPENING 0
#define X_CLOSING 1
#define X_CLOSE_IMMEDIATE 2
#define X_NOWHITESPACE 4

static void ExplainOneQuery(Query *query, int cursorOptions,
				IntoClause *into, ExplainState *es,
				const char *queryString, ParamListInfo params,
				QueryEnvironment *queryEnv);
static void report_triggers(ResultRelInfo *rInfo, bool show_relname,
				ExplainState *es);
static double elapsed_time(instr_time *starttime);
static bool ExplainPreScanNode(PlanState *planstate, Bitmapset **rels_used);
static void ExplainNode(PlanState *planstate, List *ancestors,
			const char *relationship, const char *plan_name,
			ExplainState *es);
static void show_plan_tlist(PlanState *planstate, List *ancestors,
				ExplainState *es);
static void show_expression(Node *node, const char *qlabel,
				PlanState *planstate, List *ancestors,
				bool useprefix, ExplainState *es);
static void show_qual(List *qual, const char *qlabel,
		  PlanState *planstate, List *ancestors,
		  bool useprefix, ExplainState *es);
static void show_scan_qual(List *qual, const char *qlabel,
			   PlanState *planstate, List *ancestors,
			   ExplainState *es);
static void show_upper_qual(List *qual, const char *qlabel,
				PlanState *planstate, List *ancestors,
				ExplainState *es);
static void show_sort_keys(SortState *sortstate, List *ancestors,
			   ExplainState *es);
static void show_merge_append_keys(MergeAppendState *mstate, List *ancestors,
					   ExplainState *es);
static void show_agg_keys(AggState *astate, List *ancestors,
			  ExplainState *es);
static void show_grouping_sets(PlanState *planstate, Agg *agg,
				   List *ancestors, ExplainState *es);
static void show_grouping_set_keys(PlanState *planstate,
					   Agg *aggnode, Sort *sortnode,
					   List *context, bool useprefix,
					   List *ancestors, ExplainState *es);
static void show_group_keys(GroupState *gstate, List *ancestors,
				ExplainState *es);
static void show_sort_group_keys(PlanState *planstate, const char *qlabel,
					 int nkeys, AttrNumber *keycols,
					 Oid *sortOperators, Oid *collations, bool *nullsFirst,
					 List *ancestors, ExplainState *es);
static void show_sortorder_options(StringInfo buf, Node *sortexpr,
					   Oid sortOperator, Oid collation, bool nullsFirst);
static void show_tablesample(TableSampleClause *tsc, PlanState *planstate,
				 List *ancestors, ExplainState *es);
static void show_sort_info(SortState *sortstate, ExplainState *es);
static void show_hash_info(HashState *hashstate, ExplainState *es);
static void show_tidbitmap_info(BitmapHeapScanState *planstate,
					ExplainState *es);
static void show_instrumentation_count(const char *qlabel, int which,
						   PlanState *planstate, ExplainState *es);
/* POLAR px */
static void show_result_hash_filter(const char *qlabel, 
									ResultState *resultstate, 
									ExplainState *es);
static void show_foreignscan_info(ForeignScanState *fsstate, ExplainState *es);
static void show_eval_params(Bitmapset *bms_params, ExplainState *es);

/* POLAR px */
static void polar_show_join_pruning_info(List *join_prune_ids, ExplainState *es);

static const char *explain_get_index_name(Oid indexId);
static void show_buffer_usage(ExplainState *es, const BufferUsage *usage);
static void ExplainIndexScanDetails(Oid indexid, ScanDirection indexorderdir,
						ExplainState *es);
static void ExplainScanTarget(Scan *plan, ExplainState *es);
static void ExplainModifyTarget(ModifyTable *plan, ExplainState *es);
static void ExplainTargetRel(Plan *plan, Index rti, ExplainState *es);
static void show_modifytable_info(ModifyTableState *mtstate, List *ancestors,
					  ExplainState *es);
static void ExplainMemberNodes(PlanState **planstates, int nplans,
				   List *ancestors, ExplainState *es);
static void ExplainMissingMembers(int nplans, int nchildren, ExplainState *es);
static void ExplainSubPlans(List *plans, List *ancestors,
				const char *relationship, ExplainState *es);
static void ExplainCustomChildren(CustomScanState *css,
					  List *ancestors, ExplainState *es);
static void ExplainProperty(const char *qlabel, const char *unit,
				const char *value, bool numeric, ExplainState *es);

static void ExplainDummyGroup(const char *objtype, const char *labelname,
				  ExplainState *es);
static void ExplainXMLTag(const char *tagname, int flags, ExplainState *es);
static void ExplainJSONLineEnding(ExplainState *es);
static void ExplainYAMLLineStarting(ExplainState *es);
static void escape_yaml(StringInfo buf, const char *str);

/* POLAR px */
static void show_dispatch_info(ExecSlice *slice, ExplainState *es, Plan *plan);
static void show_motion_keys(PlanState *planstate, List *hashExpr, int nkeys, AttrNumber *keycols,
							 const char *qlabel, List *ancestors, ExplainState *es);
/* POLAR end */

/*
 * ExplainQuery -
 *	  execute an EXPLAIN command
 */
void
ExplainQuery(ParseState *pstate, ExplainStmt *stmt, const char *queryString,
			 ParamListInfo params, QueryEnvironment *queryEnv,
			 DestReceiver *dest)
{
	ExplainState *es = NewExplainState();
	TupOutputState *tstate;
	List	   *rewritten;
	ListCell   *lc;
	bool		timing_set = false;
	bool		summary_set = false;

	/* Parse options list. */
	foreach(lc, stmt->options)
	{
		DefElem    *opt = (DefElem *) lfirst(lc);

		if (strcmp(opt->defname, "analyze") == 0)
			es->analyze = defGetBoolean(opt);
		else if (strcmp(opt->defname, "verbose") == 0)
			es->verbose = defGetBoolean(opt);
		else if (strcmp(opt->defname, "costs") == 0)
			es->costs = defGetBoolean(opt);
		else if (strcmp(opt->defname, "buffers") == 0)
			es->buffers = defGetBoolean(opt);
		else if (strcmp(opt->defname, "timing") == 0)
		{
			timing_set = true;
			es->timing = defGetBoolean(opt);
		}
		else if (strcmp(opt->defname, "summary") == 0)
		{
			summary_set = true;
			es->summary = defGetBoolean(opt);
		}
		else if (strcmp(opt->defname, "format") == 0)
		{
			char	   *p = defGetString(opt);

			if (strcmp(p, "text") == 0)
				es->format = EXPLAIN_FORMAT_TEXT;
			else if (strcmp(p, "xml") == 0)
				es->format = EXPLAIN_FORMAT_XML;
			else if (strcmp(p, "json") == 0)
				es->format = EXPLAIN_FORMAT_JSON;
			else if (strcmp(p, "yaml") == 0)
				es->format = EXPLAIN_FORMAT_YAML;
			else
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("unrecognized value for EXPLAIN option \"%s\": \"%s\"",
								opt->defname, p),
						 parser_errposition(pstate, opt->location)));
		}
		/* POLAR px */
		else if (strcmp(opt->defname, "dxl") == 0)
			es->dxl = defGetBoolean(opt);
		else if (strcmp(opt->defname, "slicetable") == 0)
			es->slicetable = defGetBoolean(opt);
		/* POLAR end */
		else
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("unrecognized EXPLAIN option \"%s\"",
							opt->defname),
					 parser_errposition(pstate, opt->location)));
	}

	if (es->buffers && !es->analyze)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("EXPLAIN option BUFFERS requires ANALYZE")));

	/* if the timing was not set explicitly, set default value */
	es->timing = (timing_set) ? es->timing : es->analyze;

	/* check that timing is used with EXPLAIN ANALYZE */
	if (es->timing && !es->analyze)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("EXPLAIN option TIMING requires ANALYZE")));

	/* if the summary was not set explicitly, set default value */
	es->summary = (summary_set) ? es->summary : es->analyze;

	/* POLAR px */
	if (px_explain_memory_verbosity >= EXPLAIN_MEMORY_VERBOSITY_DETAIL)
		es->memory_detail = true;
	/* POLAR end */
	/*
	 * Parse analysis was done already, but we still have to run the rule
	 * rewriter.  We do not do AcquireRewriteLocks: we assume the query either
	 * came straight from the parser, or suitable locks were acquired by
	 * plancache.c.
	 *
	 * Because the rewriter and planner tend to scribble on the input, we make
	 * a preliminary copy of the source querytree.  This prevents problems in
	 * the case that the EXPLAIN is in a portal or plpgsql function and is
	 * executed repeatedly.  (See also the same hack in DECLARE CURSOR and
	 * PREPARE.)  XXX FIXME someday.
	 */
	rewritten = QueryRewrite(castNode(Query, copyObject(stmt->query)));

	/* emit opening boilerplate */
	ExplainBeginOutput(es);

	if (rewritten == NIL)
	{
		/*
		 * In the case of an INSTEAD NOTHING, tell at least that.  But in
		 * non-text format, the output is delimited, so this isn't necessary.
		 */
		if (es->format == EXPLAIN_FORMAT_TEXT)
			appendStringInfoString(es->str, "Query rewrites to nothing\n");
	}
	else
	{
		ListCell   *l;

		/* Explain every plan */
		foreach(l, rewritten)
		{
			/* POLAR px: try to use PX if possible */
			ExplainOneQuery(lfirst_node(Query, l),
							CURSOR_OPT_PARALLEL_OK | CURSOR_OPT_PX_OK, NULL, es,
							queryString, params, queryEnv);
			/* POLAR end */

			/* Separate plans with an appropriate separator */
			if (lnext(l) != NULL)
				ExplainSeparatePlans(es);
		}
	}

	/* emit closing boilerplate */
	ExplainEndOutput(es);
	Assert(es->indent == 0);

	/* output tuples */
	tstate = begin_tup_output_tupdesc(dest, ExplainResultDesc(stmt));
	if (es->format == EXPLAIN_FORMAT_TEXT)
		do_text_output_multiline(tstate, es->str->data);
	else
		do_text_output_oneline(tstate, es->str->data);
	end_tup_output(tstate);

	pfree(es->str->data);
}

/*
 * Create a new ExplainState struct initialized with default options.
 */
ExplainState *
NewExplainState(void)
{
	ExplainState *es = (ExplainState *) palloc0(sizeof(ExplainState));

	/* Set default options (most fields can be left as zeroes). */
	es->costs = true;
	/* Prepare output buffer. */
	es->str = makeStringInfo();

	return es;
}

/*
 * ExplainResultDesc -
 *	  construct the result tupledesc for an EXPLAIN
 */
TupleDesc
ExplainResultDesc(ExplainStmt *stmt)
{
	TupleDesc	tupdesc;
	ListCell   *lc;
	Oid			result_type = TEXTOID;

	/* Check for XML format option */
	foreach(lc, stmt->options)
	{
		DefElem    *opt = (DefElem *) lfirst(lc);

		if (strcmp(opt->defname, "format") == 0)
		{
			char	   *p = defGetString(opt);

			if (strcmp(p, "xml") == 0)
				result_type = XMLOID;
			else if (strcmp(p, "json") == 0)
				result_type = JSONOID;
			else
				result_type = TEXTOID;
			/* don't "break", as ExplainQuery will use the last value */
		}
	}

	/* Need a tuple descriptor representing a single TEXT or XML column */
	tupdesc = CreateTemplateTupleDesc(1, false);
	TupleDescInitEntry(tupdesc, (AttrNumber) 1, "QUERY PLAN",
					   result_type, -1, 0);
	return tupdesc;
}

/*
 * ExplainOneQuery -
 *	  print out the execution plan for one Query
 *
 * "into" is NULL unless we are explaining the contents of a CreateTableAsStmt.
 */
static void
ExplainOneQuery(Query *query, int cursorOptions,
				IntoClause *into, ExplainState *es,
				const char *queryString, ParamListInfo params,
				QueryEnvironment *queryEnv)
{
	/* planner will not cope with utility statements */
	if (query->commandType == CMD_UTILITY)
	{
		ExplainOneUtility(query->utilityStmt, into, es, queryString, params,
						  queryEnv);
		return;
	}

	/* if an advisor plugin is present, let it manage things */
	if (ExplainOneQuery_hook)
		(*ExplainOneQuery_hook) (query, cursorOptions, into, es,
								 queryString, params, queryEnv);
	else
	{
		PlannedStmt *plan;
		instr_time	planstart,
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

/*
 * ExplainOneUtility -
 *	  print out the execution plan for one utility statement
 *	  (In general, utility statements don't have plans, but there are some
 *	  we treat as special cases)
 *
 * "into" is NULL unless we are explaining the contents of a CreateTableAsStmt.
 *
 * This is exported because it's called back from prepare.c in the
 * EXPLAIN EXECUTE case.
 */
void
ExplainOneUtility(Node *utilityStmt, IntoClause *into, ExplainState *es,
				  const char *queryString, ParamListInfo params,
				  QueryEnvironment *queryEnv)
{
	if (utilityStmt == NULL)
		return;

	if (IsA(utilityStmt, CreateTableAsStmt))
	{
		/*
		 * We have to rewrite the contained SELECT and then pass it back to
		 * ExplainOneQuery.  It's probably not really necessary to copy the
		 * contained parsetree another time, but let's be safe.
		 */
		CreateTableAsStmt *ctas = (CreateTableAsStmt *) utilityStmt;
		List	   *rewritten;

		rewritten = QueryRewrite(castNode(Query, copyObject(ctas->query)));
		Assert(list_length(rewritten) == 1);

		/* POLAR px: try to use PX if GUC enables */
		if (px_enable_create_table_as && px_enable_replay_wait &&
			!interpretOidsOption(ctas->into->options, !(ctas->into->viewQuery != NULL)))
			ExplainOneQuery(linitial_node(Query, rewritten),
							CURSOR_OPT_PARALLEL_OK | CURSOR_OPT_PX_OK, ctas->into, es,
							queryString, params, queryEnv);
		else
		/* POLAR end */
			ExplainOneQuery(linitial_node(Query, rewritten),
							CURSOR_OPT_PARALLEL_OK, ctas->into, es,
							queryString, params, queryEnv);
	}
	else if (IsA(utilityStmt, DeclareCursorStmt))
	{
		/*
		 * Likewise for DECLARE CURSOR.
		 *
		 * Notice that if you say EXPLAIN ANALYZE DECLARE CURSOR then we'll
		 * actually run the query.  This is different from pre-8.3 behavior
		 * but seems more useful than not running the query.  No cursor will
		 * be created, however.
		 */
		DeclareCursorStmt *dcs = (DeclareCursorStmt *) utilityStmt;
		List	   *rewritten;

		rewritten = QueryRewrite(castNode(Query, copyObject(dcs->query)));
		Assert(list_length(rewritten) == 1);
		ExplainOneQuery(linitial_node(Query, rewritten),
						dcs->options, NULL, es,
						queryString, params, queryEnv);
	}
	else if (IsA(utilityStmt, ExecuteStmt))
		ExplainExecuteQuery((ExecuteStmt *) utilityStmt, into, es,
							queryString, params, queryEnv);
	else if (IsA(utilityStmt, NotifyStmt))
	{
		if (es->format == EXPLAIN_FORMAT_TEXT)
			appendStringInfoString(es->str, "NOTIFY\n");
		else
			ExplainDummyGroup("Notify", NULL, es);
	}
	else
	{
		if (es->format == EXPLAIN_FORMAT_TEXT)
			appendStringInfoString(es->str,
								   "Utility statements have no plan structure\n");
		else
			ExplainDummyGroup("Utility Statement", NULL, es);
	}
}

/*
 * ExplainOnePlan -
 *		given a planned query, execute it if needed, and then print
 *		EXPLAIN output
 *
 * "into" is NULL unless we are explaining the contents of a CreateTableAsStmt,
 * in which case executing the query should result in creating that table.
 *
 * This is exported because it's called back from prepare.c in the
 * EXPLAIN EXECUTE case, and because an index advisor plugin would need
 * to call it.
 */
void
ExplainOnePlan(PlannedStmt *plannedstmt, IntoClause *into, ExplainState *es,
			   const char *queryString, ParamListInfo params,
			   QueryEnvironment *queryEnv, const instr_time *planduration)
{
	DestReceiver *dest;
	QueryDesc  *queryDesc;
	instr_time	starttime;
	double		totaltime = 0;
	int			eflags;
	int			instrument_option = 0;

	Assert(plannedstmt->commandType != CMD_UTILITY);

	if (es->analyze && es->timing)
		instrument_option |= INSTRUMENT_TIMER;
	else if (es->analyze)
		instrument_option |= INSTRUMENT_ROWS;

	if (es->buffers)
		instrument_option |= INSTRUMENT_BUFFERS;
	/* POLAR px */
	if (es->memory_detail)
		instrument_option |= INSTRUMENT_MEMORY_DETAIL;
	/* POLAR end */

	/*
	 * We always collect timing for the entire statement, even when node-level
	 * timing is off, so we don't look at es->timing here.  (We could skip
	 * this if !es->summary, but it's hardly worth the complication.)
	 */
	INSTR_TIME_SET_CURRENT(starttime);

	/*
	 * Use a snapshot with an updated command ID to ensure this query sees
	 * results of any previously executed queries.
	 */
	PushCopiedSnapshot(GetActiveSnapshot());
	UpdateActiveSnapshotCommandId();

	/*
	 * Normally we discard the query's output, but if explaining CREATE TABLE
	 * AS, we'd better use the appropriate tuple receiver.
	 */
	if (into)
		dest = CreateIntoRelDestReceiver(into);
	else
		dest = None_Receiver;

	/* Create a QueryDesc for the query */
	queryDesc = CreateQueryDesc(plannedstmt, queryString,
								GetActiveSnapshot(), InvalidSnapshot,
								dest, params, queryEnv, instrument_option);

	/* POLAR px : Allocate workarea for summary stats. */
    if (es->analyze && queryDesc->plannedstmt->planGen == PLANGEN_PX)
    {
		queryDesc->instrument_options |= INSTRUMENT_PX;
        /* Attach workarea to QueryDesc so ExecSetParamPlan() can find it. */
        queryDesc->showstatctx = pxexplain_showExecStatsBegin(queryDesc,
															   starttime);
    }
	else
		queryDesc->showstatctx = NULL;

	queryDesc->plannedstmt->query_mem = ResourceManagerGetQueryMemoryLimit(queryDesc->plannedstmt);
	/* POLAR end */

	/* Select execution options */
	if (es->analyze)
		eflags = 0;				/* default run-to-completion flags */
	else
		eflags = EXEC_FLAG_EXPLAIN_ONLY;
	if (into)
		eflags |= GetIntoRelEFlags(into);

	/* call ExecutorStart to prepare the plan for execution */
	ExecutorStart(queryDesc, eflags);

	/* Execute the plan for statistics if asked for */
	if (es->analyze)
	{
		ScanDirection dir;

		/* EXPLAIN ANALYZE CREATE TABLE AS WITH NO DATA is weird */
		if (into && into->skipData)
			dir = NoMovementScanDirection;
		else
			dir = ForwardScanDirection;

		/* run the plan */
		ExecutorRun(queryDesc, dir, 0L, true);

		/* POLAR px : Wait for completion of all qExec processes. */
		if (queryDesc->estate->dispatcherState && queryDesc->estate->dispatcherState->primaryResults)
			pxdisp_checkDispatchResult(queryDesc->estate->dispatcherState, DISPATCH_WAIT_NONE);
		/* POLAR end */

		/* run cleanup too */
		ExecutorFinish(queryDesc);

		/* We can't run ExecutorEnd 'till we're done printing the stats... */
		totaltime += elapsed_time(&starttime);
	}

	ExplainOpenGroup("Query", NULL, true, es);

	/* Create textual dump of plan tree */
	ExplainPrintPlan(es, queryDesc);

	if (es->summary && planduration)
	{
		double		plantime = INSTR_TIME_GET_DOUBLE(*planduration);

		ExplainPropertyFloat("Planning Time", "ms", 1000.0 * plantime, 3, es);
	}

	/* POLAR px */
	if (queryDesc->plannedstmt->planGen == PLANGEN_PX)
		ExplainPropertyStringInfo("Optimizer", es, "PolarDB PX Optimizer");
	
	/* Print slice table */
	if (es->slicetable)
		ExplainPrintSliceTable(es, queryDesc);

	/*
	 * Display per-slice and whole-query statistics.
	 */
	if (es->analyze && queryDesc->plannedstmt->planGen == PLANGEN_PX)
		pxexplain_showExecStatsEnd(queryDesc->plannedstmt, queryDesc->showstatctx,
									queryDesc->estate, es);
	/* POLAR end */

	/* Print info about runtime of triggers */
	if (es->analyze)
		ExplainPrintTriggers(es, queryDesc);

	/*
	 * Print info about JITing. Tied to es->costs because we don't want to
	 * display this in regression tests, as it'd cause output differences
	 * depending on build options.  Might want to separate that out from COSTS
	 * at a later stage.
	 */
	if (es->costs)
		ExplainPrintJITSummary(es, queryDesc);

	/*
	 * Close down the query and free resources.  Include time for this in the
	 * total execution time (although it should be pretty minimal).
	 */
	INSTR_TIME_SET_CURRENT(starttime);

	ExecutorEnd(queryDesc);

	FreeQueryDesc(queryDesc);

	PopActiveSnapshot();

	/* We need a CCI just in case query expanded to multiple plans */
	if (es->analyze)
		CommandCounterIncrement();

	totaltime += elapsed_time(&starttime);

	/*
	 * We only report execution time if we actually ran the query (that is,
	 * the user specified ANALYZE), and if summary reporting is enabled (the
	 * user can set SUMMARY OFF to not have the timing information included in
	 * the output).  By default, ANALYZE sets SUMMARY to true.
	 */
	if (es->summary && es->analyze)
		ExplainPropertyFloat("Execution Time", "ms", 1000.0 * totaltime, 3,
							 es);

	ExplainCloseGroup("Query", NULL, true, es);
}

/*
 * ExplainPrintPlan -
 *	  convert a QueryDesc's plan tree to text and append it to es->str
 *
 * The caller should have set up the options fields of *es, as well as
 * initializing the output buffer es->str.  Also, output formatting state
 * such as the indent level is assumed valid.  Plan-tree-specific fields
 * in *es are initialized here.
 *
 * NB: will not work on utility statements
 */
void
ExplainPrintPlan(ExplainState *es, QueryDesc *queryDesc)
{
	Bitmapset  *rels_used = NULL;
	PlanState  *ps;

	/* POLAR px */
	EState     *estate = queryDesc->estate;

	/* Set up ExplainState fields associated with this plan tree */
	Assert(queryDesc->plannedstmt != NULL);
	es->pstmt = queryDesc->plannedstmt;
	es->rtable = queryDesc->plannedstmt->rtable;
	/* POLAR px: Find slice table entry for the root slice. */
	if (queryDesc->plannedstmt->planGen == PLANGEN_PX)	{
		es->showstatctx = queryDesc->showstatctx;
		es->currentSlice = getCurrentSlice(estate, LocallyExecutingSliceIndex(estate));

		/*
		* Get local stats if root slice was executed here in the qDisp, as long
		* as we haven't already gathered the statistics. This can happen when an
		* executor hook generates EXPLAIN output.
		*/
		if (es->analyze && !es->showstatctx->stats_gathered)
		{
			if (px_role != PX_ROLE_PX && (!es->currentSlice || sliceRunsOnQC(es->currentSlice)))
				pxexplain_localExecStats(queryDesc->planstate, es->showstatctx);

			/* Fill in the plan's Instrumentation with stats from qExecs. */
			if (estate->dispatcherState && estate->dispatcherState->primaryResults)
				pxexplain_recvExecStats(queryDesc->planstate,
										estate->dispatcherState->primaryResults,
										LocallyExecutingSliceIndex(estate),
										es->showstatctx);
		}	
	}
	/* POLAR end */

	ExplainPreScanNode(queryDesc->planstate, &rels_used);
	es->rtable_names = select_rtable_names_for_explain(es->rtable, rels_used);
	es->deparse_cxt = deparse_context_for_plan_rtable(es->rtable,
													  es->rtable_names);
	es->printed_subplans = NULL;

	/*
	 * Sometimes we mark a Gather node as "invisible", which means that it's
	 * not displayed in EXPLAIN output.  The purpose of this is to allow
	 * running regression tests with force_parallel_mode=regress to get the
	 * same results as running the same tests with force_parallel_mode=off.
	 */
	ps = queryDesc->planstate;
	if (IsA(ps, GatherState) &&((Gather *) ps->plan)->invisible)
		ps = outerPlanState(ps);
	ExplainNode(ps, NIL, NULL, NULL, es);
}

/*
 * POLAR px:
 * ExplainPrintSliceTable -
 *	  convert the MPP slice table text and append it to es->str
 */
void
ExplainPrintSliceTable(ExplainState *es, QueryDesc *queryDesc)
{
	SliceTable *sliceTable = queryDesc->estate->es_sliceTable;
	int			numSlices = (sliceTable ? sliceTable->numSlices : 0);

	ExplainOpenGroup("Slice Table", "Slice Table", false, es);

	for (int i = 0; i < numSlices; i++)
	{
		ExecSlice *slice = &sliceTable->slices[i];
		const char *gangType = "???";

		switch (slice->gangType)
		{
			case GANGTYPE_UNALLOCATED:
				gangType = "Dispatcher";
				break;
			case GANGTYPE_PRIMARY_READER:
				gangType = "Reader";
				break;
			default:
				break;
		}

		if (es->format == EXPLAIN_FORMAT_TEXT)
		{
			appendStringInfo(es->str, "Slice %d: %s; root %d; parent %d; gang size %d",
							 i,
							 gangType,
							 slice->rootIndex,
							 slice->parentIndex,
							 list_length(slice->segments)); 
			if (slice->gangType == GANGTYPE_SINGLETON_READER)
				appendStringInfo(es->str, "; segment %d", linitial_int(slice->segments));
			appendStringInfoString(es->str, "\n");
		}
		else
		{
			ExplainOpenGroup("Slice", NULL, true, es);
			ExplainPropertyInteger("Slice ID", NULL, i, es);
			ExplainPropertyText("Gang Type", gangType, es);
			ExplainPropertyInteger("Root", NULL, slice->rootIndex, es);
			ExplainPropertyInteger("Parent", NULL, slice->parentIndex, es);
			ExplainPropertyInteger("Gang Size", NULL, list_length(slice->segments), es);
			if (slice->gangType == GANGTYPE_SINGLETON_READER)
				ExplainPropertyInteger("Segment", NULL, linitial_int(slice->segments), es);
			ExplainCloseGroup("Slice", NULL, true, es);
		}
	}

	ExplainCloseGroup("Slice Table", "Slice Table", false, es);
}
/* POLAR end */

/*
 * ExplainPrintTriggers -
 *	  convert a QueryDesc's trigger statistics to text and append it to
 *	  es->str
 *
 * The caller should have set up the options fields of *es, as well as
 * initializing the output buffer es->str.  Other fields in *es are
 * initialized here.
 */
void
ExplainPrintTriggers(ExplainState *es, QueryDesc *queryDesc)
{
	ResultRelInfo *rInfo;
	bool		show_relname;
	int			numrels = queryDesc->estate->es_num_result_relations;
	int			numrootrels = queryDesc->estate->es_num_root_result_relations;
	List	   *routerels;
	List	   *targrels;
	int			nr;
	ListCell   *l;

	routerels = queryDesc->estate->es_tuple_routing_result_relations;
	targrels = queryDesc->estate->es_trig_target_relations;

	ExplainOpenGroup("Triggers", "Triggers", false, es);

	show_relname = (numrels > 1 || numrootrels > 0 ||
					routerels != NIL || targrels != NIL);
	rInfo = queryDesc->estate->es_result_relations;
	for (nr = 0; nr < numrels; rInfo++, nr++)
		report_triggers(rInfo, show_relname, es);

	rInfo = queryDesc->estate->es_root_result_relations;
	for (nr = 0; nr < numrootrels; rInfo++, nr++)
		report_triggers(rInfo, show_relname, es);

	foreach(l, routerels)
	{
		rInfo = (ResultRelInfo *) lfirst(l);
		report_triggers(rInfo, show_relname, es);
	}

	foreach(l, targrels)
	{
		rInfo = (ResultRelInfo *) lfirst(l);
		report_triggers(rInfo, show_relname, es);
	}

	ExplainCloseGroup("Triggers", "Triggers", false, es);
}

/*
 * ExplainPrintJITSummary -
 *    Print summarized JIT instrumentation from leader and workers
 */
void
ExplainPrintJITSummary(ExplainState *es, QueryDesc *queryDesc)
{
	JitInstrumentation ji = {0};

	if (!(queryDesc->estate->es_jit_flags & PGJIT_PERFORM))
		return;

	/*
	 * Work with a copy instead of modifying the leader state, since this
	 * function may be called twice
	 */
	if (queryDesc->estate->es_jit)
		InstrJitAgg(&ji, &queryDesc->estate->es_jit->instr);

	/* If this process has done JIT in parallel workers, merge stats */
	if (queryDesc->estate->es_jit_worker_instr)
		InstrJitAgg(&ji, queryDesc->estate->es_jit_worker_instr);

	ExplainPrintJIT(es, queryDesc->estate->es_jit_flags, &ji, -1);
}

/*
 * ExplainPrintJIT -
 *	  Append information about JITing to es->str.
 *
 * Can be used to print the JIT instrumentation of the backend (worker_num =
 * -1) or that of a specific worker (worker_num = ...).
 */
void
ExplainPrintJIT(ExplainState *es, int jit_flags,
				JitInstrumentation *ji, int worker_num)
{
	instr_time	total_time;
	bool		for_workers = (worker_num >= 0);

	/* don't print information if no JITing happened */
	if (!ji || ji->created_functions == 0)
		return;

	/* calculate total time */
	INSTR_TIME_SET_ZERO(total_time);
	INSTR_TIME_ADD(total_time, ji->generation_counter);
	INSTR_TIME_ADD(total_time, ji->inlining_counter);
	INSTR_TIME_ADD(total_time, ji->optimization_counter);
	INSTR_TIME_ADD(total_time, ji->emission_counter);

	ExplainOpenGroup("JIT", "JIT", true, es);

	/* for higher density, open code the text output format */
	if (es->format == EXPLAIN_FORMAT_TEXT)
	{
		appendStringInfoSpaces(es->str, es->indent * 2);
		if (for_workers)
			appendStringInfo(es->str, "JIT for worker %u:\n", worker_num);
		else
			appendStringInfo(es->str, "JIT:\n");
		es->indent += 1;

		ExplainPropertyInteger("Functions", NULL, ji->created_functions, es);

		appendStringInfoSpaces(es->str, es->indent * 2);
		appendStringInfo(es->str, "Options: %s %s, %s %s, %s %s, %s %s\n",
						 "Inlining", jit_flags & PGJIT_INLINE ? "true" : "false",
						 "Optimization", jit_flags & PGJIT_OPT3 ? "true" : "false",
						 "Expressions", jit_flags & PGJIT_EXPR ? "true" : "false",
						 "Deforming", jit_flags & PGJIT_DEFORM ? "true" : "false");

		if (es->analyze && es->timing)
		{
			appendStringInfoSpaces(es->str, es->indent * 2);
			appendStringInfo(es->str,
							 "Timing: %s %.3f ms, %s %.3f ms, %s %.3f ms, %s %.3f ms, %s %.3f ms\n",
							 "Generation", 1000.0 * INSTR_TIME_GET_DOUBLE(ji->generation_counter),
							 "Inlining", 1000.0 * INSTR_TIME_GET_DOUBLE(ji->inlining_counter),
							 "Optimization", 1000.0 * INSTR_TIME_GET_DOUBLE(ji->optimization_counter),
							 "Emission", 1000.0 * INSTR_TIME_GET_DOUBLE(ji->emission_counter),
							 "Total", 1000.0 * INSTR_TIME_GET_DOUBLE(total_time));
		}

		es->indent -= 1;
	}
	else
	{
		ExplainPropertyInteger("Worker Number", NULL, worker_num, es);
		ExplainPropertyInteger("Functions", NULL, ji->created_functions, es);

		ExplainOpenGroup("Options", "Options", true, es);
		ExplainPropertyBool("Inlining", jit_flags & PGJIT_INLINE, es);
		ExplainPropertyBool("Optimization", jit_flags & PGJIT_OPT3, es);
		ExplainPropertyBool("Expressions", jit_flags & PGJIT_EXPR, es);
		ExplainPropertyBool("Deforming", jit_flags & PGJIT_DEFORM, es);
		ExplainCloseGroup("Options", "Options", true, es);

		if (es->analyze && es->timing)
		{
			ExplainOpenGroup("Timing", "Timing", true, es);

			ExplainPropertyFloat("Generation", "ms",
								 1000.0 * INSTR_TIME_GET_DOUBLE(ji->generation_counter),
								 3, es);
			ExplainPropertyFloat("Inlining", "ms",
								 1000.0 * INSTR_TIME_GET_DOUBLE(ji->inlining_counter),
								 3, es);
			ExplainPropertyFloat("Optimization", "ms",
								 1000.0 * INSTR_TIME_GET_DOUBLE(ji->optimization_counter),
								 3, es);
			ExplainPropertyFloat("Emission", "ms",
								 1000.0 * INSTR_TIME_GET_DOUBLE(ji->emission_counter),
								 3, es);
			ExplainPropertyFloat("Total", "ms",
								 1000.0 * INSTR_TIME_GET_DOUBLE(total_time),
								 3, es);

			ExplainCloseGroup("Timing", "Timing", true, es);
		}
	}

	ExplainCloseGroup("JIT", "JIT", true, es);
}

void
ExplainPropertyStringInfo(const char *qlabel, ExplainState *es, const char *fmt,...)
{
	StringInfoData buf;

	initStringInfo(&buf);

	for (;;)
	{
		va_list		args;
		int			needed;

		/* Try to format the data. */
		va_start(args, fmt);
		needed = appendStringInfoVA(&buf, fmt, args);
		va_end(args);

		if (needed == 0)
			break;

		/* Double the buffer size and try again. */
		enlargeStringInfo(&buf, needed);
	}

	ExplainPropertyText(qlabel, buf.data, es);
	pfree(buf.data);
}

/*
 * ExplainQueryText -
 *	  add a "Query Text" node that contains the actual text of the query
 *
 * The caller should have set up the options fields of *es, as well as
 * initializing the output buffer es->str.
 *
 */
void
ExplainQueryText(ExplainState *es, QueryDesc *queryDesc)
{
	if (queryDesc->sourceText)
		ExplainPropertyText("Query Text", queryDesc->sourceText, es);
}

/*
 * report_triggers -
 *		report execution stats for a single relation's triggers
 */
static void
report_triggers(ResultRelInfo *rInfo, bool show_relname, ExplainState *es)
{
	int			nt;

	if (!rInfo->ri_TrigDesc || !rInfo->ri_TrigInstrument)
		return;
	for (nt = 0; nt < rInfo->ri_TrigDesc->numtriggers; nt++)
	{
		Trigger    *trig = rInfo->ri_TrigDesc->triggers + nt;
		Instrumentation *instr = rInfo->ri_TrigInstrument + nt;
		char	   *relname;
		char	   *conname = NULL;

		/* Must clean up instrumentation state */
		InstrEndLoop(instr);

		/*
		 * We ignore triggers that were never invoked; they likely aren't
		 * relevant to the current query type.
		 */
		if (instr->ntuples == 0)
			continue;

		ExplainOpenGroup("Trigger", NULL, true, es);

		relname = RelationGetRelationName(rInfo->ri_RelationDesc);
		if (OidIsValid(trig->tgconstraint))
			conname = get_constraint_name(trig->tgconstraint);

		/*
		 * In text format, we avoid printing both the trigger name and the
		 * constraint name unless VERBOSE is specified.  In non-text formats
		 * we just print everything.
		 */
		if (es->format == EXPLAIN_FORMAT_TEXT)
		{
			if (es->verbose || conname == NULL)
				appendStringInfo(es->str, "Trigger %s", trig->tgname);
			else
				appendStringInfoString(es->str, "Trigger");
			if (conname)
				appendStringInfo(es->str, " for constraint %s", conname);
			if (show_relname)
				appendStringInfo(es->str, " on %s", relname);
			if (es->timing)
				appendStringInfo(es->str, ": time=%.3f calls=%.0f\n",
								 1000.0 * instr->total, instr->ntuples);
			else
				appendStringInfo(es->str, ": calls=%.0f\n", instr->ntuples);
		}
		else
		{
			ExplainPropertyText("Trigger Name", trig->tgname, es);
			if (conname)
				ExplainPropertyText("Constraint Name", conname, es);
			ExplainPropertyText("Relation", relname, es);
			if (es->timing)
				ExplainPropertyFloat("Time", "ms", 1000.0 * instr->total, 3,
									 es);
			ExplainPropertyFloat("Calls", NULL, instr->ntuples, 0, es);
		}

		if (conname)
			pfree(conname);

		ExplainCloseGroup("Trigger", NULL, true, es);
	}
}

/* Compute elapsed time in seconds since given timestamp */
static double
elapsed_time(instr_time *starttime)
{
	instr_time	endtime;

	INSTR_TIME_SET_CURRENT(endtime);
	INSTR_TIME_SUBTRACT(endtime, *starttime);
	return INSTR_TIME_GET_DOUBLE(endtime);
}

/*
 * ExplainPreScanNode -
 *	  Prescan the planstate tree to identify which RTEs are referenced
 *
 * Adds the relid of each referenced RTE to *rels_used.  The result controls
 * which RTEs are assigned aliases by select_rtable_names_for_explain.
 * This ensures that we don't confusingly assign un-suffixed aliases to RTEs
 * that never appear in the EXPLAIN output (such as inheritance parents).
 */
static bool
ExplainPreScanNode(PlanState *planstate, Bitmapset **rels_used)
{
	Plan	   *plan = planstate->plan;

	switch (nodeTag(plan))
	{
		case T_SeqScan:
		case T_SampleScan:
		case T_IndexScan:
		case T_IndexOnlyScan:
		case T_BitmapHeapScan:
		case T_TidScan:
		case T_SubqueryScan:
		case T_FunctionScan:
		case T_TableFuncScan:
		case T_ValuesScan:
		case T_CteScan:
		case T_NamedTuplestoreScan:
		case T_WorkTableScan:
		/* POLAR px */
		case T_ShareInputScan:
			*rels_used = bms_add_member(*rels_used,
										((Scan *) plan)->scanrelid);
			break;
		case T_ForeignScan:
			*rels_used = bms_add_members(*rels_used,
										 ((ForeignScan *) plan)->fs_relids);
			break;
		case T_CustomScan:
			*rels_used = bms_add_members(*rels_used,
										 ((CustomScan *) plan)->custom_relids);
			break;
		case T_ModifyTable:
			*rels_used = bms_add_member(*rels_used,
										((ModifyTable *) plan)->nominalRelation);
			if (((ModifyTable *) plan)->exclRelRTI)
				*rels_used = bms_add_member(*rels_used,
											((ModifyTable *) plan)->exclRelRTI);
			break;
		default:
			break;
	}

	return planstate_tree_walker(planstate, ExplainPreScanNode, rels_used);
}

/*
 * ExplainNode -
 *	  Appends a description of a plan tree to es->str
 *
 * planstate points to the executor state node for the current plan node.
 * We need to work from a PlanState node, not just a Plan node, in order to
 * get at the instrumentation data (if any) as well as the list of subplans.
 *
 * ancestors is a list of parent PlanState nodes, most-closely-nested first.
 * These are needed in order to interpret PARAM_EXEC Params.
 *
 * relationship describes the relationship of this plan node to its parent
 * (eg, "Outer", "Inner"); it can be null at top level.  plan_name is an
 * optional name to be attached to the node.
 *
 * In text format, es->indent is controlled in this function since we only
 * want it to change at plan-node boundaries.  In non-text formats, es->indent
 * corresponds to the nesting depth of logical output groups, and therefore
 * is controlled by ExplainOpenGroup/ExplainCloseGroup.
 */
static void
ExplainNode(PlanState *planstate, List *ancestors,
			const char *relationship, const char *plan_name,
			ExplainState *es)
{
	Plan	   *plan = planstate->plan;
	const char *pname;			/* node type name for text output */
	const char *sname;			/* node type name for non-text output */
	const char *strategy = NULL;
	const char *partialmode = NULL;
	const char *operation = NULL;
	const char *custom_name = NULL;
	int			save_indent = es->indent;
	bool		haschildren;

	int			motion_recv;
	int			motion_snd;

	/* POLAR px */
	ExecSlice      *save_currentSlice = es->currentSlice;

	/* POLAR px */
	ExecSlice  *parentSlice = NULL;

	/*
	 * If this is a Motion node, we're descending into a new slice.
	 */
	if (IsA(plan, Motion))
	{
		Motion	   *pMotion = (Motion *) plan;
		SliceTable *sliceTable = planstate->state->es_sliceTable;

		if (sliceTable)
		{
			es->currentSlice = &sliceTable->slices[pMotion->motionID];
			parentSlice = (es->currentSlice->parentIndex == -1) ? NULL :
				&sliceTable->slices[es->currentSlice->parentIndex];
		}
	}
	/* POLAR end */

	switch (nodeTag(plan))
	{
		case T_Result:
			pname = sname = "Result";
			break;
		case T_ProjectSet:
			pname = sname = "ProjectSet";
			break;
		case T_ModifyTable:
			sname = "ModifyTable";
			switch (((ModifyTable *) plan)->operation)
			{
				case CMD_INSERT:
					pname = operation = "Insert";
					break;
				case CMD_UPDATE:
					if (es->pstmt && PLANGEN_PG == es->pstmt->planGen)
					{
						pname = operation = "Update";
					}
					else
					{
						operation = "Update";
						pname = psprintf("Update (segment: %d)", px_update_dop_num);
					}
					break;
				case CMD_DELETE:
					if (es->pstmt && PLANGEN_PG == es->pstmt->planGen)
					{
						pname = operation = "Delete";
					}
					else
					{
						operation = "Delete";
						pname = psprintf("Delete (segment: %d)", px_delete_dop_num);
					}
					break;
				default:
					pname = "???";
					break;
			}
			break;
		case T_Append:
			pname = sname = "Append";
			break;
		case T_MergeAppend:
			pname = sname = "Merge Append";
			break;
		case T_RecursiveUnion:
			pname = sname = "Recursive Union";
			break;
		/* POLAR px */
		case T_Sequence:
			pname = sname = "Sequence";
			break;
		case T_BitmapAnd:
			pname = sname = "BitmapAnd";
			break;
		case T_BitmapOr:
			pname = sname = "BitmapOr";
			break;
		case T_NestLoop:
			pname = sname = "Nested Loop";
			break;
		case T_MergeJoin:
			pname = "Merge";	/* "Join" gets added by jointype switch */
			sname = "Merge Join";
			break;
		case T_HashJoin:
			pname = "Hash";		/* "Join" gets added by jointype switch */
			sname = "Hash Join";
			break;
		case T_SeqScan:
			pname = sname = "Seq Scan";
			break;
		case T_SampleScan:
			pname = sname = "Sample Scan";
			break;
		case T_Gather:
			pname = sname = "Gather";
			break;
		case T_GatherMerge:
			pname = sname = "Gather Merge";
			break;
		case T_IndexScan:
			pname = sname = "Index Scan";
			break;
		case T_IndexOnlyScan:
			pname = sname = "Index Only Scan";
			break;
		case T_BitmapIndexScan:
			pname = sname = "Bitmap Index Scan";
			break;
		case T_BitmapHeapScan:
			pname = sname = "Bitmap Heap Scan";
			break;
		case T_TidScan:
			pname = sname = "Tid Scan";
			break;
		case T_SubqueryScan:
			pname = sname = "Subquery Scan";
			break;
		case T_FunctionScan:
			pname = sname = "Function Scan";
			break;
		case T_TableFuncScan:
			pname = sname = "Table Function Scan";
			break;
		case T_ValuesScan:
			pname = sname = "Values Scan";
			break;
		case T_CteScan:
			pname = sname = "CTE Scan";
			break;
		case T_NamedTuplestoreScan:
			pname = sname = "Named Tuplestore Scan";
			break;
		case T_WorkTableScan:
			pname = sname = "WorkTable Scan";
			break;
		/* POLAR px */
		case T_ShareInputScan:
			pname = sname = "Shared Scan";
			break;
		case T_ForeignScan:
			sname = "Foreign Scan";
			switch (((ForeignScan *) plan)->operation)
			{
				case CMD_SELECT:
					pname = "Foreign Scan";
					operation = "Select";
					break;
				case CMD_INSERT:
					pname = "Foreign Insert";
					operation = "Insert";
					break;
				case CMD_UPDATE:
					pname = "Foreign Update";
					operation = "Update";
					break;
				case CMD_DELETE:
					pname = "Foreign Delete";
					operation = "Delete";
					break;
				default:
					pname = "???";
					break;
			}
			break;
		case T_CustomScan:
			sname = "Custom Scan";
			custom_name = ((CustomScan *) plan)->methods->CustomName;
			if (custom_name)
				pname = psprintf("Custom Scan (%s)", custom_name);
			else
				pname = sname;
			break;
		case T_Material:
			pname = sname = "Materialize";
			break;
		case T_Sort:
			pname = sname = "Sort";
			break;
		case T_Group:
			pname = sname = "Group";
			break;
		case T_Agg:
			{
				Agg		   *agg = (Agg *) plan;

				sname = "Aggregate";
				switch (agg->aggstrategy)
				{
					case AGG_PLAIN:
						pname = "Aggregate";
						strategy = "Plain";
						break;
					case AGG_SORTED:
						pname = "GroupAggregate";
						strategy = "Sorted";
						break;
					case AGG_HASHED:
						pname = "HashAggregate";
						strategy = "Hashed";
						break;
					case AGG_MIXED:
						pname = "MixedAggregate";
						strategy = "Mixed";
						break;
					default:
						pname = "Aggregate ???";
						strategy = "???";
						break;
				}

				if (DO_AGGSPLIT_SKIPFINAL(agg->aggsplit))
				{
					partialmode = "Partial";
					pname = psprintf("%s %s", partialmode, pname);
				}
				else if (DO_AGGSPLIT_COMBINE(agg->aggsplit))
				{
					partialmode = "Finalize";
					pname = psprintf("%s %s", partialmode, pname);
				}
				else
					partialmode = "Simple";
			}
			break;
		case T_WindowAgg:
			pname = sname = "WindowAgg";
			break;
		case T_Unique:
			pname = sname = "Unique";
			break;
		case T_SetOp:
			sname = "SetOp";
			switch (((SetOp *) plan)->strategy)
			{
				case SETOP_SORTED:
					pname = "SetOp";
					strategy = "Sorted";
					break;
				case SETOP_HASHED:
					pname = "HashSetOp";
					strategy = "Hashed";
					break;
				default:
					pname = "SetOp ???";
					strategy = "???";
					break;
			}
			break;
		case T_LockRows:
			pname = sname = "LockRows";
			break;
		case T_Limit:
			pname = sname = "Limit";
			break;
		case T_Hash:
			pname = sname = "Hash";
			break;
		/* POLAR px */
		case T_Motion:
			{
				Motion		*pMotion = (Motion *) plan;

				Assert(plan->lefttree);

				motion_snd = list_length(es->currentSlice->segments);
				motion_recv = (parentSlice == NULL ? 1 : list_length(parentSlice->segments));

				switch (pMotion->motionType)
				{
					case MOTIONTYPE_GATHER:
						sname = "PX Coordinator";
						motion_recv = 1;
						break;
					case MOTIONTYPE_GATHER_SINGLE:
						sname = "PX Explicit Coordinator";
						motion_recv = 1;
						break;
					case MOTIONTYPE_HASH:
						sname = "PX Hash";
						break;
					case MOTIONTYPE_BROADCAST:
						sname = "PX Broadcast";
						break;
					case MOTIONTYPE_EXPLICIT:
						sname = "PX Explicit Hash";
						break;
					default:
						sname = "???";
						break;
				}

				pname = psprintf("%s %d:%d", sname, motion_snd, motion_recv);
			}
			break;
		case T_SplitUpdate:
				pname = sname = "Split";
			break;
		case T_PartitionSelector:
			pname = sname = "Partition Selector";
			break;
		case T_AssertOp:
			pname = sname = "Assert";
			break;
		/* POLAR end */
		default:
			pname = sname = "???";
			break;
	}

	/* POLAR px */
	if ((es->pstmt->planGen == PLANGEN_PX) && (IsA(plan, SeqScan) ||
												IsA(plan, IndexScan) ||
												IsA(plan, IndexOnlyScan) ||
												IsA(plan, BitmapIndexScan) ||
												IsA(plan, BitmapHeapScan) ||
												IsA(plan, SampleScan)))
	{
		if(plan->px_scan_partial){
			pname = psprintf("%s %s", "Partial", pname);
		}
		else
		{
			pname = psprintf("%s %s", "Full", pname);
		}
	}
	/* POLAR end */

	ExplainOpenGroup("Plan",
					 relationship ? NULL : "Plan",
					 true, es);

	if (es->format == EXPLAIN_FORMAT_TEXT)
	{
		if (plan_name)
		{
			appendStringInfoSpaces(es->str, es->indent * 2);
			appendStringInfo(es->str, "%s\n", plan_name);
			/* POLAR px :
			 * Show slice information after the plan name.
			 *
			 * Note: If the top node was a Motion node, we print the slice
			 * *above* the Motion here. We will print the slice below the
			 * Motion, below.
			 */
			if (IsA(plan, Motion))
			{
				show_dispatch_info(save_currentSlice, es, plan);
				appendStringInfoChar(es->str, '\n');
			}
			/* POLAR end */
			es->indent++;
		}
		if (es->indent)
		{
			appendStringInfoSpaces(es->str, es->indent * 2);
			appendStringInfoString(es->str, "->  ");
			es->indent += 2;
		}
		if (plan->parallel_aware)
			appendStringInfoString(es->str, "Parallel ");
		appendStringInfoString(es->str, pname);

		/* POLAR px :
		 * Print information about the current slice. In order to not make
		 * the output too verbose, only print it at the slice boundaries,
		 * ie. at Motion nodes. (We already switched the "current slice"
		 * to the slice below the Motion.)
		 */
		if (IsA(plan, Motion))
			show_dispatch_info(es->currentSlice, es, plan);
		/* POLAR end */

		es->indent++;
	}
	else
	{
		ExplainPropertyText("Node Type", sname, es);
		/* POLAR px */
		if (nodeTag(plan) == T_Motion)
		{
			ExplainPropertyInteger("Senders", NULL, motion_snd, es);
			ExplainPropertyInteger("Receivers", NULL, motion_recv, es);
		}
		/* POLAR end */
		if (strategy)
			ExplainPropertyText("Strategy", strategy, es);
		if (partialmode)
			ExplainPropertyText("Partial Mode", partialmode, es);
		if (operation)
			ExplainPropertyText("Operation", operation, es);
		if (relationship)
			ExplainPropertyText("Parent Relationship", relationship, es);
		if (plan_name)
			ExplainPropertyText("Subplan Name", plan_name, es);
		if (custom_name)
			ExplainPropertyText("Custom Plan Provider", custom_name, es);
		ExplainPropertyBool("Parallel Aware", plan->parallel_aware, es);
	}

	switch (nodeTag(plan))
	{
		case T_SeqScan:
		case T_SampleScan:
		case T_BitmapHeapScan:
		case T_TidScan:
		case T_SubqueryScan:
		case T_FunctionScan:
		case T_TableFuncScan:
		case T_ValuesScan:
		case T_CteScan:
		case T_WorkTableScan:
			ExplainScanTarget((Scan *) plan, es);
			break;
		case T_ForeignScan:
		case T_CustomScan:
			if (((Scan *) plan)->scanrelid > 0)
				ExplainScanTarget((Scan *) plan, es);
			break;
		case T_IndexScan:
			{
				IndexScan  *indexscan = (IndexScan *) plan;

				ExplainIndexScanDetails(indexscan->indexid,
										indexscan->indexorderdir,
										es);
				ExplainScanTarget((Scan *) indexscan, es);
			}
			break;
		case T_IndexOnlyScan:
			{
				IndexOnlyScan *indexonlyscan = (IndexOnlyScan *) plan;

				ExplainIndexScanDetails(indexonlyscan->indexid,
										indexonlyscan->indexorderdir,
										es);
				ExplainScanTarget((Scan *) indexonlyscan, es);
			}
			break;
		case T_BitmapIndexScan:
			{
				BitmapIndexScan *bitmapindexscan = (BitmapIndexScan *) plan;
				const char *indexname =
				explain_get_index_name(bitmapindexscan->indexid);

				if (es->format == EXPLAIN_FORMAT_TEXT)
					appendStringInfo(es->str, " on %s",
									 quote_identifier(indexname));
				else
					ExplainPropertyText("Index Name", indexname, es);
			}
			break;
		case T_ModifyTable:
			ExplainModifyTarget((ModifyTable *) plan, es);
			break;
		case T_NestLoop:
		case T_MergeJoin:
		case T_HashJoin:
			{
				const char *jointype;

				switch (((Join *) plan)->jointype)
				{
					case JOIN_INNER:
						jointype = "Inner";
						break;
					case JOIN_LEFT:
						jointype = "Left";
						break;
					case JOIN_FULL:
						jointype = "Full";
						break;
					case JOIN_RIGHT:
						jointype = "Right";
						break;
					case JOIN_SEMI:
						jointype = "Semi";
						break;
					case JOIN_ANTI:
						jointype = "Anti";
						break;

					/* POLAR px */
					case JOIN_LASJ_NOTIN:
						jointype = "Left Anti Semi (Not-In)";
						break;

					default:
						jointype = "???";
						break;
				}
				if (es->format == EXPLAIN_FORMAT_TEXT)
				{
					/*
					 * For historical reasons, the join type is interpolated
					 * into the node type name...
					 */
					if (((Join *) plan)->jointype != JOIN_INNER)
						appendStringInfo(es->str, " %s Join", jointype);
					else if (!IsA(plan, NestLoop))
						appendStringInfoString(es->str, " Join");
				}
				else
					ExplainPropertyText("Join Type", jointype, es);
			}
			break;
		case T_SetOp:
			{
				const char *setopcmd;

				switch (((SetOp *) plan)->cmd)
				{
					case SETOPCMD_INTERSECT:
						setopcmd = "Intersect";
						break;
					case SETOPCMD_INTERSECT_ALL:
						setopcmd = "Intersect All";
						break;
					case SETOPCMD_EXCEPT:
						setopcmd = "Except";
						break;
					case SETOPCMD_EXCEPT_ALL:
						setopcmd = "Except All";
						break;
					default:
						setopcmd = "???";
						break;
				}
				if (es->format == EXPLAIN_FORMAT_TEXT)
					appendStringInfo(es->str, " %s", setopcmd);
				else
					ExplainPropertyText("Command", setopcmd, es);
			}
			break;
		/* POLAR px */
		case T_ShareInputScan:
			{
				ShareInputScan *sisc = (ShareInputScan *) plan;
				int				slice_id = -1;

				if (es->currentSlice)
					slice_id = es->currentSlice->sliceIndex;

				if (es->format == EXPLAIN_FORMAT_TEXT)
					appendStringInfo(es->str, " (%s; slice%d; share%d)",
						(slice_id == sisc->producer_slice_id
							? "Producer" : "Consumer"),
						slice_id, sisc->share_id);
				else
				{
					ExplainPropertyText("Identity", 
						(slice_id == sisc->producer_slice_id
							? "Producer" : "Consumer"),
						es);
					ExplainPropertyInteger("Share ID", NULL, sisc->share_id, es);
					ExplainPropertyInteger("Slice ID", NULL, slice_id, es);
				}
			}
			break;
		case T_PartitionSelector:
			{
				PartitionSelector *ps = (PartitionSelector *) plan;

				if (es->format == EXPLAIN_FORMAT_TEXT)
				{
					appendStringInfo(es->str, " (selector id: $%d)", ps->paramid);
				}
				else
				{
					ExplainPropertyInteger("Selector ID", NULL, ps->paramid, es);
				}
			}
			break;
		/* POLAR end */
		default:
			break;
	}

	if (es->costs)
	{
		if (es->format == EXPLAIN_FORMAT_TEXT)
		{
			appendStringInfo(es->str, "  (cost=%.2f..%.2f rows=%.0f width=%d)",
							 plan->startup_cost, plan->total_cost,
							 plan->plan_rows, plan->plan_width);
		}
		else
		{
			ExplainPropertyFloat("Startup Cost", NULL, plan->startup_cost,
								 2, es);
			ExplainPropertyFloat("Total Cost", NULL, plan->total_cost,
								 2, es);
			ExplainPropertyFloat("Plan Rows", NULL, plan->plan_rows,
								 0, es);
			ExplainPropertyInteger("Plan Width", NULL, plan->plan_width,
								   es);
		}
	}

	/*
	 * We have to forcibly clean up the instrumentation state because we
	 * haven't done ExecutorEnd yet.  This is pretty grotty ...
	 *
	 * Note: contrib/auto_explain could cause instrumentation to be set up
	 * even though we didn't ask for it here.  Be careful not to print any
	 * instrumentation results the user didn't ask for.  But we do the
	 * InstrEndLoop call anyway, if possible, to reduce the number of cases
	 * auto_explain has to contend with.
	 */
	if (planstate->instrument)
		InstrEndLoop(planstate->instrument);

	if (es->analyze &&
		planstate->instrument && planstate->instrument->nloops > 0)
	{
		double		nloops = planstate->instrument->nloops;
		double		startup_ms = 1000.0 * planstate->instrument->startup / nloops;
		double		total_ms = 1000.0 * planstate->instrument->total / nloops;
		double		rows = planstate->instrument->ntuples / nloops;

		if (es->format == EXPLAIN_FORMAT_TEXT)
		{
			if (es->timing)
				appendStringInfo(es->str,
								 " (actual time=%.3f..%.3f rows=%.0f loops=%.0f)",
								 startup_ms, total_ms, rows, nloops);
			else
				appendStringInfo(es->str,
								 " (actual rows=%.0f loops=%.0f)",
								 rows, nloops);
		}
		else
		{
			if (es->timing)
			{
				ExplainPropertyFloat("Actual Startup Time", "s", startup_ms,
									 3, es);
				ExplainPropertyFloat("Actual Total Time", "s", total_ms,
									 3, es);
			}
			ExplainPropertyFloat("Actual Rows", NULL, rows, 0, es);
			ExplainPropertyFloat("Actual Loops", NULL, nloops, 0, es);
		}
	}
	else if (es->analyze)
	{
		if (es->format == EXPLAIN_FORMAT_TEXT)
			appendStringInfoString(es->str, " (never executed)");
		else
		{
			if (es->timing)
			{
				ExplainPropertyFloat("Actual Startup Time", "ms", 0.0, 3, es);
				ExplainPropertyFloat("Actual Total Time", "ms", 0.0, 3, es);
			}
			ExplainPropertyFloat("Actual Rows", NULL, 0.0, 0, es);
			ExplainPropertyFloat("Actual Loops", NULL, 0.0, 0, es);
		}
	}

	/* in text format, first line ends here */
	if (es->format == EXPLAIN_FORMAT_TEXT)
		appendStringInfoChar(es->str, '\n');

	/* target list */
	if (es->verbose)
		show_plan_tlist(planstate, ancestors, es);

	/* unique join */
	switch (nodeTag(plan))
	{
		case T_NestLoop:
		case T_MergeJoin:
		case T_HashJoin:
			/* try not to be too chatty about this in text mode */
			if (es->format != EXPLAIN_FORMAT_TEXT ||
				(es->verbose && ((Join *) plan)->inner_unique))
				ExplainPropertyBool("Inner Unique",
									((Join *) plan)->inner_unique,
									es);
			break;
		default:
			break;
	}

	/* quals, sort keys, etc */
	switch (nodeTag(plan))
	{
		case T_IndexScan:
			show_scan_qual(((IndexScan *) plan)->indexqualorig,
						   "Index Cond", planstate, ancestors, es);
			if (((IndexScan *) plan)->indexqualorig)
				show_instrumentation_count("Rows Removed by Index Recheck", 2,
										   planstate, es);
			show_scan_qual(((IndexScan *) plan)->indexorderbyorig,
						   "Order By", planstate, ancestors, es);
			show_scan_qual(plan->qual, "Filter", planstate, ancestors, es);
			if (plan->qual)
				show_instrumentation_count("Rows Removed by Filter", 1,
										   planstate, es);
			break;
		case T_IndexOnlyScan:
			show_scan_qual(((IndexOnlyScan *) plan)->indexqual,
						   "Index Cond", planstate, ancestors, es);
			if (((IndexOnlyScan *) plan)->indexqual)
				show_instrumentation_count("Rows Removed by Index Recheck", 2,
										   planstate, es);
			show_scan_qual(((IndexOnlyScan *) plan)->indexorderby,
						   "Order By", planstate, ancestors, es);
			show_scan_qual(plan->qual, "Filter", planstate, ancestors, es);
			if (plan->qual)
				show_instrumentation_count("Rows Removed by Filter", 1,
										   planstate, es);
			if (es->analyze)
				ExplainPropertyFloat("Heap Fetches", NULL,
									 planstate->instrument->ntuples2, 0, es);
			break;
		case T_BitmapIndexScan:
			show_scan_qual(((BitmapIndexScan *) plan)->indexqualorig,
						   "Index Cond", planstate, ancestors, es);
			break;
		case T_BitmapHeapScan:
			show_scan_qual(((BitmapHeapScan *) plan)->bitmapqualorig,
						   "Recheck Cond", planstate, ancestors, es);
			if (((BitmapHeapScan *) plan)->bitmapqualorig)
				show_instrumentation_count("Rows Removed by Index Recheck", 2,
										   planstate, es);
			show_scan_qual(plan->qual, "Filter", planstate, ancestors, es);
			if (plan->qual)
				show_instrumentation_count("Rows Removed by Filter", 1,
										   planstate, es);
			if (es->analyze)
				show_tidbitmap_info((BitmapHeapScanState *) planstate, es);
			break;
		case T_SampleScan:
			show_tablesample(((SampleScan *) plan)->tablesample,
							 planstate, ancestors, es);
			/* fall through to print additional fields the same as SeqScan */
			/* FALLTHROUGH */
		case T_SeqScan:
		case T_ValuesScan:
		case T_CteScan:
		case T_NamedTuplestoreScan:
		case T_WorkTableScan:
		case T_SubqueryScan:
			show_scan_qual(plan->qual, "Filter", planstate, ancestors, es);
			if (plan->qual)
				show_instrumentation_count("Rows Removed by Filter", 1,
										   planstate, es);
			break;
		case T_Gather:
			{
				Gather	   *gather = (Gather *) plan;

				show_scan_qual(plan->qual, "Filter", planstate, ancestors, es);
				if (plan->qual)
					show_instrumentation_count("Rows Removed by Filter", 1,
											   planstate, es);
				ExplainPropertyInteger("Workers Planned", NULL,
									   gather->num_workers, es);

				/* Show params evaluated at gather node */
				if (gather->initParam)
					show_eval_params(gather->initParam, es);

				if (es->analyze)
				{
					int			nworkers;

					nworkers = ((GatherState *) planstate)->nworkers_launched;
					ExplainPropertyInteger("Workers Launched", NULL,
										   nworkers, es);
				}

				/*
				 * Print per-worker Jit instrumentation. Use same conditions
				 * as for the leader's JIT instrumentation, see comment there.
				 */
				if (es->costs && es->verbose &&
					outerPlanState(planstate)->worker_jit_instrument)
				{
					PlanState *child = outerPlanState(planstate);
					int			n;
					SharedJitInstrumentation *w = child->worker_jit_instrument;

					for (n = 0; n < w->num_workers; ++n)
					{
						ExplainPrintJIT(es, child->state->es_jit_flags,
										&w->jit_instr[n], n);
					}
				}

				if (gather->single_copy || es->format != EXPLAIN_FORMAT_TEXT)
					ExplainPropertyBool("Single Copy", gather->single_copy, es);
			}
			break;
		case T_GatherMerge:
			{
				GatherMerge *gm = (GatherMerge *) plan;

				show_scan_qual(plan->qual, "Filter", planstate, ancestors, es);
				if (plan->qual)
					show_instrumentation_count("Rows Removed by Filter", 1,
											   planstate, es);
				ExplainPropertyInteger("Workers Planned", NULL,
									   gm->num_workers, es);

				/* Show params evaluated at gather-merge node */
				if (gm->initParam)
					show_eval_params(gm->initParam, es);

				if (es->analyze)
				{
					int			nworkers;

					nworkers = ((GatherMergeState *) planstate)->nworkers_launched;
					ExplainPropertyInteger("Workers Launched", NULL,
										   nworkers, es);
				}
			}
			break;
		case T_FunctionScan:
			if (es->verbose)
			{
				List	   *fexprs = NIL;
				ListCell   *lc;

				foreach(lc, ((FunctionScan *) plan)->functions)
				{
					RangeTblFunction *rtfunc = (RangeTblFunction *) lfirst(lc);

					fexprs = lappend(fexprs, rtfunc->funcexpr);
				}
				/* We rely on show_expression to insert commas as needed */
				show_expression((Node *) fexprs,
								"Function Call", planstate, ancestors,
								es->verbose, es);
			}
			show_scan_qual(plan->qual, "Filter", planstate, ancestors, es);
			if (plan->qual)
				show_instrumentation_count("Rows Removed by Filter", 1,
										   planstate, es);
			break;
		case T_TableFuncScan:
			if (es->verbose)
			{
				TableFunc  *tablefunc = ((TableFuncScan *) plan)->tablefunc;

				show_expression((Node *) tablefunc,
								"Table Function Call", planstate, ancestors,
								es->verbose, es);
			}
			show_scan_qual(plan->qual, "Filter", planstate, ancestors, es);
			if (plan->qual)
				show_instrumentation_count("Rows Removed by Filter", 1,
										   planstate, es);
			break;
		case T_TidScan:
			{
				/*
				 * The tidquals list has OR semantics, so be sure to show it
				 * as an OR condition.
				 */
				List	   *tidquals = ((TidScan *) plan)->tidquals;

				if (list_length(tidquals) > 1)
					tidquals = list_make1(make_orclause(tidquals));
				show_scan_qual(tidquals, "TID Cond", planstate, ancestors, es);
				show_scan_qual(plan->qual, "Filter", planstate, ancestors, es);
				if (plan->qual)
					show_instrumentation_count("Rows Removed by Filter", 1,
											   planstate, es);
			}
			break;
		case T_ForeignScan:
			show_scan_qual(plan->qual, "Filter", planstate, ancestors, es);
			if (plan->qual)
				show_instrumentation_count("Rows Removed by Filter", 1,
										   planstate, es);
			show_foreignscan_info((ForeignScanState *) planstate, es);
			break;
		case T_CustomScan:
			{
				CustomScanState *css = (CustomScanState *) planstate;

				show_scan_qual(plan->qual, "Filter", planstate, ancestors, es);
				if (plan->qual)
					show_instrumentation_count("Rows Removed by Filter", 1,
											   planstate, es);
				if (css->methods->ExplainCustomScan)
					css->methods->ExplainCustomScan(css, ancestors, es);
			}
			break;
		case T_NestLoop:
			show_upper_qual(((NestLoop *) plan)->join.joinqual,
							"Join Filter", planstate, ancestors, es);
			if (((NestLoop *) plan)->join.joinqual)
				show_instrumentation_count("Rows Removed by Join Filter", 1,
										   planstate, es);
			show_upper_qual(plan->qual, "Filter", planstate, ancestors, es);
			if (plan->qual)
				show_instrumentation_count("Rows Removed by Filter", 2,
										   planstate, es);
			break;
		case T_MergeJoin:
			show_upper_qual(((MergeJoin *) plan)->mergeclauses,
							"Merge Cond", planstate, ancestors, es);
			show_upper_qual(((MergeJoin *) plan)->join.joinqual,
							"Join Filter", planstate, ancestors, es);
			if (((MergeJoin *) plan)->join.joinqual)
				show_instrumentation_count("Rows Removed by Join Filter", 1,
										   planstate, es);
			show_upper_qual(plan->qual, "Filter", planstate, ancestors, es);
			if (plan->qual)
				show_instrumentation_count("Rows Removed by Filter", 2,
										   planstate, es);
			break;
		case T_HashJoin:
			show_upper_qual(((HashJoin *) plan)->hashclauses,
							"Hash Cond", planstate, ancestors, es);
			show_upper_qual(((HashJoin *) plan)->join.joinqual,
							"Join Filter", planstate, ancestors, es);
			if (((HashJoin *) plan)->join.joinqual)
				show_instrumentation_count("Rows Removed by Join Filter", 1,
										   planstate, es);
			show_upper_qual(plan->qual, "Filter", planstate, ancestors, es);
			if (plan->qual)
				show_instrumentation_count("Rows Removed by Filter", 2,
										   planstate, es);
			break;
		case T_Agg:
			show_agg_keys(castNode(AggState, planstate), ancestors, es);
			show_upper_qual(plan->qual, "Filter", planstate, ancestors, es);
			if (plan->qual)
				show_instrumentation_count("Rows Removed by Filter", 1,
										   planstate, es);
			break;
		case T_Group:
			show_group_keys(castNode(GroupState, planstate), ancestors, es);
			show_upper_qual(plan->qual, "Filter", planstate, ancestors, es);
			if (plan->qual)
				show_instrumentation_count("Rows Removed by Filter", 1,
										   planstate, es);
			break;
		case T_Sort:
			show_sort_keys(castNode(SortState, planstate), ancestors, es);
			show_sort_info(castNode(SortState, planstate), es);
			break;
		case T_MergeAppend:
			show_merge_append_keys(castNode(MergeAppendState, planstate),
								   ancestors, es);
			break;
		case T_Result:
			show_upper_qual((List *) ((Result *) plan)->resconstantqual,
							"One-Time Filter", planstate, ancestors, es);
			show_upper_qual(plan->qual, "Filter", planstate, ancestors, es);
			if (plan->qual)
				show_instrumentation_count("Rows Removed by Filter", 1,
										   planstate, es);

			/* POLAR px */
			if (castNode(ResultState, planstate)->hashFilter)
				show_result_hash_filter("HashFilter", 
										castNode(ResultState, planstate),
										es);
			break;
		case T_ModifyTable:
			show_modifytable_info(castNode(ModifyTableState, planstate), ancestors,
								  es);
			break;
		case T_Hash:
			show_hash_info(castNode(HashState, planstate), es);
			break;
		/* POLAR px */
		case T_Motion:
			{
				Motion	   *pMotion = (Motion *) plan;

				if (pMotion->sendSorted || pMotion->motionType == MOTIONTYPE_HASH)
					show_motion_keys(planstate,
									 pMotion->hashExprs,
									 pMotion->numSortCols,
									 pMotion->sortColIdx,
									 "Merge Key",
									 ancestors, es);
			}
			break;
		case T_Append:
			polar_show_join_pruning_info(((Append *) plan)->join_prune_paramids, es);
			break;
		case T_AssertOp:
			show_upper_qual(plan->qual, "Assert Cond", planstate, ancestors, es);
			break;
		/* POLAR end */
		default:
			break;
	}

	/* POLAR px : Show executor statistics */
	if (planstate->instrument && planstate->instrument->need_px)
		pxexplain_showExecStats(planstate, es);
	/* POLAR end */

	/* Show buffer usage */
	if (es->buffers && planstate->instrument)
		show_buffer_usage(es, &planstate->instrument->bufusage);

	/* Show worker detail */
	if (es->analyze && es->verbose && planstate->worker_instrument)
	{
		WorkerInstrumentation *w = planstate->worker_instrument;
		bool		opened_group = false;
		int			n;

		for (n = 0; n < w->num_workers; ++n)
		{
			Instrumentation *instrument = &w->instrument[n];
			double		nloops = instrument->nloops;
			double		startup_ms;
			double		total_ms;
			double		rows;

			if (nloops <= 0)
				continue;
			startup_ms = 1000.0 * instrument->startup / nloops;
			total_ms = 1000.0 * instrument->total / nloops;
			rows = instrument->ntuples / nloops;

			if (es->format == EXPLAIN_FORMAT_TEXT)
			{
				appendStringInfoSpaces(es->str, es->indent * 2);
				appendStringInfo(es->str, "Worker %d: ", n);
				if (es->timing)
					appendStringInfo(es->str,
									 "actual time=%.3f..%.3f rows=%.0f loops=%.0f\n",
									 startup_ms, total_ms, rows, nloops);
				else
					appendStringInfo(es->str,
									 "actual rows=%.0f loops=%.0f\n",
									 rows, nloops);
				es->indent++;
				if (es->buffers)
					show_buffer_usage(es, &instrument->bufusage);
				es->indent--;
			}
			else
			{
				if (!opened_group)
				{
					ExplainOpenGroup("Workers", "Workers", false, es);
					opened_group = true;
				}
				ExplainOpenGroup("Worker", NULL, true, es);
				ExplainPropertyInteger("Worker Number", NULL, n, es);

				if (es->timing)
				{
					ExplainPropertyFloat("Actual Startup Time", "ms",
										 startup_ms, 3, es);
					ExplainPropertyFloat("Actual Total Time", "ms",
										 total_ms, 3, es);
				}
				ExplainPropertyFloat("Actual Rows", NULL, rows, 0, es);
				ExplainPropertyFloat("Actual Loops", NULL, nloops, 0, es);

				if (es->buffers)
					show_buffer_usage(es, &instrument->bufusage);

				ExplainCloseGroup("Worker", NULL, true, es);
			}
		}

		if (opened_group)
			ExplainCloseGroup("Workers", "Workers", false, es);
	}

	/*
	 * If partition pruning was done during executor initialization, the
	 * number of child plans we'll display below will be less than the number
	 * of subplans that was specified in the plan.  To make this a bit less
	 * mysterious, emit an indication that this happened.  Note that this
	 * field is emitted now because we want it to be a property of the parent
	 * node; it *cannot* be emitted within the Plans sub-node we'll open next.
	 */
	switch (nodeTag(plan))
	{
		case T_Append:
			ExplainMissingMembers(((AppendState *) planstate)->as_nplans,
								  list_length(((Append *) plan)->appendplans),
								  es);
			break;
		case T_MergeAppend:
			ExplainMissingMembers(((MergeAppendState *) planstate)->ms_nplans,
								  list_length(((MergeAppend *) plan)->mergeplans),
								  es);
			break;
		default:
			break;
	}

	/*
	 * POLAR px: dynamic paging analyze
	 */
	if (es->analyze &&
		nodeTag(plan) == T_SeqScan &&
		es->summary &&
		polar_enable_px &&
		px_adaptive_paging &&
		px_enable_adps_explain_analyze)
	{
		char *msg_pages = NULL;
		px_adps_analyze_result(plan->plan_node_id, &msg_pages);
		if (msg_pages)
		{
			ExplainPropertyText("Dynamic Pages Per Worker", msg_pages, es);
			pfree(msg_pages);
		}
	}
	/*POLAR end*/

	/* Get ready to display the child plans */
	haschildren = planstate->initPlan ||
		outerPlanState(planstate) ||
		innerPlanState(planstate) ||
		IsA(plan, ModifyTable) ||
		IsA(plan, Append) ||
		IsA(plan, MergeAppend) ||
		/* POLAR px */
		IsA(plan, Sequence) ||
		IsA(plan, BitmapAnd) ||
		IsA(plan, BitmapOr) ||
		IsA(plan, SubqueryScan) ||
		(IsA(planstate, CustomScanState) &&
		 ((CustomScanState *) planstate)->custom_ps != NIL) ||
		planstate->subPlan;
	if (haschildren)
	{
		ExplainOpenGroup("Plans", "Plans", false, es);
		/* Pass current PlanState as head of ancestors list for children */
		ancestors = lcons(planstate, ancestors);
	}

	/* initPlan-s */
	if (planstate->initPlan)
		ExplainSubPlans(planstate->initPlan, ancestors, "InitPlan", es);

	/* lefttree */
	if (outerPlanState(planstate))
		ExplainNode(outerPlanState(planstate), ancestors,
					"Outer", NULL, es);

	/* righttree */
	if (innerPlanState(planstate))
		ExplainNode(innerPlanState(planstate), ancestors,
					"Inner", NULL, es);

	/* special child plans */
	switch (nodeTag(plan))
	{
		case T_ModifyTable:
			ExplainMemberNodes(((ModifyTableState *) planstate)->mt_plans,
							   ((ModifyTableState *) planstate)->mt_nplans,
							   ancestors, es);
			break;
		case T_Append:
			ExplainMemberNodes(((AppendState *) planstate)->appendplans,
							   ((AppendState *) planstate)->as_nplans,
							   ancestors, es);
			break;
		case T_MergeAppend:
			ExplainMemberNodes(((MergeAppendState *) planstate)->mergeplans,
							   ((MergeAppendState *) planstate)->ms_nplans,
							   ancestors, es);
			break;
		/* POLAR px */
		case T_Sequence:
			ExplainMemberNodes(((SequenceState *) planstate)->subplans,
							   ((SequenceState *) planstate)->numSubplans,
								ancestors, es);
			break;
		case T_BitmapAnd:
			ExplainMemberNodes(((BitmapAndState *) planstate)->bitmapplans,
							   ((BitmapAndState *) planstate)->nplans,
							   ancestors, es);
			break;
		case T_BitmapOr:
			ExplainMemberNodes(((BitmapOrState *) planstate)->bitmapplans,
							   ((BitmapOrState *) planstate)->nplans,
							   ancestors, es);
			break;
		case T_SubqueryScan:
			ExplainNode(((SubqueryScanState *) planstate)->subplan, ancestors,
						"Subquery", NULL, es);
			break;
		case T_CustomScan:
			ExplainCustomChildren((CustomScanState *) planstate,
								  ancestors, es);
			break;
		default:
			break;
	}

	/* subPlan-s */
	if (planstate->subPlan)
		ExplainSubPlans(planstate->subPlan, ancestors, "SubPlan", es);

	/* end of child plans */
	if (haschildren)
	{
		ancestors = list_delete_first(ancestors);
		ExplainCloseGroup("Plans", "Plans", false, es);
	}

	/* in text format, undo whatever indentation we added */
	if (es->format == EXPLAIN_FORMAT_TEXT)
		es->indent = save_indent;

	ExplainCloseGroup("Plan",
					  relationship ? NULL : "Plan",
					  true, es);

	/* POLAR px */	
	es->currentSlice = save_currentSlice;
}

/*
 * Show the targetlist of a plan node
 */
static void
show_plan_tlist(PlanState *planstate, List *ancestors, ExplainState *es)
{
	Plan	   *plan = planstate->plan;
	List	   *context;
	List	   *result = NIL;
	bool		useprefix;
	ListCell   *lc;

	/* No work if empty tlist (this occurs eg in bitmap indexscans) */
	if (plan->targetlist == NIL)
		return;
	/* The tlist of an Append isn't real helpful, so suppress it */
	if (IsA(plan, Append))
		return;
	/* Likewise for MergeAppend and RecursiveUnion */
	if (IsA(plan, MergeAppend))
		return;
	if (IsA(plan, RecursiveUnion))
		return;

	/*
	 * Likewise for ForeignScan that executes a direct INSERT/UPDATE/DELETE
	 *
	 * Note: the tlist for a ForeignScan that executes a direct INSERT/UPDATE
	 * might contain subplan output expressions that are confusing in this
	 * context.  The tlist for a ForeignScan that executes a direct UPDATE/
	 * DELETE always contains "junk" target columns to identify the exact row
	 * to update or delete, which would be confusing in this context.  So, we
	 * suppress it in all the cases.
	 */
	if (IsA(plan, ForeignScan) &&
		((ForeignScan *) plan)->operation != CMD_SELECT)
		return;

	/* Set up deparsing context */
	context = set_deparse_context_planstate(es->deparse_cxt,
											(Node *) planstate,
											ancestors);
	useprefix = list_length(es->rtable) > 1;

	/* Deparse each result column (we now include resjunk ones) */
	foreach(lc, plan->targetlist)
	{
		TargetEntry *tle = (TargetEntry *) lfirst(lc);

		result = lappend(result,
						 deparse_expression((Node *) tle->expr, context,
											useprefix, false));
	}

	/* Print results */
	ExplainPropertyList("Output", result, es);
}

/*
 * Show a generic expression
 */
static void
show_expression(Node *node, const char *qlabel,
				PlanState *planstate, List *ancestors,
				bool useprefix, ExplainState *es)
{
	List	   *context;
	char	   *exprstr;

	/* Set up deparsing context */
	context = set_deparse_context_planstate(es->deparse_cxt,
											(Node *) planstate,
											ancestors);

	/* Deparse the expression */
	exprstr = deparse_expression(node, context, useprefix, false);

	/* And add to es->str */
	ExplainPropertyText(qlabel, exprstr, es);
}

/*
 * Show a qualifier expression (which is a List with implicit AND semantics)
 */
static void
show_qual(List *qual, const char *qlabel,
		  PlanState *planstate, List *ancestors,
		  bool useprefix, ExplainState *es)
{
	Node	   *node;

	/* No work if empty qual */
	if (qual == NIL)
		return;

	/* Convert AND list to explicit AND */
	node = (Node *) make_ands_explicit(qual);

	/* And show it */
	show_expression(node, qlabel, planstate, ancestors, useprefix, es);
}

/*
 * Show a qualifier expression for a scan plan node
 */
static void
show_scan_qual(List *qual, const char *qlabel,
			   PlanState *planstate, List *ancestors,
			   ExplainState *es)
{
	bool		useprefix;

	useprefix = (IsA(planstate->plan, SubqueryScan) ||es->verbose);
	show_qual(qual, qlabel, planstate, ancestors, useprefix, es);
}

/*
 * Show a qualifier expression for an upper-level plan node
 */
static void
show_upper_qual(List *qual, const char *qlabel,
				PlanState *planstate, List *ancestors,
				ExplainState *es)
{
	bool		useprefix;

	useprefix = (list_length(es->rtable) > 1 || es->verbose);
	show_qual(qual, qlabel, planstate, ancestors, useprefix, es);
}

/*
 * Show the sort keys for a Sort node.
 */
static void
show_sort_keys(SortState *sortstate, List *ancestors, ExplainState *es)
{
	Sort	   *plan = (Sort *) sortstate->ss.ps.plan;

	show_sort_group_keys((PlanState *) sortstate, "Sort Key",
						 plan->numCols, plan->sortColIdx,
						 plan->sortOperators, plan->collations,
						 plan->nullsFirst,
						 ancestors, es);
}

/*
 * Likewise, for a MergeAppend node.
 */
static void
show_merge_append_keys(MergeAppendState *mstate, List *ancestors,
					   ExplainState *es)
{
	MergeAppend *plan = (MergeAppend *) mstate->ps.plan;

	show_sort_group_keys((PlanState *) mstate, "Sort Key",
						 plan->numCols, plan->sortColIdx,
						 plan->sortOperators, plan->collations,
						 plan->nullsFirst,
						 ancestors, es);
}

/*
 * Show the grouping keys for an Agg node.
 */
static void
show_agg_keys(AggState *astate, List *ancestors,
			  ExplainState *es)
{
	Agg		   *plan = (Agg *) astate->ss.ps.plan;

	if (plan->numCols > 0 || plan->groupingSets)
	{
		/* The key columns refer to the tlist of the child plan */
		ancestors = lcons(astate, ancestors);

		if (plan->groupingSets)
			show_grouping_sets(outerPlanState(astate), plan, ancestors, es);
		else
			show_sort_group_keys(outerPlanState(astate), "Group Key",
								 plan->numCols, plan->grpColIdx,
								 NULL, NULL, NULL,
								 ancestors, es);

		ancestors = list_delete_first(ancestors);
	}
}

static void
show_grouping_sets(PlanState *planstate, Agg *agg,
				   List *ancestors, ExplainState *es)
{
	List	   *context;
	bool		useprefix;
	ListCell   *lc;

	/* Set up deparsing context */
	context = set_deparse_context_planstate(es->deparse_cxt,
											(Node *) planstate,
											ancestors);
	useprefix = (list_length(es->rtable) > 1 || es->verbose);

	ExplainOpenGroup("Grouping Sets", "Grouping Sets", false, es);

	show_grouping_set_keys(planstate, agg, NULL,
						   context, useprefix, ancestors, es);

	foreach(lc, agg->chain)
	{
		Agg		   *aggnode = lfirst(lc);
		Sort	   *sortnode = (Sort *) aggnode->plan.lefttree;

		show_grouping_set_keys(planstate, aggnode, sortnode,
							   context, useprefix, ancestors, es);
	}

	ExplainCloseGroup("Grouping Sets", "Grouping Sets", false, es);
}

static void
show_grouping_set_keys(PlanState *planstate,
					   Agg *aggnode, Sort *sortnode,
					   List *context, bool useprefix,
					   List *ancestors, ExplainState *es)
{
	Plan	   *plan = planstate->plan;
	char	   *exprstr;
	ListCell   *lc;
	List	   *gsets = aggnode->groupingSets;
	AttrNumber *keycols = aggnode->grpColIdx;
	const char *keyname;
	const char *keysetname;

	if (aggnode->aggstrategy == AGG_HASHED || aggnode->aggstrategy == AGG_MIXED)
	{
		keyname = "Hash Key";
		keysetname = "Hash Keys";
	}
	else
	{
		keyname = "Group Key";
		keysetname = "Group Keys";
	}

	ExplainOpenGroup("Grouping Set", NULL, true, es);

	if (sortnode)
	{
		show_sort_group_keys(planstate, "Sort Key",
							 sortnode->numCols, sortnode->sortColIdx,
							 sortnode->sortOperators, sortnode->collations,
							 sortnode->nullsFirst,
							 ancestors, es);
		if (es->format == EXPLAIN_FORMAT_TEXT)
			es->indent++;
	}

	ExplainOpenGroup(keysetname, keysetname, false, es);

	foreach(lc, gsets)
	{
		List	   *result = NIL;
		ListCell   *lc2;

		foreach(lc2, (List *) lfirst(lc))
		{
			Index		i = lfirst_int(lc2);
			AttrNumber	keyresno = keycols[i];
			TargetEntry *target = get_tle_by_resno(plan->targetlist,
												   keyresno);

			if (!target)
				elog(ERROR, "no tlist entry for key %d", keyresno);
			/* Deparse the expression, showing any top-level cast */
			exprstr = deparse_expression((Node *) target->expr, context,
										 useprefix, true);

			result = lappend(result, exprstr);
		}

		if (!result && es->format == EXPLAIN_FORMAT_TEXT)
			ExplainPropertyText(keyname, "()", es);
		else
			ExplainPropertyListNested(keyname, result, es);
	}

	ExplainCloseGroup(keysetname, keysetname, false, es);

	if (sortnode && es->format == EXPLAIN_FORMAT_TEXT)
		es->indent--;

	ExplainCloseGroup("Grouping Set", NULL, true, es);
}

/*
 * Show the grouping keys for a Group node.
 */
static void
show_group_keys(GroupState *gstate, List *ancestors,
				ExplainState *es)
{
	Group	   *plan = (Group *) gstate->ss.ps.plan;

	/* The key columns refer to the tlist of the child plan */
	ancestors = lcons(gstate, ancestors);
	show_sort_group_keys(outerPlanState(gstate), "Group Key",
						 plan->numCols, plan->grpColIdx,
						 NULL, NULL, NULL,
						 ancestors, es);
	ancestors = list_delete_first(ancestors);
}

/*
 * Common code to show sort/group keys, which are represented in plan nodes
 * as arrays of targetlist indexes.  If it's a sort key rather than a group
 * key, also pass sort operators/collations/nullsFirst arrays.
 */
static void
show_sort_group_keys(PlanState *planstate, const char *qlabel,
					 int nkeys, AttrNumber *keycols,
					 Oid *sortOperators, Oid *collations, bool *nullsFirst,
					 List *ancestors, ExplainState *es)
{
	Plan	   *plan = planstate->plan;
	List	   *context;
	List	   *result = NIL;
	StringInfoData sortkeybuf;
	bool		useprefix;
	int			keyno;

	if (nkeys <= 0)
		return;

	initStringInfo(&sortkeybuf);

	/* Set up deparsing context */
	context = set_deparse_context_planstate(es->deparse_cxt,
											(Node *) planstate,
											ancestors);
	useprefix = (list_length(es->rtable) > 1 || es->verbose);

	for (keyno = 0; keyno < nkeys; keyno++)
	{
		/* find key expression in tlist */
		AttrNumber	keyresno = keycols[keyno];
		TargetEntry *target = get_tle_by_resno(plan->targetlist,
											   keyresno);
		char	   *exprstr;

		if (!target)
			elog(ERROR, "no tlist entry for key %d", keyresno);
		/* Deparse the expression, showing any top-level cast */
		exprstr = deparse_expression((Node *) target->expr, context,
									 useprefix, true);
		resetStringInfo(&sortkeybuf);
		appendStringInfoString(&sortkeybuf, exprstr);
		/* Append sort order information, if relevant */
		if (sortOperators != NULL)
			show_sortorder_options(&sortkeybuf,
								   (Node *) target->expr,
								   sortOperators[keyno],
								   collations[keyno],
								   nullsFirst[keyno]);
		/* Emit one property-list item per sort key */
		result = lappend(result, pstrdup(sortkeybuf.data));
	}

	ExplainPropertyList(qlabel, result, es);
}

/*
 * Append nondefault characteristics of the sort ordering of a column to buf
 * (collation, direction, NULLS FIRST/LAST)
 */
static void
show_sortorder_options(StringInfo buf, Node *sortexpr,
					   Oid sortOperator, Oid collation, bool nullsFirst)
{
	Oid			sortcoltype = exprType(sortexpr);
	bool		reverse = false;
	TypeCacheEntry *typentry;

	typentry = lookup_type_cache(sortcoltype,
								 TYPECACHE_LT_OPR | TYPECACHE_GT_OPR);

	/*
	 * Print COLLATE if it's not default.  There are some cases where this is
	 * redundant, eg if expression is a column whose declared collation is
	 * that collation, but it's hard to distinguish that here.
	 */
	if (OidIsValid(collation) && collation != DEFAULT_COLLATION_OID)
	{
		char	   *collname = get_collation_name(collation);

		if (collname == NULL)
			elog(ERROR, "cache lookup failed for collation %u", collation);
		appendStringInfo(buf, " COLLATE %s", quote_identifier(collname));
	}

	/* Print direction if not ASC, or USING if non-default sort operator */
	if (sortOperator == typentry->gt_opr)
	{
		appendStringInfoString(buf, " DESC");
		reverse = true;
	}
	else if (sortOperator != typentry->lt_opr)
	{
		char	   *opname = get_opname(sortOperator);

		if (opname == NULL)
			elog(ERROR, "cache lookup failed for operator %u", sortOperator);
		appendStringInfo(buf, " USING %s", opname);
		/* Determine whether operator would be considered ASC or DESC */
		(void) get_equality_op_for_ordering_op(sortOperator, &reverse);
	}

	/* Add NULLS FIRST/LAST only if it wouldn't be default */
	if (nullsFirst && !reverse)
	{
		appendStringInfoString(buf, " NULLS FIRST");
	}
	else if (!nullsFirst && reverse)
	{
		appendStringInfoString(buf, " NULLS LAST");
	}
}

/*
 * Show TABLESAMPLE properties
 */
static void
show_tablesample(TableSampleClause *tsc, PlanState *planstate,
				 List *ancestors, ExplainState *es)
{
	List	   *context;
	bool		useprefix;
	char	   *method_name;
	List	   *params = NIL;
	char	   *repeatable;
	ListCell   *lc;

	/* Set up deparsing context */
	context = set_deparse_context_planstate(es->deparse_cxt,
											(Node *) planstate,
											ancestors);
	useprefix = list_length(es->rtable) > 1;

	/* Get the tablesample method name */
	method_name = get_func_name(tsc->tsmhandler);

	/* Deparse parameter expressions */
	foreach(lc, tsc->args)
	{
		Node	   *arg = (Node *) lfirst(lc);

		params = lappend(params,
						 deparse_expression(arg, context,
											useprefix, false));
	}
	if (tsc->repeatable)
		repeatable = deparse_expression((Node *) tsc->repeatable, context,
										useprefix, false);
	else
		repeatable = NULL;

	/* Print results */
	if (es->format == EXPLAIN_FORMAT_TEXT)
	{
		bool		first = true;

		appendStringInfoSpaces(es->str, es->indent * 2);
		appendStringInfo(es->str, "Sampling: %s (", method_name);
		foreach(lc, params)
		{
			if (!first)
				appendStringInfoString(es->str, ", ");
			appendStringInfoString(es->str, (const char *) lfirst(lc));
			first = false;
		}
		appendStringInfoChar(es->str, ')');
		if (repeatable)
			appendStringInfo(es->str, " REPEATABLE (%s)", repeatable);
		appendStringInfoChar(es->str, '\n');
	}
	else
	{
		ExplainPropertyText("Sampling Method", method_name, es);
		ExplainPropertyList("Sampling Parameters", params, es);
		if (repeatable)
			ExplainPropertyText("Repeatable Seed", repeatable, es);
	}
}

/*
 * If it's EXPLAIN ANALYZE, show tuplesort stats for a sort node
 */
static void
show_sort_info(SortState *sortstate, ExplainState *es)
{
	if (!es->analyze)
		return;

	if (sortstate->sort_Done && sortstate->tuplesortstate != NULL)
	{
		Tuplesortstate *state = (Tuplesortstate *) sortstate->tuplesortstate;
		TuplesortInstrumentation stats;
		const char *sortMethod;
		const char *spaceType;
		long		spaceUsed;

		tuplesort_get_stats(state, &stats);
		sortMethod = tuplesort_method_name(stats.sortMethod);
		spaceType = tuplesort_space_type_name(stats.spaceType);
		spaceUsed = stats.spaceUsed;

		if (es->format == EXPLAIN_FORMAT_TEXT)
		{
			appendStringInfoSpaces(es->str, es->indent * 2);
			appendStringInfo(es->str, "Sort Method: %s  %s: %ldkB\n",
							 sortMethod, spaceType, spaceUsed);
		}
		else
		{
			ExplainPropertyText("Sort Method", sortMethod, es);
			ExplainPropertyInteger("Sort Space Used", "kB", spaceUsed, es);
			ExplainPropertyText("Sort Space Type", spaceType, es);
		}
	}

	if (sortstate->shared_info != NULL)
	{
		int			n;
		bool		opened_group = false;

		for (n = 0; n < sortstate->shared_info->num_workers; n++)
		{
			TuplesortInstrumentation *sinstrument;
			const char *sortMethod;
			const char *spaceType;
			long		spaceUsed;

			sinstrument = &sortstate->shared_info->sinstrument[n];
			if (sinstrument->sortMethod == SORT_TYPE_STILL_IN_PROGRESS)
				continue;		/* ignore any unfilled slots */
			sortMethod = tuplesort_method_name(sinstrument->sortMethod);
			spaceType = tuplesort_space_type_name(sinstrument->spaceType);
			spaceUsed = sinstrument->spaceUsed;

			if (es->format == EXPLAIN_FORMAT_TEXT)
			{
				appendStringInfoSpaces(es->str, es->indent * 2);
				appendStringInfo(es->str,
								 "Worker %d:  Sort Method: %s  %s: %ldkB\n",
								 n, sortMethod, spaceType, spaceUsed);
			}
			else
			{
				if (!opened_group)
				{
					ExplainOpenGroup("Workers", "Workers", false, es);
					opened_group = true;
				}
				ExplainOpenGroup("Worker", NULL, true, es);
				ExplainPropertyInteger("Worker Number", NULL, n, es);
				ExplainPropertyText("Sort Method", sortMethod, es);
				ExplainPropertyInteger("Sort Space Used", "kB", spaceUsed, es);
				ExplainPropertyText("Sort Space Type", spaceType, es);
				ExplainCloseGroup("Worker", NULL, true, es);
			}
		}
		if (opened_group)
			ExplainCloseGroup("Workers", "Workers", false, es);
	}
}

/*
 * Show information on hash buckets/batches.
 */
static void
show_hash_info(HashState *hashstate, ExplainState *es)
{
	HashInstrumentation hinstrument = {0};

	/*
	 * In a parallel execution, the leader process may or may not have run the
	 * hash join, and even if it did it may not have built a hash table due to
	 * timing (if it started late it might have seen no tuples in the outer
	 * relation and skipped building the hash table).  Therefore we have to be
	 * prepared to get instrumentation data from all participants.
	 */
	if (hashstate->hashtable)
		ExecHashGetInstrumentation(&hinstrument, hashstate->hashtable);

	/*
	 * Merge results from workers.  In the parallel-oblivious case, the
	 * results from all participants should be identical, except where
	 * participants didn't run the join at all so have no data.  In the
	 * parallel-aware case, we need to consider all the results.  Each worker
	 * may have seen a different subset of batches and we want to find the
	 * highest memory usage for any one batch across all batches.
	 */
	if (hashstate->shared_info)
	{
		SharedHashInfo *shared_info = hashstate->shared_info;
		int			i;

		for (i = 0; i < shared_info->num_workers; ++i)
		{
			HashInstrumentation *worker_hi = &shared_info->hinstrument[i];

			if (worker_hi->nbatch > 0)
			{
				/*
				 * Every participant should agree on the buckets, so to be
				 * sure we have a value we'll just overwrite each time.
				 */
				hinstrument.nbuckets = worker_hi->nbuckets;
				hinstrument.nbuckets_original = worker_hi->nbuckets_original;

				/*
				 * Normally every participant should agree on the number of
				 * batches too, but it's possible for a backend that started
				 * late and missed the whole join not to have the final nbatch
				 * number.  So we'll take the largest number.
				 */
				hinstrument.nbatch = Max(hinstrument.nbatch, worker_hi->nbatch);
				hinstrument.nbatch_original = worker_hi->nbatch_original;

				/*
				 * In a parallel-aware hash join, for now we report the
				 * maximum peak memory reported by any worker.
				 */
				hinstrument.space_peak =
					Max(hinstrument.space_peak, worker_hi->space_peak);
			}
		}
	}

	if (hinstrument.nbatch > 0)
	{
		long		spacePeakKb = (hinstrument.space_peak + 1023) / 1024;

		if (es->format != EXPLAIN_FORMAT_TEXT)
		{
			ExplainPropertyInteger("Hash Buckets", NULL,
								   hinstrument.nbuckets, es);
			ExplainPropertyInteger("Original Hash Buckets", NULL,
								   hinstrument.nbuckets_original, es);
			ExplainPropertyInteger("Hash Batches", NULL,
								   hinstrument.nbatch, es);
			ExplainPropertyInteger("Original Hash Batches", NULL,
								   hinstrument.nbatch_original, es);
			ExplainPropertyInteger("Peak Memory Usage", "kB",
								   spacePeakKb, es);
		}
		else if (hinstrument.nbatch_original != hinstrument.nbatch ||
				 hinstrument.nbuckets_original != hinstrument.nbuckets)
		{
			appendStringInfoSpaces(es->str, es->indent * 2);
			appendStringInfo(es->str,
							 "Buckets: %d (originally %d)  Batches: %d (originally %d)  Memory Usage: %ldkB\n",
							 hinstrument.nbuckets,
							 hinstrument.nbuckets_original,
							 hinstrument.nbatch,
							 hinstrument.nbatch_original,
							 spacePeakKb);
		}
		else
		{
			appendStringInfoSpaces(es->str, es->indent * 2);
			appendStringInfo(es->str,
							 "Buckets: %d  Batches: %d  Memory Usage: %ldkB\n",
							 hinstrument.nbuckets, hinstrument.nbatch,
							 spacePeakKb);
		}
	}
}

/*
 * If it's EXPLAIN ANALYZE, show exact/lossy pages for a BitmapHeapScan node
 */
static void
show_tidbitmap_info(BitmapHeapScanState *planstate, ExplainState *es)
{
	if (es->format != EXPLAIN_FORMAT_TEXT)
	{
		ExplainPropertyInteger("Exact Heap Blocks", NULL,
							   planstate->exact_pages, es);
		ExplainPropertyInteger("Lossy Heap Blocks", NULL,
							   planstate->lossy_pages, es);
	}
	else
	{
		if (planstate->exact_pages > 0 || planstate->lossy_pages > 0)
		{
			appendStringInfoSpaces(es->str, es->indent * 2);
			appendStringInfoString(es->str, "Heap Blocks:");
			if (planstate->exact_pages > 0)
				appendStringInfo(es->str, " exact=%ld", planstate->exact_pages);
			if (planstate->lossy_pages > 0)
				appendStringInfo(es->str, " lossy=%ld", planstate->lossy_pages);
			appendStringInfoChar(es->str, '\n');
		}
	}
}

/*
 * If it's EXPLAIN ANALYZE, show instrumentation information for a plan node
 *
 * "which" identifies which instrumentation counter to print
 */
static void
show_instrumentation_count(const char *qlabel, int which,
						   PlanState *planstate, ExplainState *es)
{
	double		nfiltered;
	double		nloops;

	if (!es->analyze || !planstate->instrument)
		return;

	if (which == 2)
		nfiltered = planstate->instrument->nfiltered2;
	else
		nfiltered = planstate->instrument->nfiltered1;
	nloops = planstate->instrument->nloops;

	/* In text mode, suppress zero counts; they're not interesting enough */
	if (nfiltered > 0 || es->format != EXPLAIN_FORMAT_TEXT)
	{
		if (nloops > 0)
			ExplainPropertyFloat(qlabel, NULL, nfiltered / nloops, 0, es);
		else
			ExplainPropertyFloat(qlabel, NULL, 0.0, 0, es);
	}
}

/* POLAR px */
static void
show_result_hash_filter(const char *qlabel, ResultState *resultstate, 
						ExplainState *es)
{
	if (!resultstate->hashFilter)
		return;

	appendStringInfoSpaces(es->str, es->indent * 2);
	appendStringInfo(es->str, "%s: workers=%d, nattrs=%d\n",
				qlabel, resultstate->hashFilter->numsegs, 
				resultstate->hashFilter->natts);
}

/*
 * Show extra information for a ForeignScan node.
 */
static void
show_foreignscan_info(ForeignScanState *fsstate, ExplainState *es)
{
	FdwRoutine *fdwroutine = fsstate->fdwroutine;

	/* Let the FDW emit whatever fields it wants */
	if (((ForeignScan *) fsstate->ss.ps.plan)->operation != CMD_SELECT)
	{
		if (fdwroutine->ExplainDirectModify != NULL)
			fdwroutine->ExplainDirectModify(fsstate, es);
	}
	else
	{
		if (fdwroutine->ExplainForeignScan != NULL)
			fdwroutine->ExplainForeignScan(fsstate, es);
	}
}

/*
 * Show initplan params evaluated at Gather or Gather Merge node.
 */
static void
show_eval_params(Bitmapset *bms_params, ExplainState *es)
{
	int			paramid = -1;
	List	   *params = NIL;

	Assert(bms_params);

	while ((paramid = bms_next_member(bms_params, paramid)) >= 0)
	{
		char		param[32];

		snprintf(param, sizeof(param), "$%d", paramid);
		params = lappend(params, pstrdup(param));
	}

	if (params)
		ExplainPropertyList("Params Evaluated", params, es);
}

/* POLAR px */
static void
polar_show_join_pruning_info(List *join_prune_ids, ExplainState *es)
{
	List	   *params = NIL;
	ListCell   *lc;

	if (!join_prune_ids)
		return;

	foreach(lc, join_prune_ids)
	{
		int			paramid = lfirst_int(lc);
		char		param[32];

		snprintf(param, sizeof(param), "$%d", paramid);
		params = lappend(params, pstrdup(param));
	}

	ExplainPropertyList("Partition Selectors", params, es);
}

/*
 * Fetch the name of an index in an EXPLAIN
 *
 * We allow plugins to get control here so that plans involving hypothetical
 * indexes can be explained.
 *
 * Note: names returned by this function should be "raw"; the caller will
 * apply quoting if needed.  Formerly the convention was to do quoting here,
 * but we don't want that in non-text output formats.
 */
static const char *
explain_get_index_name(Oid indexId)
{
	const char *result;

	if (explain_get_index_name_hook)
		result = (*explain_get_index_name_hook) (indexId);
	else
		result = NULL;
	if (result == NULL)
	{
		/* default behavior: look it up in the catalogs */
		result = get_rel_name(indexId);
		if (result == NULL)
			elog(ERROR, "cache lookup failed for index %u", indexId);
	}
	return result;
}

/*
 * Show buffer usage details.
 */
static void
show_buffer_usage(ExplainState *es, const BufferUsage *usage)
{
	if (es->format == EXPLAIN_FORMAT_TEXT)
	{
		bool		has_shared = (usage->shared_blks_hit > 0 ||
								  usage->shared_blks_read > 0 ||
								  usage->shared_blks_dirtied > 0 ||
								  usage->shared_blks_written > 0);
		bool		has_local = (usage->local_blks_hit > 0 ||
								 usage->local_blks_read > 0 ||
								 usage->local_blks_dirtied > 0 ||
								 usage->local_blks_written > 0);
		bool		has_temp = (usage->temp_blks_read > 0 ||
								usage->temp_blks_written > 0);
		bool		has_timing = (!INSTR_TIME_IS_ZERO(usage->blk_read_time) ||
								  !INSTR_TIME_IS_ZERO(usage->blk_write_time));

		/* Show only positive counter values. */
		if (has_shared || has_local || has_temp)
		{
			appendStringInfoSpaces(es->str, es->indent * 2);
			appendStringInfoString(es->str, "Buffers:");

			if (has_shared)
			{
				appendStringInfoString(es->str, " shared");
				if (usage->shared_blks_hit > 0)
					appendStringInfo(es->str, " hit=%ld",
									 usage->shared_blks_hit);
				if (usage->shared_blks_read > 0)
					appendStringInfo(es->str, " read=%ld",
									 usage->shared_blks_read);
				if (usage->shared_blks_dirtied > 0)
					appendStringInfo(es->str, " dirtied=%ld",
									 usage->shared_blks_dirtied);
				if (usage->shared_blks_written > 0)
					appendStringInfo(es->str, " written=%ld",
									 usage->shared_blks_written);
				if (has_local || has_temp)
					appendStringInfoChar(es->str, ',');
			}
			if (has_local)
			{
				appendStringInfoString(es->str, " local");
				if (usage->local_blks_hit > 0)
					appendStringInfo(es->str, " hit=%ld",
									 usage->local_blks_hit);
				if (usage->local_blks_read > 0)
					appendStringInfo(es->str, " read=%ld",
									 usage->local_blks_read);
				if (usage->local_blks_dirtied > 0)
					appendStringInfo(es->str, " dirtied=%ld",
									 usage->local_blks_dirtied);
				if (usage->local_blks_written > 0)
					appendStringInfo(es->str, " written=%ld",
									 usage->local_blks_written);
				if (has_temp)
					appendStringInfoChar(es->str, ',');
			}
			if (has_temp)
			{
				appendStringInfoString(es->str, " temp");
				if (usage->temp_blks_read > 0)
					appendStringInfo(es->str, " read=%ld",
									 usage->temp_blks_read);
				if (usage->temp_blks_written > 0)
					appendStringInfo(es->str, " written=%ld",
									 usage->temp_blks_written);
			}
			appendStringInfoChar(es->str, '\n');
		}

		/* As above, show only positive counter values. */
		if (has_timing)
		{
			appendStringInfoSpaces(es->str, es->indent * 2);
			appendStringInfoString(es->str, "I/O Timings:");
			if (!INSTR_TIME_IS_ZERO(usage->blk_read_time))
				appendStringInfo(es->str, " read=%0.3f",
								 INSTR_TIME_GET_MILLISEC(usage->blk_read_time));
			if (!INSTR_TIME_IS_ZERO(usage->blk_write_time))
				appendStringInfo(es->str, " write=%0.3f",
								 INSTR_TIME_GET_MILLISEC(usage->blk_write_time));
			appendStringInfoChar(es->str, '\n');
		}
	}
	else
	{
		ExplainPropertyInteger("Shared Hit Blocks", NULL,
							   usage->shared_blks_hit, es);
		ExplainPropertyInteger("Shared Read Blocks", NULL,
							   usage->shared_blks_read, es);
		ExplainPropertyInteger("Shared Dirtied Blocks", NULL,
							   usage->shared_blks_dirtied, es);
		ExplainPropertyInteger("Shared Written Blocks", NULL,
							   usage->shared_blks_written, es);
		ExplainPropertyInteger("Local Hit Blocks", NULL,
							   usage->local_blks_hit, es);
		ExplainPropertyInteger("Local Read Blocks", NULL,
							   usage->local_blks_read, es);
		ExplainPropertyInteger("Local Dirtied Blocks", NULL,
							   usage->local_blks_dirtied, es);
		ExplainPropertyInteger("Local Written Blocks", NULL,
							   usage->local_blks_written, es);
		ExplainPropertyInteger("Temp Read Blocks", NULL,
							   usage->temp_blks_read, es);
		ExplainPropertyInteger("Temp Written Blocks", NULL,
							   usage->temp_blks_written, es);
		if (track_io_timing)
		{
			ExplainPropertyFloat("I/O Read Time", "ms",
								 INSTR_TIME_GET_MILLISEC(usage->blk_read_time),
								 3, es);
			ExplainPropertyFloat("I/O Write Time", "ms",
								 INSTR_TIME_GET_MILLISEC(usage->blk_write_time),
								 3, es);
		}
	}
}

/*
 * Add some additional details about an IndexScan or IndexOnlyScan
 */
static void
ExplainIndexScanDetails(Oid indexid, ScanDirection indexorderdir,
						ExplainState *es)
{
	const char *indexname = explain_get_index_name(indexid);

	if (es->format == EXPLAIN_FORMAT_TEXT)
	{
		if (ScanDirectionIsBackward(indexorderdir))
			appendStringInfoString(es->str, " Backward");
		appendStringInfo(es->str, " using %s", quote_identifier(indexname));
	}
	else
	{
		const char *scandir;

		switch (indexorderdir)
		{
			case BackwardScanDirection:
				scandir = "Backward";
				break;
			case NoMovementScanDirection:
				scandir = "NoMovement";
				break;
			case ForwardScanDirection:
				scandir = "Forward";
				break;
			default:
				scandir = "???";
				break;
		}
		ExplainPropertyText("Scan Direction", scandir, es);
		ExplainPropertyText("Index Name", indexname, es);
	}
}

/*
 * Show the target of a Scan node
 */
static void
ExplainScanTarget(Scan *plan, ExplainState *es)
{
	ExplainTargetRel((Plan *) plan, plan->scanrelid, es);
}

/*
 * Show the target of a ModifyTable node
 *
 * Here we show the nominal target (ie, the relation that was named in the
 * original query).  If the actual target(s) is/are different, we'll show them
 * in show_modifytable_info().
 */
static void
ExplainModifyTarget(ModifyTable *plan, ExplainState *es)
{
	ExplainTargetRel((Plan *) plan, plan->nominalRelation, es);
}

/*
 * Show the target relation of a scan or modify node
 */
static void
ExplainTargetRel(Plan *plan, Index rti, ExplainState *es)
{
	char	   *objectname = NULL;
	char	   *namespace = NULL;
	const char *objecttag = NULL;
	RangeTblEntry *rte;
	char	   *refname;

	rte = rt_fetch(rti, es->rtable);
	refname = (char *) list_nth(es->rtable_names, rti - 1);
	if (refname == NULL)
		refname = rte->eref->aliasname;

	switch (nodeTag(plan))
	{
		case T_SeqScan:
		case T_SampleScan:
		case T_IndexScan:
		case T_IndexOnlyScan:
		case T_BitmapHeapScan:
		case T_TidScan:
		case T_ForeignScan:
		case T_CustomScan:
		case T_ModifyTable:
			/* Assert it's on a real relation */
			Assert(rte->rtekind == RTE_RELATION);
			objectname = get_rel_name(rte->relid);
			if (es->verbose)
				namespace = get_namespace_name(get_rel_namespace(rte->relid));
			objecttag = "Relation Name";
			break;
		case T_FunctionScan:
			{
				FunctionScan *fscan = (FunctionScan *) plan;

				/* Assert it's on a RangeFunction */
				Assert(rte->rtekind == RTE_FUNCTION);

				/*
				 * If the expression is still a function call of a single
				 * function, we can get the real name of the function.
				 * Otherwise, punt.  (Even if it was a single function call
				 * originally, the optimizer could have simplified it away.)
				 */
				if (list_length(fscan->functions) == 1)
				{
					RangeTblFunction *rtfunc = (RangeTblFunction *) linitial(fscan->functions);

					if (IsA(rtfunc->funcexpr, FuncExpr))
					{
						FuncExpr   *funcexpr = (FuncExpr *) rtfunc->funcexpr;
						Oid			funcid = funcexpr->funcid;

						objectname = get_func_name(funcid);
						if (es->verbose)
							namespace =
								get_namespace_name(get_func_namespace(funcid));
					}
				}
				objecttag = "Function Name";
			}
			break;
		case T_TableFuncScan:
			Assert(rte->rtekind == RTE_TABLEFUNC);
			objectname = "xmltable";
			objecttag = "Table Function Name";
			break;
		case T_ValuesScan:
			Assert(rte->rtekind == RTE_VALUES);
			break;
		case T_CteScan:
			/* Assert it's on a non-self-reference CTE */
			Assert(rte->rtekind == RTE_CTE);
			Assert(!rte->self_reference);
			objectname = rte->ctename;
			objecttag = "CTE Name";
			break;
		case T_NamedTuplestoreScan:
			Assert(rte->rtekind == RTE_NAMEDTUPLESTORE);
			objectname = rte->enrname;
			objecttag = "Tuplestore Name";
			break;
		case T_WorkTableScan:
			/* Assert it's on a self-reference CTE */
			Assert(rte->rtekind == RTE_CTE);
			Assert(rte->self_reference);
			objectname = rte->ctename;
			objecttag = "CTE Name";
			break;
		default:
			break;
	}

	if (es->format == EXPLAIN_FORMAT_TEXT)
	{
		appendStringInfoString(es->str, " on");
		if (namespace != NULL)
			appendStringInfo(es->str, " %s.%s", quote_identifier(namespace),
							 quote_identifier(objectname));
		else if (objectname != NULL)
			appendStringInfo(es->str, " %s", quote_identifier(objectname));
		if (objectname == NULL || strcmp(refname, objectname) != 0)
			appendStringInfo(es->str, " %s", quote_identifier(refname));
	}
	else
	{
		if (objecttag != NULL && objectname != NULL)
			ExplainPropertyText(objecttag, objectname, es);
		if (namespace != NULL)
			ExplainPropertyText("Schema", namespace, es);
		ExplainPropertyText("Alias", refname, es);
	}
}

/*
 * Show extra information for a ModifyTable node
 *
 * We have three objectives here.  First, if there's more than one target
 * table or it's different from the nominal target, identify the actual
 * target(s).  Second, give FDWs a chance to display extra info about foreign
 * targets.  Third, show information about ON CONFLICT.
 */
static void
show_modifytable_info(ModifyTableState *mtstate, List *ancestors,
					  ExplainState *es)
{
	ModifyTable *node = (ModifyTable *) mtstate->ps.plan;
	const char *operation;
	const char *foperation;
	bool		labeltargets;
	int			j;
	List	   *idxNames = NIL;
	ListCell   *lst;

	switch (node->operation)
	{
		case CMD_INSERT:
			operation = "Insert";
			foperation = "Foreign Insert";
			break;
		case CMD_UPDATE:
			operation = "Update";
			foperation = "Foreign Update";
			break;
		case CMD_DELETE:
			operation = "Delete";
			foperation = "Foreign Delete";
			break;
		default:
			operation = "???";
			foperation = "Foreign ???";
			break;
	}

	/* Should we explicitly label target relations? */
	labeltargets = (mtstate->mt_nplans > 1 ||
					(mtstate->mt_nplans == 1 &&
					 mtstate->resultRelInfo[0].ri_RangeTableIndex != node->nominalRelation));

	if (labeltargets)
		ExplainOpenGroup("Target Tables", "Target Tables", false, es);

	for (j = 0; j < mtstate->mt_nplans; j++)
	{
		ResultRelInfo *resultRelInfo = mtstate->resultRelInfo + j;
		FdwRoutine *fdwroutine = resultRelInfo->ri_FdwRoutine;

		if (labeltargets)
		{
			/* Open a group for this target */
			ExplainOpenGroup("Target Table", NULL, true, es);

			/*
			 * In text mode, decorate each target with operation type, so that
			 * ExplainTargetRel's output of " on foo" will read nicely.
			 */
			if (es->format == EXPLAIN_FORMAT_TEXT)
			{
				appendStringInfoSpaces(es->str, es->indent * 2);
				appendStringInfoString(es->str,
									   fdwroutine ? foperation : operation);
			}

			/* Identify target */
			ExplainTargetRel((Plan *) node,
							 resultRelInfo->ri_RangeTableIndex,
							 es);

			if (es->format == EXPLAIN_FORMAT_TEXT)
			{
				appendStringInfoChar(es->str, '\n');
				es->indent++;
			}
		}

		/* Give FDW a chance if needed */
		if (!resultRelInfo->ri_usesFdwDirectModify &&
			fdwroutine != NULL &&
			fdwroutine->ExplainForeignModify != NULL)
		{
			List	   *fdw_private = (List *) list_nth(node->fdwPrivLists, j);

			fdwroutine->ExplainForeignModify(mtstate,
											 resultRelInfo,
											 fdw_private,
											 j,
											 es);
		}

		if (labeltargets)
		{
			/* Undo the indentation we added in text format */
			if (es->format == EXPLAIN_FORMAT_TEXT)
				es->indent--;

			/* Close the group */
			ExplainCloseGroup("Target Table", NULL, true, es);
		}
	}

	/* Gather names of ON CONFLICT arbiter indexes */
	foreach(lst, node->arbiterIndexes)
	{
		char	   *indexname = get_rel_name(lfirst_oid(lst));

		idxNames = lappend(idxNames, indexname);
	}

	if (node->onConflictAction != ONCONFLICT_NONE)
	{
		ExplainPropertyText("Conflict Resolution",
							node->onConflictAction == ONCONFLICT_NOTHING ?
							"NOTHING" : "UPDATE",
							es);

		/*
		 * Don't display arbiter indexes at all when DO NOTHING variant
		 * implicitly ignores all conflicts
		 */
		if (idxNames)
			ExplainPropertyList("Conflict Arbiter Indexes", idxNames, es);

		/* ON CONFLICT DO UPDATE WHERE qual is specially displayed */
		if (node->onConflictWhere)
		{
			show_upper_qual((List *) node->onConflictWhere, "Conflict Filter",
							&mtstate->ps, ancestors, es);
			show_instrumentation_count("Rows Removed by Conflict Filter", 1, &mtstate->ps, es);
		}

		/* EXPLAIN ANALYZE display of actual outcome for each tuple proposed */
		if (es->analyze && mtstate->ps.instrument)
		{
			double		total;
			double		insert_path;
			double		other_path;

			InstrEndLoop(mtstate->mt_plans[0]->instrument);

			/* count the number of source rows */
			total = mtstate->mt_plans[0]->instrument->ntuples;
			other_path = mtstate->ps.instrument->ntuples2;
			insert_path = total - other_path;

			ExplainPropertyFloat("Tuples Inserted", NULL,
								 insert_path, 0, es);
			ExplainPropertyFloat("Conflicting Tuples", NULL,
								 other_path, 0, es);
		}
	}

	if (labeltargets)
		ExplainCloseGroup("Target Tables", "Target Tables", false, es);
}

/*
 * Explain the constituent plans of a ModifyTable, Append, MergeAppend,
 * BitmapAnd, or BitmapOr node.
 *
 * The ancestors list should already contain the immediate parent of these
 * plans.
 */
static void
ExplainMemberNodes(PlanState **planstates, int nplans,
				   List *ancestors, ExplainState *es)
{
	int			j;

	for (j = 0; j < nplans; j++)
		ExplainNode(planstates[j], ancestors,
					"Member", NULL, es);
}

/*
 * Report about any pruned subnodes of an Append or MergeAppend node.
 *
 * nplans indicates the number of live subplans.
 * nchildren indicates the original number of subnodes in the Plan;
 * some of these may have been pruned by the run-time pruning code.
 */
static void
ExplainMissingMembers(int nplans, int nchildren, ExplainState *es)
{
	if (nplans < nchildren || es->format != EXPLAIN_FORMAT_TEXT)
		ExplainPropertyInteger("Subplans Removed", NULL,
							   nchildren - nplans, es);
}

/*
 * Explain a list of SubPlans (or initPlans, which also use SubPlan nodes).
 *
 * The ancestors list should already contain the immediate parent of these
 * SubPlanStates.
 */
static void
ExplainSubPlans(List *plans, List *ancestors,
				const char *relationship, ExplainState *es)
{
	ListCell   *lst;

	foreach(lst, plans)
	{
		SubPlanState *sps = (SubPlanState *) lfirst(lst);
		SubPlan    *sp = sps->subplan;

		/*
		 * There can be multiple SubPlan nodes referencing the same physical
		 * subplan (same plan_id, which is its index in PlannedStmt.subplans).
		 * We should print a subplan only once, so track which ones we already
		 * printed.  This state must be global across the plan tree, since the
		 * duplicate nodes could be in different plan nodes, eg both a bitmap
		 * indexscan's indexqual and its parent heapscan's recheck qual.  (We
		 * do not worry too much about which plan node we show the subplan as
		 * attached to in such cases.)
		 */
		if (bms_is_member(sp->plan_id, es->printed_subplans))
			continue;
		es->printed_subplans = bms_add_member(es->printed_subplans,
											  sp->plan_id);

		ExplainNode(sps->planstate, ancestors,
					relationship, sp->plan_name, es);
	}
}

/*
 * Explain a list of children of a CustomScan.
 */
static void
ExplainCustomChildren(CustomScanState *css, List *ancestors, ExplainState *es)
{
	ListCell   *cell;
	const char *label =
	(list_length(css->custom_ps) != 1 ? "children" : "child");

	foreach(cell, css->custom_ps)
		ExplainNode((PlanState *) lfirst(cell), ancestors, label, NULL, es);
}

/*
 * Explain a property, such as sort keys or targets, that takes the form of
 * a list of unlabeled items.  "data" is a list of C strings.
 */
void
ExplainPropertyList(const char *qlabel, List *data, ExplainState *es)
{
	ListCell   *lc;
	bool		first = true;

	switch (es->format)
	{
		case EXPLAIN_FORMAT_TEXT:
			appendStringInfoSpaces(es->str, es->indent * 2);
			appendStringInfo(es->str, "%s: ", qlabel);
			foreach(lc, data)
			{
				if (!first)
					appendStringInfoString(es->str, ", ");
				appendStringInfoString(es->str, (const char *) lfirst(lc));
				first = false;
			}
			appendStringInfoChar(es->str, '\n');
			break;

		case EXPLAIN_FORMAT_XML:
			ExplainXMLTag(qlabel, X_OPENING, es);
			foreach(lc, data)
			{
				char	   *str;

				appendStringInfoSpaces(es->str, es->indent * 2 + 2);
				appendStringInfoString(es->str, "<Item>");
				str = escape_xml((const char *) lfirst(lc));
				appendStringInfoString(es->str, str);
				pfree(str);
				appendStringInfoString(es->str, "</Item>\n");
			}
			ExplainXMLTag(qlabel, X_CLOSING, es);
			break;

		case EXPLAIN_FORMAT_JSON:
			ExplainJSONLineEnding(es);
			appendStringInfoSpaces(es->str, es->indent * 2);
			escape_json(es->str, qlabel);
			appendStringInfoString(es->str, ": [");
			foreach(lc, data)
			{
				if (!first)
					appendStringInfoString(es->str, ", ");
				escape_json(es->str, (const char *) lfirst(lc));
				first = false;
			}
			appendStringInfoChar(es->str, ']');
			break;

		case EXPLAIN_FORMAT_YAML:
			ExplainYAMLLineStarting(es);
			appendStringInfo(es->str, "%s: ", qlabel);
			foreach(lc, data)
			{
				appendStringInfoChar(es->str, '\n');
				appendStringInfoSpaces(es->str, es->indent * 2 + 2);
				appendStringInfoString(es->str, "- ");
				escape_yaml(es->str, (const char *) lfirst(lc));
			}
			break;
	}
}

/*
 * Explain a property that takes the form of a list of unlabeled items within
 * another list.  "data" is a list of C strings.
 */
void
ExplainPropertyListNested(const char *qlabel, List *data, ExplainState *es)
{
	ListCell   *lc;
	bool		first = true;

	switch (es->format)
	{
		case EXPLAIN_FORMAT_TEXT:
		case EXPLAIN_FORMAT_XML:
			ExplainPropertyList(qlabel, data, es);
			return;

		case EXPLAIN_FORMAT_JSON:
			ExplainJSONLineEnding(es);
			appendStringInfoSpaces(es->str, es->indent * 2);
			appendStringInfoChar(es->str, '[');
			foreach(lc, data)
			{
				if (!first)
					appendStringInfoString(es->str, ", ");
				escape_json(es->str, (const char *) lfirst(lc));
				first = false;
			}
			appendStringInfoChar(es->str, ']');
			break;

		case EXPLAIN_FORMAT_YAML:
			ExplainYAMLLineStarting(es);
			appendStringInfoString(es->str, "- [");
			foreach(lc, data)
			{
				if (!first)
					appendStringInfoString(es->str, ", ");
				escape_yaml(es->str, (const char *) lfirst(lc));
				first = false;
			}
			appendStringInfoChar(es->str, ']');
			break;
	}
}

/*
 * Explain a simple property.
 *
 * If "numeric" is true, the value is a number (or other value that
 * doesn't need quoting in JSON).
 *
 * If unit is non-NULL the text format will display it after the value.
 *
 * This usually should not be invoked directly, but via one of the datatype
 * specific routines ExplainPropertyText, ExplainPropertyInteger, etc.
 */
static void
ExplainProperty(const char *qlabel, const char *unit, const char *value,
				bool numeric, ExplainState *es)
{
	switch (es->format)
	{
		case EXPLAIN_FORMAT_TEXT:
			appendStringInfoSpaces(es->str, es->indent * 2);
			if (unit)
				appendStringInfo(es->str, "%s: %s %s\n", qlabel, value, unit);
			else
				appendStringInfo(es->str, "%s: %s\n", qlabel, value);
			break;

		case EXPLAIN_FORMAT_XML:
			{
				char	   *str;

				appendStringInfoSpaces(es->str, es->indent * 2);
				ExplainXMLTag(qlabel, X_OPENING | X_NOWHITESPACE, es);
				str = escape_xml(value);
				appendStringInfoString(es->str, str);
				pfree(str);
				ExplainXMLTag(qlabel, X_CLOSING | X_NOWHITESPACE, es);
				appendStringInfoChar(es->str, '\n');
			}
			break;

		case EXPLAIN_FORMAT_JSON:
			ExplainJSONLineEnding(es);
			appendStringInfoSpaces(es->str, es->indent * 2);
			escape_json(es->str, qlabel);
			appendStringInfoString(es->str, ": ");
			if (numeric)
				appendStringInfoString(es->str, value);
			else
				escape_json(es->str, value);
			break;

		case EXPLAIN_FORMAT_YAML:
			ExplainYAMLLineStarting(es);
			appendStringInfo(es->str, "%s: ", qlabel);
			if (numeric)
				appendStringInfoString(es->str, value);
			else
				escape_yaml(es->str, value);
			break;
	}
}

/*
 * Explain a string-valued property.
 */
void
ExplainPropertyText(const char *qlabel, const char *value, ExplainState *es)
{
	ExplainProperty(qlabel, NULL, value, false, es);
}

/*
 * Explain an integer-valued property.
 */
void
ExplainPropertyInteger(const char *qlabel, const char *unit, int64 value,
					   ExplainState *es)
{
	char		buf[32];

	snprintf(buf, sizeof(buf), INT64_FORMAT, value);
	ExplainProperty(qlabel, unit, buf, true, es);
}

/*
 * Explain a float-valued property, using the specified number of
 * fractional digits.
 */
void
ExplainPropertyFloat(const char *qlabel, const char *unit, double value,
					 int ndigits, ExplainState *es)
{
	char	   *buf;

	buf = psprintf("%.*f", ndigits, value);
	ExplainProperty(qlabel, unit, buf, true, es);
	pfree(buf);
}

/*
 * Explain a bool-valued property.
 */
void
ExplainPropertyBool(const char *qlabel, bool value, ExplainState *es)
{
	ExplainProperty(qlabel, NULL, value ? "true" : "false", true, es);
}

/*
 * Open a group of related objects.
 *
 * objtype is the type of the group object, labelname is its label within
 * a containing object (if any).
 *
 * If labeled is true, the group members will be labeled properties,
 * while if it's false, they'll be unlabeled objects.
 */
void
ExplainOpenGroup(const char *objtype, const char *labelname,
				 bool labeled, ExplainState *es)
{
	switch (es->format)
	{
		case EXPLAIN_FORMAT_TEXT:
			/* nothing to do */
			break;

		case EXPLAIN_FORMAT_XML:
			ExplainXMLTag(objtype, X_OPENING, es);
			es->indent++;
			break;

		case EXPLAIN_FORMAT_JSON:
			ExplainJSONLineEnding(es);
			appendStringInfoSpaces(es->str, 2 * es->indent);
			if (labelname)
			{
				escape_json(es->str, labelname);
				appendStringInfoString(es->str, ": ");
			}
			appendStringInfoChar(es->str, labeled ? '{' : '[');

			/*
			 * In JSON format, the grouping_stack is an integer list.  0 means
			 * we've emitted nothing at this grouping level, 1 means we've
			 * emitted something (and so the next item needs a comma). See
			 * ExplainJSONLineEnding().
			 */
			es->grouping_stack = lcons_int(0, es->grouping_stack);
			es->indent++;
			break;

		case EXPLAIN_FORMAT_YAML:

			/*
			 * In YAML format, the grouping stack is an integer list.  0 means
			 * we've emitted nothing at this grouping level AND this grouping
			 * level is unlabelled and must be marked with "- ".  See
			 * ExplainYAMLLineStarting().
			 */
			ExplainYAMLLineStarting(es);
			if (labelname)
			{
				appendStringInfo(es->str, "%s: ", labelname);
				es->grouping_stack = lcons_int(1, es->grouping_stack);
			}
			else
			{
				appendStringInfoString(es->str, "- ");
				es->grouping_stack = lcons_int(0, es->grouping_stack);
			}
			es->indent++;
			break;
	}
}

/*
 * Close a group of related objects.
 * Parameters must match the corresponding ExplainOpenGroup call.
 */
void
ExplainCloseGroup(const char *objtype, const char *labelname,
				  bool labeled, ExplainState *es)
{
	switch (es->format)
	{
		case EXPLAIN_FORMAT_TEXT:
			/* nothing to do */
			break;

		case EXPLAIN_FORMAT_XML:
			es->indent--;
			ExplainXMLTag(objtype, X_CLOSING, es);
			break;

		case EXPLAIN_FORMAT_JSON:
			es->indent--;
			appendStringInfoChar(es->str, '\n');
			appendStringInfoSpaces(es->str, 2 * es->indent);
			appendStringInfoChar(es->str, labeled ? '}' : ']');
			es->grouping_stack = list_delete_first(es->grouping_stack);
			break;

		case EXPLAIN_FORMAT_YAML:
			es->indent--;
			es->grouping_stack = list_delete_first(es->grouping_stack);
			break;
	}
}

/*
 * Emit a "dummy" group that never has any members.
 *
 * objtype is the type of the group object, labelname is its label within
 * a containing object (if any).
 */
static void
ExplainDummyGroup(const char *objtype, const char *labelname, ExplainState *es)
{
	switch (es->format)
	{
		case EXPLAIN_FORMAT_TEXT:
			/* nothing to do */
			break;

		case EXPLAIN_FORMAT_XML:
			ExplainXMLTag(objtype, X_CLOSE_IMMEDIATE, es);
			break;

		case EXPLAIN_FORMAT_JSON:
			ExplainJSONLineEnding(es);
			appendStringInfoSpaces(es->str, 2 * es->indent);
			if (labelname)
			{
				escape_json(es->str, labelname);
				appendStringInfoString(es->str, ": ");
			}
			escape_json(es->str, objtype);
			break;

		case EXPLAIN_FORMAT_YAML:
			ExplainYAMLLineStarting(es);
			if (labelname)
			{
				escape_yaml(es->str, labelname);
				appendStringInfoString(es->str, ": ");
			}
			else
			{
				appendStringInfoString(es->str, "- ");
			}
			escape_yaml(es->str, objtype);
			break;
	}
}

/*
 * Emit the start-of-output boilerplate.
 *
 * This is just enough different from processing a subgroup that we need
 * a separate pair of subroutines.
 */
void
ExplainBeginOutput(ExplainState *es)
{
	switch (es->format)
	{
		case EXPLAIN_FORMAT_TEXT:
			/* nothing to do */
			break;

		case EXPLAIN_FORMAT_XML:
			appendStringInfoString(es->str,
								   "<explain xmlns=\"http://www.postgresql.org/2009/explain\">\n");
			es->indent++;
			break;

		case EXPLAIN_FORMAT_JSON:
			/* top-level structure is an array of plans */
			appendStringInfoChar(es->str, '[');
			es->grouping_stack = lcons_int(0, es->grouping_stack);
			es->indent++;
			break;

		case EXPLAIN_FORMAT_YAML:
			es->grouping_stack = lcons_int(0, es->grouping_stack);
			break;
	}
}

/*
 * Emit the end-of-output boilerplate.
 */
void
ExplainEndOutput(ExplainState *es)
{
	switch (es->format)
	{
		case EXPLAIN_FORMAT_TEXT:
			/* nothing to do */
			break;

		case EXPLAIN_FORMAT_XML:
			es->indent--;
			appendStringInfoString(es->str, "</explain>");
			break;

		case EXPLAIN_FORMAT_JSON:
			es->indent--;
			appendStringInfoString(es->str, "\n]");
			es->grouping_stack = list_delete_first(es->grouping_stack);
			break;

		case EXPLAIN_FORMAT_YAML:
			es->grouping_stack = list_delete_first(es->grouping_stack);
			break;
	}
}

/*
 * Put an appropriate separator between multiple plans
 */
void
ExplainSeparatePlans(ExplainState *es)
{
	switch (es->format)
	{
		case EXPLAIN_FORMAT_TEXT:
			/* add a blank line */
			appendStringInfoChar(es->str, '\n');
			break;

		case EXPLAIN_FORMAT_XML:
		case EXPLAIN_FORMAT_JSON:
		case EXPLAIN_FORMAT_YAML:
			/* nothing to do */
			break;
	}
}

/*
 * Emit opening or closing XML tag.
 *
 * "flags" must contain X_OPENING, X_CLOSING, or X_CLOSE_IMMEDIATE.
 * Optionally, OR in X_NOWHITESPACE to suppress the whitespace we'd normally
 * add.
 *
 * XML restricts tag names more than our other output formats, eg they can't
 * contain white space or slashes.  Replace invalid characters with dashes,
 * so that for example "I/O Read Time" becomes "I-O-Read-Time".
 */
static void
ExplainXMLTag(const char *tagname, int flags, ExplainState *es)
{
	const char *s;
	const char *valid = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_.";

	if ((flags & X_NOWHITESPACE) == 0)
		appendStringInfoSpaces(es->str, 2 * es->indent);
	appendStringInfoCharMacro(es->str, '<');
	if ((flags & X_CLOSING) != 0)
		appendStringInfoCharMacro(es->str, '/');
	for (s = tagname; *s; s++)
		appendStringInfoChar(es->str, strchr(valid, *s) ? *s : '-');
	if ((flags & X_CLOSE_IMMEDIATE) != 0)
		appendStringInfoString(es->str, " /");
	appendStringInfoCharMacro(es->str, '>');
	if ((flags & X_NOWHITESPACE) == 0)
		appendStringInfoCharMacro(es->str, '\n');
}

/*
 * Emit a JSON line ending.
 *
 * JSON requires a comma after each property but the last.  To facilitate this,
 * in JSON format, the text emitted for each property begins just prior to the
 * preceding line-break (and comma, if applicable).
 */
static void
ExplainJSONLineEnding(ExplainState *es)
{
	Assert(es->format == EXPLAIN_FORMAT_JSON);
	if (linitial_int(es->grouping_stack) != 0)
		appendStringInfoChar(es->str, ',');
	else
		linitial_int(es->grouping_stack) = 1;
	appendStringInfoChar(es->str, '\n');
}

/*
 * Indent a YAML line.
 *
 * YAML lines are ordinarily indented by two spaces per indentation level.
 * The text emitted for each property begins just prior to the preceding
 * line-break, except for the first property in an unlabelled group, for which
 * it begins immediately after the "- " that introduces the group.  The first
 * property of the group appears on the same line as the opening "- ".
 */
static void
ExplainYAMLLineStarting(ExplainState *es)
{
	Assert(es->format == EXPLAIN_FORMAT_YAML);
	if (linitial_int(es->grouping_stack) == 0)
	{
		linitial_int(es->grouping_stack) = 1;
	}
	else
	{
		appendStringInfoChar(es->str, '\n');
		appendStringInfoSpaces(es->str, es->indent * 2);
	}
}

/*
 * YAML is a superset of JSON; unfortunately, the YAML quoting rules are
 * ridiculously complicated -- as documented in sections 5.3 and 7.3.3 of
 * http://yaml.org/spec/1.2/spec.html -- so we chose to just quote everything.
 * Empty strings, strings with leading or trailing whitespace, and strings
 * containing a variety of special characters must certainly be quoted or the
 * output is invalid; and other seemingly harmless strings like "0xa" or
 * "true" must be quoted, lest they be interpreted as a hexadecimal or Boolean
 * constant rather than a string.
 */
static void
escape_yaml(StringInfo buf, const char *str)
{
	escape_json(buf, str);
}

/* POLAR px */
static void
show_dispatch_info(ExecSlice *slice, ExplainState *es, Plan *plan)
{
	int			segments;

	/*
	 * In non-parallel query, there is no slice information.
	 */
	if (!slice)
		return;

	switch (slice->gangType)
	{
		case GANGTYPE_UNALLOCATED:
		case GANGTYPE_ENTRYDB_READER:
			segments = 0;
			break;

		case GANGTYPE_PRIMARY_WRITER:
		case GANGTYPE_PRIMARY_READER:
		case GANGTYPE_SINGLETON_READER:
		{
			segments = list_length(slice->segments);
			break;
		}

		default:
			segments = 0;		/* keep compiler happy */
			Assert(false);
			break;
	}

	if (es->format == EXPLAIN_FORMAT_TEXT)
	{
		if (segments == 0)
			appendStringInfo(es->str, "  (slice%d)", slice->sliceIndex);
		else if (slice->primaryGang && px_log_gang >= PXVARS_VERBOSITY_DEBUG)
			/*
			 * In px 5 there was a unique gang_id for each gang, this was
			 * retired since px 6, so we use the px identifier from the first
			 * segment of the gang to identify each gang.
			 */
			appendStringInfo(es->str, "  (slice%d; gang-notsupport-yet; segments: %d)",
							 slice->sliceIndex,
							 segments);
		else
			appendStringInfo(es->str, "  (slice%d; segments: %d)",
							 slice->sliceIndex, segments);
	}
	else
	{
		ExplainPropertyInteger("Slice", NULL, slice->sliceIndex, es);

		/* TODO */
		/* if (slice->primaryGang && px_log_gang >= PXVARS_VERBOSITY_DEBUG) */
		/* 	ExplainPropertyInteger("Gang", NULL, slice->primaryGang->db_descriptors[0]->identifier, es); */

		ExplainPropertyInteger("Segments", NULL, segments, es);
		ExplainPropertyText("Gang Type", gangTypeToString(slice->gangType), es);
	}
}


/*
 * Show the hash and merge keys for a Motion node.
 */
static void
show_motion_keys(PlanState *planstate, List *hashExpr, int nkeys, AttrNumber *keycols,
				 const char *qlabel, List *ancestors, ExplainState *es)
{
	Plan	   *plan = planstate->plan;
	List	   *context;
	char	   *exprstr;
	bool		useprefix = list_length(es->rtable) > 1;
	int			keyno;
	List	   *result = NIL;

	if (!nkeys && !hashExpr)
		return;

	/* Set up deparse context */
	context = set_deparse_context_planstate(es->deparse_cxt,
											(Node *) planstate,
											ancestors);

    /* Merge Receive ordering key */
	for (keyno = 0; keyno < nkeys; keyno++)
	{
        /* find key expression in tlist */
		AttrNumber	keyresno = keycols[keyno];
		TargetEntry *target = get_tle_by_resno(plan->targetlist, keyresno);

	    /* Deparse the expression, showing any top-level cast */
		if (target)
	        exprstr = deparse_expression((Node *) target->expr, context,
								         useprefix, true);
		else
		{
			elog(WARNING, "Gather Exchange %s error: no tlist item %d",
				 qlabel, keyresno);
			exprstr = "*BOGUS*";
        }

		result = lappend(result, exprstr);
	}

	if (list_length(result) > 0)
		ExplainPropertyList(qlabel, result, es);

    /* Hashed repartitioning key */
    if (hashExpr)
    {
	    /* Deparse the expression */
	    exprstr = deparse_expression((Node *)hashExpr, context, useprefix, true);
		ExplainPropertyText("Hash Key", exprstr, es);
    }
}
/* POLAR end */