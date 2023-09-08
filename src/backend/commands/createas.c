/*-------------------------------------------------------------------------
 *
 * createas.c
 *	  Execution of CREATE TABLE ... AS, a/k/a SELECT INTO.
 *	  Since CREATE MATERIALIZED VIEW shares syntax and most behaviors,
 *	  we implement that here, too.
 *
 * We implement this by diverting the query's normal output to a
 * specialized DestReceiver type.
 *
 * Formerly, CTAS was implemented as a variant of SELECT, which led
 * to assorted legacy behaviors that we still try to preserve, notably that
 * we must return a tuples-processed count in the completionTag.  (We no
 * longer do that for CTAS ... WITH NO DATA, however.)
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/commands/createas.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

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

#define MAX_BUFFERED_TUPLES 1000
#include "px/memquota.h"

typedef struct
{
	DestReceiver pub;			/* publicly-known function pointers */
	IntoClause *into;			/* target relation specification */
	/* These fields are filled by intorel_startup: */
	Relation	rel;			/* relation to write to */
	ObjectAddress reladdr;		/* address of rel, for ExecCreateTableAs */
	CommandId	output_cid;		/* cmin to insert in output tuples */
	int			hi_options;		/* heap_insert performance options */
	BulkInsertState bistate;	/* bulk insert state */

	/* POLAR px */
	int nBufferedTuples;		/* number of tuples currently in bufferedTuples */
	HeapTuple *bufferedTuples;	/* buffered tuples that will be inserted in bulk later */
	Size bufferedTuplesSize;	/* total size of tuples currently in bufferedTuples */
	MemoryContext tmpcontext;	/* memory context for per-row workspace */
	/* POLAR end */
} DR_intorel;

/* utility functions for CTAS definition creation */
static ObjectAddress create_ctas_internal(List *attrList, IntoClause *into);
static ObjectAddress create_ctas_nodata(List *tlist, IntoClause *into);

/* DestReceiver routines for collecting data */
static void intorel_startup(DestReceiver *self, int operation, TupleDesc typeinfo);
static bool intorel_receive(TupleTableSlot *slot, DestReceiver *self);
static void intorel_shutdown(DestReceiver *self);
static void intorel_destroy(DestReceiver *self);

/* POLAR px: buffered version of intorel for bulk insert */
static DestReceiver *CreateIntoRelDestReceiverBuffered(IntoClause *intoClause);
static void intorel_startup_buffered(DestReceiver *self, int operation, TupleDesc typeinfo);
static bool intorel_receive_buffered(TupleTableSlot *slot, DestReceiver *self);
static void intorel_shutdown_buffered(DestReceiver *self);
static void intorel_destroy_buffered(DestReceiver *self);
/* POLAR end */

/*
 * create_ctas_internal
 *
 * Internal utility used for the creation of the definition of a relation
 * created via CREATE TABLE AS or a materialized view.  Caller needs to
 * provide a list of attributes (ColumnDef nodes).
 */
static ObjectAddress
create_ctas_internal(List *attrList, IntoClause *into)
{
	CreateStmt *create = makeNode(CreateStmt);
	bool		is_matview;
	char		relkind;
	Datum		toast_options;
	static char *validnsps[] = HEAP_RELOPT_NAMESPACES;
	ObjectAddress intoRelationAddr;

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
		Query	   *query = (Query *) copyObject(into->viewQuery);

		StoreViewQuery(intoRelationAddr.objectId, query, false);
		CommandCounterIncrement();
	}

	return intoRelationAddr;
}


/*
 * create_ctas_nodata
 *
 * Create CTAS or materialized view when WITH NO DATA is used, starting from
 * the targetlist of the SELECT or view definition.
 */
static ObjectAddress
create_ctas_nodata(List *tlist, IntoClause *into)
{
	List	   *attrList;
	ListCell   *t,
			   *lc;

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
			char	   *colname;

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

	/* Create the relation definition using the ColumnDef list */
	return create_ctas_internal(attrList, into);
}


/*
 * ExecCreateTableAs -- execute a CREATE TABLE AS command
 */
ObjectAddress
ExecCreateTableAs(CreateTableAsStmt *stmt, const char *queryString,
				  ParamListInfo params, QueryEnvironment *queryEnv,
				  char *completionTag)
{
	Query	   *query = castNode(Query, stmt->query);
	IntoClause *into = stmt->into;
	bool		is_matview = (into->viewQuery != NULL);
	DestReceiver *dest;
	Oid			save_userid = InvalidOid;
	int			save_sec_context = 0;
	int			save_nestlevel = 0;
	ObjectAddress address;
	List	   *rewritten;
	PlannedStmt *plan;
	QueryDesc  *queryDesc;

	if (stmt->if_not_exists)
	{
		Oid			nspid;
		Oid			oldrelid;

		nspid = RangeVarGetCreationNamespace(into->rel);

		oldrelid = get_relname_relid(into->rel->relname, nspid);
		if (OidIsValid(oldrelid))
		{
			/*
			 * The relation exists and IF NOT EXISTS has been specified.
			 *
			 * If we are in an extension script, insist that the pre-existing
			 * object be a member of the extension, to avoid security risks.
			 */
			ObjectAddressSet(address, RelationRelationId, oldrelid);
			checkMembershipInCurrentExtension(&address);

			/* OK to skip */
			ereport(NOTICE,
					(errcode(ERRCODE_DUPLICATE_TABLE),
					 errmsg("relation \"%s\" already exists, skipping",
							into->rel->relname)));
			return InvalidObjectAddress;
		}
	}

	/* POLAR px: check the existence of the relation to be insert */
	if (px_enable_replay_wait && px_enable_create_table_as &&
		InvalidOid != get_relname_relid(into->rel->relname,
										RangeVarGetAndCheckCreationNamespace(into->rel, NoLock, NULL)))
		ereport(ERROR,
				(errcode(ERRCODE_DUPLICATE_TABLE),
				 errmsg("relation \"%s\" already exists", into->rel->relname)));
	/* POLAR end */

	/*
	 * Create the tuple receiver object and insert info it will need
	 */
	if (polar_enable_create_table_as_bulk_insert)
		dest = CreateIntoRelDestReceiverBuffered(into);
	else
		dest = CreateIntoRelDestReceiver(into);

	/*
	 * The contained Query could be a SELECT, or an EXECUTE utility command.
	 * If the latter, we just pass it off to ExecuteQuery.
	 */
	if (query->commandType == CMD_UTILITY &&
		IsA(query->utilityStmt, ExecuteStmt))
	{
		ExecuteStmt *estmt = castNode(ExecuteStmt, query->utilityStmt);

		Assert(!is_matview);	/* excluded by syntax */
		ExecuteQuery(estmt, into, queryString, params, dest, completionTag);

		/* get object address that intorel_startup saved for us */
		address = ((DR_intorel *) dest)->reladdr;

		return address;
	}
	Assert(query->commandType == CMD_SELECT);

	/*
	 * For materialized views, lock down security-restricted operations and
	 * arrange to make GUC variable changes local to this command.  This is
	 * not necessary for security, but this keeps the behavior similar to
	 * REFRESH MATERIALIZED VIEW.  Otherwise, one could create a materialized
	 * view not possible to refresh.
	 */
	if (is_matview)
	{
		GetUserIdAndSecContext(&save_userid, &save_sec_context);
		SetUserIdAndSecContext(save_userid,
							   save_sec_context | SECURITY_RESTRICTED_OPERATION);
		save_nestlevel = NewGUCNestLevel();
	}

	if (into->skipData)
	{
		/*
		 * If WITH NO DATA was specified, do not go through the rewriter,
		 * planner and executor.  Just define the relation using a code path
		 * similar to CREATE VIEW.  This avoids dump/restore problems stemming
		 * from running the planner before all dependencies are set up.
		 */
		address = create_ctas_nodata(query->targetList, into);
	}
	else
	{
		/*
		 * Parse analysis was done already, but we still have to run the rule
		 * rewriter.  We do not do AcquireRewriteLocks: we assume the query
		 * either came straight from the parser, or suitable locks were
		 * acquired by plancache.c.
		 *
		 * Because the rewriter and planner tend to scribble on the input, we
		 * make a preliminary copy of the source querytree.  This prevents
		 * problems in the case that CTAS is in a portal or plpgsql function
		 * and is executed repeatedly.  (See also the same hack in EXPLAIN and
		 * PREPARE.)
		 */
		rewritten = QueryRewrite(copyObject(query));

		/* SELECT should never rewrite to more or less than one SELECT query */
		if (list_length(rewritten) != 1)
			elog(ERROR, "unexpected rewrite result for %s",
				 is_matview ? "CREATE MATERIALIZED VIEW" :
				 "CREATE TABLE AS SELECT");
		query = linitial_node(Query, rewritten);
		Assert(query->commandType == CMD_SELECT);

		/* plan the query */
		if (px_enable_create_table_as && px_enable_replay_wait &&
			!interpretOidsOption(into->options, !is_matview))
			plan = pg_plan_query(query, CURSOR_OPT_PARALLEL_OK | CURSOR_OPT_PX_OK, params);
		else
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
									dest, params, queryEnv, 0);

		/* POLAR px */
		queryDesc->plannedstmt->query_mem = ResourceManagerGetQueryMemoryLimit(queryDesc->plannedstmt);
		/* POLAR end */

		/* call ExecutorStart to prepare the plan for execution */
		ExecutorStart(queryDesc, GetIntoRelEFlags(into));

		/* run the plan to completion */
		ExecutorRun(queryDesc, ForwardScanDirection, 0L, true);

		/* save the rowcount if we're given a completionTag to fill */
		if (completionTag)
			snprintf(completionTag, COMPLETION_TAG_BUFSIZE,
					 "SELECT " UINT64_FORMAT,
					 queryDesc->estate->es_processed);

		/* get object address that intorel_startup saved for us */
		address = ((DR_intorel *) dest)->reladdr;

		/* and clean up */
		ExecutorFinish(queryDesc);
		ExecutorEnd(queryDesc);

		FreeQueryDesc(queryDesc);

		PopActiveSnapshot();
	}

	if (is_matview)
	{
		/* Roll back any GUC changes */
		AtEOXact_GUC(false, save_nestlevel);

		/* Restore userid and security context */
		SetUserIdAndSecContext(save_userid, save_sec_context);
	}

	return address;
}

/*
 * GetIntoRelEFlags --- compute executor flags needed for CREATE TABLE AS
 *
 * This is exported because EXPLAIN and PREPARE need it too.  (Note: those
 * callers still need to deal explicitly with the skipData flag; since they
 * use different methods for suppressing execution, it doesn't seem worth
 * trying to encapsulate that part.)
 */
int
GetIntoRelEFlags(IntoClause *intoClause)
{
	int			flags;

	/*
	 * We need to tell the executor whether it has to produce OIDs or not,
	 * because it doesn't have enough information to do so itself (since we
	 * can't build the target relation until after ExecutorStart).
	 *
	 * Disallow the OIDS option for materialized views.
	 */
	if (interpretOidsOption(intoClause->options,
							(intoClause->viewQuery == NULL)))
		flags = EXEC_FLAG_WITH_OIDS;
	else
		flags = EXEC_FLAG_WITHOUT_OIDS;

	if (intoClause->skipData)
		flags |= EXEC_FLAG_WITH_NO_DATA;

	return flags;
}

/*
 * CreateIntoRelDestReceiver -- create a suitable DestReceiver object
 *
 * intoClause will be NULL if called from CreateDestReceiver(), in which
 * case it has to be provided later.  However, it is convenient to allow
 * self->into to be filled in immediately for other callers.
 */
DestReceiver *
CreateIntoRelDestReceiver(IntoClause *intoClause)
{
	DR_intorel *self = (DR_intorel *) palloc0(sizeof(DR_intorel));

	self->pub.receiveSlot = intorel_receive;
	self->pub.rStartup = intorel_startup;
	self->pub.rShutdown = intorel_shutdown;
	self->pub.rDestroy = intorel_destroy;
	self->pub.mydest = DestIntoRel;
	self->into = intoClause;
	/* other private fields will be set during intorel_startup */

	return (DestReceiver *) self;
}

/*
 * intorel_startup --- executor startup
 */
static void
intorel_startup(DestReceiver *self, int operation, TupleDesc typeinfo)
{
	DR_intorel *myState = (DR_intorel *) self;
	IntoClause *into = myState->into;
	bool		is_matview;
	char		relkind;
	List	   *attrList;
	ObjectAddress intoRelationAddr;
	Relation	intoRelationDesc;
	RangeTblEntry *rte;
	ListCell   *lc;
	int			attnum;

	Assert(into != NULL);		/* else somebody forgot to set it */

	/* This code supports both CREATE TABLE AS and CREATE MATERIALIZED VIEW */
	is_matview = (into->viewQuery != NULL);
	relkind = is_matview ? RELKIND_MATVIEW : RELKIND_RELATION;

	/*
	 * Build column definitions using "pre-cooked" type and collation info. If
	 * a column name list was specified in CREATE TABLE AS, override the
	 * column names derived from the query.  (Too few column names are OK, too
	 * many are not.)
	 */
	attrList = NIL;
	lc = list_head(into->colNames);
	for (attnum = 0; attnum < typeinfo->natts; attnum++)
	{
		Form_pg_attribute attribute = TupleDescAttr(typeinfo, attnum);
		ColumnDef  *col;
		char	   *colname;

		if (lc)
		{
			colname = strVal(lfirst(lc));
			lc = lnext(lc);
		}
		else
			colname = NameStr(attribute->attname);

		col = makeColumnDef(colname,
							attribute->atttypid,
							attribute->atttypmod,
							attribute->attcollation);

		/*
		 * It's possible that the column is of a collatable type but the
		 * collation could not be resolved, so double-check.  (We must check
		 * this here because DefineRelation would adopt the type's default
		 * collation rather than complaining.)
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

	if (lc != NULL)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("too many column names were specified")));

	/*
	 * Actually create the target table
	 */
	intoRelationAddr = create_ctas_internal(attrList, into);

	/*
	 * Finally we can open the target table
	 */
	intoRelationDesc = heap_open(intoRelationAddr.objectId, AccessExclusiveLock);

	/*
	 * Check INSERT permission on the constructed table.
	 *
	 * XXX: It would arguably make sense to skip this check if into->skipData
	 * is true.
	 */
	rte = makeNode(RangeTblEntry);
	rte->rtekind = RTE_RELATION;
	rte->relid = intoRelationAddr.objectId;
	rte->relkind = relkind;
	rte->requiredPerms = ACL_INSERT;

	for (attnum = 1; attnum <= intoRelationDesc->rd_att->natts; attnum++)
		rte->insertedCols = bms_add_member(rte->insertedCols,
										   attnum - FirstLowInvalidHeapAttributeNumber);

	ExecCheckRTPerms(list_make1(rte), true);

	/*
	 * Make sure the constructed table does not have RLS enabled.
	 *
	 * check_enable_rls() will ereport(ERROR) itself if the user has requested
	 * something invalid, and otherwise will return RLS_ENABLED if RLS should
	 * be enabled here.  We don't actually support that currently, so throw
	 * our own ereport(ERROR) if that happens.
	 */
	if (check_enable_rls(intoRelationAddr.objectId, InvalidOid, false) == RLS_ENABLED)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 (errmsg("policies not yet implemented for this command"))));

	/*
	 * Tentatively mark the target as populated, if it's a matview and we're
	 * going to fill it; otherwise, no change needed.
	 */
	if (is_matview && !into->skipData)
		SetMatViewPopulatedState(intoRelationDesc, true);

	/*
	 * Fill private fields of myState for use by later routines
	 */
	myState->rel = intoRelationDesc;
	myState->reladdr = intoRelationAddr;
	myState->output_cid = GetCurrentCommandId(true);

	/*
	 * We can skip WAL-logging the insertions, unless PITR or streaming
	 * replication is in use. We can skip the FSM in any case.
	 */
	myState->hi_options = HEAP_INSERT_SKIP_FSM |
		(XLogIsNeeded() ? 0 : HEAP_INSERT_SKIP_WAL);
	myState->bistate = GetBulkInsertState();

	/* Not using WAL requires smgr_targblock be initially invalid */
	Assert(RelationGetTargetBlock(intoRelationDesc) == InvalidBlockNumber);
}

/*
 * intorel_receive --- receive one tuple
 */
static bool
intorel_receive(TupleTableSlot *slot, DestReceiver *self)
{
	DR_intorel *myState = (DR_intorel *) self;
	HeapTuple	tuple;

	/*
	 * get the heap tuple out of the tuple table slot, making sure we have a
	 * writable copy
	 */
	tuple = ExecMaterializeSlot(slot);

	/*
	 * force assignment of new OID (see comments in ExecInsert)
	 */
	if (myState->rel->rd_rel->relhasoids)
		HeapTupleSetOid(tuple, InvalidOid);

	heap_insert(myState->rel,
				tuple,
				myState->output_cid,
				myState->hi_options,
				myState->bistate);

	/* We know this is a newly created relation, so there are no indexes */

	return true;
}

/*
 * intorel_shutdown --- executor end
 */
static void
intorel_shutdown(DestReceiver *self)
{
	DR_intorel *myState = (DR_intorel *) self;

	FreeBulkInsertState(myState->bistate);

	/* If we skipped using WAL, must heap_sync before commit */
	if (myState->hi_options & HEAP_INSERT_SKIP_WAL)
		heap_sync(myState->rel);

	/* close rel, but keep lock until commit */
	heap_close(myState->rel, NoLock);
	myState->rel = NULL;
}

/*
 * intorel_destroy --- release DestReceiver object
 */
static void
intorel_destroy(DestReceiver *self)
{
	pfree(self);
}

/* POLAR px */

/*
 * POLAR px: CreateIntoRelDestReceiverBuffered.
 *
 * Create a suitable DestReceiver object. This is the buffered version
 * of CreateIntoRelDestReceiver. Received tuples will be buffered and
 * insert into the target table in batch.
 */
static DestReceiver *
CreateIntoRelDestReceiverBuffered(IntoClause *intoClause)
{
	DR_intorel *self = (DR_intorel *) palloc0(sizeof(DR_intorel));

	/* use buffered version of intorel callbacks */
	self->pub.receiveSlot = intorel_receive_buffered;
	self->pub.rStartup = intorel_startup_buffered;
	self->pub.rShutdown = intorel_shutdown_buffered;
	self->pub.rDestroy = intorel_destroy_buffered;
	self->pub.mydest = DestIntoRel;
	self->into = intoClause;

	/* initialization of tuple buffer array */
	self->nBufferedTuples = 0;
	self->bufferedTuples = NULL;
	self->bufferedTuplesSize = 0;
	self->tmpcontext = NULL;

	/* other private fields will be set during intorel_startup */

	return (DestReceiver *) self;
}

/*
 * POLAR px: intorel_startup_buffered.
 *
 * Executor starts up.
 */
static void
intorel_startup_buffered(DestReceiver *self, int operation, TupleDesc typeinfo)
{
	DR_intorel *myState = (DR_intorel *) self;

	/* original startup */
	intorel_startup(self, operation, typeinfo);

	myState->bufferedTuples = palloc(MAX_BUFFERED_TUPLES * sizeof(HeapTuple));
	myState->tmpcontext = AllocSetContextCreate(CurrentMemoryContext,
												"ctasbulk",
												ALLOCSET_DEFAULT_SIZES);
}

/*
 * POLAR px: intorel_receive_buffered.
 *
 * Receive one tuple, store it in buffer and call heap_multi_insert
 * when # of tuples buffered reaches threshold or total size of all
 * the buffered tuples reaches the limit.
 */
static bool
intorel_receive_buffered(TupleTableSlot *slot, DestReceiver *self)
{
	DR_intorel *myState = (DR_intorel *) self;
	HeapTuple	tuple;
	HeapTuple tuple_copy;
	MemoryContext oldcontext;

	/*
	 * get the heap tuple out of the tuple table slot, making sure we have a
	 * writable copy
	 */
	tuple = ExecMaterializeSlot(slot);
	tuple_copy = heap_copytuple(tuple);

	/*
	 * force assignment of new OID (see comments in ExecInsert)
	 */
	if (myState->rel->rd_rel->relhasoids)
		HeapTupleSetOid(tuple_copy, InvalidOid);

	myState->bufferedTuples[myState->nBufferedTuples++] = tuple_copy;
	myState->bufferedTuplesSize += tuple_copy->t_len;

	/*
	 * If the buffer filled up, flush it. Also flush if the
	 * total size of all the tuples in the buffer becomes
	 * large, to avoid using large amounts of memory for the
	 * buffer when the tuples are exceptionally wide.
	 */
	if (myState->nBufferedTuples == MAX_BUFFERED_TUPLES ||
		myState->bufferedTuplesSize > 65535)
	{
		oldcontext = MemoryContextSwitchTo(myState->tmpcontext);
		heap_multi_insert(myState->rel, 
						  myState->bufferedTuples, 
						  myState->nBufferedTuples, 
						  myState->output_cid,
						  myState->hi_options,
						  myState->bistate);
		MemoryContextSwitchTo(oldcontext);
		MemoryContextReset(myState->tmpcontext);

		for (int i = 0; i < myState->nBufferedTuples; i++)
			heap_freetuple(myState->bufferedTuples[i]);

		myState->nBufferedTuples = 0;
		myState->bufferedTuplesSize = 0;
	}

	/* We know this is a newly created relation, so there are no indexes */

	return true;
}

/*
 * POLAR px: intorel_shutdown_buffered.
 *
 * Executor ends. Flush tuples that are still in the buffer.
 */
static void
intorel_shutdown_buffered(DestReceiver *self)
{
	DR_intorel *myState = (DR_intorel *) self;
	MemoryContext oldcontext;

	/* flush tuples that are still in the buffer */
	if (myState->nBufferedTuples > 0)
	{
		oldcontext = MemoryContextSwitchTo(myState->tmpcontext);
		heap_multi_insert(myState->rel, 
						  myState->bufferedTuples, 
						  myState->nBufferedTuples, 
						  myState->output_cid,
						  myState->hi_options,
						  myState->bistate);
		MemoryContextSwitchTo(oldcontext);
		MemoryContextReset(myState->tmpcontext);

		for (int i = 0; i < myState->nBufferedTuples; i++)
			heap_freetuple(myState->bufferedTuples[i]);

		myState->nBufferedTuples = 0;
		myState->bufferedTuplesSize = 0;
	}

	/* original shutdown */
	intorel_shutdown(self);
}

/*
 * POLAR px: intorel_destroy_buffered
 */
static void
intorel_destroy_buffered(DestReceiver *self)
{
	DR_intorel *myState = (DR_intorel *) self;

	/* destroy memory context for storing tuple backups */
	MemoryContextDelete(myState->tmpcontext);
	myState->tmpcontext = NULL;

	/* release tuple buffer array */
	pfree(myState->bufferedTuples);

	/* original destory */
	intorel_destroy(self);
}

/* POLAR end */
