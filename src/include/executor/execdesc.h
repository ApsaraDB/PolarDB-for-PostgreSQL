/*-------------------------------------------------------------------------
 *
 * execdesc.h
 *	  plan and query descriptor accessor macros used by the executor
 *	  and related modules.
 *
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/executor/execdesc.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef EXECDESC_H
#define EXECDESC_H

#include "nodes/execnodes.h"
#include "tcop/dest.h"
#include "executor/execdesc_px.h"
#include "executor/polar_exec_procnode.h"
/* ----------------
 *		query descriptor:
 *
 *	a QueryDesc encapsulates everything that the executor
 *	needs to execute the query.
 *
 *	For the convenience of SQL-language functions, we also support QueryDescs
 *	containing utility statements; these must not be passed to the executor
 *	however.
 * ---------------------
 */
typedef struct QueryDesc
{
	/* These fields are provided by CreateQueryDesc */
	CmdType		operation;		/* CMD_SELECT, CMD_UPDATE, etc. */
	PlannedStmt *plannedstmt;	/* planner's output (could be utility, too) */
	const char *sourceText;		/* source text of the query */
	Snapshot	snapshot;		/* snapshot to use for query */
	Snapshot	crosscheck_snapshot;	/* crosscheck for RI update/delete */
	DestReceiver *dest;			/* the destination for tuple output */
	ParamListInfo params;		/* param values being passed in */
	QueryEnvironment *queryEnv; /* query environment passed in */
	int			instrument_options; /* OR of InstrumentOption flags */

	/* These fields are set by ExecutorStart */
	TupleDesc	tupDesc;		/* descriptor for result tuples */
	EState	   *estate;			/* executor's query-wide state */
	PlanState  *planstate;		/* tree of per-plan-node state */

	/* POLAR px */
	bool		extended_query;   /* simple or extended query protocol? */
	char		*portal_name;	/* NULL for unnamed portal */
	/* POLAR end */

	/* This field is set by ExecutorRun */
	bool		already_executed;	/* true if previously executed */

	/* This is always set NULL by the core system, but plugins can change it */
	struct Instrumentation *totaltime;	/* total time spent in ExecutorRun */

	/* POLAR px */
	QueryDispatchDesc *ddesc;
	struct PxExplain_ShowStatCtx  *showstatctx;
	/* POLAR end */

	/* POLAR */ 
	const char *prepStmtName;       /* source prepared statement (NULL if none) */
	PolarStatSqlCollector *polar_sql_info;

	/* POLAR end */
	/* This value is set by ExecutorEnd */
	uint64		es_processed;	/* # of tuples processed for ProcessQuery(DML) */
	/* POLAR px */
} QueryDesc;

/* in pquery.c */
extern QueryDesc *CreateQueryDesc(PlannedStmt *plannedstmt,
				const char *sourceText,
				Snapshot snapshot,
				Snapshot crosscheck_snapshot,
				DestReceiver *dest,
				ParamListInfo params,
				QueryEnvironment *queryEnv,
				int instrument_options);

extern void FreeQueryDesc(QueryDesc *qdesc);

#endif							/* EXECDESC_H  */
