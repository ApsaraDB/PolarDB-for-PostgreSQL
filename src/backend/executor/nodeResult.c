/*-------------------------------------------------------------------------
 *
 * nodeResult.c
 *	  support for constant nodes needing special code.
 *
 * DESCRIPTION
 *
 *		Result nodes are used in queries where no relations are scanned.
 *		Examples of such queries are:
 *
 *				select 1 * 2
 *
 *				insert into emp values ('mike', 15000)
 *
 *		(Remember that in an INSERT or UPDATE, we need a plan tree that
 *		generates the new rows.)
 *
 *		Result nodes are also used to optimise queries with constant
 *		qualifications (ie, quals that do not depend on the scanned data),
 *		such as:
 *
 *				select * from emp where 2 > 1
 *
 *		In this case, the plan generated is
 *
 *						Result	(with 2 > 1 qual)
 *						/
 *				   SeqScan (emp.*)
 *
 *		At runtime, the Result node evaluates the constant qual once,
 *		which is shown by EXPLAIN as a One-Time Filter.  If it's
 *		false, we can return an empty result set without running the
 *		controlled plan at all.  If it's true, we run the controlled
 *		plan normally and pass back the results.
 *
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/executor/nodeResult.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "executor/executor.h"
#include "executor/nodeResult.h"
#include "miscadmin.h"
#include "utils/memutils.h"

/* POLAR px */
#include "px/px_vars.h"
#include "px/px_hash.h"

static bool TupleMatchesHashFilter(ResultState *node, TupleTableSlot *resultSlot);

/* ----------------------------------------------------------------
 *		ExecResult(node)
 *
 *		returns the tuples from the outer plan which satisfy the
 *		qualification clause.  Since result nodes with right
 *		subtrees are never planned, we ignore the right subtree
 *		entirely (for now).. -cim 10/7/89
 *
 *		The qualification containing only constant clauses are
 *		checked first before any processing is done. It always returns
 *		'nil' if the constant qualification is not satisfied.
 * ----------------------------------------------------------------
 */
static TupleTableSlot *
ExecResult(PlanState *pstate)
{
	ResultState *node = castNode(ResultState, pstate);
	TupleTableSlot *outerTupleSlot;
	PlanState  *outerPlan;
	ExprContext *econtext;

	/* POLAR px */
	TupleTableSlot *candidateOutputSlot;

	CHECK_FOR_INTERRUPTS();

	econtext = node->ps.ps_ExprContext;

	/*
	 * check constant qualifications like (2 > 1), if not already done
	 */
	if (node->rs_checkqual)
	{
		bool		qualResult = ExecQual(node->resconstantqual, econtext);

		node->rs_checkqual = false;
		if (!qualResult)
		{
			node->rs_done = true;
			return NULL;
		}
	}

	/*
	 * Reset per-tuple memory context to free any expression evaluation
	 * storage allocated in the previous tuple cycle.
	 */
	ResetExprContext(econtext);

	/*
	 * if rs_done is true then it means that we were asked to return a
	 * constant tuple and we already did the last time ExecResult() was
	 * called, OR that we failed the constant qual check. Either way, now we
	 * are through.
	 */
	while (!node->rs_done)
	{
		outerPlan = outerPlanState(node);

		if (outerPlan != NULL)
		{
			/*
			 * retrieve tuples from the outer plan until there are no more.
			 */
			outerTupleSlot = ExecProcNode(outerPlan);

			if (TupIsNull(outerTupleSlot))
				return NULL;

			/*
			 * POLAR px:
			 * nodeResult may perform filtering just like 'group by having'
			 * having clause performed in nodeResult not in nodeAgg
			 * this while loop would perform more than one time
			 * so econntext need reset every time.
			 */
			ResetExprContext(econtext);

			/*
			 * prepare to compute projection expressions, which will expect to
			 * access the input tuples as varno OUTER.
			 */
			econtext->ecxt_outertuple = outerTupleSlot;

			/**
			 * POLAR px
			 * Extract out qual in case result node is also performing filtering.
			 */
			if (node->ps.qual && !ExecQual(node->ps.qual, econtext))
				continue;
		}
		else
		{
			/*
			 * if we don't have an outer plan, then we are just generating the
			 * results from a constant target list.  Do it only once.
			 */
			node->rs_done = true;
		}

		/* POLAR px */
		/* form the result tuple using ExecProject(), and return it */
		candidateOutputSlot = ExecProject(node->ps.ps_ProjInfo);

		/*
		 * If there was a GPDB hash filter, check that too. Note that
		 * the hash filter is expressed in terms of *result* slot, so
		 * we must do this after projecting.
		 */
		if (!TupleMatchesHashFilter(node, candidateOutputSlot))
			continue;

		return candidateOutputSlot;
	}

	return NULL;
}


/** POLAR px
 * Returns true if tuple matches hash filter.
 */
static bool
TupleMatchesHashFilter(ResultState *node, TupleTableSlot *resultSlot)
{
	Result	   *resultNode = (Result *)node->ps.plan;
	bool		res = true;

	Assert(resultNode);
	Assert(!TupIsNull(resultSlot));

	if (node->hashFilter)
	{
		int			i;
		int			targetSeg;
		pxhashinit(node->hashFilter);
		for (i = 0; i < resultNode->numHashFilterCols; i++)
		{
			int			attnum = resultNode->hashFilterColIdx[i];
			Datum		hAttr;
			bool		isnull;

			hAttr = slot_getattr(resultSlot, attnum, &isnull);

			pxhash(node->hashFilter, i + 1, hAttr, isnull);
		}

		targetSeg = pxhashreduce(node->hashFilter);

		res = (targetSeg == PxIdentity.workerid);
	}

	return res;
}

/* ----------------------------------------------------------------
 *		ExecResultMarkPos
 * ----------------------------------------------------------------
 */
void
ExecResultMarkPos(ResultState *node)
{
	PlanState  *outerPlan = outerPlanState(node);

	if (outerPlan != NULL)
		ExecMarkPos(outerPlan);
	else
		elog(DEBUG2, "Result nodes do not support mark/restore");
}

/* ----------------------------------------------------------------
 *		ExecResultRestrPos
 * ----------------------------------------------------------------
 */
void
ExecResultRestrPos(ResultState *node)
{
	PlanState  *outerPlan = outerPlanState(node);

	if (outerPlan != NULL)
		ExecRestrPos(outerPlan);
	else
		elog(ERROR, "Result nodes do not support mark/restore");
}

/* ----------------------------------------------------------------
 *		ExecInitResult
 *
 *		Creates the run-time state information for the result node
 *		produced by the planner and initializes outer relations
 *		(child nodes).
 * ----------------------------------------------------------------
 */
ResultState *
ExecInitResult(Result *node, EState *estate, int eflags)
{
	ResultState *resstate;

	/* check for unsupported flags */
	Assert(!(eflags & (EXEC_FLAG_MARK | EXEC_FLAG_BACKWARD)) ||
		   outerPlan(node) != NULL);

	/*
	 * create state structure
	 */
	resstate = makeNode(ResultState);
	resstate->ps.plan = (Plan *) node;
	resstate->ps.state = estate;
	resstate->ps.ExecProcNode = ExecResult;

	resstate->rs_done = false;
	resstate->rs_checkqual = (node->resconstantqual == NULL) ? false : true;

	/*
	 * Miscellaneous initialization
	 *
	 * create expression context for node
	 */
	ExecAssignExprContext(estate, &resstate->ps);

	/*
	 * initialize child nodes
	 */
	outerPlanState(resstate) = ExecInitNode(outerPlan(node), estate, eflags);

	/*
	 * we don't use inner plan
	 */
	Assert(innerPlan(node) == NULL);

	/*
	 * Initialize result slot, type and projection.
	 */
	ExecInitResultTupleSlotTL(estate, &resstate->ps);
	ExecAssignProjectionInfo(&resstate->ps, NULL);

	/*
	 * initialize child expressions
	 */
	resstate->ps.qual =
		ExecInitQual(node->plan.qual, (PlanState *) resstate);
	resstate->resconstantqual =
		ExecInitQual((List *) node->resconstantqual, (PlanState *) resstate);

	/* POLAR px
	 * initialize hash filter
	 */
	if (node->numHashFilterCols > 0)
	{
		int			currentSliceId = estate->currentSliceId;
		ExecSlice *currentSlice = &estate->es_sliceTable->slices[currentSliceId];

		resstate->hashFilter = makePxHash(currentSlice->planNumSegments,
										   node->numHashFilterCols,
										   node->hashFilterFuncs);
	}

	return resstate;
}

/* ----------------------------------------------------------------
 *		ExecEndResult
 *
 *		frees up storage allocated through C routines
 * ----------------------------------------------------------------
 */
void
ExecEndResult(ResultState *node)
{
	/*
	 * Free the exprcontext
	 */
	ExecFreeExprContext(&node->ps);

	/*
	 * clean out the tuple table
	 */
	ExecClearTuple(node->ps.ps_ResultTupleSlot);

	/*
	 * shut down subplans
	 */
	ExecEndNode(outerPlanState(node));
}

void
ExecReScanResult(ResultState *node)
{
	node->rs_done = false;
	node->rs_checkqual = (node->resconstantqual == NULL) ? false : true;

	/*
	 * If chgParam of subnode is not null then plan will be re-scanned by
	 * first ExecProcNode.
	 */
	if (node->ps.lefttree &&
		node->ps.lefttree->chgParam == NULL)
		ExecReScan(node->ps.lefttree);
}
