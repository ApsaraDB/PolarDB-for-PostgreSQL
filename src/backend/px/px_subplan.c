/*-------------------------------------------------------------------------
 *
 * px_subplan.c
 *	  Provides routines for preprocessing initPlan subplans
 *		and executing queries with initPlans
 *
 * Portions Copyright (c) 2003-2008, Greenplum inc
 * Portions Copyright (c) 2012-Present Pivotal Software, Inc.
 * Portions Copyright (c) 2021, Alibaba Group Holding Limited
 *
 *
 * IDENTIFICATION
 *	    src/backend/px/px_subplan.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "px/px_disp_query.h"
#include "px/px_llize.h"
#include "px/px_plan.h"

typedef struct ParamWalkerContext
{
	plan_tree_base_prefix base; /* Required prefix for
								 * plan_tree_walker/mutator */
	List	   *params;
} ParamWalkerContext;

static bool param_walker(Node *node, ParamWalkerContext *context);

/*
 * Helper function param_walker() walks a plan and adds any Param nodes
 * to a list in the ParamWalkerContext.
 *
 * This list is input to the function findParamType(), which loops over the
 * list looking for a specific paramid, and returns its type.
 */
static bool
param_walker(Node *node, ParamWalkerContext *context)
{
	if (node == NULL)
		return false;

	if (nodeTag(node) == T_Param)
	{
		Param	   *param = (Param *) node;

		if (param->paramkind == PARAM_EXEC)
		{
			context->params = lappend(context->params, param);
			return false;
		}
	}
	return plan_tree_walker(node, param_walker, context, true);
}
