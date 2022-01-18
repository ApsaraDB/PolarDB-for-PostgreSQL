/*-------------------------------------------------------------------------
 *
 * polar_exec_procnode.c
 *
 *
 * Copyright (c) 2021, Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * IDENTIFICATION
 *	  src/backend/executor/polar_exec_procnode.c
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "nodes/nodes.h"
#include "nodes/execnodes.h"
#include "executor/instrument.h"
#include "executor/polar_exec_procnode.h"

/* ------------------------------------------------------------------------
 *		polar_check_instrument_option
 *
 *		For polar db we only gather plan node information about 
 *		scan node, join node, sort node
 *
 *		Inputs:
 *		  'plan' is the current node of the plan produced by the query planner
 *		  'estate' is the shared execution state for the plan tree
 *		Returns a polar instrument option.
 * ------------------------------------------------------------------------
 */
int polar_check_instrument_option(Plan *plan, EState *estate)
{
	/*
	 * We assume the plan node isn't scan node, join node, sort node, set the value to zero.
	 * Only when the instrument option is INSTRUMENT_POLAR_ROWS and 
	 * plan node is can node, join node, sort node, we reset the instrument option.
	 */
	int polar_instrument_option = 0;
	/*
	 * If the instrument option is not include INSTRUMENT_POLAR_ROWS, all the plan nodes need instrument info
	 * so we just return the original instrument option.
	 */
	if ((estate->es_instrument & INSTRUMENT_POLAR_PLAN) == 0 )
	{
		return estate->es_instrument;
	}
	switch (nodeTag(plan))
	{
		/*
	 	 * For scan nodes
		 */
		case T_Scan:
		case T_SeqScan:
		case T_SampleScan:
		case T_IndexScan:
		case T_IndexOnlyScan:
		case T_BitmapIndexScan:
		case T_BitmapHeapScan:
		case T_TidScan:
		case T_SubqueryScan:
		case T_FunctionScan:
		case T_ValuesScan:
		case T_TableFuncScan:
		case T_CteScan:
		case T_NamedTuplestoreScan:
		case T_WorkTableScan:
		case T_ForeignScan:
		case T_CustomScan:
		/*
	 	 * For join nodes
		 */
		case T_NestLoop:
		case T_MergeJoin:
		case T_HashJoin:
		/*
	 	 * For sort nodes
		 */
		case T_Sort:
		/*
	 	 * For group or agg nodes
		 */
		case T_Group:
		case T_Agg:
		/*
	 	 * For hash nodes
		 */
		case T_Hash:
			/*
	 		 * The instrument option is INSTRUMENT_POLAR_ROWS and plan node is scan node, hashjoin node, sort group/agg hash nodes, 
			 * so we reset the instrument option.
			 */
			polar_instrument_option = estate->es_instrument;
			break;
		default:
			break;
	}
	return polar_instrument_option;
}
