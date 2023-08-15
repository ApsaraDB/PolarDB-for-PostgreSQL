//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//	Copyright (C) 2021, Alibaba Group Holding Limiteds
//
//	@filename:
//		CTranslatorDXLToPlStmt.cpp
//
//	@doc:
//		Implementation of the methods for translating from DXL tree to GPDB
//		PlannedStmt.
//
//	@test:
//
//
//---------------------------------------------------------------------------

extern "C" {
#include "postgres.h"

#include "nodes/nodes.h"
#include "nodes/plannodes.h"
#include "nodes/primnodes.h"
#include "catalog/px_policy.h"
#include "catalog/pg_collation.h"
#include "catalog/partition.h"
#include "executor/executor.h"
#include "px/px_util.h"
#include "px/px_vars.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/typcache.h"
#include "utils/rel.h"
#include "utils/relcache.h"
#include "executor/executor.h"
#include "executor/execPartition.h"
#include "utils/partcache.h"
#include "nodes/bitmapset.h"
#include "access/sysattr.h"
}
#include <algorithm>
#include <numeric>
#include <tuple>
#include <vector>

#include "gpos/base.h"
#include "gpos/common/CBitSet.h"
#include "gpos/common/CBitSetIter.h"
#include "gpopt/mdcache/CMDAccessor.h"
#include "px_optimizer_util/translate/CTranslatorDXLToPlStmt.h"
#include "px_optimizer_util/translate/CTranslatorUtils.h"
#include "px_optimizer_util/translate/CIndexQualInfo.h"
#include "px_optimizer_util/px_wrappers.h"
#include "px_optimizer_util/utils/RelationWrapper.h"
#include "px_optimizer_util/translate/CPartPruneStepsBuilder.h"

#include "naucrates/dxl/operators/CDXLDirectDispatchInfo.h"
#include "naucrates/dxl/operators/CDXLNode.h"
#include "naucrates/dxl/operators/CDXLPhysicalAgg.h"
#include "naucrates/dxl/operators/CDXLPhysicalAppend.h"
#include "naucrates/dxl/operators/CDXLPhysicalAssert.h"
#include "naucrates/dxl/operators/CDXLPhysicalBitmapTableScan.h"
#include "naucrates/dxl/operators/CDXLPhysicalCTAS.h"
#include "naucrates/dxl/operators/CDXLPhysicalCTEConsumer.h"
#include "naucrates/dxl/operators/CDXLPhysicalCTEProducer.h"
#include "naucrates/dxl/operators/CDXLPhysicalGatherMotion.h"
#include "naucrates/dxl/operators/CDXLPhysicalHashJoin.h"
#include "naucrates/dxl/operators/CDXLPhysicalIndexOnlyScan.h"
#include "naucrates/dxl/operators/CDXLPhysicalLimit.h"
#include "naucrates/dxl/operators/CDXLPhysicalMaterialize.h"
#include "naucrates/dxl/operators/CDXLPhysicalMergeJoin.h"
#include "naucrates/dxl/operators/CDXLPhysicalNLJoin.h"
#include "naucrates/dxl/operators/CDXLPhysicalPartitionSelector.h"
#include "naucrates/dxl/operators/CDXLPhysicalRedistributeMotion.h"
#include "naucrates/dxl/operators/CDXLPhysicalResult.h"
#include "naucrates/dxl/operators/CDXLPhysicalRoutedDistributeMotion.h"
#include "naucrates/dxl/operators/CDXLPhysicalSort.h"
#include "naucrates/dxl/operators/CDXLPhysicalSplit.h"
#include "naucrates/dxl/operators/CDXLPhysicalSubqueryScan.h"
#include "naucrates/dxl/operators/CDXLPhysicalTVF.h"
#include "naucrates/dxl/operators/CDXLPhysicalTableScan.h"
#include "naucrates/dxl/operators/CDXLPhysicalTableShareScan.h"
#include "naucrates/dxl/operators/CDXLPhysicalValuesScan.h"
#include "naucrates/dxl/operators/CDXLPhysicalWindow.h"
#include "naucrates/dxl/operators/CDXLScalarBitmapBoolOp.h"
#include "naucrates/dxl/operators/CDXLScalarBitmapIndexProbe.h"
#include "naucrates/dxl/operators/CDXLScalarBoolExpr.h"
#include "naucrates/dxl/operators/CDXLScalarFuncExpr.h"
#include "naucrates/dxl/operators/CDXLScalarHashExpr.h"
#include "naucrates/dxl/operators/CDXLScalarOpExpr.h"
#include "naucrates/dxl/operators/CDXLScalarProjElem.h"
#include "naucrates/dxl/operators/CDXLScalarSortCol.h"
#include "naucrates/dxl/operators/CDXLScalarWindowFrameEdge.h"
#include "naucrates/exception.h"
#include "naucrates/md/IMDAggregate.h"
#include "naucrates/md/IMDFunction.h"
#include "naucrates/md/IMDIndex.h"
#include "naucrates/md/IMDRelationExternal.h"
#include "naucrates/md/IMDScalarOp.h"
#include "naucrates/md/IMDType.h"
#include "naucrates/md/IMDTypeBool.h"
#include "naucrates/md/IMDTypeInt4.h"
#include "naucrates/traceflags/traceflags.h"

#include "naucrates/dxl/operators/CDXLPhysicalShareIndexScan.h"

using namespace gpdxl;
using namespace gpos;
using namespace gpopt;
using namespace gpmd;

#define GPDXL_ROOT_PLAN_ID -1
#define GPDXL_PLAN_ID_START 1
#define GPDXL_MOTION_ID_START 1
#define GPDXL_PARAM_ID_START 0

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToPlStmt::CTranslatorDXLToPlStmt
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CTranslatorDXLToPlStmt::CTranslatorDXLToPlStmt(
	CMemoryPool *mp, CMDAccessor *md_accessor,
	CContextDXLToPlStmt *dxl_to_plstmt_context, ULONG num_of_segments)
	: m_mp(mp),
	  m_md_accessor(md_accessor),
	  m_dxl_to_plstmt_context(dxl_to_plstmt_context),
	  m_cmd_type(CMD_SELECT),
	  m_is_tgt_tbl_distributed(false),
	  m_result_rel_list(nullptr),
	  m_num_of_segments(num_of_segments)
{
	m_translator_dxl_to_scalar = GPOS_NEW(m_mp)
		CTranslatorDXLToScalar(m_mp, m_md_accessor, m_num_of_segments);
}


//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToPlStmt::~CTranslatorDXLToPlStmt
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CTranslatorDXLToPlStmt::~CTranslatorDXLToPlStmt()
{
	GPOS_DELETE(m_translator_dxl_to_scalar);
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToPlStmt::GetPlannedStmtFromDXL
//
//	@doc:
//		Translate DXL node into a PlannedStmt
//
//---------------------------------------------------------------------------
PlannedStmt *
CTranslatorDXLToPlStmt::GetPlannedStmtFromDXL(const CDXLNode *dxlnode,
											  const Query *orig_query,
											  bool can_set_tag)
{
	GPOS_ASSERT(NULL != dxlnode);

	CDXLTranslateContext dxl_translate_ctxt(m_mp, false);

	PlanSlice *topslice;

	topslice = (PlanSlice *) px::GPDBAlloc(sizeof(PlanSlice));
	memset(topslice, 0, sizeof(PlanSlice));
	topslice->sliceIndex = 0;
	topslice->parentIndex = -1;
	topslice->gangType = GANGTYPE_UNALLOCATED;
	topslice->numsegments = 1;
	topslice->worker_idx = -1;
	topslice->directDispatch.isDirectDispatch = false;
	topslice->directDispatch.contentIds = NIL;
	topslice->directDispatch.haveProcessedAnyCalculations = false;

	m_dxl_to_plstmt_context->AddSlice(topslice);
	m_dxl_to_plstmt_context->SetCurrentSlice(topslice);

	CDXLTranslationContextArray *ctxt_translation_prev_siblings =
		GPOS_NEW(m_mp) CDXLTranslationContextArray(m_mp);
	Plan *plan = TranslateDXLOperatorToPlan(dxlnode, &dxl_translate_ctxt,
											ctxt_translation_prev_siblings);
	ctxt_translation_prev_siblings->Release();

	GPOS_ASSERT(NULL != plan);

	// collect oids from rtable
	List *oids_list = NIL;

	ListCell *lc_rte = NULL;

	ForEach(lc_rte, m_dxl_to_plstmt_context->GetRTableEntriesList())
	{
		RangeTblEntry *pRTE = (RangeTblEntry *) lfirst(lc_rte);

		if (pRTE->rtekind == RTE_RELATION)
		{
			oids_list = px::LAppendOid(oids_list, pRTE->relid);
		}
	}

	// assemble planned stmt
	PlannedStmt *planned_stmt = MakeNode(PlannedStmt);
	planned_stmt->planGen = PLANGEN_PX;

	planned_stmt->rtable = m_dxl_to_plstmt_context->GetRTableEntriesList();
	planned_stmt->subplans = m_dxl_to_plstmt_context->GetSubplanEntriesList();
	planned_stmt->planTree = plan;

#if 0
	// store partitioned table indexes in planned stmt
	planned_stmt->queryPartOids = m_dxl_to_plstmt_context->GetPartitionedTablesList();
	planned_stmt->numSelectorsPerScanId = m_dxl_to_plstmt_context->GetNumPartitionSelectorsList();
#endif
	planned_stmt->canSetTag = can_set_tag;
	planned_stmt->relationOids = oids_list;

	planned_stmt->commandType = m_cmd_type;

	planned_stmt->resultRelations = m_result_rel_list;
	// GPDB_92_MERGE_FIXME: we really *should* be handling intoClause
	// but currently planner cheats (c.f. createas.c)
	// shift the intoClause handling into planner and re-enable this
	//	pplstmt->intoClause = m_pctxdxltoplstmt->Pintocl();
	// planned_stmt->intoPolicy = m_dxl_to_plstmt_context->GetDistributionPolicy();

	planned_stmt->paramExecTypes = m_dxl_to_plstmt_context->GetParamTypes();
	planned_stmt->nParamExec = px::ListLength(planned_stmt->paramExecTypes);
	planned_stmt->slices = m_dxl_to_plstmt_context->GetSlices(&planned_stmt->numSlices);
	planned_stmt->subplan_sliceIds = m_dxl_to_plstmt_context->GetSubplanSliceIdArray();

	topslice = &planned_stmt->slices[0];

	// Can we do direct dispatch?
	if (CMD_SELECT == m_cmd_type && NULL != dxlnode->GetDXLDirectDispatchInfo())
	{
		List *direct_dispatch_segids =
			TranslateDXLDirectDispatchInfo(dxlnode->GetDXLDirectDispatchInfo());

		if (direct_dispatch_segids != NIL)
		{
			for (int i = 0; i < planned_stmt->numSlices; i++)
			{
				PlanSlice *slice = &planned_stmt->slices[i];

				slice->directDispatch.isDirectDispatch = true;
				slice->directDispatch.contentIds = direct_dispatch_segids;
			}
		}
	}

	if (CMD_INSERT == m_cmd_type && planned_stmt->numSlices == 1 &&
		dxlnode->GetOperator()->GetDXLOperator() == EdxlopPhysicalDML)
	{
		CDXLPhysicalDML *phy_dml_dxlop =
			CDXLPhysicalDML::Cast(dxlnode->GetOperator());

		List *direct_dispatch_segids = TranslateDXLDirectDispatchInfo(
			phy_dml_dxlop->GetDXLDirectDispatchInfo());
		if (direct_dispatch_segids != NIL)
		{
			topslice->directDispatch.isDirectDispatch = true;
			topslice->directDispatch.contentIds = direct_dispatch_segids;
		}
	}
#if 0
	/*
	 * If it's a CREATE TABLE AS, we have to dispatch the top slice to
	 * all segments, because the catalog changes need to be made
	 * everywhere even if the data originates from only some segments.
	 */
	if (orig_query->commandType == CMD_SELECT &&
		orig_query->parentStmtType == PARENTSTMTTYPE_CTAS)
	{
		topslice->numsegments = m_num_of_segments;
		topslice->gangType = GANGTYPE_PRIMARY_WRITER;
	}
#endif

	return planned_stmt;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToPlStmt::TranslateDXLOperatorToPlan
//
//	@doc:
//		Translates a DXL tree into a Plan node
//
//---------------------------------------------------------------------------
Plan *
CTranslatorDXLToPlStmt::TranslateDXLOperatorToPlan(
	const CDXLNode *dxlnode, CDXLTranslateContext *output_context,
	CDXLTranslationContextArray *ctxt_translation_prev_siblings)
{
	GPOS_ASSERT(nullptr != dxlnode);
	GPOS_ASSERT(nullptr != ctxt_translation_prev_siblings);

	Plan *plan;

	const CDXLOperator *dxlop = dxlnode->GetOperator();
	gpdxl::Edxlopid ulOpId = dxlop->GetDXLOperator();

	switch (ulOpId)
	{
		default:
		{
			GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXL2PlStmtConversion,
					   dxlnode->GetOperator()->GetOpNameStr()->GetBuffer());
		}
		case EdxlopPhysicalTableScan:
		case EdxlopPhysicalExternalScan:
		{
			plan = TranslateDXLTblScan(dxlnode, output_context,
									   ctxt_translation_prev_siblings);
			break;
		}
		case EdxlopPhysicalTableShareScan:
		{
			plan = TranslateDXLTblShareScan(dxlnode, output_context,
									   ctxt_translation_prev_siblings);
			break;
		}
		case EdxlopPhysicalIndexScan:
		{
			plan = TranslateDXLIndexScan(dxlnode, output_context,
										 ctxt_translation_prev_siblings);
			break;
		}
		case EdxlopPhysicalIndexOnlyScan:
		{
			plan = TranslateDXLIndexOnlyScan(dxlnode, output_context,
											 ctxt_translation_prev_siblings);
			break;
		}

		/* POLAR px */
		case EdxlopPhysicalShareIndexScan:
		{
			plan = TranslateDXLShareIndexScan(dxlnode, output_context,
											 ctxt_translation_prev_siblings);
			break;
		}
		case EdxlopPhysicalHashJoin:
		{
			plan = TranslateDXLHashJoin(dxlnode, output_context,
										ctxt_translation_prev_siblings);
			break;
		}
		case EdxlopPhysicalNLJoin:
		{
			plan = TranslateDXLNLJoin(dxlnode, output_context,
									  ctxt_translation_prev_siblings);
			break;
		}
		case EdxlopPhysicalMergeJoin:
		{
			plan = TranslateDXLMergeJoin(dxlnode, output_context,
										 ctxt_translation_prev_siblings);
			break;
		}
		case EdxlopPhysicalMotionGather:
		case EdxlopPhysicalMotionBroadcast:
		case EdxlopPhysicalMotionRoutedDistribute:
		{
			plan = TranslateDXLMotion(dxlnode, output_context,
									  ctxt_translation_prev_siblings);
			break;
		}
		case EdxlopPhysicalMotionRedistribute:
		case EdxlopPhysicalMotionRandom:
		{
			plan = TranslateDXLDuplicateSensitiveMotion(
				dxlnode, output_context, ctxt_translation_prev_siblings);
			break;
		}
		case EdxlopPhysicalLimit:
		{
			plan = TranslateDXLLimit(dxlnode, output_context,
									 ctxt_translation_prev_siblings);
			break;
		}
		case EdxlopPhysicalAgg:
		{
			plan = TranslateDXLAgg(dxlnode, output_context,
								   ctxt_translation_prev_siblings);
			break;
		}
		case EdxlopPhysicalWindow:
		{
			plan = TranslateDXLWindow(dxlnode, output_context,
									  ctxt_translation_prev_siblings);
			break;
		}
		case EdxlopPhysicalSort:
		{
			plan = TranslateDXLSort(dxlnode, output_context,
									ctxt_translation_prev_siblings);
			break;
		}
		case EdxlopPhysicalSubqueryScan:
		{
			plan = TranslateDXLSubQueryScan(dxlnode, output_context,
											ctxt_translation_prev_siblings);
			break;
		}
		case EdxlopPhysicalResult:
		{
			plan = TranslateDXLResult(dxlnode, output_context,
									  ctxt_translation_prev_siblings);
			break;
		}
		case EdxlopPhysicalAppend:
		{
			plan = TranslateDXLAppend(dxlnode, output_context,
									  ctxt_translation_prev_siblings);
			break;
		}
		case EdxlopPhysicalMaterialize:
		{
			plan = TranslateDXLMaterialize(dxlnode, output_context,
										   ctxt_translation_prev_siblings);
			break;
		}
		case EdxlopPhysicalSequence:
		{
			plan = TranslateDXLSequence(dxlnode, output_context,
										ctxt_translation_prev_siblings);
			break;
		}
		/* case EdxlopPhysicalDynamicIndexScan: { plan = TranslateDXLDynIdxScan(dxlnode, output_context, ctxt_translation_prev_siblings); break;} */
		case EdxlopPhysicalTVF:
		{
			plan = TranslateDXLTvf(dxlnode, output_context,
								   ctxt_translation_prev_siblings);
			break;
		}
		case EdxlopPhysicalDML:
		{
			plan = TranslateDXLDml(dxlnode, output_context,
								   ctxt_translation_prev_siblings);
			break;
		}
		case EdxlopPhysicalSplit:
		{
			plan = TranslateDXLSplit(dxlnode, output_context,
									 ctxt_translation_prev_siblings);
			break;
		}
		// GPDB_12_MERGE_FIXME: stop generating AssertOp from ORCA
		case EdxlopPhysicalAssert: 
		{
			plan = TranslateDXLAssert(dxlnode, output_context, 
			ctxt_translation_prev_siblings); 
			break;
		}
		case EdxlopPhysicalCTEProducer:
		{
			plan = TranslateDXLCTEProducerToSharedScan(
				dxlnode, output_context, ctxt_translation_prev_siblings);
			break;
		}
		case EdxlopPhysicalCTEConsumer:
		{
			plan = TranslateDXLCTEConsumerToSharedScan(
				dxlnode, output_context, ctxt_translation_prev_siblings);
			break;
		}
		case EdxlopPhysicalBitmapTableScan:
		{
			plan = TranslateDXLBitmapTblScan(dxlnode, output_context,
											 ctxt_translation_prev_siblings);
			break;
		}
		// case EdxlopPhysicalCTAS:
		// {
		// 	plan = TranslateDXLCtas(dxlnode, output_context,
		// 							ctxt_translation_prev_siblings);
		// 	break;
		// }
		case EdxlopPhysicalPartitionSelector:
		{
			plan = TranslateDXLPartSelector(dxlnode, output_context,
											ctxt_translation_prev_siblings);
			break;
		}
		case EdxlopPhysicalValuesScan:
		{
			plan = TranslateDXLValueScan(dxlnode, output_context,
										 ctxt_translation_prev_siblings);
			break;
		}
	}

	if (nullptr == plan)
	{
/*no cover begin*/
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXL2PlStmtConversion,
				   dxlnode->GetOperator()->GetOpNameStr()->GetBuffer());
/*no cover end*/
	}
	return plan;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToPlStmt::SetParamIds
//
//	@doc:
//		Set the bitmapset with the param_ids defined in the plan
//
//---------------------------------------------------------------------------
void
CTranslatorDXLToPlStmt::SetParamIds(Plan *plan)
{
	List *params_node_list = px::ExtractNodesPlan(
		plan, T_Param, true /* descend_into_subqueries */);

	ListCell *lc = NULL;

	Bitmapset *bitmapset = NULL;

	ForEach(lc, params_node_list)
	{
		Param *param = (Param *) lfirst(lc);
		bitmapset = px::BmsAddMember(bitmapset, param->paramid);
	}

	plan->extParam = bitmapset;
	plan->allParam = bitmapset;
}


//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToPlStmt::TranslateDXLTblScan
//
//	@doc:
//		Translates a DXL table scan node into a TableScan node
//
//---------------------------------------------------------------------------
Plan *
CTranslatorDXLToPlStmt::TranslateDXLTblScan(
	const CDXLNode *tbl_scan_dxlnode, CDXLTranslateContext *output_context,
	CDXLTranslationContextArray *ctxt_translation_prev_siblings)
{
	// translate table descriptor into a range table entry
	CDXLPhysicalTableScan *phy_tbl_scan_dxlop =
		CDXLPhysicalTableScan::Cast(tbl_scan_dxlnode->GetOperator());

	// translation context for column mappings in the base relation
	CDXLTranslateContextBaseTable base_table_context(m_mp);

	// we will add the new range table entry as the last element of the range table
	Index index =
		px::ListLength(m_dxl_to_plstmt_context->GetRTableEntriesList()) + 1;

	const CDXLTableDescr *dxl_table_descr =
		phy_tbl_scan_dxlop->GetDXLTableDescr();

	const IMDRelation *md_rel =
		m_md_accessor->RetrieveRel(dxl_table_descr->MDId());

	// Lock any table we are to scan, since it may not have been properly locked
	// by the parser (e.g in case of generated scans for partitioned tables)
	CMDIdGPDB *mdid = CMDIdGPDB::CastMdid(md_rel->MDId());
	GPOS_RTL_ASSERT(dxl_table_descr->LockMode() != -1);
	px::GPDBLockRelationOid(mdid->Oid(), dxl_table_descr->LockMode());

	RangeTblEntry *rte = TranslateDXLTblDescrToRangeTblEntry(
		dxl_table_descr, index, &base_table_context);
	GPOS_ASSERT(NULL != rte);
	rte->requiredPerms |= ACL_SELECT;
	m_dxl_to_plstmt_context->AddRTE(rte);

	// a table scan node must have 2 children: projection list and filter
	GPOS_ASSERT(2 == tbl_scan_dxlnode->Arity());

	// translate proj list and filter
	CDXLNode *project_list_dxlnode = (*tbl_scan_dxlnode)[EdxltsIndexProjList];
	CDXLNode *filter_dxlnode = (*tbl_scan_dxlnode)[EdxltsIndexFilter];

	List *targetlist = NIL;
	List *qual = NIL;

	TranslateProjListAndFilter(
		project_list_dxlnode, filter_dxlnode,
		&base_table_context,  // translate context for the base table
		NULL,				  // translate_ctxt_left and pdxltrctxRight,
		&targetlist, &qual, output_context);

	Plan *plan = NULL;
	Plan *plan_return = NULL;
#if 0
	if (IMDRelation::ErelstorageExternal == md_rel->RetrieveRelStorageType())
	{
		OID oidRel = CMDIdGPDB::CastMdid(md_rel->MDId())->Oid();

		// create foreign scan node
		ForeignScan *foreign_scan = px::CreateForeignScanForExternalTable(
			oidRel, index, qual, targetlist);
		plan = &(foreign_scan->scan.plan);
		plan_return = (Plan *) foreign_scan;
	}
	else
#endif
	if (IMDRelation::ErelstorageExternal != md_rel->RetrieveRelStorageType())
	{
		// create seq scan node
		SeqScan *seq_scan = MakeNode(SeqScan);
		seq_scan->scanrelid = index;
		seq_scan->plan.px_scan_partial = true;

		plan = &(seq_scan->plan);
		plan_return = (Plan *) seq_scan;

		plan->targetlist = targetlist;
		plan->qual = qual;
	}


	plan->plan_node_id = m_dxl_to_plstmt_context->GetNextPlanId();

	// translate operator costs
	TranslatePlanCosts(tbl_scan_dxlnode, plan);

	SetParamIds(plan);

	return plan_return;
}

/*-------------------------------------------------------------------------
*	@function:
*		CTranslatorDXLToPlStmt::TranslateDXLTblShareScan
*
*	@doc:
*		Translates a DXL table share scan node into a TableScan node
*
-------------------------------------------------------------------------*/
Plan *
CTranslatorDXLToPlStmt::TranslateDXLTblShareScan
	(
	const CDXLNode *tbl_scan_dxlnode,
	CDXLTranslateContext *output_context,
	CDXLTranslationContextArray *ctxt_translation_prev_siblings
	)
{
	// translate table descriptor into a range table entry
	CDXLPhysicalTableShareScan *phy_tbl_scan_dxlop = CDXLPhysicalTableShareScan::Cast(tbl_scan_dxlnode->GetOperator());

	// translation context for column mappings in the base relation
	CDXLTranslateContextBaseTable base_table_context(m_mp);

	// we will add the new range table entry as the last element of the range table
	Index index = px::ListLength(m_dxl_to_plstmt_context->GetRTableEntriesList()) + 1;

	const CDXLTableDescr *dxl_table_descr = phy_tbl_scan_dxlop->GetDXLTableDescr();
	RangeTblEntry *rte = TranslateDXLTblDescrToRangeTblEntry(dxl_table_descr, index, &base_table_context);
	GPOS_ASSERT(NULL != rte);
	rte->requiredPerms |= ACL_SELECT;
	m_dxl_to_plstmt_context->AddRTE(rte);

	Plan *plan = NULL;
	Plan *plan_return = NULL;

	{
		// create seq scan node
		SeqScan *seq_scan = MakeNode(SeqScan);
		seq_scan->scanrelid = index;
		plan = &(seq_scan->plan);
		plan_return = (Plan *) seq_scan;
	}

	plan->plan_node_id = m_dxl_to_plstmt_context->GetNextPlanId();
	// translate operator costs
	TranslatePlanCosts(tbl_scan_dxlnode, plan);

	// a table scan node must have 2 children: projection list and filter
	GPOS_ASSERT(2 == tbl_scan_dxlnode->Arity());

	// translate proj list and filter
	CDXLNode *project_list_dxlnode = (*tbl_scan_dxlnode)[EdxltsIndexProjList];
	CDXLNode *filter_dxlnode = (*tbl_scan_dxlnode)[EdxltsIndexFilter];

	TranslateProjListAndFilter
	(
		project_list_dxlnode,
		filter_dxlnode,
		&base_table_context,	// translate context for the base table
		NULL,			// translate_ctxt_left and pdxltrctxRight,
		&plan->targetlist,
		&plan->qual,
		output_context
		);

	SetParamIds(plan);

	return plan_return;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToPlStmt::SetIndexVarAttnoWalker
//
//	@doc:
//		Walker to set index var attno's,
//		attnos of index vars are set to their relative positions in index keys,
//		skip any outer references while walking the expression tree
//
//---------------------------------------------------------------------------
BOOL
CTranslatorDXLToPlStmt::SetIndexVarAttnoWalker(
	Node *node, SContextIndexVarAttno *ctxt_index_var_attno_walker)
{
	if (NULL == node)
	{
		return false;
	}

	if (IsA(node, Var) && ((Var *) node)->varno != OUTER_VAR)
	{
		INT attno = ((Var *) node)->varattno;
		const IMDRelation *md_rel = ctxt_index_var_attno_walker->m_md_rel;
		const IMDIndex *index = ctxt_index_var_attno_walker->m_md_index;

		ULONG index_col_pos_idx_max = gpos::ulong_max;
		const ULONG arity = md_rel->ColumnCount();
		for (ULONG col_pos_idx = 0; col_pos_idx < arity; col_pos_idx++)
		{
			const IMDColumn *md_col = md_rel->GetMdCol(col_pos_idx);
			if (attno == md_col->AttrNum())
			{
				index_col_pos_idx_max = col_pos_idx;
				break;
			}
		}

		if (gpos::ulong_max > index_col_pos_idx_max)
		{
			((Var *) node)->varattno =
				1 + index->GetKeyPos(index_col_pos_idx_max);
		}

		return false;
	}

	return px::WalkExpressionTree(
		node, (BOOL(*)()) CTranslatorDXLToPlStmt::SetIndexVarAttnoWalker,
		ctxt_index_var_attno_walker);
}


//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToPlStmt::TranslateDXLIndexScan
//
//	@doc:
//		Translates a DXL index scan node into a IndexScan node
//
//---------------------------------------------------------------------------
Plan *
CTranslatorDXLToPlStmt::TranslateDXLIndexScan(
	const CDXLNode *index_scan_dxlnode, CDXLTranslateContext *output_context,
	CDXLTranslationContextArray *ctxt_translation_prev_siblings)
{
	// translate table descriptor into a range table entry
	CDXLPhysicalIndexScan *physical_idx_scan_dxlop =
		CDXLPhysicalIndexScan::Cast(index_scan_dxlnode->GetOperator());

	return TranslateDXLIndexScan(index_scan_dxlnode, physical_idx_scan_dxlop,
								 output_context,
								 ctxt_translation_prev_siblings);
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToPlStmt::TranslateDXLIndexScan
//
//	@doc:
//		Translates a DXL index scan node into a IndexScan node
//
//---------------------------------------------------------------------------
Plan *
CTranslatorDXLToPlStmt::TranslateDXLIndexScan(
	const CDXLNode *index_scan_dxlnode,
	CDXLPhysicalIndexScan *physical_idx_scan_dxlop,
	CDXLTranslateContext *output_context,
	CDXLTranslationContextArray *ctxt_translation_prev_siblings)
{
	// translation context for column mappings in the base relation
	CDXLTranslateContextBaseTable base_table_context(m_mp);

	Index index =
		px::ListLength(m_dxl_to_plstmt_context->GetRTableEntriesList()) + 1;

	const CDXLTableDescr *dxl_table_descr =
		physical_idx_scan_dxlop->GetDXLTableDescr();
	const IMDRelation *md_rel =
		m_md_accessor->RetrieveRel(dxl_table_descr->MDId());

	// Lock any table we are to scan, since it may not have been properly locked
	// by the parser (e.g in case of generated scans for partitioned tables)
	CMDIdGPDB *mdid = CMDIdGPDB::CastMdid(md_rel->MDId());
	GPOS_RTL_ASSERT(dxl_table_descr->LockMode() != -1);
	px::GPDBLockRelationOid(mdid->Oid(), dxl_table_descr->LockMode());

	RangeTblEntry *rte = TranslateDXLTblDescrToRangeTblEntry(
		physical_idx_scan_dxlop->GetDXLTableDescr(), index,
		&base_table_context);
	GPOS_ASSERT(NULL != rte);
	rte->requiredPerms |= ACL_SELECT;
	m_dxl_to_plstmt_context->AddRTE(rte);

	IndexScan *index_scan = NULL;
	index_scan = MakeNode(IndexScan);
	index_scan->scan.scanrelid = index;
	index_scan->scan.plan.px_scan_partial = true;

	CMDIdGPDB *mdid_index = CMDIdGPDB::CastMdid(
		physical_idx_scan_dxlop->GetDXLIndexDescr()->MDId());
	const IMDIndex *md_index = m_md_accessor->RetrieveIndex(mdid_index);
	Oid index_oid = mdid_index->Oid();

	GPOS_ASSERT(InvalidOid != index_oid);
	// Lock any index we are to scan, since it may not have been properly locked
	// by the parser (e.g in case of generated scans for partitioned indexes)
	px::GPDBLockRelationOid(index_oid, dxl_table_descr->LockMode());
	index_scan->indexid = index_oid;

	Plan *plan = &(index_scan->scan.plan);
	plan->plan_node_id = m_dxl_to_plstmt_context->GetNextPlanId();

	// translate operator costs
	TranslatePlanCosts(index_scan_dxlnode, plan);

	// an index scan node must have 3 children: projection list, filter and index condition list
	GPOS_ASSERT(3 == index_scan_dxlnode->Arity());

	// translate proj list and filter
	CDXLNode *project_list_dxlnode = (*index_scan_dxlnode)[EdxlisIndexProjList];
	CDXLNode *filter_dxlnode = (*index_scan_dxlnode)[EdxlisIndexFilter];
	CDXLNode *index_cond_list_dxlnode =
		(*index_scan_dxlnode)[EdxlisIndexCondition];

	// translate proj list
	plan->targetlist =
		TranslateDXLProjList(project_list_dxlnode, &base_table_context,
							 NULL /*child_contexts*/, output_context);

	// translate index filter
	plan->qual = TranslateDXLIndexFilter(filter_dxlnode, output_context,
										 &base_table_context,
										 ctxt_translation_prev_siblings);

	index_scan->indexorderdir = CTranslatorUtils::GetScanDirection(
		physical_idx_scan_dxlop->GetIndexScanDir());

	// translate index condition list
	List *index_cond = NIL;
	List *index_orig_cond = NIL;
	List *index_strategy_list = NIL;
	List *index_subtype_list = NIL;

	TranslateIndexConditions(
		index_cond_list_dxlnode, physical_idx_scan_dxlop->GetDXLTableDescr(),
		false,	// is_bitmap_index_probe
		md_index, md_rel, output_context, &base_table_context,
		ctxt_translation_prev_siblings, &index_cond, &index_orig_cond,
		&index_strategy_list, &index_subtype_list);

	index_scan->indexqual = index_cond;
	index_scan->indexqualorig = index_orig_cond;

	/*
	 * As of 8.4, the indexstrategy and indexsubtype fields are no longer
	 * available or needed in IndexScan. Ignore them.
	 */
	SetParamIds(plan);

	return (Plan *) index_scan;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToPlStmt::TranslateDXLShareIndexScan
//
//	@doc:
//		Translates a DXL index scan node into a ShareIndexScan node
//
//---------------------------------------------------------------------------
Plan *
CTranslatorDXLToPlStmt::TranslateDXLShareIndexScan(
	const CDXLNode *index_scan_dxlnode, CDXLTranslateContext *output_context,
	CDXLTranslationContextArray *ctxt_translation_prev_siblings)
{
	// translate table descriptor into a range table entry
	CDXLPhysicalShareIndexScan *physical_idx_scan_dxlop =
		CDXLPhysicalShareIndexScan::Cast(index_scan_dxlnode->GetOperator());

	return TranslateDXLShareIndexScan(index_scan_dxlnode, physical_idx_scan_dxlop,
								 output_context,
								 ctxt_translation_prev_siblings);
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToPlStmt::TranslateDXLShareIndexScan
//
//	@doc:
//		Translates a DXL index scan node into a ShareIndexScan node
//
//---------------------------------------------------------------------------
Plan *
CTranslatorDXLToPlStmt::TranslateDXLShareIndexScan(
	const CDXLNode *index_scan_dxlnode,
	CDXLPhysicalShareIndexScan *physical_idx_scan_dxlop,
	CDXLTranslateContext *output_context,
	CDXLTranslationContextArray *ctxt_translation_prev_siblings)
{
	// translation context for column mappings in the base relation
	CDXLTranslateContextBaseTable base_table_context(m_mp);

	Index index =
		px::ListLength(m_dxl_to_plstmt_context->GetRTableEntriesList()) + 1;

	const CDXLTableDescr *dxl_table_descr =
		physical_idx_scan_dxlop->GetDXLTableDescr();
	const IMDRelation *md_rel =
		m_md_accessor->RetrieveRel(dxl_table_descr->MDId());

	// Lock any table we are to scan, since it may not have been properly locked
	// by the parser (e.g in case of generated scans for partitioned tables)
	CMDIdGPDB *mdid = CMDIdGPDB::CastMdid(md_rel->MDId());
	GPOS_RTL_ASSERT(dxl_table_descr->LockMode() != -1);
	px::GPDBLockRelationOid(mdid->Oid(), dxl_table_descr->LockMode());

	RangeTblEntry *rte = TranslateDXLTblDescrToRangeTblEntry(
		physical_idx_scan_dxlop->GetDXLTableDescr(), index,
		&base_table_context);
	GPOS_ASSERT(NULL != rte);
	rte->requiredPerms |= ACL_SELECT;
	m_dxl_to_plstmt_context->AddRTE(rte);

	IndexScan *index_scan = NULL;
	index_scan = MakeNode(IndexScan);
	index_scan->scan.scanrelid = index;

	CMDIdGPDB *mdid_index = CMDIdGPDB::CastMdid(
		physical_idx_scan_dxlop->GetDXLIndexDescr()->MDId());
	const IMDIndex *md_index = m_md_accessor->RetrieveIndex(mdid_index);
	Oid index_oid = mdid_index->Oid();

	GPOS_ASSERT(InvalidOid != index_oid);
	// Lock any index we are to scan, since it may not have been properly locked
	// by the parser (e.g in case of generated scans for partitioned indexes)
	px::GPDBLockRelationOid(index_oid, dxl_table_descr->LockMode());
	index_scan->indexid = index_oid;

	Plan *plan = &(index_scan->scan.plan);
	plan->plan_node_id = m_dxl_to_plstmt_context->GetNextPlanId();

	// translate operator costs
	TranslatePlanCosts(index_scan_dxlnode, plan);

	// an index scan node must have 3 children: projection list, filter and index condition list
	GPOS_ASSERT(3 == index_scan_dxlnode->Arity());

	// translate proj list and filter
	CDXLNode *project_list_dxlnode = (*index_scan_dxlnode)[EdxlisIndexProjList];
	CDXLNode *filter_dxlnode = (*index_scan_dxlnode)[EdxlisIndexFilter];
	CDXLNode *index_cond_list_dxlnode =
		(*index_scan_dxlnode)[EdxlisIndexCondition];

	// translate proj list
	plan->targetlist =
		TranslateDXLProjList(project_list_dxlnode, &base_table_context,
							 NULL /*child_contexts*/, output_context);

	// translate index filter
	plan->qual = TranslateDXLIndexFilter(filter_dxlnode, output_context,
										 &base_table_context,
										 ctxt_translation_prev_siblings);

	index_scan->indexorderdir = CTranslatorUtils::GetScanDirection(
		physical_idx_scan_dxlop->GetIndexScanDir());

	// translate index condition list
	List *index_cond = NIL;
	List *index_orig_cond = NIL;
	List *index_strategy_list = NIL;
	List *index_subtype_list = NIL;

	TranslateIndexConditions(
		index_cond_list_dxlnode, physical_idx_scan_dxlop->GetDXLTableDescr(),
		false,	// is_bitmap_index_probe
		md_index, md_rel, output_context, &base_table_context,
		ctxt_translation_prev_siblings, &index_cond, &index_orig_cond,
		&index_strategy_list, &index_subtype_list);

	index_scan->indexqual = index_cond;
	index_scan->indexqualorig = index_orig_cond;

	/*
	 * As of 8.4, the indexstrategy and indexsubtype fields are no longer
	 * available or needed in ShareIndexScan. Ignore them.
	 */
	SetParamIds(plan);

	return (Plan *) index_scan;
}


/*no cover begin*/
// Index Only Scan is not support by orca now
static List *
TranslateDXLIndexTList(const IMDRelation *md_rel, const IMDIndex *md_index,
					   Index new_varno, const CDXLTableDescr *table_descr,
					   CDXLTranslateContextBaseTable *index_context)
{
	List *target_list = NIL;

	index_context->SetRelIndex(INDEX_VAR);

	for (ULONG ul = 0; ul < md_index->Keys(); ul++)
	{
		ULONG key = md_index->KeyAt(ul);

		const IMDColumn *col = md_rel->GetMdCol(key);

		TargetEntry *target_entry = MakeNode(TargetEntry);
		target_entry->resno = (AttrNumber) ul + 1;

		Expr *indexvar = (Expr *) px::MakeVar(
			new_varno, col->AttrNum(),
			CMDIdGPDB::CastMdid(col->MdidType())->Oid(),
			col->TypeModifier() /*vartypmod*/, 0 /*varlevelsup*/);
		target_entry->expr = indexvar;

		// Fix up proj list. Since index only scan does not read full tuples,
		// the var->varattno must be updated as it should no longer point to
		// column in the table, but rather a column in the index. We achieve
		// this by mapping col id to a new varattno based on index columns.
		for (ULONG j = 0; j < table_descr->Arity(); j++)
		{
			const CDXLColDescr *dxl_col_descr =
				table_descr->GetColumnDescrAt(j);
			if (dxl_col_descr->AttrNum() == ((Var *) indexvar)->varattno)
			{
				(void) index_context->InsertMapping(dxl_col_descr->Id(),
													ul + 1);
				break;
			}
		}

		target_list = px::LAppend(target_list, target_entry);
	}

	return target_list;
}

Plan *
CTranslatorDXLToPlStmt::TranslateDXLIndexOnlyScan(
	const CDXLNode *index_scan_dxlnode, CDXLTranslateContext *output_context,
	CDXLTranslationContextArray *ctxt_translation_prev_siblings)
{
	// translate table descriptor into a range table entry
	CDXLPhysicalIndexOnlyScan *physical_idx_scan_dxlop =
		CDXLPhysicalIndexOnlyScan::Cast(index_scan_dxlnode->GetOperator());
	const CDXLTableDescr *table_desc =
		physical_idx_scan_dxlop->GetDXLTableDescr();

	// translation context for column mappings in the base relation
	CDXLTranslateContextBaseTable base_table_context(m_mp);

	Index index =
		px::ListLength(m_dxl_to_plstmt_context->GetRTableEntriesList()) + 1;

	const IMDRelation *md_rel = m_md_accessor->RetrieveRel(
		physical_idx_scan_dxlop->GetDXLTableDescr()->MDId());

	RangeTblEntry *rte = TranslateDXLTblDescrToRangeTblEntry(
		physical_idx_scan_dxlop->GetDXLTableDescr(), index,
		&base_table_context);
	GPOS_ASSERT(NULL != rte);
	rte->requiredPerms |= ACL_SELECT;
	m_dxl_to_plstmt_context->AddRTE(rte);

	IndexOnlyScan *index_scan = MakeNode(IndexOnlyScan);
	index_scan->scan.scanrelid = index;
	index_scan->scan.plan.px_scan_partial = true;

	CMDIdGPDB *mdid_index = CMDIdGPDB::CastMdid(
		physical_idx_scan_dxlop->GetDXLIndexDescr()->MDId());
	const IMDIndex *md_index = m_md_accessor->RetrieveIndex(mdid_index);
	Oid index_oid = mdid_index->Oid();

	GPOS_ASSERT(InvalidOid != index_oid);
	index_scan->indexid = index_oid;

	Plan *plan = &(index_scan->scan.plan);
	plan->plan_node_id = m_dxl_to_plstmt_context->GetNextPlanId();

	// translate operator costs
	TranslatePlanCosts(index_scan_dxlnode, plan);

	// an index scan node must have 3 children: projection list, filter and index condition list
	GPOS_ASSERT(3 == index_scan_dxlnode->Arity());

	// translate proj list and filter
	CDXLNode *project_list_dxlnode = (*index_scan_dxlnode)[EdxlisIndexProjList];
	CDXLNode *filter_dxlnode = (*index_scan_dxlnode)[EdxlisIndexFilter];
	CDXLNode *index_cond_list_dxlnode =
		(*index_scan_dxlnode)[EdxlisIndexCondition];

	CDXLTranslateContextBaseTable index_context(m_mp);

	// translate index targetlist
	index_scan->indextlist = TranslateDXLIndexTList(md_rel, md_index, index,
													table_desc, &index_context);

	// translate target list
	plan->targetlist =
		TranslateDXLProjList(project_list_dxlnode, &index_context,
							 NULL /*child_contexts*/, output_context);

	// translate index filter
	plan->qual =
		TranslateDXLIndexFilter(filter_dxlnode, output_context, &index_context,
								ctxt_translation_prev_siblings);

	index_scan->indexorderdir = CTranslatorUtils::GetScanDirection(
		physical_idx_scan_dxlop->GetIndexScanDir());

	// translate index condition list
	List *index_cond = NIL;
	List *index_orig_cond = NIL;
	List *index_strategy_list = NIL;
	List *index_subtype_list = NIL;

	TranslateIndexConditions(
		index_cond_list_dxlnode, physical_idx_scan_dxlop->GetDXLTableDescr(),
		false,	// is_bitmap_index_probe
		md_index, md_rel, output_context, &base_table_context,
		ctxt_translation_prev_siblings, &index_cond, &index_orig_cond,
		&index_strategy_list, &index_subtype_list);

	index_scan->indexqual = index_cond;
	// index_scan->indexqualorig = index_orig_cond;
	SetParamIds(plan);

	return (Plan *) index_scan;
}
/*no cover end*/

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToPlStmt::TranslateIndexFilter
//
//	@doc:
//		Translate the index filter list in an Index scan
//
//---------------------------------------------------------------------------
List *
CTranslatorDXLToPlStmt::TranslateDXLIndexFilter(
	CDXLNode *filter_dxlnode, CDXLTranslateContext *output_context,
	CDXLTranslateContextBaseTable *base_table_context,
	CDXLTranslationContextArray *ctxt_translation_prev_siblings)
{
	List *quals_list = NIL;

	// build colid->var mapping
	CMappingColIdVarPlStmt colid_var_mapping(
		m_mp, base_table_context, ctxt_translation_prev_siblings,
		output_context, m_dxl_to_plstmt_context);

	const ULONG arity = filter_dxlnode->Arity();
	for (ULONG ul = 0; ul < arity; ul++)
	{
		CDXLNode *index_filter_dxlnode = (*filter_dxlnode)[ul];
		Expr *index_filter_expr =
			m_translator_dxl_to_scalar->TranslateDXLToScalar(
				index_filter_dxlnode, &colid_var_mapping);
		quals_list = px::LAppend(quals_list, index_filter_expr);
	}

	return quals_list;
}


//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToPlStmt::TranslateIndexConditions
//
//	@doc:
//		Translate the index condition list in an Index scan
//
//---------------------------------------------------------------------------
void
CTranslatorDXLToPlStmt::TranslateIndexConditions(
	CDXLNode *index_cond_list_dxlnode, const CDXLTableDescr *dxl_tbl_descr,
	BOOL is_bitmap_index_probe, const IMDIndex *index,
	const IMDRelation *md_rel, CDXLTranslateContext *output_context,
	CDXLTranslateContextBaseTable *base_table_context,
	CDXLTranslationContextArray *ctxt_translation_prev_siblings,
	List **index_cond, List **index_orig_cond, List **index_strategy_list,
	List **index_subtype_list)
{
	// array of index qual info
	CIndexQualInfoArray *index_qual_info_array =
		GPOS_NEW(m_mp) CIndexQualInfoArray(m_mp);

	// build colid->var mapping
	CMappingColIdVarPlStmt colid_var_mapping(
		m_mp, base_table_context, ctxt_translation_prev_siblings,
		output_context, m_dxl_to_plstmt_context);

	const ULONG arity = index_cond_list_dxlnode->Arity();
	for (ULONG ul = 0; ul < arity; ul++)
	{
		CDXLNode *index_cond_dxlnode = (*index_cond_list_dxlnode)[ul];

		Expr *original_index_cond_expr =
			m_translator_dxl_to_scalar->TranslateDXLToScalar(
				index_cond_dxlnode, &colid_var_mapping);
		Expr *index_cond_expr =
			m_translator_dxl_to_scalar->TranslateDXLToScalar(
				index_cond_dxlnode, &colid_var_mapping);
		GPOS_ASSERT((IsA(index_cond_expr, OpExpr) ||
					 IsA(index_cond_expr, ScalarArrayOpExpr)) &&
					"expected OpExpr or ScalarArrayOpExpr in index qual");

		if (!is_bitmap_index_probe && IsA(index_cond_expr, ScalarArrayOpExpr) &&
			IMDIndex::EmdindBitmap != index->IndexType())
		{
			GPOS_RAISE(
				gpdxl::ExmaDXL, gpdxl::ExmiDXL2PlStmtConversion,
				GPOS_WSZ_LIT("ScalarArrayOpExpr condition on index scan"));
		}

		// We need to perform mapping of Varattnos relative to column positions in index keys
		SContextIndexVarAttno index_varattno_ctxt(md_rel, index);
		SetIndexVarAttnoWalker((Node *) index_cond_expr, &index_varattno_ctxt);

		// find index key's attno
		List *args_list = NULL;
		if (IsA(index_cond_expr, OpExpr))
		{
			args_list = ((OpExpr *) index_cond_expr)->args;
		}
		else
		{
			args_list = ((ScalarArrayOpExpr *) index_cond_expr)->args;
		}

		Node *left_arg = (Node *) lfirst(px::ListHead(args_list));
		Node *right_arg = (Node *) lfirst(px::ListTail(args_list));

		BOOL is_relabel_type = false;
		if (IsA(left_arg, RelabelType) &&
			IsA(((RelabelType *) left_arg)->arg, Var))
		{
			left_arg = (Node *) ((RelabelType *) left_arg)->arg;
			is_relabel_type = true;
		}
		else if (IsA(right_arg, RelabelType) &&
				 IsA(((RelabelType *) right_arg)->arg, Var))
		{
			right_arg = (Node *) ((RelabelType *) right_arg)->arg;
			is_relabel_type = true;
		}

		if (is_relabel_type)
		{
			List *new_args_list = ListMake2(left_arg, right_arg);
			px::GPDBFree(args_list);
			if (IsA(index_cond_expr, OpExpr))
			{
				((OpExpr *) index_cond_expr)->args = new_args_list;
			}
			else
			{
				((ScalarArrayOpExpr *) index_cond_expr)->args = new_args_list;
			}
		}

		GPOS_ASSERT((IsA(left_arg, Var) || IsA(right_arg, Var)) &&
					"expected index key in index qual");

		INT attno = 0;
		if (IsA(left_arg, Var) && ((Var *) left_arg)->varno != OUTER_VAR)
		{
			// index key is on the left side
			attno = ((Var *) left_arg)->varattno;
			// GPDB_92_MERGE_FIXME: helluva hack
			// Upstream commit a0185461 cleaned up how the varno of indices
			// We are patching up varno here, but it seems this really should
			// happen in CTranslatorDXLToScalar::PexprFromDXLNodeScalar .
			// Furthermore, should we guard against nonsensical varno?
			((Var *) left_arg)->varno = INDEX_VAR;
		}
		else
		{
			// index key is on the right side
			GPOS_ASSERT(((Var *) right_arg)->varno != OUTER_VAR &&
						"unexpected outer reference in index qual");
			attno = ((Var *) right_arg)->varattno;
		}

		// retrieve index strategy and subtype
		INT strategy_num = 0;
		OID index_subtype_oid = InvalidOid;

		OID cmp_operator_oid =
			CTranslatorUtils::OidCmpOperator(index_cond_expr);
		GPOS_ASSERT(InvalidOid != cmp_operator_oid);
		OID op_family_oid = CTranslatorUtils::GetOpFamilyForIndexQual(
			attno, CMDIdGPDB::CastMdid(index->MDId())->Oid());
		GPOS_ASSERT(InvalidOid != op_family_oid);
		px::IndexOpProperties(cmp_operator_oid, op_family_oid, &strategy_num,
								&index_subtype_oid);

		// create index qual
		index_qual_info_array->Append(GPOS_NEW(m_mp) CIndexQualInfo(
			attno, index_cond_expr, original_index_cond_expr,
			(StrategyNumber) strategy_num, index_subtype_oid));
	}

	// the index quals much be ordered by attribute number
	index_qual_info_array->Sort(CIndexQualInfo::IndexQualInfoCmp);

	ULONG length = index_qual_info_array->Size();
	for (ULONG ul = 0; ul < length; ul++)
	{
		CIndexQualInfo *index_qual_info = (*index_qual_info_array)[ul];
		*index_cond = px::LAppend(*index_cond, index_qual_info->m_expr);
		*index_orig_cond =
			px::LAppend(*index_orig_cond, index_qual_info->m_original_expr);
		*index_strategy_list = px::LAppendInt(
			*index_strategy_list, index_qual_info->m_index_subtype_oid);
		*index_subtype_list = px::LAppendOid(
			*index_subtype_list, index_qual_info->m_index_subtype_oid);
	}

	// clean up
	index_qual_info_array->Release();
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToPlStmt::TranslateDXLLimit
//
//	@doc:
//		Translates a DXL Limit node into a Limit node
//
//---------------------------------------------------------------------------
Plan *
CTranslatorDXLToPlStmt::TranslateDXLLimit(
	const CDXLNode *limit_dxlnode, CDXLTranslateContext *output_context,
	CDXLTranslationContextArray *ctxt_translation_prev_siblings)
{
	// create limit node
	Limit *limit = MakeNode(Limit);

	Plan *plan = &(limit->plan);
	plan->plan_node_id = m_dxl_to_plstmt_context->GetNextPlanId();

	// translate operator costs
	TranslatePlanCosts(limit_dxlnode, plan);

	GPOS_ASSERT(4 == limit_dxlnode->Arity());

	CDXLTranslateContext left_dxl_translate_ctxt(
		m_mp, false, output_context->GetColIdToParamIdMap());

	// translate proj list
	CDXLNode *project_list_dxlnode = (*limit_dxlnode)[EdxllimitIndexProjList];
	CDXLNode *child_plan_dxlnode = (*limit_dxlnode)[EdxllimitIndexChildPlan];
	CDXLNode *limit_count_dxlnode = (*limit_dxlnode)[EdxllimitIndexLimitCount];
	CDXLNode *limit_offset_dxlnode =
		(*limit_dxlnode)[EdxllimitIndexLimitOffset];

	// NOTE: Limit node has only the left plan while the right plan is left empty
	Plan *left_plan =
		TranslateDXLOperatorToPlan(child_plan_dxlnode, &left_dxl_translate_ctxt,
								   ctxt_translation_prev_siblings);

	CDXLTranslationContextArray *child_contexts =
		GPOS_NEW(m_mp) CDXLTranslationContextArray(m_mp);
	child_contexts->Append(&left_dxl_translate_ctxt);

	plan->targetlist =
		TranslateDXLProjList(project_list_dxlnode,
							 NULL,	// base table translation context
							 child_contexts, output_context);

	plan->lefttree = left_plan;

	if (NULL != limit_count_dxlnode && limit_count_dxlnode->Arity() > 0)
	{
		CMappingColIdVarPlStmt colid_var_mapping(m_mp, NULL, child_contexts,
												 output_context,
												 m_dxl_to_plstmt_context);
		Node *limit_count =
			(Node *) m_translator_dxl_to_scalar->TranslateDXLToScalar(
				(*limit_count_dxlnode)[0], &colid_var_mapping);
		limit->limitCount = limit_count;
	}

	if (NULL != limit_offset_dxlnode && limit_offset_dxlnode->Arity() > 0)
	{
		CMappingColIdVarPlStmt colid_var_mapping =
			CMappingColIdVarPlStmt(m_mp, NULL, child_contexts, output_context,
								   m_dxl_to_plstmt_context);
		Node *limit_offset =
			(Node *) m_translator_dxl_to_scalar->TranslateDXLToScalar(
				(*limit_offset_dxlnode)[0], &colid_var_mapping);
		limit->limitOffset = limit_offset;
	}

	SetParamIds(plan);

	// cleanup
	child_contexts->Release();

	return (Plan *) limit;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToPlStmt::TranslateDXLHashJoin
//
//	@doc:
//		Translates a DXL hash join node into a HashJoin node
//
//---------------------------------------------------------------------------
Plan *
CTranslatorDXLToPlStmt::TranslateDXLHashJoin(
	const CDXLNode *hj_dxlnode, CDXLTranslateContext *output_context,
	CDXLTranslationContextArray *ctxt_translation_prev_siblings)
{
	GPOS_ASSERT(hj_dxlnode->GetOperator()->GetDXLOperator() ==
				EdxlopPhysicalHashJoin);
	GPOS_ASSERT(hj_dxlnode->Arity() == EdxlhjIndexSentinel);

	// create hash join node
	HashJoin *hashjoin = MakeNode(HashJoin);

	Join *join = &(hashjoin->join);
	Plan *plan = &(join->plan);
	plan->plan_node_id = m_dxl_to_plstmt_context->GetNextPlanId();

	CDXLPhysicalHashJoin *hashjoin_dxlop =
		CDXLPhysicalHashJoin::Cast(hj_dxlnode->GetOperator());

	// set join type
	join->jointype =
		GetGPDBJoinTypeFromDXLJoinType(hashjoin_dxlop->GetJoinType());
	join->prefetch_inner = px_enable_join_prefetch_inner;

	// translate operator costs
	TranslatePlanCosts(hj_dxlnode, plan);

	// translate join children
	CDXLNode *left_tree_dxlnode = (*hj_dxlnode)[EdxlhjIndexHashLeft];
	CDXLNode *right_tree_dxlnode = (*hj_dxlnode)[EdxlhjIndexHashRight];
	CDXLNode *project_list_dxlnode = (*hj_dxlnode)[EdxlhjIndexProjList];
	CDXLNode *filter_dxlnode = (*hj_dxlnode)[EdxlhjIndexFilter];
	CDXLNode *join_filter_dxlnode = (*hj_dxlnode)[EdxlhjIndexJoinFilter];
	CDXLNode *hash_cond_list_dxlnode = (*hj_dxlnode)[EdxlhjIndexHashCondList];

	CDXLTranslateContext left_dxl_translate_ctxt(
		m_mp, false, output_context->GetColIdToParamIdMap());
	CDXLTranslateContext right_dxl_translate_ctxt(
		m_mp, false, output_context->GetColIdToParamIdMap());

	Plan *left_plan =
		TranslateDXLOperatorToPlan(left_tree_dxlnode, &left_dxl_translate_ctxt,
								   ctxt_translation_prev_siblings);

	// the right side of the join is the one where the hash phase is done
	CDXLTranslationContextArray *translation_context_arr_with_siblings =
		GPOS_NEW(m_mp) CDXLTranslationContextArray(m_mp);
	translation_context_arr_with_siblings->Append(&left_dxl_translate_ctxt);
	translation_context_arr_with_siblings->AppendArray(
		ctxt_translation_prev_siblings);
	Plan *right_plan =
		(Plan *) TranslateDXLHash(right_tree_dxlnode, &right_dxl_translate_ctxt,
								  translation_context_arr_with_siblings);

	CDXLTranslationContextArray *child_contexts =
		GPOS_NEW(m_mp) CDXLTranslationContextArray(m_mp);
	child_contexts->Append(
		const_cast<CDXLTranslateContext *>(&left_dxl_translate_ctxt));
	child_contexts->Append(
		const_cast<CDXLTranslateContext *>(&right_dxl_translate_ctxt));
	// translate proj list and filter
	TranslateProjListAndFilter(project_list_dxlnode, filter_dxlnode,
							   NULL,  // translate context for the base table
							   child_contexts, &plan->targetlist, &plan->qual,
							   output_context);

	// translate join filter
	join->joinqual =
		TranslateDXLFilterToQual(join_filter_dxlnode,
								 NULL,	// translate context for the base table
								 child_contexts, output_context);

	// translate hash cond
	List *hash_conditions_list = NIL;

	BOOL has_is_not_distinct_from_cond = false;

	const ULONG arity = hash_cond_list_dxlnode->Arity();
	for (ULONG ul = 0; ul < arity; ul++)
	{
		CDXLNode *hash_cond_dxlnode = (*hash_cond_list_dxlnode)[ul];

		List *hash_cond_list =
			TranslateDXLScCondToQual(hash_cond_dxlnode,
									 NULL,	// base table translation context
									 child_contexts, output_context);

		GPOS_ASSERT(1 == px::ListLength(hash_cond_list));

		Expr *expr = (Expr *) LInitial(hash_cond_list);
		if (IsA(expr, BoolExpr) && ((BoolExpr *) expr)->boolop == NOT_EXPR)
		{
			// INDF test
			GPOS_ASSERT(px::ListLength(((BoolExpr *) expr)->args) == 1 &&
						(IsA((Expr *) LInitial(((BoolExpr *) expr)->args),
							 DistinctExpr)));
			has_is_not_distinct_from_cond = true;
		}
		hash_conditions_list =
			px::ListConcat(hash_conditions_list, hash_cond_list);
	}

	if (!has_is_not_distinct_from_cond)
	{
		// no INDF conditions in the hash condition list
		hashjoin->hashclauses = hash_conditions_list;
	}
	else
	{
		// hash conditions contain INDF clauses -> extract equality conditions to
		// construct the hash clauses list
		List *hash_clauses_list = NIL;

		for (ULONG ul = 0; ul < arity; ul++)
		{
			CDXLNode *hash_cond_dxlnode = (*hash_cond_list_dxlnode)[ul];

			// condition can be either a scalar comparison or a NOT DISTINCT FROM expression
			GPOS_ASSERT(
				EdxlopScalarCmp ==
					hash_cond_dxlnode->GetOperator()->GetDXLOperator() ||
				EdxlopScalarBoolExpr ==
					hash_cond_dxlnode->GetOperator()->GetDXLOperator());

			if (EdxlopScalarBoolExpr ==
				hash_cond_dxlnode->GetOperator()->GetDXLOperator())
			{
				// clause is a NOT DISTINCT FROM check -> extract the distinct comparison node
				GPOS_ASSERT(Edxlnot == CDXLScalarBoolExpr::Cast(
										   hash_cond_dxlnode->GetOperator())
										   ->GetDxlBoolTypeStr());
				hash_cond_dxlnode = (*hash_cond_dxlnode)[0];
				GPOS_ASSERT(EdxlopScalarDistinct ==
							hash_cond_dxlnode->GetOperator()->GetDXLOperator());
			}

			CMappingColIdVarPlStmt colid_var_mapping =
				CMappingColIdVarPlStmt(m_mp, NULL, child_contexts,
									   output_context, m_dxl_to_plstmt_context);

			// translate the DXL scalar or scalar distinct comparison into an equality comparison
			// to store in the hash clauses
			Expr *hash_clause_expr =
				(Expr *)
					m_translator_dxl_to_scalar->TranslateDXLScalarCmpToScalar(
						hash_cond_dxlnode, &colid_var_mapping);

			hash_clauses_list =
				px::LAppend(hash_clauses_list, hash_clause_expr);
		}

		hashjoin->hashclauses = hash_clauses_list;
		hashjoin->hashqualclauses = hash_conditions_list;
	}

	GPOS_ASSERT(NIL != hashjoin->hashclauses);

	plan->lefttree = left_plan;
	plan->righttree = right_plan;
	SetParamIds(plan);

	// cleanup
	translation_context_arr_with_siblings->Release();
	child_contexts->Release();

	return (Plan *) hashjoin;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToPlStmt::TranslateDXLTvf
//
//	@doc:
//		Translates a DXL TVF node into a GPDB Function scan node
//
//---------------------------------------------------------------------------
Plan *
CTranslatorDXLToPlStmt::TranslateDXLTvf(
	const CDXLNode *tvf_dxlnode, CDXLTranslateContext *output_context,
	CDXLTranslationContextArray *ctxt_translation_prev_siblings)
{
	// translation context for column mappings
	CDXLTranslateContextBaseTable base_table_context(m_mp);

	// create function scan node
	FunctionScan *func_scan = MakeNode(FunctionScan);
	Plan *plan = &(func_scan->scan.plan);

	RangeTblEntry *rte = TranslateDXLTvfToRangeTblEntry(
		tvf_dxlnode, output_context, &base_table_context);
	GPOS_ASSERT(rte != nullptr);
	GPOS_ASSERT(list_length(rte->functions) == 1);
	RangeTblFunction *rtfunc =
		(RangeTblFunction *) px::CopyObject(linitial(rte->functions));

	// we will add the new range table entry as the last element of the range table
	Index index =
		px::ListLength(m_dxl_to_plstmt_context->GetRTableEntriesList()) + 1;
	base_table_context.SetRelIndex(index);
	func_scan->scan.scanrelid = index;

	m_dxl_to_plstmt_context->AddRTE(rte);

	plan->plan_node_id = m_dxl_to_plstmt_context->GetNextPlanId();

	// translate operator costs
	TranslatePlanCosts(tvf_dxlnode, plan);

	// a table scan node must have at least 1 child: projection list
	GPOS_ASSERT(1 <= tvf_dxlnode->Arity());

	CDXLNode *project_list_dxlnode = (*tvf_dxlnode)[EdxltsIndexProjList];

	// translate proj list
	List *target_list = TranslateDXLProjList(
		project_list_dxlnode, &base_table_context, nullptr, output_context);

	plan->targetlist = target_list;

	ListCell *lc_target_entry = nullptr;

	rtfunc->funccolnames = NIL;
	rtfunc->funccoltypes = NIL;
	rtfunc->funccoltypmods = NIL;
	rtfunc->funccolcollations = NIL;
	ForEach(lc_target_entry, target_list)
	{
		TargetEntry *target_entry = (TargetEntry *) lfirst(lc_target_entry);
		OID oid_type = px::ExprType((Node *) target_entry->expr);
		GPOS_ASSERT(InvalidOid != oid_type);

		INT typ_mod = px::ExprTypeMod((Node *) target_entry->expr);
		Oid collation_type_oid = px::TypeCollation(oid_type);

		rtfunc->funccolnames = px::LAppend(
			rtfunc->funccolnames, px::MakeStringValue(target_entry->resname));
		rtfunc->funccoltypes = px::LAppendOid(rtfunc->funccoltypes, oid_type);
		rtfunc->funccoltypmods =
			px::LAppendInt(rtfunc->funccoltypmods, typ_mod);
		// GPDB_91_MERGE_FIXME: collation
		rtfunc->funccolcollations =
			px::LAppendOid(rtfunc->funccolcollations, collation_type_oid);
	}
	func_scan->functions = ListMake1(rtfunc);

	SetParamIds(plan);

	return (Plan *) func_scan;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToPlStmt::CreateTargetListWithNullsForDroppedCols
//
//	@doc:
//		Construct the target list for a DML statement by adding NULL elements
//		for dropped columns
//
//---------------------------------------------------------------------------
List *
CTranslatorDXLToPlStmt::CreateTargetListWithNullsForDroppedCols(
	List *target_list, const IMDRelation *md_rel)
{
	GPOS_ASSERT(nullptr != target_list);
	GPOS_ASSERT(px::ListLength(target_list) <= md_rel->ColumnCount());

	List *result_list = NIL;
	ULONG last_tgt_elem = 0;
	ULONG resno = 1;

	const ULONG num_of_rel_cols = md_rel->ColumnCount();

	for (ULONG ul = 0; ul < num_of_rel_cols; ul++)
	{
		const IMDColumn *md_col = md_rel->GetMdCol(ul);

		if (md_col->IsSystemColumn())
		{
			continue;
		}

		Expr *expr = nullptr;
		if (md_col->IsDropped())
		{
			// add a NULL element
			OID oid_type = CMDIdGPDB::CastMdid(
							   m_md_accessor->PtMDType<IMDTypeInt4>()->MDId())
							   ->Oid();

			expr = (Expr *) px::MakeNULLConst(oid_type);
		}
		else
		{
			TargetEntry *target_entry =
				(TargetEntry *) px::ListNth(target_list, last_tgt_elem);
			expr = (Expr *) px::CopyObject(target_entry->expr);
			last_tgt_elem++;
		}

		CHAR *name_str =
			CTranslatorUtils::CreateMultiByteCharStringFromWCString(
				md_col->Mdname().GetMDName()->GetBuffer());
		TargetEntry *te_new =
			px::MakeTargetEntry(expr, resno, name_str, false /*resjunk*/);
		result_list = px::LAppend(result_list, te_new);
		resno++;
	}

	return result_list;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToPlStmt::TranslateDXLDml
//
//	@doc:
//		Translates a DXL DML node
//
//---------------------------------------------------------------------------
Plan *
CTranslatorDXLToPlStmt::TranslateDXLDml(
	const CDXLNode *dml_dxlnode, CDXLTranslateContext *output_context,
	CDXLTranslationContextArray *ctxt_translation_prev_siblings)
{
	// translate table descriptor into a range table entry
	CDXLPhysicalDML *phy_dml_dxlop =
		CDXLPhysicalDML::Cast(dml_dxlnode->GetOperator());

	ModifyTable *dml = MakeNode(ModifyTable);
	Plan *plan = &(dml->plan);
	AclMode acl_mode = ACL_NO_RIGHTS;

	switch (phy_dml_dxlop->GetDmlOpType())
	{
		case gpdxl::Edxldmlinsert:
		{
			m_cmd_type = CMD_INSERT;
			acl_mode = ACL_INSERT;
			break;
		}
		case gpdxl::Edxldmlupdate:
		{
			m_cmd_type = CMD_UPDATE;
			acl_mode = ACL_UPDATE;
			acl_mode |= ACL_SELECT;
			break;
		}
		case gpdxl::Edxldmldelete:
		{
			m_cmd_type = CMD_DELETE;
			acl_mode = ACL_DELETE;
			acl_mode |= ACL_SELECT;
			break;
		}
		case gpdxl::EdxldmlSentinel:
		default:
		{
			GPOS_RAISE(
				gpdxl::ExmaDXL, gpdxl::ExmiDXL2PlStmtConversion,
				GPOS_WSZ_LIT("Unexpected error during plan generation."));
			break;
		}
	}

	IMDId *mdid_target_table = phy_dml_dxlop->GetDXLTableDescr()->MDId();
	const IMDRelation *md_rel = m_md_accessor->RetrieveRel(mdid_target_table);
	/* PolarDB PX: insert into Partitioned Table */
	if (md_rel->IsPartitioned() && !px_enable_insert_partition_table) {
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiQuery2DXLUnsupportedFeature,
				   GPOS_WSZ_LIT("Close Insert Into Partition Table in config"));
	}

	if (IMDRelation::EreldistrMasterOnly != md_rel->GetRelDistribution())
	{
		m_is_tgt_tbl_distributed = true;
	}

	if (px::HasUpdateTriggers(CMDIdGPDB::CastMdid(mdid_target_table)->Oid()))
	{
		switch (m_cmd_type)
		{
			case CMD_INSERT:
			{
				GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiQuery2DXLUnsupportedFeature,
							GPOS_WSZ_LIT("INSERT on a table with UPDATE triggers"));
				break;
			}
			case CMD_UPDATE:
			{
				GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiQuery2DXLUnsupportedFeature,
							GPOS_WSZ_LIT("UPDATE on a table with UPDATE triggers"));
				break;				
			}
			case CMD_DELETE:
			{
				GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiQuery2DXLUnsupportedFeature,
							GPOS_WSZ_LIT("DELETE on a table with UPDATE triggers"));
				break;				
			}
			default:
			{
				GPOS_RAISE(
					gpdxl::ExmaDXL, gpdxl::ExmiDXL2PlStmtConversion,
					GPOS_WSZ_LIT("Unexpected error during plan generation."));
				break;
			}
		}
	}

	// translation context for column mappings in the base relation
	CDXLTranslateContextBaseTable base_table_context(m_mp);

	// add the new range table entry as the last element of the range table
	Index index =
		px::ListLength(m_dxl_to_plstmt_context->GetRTableEntriesList()) + 1;

	m_result_rel_list = px::LAppendInt(m_result_rel_list, index);

	CDXLTableDescr *table_descr = phy_dml_dxlop->GetDXLTableDescr();
	RangeTblEntry *rte = TranslateDXLTblDescrToRangeTblEntry(
		table_descr, index, &base_table_context);
	GPOS_ASSERT(nullptr != rte);
	rte->requiredPerms |= acl_mode;
	m_dxl_to_plstmt_context->AddRTE(rte, true);

	// check permission for UPDATE or DELETE
	if ((CMD_UPDATE == m_cmd_type || CMD_DELETE == m_cmd_type) &&
		!CTranslatorUtils::CheckRTEPermissionsWithRet(m_dxl_to_plstmt_context->GetRTLists()))
	{
		if (CMD_UPDATE == m_cmd_type)
				GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiQuery2DXLUnsupportedFeature,
							GPOS_WSZ_LIT("UPDATE on a table without SELECT or UPDATE permissions"));
		if (CMD_DELETE == m_cmd_type)
				GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiQuery2DXLUnsupportedFeature,
							GPOS_WSZ_LIT("DELETE on a table without SELECT or UPDATE permissions"));
	}

	CDXLNode *project_list_dxlnode = (*dml_dxlnode)[0];
	CDXLNode *child_dxlnode = (*dml_dxlnode)[1];

	CDXLTranslateContext child_context(m_mp, false,
									   output_context->GetColIdToParamIdMap());

	Plan *child_plan = TranslateDXLOperatorToPlan(
		child_dxlnode, &child_context, ctxt_translation_prev_siblings);

	CDXLTranslationContextArray *child_contexts =
		GPOS_NEW(m_mp) CDXLTranslationContextArray(m_mp);
	child_contexts->Append(&child_context);

	// translate proj list
	List *dml_target_list =
		TranslateDXLProjList(project_list_dxlnode,
							 nullptr,  // translate context for the base table
							 child_contexts, output_context);

	// pad child plan's target list with NULLs for dropped columns for all DML operator types
	List *target_list_with_dropped_cols =
		CreateTargetListWithNullsForDroppedCols(dml_target_list, md_rel);
	dml_target_list = target_list_with_dropped_cols;

	// Add junk columns to the target list for the 'action', 'ctid',
	// , and tuple's 'oid'. The ModifyTable node will find
	// these based on the resnames. ORCA also includes a similar column for
	// partition Oid in the child's target list, but we don't use it for
	// anything in GPDB.
	if (m_cmd_type == CMD_UPDATE)
		(void) AddJunkTargetEntryForColId(&dml_target_list, &child_context,
										  phy_dml_dxlop->ActionColId(),
										  "DMLAction");

	if (m_cmd_type == CMD_UPDATE || m_cmd_type == CMD_DELETE)
	{
		AddJunkTargetEntryForColId(&dml_target_list, &child_context,
								   phy_dml_dxlop->GetCtIdColId(), "ctid");
	}
	if (m_cmd_type == CMD_UPDATE && phy_dml_dxlop->IsOidsPreserved())
		AddJunkTargetEntryForColId(&dml_target_list, &child_context,
								   phy_dml_dxlop->GetTupleOid(), "oid");

	// Add a Result node on top of the child plan, to coerce the target
	// list to match the exact physical layout of the target table,
	// including dropped columns.  Often, the Result node isn't really
	// needed, as the child node could do the projection, but we don't have
	// the information to determine that here. There's a step in the
	// backend optimize_query() function to eliminate unnecessary Results
	// throught the plan, hopefully this Result gets eliminated there.
	Result *result = MakeNode(Result);
	Plan *result_plan = &(result->plan);

	result_plan->plan_node_id = m_dxl_to_plstmt_context->GetNextPlanId();
	result_plan->lefttree = child_plan;

	result_plan->targetlist = target_list_with_dropped_cols;
	SetParamIds(result_plan);

	child_plan = (Plan *) result;

	dml->operation = m_cmd_type;
	dml->canSetTag = true;	// FIXME
	dml->nominalRelation = index;
	dml->resultRelations = ListMake1Int(index);
	dml->resultRelIndex = list_length(m_result_rel_list) - 1;
	dml->plans = ListMake1(child_plan);

	dml->fdwPrivLists = ListMake1(NIL);
	/* PX OPT plans all updates as split updates */
	if (m_cmd_type == CMD_UPDATE)
		dml->isSplitUpdates = ListMake1Int((int) true);

	/* POLAR px */
	dml->rootResultRelIndex = -1;

	plan->targetlist = NIL;
	plan->plan_node_id = m_dxl_to_plstmt_context->GetNextPlanId();

	SetParamIds(plan);

	// Should be m_is_tgt_tbl_distributed
	if (m_is_tgt_tbl_distributed)
	{
		PlanSlice *current_slice = m_dxl_to_plstmt_context->GetCurrentSlice();
		if (CMD_UPDATE == m_cmd_type)
		{
			current_slice->numsegments = px_update_dop_num;
			current_slice->gangType = GANGTYPE_PRIMARY_WRITER;
		}
		else if (CMD_INSERT == m_cmd_type)
		{
			current_slice->numsegments = local_px_insert_dop_num;
			if (1 == local_px_insert_dop_num)
			{
				current_slice->gangType = GANGTYPE_UNALLOCATED;
			}
			else
			{
				current_slice->gangType = GANGTYPE_PRIMARY_WRITER;
			}
		}
		else
		{
			/* Delete */
			current_slice->numsegments = px_delete_dop_num;
			current_slice->gangType = GANGTYPE_PRIMARY_WRITER;
		}
	}

	// cleanup
	child_contexts->Release();

	// translate operator costs
	TranslatePlanCosts(dml_dxlnode, plan);

	return (Plan *) dml;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToPlStmt::AddJunkTargetEntryForColId
//
//	@doc:
//		Add a new target entry for the given colid to the given target list
//
//---------------------------------------------------------------------------
void
CTranslatorDXLToPlStmt::AddJunkTargetEntryForColId(
	List **target_list, CDXLTranslateContext *dxl_translate_ctxt, ULONG colid,
	const char *resname)
{
	GPOS_ASSERT(nullptr != target_list);

	const TargetEntry *target_entry = dxl_translate_ctxt->GetTargetEntry(colid);

	if (nullptr == target_entry)
	{
		// colid not found in translate context
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXL2PlStmtAttributeNotFound,
				   colid);
	}

	// TODO: Oct 29, 2012; see if entry already exists in the target list

	OID expr_oid = px::ExprType((Node *) target_entry->expr);
	INT type_modifier = px::ExprTypeMod((Node *) target_entry->expr);
	Var *var =
		px::MakeVar(OUTER_VAR, target_entry->resno, expr_oid, type_modifier,
					  0	 // varlevelsup
		);
	ULONG resno = px::ListLength(*target_list) + 1;
	CHAR *resname_str = PStrDup(resname);
	TargetEntry *te_new = px::MakeTargetEntry(
		(Expr *) var, resno, resname_str, true /* resjunk */);
	*target_list = px::LAppend(*target_list, te_new);
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToPlStmt::TranslateDXLTvfToRangeTblEntry
//
//	@doc:
//		Create a range table entry from a CDXLPhysicalTVF node
//
//---------------------------------------------------------------------------
RangeTblEntry *
CTranslatorDXLToPlStmt::TranslateDXLTvfToRangeTblEntry(
	const CDXLNode *tvf_dxlnode, CDXLTranslateContext *output_context,
	CDXLTranslateContextBaseTable *base_table_context)
{
	CDXLPhysicalTVF *dxlop = CDXLPhysicalTVF::Cast(tvf_dxlnode->GetOperator());

	RangeTblEntry *rte = MakeNode(RangeTblEntry);
	rte->rtekind = RTE_FUNCTION;

	FuncExpr *func_expr = MakeNode(FuncExpr);

	func_expr->funcid = CMDIdGPDB::CastMdid(dxlop->FuncMdId())->Oid();
	func_expr->funcretset = px::GetFuncRetset(func_expr->funcid);
	// this is a function call, as opposed to a cast
	func_expr->funcformat = COERCE_EXPLICIT_CALL;
	func_expr->funcresulttype =
		CMDIdGPDB::CastMdid(dxlop->ReturnTypeMdId())->Oid();

	// POLAR px
	func_expr->location = -1;

	Alias *alias = MakeNode(Alias);
	alias->colnames = NIL;

	// get function alias
	alias->aliasname = CTranslatorUtils::CreateMultiByteCharStringFromWCString(
		dxlop->Pstr()->GetBuffer());

	// project list
	CDXLNode *project_list_dxlnode = (*tvf_dxlnode)[EdxltsIndexProjList];

	// get column names
	const ULONG num_of_cols = project_list_dxlnode->Arity();
	for (ULONG ul = 0; ul < num_of_cols; ul++)
	{
		CDXLNode *proj_elem_dxlnode = (*project_list_dxlnode)[ul];
		CDXLScalarProjElem *dxl_proj_elem =
			CDXLScalarProjElem::Cast(proj_elem_dxlnode->GetOperator());

		CHAR *col_name_char_array =
			CTranslatorUtils::CreateMultiByteCharStringFromWCString(
				dxl_proj_elem->GetMdNameAlias()->GetMDName()->GetBuffer());

		Value *val_colname = px::MakeStringValue(col_name_char_array);
		alias->colnames = px::LAppend(alias->colnames, val_colname);

		// save mapping col id -> index in translate context
		(void) base_table_context->InsertMapping(dxl_proj_elem->Id(),
												 ul + 1 /*attno*/);
	}

	// function arguments
	const ULONG num_of_child = tvf_dxlnode->Arity();
	for (ULONG ul = 1; ul < num_of_child; ++ul)
	{
		CDXLNode *func_arg_dxlnode = (*tvf_dxlnode)[ul];

		CMappingColIdVarPlStmt colid_var_mapping(m_mp, base_table_context,
												 nullptr, output_context,
												 m_dxl_to_plstmt_context);

		Expr *pexprFuncArg = m_translator_dxl_to_scalar->TranslateDXLToScalar(
			func_arg_dxlnode, &colid_var_mapping);
		func_expr->args = px::LAppend(func_expr->args, pexprFuncArg);
	}

	// GPDB_91_MERGE_FIXME: collation
	func_expr->inputcollid = px::ExprCollation((Node *) func_expr->args);
	func_expr->funccollid = px::TypeCollation(func_expr->funcresulttype);

	// Populate RangeTblFunction::funcparams, by walking down the entire
	// func_expr to capture ids of all the PARAMs
	ListCell *lc = nullptr;
	List *param_exprs = px::ExtractNodesExpression(
		(Node *) func_expr, T_Param, false /*descend_into_subqueries */);
	Bitmapset *funcparams = nullptr;
	ForEach(lc, param_exprs)
	{
		Param *param = (Param *) lfirst(lc);
		funcparams = px::BmsAddMember(funcparams, param->paramid);
	}

	RangeTblFunction *rtfunc = MakeNode(RangeTblFunction);
	rtfunc->funcexpr = (Node *) func_expr;
	rtfunc->funcparams = funcparams;
	// GPDB_91_MERGE_FIXME: collation
	// set rtfunc->funccoltypemods & rtfunc->funccolcollations?
	rte->functions = ListMake1(rtfunc);

	rte->inFromCl = true;
	rte->eref = alias;

	return rte;
}

// create a range table entry from a CDXLPhysicalValuesScan node
RangeTblEntry *
CTranslatorDXLToPlStmt::TranslateDXLValueScanToRangeTblEntry(
	const CDXLNode *value_scan_dxlnode, CDXLTranslateContext *output_context,
	CDXLTranslateContextBaseTable *base_table_context)
{
	CDXLPhysicalValuesScan *phy_values_scan_dxlop =
		CDXLPhysicalValuesScan::Cast(value_scan_dxlnode->GetOperator());

	RangeTblEntry *rte = MakeNode(RangeTblEntry);

	rte->relid = InvalidOid;
	rte->subquery = NULL;
	rte->rtekind = RTE_VALUES;
	rte->inh = false; /* never true for values RTEs */
	rte->inFromCl = true;
	rte->requiredPerms = 0;
	rte->checkAsUser = InvalidOid;

	Alias *alias = MakeNode(Alias);
	alias->colnames = NIL;

	// get value alias
	alias->aliasname = CTranslatorUtils::CreateMultiByteCharStringFromWCString(
		phy_values_scan_dxlop->GetOpNameStr()->GetBuffer());

	// project list
	CDXLNode *project_list_dxlnode = (*value_scan_dxlnode)[EdxltsIndexProjList];

	// get column names
	const ULONG num_of_cols = project_list_dxlnode->Arity();
	for (ULONG ul = 0; ul < num_of_cols; ul++)
	{
		CDXLNode *proj_elem_dxlnode = (*project_list_dxlnode)[ul];
		CDXLScalarProjElem *dxl_proj_elem =
			CDXLScalarProjElem::Cast(proj_elem_dxlnode->GetOperator());

		CHAR *col_name_char_array =
			CTranslatorUtils::CreateMultiByteCharStringFromWCString(
				dxl_proj_elem->GetMdNameAlias()->GetMDName()->GetBuffer());

		Value *val_colname = px::MakeStringValue(col_name_char_array);
		alias->colnames = px::LAppend(alias->colnames, val_colname);

		// save mapping col id -> index in translate context
		(void) base_table_context->InsertMapping(dxl_proj_elem->Id(),
												 ul + 1 /*attno*/);
	}

	CMappingColIdVarPlStmt colid_var_mapping =
		CMappingColIdVarPlStmt(m_mp, base_table_context, NULL, output_context,
							   m_dxl_to_plstmt_context);
	const ULONG num_of_child = value_scan_dxlnode->Arity();
	List *values_lists = NIL;
	List *values_collations = NIL;

	for (ULONG ulValue = EdxlValIndexConstStart; ulValue < num_of_child;
		 ulValue++)
	{
		CDXLNode *value_list_dxlnode = (*value_scan_dxlnode)[ulValue];
		const ULONG num_of_cols = value_list_dxlnode->Arity();
		List *value = NIL;
		for (ULONG ulCol = 0; ulCol < num_of_cols; ulCol++)
		{
			Expr *const_expr = m_translator_dxl_to_scalar->TranslateDXLToScalar(
				(*value_list_dxlnode)[ulCol], &colid_var_mapping);
			value = px::LAppend(value, const_expr);
		}
		values_lists = px::LAppend(values_lists, value);

		// GPDB_91_MERGE_FIXME: collation
		if (NIL == values_collations)
		{
			// Set collation based on the first list of values
			for (ULONG ulCol = 0; ulCol < num_of_cols; ulCol++)
			{
				values_collations = px::LAppendOid(
					values_collations, px::ExprCollation((Node *) value));
			}
		}
	}

	rte->values_lists = values_lists;
	rte->colcollations = values_collations;
	rte->eref = alias;

	return rte;
}


//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToPlStmt::TranslateDXLNLJoin
//
//	@doc:
//		Translates a DXL nested loop join node into a NestLoop plan node
//
//---------------------------------------------------------------------------
Plan *
CTranslatorDXLToPlStmt::TranslateDXLNLJoin(
	const CDXLNode *nl_join_dxlnode, CDXLTranslateContext *output_context,
	CDXLTranslationContextArray *ctxt_translation_prev_siblings)
{
	GPOS_ASSERT(nl_join_dxlnode->GetOperator()->GetDXLOperator() ==
				EdxlopPhysicalNLJoin);
	GPOS_ASSERT(nl_join_dxlnode->Arity() == EdxlnljIndexSentinel);

	// create hash join node
	NestLoop *nested_loop = MakeNode(NestLoop);

	Join *join = &(nested_loop->join);
	Plan *plan = &(join->plan);
	plan->plan_node_id = m_dxl_to_plstmt_context->GetNextPlanId();

	CDXLPhysicalNLJoin *dxl_nlj =
		CDXLPhysicalNLJoin::PdxlConvert(nl_join_dxlnode->GetOperator());

	// set join type
	join->jointype = GetGPDBJoinTypeFromDXLJoinType(dxl_nlj->GetJoinType());

	// translate operator costs
	TranslatePlanCosts(nl_join_dxlnode, plan);

	// translate join children
	CDXLNode *left_tree_dxlnode = (*nl_join_dxlnode)[EdxlnljIndexLeftChild];
	CDXLNode *right_tree_dxlnode = (*nl_join_dxlnode)[EdxlnljIndexRightChild];

	CDXLNode *project_list_dxlnode = (*nl_join_dxlnode)[EdxlnljIndexProjList];
	CDXLNode *filter_dxlnode = (*nl_join_dxlnode)[EdxlnljIndexFilter];
	CDXLNode *join_filter_dxlnode = (*nl_join_dxlnode)[EdxlnljIndexJoinFilter];

	CDXLTranslateContext left_dxl_translate_ctxt(
		m_mp, false, output_context->GetColIdToParamIdMap());
	CDXLTranslateContext right_dxl_translate_ctxt(
		m_mp, false, output_context->GetColIdToParamIdMap());

	// setting of prefetch_inner to true except for the case of index NLJ where we cannot prefetch inner
	// because inner child depends on variables coming from outer child
	join->prefetch_inner = (GPOS_FTRACE(EopttraceEnableLeftIndexNLJoin) ?
		px_enable_join_prefetch_inner :
		!dxl_nlj->IsIndexNLJ());

	CDXLTranslationContextArray *translation_context_arr_with_siblings =
		GPOS_NEW(m_mp) CDXLTranslationContextArray(m_mp);
	Plan *left_plan = NULL;
	Plan *right_plan = NULL;
	if (dxl_nlj->IsIndexNLJ())
	{
		const CDXLColRefArray *pdrgdxlcrOuterRefs =
			dxl_nlj->GetNestLoopParamsColRefs();
		const ULONG ulLen = pdrgdxlcrOuterRefs->Size();
		for (ULONG ul = 0; ul < ulLen; ul++)
		{
			CDXLColRef *pdxlcr = (*pdrgdxlcrOuterRefs)[ul];
			IMDId *pmdid = pdxlcr->MdidType();
			ULONG ulColid = pdxlcr->Id();
			INT iTypeModifier = pdxlcr->TypeModifier();
			OID iTypeOid = CMDIdGPDB::CastMdid(pmdid)->Oid();

			if (NULL ==
				right_dxl_translate_ctxt.GetParamIdMappingElement(ulColid))
			{
				ULONG param_id =
					m_dxl_to_plstmt_context->GetNextParamId(iTypeOid);
				CMappingElementColIdParamId *pmecolidparamid =
					GPOS_NEW(m_mp) CMappingElementColIdParamId(
						ulColid, param_id, pmdid, iTypeModifier);
#ifdef GPOS_DEBUG
				BOOL fInserted GPOS_ASSERTS_ONLY =
#endif
					right_dxl_translate_ctxt.FInsertParamMapping(
						ulColid, pmecolidparamid);
				GPOS_ASSERT(fInserted);
			}
		}
		// right child (the index scan side) has references to left child's columns,
		// we need to translate left child first to load its columns into translation context
		left_plan = TranslateDXLOperatorToPlan(left_tree_dxlnode,
											   &left_dxl_translate_ctxt,
											   ctxt_translation_prev_siblings);

		translation_context_arr_with_siblings->Append(&left_dxl_translate_ctxt);
		translation_context_arr_with_siblings->AppendArray(
			ctxt_translation_prev_siblings);

		// translate right child after left child translation is complete
		right_plan = TranslateDXLOperatorToPlan(
			right_tree_dxlnode, &right_dxl_translate_ctxt,
			translation_context_arr_with_siblings);
	}
	else
	{
		// left child may include a PartitionSelector with references to right child's columns,
		// we need to translate right child first to load its columns into translation context
		right_plan = TranslateDXLOperatorToPlan(right_tree_dxlnode,
												&right_dxl_translate_ctxt,
												ctxt_translation_prev_siblings);

		translation_context_arr_with_siblings->Append(
			&right_dxl_translate_ctxt);
		translation_context_arr_with_siblings->AppendArray(
			ctxt_translation_prev_siblings);

		// translate left child after right child translation is complete
		left_plan = TranslateDXLOperatorToPlan(
			left_tree_dxlnode, &left_dxl_translate_ctxt,
			translation_context_arr_with_siblings);
	}
	CDXLTranslationContextArray *child_contexts =
		GPOS_NEW(m_mp) CDXLTranslationContextArray(m_mp);
	child_contexts->Append(&left_dxl_translate_ctxt);
	child_contexts->Append(&right_dxl_translate_ctxt);

	// translate proj list and filter
	TranslateProjListAndFilter(project_list_dxlnode, filter_dxlnode,
							   NULL,  // translate context for the base table
							   child_contexts, &plan->targetlist, &plan->qual,
							   output_context);

	// translate join condition
	join->joinqual =
		TranslateDXLFilterToQual(join_filter_dxlnode,
								 NULL,	// translate context for the base table
								 child_contexts, output_context);

	// create nest loop params for index nested loop joins
	if (dxl_nlj->IsIndexNLJ())
	{
		((NestLoop *) plan)->nestParams = TranslateNestLoopParamList(
			dxl_nlj->GetNestLoopParamsColRefs(), &left_dxl_translate_ctxt,
			&right_dxl_translate_ctxt);
	}
	plan->lefttree = left_plan;
	plan->righttree = right_plan;
	SetParamIds(plan);

	// cleanup
	translation_context_arr_with_siblings->Release();
	child_contexts->Release();

	return (Plan *) nested_loop;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToPlStmt::TranslateDXLMergeJoin
//
//	@doc:
//		Translates a DXL merge join node into a MergeJoin node
//
//---------------------------------------------------------------------------
Plan *
CTranslatorDXLToPlStmt::TranslateDXLMergeJoin(
	const CDXLNode *merge_join_dxlnode, CDXLTranslateContext *output_context,
	CDXLTranslationContextArray *ctxt_translation_prev_siblings)
{
	GPOS_ASSERT(merge_join_dxlnode->GetOperator()->GetDXLOperator() ==
				EdxlopPhysicalMergeJoin);
	GPOS_ASSERT(merge_join_dxlnode->Arity() == EdxlmjIndexSentinel);

	// create merge join node
	MergeJoin *merge_join = MakeNode(MergeJoin);

	Join *join = &(merge_join->join);
	Plan *plan = &(join->plan);
	plan->plan_node_id = m_dxl_to_plstmt_context->GetNextPlanId();

	CDXLPhysicalMergeJoin *merge_join_dxlop =
		CDXLPhysicalMergeJoin::Cast(merge_join_dxlnode->GetOperator());

	// set join type
	join->jointype =
		GetGPDBJoinTypeFromDXLJoinType(merge_join_dxlop->GetJoinType());

	// translate operator costs
	TranslatePlanCosts(merge_join_dxlnode, plan);

	// translate join children
	CDXLNode *left_tree_dxlnode = (*merge_join_dxlnode)[EdxlmjIndexLeftChild];
	CDXLNode *right_tree_dxlnode = (*merge_join_dxlnode)[EdxlmjIndexRightChild];

	CDXLNode *project_list_dxlnode = (*merge_join_dxlnode)[EdxlmjIndexProjList];
	CDXLNode *filter_dxlnode = (*merge_join_dxlnode)[EdxlmjIndexFilter];
	CDXLNode *join_filter_dxlnode =
		(*merge_join_dxlnode)[EdxlmjIndexJoinFilter];
	CDXLNode *merge_cond_list_dxlnode =
		(*merge_join_dxlnode)[EdxlmjIndexMergeCondList];

	CDXLTranslateContext left_dxl_translate_ctxt(
		m_mp, false, output_context->GetColIdToParamIdMap());
	CDXLTranslateContext right_dxl_translate_ctxt(
		m_mp, false, output_context->GetColIdToParamIdMap());

	Plan *left_plan =
		TranslateDXLOperatorToPlan(left_tree_dxlnode, &left_dxl_translate_ctxt,
								   ctxt_translation_prev_siblings);

	CDXLTranslationContextArray *translation_context_arr_with_siblings =
		GPOS_NEW(m_mp) CDXLTranslationContextArray(m_mp);
	translation_context_arr_with_siblings->Append(&left_dxl_translate_ctxt);
	translation_context_arr_with_siblings->AppendArray(
		ctxt_translation_prev_siblings);

	Plan *right_plan = TranslateDXLOperatorToPlan(
		right_tree_dxlnode, &right_dxl_translate_ctxt,
		translation_context_arr_with_siblings);

	CDXLTranslationContextArray *child_contexts =
		GPOS_NEW(m_mp) CDXLTranslationContextArray(m_mp);
	child_contexts->Append(
		const_cast<CDXLTranslateContext *>(&left_dxl_translate_ctxt));
	child_contexts->Append(
		const_cast<CDXLTranslateContext *>(&right_dxl_translate_ctxt));

	// translate proj list and filter
	TranslateProjListAndFilter(project_list_dxlnode, filter_dxlnode,
							   NULL,  // translate context for the base table
							   child_contexts, &plan->targetlist, &plan->qual,
							   output_context);

	// translate join filter
	join->joinqual =
		TranslateDXLFilterToQual(join_filter_dxlnode,
								 NULL,	// translate context for the base table
								 child_contexts, output_context);

	// translate merge cond
	List *merge_conditions_list = NIL;

	const ULONG num_join_conds = merge_cond_list_dxlnode->Arity();
	for (ULONG ul = 0; ul < num_join_conds; ul++)
	{
		CDXLNode *merge_condition_dxlnode = (*merge_cond_list_dxlnode)[ul];
		List *merge_condition_list =
			TranslateDXLScCondToQual(merge_condition_dxlnode,
									 NULL,	// base table translation context
									 child_contexts, output_context);

		GPOS_ASSERT(1 == px::ListLength(merge_condition_list));
		merge_conditions_list =
			px::ListConcat(merge_conditions_list, merge_condition_list);
	}

	GPOS_ASSERT(NIL != merge_conditions_list);

	merge_join->mergeclauses = merge_conditions_list;

	plan->lefttree = left_plan;
	plan->righttree = right_plan;
	SetParamIds(plan);

	merge_join->mergeFamilies =
		(Oid *) px::GPDBAlloc(sizeof(Oid) * num_join_conds);
	merge_join->mergeStrategies =
		(int *) px::GPDBAlloc(sizeof(int) * num_join_conds);
	merge_join->mergeCollations =
		(Oid *) px::GPDBAlloc(sizeof(Oid) * num_join_conds);
	merge_join->mergeNullsFirst =
		(bool *) px::GPDBAlloc(sizeof(bool) * num_join_conds);

	ListCell *lc;
	ULONG ul = 0;
	foreach (lc, merge_join->mergeclauses)
	{
		Expr *expr = (Expr *) lfirst(lc);

		if (IsA(expr, OpExpr))
		{
			// we are ok - phew
			OpExpr *opexpr = (OpExpr *) expr;
			List *mergefamilies = px::GetMergeJoinOpFamilies(opexpr->opno);

			GPOS_ASSERT(NULL != mergefamilies &&
						px::ListLength(mergefamilies) > 0);

			// Pick the first - it's probably what we want
			merge_join->mergeFamilies[ul] = px::ListNthOid(mergefamilies, 0);

			GPOS_ASSERT(px::ListLength(opexpr->args) == 2);
			Expr *leftarg = (Expr *) px::ListNth(opexpr->args, 0);

			Expr *rightarg pg_attribute_unused() =
				(Expr *) px::ListNth(opexpr->args, 1);
			GPOS_ASSERT(px::ExprCollation((Node *) leftarg) ==
						px::ExprCollation((Node *) rightarg));

			merge_join->mergeCollations[ul] =
				px::ExprCollation((Node *) leftarg);

			// Make sure that the following properties match
			// those in CPhysicalFullMergeJoin::PosRequired().
			merge_join->mergeStrategies[ul] = BTLessStrategyNumber;
			merge_join->mergeNullsFirst[ul] = false;
			++ul;
		}
		else
		{
			GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiQuery2DXLUnsupportedFeature,
					   GPOS_WSZ_LIT("Not an op expression in merge clause"));
			break;
		}
	}

	// cleanup
	translation_context_arr_with_siblings->Release();
	child_contexts->Release();

	return (Plan *) merge_join;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToPlStmt::TranslateDXLHash
//
//	@doc:
//		Translates a DXL physical operator node into a Hash node
//
//---------------------------------------------------------------------------
Plan *
CTranslatorDXLToPlStmt::TranslateDXLHash(
	const CDXLNode *dxlnode, CDXLTranslateContext *output_context,
	CDXLTranslationContextArray *ctxt_translation_prev_siblings)
{
	Hash *hash = MakeNode(Hash);

	Plan *plan = &(hash->plan);
	plan->plan_node_id = m_dxl_to_plstmt_context->GetNextPlanId();

	// translate dxl node
	CDXLTranslateContext dxl_translate_ctxt(
		m_mp, false, output_context->GetColIdToParamIdMap());

	Plan *left_plan = TranslateDXLOperatorToPlan(
		dxlnode, &dxl_translate_ctxt, ctxt_translation_prev_siblings);

	GPOS_ASSERT(0 < dxlnode->Arity());

	// create a reference to each entry in the child project list to create the target list of
	// the hash node
	CDXLNode *project_list_dxlnode = (*dxlnode)[0];
	List *target_list = TranslateDXLProjectListToHashTargetList(
		project_list_dxlnode, &dxl_translate_ctxt, output_context);

	// copy costs from child node; the startup cost for the hash node is the total cost
	// of the child plan, see make_hash in createplan.c
	plan->startup_cost = left_plan->total_cost;
	plan->total_cost = left_plan->total_cost;
	plan->plan_rows = left_plan->plan_rows;
	plan->plan_width = left_plan->plan_width;

	plan->targetlist = target_list;
	plan->lefttree = left_plan;
	plan->righttree = NULL;
	plan->qual = NIL;
	hash->rescannable = false;

	SetParamIds(plan);

	return (Plan *) hash;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToPlStmt::TranslateDXLDuplicateSensitiveMotion
//
//	@doc:
//		Translate DXL motion node
//
//---------------------------------------------------------------------------
Plan *
CTranslatorDXLToPlStmt::TranslateDXLDuplicateSensitiveMotion(
	const CDXLNode *motion_dxlnode, CDXLTranslateContext *output_context,
	CDXLTranslationContextArray *ctxt_translation_prev_siblings)
{
	CDXLPhysicalMotion *motion_dxlop =
		CDXLPhysicalMotion::Cast(motion_dxlnode->GetOperator());
	/* POLAR px */
	if (CTranslatorUtils::IsDuplicateSensitiveMotion(motion_dxlop) &&
		px_enable_result_hash_filter)
	{
		return TranslateDXLRedistributeMotionToResultHashFilters(
			motion_dxlnode, output_context, ctxt_translation_prev_siblings);
	}

	return TranslateDXLMotion(motion_dxlnode, output_context,
							  ctxt_translation_prev_siblings);
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToPlStmt::TranslateDXLMotion
//
//	@doc:
//		Translate DXL motion node into GPDB Motion plan node
//
//---------------------------------------------------------------------------
Plan *
CTranslatorDXLToPlStmt::TranslateDXLMotion(
	const CDXLNode *motion_dxlnode, CDXLTranslateContext *output_context,
	CDXLTranslationContextArray *ctxt_translation_prev_siblings)
{
	CDXLPhysicalMotion *motion_dxlop =
		CDXLPhysicalMotion::Cast(motion_dxlnode->GetOperator());
	const IntPtrArray *input_segids_array = motion_dxlop->GetInputSegIdsArray();
	PlanSlice *recvslice = m_dxl_to_plstmt_context->GetCurrentSlice();

	// create motion node
	Motion *motion = MakeNode(Motion);

	Plan *plan = &(motion->plan);
	plan->plan_node_id = m_dxl_to_plstmt_context->GetNextPlanId();

	// Translate operator costs before changing the current slice.
	TranslatePlanCosts(motion_dxlnode, plan);

	CDXLNode *project_list_dxlnode = (*motion_dxlnode)[EdxlgmIndexProjList];
	CDXLNode *filter_dxlnode = (*motion_dxlnode)[EdxlgmIndexFilter];
	CDXLNode *sort_col_list_dxl = (*motion_dxlnode)[EdxlgmIndexSortColList];

	PlanSlice *sendslice = (PlanSlice *) px::GPDBAlloc(sizeof(PlanSlice));
	memset(sendslice, 0, sizeof(PlanSlice));

	sendslice->sliceIndex = m_dxl_to_plstmt_context->AddSlice(sendslice);
	sendslice->parentIndex = recvslice->sliceIndex;
	m_dxl_to_plstmt_context->SetCurrentSlice(sendslice);

	// only one sender
	if (1 == input_segids_array->Size())
	{
		int worker_idx = *((*input_segids_array)[0]);

		// only one segment in total
		if (worker_idx == MASTER_CONTENT_ID)
		{
			// sender is on master, must be singleton gang
			sendslice->gangType = GANGTYPE_ENTRYDB_READER;
		}
		else if (1 == px::GetPxWorkerCount())
		{
			// sender is on segment, can not tell it's singleton or
			// all-segment gang, so treat it as all-segment reader gang.
			// It can be promoted to writer gang later if needed.
			sendslice->gangType = GANGTYPE_PRIMARY_READER;
		}
		else
		{
			// multiple segments, must be singleton gang
			sendslice->gangType = GANGTYPE_SINGLETON_READER;
		}
		sendslice->numsegments = 1;
		sendslice->worker_idx = worker_idx;
	}
	else
	{
		// Mark it as reader for now. Will be overwritten into WRITER, if we
		// encounter a DML node.
		sendslice->gangType = GANGTYPE_PRIMARY_READER;
		sendslice->numsegments = m_num_of_segments;
		sendslice->worker_idx = 0;
	}
	sendslice->directDispatch.isDirectDispatch = false;
	sendslice->directDispatch.contentIds = NIL;
	sendslice->directDispatch.haveProcessedAnyCalculations = false;

	motion->motionID = sendslice->sliceIndex;

	// translate motion child
	// child node is in the same position in broadcast and gather motion nodes
	// but different in redistribute motion nodes

	ULONG child_index = motion_dxlop->GetRelationChildIdx();

	CDXLNode *child_dxlnode = (*motion_dxlnode)[child_index];

	CDXLTranslateContext child_context(m_mp, false,
									   output_context->GetColIdToParamIdMap());

	// Recurse into the child, which runs in the sending slice.
	m_dxl_to_plstmt_context->SetCurrentSlice(sendslice);

	Plan *child_plan = TranslateDXLOperatorToPlan(
		child_dxlnode, &child_context, ctxt_translation_prev_siblings);

	CDXLTranslationContextArray *child_contexts =
		GPOS_NEW(m_mp) CDXLTranslationContextArray(m_mp);
	child_contexts->Append(const_cast<CDXLTranslateContext *>(&child_context));

	// translate proj list and filter
	TranslateProjListAndFilter(project_list_dxlnode, filter_dxlnode,
							   NULL,  // translate context for the base table
							   child_contexts, &plan->targetlist, &plan->qual,
							   output_context);

	// translate sorting info
	ULONG num_sort_cols = sort_col_list_dxl->Arity();
	if (0 < num_sort_cols)
	{
		motion->sendSorted = true;
		motion->numSortCols = num_sort_cols;
		motion->sortColIdx =
			(AttrNumber *) px::GPDBAlloc(num_sort_cols * sizeof(AttrNumber));
		motion->sortOperators =
			(Oid *) px::GPDBAlloc(num_sort_cols * sizeof(Oid));
		motion->collations =
			(Oid *) px::GPDBAlloc(num_sort_cols * sizeof(Oid));
		motion->nullsFirst =
			(bool *) px::GPDBAlloc(num_sort_cols * sizeof(bool));

		TranslateSortCols(sort_col_list_dxl, output_context, motion->sortColIdx,
						  motion->sortOperators, motion->collations,
						  motion->nullsFirst);
	}
	else
	{
		// not a sorting motion
		motion->sendSorted = false;
		motion->numSortCols = 0;
		motion->sortColIdx = NULL;
		motion->sortOperators = NULL;
		motion->nullsFirst = NULL;
	}

	if (motion_dxlop->GetDXLOperator() == EdxlopPhysicalMotionRedistribute ||
		motion_dxlop->GetDXLOperator() ==
			EdxlopPhysicalMotionRoutedDistribute ||
		motion_dxlop->GetDXLOperator() == EdxlopPhysicalMotionRandom)
	{
		// translate hash expr list
		List *hash_expr_list = NIL;
		List *hash_expr_opfamilies = NIL;
		int numHashExprs;

		if (EdxlopPhysicalMotionRedistribute == motion_dxlop->GetDXLOperator())
		{
			CDXLNode *hash_expr_list_dxlnode =
				(*motion_dxlnode)[EdxlrmIndexHashExprList];

			TranslateHashExprList(hash_expr_list_dxlnode, &child_context,
								  &hash_expr_list, &hash_expr_opfamilies,
								  output_context);
		}
		numHashExprs = px::ListLength(hash_expr_list);

		int i = 0;
		ListCell *lc, *lcoid;
		Oid *hashFuncs = (Oid *) px::GPDBAlloc(numHashExprs * sizeof(Oid));

		if (GPOS_FTRACE(EopttraceConsiderOpfamiliesForDistribution))
		{
			GPOS_ASSERT(px::ListLength(hash_expr_list) ==
						px::ListLength(hash_expr_opfamilies));
			forboth(lc, hash_expr_list, lcoid, hash_expr_opfamilies)
			{
				Node *expr = (Node *) lfirst(lc);
				Oid typeoid = px::ExprType(expr);
				Oid opfamily = lfirst_oid(lcoid);
				hashFuncs[i] = px::GetHashProcInOpfamily(opfamily, typeoid);
				i++;
			}
		}
		else
		{
			foreach (lc, hash_expr_list)
			{
				Node *expr = (Node *) lfirst(lc);
				Oid typeoid = px::ExprType(expr);
				hashFuncs[i] =
					m_dxl_to_plstmt_context->GetDistributionHashFuncForType(
						typeoid);
				i++;
			}
		}


		motion->hashExprs = hash_expr_list;
		motion->hashFuncs = hashFuncs;
	}

	// cleanup
	child_contexts->Release();

	m_dxl_to_plstmt_context->SetCurrentSlice(recvslice);

	plan->lefttree = child_plan;

	// translate properties of the specific type of motion operator

	switch (motion_dxlop->GetDXLOperator())
	{
		case EdxlopPhysicalMotionGather:
		{
			motion->motionType = MOTIONTYPE_GATHER;
			break;
		}
		case EdxlopPhysicalMotionRedistribute:
		case EdxlopPhysicalMotionRandom:
		{
			motion->motionType = MOTIONTYPE_HASH;
			// motion->numHashSegments =
			// 		(int) motion_dxlop->GetOutputSegIdsArray()->Size();
			// GPOS_ASSERT(motion->numHashSegments > 0);
			break;
		}
		case EdxlopPhysicalMotionBroadcast:
		{
			motion->motionType = MOTIONTYPE_BROADCAST;
			break;
		}
		case EdxlopPhysicalMotionRoutedDistribute:
		{
			ULONG segid_col =
				CDXLPhysicalRoutedDistributeMotion::Cast(motion_dxlop)
					->SegmentIdCol();
			const TargetEntry *te_sort_col =
				child_context.GetTargetEntry(segid_col);

			motion->motionType = MOTIONTYPE_EXPLICIT;
			motion->segidColIdx = te_sort_col->resno;
			break;
		}
		default:
			GPOS_ASSERT(!"Unrecognized Motion operator");
			return NULL;
	}

	SetParamIds(plan);

	return (Plan *) motion;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToPlStmt::TranslateDXLRedistributeMotionToResultHashFilters
//
//	@doc:
//		Translate DXL duplicate sensitive redistribute motion node into
//		GPDB result node with hash filters
//
//---------------------------------------------------------------------------
Plan *
CTranslatorDXLToPlStmt::TranslateDXLRedistributeMotionToResultHashFilters(
	const CDXLNode *motion_dxlnode, CDXLTranslateContext *output_context,
	CDXLTranslationContextArray *ctxt_translation_prev_siblings)
{
	// create motion node
	Result *result = MakeNode(Result);

	Plan *plan = &(result->plan);
	plan->plan_node_id = m_dxl_to_plstmt_context->GetNextPlanId();

	CDXLPhysicalMotion *motion_dxlop =
		CDXLPhysicalMotion::Cast(motion_dxlnode->GetOperator());

	// translate operator costs
	TranslatePlanCosts(motion_dxlnode, plan);

	CDXLNode *project_list_dxlnode = (*motion_dxlnode)[EdxlrmIndexProjList];
	CDXLNode *filter_dxlnode = (*motion_dxlnode)[EdxlrmIndexFilter];
	CDXLNode *child_dxlnode =
		(*motion_dxlnode)[motion_dxlop->GetRelationChildIdx()];

	CDXLTranslateContext child_context(m_mp, false,
									   output_context->GetColIdToParamIdMap());

	Plan *child_plan = TranslateDXLOperatorToPlan(
		child_dxlnode, &child_context, ctxt_translation_prev_siblings);

	CDXLTranslationContextArray *child_contexts =
		GPOS_NEW(m_mp) CDXLTranslationContextArray(m_mp);
	child_contexts->Append(const_cast<CDXLTranslateContext *>(&child_context));

	// translate proj list and filter
	TranslateProjListAndFilter(project_list_dxlnode, filter_dxlnode,
							   NULL,  // translate context for the base table
							   child_contexts, &plan->targetlist, &plan->qual,
							   output_context);

	bool targetlist_modified = false;

	// translate hash expr list
	if (EdxlopPhysicalMotionRedistribute == motion_dxlop->GetDXLOperator())
	{
		CDXLNode *hash_expr_list_dxlnode =
			(*motion_dxlnode)[EdxlrmIndexHashExprList];
		const ULONG length = hash_expr_list_dxlnode->Arity();
		GPOS_ASSERT(0 < length);

		result->numHashFilterCols = length;
		result->hashFilterColIdx =
			(AttrNumber *) px::GPDBAlloc(length * sizeof(AttrNumber));
		result->hashFilterFuncs = (Oid *) px::GPDBAlloc(length * sizeof(Oid));

		for (ULONG ul = 0; ul < length; ul++)
		{
			CDXLNode *hash_expr_dxlnode = (*hash_expr_list_dxlnode)[ul];
			CDXLNode *expr_dxlnode = (*hash_expr_dxlnode)[0];
			const TargetEntry *target_entry;

			if (EdxlopScalarIdent ==
				expr_dxlnode->GetOperator()->GetDXLOperator())
			{
				ULONG colid = CDXLScalarIdent::Cast(expr_dxlnode->GetOperator())
								  ->GetDXLColRef()
								  ->Id();
				target_entry = output_context->GetTargetEntry(colid);
			}
			else
			{
				// The expression is not a scalar ident that points to an output column in the child node.
				// Rather, it is an expresssion that is evaluated by the hash filter such as CAST(a) or a+b.
				// We therefore, create a corresponding GPDB scalar expression and add it to the project list
				// of the hash filter
				CMappingColIdVarPlStmt colid_var_mapping =
					CMappingColIdVarPlStmt(
						m_mp,
						NULL,  // translate context for the base table
						child_contexts, output_context,
						m_dxl_to_plstmt_context);

				Expr *expr = m_translator_dxl_to_scalar->TranslateDXLToScalar(
					expr_dxlnode, &colid_var_mapping);
				GPOS_ASSERT(NULL != expr);

				// create a target entry for the hash filter
				CWStringConst str_unnamed_col(GPOS_WSZ_LIT("?column?"));
				target_entry = px::MakeTargetEntry(
					expr, px::ListLength(plan->targetlist) + 1,
					CTranslatorUtils::CreateMultiByteCharStringFromWCString(
						str_unnamed_col.GetBuffer()),
					false /* resjunk */);
				plan->targetlist =
					px::LAppend(plan->targetlist, (void *) target_entry);
				targetlist_modified = true;
			}

			result->hashFilterColIdx[ul] = target_entry->resno;
			result->hashFilterFuncs[ul] =
				m_dxl_to_plstmt_context->GetDistributionHashFuncForType(
					px::ExprType((Node *) target_entry->expr));
		}
	}
	else
	{
		// A Redistribute Motion without any expressions to hash, means that
		// the subtree should run on one segment only, and we don't care which
		// segment it is. That is represented by a One-Off Filter, where we
		// check that the segment number matches an arbitrarily chosen one.
		int segment = px::PxHashRandomSeg(px::GetPxWorkerCount());

		result->resconstantqual =
			(Node *) ListMake1(px::MakePXWorkerIndexFilterExpr(segment));
	}

	// cleanup
	child_contexts->Release();

	plan->lefttree = child_plan;

	SetParamIds(plan);

	Plan *child_result = (Plan *) result;
/*no cover begin*/
	if (targetlist_modified)
	{
		// If the targetlist is modified by adding any expressions, such as for
		// hashFilterColIdx & hashFilterFuncs, add an additional Result node on top
		// to project only the elements from the original targetlist.
		// This is needed in case the Result node is created under the Hash
		// operator (or any non-projecting node), which expects the targetlist of its
		// child node to contain only elements that are to be hashed.
		// We should not generate a plan where the target list of a non-projecting
		// node such as Hash does not match its child. Additional expressions
		// here can cause issues with memtuple bindings that can lead to errors.
		Result *result = MakeNode(Result);

		Plan *plan = &(result->plan);
		plan->plan_node_id = m_dxl_to_plstmt_context->GetNextPlanId();

		// keep the same costs & rows estimates
		plan->startup_cost = child_result->startup_cost;
		plan->total_cost = child_result->total_cost;
		plan->plan_rows = child_result->plan_rows;
		plan->plan_width = child_result->plan_width;

		// populate the targetlist based on child_result's original targetlist
		plan->targetlist = NIL;
		ListCell *lc = NULL;
		ULONG ul = 0;
		ForEach(lc, child_result->targetlist)
		{
			if (ul++ >= project_list_dxlnode->Arity())
			{
				// done with the original targetlist, stop
				// all expressions added after project_list_dxlnode->Arity() are
				// not output cols, but rather hash expressions and should not be projected
				break;
			}

			TargetEntry *te = (TargetEntry *) lfirst(lc);
			Var *var = px::MakeVar(
				OUTER_VAR, te->resno, px::ExprType((Node *) te->expr),
				px::ExprTypeMod((Node *) te->expr), 0 /* varlevelsup */);
			TargetEntry *new_te =
				px::MakeTargetEntry((Expr *) var, ul, /* resno */
									  te->resname, te->resjunk);
			plan->targetlist = px::LAppend(plan->targetlist, new_te);
		}

		plan->qual = NIL;
		plan->lefttree = child_result;

		SetParamIds(plan);

		return (Plan *) result;
	}
/*no cover end*/
	return (Plan *) result;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToPlStmt::TranslateDXLAgg
//
//	@doc:
//		Translate DXL aggregate node into GPDB Agg plan node
//
//---------------------------------------------------------------------------
Plan *
CTranslatorDXLToPlStmt::TranslateDXLAgg(
	const CDXLNode *agg_dxlnode, CDXLTranslateContext *output_context,
	CDXLTranslationContextArray *ctxt_translation_prev_siblings)
{
	// create aggregate plan node
	Agg *agg = MakeNode(Agg);

	Plan *plan = &(agg->plan);
	plan->plan_node_id = m_dxl_to_plstmt_context->GetNextPlanId();

	CDXLPhysicalAgg *dxl_phy_agg_dxlop =
		CDXLPhysicalAgg::Cast(agg_dxlnode->GetOperator());

	// translate operator costs
	TranslatePlanCosts(agg_dxlnode, plan);

	// translate agg child
	CDXLNode *child_dxlnode = (*agg_dxlnode)[EdxlaggIndexChild];

	CDXLNode *project_list_dxlnode = (*agg_dxlnode)[EdxlaggIndexProjList];
	CDXLNode *filter_dxlnode = (*agg_dxlnode)[EdxlaggIndexFilter];

	CDXLTranslateContext child_context(m_mp, true,
									   output_context->GetColIdToParamIdMap());

	Plan *child_plan = TranslateDXLOperatorToPlan(
		child_dxlnode, &child_context, ctxt_translation_prev_siblings);

	CDXLTranslationContextArray *child_contexts =
		GPOS_NEW(m_mp) CDXLTranslationContextArray(m_mp);
	child_contexts->Append(const_cast<CDXLTranslateContext *>(&child_context));

	// translate proj list and filter
	TranslateProjListAndFilter(project_list_dxlnode, filter_dxlnode,
							   NULL,  // translate context for the base table
							   child_contexts,	// pdxltrctxRight,
							   &plan->targetlist, &plan->qual, output_context);

	// Set the aggsplit for the agg node
	ListCell *lc;
	foreach (lc, plan->targetlist)
	{
		TargetEntry *te = (TargetEntry *) lfirst(lc);
		if (IsA(te->expr, Aggref))
		{
			Aggref *aggref = (Aggref *) te->expr;
			agg->aggsplit = aggref->aggsplit;
			break;
		}
	}

	plan->lefttree = child_plan;

	// translate aggregation strategy
	switch (dxl_phy_agg_dxlop->GetAggStrategy())
	{
		case EdxlaggstrategyPlain:
			agg->aggstrategy = AGG_PLAIN;
			break;
		case EdxlaggstrategySorted:
			agg->aggstrategy = AGG_SORTED;
			break;
		case EdxlaggstrategyHashed:
			agg->aggstrategy = AGG_HASHED;
			break;
		default:
			GPOS_ASSERT(!"Invalid aggregation strategy");
	}

	agg->streaming = dxl_phy_agg_dxlop->IsStreamSafe();

	// translate grouping cols
	const ULongPtrArray *grouping_colid_array =
		dxl_phy_agg_dxlop->GetGroupingColidArray();
	agg->numCols = grouping_colid_array->Size();
	if (agg->numCols > 0)
	{
		agg->grpColIdx =
			(AttrNumber *) px::GPDBAlloc(agg->numCols * sizeof(AttrNumber));
		agg->grpOperators = (Oid *) px::GPDBAlloc(agg->numCols * sizeof(Oid));
		// agg->grpCollations =
		//	(Oid *) px::GPDBAlloc(agg->numCols * sizeof(Oid));
	}
	else
	{
		agg->grpColIdx = NULL;
		agg->grpOperators = NULL;
		// agg->grpCollations = NULL;
	}

	const ULONG length = grouping_colid_array->Size();
	for (ULONG ul = 0; ul < length; ul++)
	{
		ULONG grouping_colid = *((*grouping_colid_array)[ul]);
		const TargetEntry *target_entry_grouping_col =
			child_context.GetTargetEntry(grouping_colid);
		if (NULL == target_entry_grouping_col)
		{
			GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXL2PlStmtAttributeNotFound,
					   grouping_colid);
		}
		agg->grpColIdx[ul] = target_entry_grouping_col->resno;

		// Also find the equality operators to use for each grouping col.
		Oid typeId = px::ExprType((Node *) target_entry_grouping_col->expr);
		agg->grpOperators[ul] = px::GetEqualityOp(typeId);
		// agg->grpCollations[ul] =
		//   px::ExprCollation((Node *) target_entry_grouping_col->expr);
		Assert(agg->grpOperators[ul] != 0);
	}

	agg->numGroups =
		std::max(1L, (long) std::min(agg->plan.plan_rows, (double) LONG_MAX));
	SetParamIds(plan);

	// cleanup
	child_contexts->Release();

	return (Plan *) agg;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToPlStmt::TranslateDXLWindow
//
//	@doc:
//		Translate DXL window node into GPDB window plan node
//
//---------------------------------------------------------------------------
Plan *
CTranslatorDXLToPlStmt::TranslateDXLWindow(
	const CDXLNode *window_dxlnode, CDXLTranslateContext *output_context,
	CDXLTranslationContextArray *ctxt_translation_prev_siblings)
{
	// create a WindowAgg plan node
	WindowAgg *window = MakeNode(WindowAgg);

	Plan *plan = &(window->plan);
	plan->plan_node_id = m_dxl_to_plstmt_context->GetNextPlanId();

	CDXLPhysicalWindow *window_dxlop =
		CDXLPhysicalWindow::Cast(window_dxlnode->GetOperator());

	// translate the operator costs
	TranslatePlanCosts(window_dxlnode, plan);

	// translate children
	CDXLNode *child_dxlnode = (*window_dxlnode)[EdxlwindowIndexChild];
	CDXLNode *project_list_dxlnode = (*window_dxlnode)[EdxlwindowIndexProjList];
	CDXLNode *filter_dxlnode = (*window_dxlnode)[EdxlwindowIndexFilter];

	CDXLTranslateContext child_context(m_mp, true,
									   output_context->GetColIdToParamIdMap());
	Plan *child_plan = TranslateDXLOperatorToPlan(
		child_dxlnode, &child_context, ctxt_translation_prev_siblings);

	CDXLTranslationContextArray *child_contexts =
		GPOS_NEW(m_mp) CDXLTranslationContextArray(m_mp);
	child_contexts->Append(const_cast<CDXLTranslateContext *>(&child_context));

	// translate proj list and filter
	TranslateProjListAndFilter(project_list_dxlnode, filter_dxlnode,
							   NULL,  // translate context for the base table
							   child_contexts,	// pdxltrctxRight,
							   &plan->targetlist, &plan->qual, output_context);

	ListCell *lc;

	foreach (lc, plan->targetlist)
	{
		TargetEntry *target_entry = (TargetEntry *) lfirst(lc);
		if (IsA(target_entry->expr, WindowFunc))
		{
			WindowFunc *window_func = (WindowFunc *) target_entry->expr;
			window->winref = window_func->winref;
			break;
		}
	}

	plan->lefttree = child_plan;

	// translate partition columns
	const ULongPtrArray *part_by_cols_array =
		window_dxlop->GetPartByColsArray();
	window->partNumCols = part_by_cols_array->Size();
	window->partColIdx = NULL;
	window->partOperators = NULL;
	// window->partCollations = NULL;

	if (window->partNumCols > 0)
	{
		window->partColIdx = (AttrNumber *) px::GPDBAlloc(
			window->partNumCols * sizeof(AttrNumber));
		window->partOperators =
			(Oid *) px::GPDBAlloc(window->partNumCols * sizeof(Oid));
		// window->partCollations =
		// 	(Oid *) px::GPDBAlloc(window->partNumCols * sizeof(Oid));
	}

	const ULONG num_of_part_cols = part_by_cols_array->Size();
	for (ULONG ul = 0; ul < num_of_part_cols; ul++)
	{
		ULONG part_colid = *((*part_by_cols_array)[ul]);
		const TargetEntry *te_part_colid =
			child_context.GetTargetEntry(part_colid);
		if (NULL == te_part_colid)
		{
			GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXL2PlStmtAttributeNotFound,
					   part_colid);
		}
		window->partColIdx[ul] = te_part_colid->resno;

		// Also find the equality operators to use for each partitioning key col.
		Oid type_id = px::ExprType((Node *) te_part_colid->expr);
		window->partOperators[ul] = px::GetEqualityOp(type_id);
		Assert(window->partOperators[ul] != 0);
		// window->partCollations[ul] =
		// 	px::ExprCollation((Node *) te_part_colid->expr);
	}

	// translate window keys
	const ULONG size = window_dxlop->WindowKeysCount();
	if (size > 1)
	{
		PxEreport(ERRCODE_INTERNAL_ERROR,
			ERROR,
			"PXOPT produced a plan with more than one window key",
			NULL);
	}
	GPOS_ASSERT(size <= 1 && "cannot have more than one window key");

	if (size == 1)
	{
		// translate the sorting columns used in the window key
		const CDXLWindowKey *window_key = window_dxlop->GetDXLWindowKeyAt(0);
		const CDXLWindowFrame *window_frame = window_key->GetWindowFrame();
		const CDXLNode *sort_col_list_dxlnode = window_key->GetSortColListDXL();

		const ULONG num_of_cols = sort_col_list_dxlnode->Arity();

		window->ordNumCols = num_of_cols;
		window->ordColIdx =
			(AttrNumber *) px::GPDBAlloc(num_of_cols * sizeof(AttrNumber));
		window->ordOperators =
			(Oid *) px::GPDBAlloc(num_of_cols * sizeof(Oid));
		// window->ordCollations =
		//	(Oid *) px::GPDBAlloc(num_of_cols * sizeof(Oid));
		bool *is_nulls_first =
			(bool *) px::GPDBAlloc(num_of_cols * sizeof(bool));
		TranslateSortCols(sort_col_list_dxlnode, &child_context,
						  window->ordColIdx, window->ordOperators,
						  NULL, is_nulls_first);

		// The firstOrder* fields are separate from just picking the first of ordCol*,
		// because the Postgres planner might omit columns that are redundant with the
		// PARTITION BY from ordCol*. But ORCA doesn't do that, so we can just copy
		// the first entry of ordColIdx/ordOperators into firstOrder* fields.
		if (num_of_cols > 0)
		{
			// TOOD: for windowagg
			// window->firstOrderCol = window->ordColIdx[0];
			// window->firstOrderCmpOperator = window->ordOperators[0];
			// window->firstOrderNullsFirst = is_nulls_first[0];
		}
		px::GPDBFree(is_nulls_first);

		// The ordOperators array is actually supposed to contain equality operators,
		// not ordering operators (< or >). So look up the corresponding equality
		// operator for each ordering operator.
		for (ULONG i = 0; i < num_of_cols; i++)
		{
			window->ordOperators[i] =
				px::GetEqualityOpForOrderingOp(window->ordOperators[i], NULL);
		}

		// translate the window frame specified in the window key
		if (NULL != window_key->GetWindowFrame())
		{
			window->frameOptions = FRAMEOPTION_NONDEFAULT;
			if (EdxlfsRow == window_frame->ParseDXLFrameSpec())
			{
				window->frameOptions |= FRAMEOPTION_ROWS;
			}
			else
			{
				window->frameOptions |= FRAMEOPTION_RANGE;
			}

			if (window_frame->ParseFrameExclusionStrategy() != EdxlfesNulls)
			{
				GPOS_RAISE(gpdxl::ExmaDXL,
						   gpdxl::ExmiQuery2DXLUnsupportedFeature,
						   GPOS_WSZ_LIT("EXCLUDE clause in window frame"));
			}

			// translate the CDXLNodes representing the leading and trailing edge
			CDXLTranslationContextArray *child_contexts =
				GPOS_NEW(m_mp) CDXLTranslationContextArray(m_mp);
			child_contexts->Append(&child_context);

			CMappingColIdVarPlStmt colid_var_mapping =
				CMappingColIdVarPlStmt(m_mp, NULL, child_contexts,
									   output_context, m_dxl_to_plstmt_context);

			// Translate lead boundary
			//
			// Note that we don't distinguish between the delayed and undelayed
			// versions beoynd this point. Executor will make that decision
			// without our help.
			//
			CDXLNode *win_frame_leading_dxlnode = window_frame->PdxlnLeading();
			EdxlFrameBoundary lead_boundary_type =
				CDXLScalarWindowFrameEdge::Cast(
					win_frame_leading_dxlnode->GetOperator())
					->ParseDXLFrameBoundary();
			if (lead_boundary_type == EdxlfbUnboundedPreceding)
				window->frameOptions |= FRAMEOPTION_END_UNBOUNDED_PRECEDING;
			if (lead_boundary_type == EdxlfbBoundedPreceding)
				window->frameOptions |= FRAMEOPTION_END_OFFSET_PRECEDING;
			if (lead_boundary_type == EdxlfbCurrentRow)
				window->frameOptions |= FRAMEOPTION_END_CURRENT_ROW;
			if (lead_boundary_type == EdxlfbBoundedFollowing)
				window->frameOptions |= FRAMEOPTION_END_OFFSET_FOLLOWING;
			if (lead_boundary_type == EdxlfbUnboundedFollowing)
				window->frameOptions |= FRAMEOPTION_END_UNBOUNDED_FOLLOWING;
			if (lead_boundary_type == EdxlfbDelayedBoundedPreceding)
				window->frameOptions |= FRAMEOPTION_END_OFFSET_PRECEDING;
			if (lead_boundary_type == EdxlfbDelayedBoundedFollowing)
				window->frameOptions |= FRAMEOPTION_END_OFFSET_FOLLOWING;
			if (0 != win_frame_leading_dxlnode->Arity())
			{
				window->endOffset =
					(Node *) m_translator_dxl_to_scalar->TranslateDXLToScalar(
						(*win_frame_leading_dxlnode)[0], &colid_var_mapping);
			}

			// And the same for the trail boundary
			CDXLNode *win_frame_trailing_dxlnode =
				window_frame->PdxlnTrailing();
			EdxlFrameBoundary trail_boundary_type =
				CDXLScalarWindowFrameEdge::Cast(
					win_frame_trailing_dxlnode->GetOperator())
					->ParseDXLFrameBoundary();
			if (trail_boundary_type == EdxlfbUnboundedPreceding)
				window->frameOptions |= FRAMEOPTION_START_UNBOUNDED_PRECEDING;
			if (trail_boundary_type == EdxlfbBoundedPreceding)
				window->frameOptions |= FRAMEOPTION_START_OFFSET_PRECEDING;
			if (trail_boundary_type == EdxlfbCurrentRow)
				window->frameOptions |= FRAMEOPTION_START_CURRENT_ROW;
			if (trail_boundary_type == EdxlfbBoundedFollowing)
				window->frameOptions |= FRAMEOPTION_START_OFFSET_FOLLOWING;
			if (trail_boundary_type == EdxlfbUnboundedFollowing)
				window->frameOptions |= FRAMEOPTION_START_UNBOUNDED_FOLLOWING;
			if (trail_boundary_type == EdxlfbDelayedBoundedPreceding)
				window->frameOptions |= FRAMEOPTION_START_OFFSET_PRECEDING;
			if (trail_boundary_type == EdxlfbDelayedBoundedFollowing)
				window->frameOptions |= FRAMEOPTION_START_OFFSET_FOLLOWING;
			if (0 != win_frame_trailing_dxlnode->Arity())
			{
				window->startOffset =
					(Node *) m_translator_dxl_to_scalar->TranslateDXLToScalar(
						(*win_frame_trailing_dxlnode)[0], &colid_var_mapping);
			}

			// cleanup
			child_contexts->Release();
		}
		else
			window->frameOptions = FRAMEOPTION_DEFAULTS;
	}

	SetParamIds(plan);

	// cleanup
	child_contexts->Release();

	return (Plan *) window;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToPlStmt::TranslateDXLSort
//
//	@doc:
//		Translate DXL sort node into GPDB Sort plan node
//
//---------------------------------------------------------------------------
Plan *
CTranslatorDXLToPlStmt::TranslateDXLSort(
	const CDXLNode *sort_dxlnode, CDXLTranslateContext *output_context,
	CDXLTranslationContextArray *ctxt_translation_prev_siblings)
{
	// create sort plan node
	Sort *sort = MakeNode(Sort);

	Plan *plan = &(sort->plan);
	plan->plan_node_id = m_dxl_to_plstmt_context->GetNextPlanId();

	CDXLPhysicalSort *sort_dxlop =
		CDXLPhysicalSort::Cast(sort_dxlnode->GetOperator());

	// translate operator costs
	TranslatePlanCosts(sort_dxlnode, plan);

	// translate sort child
	CDXLNode *child_dxlnode = (*sort_dxlnode)[EdxlsortIndexChild];
	CDXLNode *project_list_dxlnode = (*sort_dxlnode)[EdxlsortIndexProjList];
	CDXLNode *filter_dxlnode = (*sort_dxlnode)[EdxlsortIndexFilter];

	CDXLTranslateContext child_context(m_mp, false,
									   output_context->GetColIdToParamIdMap());

	Plan *child_plan = TranslateDXLOperatorToPlan(
		child_dxlnode, &child_context, ctxt_translation_prev_siblings);

	CDXLTranslationContextArray *child_contexts =
		GPOS_NEW(m_mp) CDXLTranslationContextArray(m_mp);
	child_contexts->Append(const_cast<CDXLTranslateContext *>(&child_context));

	// translate proj list and filter
	TranslateProjListAndFilter(project_list_dxlnode, filter_dxlnode,
							   NULL,  // translate context for the base table
							   child_contexts, &plan->targetlist, &plan->qual,
							   output_context);

	plan->lefttree = child_plan;

	// set sorting info
	sort->noduplicates = sort_dxlop->FDiscardDuplicates();

	// translate sorting columns

	const CDXLNode *sort_col_list_dxl =
		(*sort_dxlnode)[EdxlsortIndexSortColList];

	const ULONG num_of_cols = sort_col_list_dxl->Arity();
	sort->numCols = num_of_cols;
	sort->sortColIdx =
		(AttrNumber *) px::GPDBAlloc(num_of_cols * sizeof(AttrNumber));
	sort->sortOperators = (Oid *) px::GPDBAlloc(num_of_cols * sizeof(Oid));
	sort->collations = (Oid *) px::GPDBAlloc(num_of_cols * sizeof(Oid));
	sort->nullsFirst = (bool *) px::GPDBAlloc(num_of_cols * sizeof(bool));

	TranslateSortCols(sort_col_list_dxl, &child_context, sort->sortColIdx,
					  sort->sortOperators, sort->collations, sort->nullsFirst);

	SetParamIds(plan);

	// cleanup
	child_contexts->Release();

	return (Plan *) sort;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToPlStmt::TranslateDXLSubQueryScan
//
//	@doc:
//		Translate DXL subquery scan node into GPDB SubqueryScan plan node
//
//---------------------------------------------------------------------------
/*no cover begin*/
Plan *
CTranslatorDXLToPlStmt::TranslateDXLSubQueryScan(
	const CDXLNode *subquery_scan_dxlnode, CDXLTranslateContext *output_context,
	CDXLTranslationContextArray *ctxt_translation_prev_siblings)
{
	// create sort plan node
	SubqueryScan *subquery_scan = MakeNode(SubqueryScan);

	Plan *plan = &(subquery_scan->scan.plan);
	plan->plan_node_id = m_dxl_to_plstmt_context->GetNextPlanId();

	CDXLPhysicalSubqueryScan *subquery_scan_dxlop =
		CDXLPhysicalSubqueryScan::Cast(subquery_scan_dxlnode->GetOperator());

	// translate operator costs
	TranslatePlanCosts(subquery_scan_dxlnode, plan);

	// translate subplan
	CDXLNode *child_dxlnode = (*subquery_scan_dxlnode)[EdxlsubqscanIndexChild];
	CDXLNode *project_list_dxlnode =
		(*subquery_scan_dxlnode)[EdxlsubqscanIndexProjList];
	CDXLNode *filter_dxlnode =
		(*subquery_scan_dxlnode)[EdxlsubqscanIndexFilter];

	CDXLTranslateContext child_context(m_mp, false,
									   output_context->GetColIdToParamIdMap());

	Plan *child_plan = TranslateDXLOperatorToPlan(
		child_dxlnode, &child_context, ctxt_translation_prev_siblings);

	// create an rtable entry for the subquery scan
	RangeTblEntry *rte = MakeNode(RangeTblEntry);
	rte->rtekind = RTE_SUBQUERY;

	Alias *alias = MakeNode(Alias);
	alias->colnames = NIL;

	// get table alias
	alias->aliasname = CTranslatorUtils::CreateMultiByteCharStringFromWCString(
		subquery_scan_dxlop->MdName()->GetMDName()->GetBuffer());

	// get column names from child project list
	CDXLTranslateContextBaseTable base_table_context(m_mp);

	Index index =
		px::ListLength(m_dxl_to_plstmt_context->GetRTableEntriesList()) + 1;
	(subquery_scan->scan).scanrelid = index;
	base_table_context.SetRelIndex(index);

	ListCell *lc_tgtentry = NULL;

	CDXLNode *child_proj_list_dxlnode = (*child_dxlnode)[0];

	ULONG ul = 0;

	ForEach(lc_tgtentry, child_plan->targetlist)
	{
		TargetEntry *target_entry = (TargetEntry *) lfirst(lc_tgtentry);

		// non-system attribute
		CHAR *col_name_char_array = PStrDup(target_entry->resname);
		Value *val_colname = px::MakeStringValue(col_name_char_array);
		alias->colnames = px::LAppend(alias->colnames, val_colname);

		// get corresponding child project element
		CDXLScalarProjElem *sc_proj_elem_dxlop = CDXLScalarProjElem::Cast(
			(*child_proj_list_dxlnode)[ul]->GetOperator());

		// save mapping col id -> index in translate context
		(void) base_table_context.InsertMapping(sc_proj_elem_dxlop->Id(),
												target_entry->resno);
		ul++;
	}

	rte->eref = alias;

	// add range table entry for the subquery to the list
	m_dxl_to_plstmt_context->AddRTE(rte);

	// translate proj list and filter
	TranslateProjListAndFilter(
		project_list_dxlnode, filter_dxlnode,
		&base_table_context,  // translate context for the base table
		NULL, &plan->targetlist, &plan->qual, output_context);

	subquery_scan->subplan = child_plan;

	SetParamIds(plan);
	return (Plan *) subquery_scan;
}
/*no cover end*/

static bool
ContainsSetReturningFuncOrOp(const CDXLNode *project_list_dxlnode,
							 CMDAccessor *md_accessor)
{
	const ULONG arity = project_list_dxlnode->Arity();
	for (ULONG ul = 0; ul < arity; ++ul)
	{
		CDXLNode *proj_elem_dxlnode = (*project_list_dxlnode)[ul];
		GPOS_ASSERT(EdxlopScalarProjectElem ==
					proj_elem_dxlnode->GetOperator()->GetDXLOperator());
		GPOS_ASSERT(1 == proj_elem_dxlnode->Arity());

		// translate proj element expression
		CDXLNode *expr_dxlnode = (*proj_elem_dxlnode)[0];

		CDXLOperator *op = expr_dxlnode->GetOperator();
		switch (op->GetDXLOperator())
		{
			case EdxlopScalarFuncExpr:
				if (CDXLScalarFuncExpr::Cast(op)->ReturnsSet())
					return true;
				break;
			case EdxlopScalarOpExpr:
			{
				const IMDScalarOp *md_sclar_op = md_accessor->RetrieveScOp(
					CDXLScalarOpExpr::Cast(op)->MDId());
				const IMDFunction *md_func =
					md_accessor->RetrieveFunc(md_sclar_op->FuncMdId());
				if (md_func->ReturnsSet())
					return true;
				break;
			}
			default:
				break;
		}
	}
	return false;
}

// GPDB_12_MERGE_FIXME: this duplicates a check in ExecInitProjectSet
static bool
SanityCheckProjectSetTargetList(List *targetlist)
{
	ListCell *lc;
	ForEach(lc, targetlist)
	{
		TargetEntry *te = (TargetEntry *) lfirst(lc);
		Expr *expr = te->expr;
		List *args;
		if ((IsA(expr, FuncExpr) && ((FuncExpr *) expr)->funcretset) ||
			(IsA(expr, OpExpr) && ((OpExpr *) expr)->opretset))
		{
			if (IsA(expr, FuncExpr))
				args = ((FuncExpr *) expr)->args;
			else
				args = ((OpExpr *) expr)->args;
			if (px::ExpressionReturnsSet((Node *) args))
				return false;
			continue;
		}

		if (px::ExpressionReturnsSet((Node *) expr))
			return false;
	}
	return true;
}

// XXX: this is a copy-pasta of TranslateDXLResult
// Is there a way to reduce the duplication?
Plan *
CTranslatorDXLToPlStmt::TranslateDXLProjectSet(
	const CDXLNode *result_dxlnode, CDXLTranslateContext *output_context,
	CDXLTranslationContextArray *ctxt_translation_prev_siblings)
{
	// GPDB_12_MERGE_FIXME: had we generated a DXLProjectSet in ORCA we wouldn't
	// have needed to be defensive here...
	if ((*result_dxlnode)[EdxlresultIndexFilter]->Arity() > 0)
		GPOS_RAISE(
			gpdxl::ExmaDXL, gpdxl::ExmiQuery2DXLUnsupportedFeature,
			GPOS_WSZ_LIT("Unsupported one-time filter in ProjectSet node"));

	// create project set (nee result) plan node
	ProjectSet *project_set = MakeNode(ProjectSet);

	Plan *plan = &(project_set->plan);
	plan->plan_node_id = m_dxl_to_plstmt_context->GetNextPlanId();

	// translate operator costs
	TranslatePlanCosts(result_dxlnode, plan);

	CDXLNode *child_dxlnode = NULL;
	CDXLTranslateContext child_context(m_mp, false,
									   output_context->GetColIdToParamIdMap());

	if (result_dxlnode->Arity() - 1 == EdxlresultIndexChild)
	{
		// translate child plan
		child_dxlnode = (*result_dxlnode)[EdxlresultIndexChild];

		Plan *child_plan = TranslateDXLOperatorToPlan(
			child_dxlnode, &child_context, ctxt_translation_prev_siblings);

		GPOS_ASSERT(NULL != child_plan && "child plan cannot be NULL");

		project_set->plan.lefttree = child_plan;
	}

	CDXLNode *project_list_dxlnode = (*result_dxlnode)[EdxlresultIndexProjList];
	CDXLNode *filter_dxlnode = (*result_dxlnode)[EdxlresultIndexFilter];

	List *quals_list = NULL;

	CDXLTranslationContextArray *child_contexts =
		GPOS_NEW(m_mp) CDXLTranslationContextArray(m_mp);
	child_contexts->Append(const_cast<CDXLTranslateContext *>(&child_context));

	// translate proj list and filter
	TranslateProjListAndFilter(project_list_dxlnode, filter_dxlnode,
							   NULL,  // translate context for the base table
							   child_contexts, &plan->targetlist, &quals_list,
							   output_context);


	plan->qual = quals_list;

	SetParamIds(plan);

	// cleanup
	child_contexts->Release();

	// double check the targetlist is kosher
	// we are only doing this because ORCA didn't do it...
	if (!SanityCheckProjectSetTargetList(plan->targetlist))
		GPOS_RAISE(
			gpdxl::ExmaDXL, gpdxl::ExmiQuery2DXLUnsupportedFeature,
			GPOS_WSZ_LIT("Unexpected target list entries in ProjectSet node"));

	return (Plan *) project_set;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToPlStmt::TranslateDXLResult
//
//	@doc:
//		Translate DXL result node into GPDB result plan node
//
//---------------------------------------------------------------------------
Plan *
CTranslatorDXLToPlStmt::TranslateDXLResult(
	const CDXLNode *result_dxlnode, CDXLTranslateContext *output_context,
	CDXLTranslationContextArray *ctxt_translation_prev_siblings)
{
	// GPDB_12_MERGE_FIXME: this *really* should be done inside ORCA
	// at the latest during CTranslatorExprToDXL, to create a DXLProjectSet
	// that way we don't have to "frisk" the DXLResult to distinguish it from an
	// actual result node
	if (ContainsSetReturningFuncOrOp((*result_dxlnode)[EdxlresultIndexProjList],
									 m_md_accessor))
		return TranslateDXLProjectSet(result_dxlnode, output_context,
									  ctxt_translation_prev_siblings);

	// create result plan node
	Result *result = MakeNode(Result);

	Plan *plan = &(result->plan);
	plan->plan_node_id = m_dxl_to_plstmt_context->GetNextPlanId();

	// translate operator costs
	TranslatePlanCosts(result_dxlnode, plan);

	CDXLNode *child_dxlnode = NULL;
	CDXLTranslateContext child_context(m_mp, false,
									   output_context->GetColIdToParamIdMap());

	if (result_dxlnode->Arity() - 1 == EdxlresultIndexChild)
	{
		// translate child plan
		child_dxlnode = (*result_dxlnode)[EdxlresultIndexChild];

		Plan *child_plan = TranslateDXLOperatorToPlan(
			child_dxlnode, &child_context, ctxt_translation_prev_siblings);

		GPOS_ASSERT(NULL != child_plan && "child plan cannot be NULL");

		result->plan.lefttree = child_plan;
	}

	CDXLNode *project_list_dxlnode = (*result_dxlnode)[EdxlresultIndexProjList];
	CDXLNode *filter_dxlnode = (*result_dxlnode)[EdxlresultIndexFilter];
	CDXLNode *one_time_filter_dxlnode =
		(*result_dxlnode)[EdxlresultIndexOneTimeFilter];

	List *quals_list = NULL;

	CDXLTranslationContextArray *child_contexts =
		GPOS_NEW(m_mp) CDXLTranslationContextArray(m_mp);
	child_contexts->Append(const_cast<CDXLTranslateContext *>(&child_context));

	// translate proj list and filter
	TranslateProjListAndFilter(project_list_dxlnode, filter_dxlnode,
							   NULL,  // translate context for the base table
							   child_contexts, &plan->targetlist, &quals_list,
							   output_context);

	// translate one time filter
	List *one_time_quals_list =
		TranslateDXLFilterToQual(one_time_filter_dxlnode,
								 NULL,	// base table translation context
								 child_contexts, output_context);

	plan->qual = quals_list;

	result->resconstantqual = (Node *) one_time_quals_list;

	SetParamIds(plan);

	// cleanup
	child_contexts->Release();

	// double check the targetlist is kosher
	// we are only doing this because ORCA didn't do it...
	if (!SanityCheckProjectSetTargetList(plan->targetlist))
		GPOS_RAISE(
			gpdxl::ExmaDXL, gpdxl::ExmiQuery2DXLUnsupportedFeature,
			GPOS_WSZ_LIT("Unexpected target list entries in ProjectSet node"));

	return (Plan *) result;
}


//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToPlStmt::TranslateDXLPartSelector
//
//	@doc:
//		Translate DXL PartitionSelector into a GPDB PartitionSelector node
//
//---------------------------------------------------------------------------
Plan *
CTranslatorDXLToPlStmt::TranslateDXLPartSelector(
	const CDXLNode *partition_selector_dxlnode,
	CDXLTranslateContext *output_context,
	CDXLTranslationContextArray *ctxt_translation_prev_siblings)
{
	PartitionSelector *partition_selector = MakeNode(PartitionSelector);

	Plan *plan = &(partition_selector->plan);
	plan->plan_node_id = m_dxl_to_plstmt_context->GetNextPlanId();
	CDXLPhysicalPartitionSelector *partition_selector_dxlop =
		CDXLPhysicalPartitionSelector::Cast(
			partition_selector_dxlnode->GetOperator());

	TranslatePlanCosts(partition_selector_dxlnode, plan);

	CDXLNode *child_dxlnode = NULL;
	CDXLTranslationContextArray *child_contexts =
		GPOS_NEW(m_mp) CDXLTranslationContextArray(m_mp);

	CDXLTranslateContext child_context(m_mp, false,
									   output_context->GetColIdToParamIdMap());

	// translate child plan
	child_dxlnode = (*partition_selector_dxlnode)[2];

	Plan *child_plan = TranslateDXLOperatorToPlan(
		child_dxlnode, &child_context, ctxt_translation_prev_siblings);
	GPOS_ASSERT(NULL != child_plan && "child plan cannot be NULL");

	partition_selector->plan.lefttree = child_plan;

	child_contexts->Append(&child_context);

	CDXLNode *project_list_dxlnode = (*partition_selector_dxlnode)[0];
	plan->targetlist =
		TranslateDXLProjList(project_list_dxlnode, NULL /*base_table_context*/,
							 child_contexts, output_context);

	CMDIdGPDB *mdid =
		CMDIdGPDB::CastMdid(partition_selector_dxlop->GetRelMdId());
	px::RelationWrapper relation = px::GetRelation(mdid->Oid());

	CMappingColIdVarPlStmt colid_var_mapping = CMappingColIdVarPlStmt(
		m_mp, NULL /*base_table_context*/, child_contexts, output_context,
		m_dxl_to_plstmt_context);

	// paramid
	OID oid_type =
		CMDIdGPDB::CastMdid(m_md_accessor->PtMDType<IMDTypeInt4>()->MDId())
			->Oid();
	partition_selector->paramid =
		m_dxl_to_plstmt_context->GetParamIdForSelector(
			oid_type, partition_selector_dxlop->SelectorId());

	// search the rtable for rtindex
	// an Append node on the outer side of a parent HashJoin would already have
	// beeen translated and would have populated the rtable with the root RTE
	Index rtindex = m_dxl_to_plstmt_context->FindRTE(mdid->Oid());
	GPOS_ASSERT(rtindex > 0);

	// part_prune_info
	CDXLNode *filterNode = (*partition_selector_dxlnode)[1];

	ULongPtrArray *part_indexes = partition_selector_dxlop->Partitions();
	List *prune_infos = CPartPruneStepsBuilder::CreatePartPruneInfos(
		filterNode, relation.get(), rtindex, mdid->Oid(), part_indexes,
		&colid_var_mapping, m_translator_dxl_to_scalar);

	partition_selector->part_prune_info = MakeNode(PartitionPruneInfo);
	partition_selector->part_prune_info->prune_infos = prune_infos;

	SetParamIds(plan);
	// cleanup
	child_contexts->Release();

	return (Plan *) partition_selector;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToPlStmt::TranslateDXLAppend
//
//	@doc:
//		Translate DXL append node into GPDB Append plan node
//
//---------------------------------------------------------------------------
Plan *
CTranslatorDXLToPlStmt::TranslateDXLAppend(
	const CDXLNode *append_dxlnode, CDXLTranslateContext *output_context,
	CDXLTranslationContextArray *ctxt_translation_prev_siblings)
{
	// create append plan node
	Append *append = MakeNode(Append);

	Plan *plan = &(append->plan);
	plan->plan_node_id = m_dxl_to_plstmt_context->GetNextPlanId();

	// translate operator costs
	TranslatePlanCosts(append_dxlnode, plan);

	const ULONG arity = append_dxlnode->Arity();
	GPOS_ASSERT(EdxlappendIndexFirstChild < arity);
	append->appendplans = NIL;

	// translate table descriptor into a range table entry
	CDXLPhysicalAppend *phy_append_dxlop =
		CDXLPhysicalAppend::Cast(append_dxlnode->GetOperator());

	// If this append was create from a DynamicTableScan node in ORCA, it will
	// contain the table descriptor of the root partitioned table. Add that to
	// the range table in the PlStmt.
	if (phy_append_dxlop->GetScanId() != gpos::ulong_max)
	{
		GPOS_ASSERT(nullptr != phy_append_dxlop->GetDXLTableDesc());

		// translation context for column mappings in the base relation
		CDXLTranslateContextBaseTable base_table_context(m_mp);
		// we will add the new range table entry as the last element of the range table
		Index index =
			px::ListLength(m_dxl_to_plstmt_context->GetRTableEntriesList()) +
			1;
		RangeTblEntry *rte = TranslateDXLTblDescrToRangeTblEntry(
			phy_append_dxlop->GetDXLTableDesc(), index, &base_table_context);
		GPOS_ASSERT(NULL != rte);
		rte->requiredPerms |= ACL_SELECT;

		m_dxl_to_plstmt_context->AddRTE(rte);

		append->join_prune_paramids = NIL;
		const ULongPtrArray *selector_ids = phy_append_dxlop->GetSelectorIds();
		OID oid_type =
			CMDIdGPDB::CastMdid(m_md_accessor->PtMDType<IMDTypeInt4>()->MDId())
				->Oid();
		for (ULONG ul = 0; ul < selector_ids->Size(); ++ul)
		{
			ULONG selector_id = *(*selector_ids)[ul];
			ULONG param_id = m_dxl_to_plstmt_context->GetParamIdForSelector(
				oid_type, selector_id);
			append->join_prune_paramids =
				px::LAppendInt(append->join_prune_paramids, param_id);
		}
	}

	// translate children
	CDXLTranslateContext child_context(m_mp, false,
									   output_context->GetColIdToParamIdMap());
	for (ULONG ul = EdxlappendIndexFirstChild; ul < arity; ul++)
	{
		CDXLNode *child_dxlnode = (*append_dxlnode)[ul];

		Plan *child_plan = TranslateDXLOperatorToPlan(
			child_dxlnode, &child_context, ctxt_translation_prev_siblings);

		GPOS_ASSERT(NULL != child_plan && "child plan cannot be NULL");

		append->appendplans = px::LAppend(append->appendplans, child_plan);
	}

	CDXLNode *project_list_dxlnode = (*append_dxlnode)[EdxlappendIndexProjList];
	CDXLNode *filter_dxlnode = (*append_dxlnode)[EdxlappendIndexFilter];

	plan->targetlist = NIL;
	const ULONG length = project_list_dxlnode->Arity();
	for (ULONG ul = 0; ul < length; ++ul)
	{
		CDXLNode *proj_elem_dxlnode = (*project_list_dxlnode)[ul];
		GPOS_ASSERT(EdxlopScalarProjectElem ==
					proj_elem_dxlnode->GetOperator()->GetDXLOperator());

		CDXLScalarProjElem *sc_proj_elem_dxlop =
			CDXLScalarProjElem::Cast(proj_elem_dxlnode->GetOperator());
		GPOS_ASSERT(1 == proj_elem_dxlnode->Arity());

		// translate proj element expression
		CDXLNode *expr_dxlnode = (*proj_elem_dxlnode)[0];
		CDXLScalarIdent *sc_ident_dxlop =
			CDXLScalarIdent::Cast(expr_dxlnode->GetOperator());

		Index idxVarno = OUTER_VAR;
		AttrNumber attno = (AttrNumber)(ul + 1);

		Var *var = px::MakeVar(
			idxVarno, attno,
			CMDIdGPDB::CastMdid(sc_ident_dxlop->MdidType())->Oid(),
			sc_ident_dxlop->TypeModifier(),
			0  // varlevelsup
		);

		TargetEntry *target_entry = MakeNode(TargetEntry);
		target_entry->expr = (Expr *) var;
		target_entry->resname =
			CTranslatorUtils::CreateMultiByteCharStringFromWCString(
				sc_proj_elem_dxlop->GetMdNameAlias()->GetMDName()->GetBuffer());
		target_entry->resno = attno;

		// add column mapping to output translation context
		output_context->InsertMapping(sc_proj_elem_dxlop->Id(), target_entry);

		plan->targetlist = px::LAppend(plan->targetlist, target_entry);
	}

	CDXLTranslationContextArray *child_contexts =
		GPOS_NEW(m_mp) CDXLTranslationContextArray(m_mp);
	child_contexts->Append(const_cast<CDXLTranslateContext *>(output_context));

	// translate filter
	plan->qual =
		TranslateDXLFilterToQual(filter_dxlnode,
								 NULL,	// translate context for the base table
								 child_contexts, output_context);

	SetParamIds(plan);

	// cleanup
	child_contexts->Release();

	return (Plan *) append;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToPlStmt::TranslateDXLMaterialize
//
//	@doc:
//		Translate DXL materialize node into GPDB Material plan node
//
//---------------------------------------------------------------------------
Plan *
CTranslatorDXLToPlStmt::TranslateDXLMaterialize(
	const CDXLNode *materialize_dxlnode, CDXLTranslateContext *output_context,
	CDXLTranslationContextArray *ctxt_translation_prev_siblings)
{
	// create materialize plan node
	Material *materialize = MakeNode(Material);

	Plan *plan = &(materialize->plan);
	plan->plan_node_id = m_dxl_to_plstmt_context->GetNextPlanId();

	CDXLPhysicalMaterialize *materialize_dxlop =
		CDXLPhysicalMaterialize::Cast(materialize_dxlnode->GetOperator());

	materialize->px_strict = materialize_dxlop->IsEager();
	// ensure that executor actually materializes results
	materialize->px_shield_child_from_rescans = true;

	// translate operator costs
	TranslatePlanCosts(materialize_dxlnode, plan);

	// translate materialize child
	CDXLNode *child_dxlnode = (*materialize_dxlnode)[EdxlmatIndexChild];

	CDXLNode *project_list_dxlnode =
		(*materialize_dxlnode)[EdxlmatIndexProjList];
	CDXLNode *filter_dxlnode = (*materialize_dxlnode)[EdxlmatIndexFilter];

	CDXLTranslateContext child_context(m_mp, false,
									   output_context->GetColIdToParamIdMap());

	Plan *child_plan = TranslateDXLOperatorToPlan(
		child_dxlnode, &child_context, ctxt_translation_prev_siblings);

	CDXLTranslationContextArray *child_contexts =
		GPOS_NEW(m_mp) CDXLTranslationContextArray(m_mp);
	child_contexts->Append(const_cast<CDXLTranslateContext *>(&child_context));

	// translate proj list and filter
	TranslateProjListAndFilter(project_list_dxlnode, filter_dxlnode,
							   NULL,  // translate context for the base table
							   child_contexts, &plan->targetlist, &plan->qual,
							   output_context);

	plan->lefttree = child_plan;

	SetParamIds(plan);

	// cleanup
	child_contexts->Release();

	return (Plan *) materialize;
}


//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToPlStmt::TranslateDXLCTEProducerToSharedScan
//
//	@doc:
//		Translate DXL CTE Producer node into GPDB share input scan plan node
//
//---------------------------------------------------------------------------
Plan *
CTranslatorDXLToPlStmt::TranslateDXLCTEProducerToSharedScan(
	const CDXLNode *cte_producer_dxlnode, CDXLTranslateContext *output_context,
	CDXLTranslationContextArray *ctxt_translation_prev_siblings)
{
	CDXLPhysicalCTEProducer *cte_prod_dxlop =
		CDXLPhysicalCTEProducer::Cast(cte_producer_dxlnode->GetOperator());
	ULONG cte_id = cte_prod_dxlop->Id();

	// create the shared input scan representing the CTE Producer
	ShareInputScan *shared_input_scan = MakeNode(ShareInputScan);
	shared_input_scan->share_id = cte_id;
	Plan *plan = &(shared_input_scan->scan.plan);
	plan->plan_node_id = m_dxl_to_plstmt_context->GetNextPlanId();

	// store share scan node for the translation of CTE Consumers
	m_dxl_to_plstmt_context->AddCTEConsumerInfo(cte_id, shared_input_scan);

	// translate cost of the producer
	TranslatePlanCosts(cte_producer_dxlnode, plan);

	// translate child plan
	CDXLNode *project_list_dxlnode = (*cte_producer_dxlnode)[0];
	CDXLNode *child_dxlnode = (*cte_producer_dxlnode)[1];

	CDXLTranslateContext child_context(m_mp, false,
									   output_context->GetColIdToParamIdMap());
	Plan *child_plan = TranslateDXLOperatorToPlan(
		child_dxlnode, &child_context, ctxt_translation_prev_siblings);
	GPOS_ASSERT(NULL != child_plan && "child plan cannot be NULL");

	CDXLTranslationContextArray *child_contexts =
		GPOS_NEW(m_mp) CDXLTranslationContextArray(m_mp);
	child_contexts->Append(&child_context);
	// translate proj list
	plan->targetlist =
		TranslateDXLProjList(project_list_dxlnode,
							 NULL,	// base table translation context
							 child_contexts, output_context);

	plan->lefttree = child_plan;
	plan->qual = NIL;
	SetParamIds(plan);

	// cleanup
	child_contexts->Release();

	return (Plan *) shared_input_scan;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToPlStmt::TranslateDXLCTEConsumerToSharedScan
//
//	@doc:
//		Translate DXL CTE Consumer node into GPDB share input scan plan node
//
//---------------------------------------------------------------------------
Plan *
CTranslatorDXLToPlStmt::TranslateDXLCTEConsumerToSharedScan(
	const CDXLNode *cte_consumer_dxlnode, CDXLTranslateContext *output_context,
	CDXLTranslationContextArray *ctxt_translation_prev_siblings)
{
	CDXLPhysicalCTEConsumer *cte_consumer_dxlop =
		CDXLPhysicalCTEConsumer::Cast(cte_consumer_dxlnode->GetOperator());
	ULONG cte_id = cte_consumer_dxlop->Id();

	ShareInputScan *share_input_scan_cte_consumer = MakeNode(ShareInputScan);
	share_input_scan_cte_consumer->share_id = cte_id;

	Plan *plan = &(share_input_scan_cte_consumer->scan.plan);
	plan->plan_node_id = m_dxl_to_plstmt_context->GetNextPlanId();

	// translate operator costs
	TranslatePlanCosts(cte_consumer_dxlnode, plan);

#ifdef GPOS_DEBUG
	ULongPtrArray *output_colids_array =
		cte_consumer_dxlop->GetOutputColIdsArray();
#endif

	// generate the target list of the CTE Consumer
	plan->targetlist = NIL;
	CDXLNode *project_list_dxlnode = (*cte_consumer_dxlnode)[0];
	const ULONG num_of_proj_list_elem = project_list_dxlnode->Arity();
	GPOS_ASSERT(num_of_proj_list_elem == output_colids_array->Size());
	for (ULONG ul = 0; ul < num_of_proj_list_elem; ul++)
	{
		CDXLNode *proj_elem_dxlnode = (*project_list_dxlnode)[ul];
		CDXLScalarProjElem *sc_proj_elem_dxlop =
			CDXLScalarProjElem::Cast(proj_elem_dxlnode->GetOperator());
		ULONG colid = sc_proj_elem_dxlop->Id();
		GPOS_ASSERT(colid == *(*output_colids_array)[ul]);

		CDXLNode *sc_ident_dxlnode = (*proj_elem_dxlnode)[0];
		CDXLScalarIdent *sc_ident_dxlop =
			CDXLScalarIdent::Cast(sc_ident_dxlnode->GetOperator());
		OID oid_type = CMDIdGPDB::CastMdid(sc_ident_dxlop->MdidType())->Oid();

		Var *var =
			px::MakeVar(OUTER_VAR, (AttrNumber)(ul + 1), oid_type,
						  sc_ident_dxlop->TypeModifier(), 0 /* varlevelsup */);

		CHAR *resname = CTranslatorUtils::CreateMultiByteCharStringFromWCString(
			sc_proj_elem_dxlop->GetMdNameAlias()->GetMDName()->GetBuffer());
		TargetEntry *target_entry = px::MakeTargetEntry(
			(Expr *) var, (AttrNumber)(ul + 1), resname, false /* resjunk */);
		plan->targetlist = px::LAppend(plan->targetlist, target_entry);

		output_context->InsertMapping(colid, target_entry);
	}

	plan->qual = NULL;

	SetParamIds(plan);

	// store share scan node for the translation of CTE Consumers
	m_dxl_to_plstmt_context->AddCTEConsumerInfo(cte_id,
												share_input_scan_cte_consumer);

	return (Plan *) share_input_scan_cte_consumer;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToPlStmt::TranslateDXLSequence
//
//	@doc:
//		Translate DXL sequence node into GPDB Sequence plan node
//
//---------------------------------------------------------------------------
/*no cover begin*/
Plan *
CTranslatorDXLToPlStmt::TranslateDXLSequence(
	const CDXLNode *sequence_dxlnode, CDXLTranslateContext *output_context,
	CDXLTranslationContextArray *ctxt_translation_prev_siblings)
{
	// create append plan node
	Sequence *psequence = MakeNode(Sequence);

	Plan *plan = &(psequence->plan);
	plan->plan_node_id = m_dxl_to_plstmt_context->GetNextPlanId();

	// translate operator costs
	TranslatePlanCosts(sequence_dxlnode, plan);

	ULONG arity = sequence_dxlnode->Arity();

	CDXLTranslateContext child_context(m_mp, false,
									   output_context->GetColIdToParamIdMap());

	for (ULONG ul = 1; ul < arity; ul++)
	{
		CDXLNode *child_dxlnode = (*sequence_dxlnode)[ul];

		Plan *child_plan = TranslateDXLOperatorToPlan(
			child_dxlnode, &child_context, ctxt_translation_prev_siblings);

		psequence->subplans = px::LAppend(psequence->subplans, child_plan);
	}

	CDXLNode *project_list_dxlnode = (*sequence_dxlnode)[0];

	CDXLTranslationContextArray *child_contexts =
		GPOS_NEW(m_mp) CDXLTranslationContextArray(m_mp);
	child_contexts->Append(const_cast<CDXLTranslateContext *>(&child_context));

	// translate proj list
	plan->targetlist =
		TranslateDXLProjList(project_list_dxlnode,
							 NULL,	// base table translation context
							 child_contexts, output_context);

	SetParamIds(plan);

	// cleanup
	child_contexts->Release();

	return (Plan *) psequence;
}
/*no cover end*/

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToPlStmt::TranslateDXLDirectDispatchInfo
//
//	@doc:
//		Translate the direct dispatch info
//
//---------------------------------------------------------------------------
/*no cover begin*/
List *
CTranslatorDXLToPlStmt::TranslateDXLDirectDispatchInfo(
	CDXLDirectDispatchInfo *dxl_direct_dispatch_info)
{
	if (!px_optimizer_enable_direct_dispatch || NULL == dxl_direct_dispatch_info)
	{
		return NIL;
	}

	CDXLDatum2dArray *dispatch_identifier_datum_arrays =
		dxl_direct_dispatch_info->GetDispatchIdentifierDatumArray();

	if (dispatch_identifier_datum_arrays == NULL ||
		0 == dispatch_identifier_datum_arrays->Size())
	{
		return NIL;
	}

	CDXLDatumArray *dxl_datum_array = (*dispatch_identifier_datum_arrays)[0];
	GPOS_ASSERT(0 < dxl_datum_array->Size());

	const ULONG length = dispatch_identifier_datum_arrays->Size();

	if (dxl_direct_dispatch_info->FContainsRawValues())
	{
		List *segids_list = NIL;
		INT segid = -1;
		Const *const_expr = NULL;

		for (ULONG ul = 0; ul < length; ul++)
		{
			CDXLDatumArray *dispatch_identifier_datum_array =
				(*dispatch_identifier_datum_arrays)[ul];
			GPOS_ASSERT(1 == dispatch_identifier_datum_array->Size());
			const_expr =
				(Const *) m_translator_dxl_to_scalar->TranslateDXLDatumToScalar(
					(*dispatch_identifier_datum_array)[0]);

			segid = DatumGetInt32(const_expr->constvalue);
			if (segid >= -1 && segid < (INT) m_num_of_segments)
			{
				segids_list = px::LAppendInt(segids_list, segid);
			}
		}

		if (segids_list == NIL && const_expr)
		{
			// If no valid segids were found, and there were items in the
			// dispatch identifier array, then append the last item to behave
			// in same manner as Planner for consistency. Currently this will
			// lead to a FATAL in the backend when we dispatch.
			segids_list = px::LAppendInt(segids_list, segid);
		}
		return segids_list;
	}

	ULONG hash_code = GetDXLDatumGPDBHash(dxl_datum_array);
	for (ULONG ul = 0; ul < length; ul++)
	{
		CDXLDatumArray *dispatch_identifier_datum_array =
			(*dispatch_identifier_datum_arrays)[ul];
		GPOS_ASSERT(0 < dispatch_identifier_datum_array->Size());
		ULONG hash_code_new =
			GetDXLDatumGPDBHash(dispatch_identifier_datum_array);

		if (hash_code != hash_code_new)
		{
			// values don't hash to the same segment
			return NIL;
		}
	}

	List *segids_list = px::LAppendInt(NIL, hash_code);
	return segids_list;
}
/*no cover end*/

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToPlStmt::GetDXLDatumGPDBHash
//
//	@doc:
//		Hash a DXL datum
//
//---------------------------------------------------------------------------
ULONG
CTranslatorDXLToPlStmt::GetDXLDatumGPDBHash(CDXLDatumArray *dxl_datum_array)
{
	List *consts_list = NIL;
	Oid *hashfuncs;

	const ULONG length = dxl_datum_array->Size();

	hashfuncs = (Oid *) px::GPDBAlloc(length * sizeof(Oid));

	for (ULONG ul = 0; ul < length; ul++)
	{
		CDXLDatum *datum_dxl = (*dxl_datum_array)[ul];

		Const *const_expr =
			(Const *) m_translator_dxl_to_scalar->TranslateDXLDatumToScalar(
				datum_dxl);
		consts_list = px::LAppend(consts_list, const_expr);
		hashfuncs[ul] = m_dxl_to_plstmt_context->GetDistributionHashFuncForType(
			const_expr->consttype);
	}

	ULONG hash =
		px::PxHashConstList(consts_list, m_num_of_segments, hashfuncs);

	px::ListFreeDeep(consts_list);
	px::GPDBFree(hashfuncs);

	return hash;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToPlStmt::TranslateDXLSplit
//
//	@doc:
//		Translates a DXL Split node
//
//---------------------------------------------------------------------------
Plan *
CTranslatorDXLToPlStmt::TranslateDXLSplit(
	const CDXLNode *split_dxlnode, CDXLTranslateContext *output_context,
	CDXLTranslationContextArray *ctxt_translation_prev_siblings)
{
	CDXLPhysicalSplit *phy_split_dxlop =
		CDXLPhysicalSplit::Cast(split_dxlnode->GetOperator());

	// create SplitUpdate node
	SplitUpdate *split = MakeNode(SplitUpdate);
	Plan *plan = &(split->plan);

	CDXLNode *project_list_dxlnode = (*split_dxlnode)[0];
	CDXLNode *child_dxlnode = (*split_dxlnode)[1];

	CDXLTranslateContext child_context(m_mp, false,
									   output_context->GetColIdToParamIdMap());

	Plan *child_plan = TranslateDXLOperatorToPlan(
		child_dxlnode, &child_context, ctxt_translation_prev_siblings);

	CDXLTranslationContextArray *child_contexts =
		GPOS_NEW(m_mp) CDXLTranslationContextArray(m_mp);
	child_contexts->Append(&child_context);

	// translate proj list and filter
	plan->targetlist =
		TranslateDXLProjList(project_list_dxlnode,
							 nullptr,  // translate context for the base table
							 child_contexts, output_context);

	// translate delete and insert columns
	ULongPtrArray *deletion_colid_array =
		phy_split_dxlop->GetDeletionColIdArray();
	ULongPtrArray *insertion_colid_array =
		phy_split_dxlop->GetInsertionColIdArray();

	GPOS_ASSERT(insertion_colid_array->Size() == deletion_colid_array->Size());

	split->deleteColIdx = CTranslatorUtils::ConvertColidToAttnos(
		deletion_colid_array, &child_context);
	split->insertColIdx = CTranslatorUtils::ConvertColidToAttnos(
		insertion_colid_array, &child_context);

	const TargetEntry *te_action_col =
		output_context->GetTargetEntry(phy_split_dxlop->ActionColId());
	const TargetEntry *te_tuple_oid_col =
		output_context->GetTargetEntry(phy_split_dxlop->GetTupleOid());

	if (nullptr == te_action_col)
	{
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXL2PlStmtAttributeNotFound,
				   phy_split_dxlop->ActionColId());
	}

	split->actionColIdx = te_action_col->resno;

	split->tupleoidColIdx = FirstLowInvalidHeapAttributeNumber;
	if (nullptr != te_tuple_oid_col)
	{
		split->tupleoidColIdx = te_tuple_oid_col->resno;
	}

	plan->lefttree = child_plan;
	plan->plan_node_id = m_dxl_to_plstmt_context->GetNextPlanId();

	SetParamIds(plan);

	// cleanup
	child_contexts->Release();

	// translate operator costs
	TranslatePlanCosts(split_dxlnode, plan);

	return (Plan *) split;
}


//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToPlStmt::TranslateDXLAssertConstraints
//
//	@doc:
//		Translate the constraints from an Assert node into a list of quals
//
//---------------------------------------------------------------------------
List *
CTranslatorDXLToPlStmt::TranslateDXLAssertConstraints(
	CDXLNode *assert_contraint_list_dxlnode,
	CDXLTranslateContext *output_context,
	CDXLTranslationContextArray *child_contexts)
{
	List *quals_list = NIL;

	// build colid->var mapping
	CMappingColIdVarPlStmt colid_var_mapping(
		m_mp, nullptr /*base_table_context*/, child_contexts, output_context,
		m_dxl_to_plstmt_context);

	const ULONG arity = assert_contraint_list_dxlnode->Arity();
	for (ULONG ul = 0; ul < arity; ul++)
	{
		CDXLNode *assert_contraint_dxlnode =
			(*assert_contraint_list_dxlnode)[ul];
		Expr *assert_contraint_expr =
			m_translator_dxl_to_scalar->TranslateDXLToScalar(
				(*assert_contraint_dxlnode)[0], &colid_var_mapping);
		quals_list = px::LAppend(quals_list, assert_contraint_expr);
	}

	return quals_list;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToPlStmt::TranslateDXLAssert
//
//	@doc:
//		Translate DXL assert node into GPDB assert plan node
//
//---------------------------------------------------------------------------
Plan *
CTranslatorDXLToPlStmt::TranslateDXLAssert(
	const CDXLNode *assert_dxlnode, CDXLTranslateContext *output_context,
	CDXLTranslationContextArray *ctxt_translation_prev_siblings)
{
	// create assert plan node
	AssertOp *assert_node = MakeNode(AssertOp);

	Plan *plan = &(assert_node->plan);
	plan->plan_node_id = m_dxl_to_plstmt_context->GetNextPlanId();

	CDXLPhysicalAssert *assert_dxlop =
		CDXLPhysicalAssert::Cast(assert_dxlnode->GetOperator());

	// translate error code into the its internal GPDB representation
	const CHAR *error_code = assert_dxlop->GetSQLState();
	GPOS_ASSERT(GPOS_SQLSTATE_LENGTH == clib::Strlen(error_code));

	assert_node->errcode =
		MAKE_SQLSTATE(error_code[0], error_code[1], error_code[2],
					  error_code[3], error_code[4]);
	CDXLNode *filter_dxlnode =
		(*assert_dxlnode)[CDXLPhysicalAssert::EdxlassertIndexFilter];

	assert_node->errmessage =
		CTranslatorUtils::GetAssertErrorMsgs(filter_dxlnode);

	// translate operator costs
	TranslatePlanCosts(assert_dxlnode, plan);

	CDXLTranslateContext child_context(m_mp, false,
									   output_context->GetColIdToParamIdMap());

	// translate child plan
	CDXLNode *child_dxlnode =
		(*assert_dxlnode)[CDXLPhysicalAssert::EdxlassertIndexChild];
	Plan *child_plan = TranslateDXLOperatorToPlan(
		child_dxlnode, &child_context, ctxt_translation_prev_siblings);

	GPOS_ASSERT(NULL != child_plan && "child plan cannot be NULL");

	assert_node->plan.lefttree = child_plan;

	CDXLNode *project_list_dxlnode =
		(*assert_dxlnode)[CDXLPhysicalAssert::EdxlassertIndexProjList];

	CDXLTranslationContextArray *child_contexts =
		GPOS_NEW(m_mp) CDXLTranslationContextArray(m_mp);
	child_contexts->Append(const_cast<CDXLTranslateContext *>(&child_context));

	// translate proj list
	plan->targetlist =
		TranslateDXLProjList(project_list_dxlnode,
							 NULL,	// translate context for the base table
							 child_contexts, output_context);

	// translate assert constraints
	plan->qual = TranslateDXLAssertConstraints(filter_dxlnode, output_context,
											   child_contexts);

	GPOS_ASSERT(px::ListLength(plan->qual) ==
				px::ListLength(assert_node->errmessage));
	SetParamIds(plan);

	// cleanup
	child_contexts->Release();

	return (Plan *) assert_node;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToPlStmt::TranslateDXLTblDescrToRangeTblEntry
//
//	@doc:
//		Translates a DXL table descriptor into a range table entry. If an index
//		descriptor is provided, we use the mapping from colids to index attnos
//		instead of table attnos
//
//---------------------------------------------------------------------------
RangeTblEntry *
CTranslatorDXLToPlStmt::TranslateDXLTblDescrToRangeTblEntry(
	const CDXLTableDescr *table_descr, Index index,
	CDXLTranslateContextBaseTable *base_table_context)
{
	GPOS_ASSERT(NULL != table_descr);

	const IMDRelation *md_rel = m_md_accessor->RetrieveRel(table_descr->MDId());
	const ULONG num_of_non_sys_cols =
		CTranslatorUtils::GetNumNonSystemColumns(md_rel);

	RangeTblEntry *rte = MakeNode(RangeTblEntry);
	rte->rtekind = RTE_RELATION;

	// get oid for table
	Oid oid = CMDIdGPDB::CastMdid(table_descr->MDId())->Oid();
	GPOS_ASSERT(InvalidOid != oid);

	rte->relid = oid;
	rte->checkAsUser = table_descr->GetExecuteAsUserId();
	rte->requiredPerms |= ACL_NO_RIGHTS;


	// save oid and range index in translation context
	base_table_context->SetOID(oid);
	base_table_context->SetRelIndex(index);

	Alias *alias = MakeNode(Alias);
	alias->colnames = NIL;

	// get table alias
	alias->aliasname = CTranslatorUtils::CreateMultiByteCharStringFromWCString(
		table_descr->MdName()->GetMDName()->GetBuffer());

	// get column names
	const ULONG arity = table_descr->Arity();

	INT last_attno = 0;

	for (ULONG ul = 0; ul < arity; ++ul)
	{
		const CDXLColDescr *dxl_col_descr = table_descr->GetColumnDescrAt(ul);
		GPOS_ASSERT(NULL != dxl_col_descr);

		INT attno = dxl_col_descr->AttrNum();

		GPOS_ASSERT(0 != attno);

		if (0 < attno)
		{
			// if attno > last_attno + 1, there were dropped attributes
			// add those to the RTE as they are required by GPDB
			for (INT dropped_col_attno = last_attno + 1;
				 dropped_col_attno < attno; dropped_col_attno++)
			{
				Value *val_dropped_colname = px::MakeStringValue(PStrDup(""));
				alias->colnames =
					px::LAppend(alias->colnames, val_dropped_colname);
			}

			// non-system attribute
			CHAR *col_name_char_array =
				CTranslatorUtils::CreateMultiByteCharStringFromWCString(
					dxl_col_descr->MdName()->GetMDName()->GetBuffer());
			Value *val_colname = px::MakeStringValue(col_name_char_array);

			alias->colnames = px::LAppend(alias->colnames, val_colname);
			last_attno = attno;
		}

		// save mapping col id -> index in translate context
		(void) base_table_context->InsertMapping(dxl_col_descr->Id(), attno);
	}

	// if there are any dropped columns at the end, add those too to the RangeTblEntry
	for (ULONG ul = last_attno + 1; ul <= num_of_non_sys_cols; ul++)
	{
		Value *val_dropped_colname = px::MakeStringValue(PStrDup(""));
		alias->colnames = px::LAppend(alias->colnames, val_dropped_colname);
	}

	rte->eref = alias;

	return rte;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToPlStmt::TranslateDXLProjList
//
//	@doc:
//		Translates a DXL projection list node into a target list.
//		For base table projection lists, the caller should provide a base table
//		translation context with table oid, rtable index and mappings for the columns.
//		For other nodes translate_ctxt_left and pdxltrctxRight give
//		the mappings of column ids to target entries in the corresponding child nodes
//		for resolving the origin of the target entries
//
//---------------------------------------------------------------------------
List *
CTranslatorDXLToPlStmt::TranslateDXLProjList(
	const CDXLNode *project_list_dxlnode,
	const CDXLTranslateContextBaseTable *base_table_context,
	CDXLTranslationContextArray *child_contexts,
	CDXLTranslateContext *output_context)
{
	if (NULL == project_list_dxlnode)
	{
		return NULL;
	}

	List *target_list = NIL;

	// translate each DXL project element into a target entry
	const ULONG arity = project_list_dxlnode->Arity();
	for (ULONG ul = 0; ul < arity; ++ul)
	{
		CDXLNode *proj_elem_dxlnode = (*project_list_dxlnode)[ul];
		GPOS_ASSERT(EdxlopScalarProjectElem ==
					proj_elem_dxlnode->GetOperator()->GetDXLOperator());
		CDXLScalarProjElem *sc_proj_elem_dxlop =
			CDXLScalarProjElem::Cast(proj_elem_dxlnode->GetOperator());
		GPOS_ASSERT(1 == proj_elem_dxlnode->Arity());

		// translate proj element expression
		CDXLNode *expr_dxlnode = (*proj_elem_dxlnode)[0];

		CMappingColIdVarPlStmt colid_var_mapping =
			CMappingColIdVarPlStmt(m_mp, base_table_context, child_contexts,
								   output_context, m_dxl_to_plstmt_context);

		Expr *expr = m_translator_dxl_to_scalar->TranslateDXLToScalar(
			expr_dxlnode, &colid_var_mapping);

		GPOS_ASSERT(NULL != expr);

		TargetEntry *target_entry = MakeNode(TargetEntry);
		target_entry->expr = expr;
		target_entry->resname =
			CTranslatorUtils::CreateMultiByteCharStringFromWCString(
				sc_proj_elem_dxlop->GetMdNameAlias()->GetMDName()->GetBuffer());
		target_entry->resno = (AttrNumber)(ul + 1);

		if (IsA(expr, Var))
		{
			// check the origin of the left or the right side
			// of the current operator and if it is derived from a base relation,
			// set resorigtbl and resorigcol appropriately

			if (NULL != base_table_context)
			{
				// translating project list of a base table
				target_entry->resorigtbl = base_table_context->GetOid();
				target_entry->resorigcol = ((Var *) expr)->varattno;
			}
			else
			{
				// not translating a base table proj list: variable must come from
				// the left or right child of the operator

				GPOS_ASSERT(NULL != child_contexts);
				GPOS_ASSERT(0 != child_contexts->Size());
				ULONG colid = CDXLScalarIdent::Cast(expr_dxlnode->GetOperator())
								  ->GetDXLColRef()
								  ->Id();

				const CDXLTranslateContext *translate_ctxt_left =
					(*child_contexts)[0];
				GPOS_ASSERT(NULL != translate_ctxt_left);
				const TargetEntry *pteOriginal =
					translate_ctxt_left->GetTargetEntry(colid);

				if (NULL == pteOriginal)
				{
					// variable not found on the left side
					GPOS_ASSERT(2 == child_contexts->Size());
					const CDXLTranslateContext *pdxltrctxRight =
						(*child_contexts)[1];

					GPOS_ASSERT(NULL != pdxltrctxRight);
					pteOriginal = pdxltrctxRight->GetTargetEntry(colid);
				}

				if (NULL == pteOriginal)
				{
					GPOS_RAISE(gpdxl::ExmaDXL,
							   gpdxl::ExmiDXL2PlStmtAttributeNotFound, colid);
				}
				target_entry->resorigtbl = pteOriginal->resorigtbl;
				target_entry->resorigcol = pteOriginal->resorigcol;
			}
		}

		// add column mapping to output translation context
		output_context->InsertMapping(sc_proj_elem_dxlop->Id(), target_entry);

		target_list = px::LAppend(target_list, target_entry);
	}

	return target_list;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToPlStmt::TranslateDXLProjectListToHashTargetList
//
//	@doc:
//		Create a target list for the hash node of a hash join plan node by creating a list
//		of references to the elements in the child project list
//
//---------------------------------------------------------------------------
List *
CTranslatorDXLToPlStmt::TranslateDXLProjectListToHashTargetList(
	const CDXLNode *project_list_dxlnode, CDXLTranslateContext *child_context,
	CDXLTranslateContext *output_context)
{
	List *target_list = NIL;
	const ULONG arity = project_list_dxlnode->Arity();
	for (ULONG ul = 0; ul < arity; ul++)
	{
		CDXLNode *proj_elem_dxlnode = (*project_list_dxlnode)[ul];
		CDXLScalarProjElem *sc_proj_elem_dxlop =
			CDXLScalarProjElem::Cast(proj_elem_dxlnode->GetOperator());

		const TargetEntry *te_child =
			child_context->GetTargetEntry(sc_proj_elem_dxlop->Id());
		if (NULL == te_child)
		{
			GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXL2PlStmtAttributeNotFound,
					   sc_proj_elem_dxlop->Id());
		}

		// get type oid for project element's expression
		GPOS_ASSERT(1 == proj_elem_dxlnode->Arity());

		// find column type
		OID oid_type = px::ExprType((Node *) te_child->expr);
		INT type_modifier = px::ExprTypeMod((Node *) te_child->expr);

		// find the original varno and attno for this column
		Index idx_varnoold = 0;
		AttrNumber attno_old = 0;

		if (IsA(te_child->expr, Var))
		{
			Var *pv = (Var *) te_child->expr;
			idx_varnoold = pv->varnoold;
			attno_old = pv->varoattno;
		}
		else
		{
			idx_varnoold = OUTER_VAR;
			attno_old = te_child->resno;
		}

		// create a Var expression for this target list entry expression
		Var *var =
			px::MakeVar(OUTER_VAR, te_child->resno, oid_type, type_modifier,
						  0	 // varlevelsup
			);

		// set old varno and varattno since makeVar does not set them
		var->varnoold = idx_varnoold;
		var->varoattno = attno_old;

		CHAR *resname = CTranslatorUtils::CreateMultiByteCharStringFromWCString(
			sc_proj_elem_dxlop->GetMdNameAlias()->GetMDName()->GetBuffer());

		TargetEntry *target_entry =
			px::MakeTargetEntry((Expr *) var, (AttrNumber)(ul + 1), resname,
								  false	 // resjunk
			);

		target_list = px::LAppend(target_list, target_entry);
		output_context->InsertMapping(sc_proj_elem_dxlop->Id(), target_entry);
	}

	return target_list;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToPlStmt::TranslateDXLFilterToQual
//
//	@doc:
//		Translates a DXL filter node into a Qual list.
//
//---------------------------------------------------------------------------
List *
CTranslatorDXLToPlStmt::TranslateDXLFilterToQual(
	const CDXLNode *filter_dxlnode,
	const CDXLTranslateContextBaseTable *base_table_context,
	CDXLTranslationContextArray *child_contexts,
	CDXLTranslateContext *output_context)
{
	const ULONG arity = filter_dxlnode->Arity();
	if (0 == arity)
	{
		return NIL;
	}

	GPOS_ASSERT(1 == arity);

	CDXLNode *filter_cond_dxlnode = (*filter_dxlnode)[0];
	GPOS_ASSERT(CTranslatorDXLToScalar::HasBoolResult(filter_cond_dxlnode,
													  m_md_accessor));

	return TranslateDXLScCondToQual(filter_cond_dxlnode, base_table_context,
									child_contexts, output_context);
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToPlStmt::TranslateDXLScCondToQual
//
//	@doc:
//		Translates a DXL scalar condition node node into a Qual list.
//
//---------------------------------------------------------------------------
List *
CTranslatorDXLToPlStmt::TranslateDXLScCondToQual(
	const CDXLNode *condition_dxlnode,
	const CDXLTranslateContextBaseTable *base_table_context,
	CDXLTranslationContextArray *child_contexts,
	CDXLTranslateContext *output_context)
{
	List *quals_list = NIL;

	GPOS_ASSERT(CTranslatorDXLToScalar::HasBoolResult(
		const_cast<CDXLNode *>(condition_dxlnode), m_md_accessor));

	CMappingColIdVarPlStmt colid_var_mapping =
		CMappingColIdVarPlStmt(m_mp, base_table_context, child_contexts,
							   output_context, m_dxl_to_plstmt_context);

	Expr *expr = m_translator_dxl_to_scalar->TranslateDXLToScalar(
		condition_dxlnode, &colid_var_mapping);

	quals_list = px::LAppend(quals_list, expr);

	return quals_list;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToPlStmt::TranslateProjListAndFilter
//
//	@doc:
//		Translates DXL proj list and filter into GPDB's target and qual lists,
//		respectively
//
//---------------------------------------------------------------------------
void
CTranslatorDXLToPlStmt::TranslateProjListAndFilter(
	const CDXLNode *project_list_dxlnode, const CDXLNode *filter_dxlnode,
	const CDXLTranslateContextBaseTable *base_table_context,
	CDXLTranslationContextArray *child_contexts, List **targetlist_out,
	List **qual_out, CDXLTranslateContext *output_context)
{
	// translate proj list
	*targetlist_out = TranslateDXLProjList(
		project_list_dxlnode,
		base_table_context,	 // base table translation context
		child_contexts, output_context);

	// translate filter
	*qual_out = TranslateDXLFilterToQual(
		filter_dxlnode,
		base_table_context,	 // base table translation context
		child_contexts, output_context);
}


//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToPlStmt::TranslateHashExprList
//
//	@doc:
//		Translates DXL hash expression list in a redistribute motion node into
//		GPDB's hash expression and expression types lists, respectively
//
//---------------------------------------------------------------------------
void
CTranslatorDXLToPlStmt::TranslateHashExprList(
	const CDXLNode *hash_expr_list_dxlnode,
	const CDXLTranslateContext *child_context, List **hash_expr_out_list,
	List **hash_expr_opfamilies_out_list, CDXLTranslateContext *output_context)
{
	GPOS_ASSERT(NIL == *hash_expr_out_list);
	GPOS_ASSERT(NIL == *hash_expr_opfamilies_out_list);

	List *hash_expr_list = NIL;

	CDXLTranslationContextArray *child_contexts =
		GPOS_NEW(m_mp) CDXLTranslationContextArray(m_mp);
	child_contexts->Append(child_context);

	const ULONG arity = hash_expr_list_dxlnode->Arity();
	for (ULONG ul = 0; ul < arity; ul++)
	{
		CDXLNode *hash_expr_dxlnode = (*hash_expr_list_dxlnode)[ul];

		GPOS_ASSERT(1 == hash_expr_dxlnode->Arity());
		CDXLNode *expr_dxlnode = (*hash_expr_dxlnode)[0];

		CMappingColIdVarPlStmt colid_var_mapping =
			CMappingColIdVarPlStmt(m_mp, NULL, child_contexts, output_context,
								   m_dxl_to_plstmt_context);

		Expr *expr = m_translator_dxl_to_scalar->TranslateDXLToScalar(
			expr_dxlnode, &colid_var_mapping);

		hash_expr_list = px::LAppend(hash_expr_list, expr);

		GPOS_ASSERT((ULONG) px::ListLength(hash_expr_list) == ul + 1);
	}

	List *hash_expr_opfamilies = NIL;
/*no cover begin*/
	if (GPOS_FTRACE(EopttraceConsiderOpfamiliesForDistribution))
	{
		for (ULONG ul = 0; ul < arity; ul++)
		{
			CDXLNode *hash_expr_dxlnode = (*hash_expr_list_dxlnode)[ul];
			CDXLScalarHashExpr *hash_expr_dxlop =
				CDXLScalarHashExpr::Cast(hash_expr_dxlnode->GetOperator());
			const IMDId *opfamily = hash_expr_dxlop->MdidOpfamily();
			hash_expr_opfamilies = px::LAppendOid(
				hash_expr_opfamilies, CMDIdGPDB::CastMdid(opfamily)->Oid());
		}
	}
/*no cover end*/

	*hash_expr_out_list = hash_expr_list;
	*hash_expr_opfamilies_out_list = hash_expr_opfamilies;

	// cleanup
	child_contexts->Release();
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToPlStmt::TranslateSortCols
//
//	@doc:
//		Translates DXL sorting columns list into GPDB's arrays of sorting attribute numbers,
//		and sorting operator ids, respectively.
//		The two arrays must be allocated by the caller.
//
//---------------------------------------------------------------------------
void
CTranslatorDXLToPlStmt::TranslateSortCols(
	const CDXLNode *sort_col_list_dxl,
	const CDXLTranslateContext *child_context, AttrNumber *att_no_sort_colids,
	Oid *sort_op_oids, Oid *sort_collations_oids, bool *is_nulls_first)
{
	const ULONG arity = sort_col_list_dxl->Arity();
	for (ULONG ul = 0; ul < arity; ul++)
	{
		CDXLNode *sort_col_dxlnode = (*sort_col_list_dxl)[ul];
		CDXLScalarSortCol *sc_sort_col_dxlop =
			CDXLScalarSortCol::Cast(sort_col_dxlnode->GetOperator());

		ULONG sort_colid = sc_sort_col_dxlop->GetColId();
		const TargetEntry *te_sort_col =
			child_context->GetTargetEntry(sort_colid);
		if (NULL == te_sort_col)
		{
			GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXL2PlStmtAttributeNotFound,
					   sort_colid);
		}

		att_no_sort_colids[ul] = te_sort_col->resno;
		sort_op_oids[ul] =
			CMDIdGPDB::CastMdid(sc_sort_col_dxlop->GetMdIdSortOp())->Oid();
		if (sort_collations_oids)
		{
			sort_collations_oids[ul] =
				px::ExprCollation((Node *) te_sort_col->expr);
		}
		is_nulls_first[ul] = sc_sort_col_dxlop->IsSortedNullsFirst();
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToPlStmt::CostFromStr
//
//	@doc:
//		Parses a cost value from a string
//
//---------------------------------------------------------------------------
Cost
CTranslatorDXLToPlStmt::CostFromStr(const CWStringBase *str)
{
	CHAR *sz = CTranslatorUtils::CreateMultiByteCharStringFromWCString(
		str->GetBuffer());
	return gpos::clib::Strtod(sz);
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToPlStmt::GetGPDBJoinTypeFromDXLJoinType
//
//	@doc:
//		Translates the join type from its DXL representation into the GPDB one
//
//---------------------------------------------------------------------------
JoinType
CTranslatorDXLToPlStmt::GetGPDBJoinTypeFromDXLJoinType(EdxlJoinType join_type)
{
	GPOS_ASSERT(EdxljtSentinel > join_type);

	JoinType jt = JOIN_INNER;

	switch (join_type)
	{
		case EdxljtInner:
			jt = JOIN_INNER;
			break;
		case EdxljtLeft:
			jt = JOIN_LEFT;
			break;
		case EdxljtFull:
			jt = JOIN_FULL;
			break;
		case EdxljtRight:
			jt = JOIN_RIGHT;
			break;
		case EdxljtIn:
			jt = JOIN_SEMI;
			break;
		case EdxljtLeftAntiSemijoin:
			jt = JOIN_ANTI;
			break;
		case EdxljtLeftAntiSemijoinNotIn:
			jt = JOIN_LASJ_NOTIN;
			break;
		default:
			GPOS_ASSERT(!"Unrecognized join type");
	}

	return jt;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToPlStmt::TranslateDXLBitmapTblScan
//
//	@doc:
//		Translates a DXL bitmap table scan node into a BitmapHeapScan node
//
//---------------------------------------------------------------------------
Plan *
CTranslatorDXLToPlStmt::TranslateDXLBitmapTblScan(
	const CDXLNode *bitmapscan_dxlnode, CDXLTranslateContext *output_context,
	CDXLTranslationContextArray *ctxt_translation_prev_siblings)
{
	const CDXLTableDescr *table_descr = nullptr;

	CDXLOperator *dxl_operator = bitmapscan_dxlnode->GetOperator();

	table_descr =
		CDXLPhysicalBitmapTableScan::Cast(dxl_operator)->GetDXLTableDescr();

	// translation context for column mappings in the base relation
	CDXLTranslateContextBaseTable base_table_context(m_mp);

	// add the new range table entry as the last element of the range table
	Index index =
		px::ListLength(m_dxl_to_plstmt_context->GetRTableEntriesList()) + 1;

	const IMDRelation *md_rel = m_md_accessor->RetrieveRel(table_descr->MDId());

	// Lock any table we are to scan, since it may not have been properly locked
	// by the parser (e.g in case of generated scans for partitioned tables)
	CMDIdGPDB *mdid = CMDIdGPDB::CastMdid(md_rel->MDId());
	GPOS_RTL_ASSERT(table_descr->LockMode() != -1);
	px::GPDBLockRelationOid(mdid->Oid(), table_descr->LockMode());

	RangeTblEntry *rte = TranslateDXLTblDescrToRangeTblEntry(
		table_descr, index, &base_table_context);
	GPOS_ASSERT(nullptr != rte);
	rte->requiredPerms |= ACL_SELECT;

	m_dxl_to_plstmt_context->AddRTE(rte);

	BitmapHeapScan *bitmap_tbl_scan;

	bitmap_tbl_scan = MakeNode(BitmapHeapScan);
	bitmap_tbl_scan->scan.scanrelid = index;
	bitmap_tbl_scan->scan.plan.px_scan_partial = true;

	Plan *plan = &(bitmap_tbl_scan->scan.plan);
	plan->plan_node_id = m_dxl_to_plstmt_context->GetNextPlanId();

	// translate operator costs
	TranslatePlanCosts(bitmapscan_dxlnode, plan);

	GPOS_ASSERT(4 == bitmapscan_dxlnode->Arity());

	// translate proj list and filter
	CDXLNode *project_list_dxlnode = (*bitmapscan_dxlnode)[0];
	CDXLNode *filter_dxlnode = (*bitmapscan_dxlnode)[1];
	CDXLNode *recheck_cond_dxlnode = (*bitmapscan_dxlnode)[2];
	CDXLNode *bitmap_access_path_dxlnode = (*bitmapscan_dxlnode)[3];

	List *quals_list = nullptr;
	TranslateProjListAndFilter(
		project_list_dxlnode, filter_dxlnode,
		&base_table_context,  // translate context for the base table
		ctxt_translation_prev_siblings, &plan->targetlist, &quals_list,
		output_context);
	plan->qual = quals_list;

	bitmap_tbl_scan->bitmapqualorig = TranslateDXLFilterToQual(
		recheck_cond_dxlnode, &base_table_context,
		ctxt_translation_prev_siblings, output_context);

	bitmap_tbl_scan->scan.plan.lefttree = TranslateDXLBitmapAccessPath(
		bitmap_access_path_dxlnode, output_context, md_rel, table_descr,
		&base_table_context, ctxt_translation_prev_siblings, bitmap_tbl_scan);
	SetParamIds(plan);

	return (Plan *) bitmap_tbl_scan;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToPlStmt::TranslatePlanCosts
//
//	@doc:
//		Translates DXL plan costs into the GPDB cost variables
//
//---------------------------------------------------------------------------
void
CTranslatorDXLToPlStmt::TranslatePlanCosts(const CDXLNode *dxlnode, Plan *plan)
{
	CDXLOperatorCost *costs =
		CDXLPhysicalProperties::PdxlpropConvert(dxlnode->GetProperties())
			->GetDXLOperatorCost();

	plan->startup_cost = CostFromStr(costs->GetStartUpCostStr());
	plan->total_cost = CostFromStr(costs->GetTotalCostStr());
	plan->plan_width = CTranslatorUtils::GetIntFromStr(costs->GetWidthStr());

	// In the Postgres planner, the estimates on each node are per QE
	// process, whereas the row estimates in GPORCA are global, across all
	// processes. Divide the row count estimate by the number of segments
	// executing it.
	plan->plan_rows =
		ceil(CostFromStr(costs->GetRowsOutStr()) /
			 m_dxl_to_plstmt_context->GetCurrentSlice()->numsegments);
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToPlStmt::TranslateDXLBitmapAccessPath
//
//	@doc:
//		Translate the tree of bitmap index operators that are under the given
//		bitmap table scan.
//
//---------------------------------------------------------------------------
Plan *
CTranslatorDXLToPlStmt::TranslateDXLBitmapAccessPath(
	const CDXLNode *bitmap_access_path_dxlnode,
	CDXLTranslateContext *output_context, const IMDRelation *md_rel,
	const CDXLTableDescr *table_descr,
	CDXLTranslateContextBaseTable *base_table_context,
	CDXLTranslationContextArray *ctxt_translation_prev_siblings,
	BitmapHeapScan *bitmap_tbl_scan)
{
	Edxlopid dxl_op_id =
		bitmap_access_path_dxlnode->GetOperator()->GetDXLOperator();
	if (EdxlopScalarBitmapIndexProbe == dxl_op_id)
	{
		return TranslateDXLBitmapIndexProbe(
			bitmap_access_path_dxlnode, output_context, md_rel, table_descr,
			base_table_context, ctxt_translation_prev_siblings,
			bitmap_tbl_scan);
	}
	GPOS_ASSERT(EdxlopScalarBitmapBoolOp == dxl_op_id);

	return TranslateDXLBitmapBoolOp(
		bitmap_access_path_dxlnode, output_context, md_rel, table_descr,
		base_table_context, ctxt_translation_prev_siblings, bitmap_tbl_scan);
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToScalar::TranslateDXLBitmapBoolOp
//
//	@doc:
//		Translates a DML bitmap bool op expression
//
//---------------------------------------------------------------------------
Plan *
CTranslatorDXLToPlStmt::TranslateDXLBitmapBoolOp(
	const CDXLNode *bitmap_boolop_dxlnode, CDXLTranslateContext *output_context,
	const IMDRelation *md_rel, const CDXLTableDescr *table_descr,
	CDXLTranslateContextBaseTable *base_table_context,
	CDXLTranslationContextArray *ctxt_translation_prev_siblings,
	BitmapHeapScan *bitmap_tbl_scan)
{
	GPOS_ASSERT(NULL != bitmap_boolop_dxlnode);
	GPOS_ASSERT(EdxlopScalarBitmapBoolOp ==
				bitmap_boolop_dxlnode->GetOperator()->GetDXLOperator());

	CDXLScalarBitmapBoolOp *sc_bitmap_boolop_dxlop =
		CDXLScalarBitmapBoolOp::Cast(bitmap_boolop_dxlnode->GetOperator());

	CDXLNode *left_tree_dxlnode = (*bitmap_boolop_dxlnode)[0];
	CDXLNode *right_tree_dxlnode = (*bitmap_boolop_dxlnode)[1];

	Plan *left_plan = TranslateDXLBitmapAccessPath(
		left_tree_dxlnode, output_context, md_rel, table_descr,
		base_table_context, ctxt_translation_prev_siblings, bitmap_tbl_scan);
	Plan *right_plan = TranslateDXLBitmapAccessPath(
		right_tree_dxlnode, output_context, md_rel, table_descr,
		base_table_context, ctxt_translation_prev_siblings, bitmap_tbl_scan);
	List *child_plan_list = ListMake2(left_plan, right_plan);

	Plan *plan = NULL;

	if (CDXLScalarBitmapBoolOp::EdxlbitmapAnd ==
		sc_bitmap_boolop_dxlop->GetDXLBitmapOpType())
	{
		BitmapAnd *bitmapand = MakeNode(BitmapAnd);
		bitmapand->plan.plan_node_id = m_dxl_to_plstmt_context->GetNextPlanId();
		bitmapand->bitmapplans = child_plan_list;
		bitmapand->plan.targetlist = NULL;
		bitmapand->plan.qual = NULL;
		plan = (Plan *) bitmapand;
	}
	else
	{
		BitmapOr *bitmapor = MakeNode(BitmapOr);
		bitmapor->plan.plan_node_id = m_dxl_to_plstmt_context->GetNextPlanId();
		bitmapor->bitmapplans = child_plan_list;
		bitmapor->plan.targetlist = NULL;
		bitmapor->plan.qual = NULL;
		plan = (Plan *) bitmapor;
	}


	return plan;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToPlStmt::TranslateDXLBitmapIndexProbe
//
//	@doc:
//		Translate CDXLScalarBitmapIndexProbe into a BitmapIndexScan
//
//---------------------------------------------------------------------------
Plan *
CTranslatorDXLToPlStmt::TranslateDXLBitmapIndexProbe(
	const CDXLNode *bitmap_index_probe_dxlnode,
	CDXLTranslateContext *output_context, const IMDRelation *md_rel,
	const CDXLTableDescr *table_descr,
	CDXLTranslateContextBaseTable *base_table_context,
	CDXLTranslationContextArray *ctxt_translation_prev_siblings,
	BitmapHeapScan *bitmap_tbl_scan)
{
	CDXLScalarBitmapIndexProbe *sc_bitmap_idx_probe_dxlop =
		CDXLScalarBitmapIndexProbe::Cast(
			bitmap_index_probe_dxlnode->GetOperator());

	BitmapIndexScan *bitmap_idx_scan;

	bitmap_idx_scan = MakeNode(BitmapIndexScan);
	bitmap_idx_scan->scan.scanrelid = bitmap_tbl_scan->scan.scanrelid;
	bitmap_idx_scan->scan.plan.px_scan_partial = true;

	CMDIdGPDB *mdid_index = CMDIdGPDB::CastMdid(
		sc_bitmap_idx_probe_dxlop->GetDXLIndexDescr()->MDId());
	const IMDIndex *index = m_md_accessor->RetrieveIndex(mdid_index);
	Oid index_oid = mdid_index->Oid();
	// Lock any index we are to scan, since it may not have been properly locked
	// by the parser (e.g in case of generated scans for partitioned indexes)
	px::GPDBLockRelationOid(index_oid, table_descr->LockMode());

	GPOS_ASSERT(InvalidOid != index_oid);
	bitmap_idx_scan->indexid = index_oid;
	Plan *plan = &(bitmap_idx_scan->scan.plan);
	plan->plan_node_id = m_dxl_to_plstmt_context->GetNextPlanId();

	GPOS_ASSERT(1 == bitmap_index_probe_dxlnode->Arity());
	CDXLNode *index_cond_list_dxlnode = (*bitmap_index_probe_dxlnode)[0];
	List *index_cond = NIL;
	List *index_orig_cond = NIL;
	List *index_strategy_list = NIL;
	List *index_subtype_list = NIL;

	TranslateIndexConditions(
		index_cond_list_dxlnode, table_descr, true /*is_bitmap_index_probe*/,
		index, md_rel, output_context, base_table_context,
		ctxt_translation_prev_siblings, &index_cond, &index_orig_cond,
		&index_strategy_list, &index_subtype_list);

	bitmap_idx_scan->indexqual = index_cond;
	bitmap_idx_scan->indexqualorig = index_orig_cond;
	/*
	 * As of 8.4, the indexstrategy and indexsubtype fields are no longer
	 * available or needed in IndexScan. Ignore them.
	 */
	SetParamIds(plan);

	return plan;
}

// translates a DXL Value Scan node into a GPDB Value scan node
Plan *
CTranslatorDXLToPlStmt::TranslateDXLValueScan(
	const CDXLNode *value_scan_dxlnode, CDXLTranslateContext *output_context,
	CDXLTranslationContextArray *ctxt_translation_prev_siblings)
{
	// translation context for column mappings
	CDXLTranslateContextBaseTable base_table_context(m_mp);

	// we will add the new range table entry as the last element of the range table
	Index index =
		px::ListLength(m_dxl_to_plstmt_context->GetRTableEntriesList()) + 1;

	base_table_context.SetRelIndex(index);

	// create value scan node
	ValuesScan *value_scan = MakeNode(ValuesScan);
	value_scan->scan.scanrelid = index;
	Plan *plan = &(value_scan->scan.plan);

	RangeTblEntry *rte = TranslateDXLValueScanToRangeTblEntry(
		value_scan_dxlnode, output_context, &base_table_context);
	GPOS_ASSERT(NULL != rte);

	value_scan->values_lists = (List *) px::CopyObject(rte->values_lists);

	m_dxl_to_plstmt_context->AddRTE(rte);

	plan->plan_node_id = m_dxl_to_plstmt_context->GetNextPlanId();

	// translate operator costs
	TranslatePlanCosts(value_scan_dxlnode, plan);

	// a table scan node must have at least 2 children: projection list and at least 1 value list
	GPOS_ASSERT(2 <= value_scan_dxlnode->Arity());

	CDXLNode *project_list_dxlnode = (*value_scan_dxlnode)[EdxltsIndexProjList];

	// translate proj list
	List *target_list = TranslateDXLProjList(
		project_list_dxlnode, &base_table_context, NULL, output_context);

	plan->targetlist = target_list;

	return (Plan *) value_scan;
}

List *
CTranslatorDXLToPlStmt::TranslateNestLoopParamList(
	CDXLColRefArray *pdrgdxlcrOuterRefs, CDXLTranslateContext *dxltrctxLeft,
	CDXLTranslateContext *dxltrctxRight)
{
	List *nest_params_list = NIL;
	for (ULONG ul = 0; ul < pdrgdxlcrOuterRefs->Size(); ul++)
	{
		CDXLColRef *pdxlcr = (*pdrgdxlcrOuterRefs)[ul];
		ULONG ulColid = pdxlcr->Id();
		// left child context contains the target entry for the nest params col refs
		const TargetEntry *target_entry = dxltrctxLeft->GetTargetEntry(ulColid);
		GPOS_ASSERT(NULL != target_entry);

		/*
		 * POLAR px: The ParamList of a nested loop may consist of Var or Const type
		 * expressions, we should process them separately.
		 */
		Var *new_var = NULL;
		if (IsA(target_entry->expr, Var))
		{
			Var *old_var = (Var *) target_entry->expr;
			new_var =
				px::MakeVar(OUTER_VAR, target_entry->resno, old_var->vartype,
					old_var->vartypmod, 0 /*varlevelsup*/);
			new_var->varnoold = old_var->varnoold;
			new_var->varoattno = old_var->varoattno;
		}
		else if (IsA(target_entry->expr, Const))
		{
			Const *old_var = (Const *) target_entry->expr;
			new_var =
				px::MakeVar(OUTER_VAR, target_entry->resno, old_var->consttype,
					old_var->consttypmod, 0 /*varlevelsup*/);
			/*
			 * POLAR px: For a const expression, we don't care the varnoold and
			 * varoattno fields, since they are reserved for debugging the Var
			 * type expression.
			 */
			new_var->varnoold = -1;
			new_var->varoattno = -1;
		}
		else
		{
			/*
			 * POLAR px: For unsupported expressions, we should warn user
			 * about this.
			 */
			GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXL2PlStmtAttributeNotFound, ulColid);
		}

		NestLoopParam *nest_params = MakeNode(NestLoopParam);
		// right child context contains the param entry for the nest params col refs
		const CMappingElementColIdParamId *colid_param_mapping =
			dxltrctxRight->GetParamIdMappingElement(ulColid);
		GPOS_ASSERT(NULL != colid_param_mapping);
		nest_params->paramno = colid_param_mapping->ParamId();
		nest_params->paramval = new_var;
		nest_params_list =
			px::LAppend(nest_params_list, (void *) nest_params);
	}
	return nest_params_list;
}
// EOF
