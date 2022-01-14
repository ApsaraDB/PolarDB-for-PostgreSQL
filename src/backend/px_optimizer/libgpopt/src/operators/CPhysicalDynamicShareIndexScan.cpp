//---------------------------------------------------------------------------
//
// PolarDB PX Optimizer
//
//	Copyright (C) 2021, Alibaba Group Holding Limited
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
//	@filename:
//		CPhysicalDynamicShareIndexScan.cpp
//
//	@doc:
//		Implementation of dynamic index scan operator
//---------------------------------------------------------------------------

#include "gpopt/operators/CPhysicalDynamicShareIndexScan.h"

#include "gpos/base.h"
#include "gpos/error/CAutoTrace.h"

#include "gpopt/base/COptCtxt.h"
#include "gpopt/base/CUtils.h"
#include "gpopt/cost/ICostModel.h"
#include "gpopt/metadata/CPartConstraint.h"
#include "gpopt/operators/CExpressionHandle.h"
#include "gpopt/operators/CPredicateUtils.h"
#include "naucrates/statistics/CFilterStatsProcessor.h"
#include "naucrates/statistics/CStatisticsUtils.h"
#include "gpopt/base/CDistributionSpec.h"
#include "gpopt/base/CDistributionSpecReplicated.h"

using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalDynamicShareIndexScan::CPhysicalDynamicShareIndexScan
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CPhysicalDynamicShareIndexScan::CPhysicalDynamicShareIndexScan(
	CMemoryPool *mp, CIndexDescriptor *pindexdesc, CTableDescriptor *ptabdesc,
	ULONG ulOriginOpId, const CName *pnameAlias, CColRefArray *pdrgpcrOutput,
	ULONG scan_id, CColRef2dArray *pdrgpdrgpcrPart, COrderSpec *pos,
	IMdIdArray *partition_mdids,
	ColRefToUlongMapArray *root_col_mapping_per_part)
	: CPhysicalDynamicScan(mp, ptabdesc, ulOriginOpId, pnameAlias, scan_id,
						   pdrgpcrOutput, pdrgpdrgpcrPart, partition_mdids,
						   root_col_mapping_per_part),
	  m_pindexdesc(pindexdesc),
	  m_pos(pos)
{
	GPOS_ASSERT(nullptr != pindexdesc);
	GPOS_ASSERT(nullptr != pos);

	/* POLAR px */
	GPOS_DELETE(m_pds);
	m_pds = GPOS_NEW(mp) CDistributionSpecReplicated(
				CDistributionSpec::EdtStrictReplicated);
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalDynamicShareIndexScan::~CPhysicalDynamicShareIndexScan
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CPhysicalDynamicShareIndexScan::~CPhysicalDynamicShareIndexScan()
{
	m_pindexdesc->Release();
	m_pos->Release();
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalDynamicShareIndexScan::EpetOrder
//
//	@doc:
//		Return the enforcing type for order property based on this operator
//
//---------------------------------------------------------------------------
CEnfdProp::EPropEnforcingType
CPhysicalDynamicShareIndexScan::EpetOrder(CExpressionHandle &,  // exprhdl
									 const CEnfdOrder *peo) const
{
	GPOS_ASSERT(nullptr != peo);
	GPOS_ASSERT(!peo->PosRequired()->IsEmpty());

	if (peo->FCompatible(m_pos))
	{
		// required order is already established by the index
		return CEnfdProp::EpetUnnecessary;
	}

	return CEnfdProp::EpetRequired;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalDynamicShareIndexScan::HashValue
//
//	@doc:
//		Combine pointers for table descriptor, index descriptor and Eop
//
//---------------------------------------------------------------------------
ULONG
CPhysicalDynamicShareIndexScan::HashValue() const
{
	ULONG scan_id = ScanId();
	return gpos::CombineHashes(
		COperator::HashValue(),
		gpos::CombineHashes(gpos::HashValue(&scan_id),
							m_pindexdesc->MDId()->HashValue()));
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalDynamicShareIndexScan::Matches
//
//	@doc:
//		match operator
//
//---------------------------------------------------------------------------
BOOL
CPhysicalDynamicShareIndexScan::Matches(COperator *pop) const
{
	return CUtils::FMatchDynamicIndex(this, pop);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalDynamicShareIndexScan::OsPrint
//
//	@doc:
//		debug print
//
//---------------------------------------------------------------------------
IOstream &
CPhysicalDynamicShareIndexScan::OsPrint(IOstream &os) const
{
	if (m_fPattern)
	{
		return COperator::OsPrint(os);
	}

	os << SzId() << " ";
	// index name
	os << "  Index: (";
	m_pindexdesc->Name().OsPrint(os);
	// table name
	os << ")";
	os << ", Table: (";
	Ptabdesc()->Name().OsPrint(os);
	os << ")";
	os << ", Columns: [";
	CUtils::OsPrintDrgPcr(os, PdrgpcrOutput());
	os << "] Scan Id: " << ScanId();


	return os;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalDynamicShareIndexScan::PstatsDerive
//
//	@doc:
//		Statistics derivation during costing
//
//---------------------------------------------------------------------------
IStatistics *
CPhysicalDynamicShareIndexScan::PstatsDerive(CMemoryPool *mp,
										CExpressionHandle &exprhdl,
										CReqdPropPlan *prpplan GPOS_UNUSED,
										IStatisticsArray *stats_ctxt) const
{
	GPOS_ASSERT(nullptr != prpplan);

	IStatistics *pstatsBaseTable =
		CStatisticsUtils::DeriveStatsForDynamicScan(mp, exprhdl, ScanId(),
											prpplan->Pepp()->PppsRequired());

	// create a conjunction of index condition and additional filters
	CExpression *pexprScalar = exprhdl.PexprScalarRepChild(0 /*ulChidIndex*/);
	CExpression *local_expr = nullptr;
	CExpression *expr_with_outer_refs = nullptr;

	// get outer references from expression handle
	CColRefSet *outer_refs = exprhdl.DeriveOuterReferences();

	CPredicateUtils::SeparateOuterRefs(mp, pexprScalar, outer_refs, &local_expr,
									   &expr_with_outer_refs);

	IStatistics *stats = CFilterStatsProcessor::MakeStatsFilterForScalarExpr(
		mp, exprhdl, pstatsBaseTable, local_expr, expr_with_outer_refs,
		stats_ctxt);

	pstatsBaseTable->Release();
	local_expr->Release();
	expr_with_outer_refs->Release();

	return stats;
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalDynamicShareIndexScan::PdsDerive
//
//	@doc:
//		Derive distribution
//
//---------------------------------------------------------------------------
CDistributionSpec *
CPhysicalDynamicShareIndexScan::PdsDerive
	(
	CMemoryPool *mp,
	CExpressionHandle &
	)
	const
{
	return GPOS_NEW(mp) CDistributionSpecReplicated(CDistributionSpec::EdtStrictReplicated);
}
// EOF
