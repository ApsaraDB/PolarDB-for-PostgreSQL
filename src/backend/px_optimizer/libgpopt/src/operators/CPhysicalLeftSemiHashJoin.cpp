//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CPhysicalLeftSemiHashJoin.cpp
//
//	@doc:
//		Implementation of left semi hash join operator
//---------------------------------------------------------------------------

#include "gpopt/operators/CPhysicalLeftSemiHashJoin.h"

#include "gpos/base.h"

#include "gpopt/base/CDistributionSpecHashed.h"
#include "gpopt/base/CUtils.h"


using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalLeftSemiHashJoin::CPhysicalLeftSemiHashJoin
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CPhysicalLeftSemiHashJoin::CPhysicalLeftSemiHashJoin(
	CMemoryPool *mp, CExpressionArray *pdrgpexprOuterKeys,
	CExpressionArray *pdrgpexprInnerKeys, IMdIdArray *hash_opfamilies)
	: CPhysicalHashJoin(mp, pdrgpexprOuterKeys, pdrgpexprInnerKeys,
						hash_opfamilies)
{
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalLeftSemiHashJoin::~CPhysicalLeftSemiHashJoin
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CPhysicalLeftSemiHashJoin::~CPhysicalLeftSemiHashJoin() = default;


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalLeftSemiHashJoin::FProvidesReqdCols
//
//	@doc:
//		Check if required columns are included in output columns
//
//---------------------------------------------------------------------------
BOOL
CPhysicalLeftSemiHashJoin::FProvidesReqdCols(CExpressionHandle &exprhdl,
											 CColRefSet *pcrsRequired,
											 ULONG	// ulOptReq
) const
{
	// left semi join only propagates columns from left child
	return FOuterProvidesReqdCols(exprhdl, pcrsRequired);
}

// EOF
