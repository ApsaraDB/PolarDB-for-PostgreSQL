//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2009 Greenplum, Inc.
//
//	@filename:
//		CPattern.cpp
//
//	@doc:
//		Implementation of base class of pattern operators
//---------------------------------------------------------------------------

#include "gpopt/operators/CPattern.h"

#include "gpos/base.h"

using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CPattern::PdpCreate
//
//	@doc:
//		Pattern operators cannot derive properties; the assembly of the
//		expression has to take care of this on a higher level
//
//---------------------------------------------------------------------------
CDrvdProp *
CPattern::PdpCreate(CMemoryPool *  // mp
) const
{
	GPOS_ASSERT(!"Cannot derive properties on pattern");
	return nullptr;
}


//---------------------------------------------------------------------------
//	@function:
//		CPattern::PrpCreate
//
//	@doc:
//		Pattern operators cannot compute required properties; the assembly of the
//		expression has to take care of this on a higher level
//
//---------------------------------------------------------------------------
CReqdProp *
CPattern::PrpCreate(CMemoryPool *  // mp
) const
{
	GPOS_ASSERT(!"Cannot compute required properties on pattern");
	return nullptr;
}


//---------------------------------------------------------------------------
//	@function:
//		CPattern::Matches
//
//	@doc:
//		match against an operator
//
//---------------------------------------------------------------------------
BOOL
CPattern::Matches(COperator *pop) const
{
	return Eopid() == pop->Eopid();
}


//---------------------------------------------------------------------------
//	@function:
//		CPattern::FInputOrderSensitive
//
//	@doc:
//		By default patterns are leaves; no need to call this function ever
//
//---------------------------------------------------------------------------
BOOL
CPattern::FInputOrderSensitive() const
{
	GPOS_ASSERT(!"Unexpected call to function FInputOrderSensitive");
	return true;
}

//---------------------------------------------------------------------------
//	@function:
//		CPattern::PopCopyWithRemappedColumns
//
//	@doc:
//		Return a copy of the operator with remapped columns
//
//---------------------------------------------------------------------------
COperator *
CPattern::PopCopyWithRemappedColumns(CMemoryPool *,		  //mp,
									 UlongToColRefMap *,  //colref_mapping,
									 BOOL				  //must_exist
)
{
	GPOS_ASSERT(
		!"PopCopyWithRemappedColumns should not be called for a pattern");
	return nullptr;
}

// EOF
