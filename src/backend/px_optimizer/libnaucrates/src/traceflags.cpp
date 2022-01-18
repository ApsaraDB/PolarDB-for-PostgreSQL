//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 EMC Corp.
//
//	@filename:
//		traceflags.cpp
//
//	@doc:
//		Implementation of trace flags routines
//---------------------------------------------------------------------------

#include "naucrates/traceflags/traceflags.h"

#include "gpos/base.h"
#include "gpos/common/CBitSetIter.h"
#include "gpos/task/CAutoTraceFlag.h"

using namespace gpos;

//---------------------------------------------------------------------------
//	@function:
//		SetTraceflags
//
//	@doc:
//		Set trace flags based on given bit set, and return two output bit
//		sets of old trace flags values
//
//---------------------------------------------------------------------------
void
SetTraceflags(
	CMemoryPool *mp,
	const CBitSet *pbsInput,  // set of trace flags to be enabled
	CBitSet *
		*ppbsEnabled,  // output: enabled trace flags before function is called
	CBitSet *
		*ppbsDisabled  // output: disabled trace flags before function is called
)
{
	if (nullptr == pbsInput)
	{
		// bail out if input set is null
		return;
	}

	GPOS_ASSERT(nullptr != ppbsEnabled);
	GPOS_ASSERT(nullptr != ppbsDisabled);

	*ppbsEnabled = GPOS_NEW(mp) CBitSet(mp, EopttraceSentinel);
	*ppbsDisabled = GPOS_NEW(mp) CBitSet(mp, EopttraceSentinel);
	CBitSetIter bsiter(*pbsInput);
	while (bsiter.Advance())
	{
		ULONG ulTraceFlag = bsiter.Bit();
		if (GPOS_FTRACE(ulTraceFlag))
		{
			// set trace flag in the enabled set
			BOOL fSet GPOS_ASSERTS_ONLY =
				(*ppbsEnabled)->ExchangeSet(ulTraceFlag);
			GPOS_ASSERT(!fSet);
		}
		else
		{
			// set trace flag in the disabled set
			BOOL fSet GPOS_ASSERTS_ONLY =
				(*ppbsDisabled)->ExchangeSet(ulTraceFlag);
			GPOS_ASSERT(!fSet);
		}

		// set trace flag
		GPOS_SET_TRACE(ulTraceFlag);
	}
}


//---------------------------------------------------------------------------
//	@function:
//		ResetTraceflags
//
//	@doc:
//		Reset trace flags based on values given by input sets
//
//---------------------------------------------------------------------------
void
ResetTraceflags(CBitSet *pbsEnabled, CBitSet *pbsDisabled)
{
	if (nullptr == pbsEnabled || nullptr == pbsDisabled)
	{
		// bail out if input sets are null
		return;
	}

	GPOS_ASSERT(nullptr != pbsEnabled);
	GPOS_ASSERT(nullptr != pbsDisabled);

	CBitSetIter bsiterEnabled(*pbsEnabled);
	while (bsiterEnabled.Advance())
	{
		ULONG ulTraceFlag = bsiterEnabled.Bit();
		GPOS_SET_TRACE(ulTraceFlag);
	}

	CBitSetIter bsiterDisabled(*pbsDisabled);
	while (bsiterDisabled.Advance())
	{
		ULONG ulTraceFlag = bsiterDisabled.Bit();
		GPOS_UNSET_TRACE(ulTraceFlag);
	}
}

// EOF
