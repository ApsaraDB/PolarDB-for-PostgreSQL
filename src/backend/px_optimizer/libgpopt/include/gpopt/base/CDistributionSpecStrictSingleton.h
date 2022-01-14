//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 EMC Corp.
//
//	@filename:
//		CDistributionSpecStrictSingleton.h
//
//	@doc:
//		Description of a strict singleton distribution. Unlike the simple singleton distribution,
//		that satisfies hash distribution requests, the strict singleton distribution is only
//		compatible with other singleton distributions
//		Can be used as a derived property;
//---------------------------------------------------------------------------
#ifndef GPOPT_CDistributionSpecStrictSingleton_H
#define GPOPT_CDistributionSpecStrictSingleton_H

#include "gpos/base.h"
#include "gpos/utils.h"

#include "gpopt/base/CDistributionSpecSingleton.h"

namespace gpopt
{
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CDistributionSpecStrictSingleton
//
//	@doc:
//		Class for representing singleton distribution specification.
//
//---------------------------------------------------------------------------
class CDistributionSpecStrictSingleton : public CDistributionSpecSingleton
{
private:
public:
	CDistributionSpecStrictSingleton(const CDistributionSpecStrictSingleton &) =
		delete;

	// ctor
	explicit CDistributionSpecStrictSingleton(ESegmentType esegtype);

	// distribution type accessor
	EDistributionType
	Edt() const override
	{
		return CDistributionSpec::EdtStrictSingleton;
	}

	// return true if distribution spec can be required
	BOOL
	FRequirable() const override
	{
		return false;
	}

	// does this distribution satisfy the given one
	BOOL FSatisfies(const CDistributionSpec *pds) const override;

	// append enforcers to dynamic array for the given plan properties
	void
	AppendEnforcers(CMemoryPool *,		  // mp
					CExpressionHandle &,  // exprhdl
					CReqdPropPlan *,	  // prpp
					CExpressionArray *,	  // pdrgpexpr
					CExpression *		  // pexpr
					) override
	{
		GPOS_ASSERT(!"attempt to enforce strict SINGLETON distribution");
	}

	// print
	IOstream &OsPrint(IOstream &os) const override;

	// conversion function
	static CDistributionSpecStrictSingleton *
	PdssConvert(CDistributionSpec *pds)
	{
		GPOS_ASSERT(nullptr != pds);
		GPOS_ASSERT(EdtStrictSingleton == pds->Edt());

		return dynamic_cast<CDistributionSpecStrictSingleton *>(pds);
	}

};	// class CDistributionSpecStrictSingleton

}  // namespace gpopt

#endif	// !GPOPT_CDistributionSpecStrictSingleton_H

// EOF
