//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CPropSpec.h
//
//	@doc:
//		Abstraction for specification of properties;
//---------------------------------------------------------------------------
#ifndef GPOPT_CPropSpec_H
#define GPOPT_CPropSpec_H

#include "gpos/base.h"
#include "gpos/common/CRefCount.h"

#include "gpopt/operators/CExpression.h"

namespace gpopt
{
using namespace gpos;

// prototypes
class CReqdPropPlan;

//---------------------------------------------------------------------------
//	@class:
//		CPropSpec
//
//	@doc:
//		Property specification
//
//---------------------------------------------------------------------------
class CPropSpec : public CRefCount, public DbgPrintMixin<CPropSpec>
{
public:
	// property type
	enum EPropSpecType
	{
		EpstOrder,
		EpstDistribution,
		EpstRewindability,
		EpstPartPropagation,

		EpstSentinel
	};

private:
protected:
	// ctor
	CPropSpec() = default;

	// dtor
	~CPropSpec() override = default;

public:
	CPropSpec(const CPropSpec &) = delete;

	// append enforcers to dynamic array for the given plan properties
	virtual void AppendEnforcers(CMemoryPool *mp, CExpressionHandle &exprhdl,
								 CReqdPropPlan *prpp,
								 CExpressionArray *pdrgpexpr,
								 CExpression *pexpr) = 0;

	// hash function
	virtual ULONG HashValue() const = 0;

	// extract columns used by the property
	virtual CColRefSet *PcrsUsed(CMemoryPool *mp) const = 0;

	// property type
	virtual EPropSpecType Epst() const = 0;

	virtual gpos::IOstream &OsPrint(gpos::IOstream &os) const = 0;

};	// class CPropSpec


// shorthand for printing
inline IOstream &
operator<<(IOstream &os, const CPropSpec &ospec)
{
	return ospec.OsPrint(os);
}

}  // namespace gpopt

FORCE_GENERATE_DBGSTR(gpopt::CPropSpec);

#endif	// !GPOPT_CPropSpec_H

// EOF
