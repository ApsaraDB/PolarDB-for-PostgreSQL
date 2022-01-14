//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CPropConstraint.h
//
//	@doc:
//		Representation of constraint property
//---------------------------------------------------------------------------
#ifndef GPOPT_CPropConstraint_H
#define GPOPT_CPropConstraint_H

#include "gpos/base.h"
#include "gpos/common/CRefCount.h"

#include "gpopt/base/CColRefSet.h"
#include "gpopt/base/CConstraint.h"

namespace gpopt
{
using namespace gpos;

// forward declaration
class CConstraint;
class CExpression;

//---------------------------------------------------------------------------
//	@class:
//		CPropConstraint
//
//	@doc:
//		Representation of constraint property
//
//---------------------------------------------------------------------------
class CPropConstraint : public CRefCount, public DbgPrintMixin<CPropConstraint>
{
private:
	// array of equivalence classes
	CColRefSetArray *m_pdrgpcrs;

	// mapping from column to equivalence class
	ColRefToColRefSetMap *m_phmcrcrs;

	// constraint
	CConstraint *m_pcnstr;

	// initialize mapping from columns to equivalence classes
	void InitHashMap(CMemoryPool *mp);

public:
	CPropConstraint(const CPropConstraint &) = delete;

	// ctor
	CPropConstraint(CMemoryPool *mp, CColRefSetArray *pdrgpcrs,
					CConstraint *pcnstr);

	// dtor
	~CPropConstraint() override;

	// equivalence classes
	CColRefSetArray *
	PdrgpcrsEquivClasses() const
	{
		return m_pdrgpcrs;
	}

	// mapping
	CColRefSet *
	PcrsEquivClass(const CColRef *colref) const
	{
		if (nullptr == m_phmcrcrs)
		{
			return nullptr;
		}
		return m_phmcrcrs->Find(colref);
	}

	// constraint
	CConstraint *
	Pcnstr() const
	{
		return m_pcnstr;
	}

	// is this a contradiction
	BOOL FContradiction() const;

	// scalar expression on given column mapped from all constraints
	// on its equivalent columns
	CExpression *PexprScalarMappedFromEquivCols(
		CMemoryPool *mp, CColRef *colref,
		CPropConstraint *constraintsForOuterRefs) const;

	// print
	IOstream &OsPrint(IOstream &os) const;

};	// class CPropConstraint

// shorthand for printing
inline IOstream &
operator<<(IOstream &os, CPropConstraint &pc)
{
	return pc.OsPrint(os);
}
}  // namespace gpopt

#endif	// !GPOPT_CPropConstraint_H

// EOF
