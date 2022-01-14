//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CPropConstraint.cpp
//
//	@doc:
//		Implementation of constraint property
//---------------------------------------------------------------------------

#include "gpopt/base/CPropConstraint.h"

#include "gpos/base.h"
#include "gpos/error/CAutoTrace.h"

#include "gpopt/base/CColRefSetIter.h"
#include "gpopt/base/CConstraintConjunction.h"
#include "gpopt/base/COptCtxt.h"

using namespace gpopt;

FORCE_GENERATE_DBGSTR(CPropConstraint);

//---------------------------------------------------------------------------
//	@function:
//		CPropConstraint::CPropConstraint
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CPropConstraint::CPropConstraint(CMemoryPool *mp, CColRefSetArray *pdrgpcrs,
								 CConstraint *pcnstr)
	: m_pdrgpcrs(pdrgpcrs), m_phmcrcrs(nullptr), m_pcnstr(pcnstr)
{
	GPOS_ASSERT(nullptr != pdrgpcrs);
	InitHashMap(mp);
}

//---------------------------------------------------------------------------
//	@function:
//		CPropConstraint::~CPropConstraint
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CPropConstraint::~CPropConstraint()
{
	m_pdrgpcrs->Release();
	CRefCount::SafeRelease(m_phmcrcrs);
	CRefCount::SafeRelease(m_pcnstr);
}

//---------------------------------------------------------------------------
//	@function:
//		CPropConstraint::InitHashMap
//
//	@doc:
//		Initialize mapping between columns and equivalence classes
//
//---------------------------------------------------------------------------
void
CPropConstraint::InitHashMap(CMemoryPool *mp)
{
	GPOS_ASSERT(nullptr == m_phmcrcrs);
	const ULONG ulEquiv = m_pdrgpcrs->Size();

	// m_phmcrcrs is only needed when storing equivalent columns
	if (0 != ulEquiv)
	{
		m_phmcrcrs = GPOS_NEW(mp) ColRefToColRefSetMap(mp);
	}
	for (ULONG ul = 0; ul < ulEquiv; ul++)
	{
		CColRefSet *pcrs = (*m_pdrgpcrs)[ul];

		CColRefSetIter crsi(*pcrs);
		while (crsi.Advance())
		{
			pcrs->AddRef();
			BOOL fres GPOS_ASSERTS_ONLY = m_phmcrcrs->Insert(crsi.Pcr(), pcrs);
			GPOS_ASSERT(fres);
		}
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CPropConstraint::FContradiction
//
//	@doc:
//		Is this a contradiction
//
//---------------------------------------------------------------------------
BOOL
CPropConstraint::FContradiction() const
{
	return (nullptr != m_pcnstr && m_pcnstr->FContradiction());
}

//---------------------------------------------------------------------------
//	@function:
//		CPropConstraint::PexprScalarMappedFromEquivCols
//
//	@doc:
//		Return scalar expression on the given column mapped from all constraints
//		on its equivalent columns
//
//---------------------------------------------------------------------------
CExpression *
CPropConstraint::PexprScalarMappedFromEquivCols(
	CMemoryPool *mp, CColRef *colref,
	CPropConstraint *constraintsForOuterRefs) const
{
	if (nullptr == m_pcnstr || nullptr == m_phmcrcrs)
	{
		return nullptr;
	}
	CColRefSet *pcrs = m_phmcrcrs->Find(colref);
	CColRefSet *equivOuterRefs = nullptr;

	if (nullptr != constraintsForOuterRefs &&
		nullptr != constraintsForOuterRefs->m_phmcrcrs)
	{
		equivOuterRefs = constraintsForOuterRefs->m_phmcrcrs->Find(colref);
	}

	if ((nullptr == pcrs || 1 == pcrs->Size()) &&
		(nullptr == equivOuterRefs || 1 == equivOuterRefs->Size()))
	{
		// we have no columns that are equivalent to 'colref'
		return nullptr;
	}

	// get constraints for all other columns in this equivalence class
	// except the current column
	CColRefSet *pcrsEquiv = GPOS_NEW(mp) CColRefSet(mp);
	pcrsEquiv->Include(pcrs);
	if (nullptr != equivOuterRefs)
	{
		pcrsEquiv->Include(equivOuterRefs);
	}
	pcrsEquiv->Exclude(colref);

	// local constraints on the equivalent column(s)
	CConstraint *pcnstr = m_pcnstr->Pcnstr(mp, pcrsEquiv);
	CConstraint *pcnstrFromOuterRefs = nullptr;

	if (nullptr != constraintsForOuterRefs &&
		nullptr != constraintsForOuterRefs->m_pcnstr)
	{
		// constraints that exist in the outer scope
		pcnstrFromOuterRefs =
			constraintsForOuterRefs->m_pcnstr->Pcnstr(mp, pcrsEquiv);
	}
	pcrsEquiv->Release();
	CRefCount::SafeRelease(equivOuterRefs);

	// combine local and outer ref constraints, if we have any, into pcnstr
	if (nullptr == pcnstr && nullptr == pcnstrFromOuterRefs)
	{
		// neither local nor outer ref constraints
		return nullptr;
	}
	else if (nullptr == pcnstr)
	{
		// only constraints from outer refs, move to pcnstr
		pcnstr = pcnstrFromOuterRefs;
		pcnstrFromOuterRefs = nullptr;
	}
	else if (nullptr != pcnstr && nullptr != pcnstrFromOuterRefs)
	{
		// constraints from both local and outer refs, make a conjunction
		// and store it in pcnstr
		CConstraintArray *conjArray = GPOS_NEW(mp) CConstraintArray(mp);

		conjArray->Append(pcnstr);
		conjArray->Append(pcnstrFromOuterRefs);
		pcnstrFromOuterRefs = nullptr;
		pcnstr = GPOS_NEW(mp) CConstraintConjunction(mp, conjArray);
	}

	// Now, pcnstr contains constraints on columns that are equivalent
	// to 'colref'. These constraints may be local or in an outer scope.
	// Generate a copy of all these constraints for the current column.
	CConstraint *pcnstrCol = pcnstr->PcnstrRemapForColumn(mp, colref);
	CExpression *pexprScalar = pcnstrCol->PexprScalar(mp);
	pexprScalar->AddRef();

	pcnstr->Release();
	GPOS_ASSERT(nullptr == pcnstrFromOuterRefs);
	pcnstrCol->Release();

	return pexprScalar;
}

//---------------------------------------------------------------------------
//	@function:
//		CPropConstraint::OsPrint
//
//	@doc:
//		debug print
//
//---------------------------------------------------------------------------
IOstream &
CPropConstraint::OsPrint(IOstream &os) const
{
	const ULONG length = m_pdrgpcrs->Size();
	if (0 < length)
	{
		os << "Equivalence Classes: { ";

		for (ULONG ul = 0; ul < length; ul++)
		{
			CColRefSet *pcrs = (*m_pdrgpcrs)[ul];
			os << "(" << *pcrs << ") ";
		}

		os << "} ";
	}

	if (nullptr != m_pcnstr)
	{
		os << "Constraint:" << *m_pcnstr;
	}

	return os;
}

// EOF
