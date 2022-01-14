//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2009 - 2011 EMC CORP.
//
//	@filename:
//		CDrvdPropRelational.cpp
//
//	@doc:
//		Relational derived properties;
//---------------------------------------------------------------------------

#include "gpopt/base/CDrvdPropRelational.h"

#include "gpos/base.h"
#include "gpos/task/CAutoSuspendAbort.h"
#include "gpos/task/CWorker.h"

#include "gpopt/base/CColRefSet.h"
#include "gpopt/base/CKeyCollection.h"
#include "gpopt/base/CPartInfo.h"
#include "gpopt/base/CReqdPropPlan.h"
#include "gpopt/base/CUtils.h"
#include "gpopt/operators/CExpressionHandle.h"
#include "gpopt/operators/CLogical.h"
#include "gpopt/operators/CLogicalDynamicGet.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CDrvdPropRelational::CDrvdPropRelational
//
//	@doc:
//		ctor
//
//---------------------------------------------------------------------------
CDrvdPropRelational::CDrvdPropRelational(CMemoryPool *mp)
	: m_mp(mp),
	  m_is_prop_derived(nullptr),
	  m_pcrsOutput(nullptr),
	  m_pcrsOuter(nullptr),
	  m_pcrsNotNull(nullptr),
	  m_pcrsCorrelatedApply(nullptr),
	  m_pkc(nullptr),
	  m_pdrgpfd(nullptr),
	  m_ulJoinDepth(0),
	  m_ppartinfo(nullptr),
	  m_ppc(nullptr),
	  m_pfp(nullptr),
	  m_is_complete(false)
{
	m_is_prop_derived = GPOS_NEW(mp) CBitSet(mp, EdptSentinel);
}


//---------------------------------------------------------------------------
//	@function:
//		CDrvdPropRelational::~CDrvdPropRelational
//
//	@doc:
//		ctor
//
//---------------------------------------------------------------------------
CDrvdPropRelational::~CDrvdPropRelational()
{
	{
		CAutoSuspendAbort asa;

		CRefCount::SafeRelease(m_is_prop_derived);
		CRefCount::SafeRelease(m_pcrsOutput);
		CRefCount::SafeRelease(m_pcrsOuter);
		CRefCount::SafeRelease(m_pcrsNotNull);
		CRefCount::SafeRelease(m_pcrsCorrelatedApply);
		CRefCount::SafeRelease(m_pkc);
		CRefCount::SafeRelease(m_pdrgpfd);
		CRefCount::SafeRelease(m_ppartinfo);
		CRefCount::SafeRelease(m_ppc);
		CRefCount::SafeRelease(m_pfp);
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CDrvdPropRelational::Derive
//
//	@doc:
//		Derive relational props. This derives ALL properties
//
//---------------------------------------------------------------------------
void
CDrvdPropRelational::Derive(CMemoryPool *,	//mp,
							CExpressionHandle &exprhdl,
							CDrvdPropCtxt *	 // pdpctxt
)
{
	GPOS_CHECK_ABORT;

	// call output derivation function on the operator
	DeriveOutputColumns(exprhdl);

	// derive outer-references
	DeriveOuterReferences(exprhdl);

	// derive not null columns
	DeriveNotNullColumns(exprhdl);

	// derive correlated apply columns
	DeriveCorrelatedApplyColumns(exprhdl);

	// derive constraint
	DerivePropertyConstraint(exprhdl);

	// compute max card
	DeriveMaxCard(exprhdl);

	// derive keys
	DeriveKeyCollection(exprhdl);

	// derive join depth
	DeriveJoinDepth(exprhdl);

	// derive function properties
	DeriveFunctionProperties(exprhdl);

	// derive functional dependencies
	DeriveFunctionalDependencies(exprhdl);

	// derive partition consumers
	DerivePartitionInfo(exprhdl);
	GPOS_ASSERT(nullptr != m_ppartinfo);

	DeriveTableDescriptor(exprhdl);

	m_is_complete = true;
}

//---------------------------------------------------------------------------
//	@function:
//		CDrvdPropRelational::FSatisfies
//
//	@doc:
//		Check for satisfying required properties
//
//---------------------------------------------------------------------------
BOOL
CDrvdPropRelational::FSatisfies(const CReqdPropPlan *prpp) const
{
	GPOS_ASSERT(nullptr != prpp);
	GPOS_ASSERT(nullptr != prpp->PcrsRequired());

	BOOL fSatisfies = GetOutputColumns()->ContainsAll(prpp->PcrsRequired());

	return fSatisfies;
}


//---------------------------------------------------------------------------
//	@function:
//		CDrvdPropRelational::GetRelationalProperties
//
//	@doc:
//		Short hand for conversion
//
//---------------------------------------------------------------------------
CDrvdPropRelational *
CDrvdPropRelational::GetRelationalProperties(CDrvdProp *pdp)
{
	GPOS_ASSERT(nullptr != pdp);
	GPOS_ASSERT(EptRelational == pdp->Ept() &&
				"This is not a relational properties container");
	return dynamic_cast<CDrvdPropRelational *>(pdp);
}


//---------------------------------------------------------------------------
//	@function:
//		CDrvdPropRelational::PdrgpfdChild
//
//	@doc:
//		Helper for getting applicable FDs from child
//
//---------------------------------------------------------------------------
CFunctionalDependencyArray *
CDrvdPropRelational::DeriveChildFunctionalDependencies(
	CMemoryPool *mp, ULONG child_index, CExpressionHandle &exprhdl)
{
	GPOS_ASSERT(child_index < exprhdl.Arity());
	GPOS_ASSERT(!exprhdl.FScalarChild(child_index));

	// get FD's of the child
	CFunctionalDependencyArray *pdrgpfdChild = exprhdl.Pdrgpfd(child_index);

	// get output columns of the parent
	CColRefSet *pcrsOutput = exprhdl.DeriveOutputColumns();

	// collect child FD's that are applicable to the parent
	CFunctionalDependencyArray *pdrgpfd =
		GPOS_NEW(mp) CFunctionalDependencyArray(mp);
	const ULONG size = pdrgpfdChild->Size();
	for (ULONG ul = 0; ul < size; ul++)
	{
		CFunctionalDependency *pfd = (*pdrgpfdChild)[ul];

		// check applicability of FD's LHS
		if (pcrsOutput->ContainsAll(pfd->PcrsKey()))
		{
			// decompose FD's RHS to extract the applicable part
			CColRefSet *pcrsDetermined = GPOS_NEW(mp) CColRefSet(mp);
			pcrsDetermined->Include(pfd->PcrsDetermined());
			pcrsDetermined->Intersection(pcrsOutput);
			if (0 < pcrsDetermined->Size())
			{
				// create a new FD and add it to the output array
				pfd->PcrsKey()->AddRef();
				pcrsDetermined->AddRef();
				CFunctionalDependency *pfdNew = GPOS_NEW(mp)
					CFunctionalDependency(pfd->PcrsKey(), pcrsDetermined);
				pdrgpfd->Append(pfdNew);
			}
			pcrsDetermined->Release();
		}
	}

	return pdrgpfd;
}


//---------------------------------------------------------------------------
//	@function:
//		CDrvdPropRelational::PdrgpfdLocal
//
//	@doc:
//		Helper for deriving local FDs
//
//---------------------------------------------------------------------------
CFunctionalDependencyArray *
CDrvdPropRelational::DeriveLocalFunctionalDependencies(
	CMemoryPool *mp, CExpressionHandle &exprhdl)
{
	CFunctionalDependencyArray *pdrgpfd =
		GPOS_NEW(mp) CFunctionalDependencyArray(mp);

	// get local key
	CKeyCollection *pkc = exprhdl.DeriveKeyCollection();

	if (nullptr == pkc)
	{
		return pdrgpfd;
	}

	ULONG ulKeys = pkc->Keys();
	for (ULONG ul = 0; ul < ulKeys; ul++)
	{
		CColRefArray *pdrgpcrKey = pkc->PdrgpcrKey(mp, ul);
		CColRefSet *pcrsKey = GPOS_NEW(mp) CColRefSet(mp);
		pcrsKey->Include(pdrgpcrKey);

		// get output columns
		CColRefSet *pcrsOutput = exprhdl.DeriveOutputColumns();
		CColRefSet *pcrsDetermined = GPOS_NEW(mp) CColRefSet(mp);
		pcrsDetermined->Include(pcrsOutput);
		pcrsDetermined->Exclude(pcrsKey);

		if (0 < pcrsDetermined->Size())
		{
			// add FD between key and the rest of output columns
			pcrsKey->AddRef();
			pcrsDetermined->AddRef();
			CFunctionalDependency *pfdLocal =
				GPOS_NEW(mp) CFunctionalDependency(pcrsKey, pcrsDetermined);
			pdrgpfd->Append(pfdLocal);
		}

		pcrsKey->Release();
		pcrsDetermined->Release();
		CRefCount::SafeRelease(pdrgpcrKey);
	}

	return pdrgpfd;
}


//---------------------------------------------------------------------------
//	@function:
//		CDrvdPropRelational::OsPrint
//
//	@doc:
//		Debug print
//
//---------------------------------------------------------------------------
IOstream &
CDrvdPropRelational::OsPrint(IOstream &os) const
{
	os << "Output Cols: [" << *GetOutputColumns() << "]"
	   << ", Outer Refs: [" << *GetOuterReferences() << "]"
	   << ", Not Null Cols: [" << *GetNotNullColumns() << "]"
	   << ", Corr. Apply Cols: [" << *GetCorrelatedApplyColumns() << "]";

	if (nullptr == GetKeyCollection())
	{
		os << ", Keys: []";
	}
	else
	{
		os << ", " << *GetKeyCollection();
	}

	os << ", Max Card: " << GetMaxCard();

	os << ", Join Depth: " << GetJoinDepth();

	os << ", Constraint Property: [" << *GetPropertyConstraint() << "]";

	const ULONG ulFDs = GetFunctionalDependencies()->Size();

	os << ", FDs: [";
	for (ULONG ul = 0; ul < ulFDs; ul++)
	{
		CFunctionalDependency *pfd = (*GetFunctionalDependencies())[ul];
		os << *pfd;
	}
	os << "]";

	os << ", Function Properties: [" << *GetFunctionProperties() << "]";

	os << ", Part Info: [" << *GetPartitionInfo() << "]";

	return os;
}

// output columns
CColRefSet *
CDrvdPropRelational::GetOutputColumns() const
{
	GPOS_RTL_ASSERT(IsComplete());
	return m_pcrsOutput;
}

// output columns
CColRefSet *
CDrvdPropRelational::DeriveOutputColumns(CExpressionHandle &exprhdl)
{
	if (!m_is_prop_derived->ExchangeSet(EdptPcrsOutput))
	{
		CLogical *popLogical = CLogical::PopConvert(exprhdl.Pop());
		m_pcrsOutput = popLogical->DeriveOutputColumns(m_mp, exprhdl);
	}

	return m_pcrsOutput;
}

// outer references
CColRefSet *
CDrvdPropRelational::GetOuterReferences() const
{
	GPOS_RTL_ASSERT(IsComplete());
	return m_pcrsOuter;
}

// outer references
CColRefSet *
CDrvdPropRelational::DeriveOuterReferences(CExpressionHandle &exprhdl)
{
	if (!m_is_prop_derived->ExchangeSet(EdptPcrsOuter))
	{
		CLogical *popLogical = CLogical::PopConvert(exprhdl.Pop());
		m_pcrsOuter = popLogical->DeriveOuterReferences(m_mp, exprhdl);
	}

	return m_pcrsOuter;
}

// nullable columns
CColRefSet *
CDrvdPropRelational::GetNotNullColumns() const
{
	GPOS_RTL_ASSERT(IsComplete());
	return m_pcrsNotNull;
}

CColRefSet *
CDrvdPropRelational::DeriveNotNullColumns(CExpressionHandle &exprhdl)
{
	if (!m_is_prop_derived->ExchangeSet(EdptPcrsNotNull))
	{
		CLogical *popLogical = CLogical::PopConvert(exprhdl.Pop());
		m_pcrsNotNull = popLogical->DeriveNotNullColumns(m_mp, exprhdl);
	}

	return m_pcrsNotNull;
}

// columns from the inner child of a correlated-apply expression that can be used above the apply expression
CColRefSet *
CDrvdPropRelational::GetCorrelatedApplyColumns() const
{
	GPOS_RTL_ASSERT(IsComplete());
	return m_pcrsCorrelatedApply;
}

CColRefSet *
CDrvdPropRelational::DeriveCorrelatedApplyColumns(CExpressionHandle &exprhdl)
{
	if (!m_is_prop_derived->ExchangeSet(EdptPcrsCorrelatedApply))
	{
		CLogical *popLogical = CLogical::PopConvert(exprhdl.Pop());
		m_pcrsCorrelatedApply =
			popLogical->DeriveCorrelatedApplyColumns(m_mp, exprhdl);
	}

	return m_pcrsCorrelatedApply;
}

// key collection
CKeyCollection *
CDrvdPropRelational::GetKeyCollection() const
{
	GPOS_RTL_ASSERT(IsComplete());
	return m_pkc;
}

CKeyCollection *
CDrvdPropRelational::DeriveKeyCollection(CExpressionHandle &exprhdl)
{
	if (!m_is_prop_derived->ExchangeSet(EdptPkc))
	{
		CLogical *popLogical = CLogical::PopConvert(exprhdl.Pop());
		m_pkc = popLogical->DeriveKeyCollection(m_mp, exprhdl);

		if (nullptr == m_pkc && 1 == DeriveMaxCard(exprhdl))
		{
			m_pcrsOutput = DeriveOutputColumns(exprhdl);

			if (0 < m_pcrsOutput->Size())
			{
				m_pcrsOutput->AddRef();
				m_pkc = GPOS_NEW(m_mp) CKeyCollection(m_mp, m_pcrsOutput);
			}
		}
	}

	return m_pkc;
}

// functional dependencies
CFunctionalDependencyArray *
CDrvdPropRelational::GetFunctionalDependencies() const
{
	GPOS_RTL_ASSERT(IsComplete());
	return m_pdrgpfd;
}

CFunctionalDependencyArray *
CDrvdPropRelational::DeriveFunctionalDependencies(CExpressionHandle &exprhdl)
{
	if (!m_is_prop_derived->ExchangeSet(EdptPdrgpfd))
	{
		CFunctionalDependencyArray *pdrgpfd =
			GPOS_NEW(m_mp) CFunctionalDependencyArray(m_mp);
		const ULONG arity = exprhdl.Arity();

		// collect applicable FD's from logical children
		for (ULONG ul = 0; ul < arity; ul++)
		{
			if (!exprhdl.FScalarChild(ul))
			{
				CFunctionalDependencyArray *pdrgpfdChild =
					DeriveChildFunctionalDependencies(m_mp, ul, exprhdl);
				CUtils::AddRefAppend(pdrgpfd, pdrgpfdChild);
				pdrgpfdChild->Release();
			}
		}
		// add local FD's
		CFunctionalDependencyArray *pdrgpfdLocal =
			DeriveLocalFunctionalDependencies(m_mp, exprhdl);
		CUtils::AddRefAppend(pdrgpfd, pdrgpfdLocal);
		pdrgpfdLocal->Release();

		m_pdrgpfd = pdrgpfd;
	}

	return m_pdrgpfd;
}

// max cardinality
CMaxCard
CDrvdPropRelational::GetMaxCard() const
{
	GPOS_RTL_ASSERT(IsComplete());
	return m_maxcard;
}

CMaxCard
CDrvdPropRelational::DeriveMaxCard(CExpressionHandle &exprhdl)
{
	if (!m_is_prop_derived->ExchangeSet(EdptMaxCard))
	{
		CLogical *popLogical = CLogical::PopConvert(exprhdl.Pop());
		m_maxcard = popLogical->DeriveMaxCard(m_mp, exprhdl);
	}

	return m_maxcard;
}

// join depth
ULONG
CDrvdPropRelational::GetJoinDepth() const
{
	GPOS_RTL_ASSERT(IsComplete());
	return m_ulJoinDepth;
}

ULONG
CDrvdPropRelational::DeriveJoinDepth(CExpressionHandle &exprhdl)
{
	if (!m_is_prop_derived->ExchangeSet(EdptJoinDepth))
	{
		CLogical *popLogical = CLogical::PopConvert(exprhdl.Pop());
		m_ulJoinDepth = popLogical->DeriveJoinDepth(m_mp, exprhdl);
	}

	return m_ulJoinDepth;
}

// partition consumers
CPartInfo *
CDrvdPropRelational::GetPartitionInfo() const
{
	GPOS_RTL_ASSERT(IsComplete());
	return m_ppartinfo;
}

CPartInfo *
CDrvdPropRelational::DerivePartitionInfo(CExpressionHandle &exprhdl)
{
	if (!m_is_prop_derived->ExchangeSet(EdptPpartinfo))
	{
		CLogical *popLogical = CLogical::PopConvert(exprhdl.Pop());
		m_ppartinfo = popLogical->DerivePartitionInfo(m_mp, exprhdl);

		GPOS_ASSERT(nullptr != m_ppartinfo);
	}

	return m_ppartinfo;
}

// constraint property
CPropConstraint *
CDrvdPropRelational::GetPropertyConstraint() const
{
	GPOS_RTL_ASSERT(IsComplete());
	return m_ppc;
}

CPropConstraint *
CDrvdPropRelational::DerivePropertyConstraint(CExpressionHandle &exprhdl)
{
	if (!m_is_prop_derived->ExchangeSet(EdptPpc))
	{
		CLogical *popLogical = CLogical::PopConvert(exprhdl.Pop());
		m_ppc = popLogical->DerivePropertyConstraint(m_mp, exprhdl);
	}

	return m_ppc;
}

// function properties
CFunctionProp *
CDrvdPropRelational::GetFunctionProperties() const
{
	GPOS_RTL_ASSERT(IsComplete());
	return m_pfp;
}

CFunctionProp *
CDrvdPropRelational::DeriveFunctionProperties(CExpressionHandle &exprhdl)
{
	if (!m_is_prop_derived->ExchangeSet(EdptPfp))
	{
		CLogical *popLogical = CLogical::PopConvert(exprhdl.Pop());
		m_pfp = popLogical->DeriveFunctionProperties(m_mp, exprhdl);
	}

	return m_pfp;
}

// table descriptor
CTableDescriptor *
CDrvdPropRelational::GetTableDescriptor() const
{
	GPOS_RTL_ASSERT(IsComplete());
	return m_table_descriptor;
}

CTableDescriptor *
CDrvdPropRelational::DeriveTableDescriptor(CExpressionHandle &exprhdl)
{
	if (!m_is_prop_derived->ExchangeSet(EdptTableDescriptor))
	{
		CLogical *popLogical = CLogical::PopConvert(exprhdl.Pop());
		m_table_descriptor = popLogical->DeriveTableDescriptor(m_mp, exprhdl);
	}

	return m_table_descriptor;
}
// EOF
