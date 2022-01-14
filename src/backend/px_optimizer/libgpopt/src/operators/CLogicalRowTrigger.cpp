//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CLogicalRowTrigger.cpp
//
//	@doc:
//		Implementation of logical row-level trigger operator
//---------------------------------------------------------------------------

#include "gpopt/operators/CLogicalRowTrigger.h"

#include "gpos/base.h"

#include "gpopt/base/COptCtxt.h"
#include "gpopt/operators/CExpression.h"
#include "gpopt/operators/CExpressionHandle.h"
#include "naucrates/md/CMDTriggerGPDB.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CLogicalRowTrigger::CLogicalRowTrigger
//
//	@doc:
//		Ctor - for pattern
//
//---------------------------------------------------------------------------
CLogicalRowTrigger::CLogicalRowTrigger(CMemoryPool *mp)
	: CLogical(mp),
	  m_rel_mdid(nullptr),
	  m_type(0),
	  m_pdrgpcrOld(nullptr),
	  m_pdrgpcrNew(nullptr)
{
	m_fPattern = true;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalRowTrigger::CLogicalRowTrigger
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CLogicalRowTrigger::CLogicalRowTrigger(CMemoryPool *mp, IMDId *rel_mdid,
									   INT type, CColRefArray *pdrgpcrOld,
									   CColRefArray *pdrgpcrNew)
	: CLogical(mp),
	  m_rel_mdid(rel_mdid),
	  m_type(type),
	  m_pdrgpcrOld(pdrgpcrOld),
	  m_pdrgpcrNew(pdrgpcrNew)
{
	GPOS_ASSERT(rel_mdid->IsValid());
	GPOS_ASSERT(0 != type);
	GPOS_ASSERT(nullptr != pdrgpcrNew || nullptr != pdrgpcrOld);
	GPOS_ASSERT_IMP(nullptr != pdrgpcrNew && nullptr != pdrgpcrOld,
					pdrgpcrNew->Size() == pdrgpcrOld->Size());
	InitFunctionProperties();
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalRowTrigger::~CLogicalRowTrigger
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CLogicalRowTrigger::~CLogicalRowTrigger()
{
	CRefCount::SafeRelease(m_rel_mdid);
	CRefCount::SafeRelease(m_pdrgpcrOld);
	CRefCount::SafeRelease(m_pdrgpcrNew);
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalRowTrigger::InitFunctionProperties
//
//	@doc:
//		Initialize function properties
//
//---------------------------------------------------------------------------
void
CLogicalRowTrigger::InitFunctionProperties()
{
	CMDAccessor *md_accessor = COptCtxt::PoctxtFromTLS()->Pmda();
	const IMDRelation *pmdrel = md_accessor->RetrieveRel(m_rel_mdid);
	const ULONG ulTriggers = pmdrel->TriggerCount();

	for (ULONG ul = 0; ul < ulTriggers; ul++)
	{
		const IMDTrigger *pmdtrigger =
			md_accessor->RetrieveTrigger(pmdrel->TriggerMDidAt(ul));
		if (!pmdtrigger->IsEnabled() || !pmdtrigger->ExecutesOnRowLevel() ||
			(ITriggerType(pmdtrigger) & m_type) != m_type)
		{
			continue;
		}

		const IMDFunction *pmdfunc =
			md_accessor->RetrieveFunc(pmdtrigger->FuncMdId());
		IMDFunction::EFuncStbl efs = pmdfunc->GetFuncStability();

		if (efs > m_efs)
		{
			m_efs = efs;
		}
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalRowTrigger::ITriggerType
//
//	@doc:
//		Return the type of a given trigger as an integer
//
//---------------------------------------------------------------------------
INT
CLogicalRowTrigger::ITriggerType(const IMDTrigger *pmdtrigger)
{
	INT type = GPMD_TRIGGER_ROW;
	if (pmdtrigger->IsBefore())
	{
		type |= GPMD_TRIGGER_BEFORE;
	}

	if (pmdtrigger->IsInsert())
	{
		type |= GPMD_TRIGGER_INSERT;
	}

	if (pmdtrigger->IsDelete())
	{
		type |= GPMD_TRIGGER_DELETE;
	}

	if (pmdtrigger->IsUpdate())
	{
		type |= GPMD_TRIGGER_UPDATE;
	}

	return type;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalRowTrigger::Matches
//
//	@doc:
//		Match function
//
//---------------------------------------------------------------------------
BOOL
CLogicalRowTrigger::Matches(COperator *pop) const
{
	if (pop->Eopid() != Eopid())
	{
		return false;
	}

	CLogicalRowTrigger *popRowTrigger = CLogicalRowTrigger::PopConvert(pop);

	return m_rel_mdid->Equals(popRowTrigger->GetRelMdId()) &&
		   m_type == popRowTrigger->GetType() &&
		   m_pdrgpcrOld->Equals(popRowTrigger->PdrgpcrOld()) &&
		   m_pdrgpcrNew->Equals(popRowTrigger->PdrgpcrNew());
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalRowTrigger::HashValue
//
//	@doc:
//		Hash function
//
//---------------------------------------------------------------------------
ULONG
CLogicalRowTrigger::HashValue() const
{
	ULONG ulHash =
		gpos::CombineHashes(COperator::HashValue(), m_rel_mdid->HashValue());
	ulHash = gpos::CombineHashes(ulHash, gpos::HashValue<INT>(&m_type));

	if (nullptr != m_pdrgpcrOld)
	{
		ulHash =
			gpos::CombineHashes(ulHash, CUtils::UlHashColArray(m_pdrgpcrOld));
	}

	if (nullptr != m_pdrgpcrNew)
	{
		ulHash =
			gpos::CombineHashes(ulHash, CUtils::UlHashColArray(m_pdrgpcrNew));
	}

	return ulHash;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalRowTrigger::PopCopyWithRemappedColumns
//
//	@doc:
//		Return a copy of the operator with remapped columns
//
//---------------------------------------------------------------------------
COperator *
CLogicalRowTrigger::PopCopyWithRemappedColumns(CMemoryPool *mp,
											   UlongToColRefMap *colref_mapping,
											   BOOL must_exist)
{
	CColRefArray *pdrgpcrOld = nullptr;
	if (nullptr != m_pdrgpcrOld)
	{
		pdrgpcrOld =
			CUtils::PdrgpcrRemap(mp, m_pdrgpcrOld, colref_mapping, must_exist);
	}

	CColRefArray *pdrgpcrNew = nullptr;
	if (nullptr != m_pdrgpcrNew)
	{
		pdrgpcrNew =
			CUtils::PdrgpcrRemap(mp, m_pdrgpcrNew, colref_mapping, must_exist);
	}

	m_rel_mdid->AddRef();

	return GPOS_NEW(mp)
		CLogicalRowTrigger(mp, m_rel_mdid, m_type, pdrgpcrOld, pdrgpcrNew);
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalRowTrigger::DeriveOutputColumns
//
//	@doc:
//		Derive output columns
//
//---------------------------------------------------------------------------
CColRefSet *
CLogicalRowTrigger::DeriveOutputColumns(CMemoryPool *,	//mp,
										CExpressionHandle &exprhdl)
{
	return PcrsDeriveOutputPassThru(exprhdl);
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalRowTrigger::PkcDeriveKeys
//
//	@doc:
//		Derive key collection
//
//---------------------------------------------------------------------------
CKeyCollection *
CLogicalRowTrigger::DeriveKeyCollection(CMemoryPool *,	// mp
										CExpressionHandle &exprhdl) const
{
	return PkcDeriveKeysPassThru(exprhdl, 0 /* ulChild */);
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalRowTrigger::DeriveMaxCard
//
//	@doc:
//		Derive max card
//
//---------------------------------------------------------------------------
CMaxCard
CLogicalRowTrigger::DeriveMaxCard(CMemoryPool *,  // mp
								  CExpressionHandle &exprhdl) const
{
	// pass on max card of first child
	return exprhdl.DeriveMaxCard(0);
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalRowTrigger::PxfsCandidates
//
//	@doc:
//		Get candidate xforms
//
//---------------------------------------------------------------------------
CXformSet *
CLogicalRowTrigger::PxfsCandidates(CMemoryPool *mp) const
{
	CXformSet *xform_set = GPOS_NEW(mp) CXformSet(mp);
	(void) xform_set->ExchangeSet(CXform::ExfImplementRowTrigger);
	return xform_set;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalRowTrigger::PstatsDerive
//
//	@doc:
//		Derive statistics
//
//---------------------------------------------------------------------------
IStatistics *
CLogicalRowTrigger::PstatsDerive(CMemoryPool *,	 // mp,
								 CExpressionHandle &exprhdl,
								 IStatisticsArray *	 // not used
) const
{
	return PstatsPassThruOuter(exprhdl);
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalRowTrigger::DeriveFunctionProperties
//
//	@doc:
//		Derive function properties
//
//---------------------------------------------------------------------------
CFunctionProp *
CLogicalRowTrigger::DeriveFunctionProperties(CMemoryPool *mp,
											 CExpressionHandle &exprhdl) const
{
	return PfpDeriveFromChildren(mp, exprhdl, m_efs,
								 false /*fHasVolatileFunctionScan*/,
								 false /*fScan*/);
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalRowTrigger::OsPrint
//
//	@doc:
//		debug print
//
//---------------------------------------------------------------------------
IOstream &
CLogicalRowTrigger::OsPrint(IOstream &os) const
{
	if (m_fPattern)
	{
		return COperator::OsPrint(os);
	}

	os << SzId() << " (Type: " << m_type << ")";

	if (nullptr != m_pdrgpcrOld)
	{
		os << ", Old Columns: [";
		CUtils::OsPrintDrgPcr(os, m_pdrgpcrOld);
		os << "]";
	}

	if (nullptr != m_pdrgpcrNew)
	{
		os << ", New Columns: [";
		CUtils::OsPrintDrgPcr(os, m_pdrgpcrNew);
		os << "]";
	}

	return os;
}

// EOF
