//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2009 Greenplum, Inc.
//	Copyright (C) 2021, Alibaba Group Holding Limited
//
//	@filename:
//		CTableDescriptor.cpp
//
//	@doc:
//		Implementation of table abstraction
//---------------------------------------------------------------------------

#include "gpopt/metadata/CTableDescriptor.h"

#include "gpos/base.h"
#include "gpos/memory/CAutoMemoryPool.h"

#include "gpopt/base/CColumnFactory.h"
#include "gpopt/base/CDistributionSpecAny.h"
#include "gpopt/base/CDistributionSpecSingleton.h"
#include "gpopt/base/COptCtxt.h"
#include "gpopt/base/CUtils.h"
#include "naucrates/exception.h"
#include "naucrates/md/IMDIndex.h"

using namespace gpopt;

FORCE_GENERATE_DBGSTR(CTableDescriptor);

//---------------------------------------------------------------------------
//	@function:
//		CTableDescriptor::CTableDescriptor
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CTableDescriptor::CTableDescriptor(
	CMemoryPool *mp, IMDId *mdid, const CName &name,
	BOOL convert_hash_to_random, IMDRelation::Ereldistrpolicy rel_distr_policy,
	IMDRelation::Erelstoragetype erelstoragetype, ULONG ulExecuteAsUser,
	INT lockmode)
	: m_mp(mp),
	  m_mdid(mdid),
	  m_name(mp, name),
	  m_pdrgpcoldesc(nullptr),
	  m_rel_distr_policy(rel_distr_policy),
	  m_erelstoragetype(erelstoragetype),
	  m_pdrgpcoldescDist(nullptr),
	  m_distr_opfamilies(nullptr),
	  m_convert_hash_to_random(convert_hash_to_random),
	  m_pdrgpulPart(nullptr),
	  m_pdrgpbsKeys(nullptr),
	  m_execute_as_user_id(ulExecuteAsUser),
	  m_lockmode(lockmode)
{
	GPOS_ASSERT(nullptr != mp);
	GPOS_ASSERT(mdid->IsValid());

	m_pdrgpcoldesc = GPOS_NEW(m_mp) CColumnDescriptorArray(m_mp);
	m_pdrgpcoldescDist = GPOS_NEW(m_mp) CColumnDescriptorArray(m_mp);
	m_pdrgpulPart = GPOS_NEW(m_mp) ULongPtrArray(m_mp);
	m_pdrgpbsKeys = GPOS_NEW(m_mp) CBitSetArray(m_mp);
	if (GPOS_FTRACE(EopttraceConsiderOpfamiliesForDistribution))
	{
		m_distr_opfamilies = GPOS_NEW(m_mp) IMdIdArray(m_mp);
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CTableDescriptor::~CTableDescriptor
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CTableDescriptor::~CTableDescriptor()
{
	m_mdid->Release();

	m_pdrgpcoldesc->Release();
	m_pdrgpcoldescDist->Release();
	m_pdrgpulPart->Release();
	m_pdrgpbsKeys->Release();
	CRefCount::SafeRelease(m_distr_opfamilies);
}


//---------------------------------------------------------------------------
//	@function:
//		CTableDescriptor::ColumnCount
//
//	@doc:
//		number of columns
//
//---------------------------------------------------------------------------
ULONG
CTableDescriptor::ColumnCount() const
{
	// array allocated in ctor
	GPOS_ASSERT(nullptr != m_pdrgpcoldesc);

	return m_pdrgpcoldesc->Size();
}

//---------------------------------------------------------------------------
//	@function:
//		CTableDescriptor::UlPos
//
//	@doc:
//		Find the position of a column descriptor in an array of column descriptors.
//		If not found, return the size of the array
//
//---------------------------------------------------------------------------
ULONG
CTableDescriptor::UlPos(const CColumnDescriptor *pcoldesc,
						const CColumnDescriptorArray *pdrgpcoldesc)
{
	GPOS_ASSERT(nullptr != pcoldesc);
	GPOS_ASSERT(nullptr != pdrgpcoldesc);

	ULONG arity = pdrgpcoldesc->Size();
	for (ULONG ul = 0; ul < arity; ul++)
	{
		if (pcoldesc == (*pdrgpcoldesc)[ul])
		{
			return ul;
		}
	}

	return arity;
}

//---------------------------------------------------------------------------
//	@function:
//		CTableDescriptor::GetAttributePosition
//
//	@doc:
//		Find the position of the attribute in the array of column descriptors
//
//---------------------------------------------------------------------------
ULONG
CTableDescriptor::GetAttributePosition(INT attno) const
{
	GPOS_ASSERT(nullptr != m_pdrgpcoldesc);
	ULONG ulPos = gpos::ulong_max;
	ULONG arity = m_pdrgpcoldesc->Size();

	for (ULONG ul = 0; ul < arity; ul++)
	{
		CColumnDescriptor *pcoldesc = (*m_pdrgpcoldesc)[ul];
		if (pcoldesc->AttrNum() == attno)
		{
			ulPos = ul;
		}
	}
	GPOS_ASSERT(gpos::ulong_max != ulPos);

	return ulPos;
}

//---------------------------------------------------------------------------
//	@function:
//		CTableDescriptor::AddColumn
//
//	@doc:
//		Add column to table descriptor
//
//---------------------------------------------------------------------------
void
CTableDescriptor::AddColumn(CColumnDescriptor *pcoldesc)
{
	GPOS_ASSERT(nullptr != pcoldesc);

	m_pdrgpcoldesc->Append(pcoldesc);
}

//---------------------------------------------------------------------------
//	@function:
//		CTableDescriptor::AddDistributionColumn
//
//	@doc:
//		Add the column at the specified position to the array of column
//		descriptors defining a hash distribution
//
//---------------------------------------------------------------------------
void
CTableDescriptor::AddDistributionColumn(ULONG ulPos, IMDId *opfamily)
{
	CColumnDescriptor *pcoldesc = (*m_pdrgpcoldesc)[ulPos];
	pcoldesc->AddRef();
	m_pdrgpcoldescDist->Append(pcoldesc);
	pcoldesc->SetAsDistCol();

	if (GPOS_FTRACE(EopttraceConsiderOpfamiliesForDistribution))
	{
		GPOS_ASSERT(nullptr != opfamily && opfamily->IsValid());
		opfamily->AddRef();
		m_distr_opfamilies->Append(opfamily);

		GPOS_ASSERT(m_pdrgpcoldescDist->Size() == m_distr_opfamilies->Size());
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CTableDescriptor::SetDistributionColumn
//
//	@doc:
//		Polar PX for DML
//		
//
//---------------------------------------------------------------------------
void CTableDescriptor::SetDistributionColumn() {
	if (m_pdrgpcoldescDist->Size())
		return;
	// Spec for first column
	CColumnDescriptor *pcoldesc = (*m_pdrgpcoldesc)[0];
	pcoldesc->AddRef();
	m_pdrgpcoldescDist->Append(pcoldesc);
	pcoldesc->SetAsDistCol();
}


//---------------------------------------------------------------------------
//	@function:
//		CTableDescriptor::AddPartitionColumn
//
//	@doc:
//		Add the column at the specified position to the array of partition column
//		descriptors
//
//---------------------------------------------------------------------------
void
CTableDescriptor::AddPartitionColumn(ULONG ulPos)
{
	m_pdrgpulPart->Append(GPOS_NEW(m_mp) ULONG(ulPos));
}

//---------------------------------------------------------------------------
//	@function:
//		CTableDescriptor::FAddKeySet
//
//	@doc:
//		Add a keyset, returns true if key set is successfully added
//
//---------------------------------------------------------------------------
BOOL
CTableDescriptor::FAddKeySet(CBitSet *pbs)
{
	GPOS_ASSERT(nullptr != pbs);
	GPOS_ASSERT(pbs->Size() <= m_pdrgpcoldesc->Size());

	const ULONG size = m_pdrgpbsKeys->Size();
	BOOL fFound = false;
	for (ULONG ul = 0; !fFound && ul < size; ul++)
	{
		CBitSet *pbsCurrent = (*m_pdrgpbsKeys)[ul];
		fFound = pbsCurrent->Equals(pbs);
	}

	if (!fFound)
	{
		m_pdrgpbsKeys->Append(pbs);
	}

	return !fFound;
}

//---------------------------------------------------------------------------
//	@function:
//		CTableDescriptor::Pcoldesc
//
//	@doc:
//		Get n-th column descriptor
//
//---------------------------------------------------------------------------
const CColumnDescriptor *
CTableDescriptor::Pcoldesc(ULONG ulCol) const
{
	GPOS_ASSERT(ulCol < ColumnCount());

	return (*m_pdrgpcoldesc)[ulCol];
}


//---------------------------------------------------------------------------
//	@function:
//		CTableDescriptor::OsPrint
//
//	@doc:
//		debug print
//
//---------------------------------------------------------------------------
IOstream &
CTableDescriptor::OsPrint(IOstream &os) const
{
	m_name.OsPrint(os);
	os << ": (";
	CUtils::OsPrintDrgPcoldesc(os, m_pdrgpcoldesc, m_pdrgpcoldesc->Size());
	os << ")";
	return os;
}


//---------------------------------------------------------------------------
//	@function:
//		CTableDescriptor::IndexCount
//
//	@doc:
//		 Returns number of b-tree indices
//
//
//---------------------------------------------------------------------------
ULONG
CTableDescriptor::IndexCount()
{
	GPOS_ASSERT(nullptr != m_mdid);

	CMDAccessor *md_accessor = COptCtxt::PoctxtFromTLS()->Pmda();
	const IMDRelation *pmdrel = md_accessor->RetrieveRel(m_mdid);
	const ULONG ulIndices = pmdrel->IndexCount();

	return ulIndices;
}

//---------------------------------------------------------------------------
//	@function:
//		CTableDescriptor::PartitionCount
//
//	@doc:
//		 Returns number of leaf partitions
//
//
//---------------------------------------------------------------------------
ULONG
CTableDescriptor::PartitionCount() const
{
	GPOS_ASSERT(nullptr != m_mdid);

	CMDAccessor *md_accessor = COptCtxt::PoctxtFromTLS()->Pmda();
	const IMDRelation *pmdrel = md_accessor->RetrieveRel(m_mdid);
	const ULONG ulPartitions = pmdrel->PartitionCount();

	return ulPartitions;
}


// EOF
