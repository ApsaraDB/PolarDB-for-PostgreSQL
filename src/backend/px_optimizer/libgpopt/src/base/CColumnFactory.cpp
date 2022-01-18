//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2008, 2009 Greenplum, Inc.
//
//	@filename:
//		CColumnFactory.cpp
//
//	@doc:
//		Implementation of column reference management
//---------------------------------------------------------------------------

#include "gpopt/base/CColumnFactory.h"

#include "gpos/base.h"
#include "gpos/common/CAutoP.h"
#include "gpos/common/CSyncHashtableAccessByKey.h"
#include "gpos/memory/CAutoMemoryPool.h"
#include "gpos/string/CWStringDynamic.h"

#include "gpopt/base/CColRefComputed.h"
#include "gpopt/base/CColRefTable.h"
#include "gpopt/operators/CExpression.h"
#include "gpopt/operators/CScalarProjectElement.h"
#include "naucrates/md/CMDIdGPDB.h"

using namespace gpopt;
using namespace gpmd;

#define GPOPT_COLFACTORY_HT_BUCKETS 9999

//---------------------------------------------------------------------------
//	@function:
//		CColumnFactory::CColumnFactory
//
//	@doc:
//		ctor
//
//---------------------------------------------------------------------------
CColumnFactory::CColumnFactory()
{
	CAutoMemoryPool amp;
	m_mp = amp.Pmp();

	// initialize hash table
	m_sht.Init(m_mp, GPOPT_COLFACTORY_HT_BUCKETS, GPOS_OFFSET(CColRef, m_link),
			   GPOS_OFFSET(CColRef, m_id), &(CColRef::m_ulInvalid),
			   CColRef::HashValue, CColRef::Equals);

	// now it's safe to detach the auto pool
	(void) amp.Detach();
}


//---------------------------------------------------------------------------
//	@function:
//		CColumnFactory::~CColumnFactory
//
//	@doc:
//		dtor
//
//---------------------------------------------------------------------------
CColumnFactory::~CColumnFactory()
{
	CRefCount::SafeRelease(m_phmcrcrs);

	// dealloc hash table
	m_sht.Cleanup();

	// destroy mem pool
	CMemoryPoolManager *pmpm = CMemoryPoolManager::GetMemoryPoolMgr();
	pmpm->Destroy(m_mp);
}

//---------------------------------------------------------------------------
//	@function:
//		CColumnFactory::Initialize
//
//	@doc:
//		Initialize the hash map between computed column and used columns
//
//---------------------------------------------------------------------------
void
CColumnFactory::Initialize()
{
	m_phmcrcrs = GPOS_NEW(m_mp) ColRefToColRefSetMap(m_mp);
}

//---------------------------------------------------------------------------
//	@function:
//		CColumnFactory::PcrCreate
//
//	@doc:
//		Variant without name for computed columns
//
//---------------------------------------------------------------------------
CColRef *
CColumnFactory::PcrCreate(const IMDType *pmdtype, INT type_modifier)
{
	// increment atomic counter
	ULONG id = m_aul++;

	WCHAR wszFmt[] = GPOS_WSZ_LIT("ColRef_%04d");
	CWStringDynamic *pstrTempName = GPOS_NEW(m_mp) CWStringDynamic(m_mp);
	CAutoP<CWStringDynamic> a_pstrTempName(pstrTempName);
	pstrTempName->AppendFormat(wszFmt, id);
	CWStringConst strName(pstrTempName->GetBuffer());
	return PcrCreate(pmdtype, type_modifier, id, CName(&strName));
}


//---------------------------------------------------------------------------
//	@function:
//		CColumnFactory::PcrCreate
//
//	@doc:
//		Variant without name for computed columns
//
//---------------------------------------------------------------------------
CColRef *
CColumnFactory::PcrCreate(const IMDType *pmdtype, INT type_modifier,
						  const CName &name)
{
	ULONG id = m_aul++;

	return PcrCreate(pmdtype, type_modifier, id, name);
}


//---------------------------------------------------------------------------
//	@function:
//		CColumnFactory::PcrCreate
//
//	@doc:
//		Basic implementation of all factory methods;
//		Name and id have already determined, we just create the ColRef and
//		insert it into the hashtable
//
//---------------------------------------------------------------------------
CColRef *
CColumnFactory::PcrCreate(const IMDType *pmdtype, INT type_modifier, ULONG id,
						  const CName &name)
{
	CName *pnameCopy = GPOS_NEW(m_mp) CName(m_mp, name);
	CAutoP<CName> a_pnameCopy(pnameCopy);

	CColRef *colref =
		GPOS_NEW(m_mp) CColRefComputed(pmdtype, type_modifier, id, pnameCopy);
	(void) a_pnameCopy.Reset();
	CAutoP<CColRef> a_pcr(colref);

	// ensure uniqueness
	GPOS_ASSERT(nullptr == LookupColRef(id));
	m_sht.Insert(colref);
	colref->MarkAsUsed();

	return a_pcr.Reset();
}


//---------------------------------------------------------------------------
//	@function:
//		CColumnFactory::PcrCreate
//
//	@doc:
//		Basic implementation of all factory methods;
//		Name and id have already determined, we just create the ColRef and
//		insert it into the hashtable
//
//---------------------------------------------------------------------------
CColRef *
CColumnFactory::PcrCreate(const CColumnDescriptor *pcoldesc, ULONG id,
						  const CName &name, ULONG ulOpSource,
						  BOOL mark_as_used, IMDId *mdid_table)
{
	CName *pnameCopy = GPOS_NEW(m_mp) CName(m_mp, name);
	CAutoP<CName> a_pnameCopy(pnameCopy);

	CColRef *colref =
		GPOS_NEW(m_mp) CColRefTable(pcoldesc, id, pnameCopy, ulOpSource);
	(void) a_pnameCopy.Reset();
	CAutoP<CColRef> a_pcr(colref);

	// ensure uniqueness
	GPOS_ASSERT(nullptr == LookupColRef(id));
	m_sht.Insert(colref);
	if (mark_as_used)
	{
		colref->MarkAsUsed();
	}
	if (mdid_table)
	{
		colref->SetMdidTable(mdid_table);
	}

	return a_pcr.Reset();
}


//---------------------------------------------------------------------------
//	@function:
//		CColumnFactory::PcrCreate
//
//	@doc:
//		Basic implementation of all factory methods;
//		Name and id have already determined, we just create the ColRef and
//		insert it into the hashtable
//
//---------------------------------------------------------------------------
CColRef *
CColumnFactory::PcrCreate(const IMDType *pmdtype, INT type_modifier,
						  IMDId *mdid_table, INT attno, BOOL is_nullable,
						  ULONG id, const CName &name, ULONG ulOpSource,
						  BOOL isDistCol, ULONG ulWidth)
{
	CName *pnameCopy = GPOS_NEW(m_mp) CName(m_mp, name);
	CAutoP<CName> a_pnameCopy(pnameCopy);

	CColRef *colref = GPOS_NEW(m_mp)
		CColRefTable(pmdtype, type_modifier, attno, is_nullable, id, pnameCopy,
					 ulOpSource, isDistCol, ulWidth);
	(void) a_pnameCopy.Reset();
	CAutoP<CColRef> a_pcr(colref);

	// ensure uniqueness
	GPOS_ASSERT(nullptr == LookupColRef(id));
	m_sht.Insert(colref);
	colref->MarkAsUsed();
	colref->SetMdidTable(mdid_table);

	return a_pcr.Reset();
}

//---------------------------------------------------------------------------
//	@function:
//		CColumnFactory::PcrCreate
//
//	@doc:
//		Variant with alias/name for base table columns
//
//---------------------------------------------------------------------------
CColRef *
CColumnFactory::PcrCreate(const CColumnDescriptor *pcoldesc, const CName &name,
						  ULONG ulOpSource, BOOL mark_as_used,
						  IMDId *mdid_table)
{
	ULONG id = m_aul++;

	return PcrCreate(pcoldesc, id, name, ulOpSource, mark_as_used, mdid_table);
}

//---------------------------------------------------------------------------
//	@function:
//		CColumnFactory::PcrCopy
//
//	@doc:
//		Create a copy of the given colref
//
//---------------------------------------------------------------------------
CColRef *
CColumnFactory::PcrCopy(const CColRef *colref)
{
	CName name(colref->Name());
	if (CColRef::EcrtComputed == colref->Ecrt())
	{
		return PcrCreate(colref->RetrieveType(), colref->TypeModifier(), name);
	}

	GPOS_ASSERT(CColRef::EcrtTable == colref->Ecrt());
	ULONG id = m_aul++;
	CColRefTable *pcrTable =
		CColRefTable::PcrConvert(const_cast<CColRef *>(colref));

	return PcrCreate(colref->RetrieveType(), colref->TypeModifier(),
					 colref->GetMdidTable(), pcrTable->AttrNum(),
					 pcrTable->IsNullable(), id, name, pcrTable->UlSourceOpId(),
					 colref->IsDistCol(), pcrTable->Width());
}

//---------------------------------------------------------------------------
//	@function:
//		CColumnFactory::LookupColRef
//
//	@doc:
//		Lookup by id
//
//---------------------------------------------------------------------------
CColRef *
CColumnFactory::LookupColRef(ULONG id)
{
	CSyncHashtableAccessByKey<CColRef, ULONG> shtacc(m_sht, id);

	CColRef *colref = shtacc.Find();

	return colref;
}


//---------------------------------------------------------------------------
//	@function:
//		CColumnFactory::Destroy
//
//	@doc:
//		unlink and destruct
//
//---------------------------------------------------------------------------
void
CColumnFactory::Destroy(CColRef *colref)
{
	GPOS_ASSERT(nullptr != colref);

	ULONG id = colref->m_id;

	{
		// scope for the hash table accessor
		CSyncHashtableAccessByKey<CColRef, ULONG> shtacc(m_sht, id);

		CColRef *pcrFound = shtacc.Find();
		GPOS_ASSERT(colref == pcrFound);

		// unlink from hashtable
		shtacc.Remove(pcrFound);
	}

	GPOS_DELETE(colref);
}


//---------------------------------------------------------------------------
//	@function:
//		CColumnFactory::PcrsUsedInComputedCol
//
//	@doc:
//		Lookup the set of used column references (if any) based on id of
//		computed column
//---------------------------------------------------------------------------
const CColRefSet *
CColumnFactory::PcrsUsedInComputedCol(const CColRef *colref)
{
	GPOS_ASSERT(nullptr != colref);
	GPOS_ASSERT(nullptr != m_phmcrcrs);

	// get its column reference set from the hash map
	const CColRefSet *pcrs = m_phmcrcrs->Find(colref);

	return pcrs;
}


//---------------------------------------------------------------------------
//	@function:
//		CColumnFactory::AddComputedToUsedColsMap
//
//	@doc:
//		Add the map between computed column and its used columns
//
//---------------------------------------------------------------------------
void
CColumnFactory::AddComputedToUsedColsMap(CExpression *pexpr)
{
	GPOS_ASSERT(nullptr != pexpr);
	GPOS_ASSERT(nullptr != m_phmcrcrs);

	const CScalarProjectElement *popScPrEl =
		CScalarProjectElement::PopConvert(pexpr->Pop());
	CColRef *pcrComputedCol = popScPrEl->Pcr();

	CColRefSet *pcrsUsed = pexpr->DeriveUsedColumns();
	if (nullptr != pcrsUsed && 0 < pcrsUsed->Size())
	{
		BOOL fres GPOS_ASSERTS_ONLY = m_phmcrcrs->Insert(
			pcrComputedCol, GPOS_NEW(m_mp) CColRefSet(m_mp, *pcrsUsed));
		GPOS_ASSERT(fres);
	}
}


// EOF
