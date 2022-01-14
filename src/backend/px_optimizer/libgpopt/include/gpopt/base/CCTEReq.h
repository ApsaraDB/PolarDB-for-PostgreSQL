//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 EMC Corp.
//
//	@filename:
//		CCTEReq.h
//
//	@doc:
//		CTE requirements. Each entry has a CTE id, whether it is a producer or
//		a consumer, whether it is required or optional (a required CTE has to
//		be in the derived CTE map of that subtree, while an optional CTE may or
//		may not be there). If the CTE entry represents a consumer, then the
//		plan properties of the corresponding producer are also part of that entry
//---------------------------------------------------------------------------
#ifndef GPOPT_CCTEReq_H
#define GPOPT_CCTEReq_H

#include "gpos/base.h"
#include "gpos/common/CHashMap.h"
#include "gpos/common/CHashMapIter.h"
#include "gpos/common/CRefCount.h"

#include "gpopt/base/CCTEMap.h"
#include "gpopt/base/CDrvdPropPlan.h"

namespace gpopt
{
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CCTEReq
//
//	@doc:
//		CTE requirements
//
//---------------------------------------------------------------------------
class CCTEReq : public CRefCount, public DbgPrintMixin<CCTEReq>
{
private:
	//---------------------------------------------------------------------------
	//	@class:
	//		CCTEReqEntry
	//
	//	@doc:
	//		A single entry in the CTE requirement
	//
	//---------------------------------------------------------------------------
	class CCTEReqEntry : public CRefCount, public DbgPrintMixin<CCTEReqEntry>
	{
	private:
		// cte id
		ULONG m_id;

		// cte type
		CCTEMap::ECteType m_ect;

		// is it required or optional
		BOOL m_fRequired;

		// plan properties of corresponding producer
		CDrvdPropPlan *m_pdpplan;

	public:
		CCTEReqEntry(const CCTEReqEntry &) = delete;

		// ctor
		CCTEReqEntry(ULONG id, CCTEMap::ECteType ect, BOOL fRequired,
					 CDrvdPropPlan *pdpplan);

		// dtor
		~CCTEReqEntry() override;

		// cte id
		ULONG
		Id() const
		{
			return m_id;
		}

		// cte type
		CCTEMap::ECteType
		Ect() const
		{
			return m_ect;
		}

		// required flag
		BOOL
		FRequired() const
		{
			return m_fRequired;
		}

		// plan properties
		CDrvdPropPlan *
		PdpplanProducer() const
		{
			return m_pdpplan;
		}

		// hash function
		ULONG HashValue() const;

		// equality function
		BOOL Equals(CCTEReqEntry *pcre) const;

		// print function
		IOstream &OsPrint(IOstream &os) const;

	};	// class CCTEReqEntry

	// map CTE id to CTE Req entry
	typedef CHashMap<ULONG, CCTEReqEntry, gpos::HashValue<ULONG>,
					 gpos::Equals<ULONG>, CleanupDelete<ULONG>,
					 CleanupRelease<CCTEReqEntry> >
		UlongToCTEReqEntryMap;

	// map iterator
	typedef CHashMapIter<ULONG, CCTEReqEntry, gpos::HashValue<ULONG>,
						 gpos::Equals<ULONG>, CleanupDelete<ULONG>,
						 CleanupRelease<CCTEReqEntry> >
		UlongToCTEReqEntryMapIter;

	// memory pool
	CMemoryPool *m_mp;

	// cte map
	UlongToCTEReqEntryMap *m_phmcter;

	// required cte ids (not optional)
	ULongPtrArray *m_pdrgpulRequired;

	// lookup info for given cte id
	CCTEReqEntry *PcreLookup(ULONG ulCteId) const;

public:
	CCTEReq(const CCTEReq &) = delete;

	// ctor
	explicit CCTEReq(CMemoryPool *mp);

	// dtor
	~CCTEReq() override;

	// required cte ids
	ULongPtrArray *
	PdrgpulRequired() const
	{
		return m_pdrgpulRequired;
	}

	// return the CTE type associated with the given ID in the requirements
	CCTEMap::ECteType Ect(const ULONG id) const;

	// insert a new entry, no entry with the same id can already exist
	void Insert(ULONG ulCteId, CCTEMap::ECteType ect, BOOL fRequired,
				CDrvdPropPlan *pdpplan);

	// insert a new consumer entry with the given id. The plan properties are
	// taken from the given context
	void InsertConsumer(ULONG id, CDrvdPropArray *pdrgpdpCtxt);

	// check if two cte requirements are equal
	BOOL
	Equals(const CCTEReq *pcter) const
	{
		GPOS_ASSERT(nullptr != pcter);
		return (m_phmcter->Size() == pcter->m_phmcter->Size()) &&
			   this->FSubset(pcter);
	}

	// check if current requirement is a subset of the given one
	BOOL FSubset(const CCTEReq *pcter) const;

	// check if the given CTE is in the requirements
	BOOL FContainsRequirement(const ULONG id,
							  const CCTEMap::ECteType ect) const;

	// hash function
	ULONG HashValue() const;

	// returns a new requirement containing unresolved CTE requirements given a derived CTE map
	CCTEReq *PcterUnresolved(CMemoryPool *mp, CCTEMap *pcm);

	// unresolved CTE requirements given a derived CTE map for a sequence
	// operator
	CCTEReq *PcterUnresolvedSequence(CMemoryPool *mp, CCTEMap *pcm,
									 CDrvdPropArray *pdrgpdpCtxt);

	// create a copy of the current requirement where all the entries are marked optional
	CCTEReq *PcterAllOptional(CMemoryPool *mp);

	// lookup plan properties for given cte id
	CDrvdPropPlan *Pdpplan(ULONG ulCteId) const;

	// print function
	IOstream &OsPrint(IOstream &os) const;

};	// class CCTEMap

// shorthand for printing
IOstream &operator<<(IOstream &os, CCTEReq &cter);

}  // namespace gpopt

#endif	// !GPOPT_CCTEMap_H

// EOF
