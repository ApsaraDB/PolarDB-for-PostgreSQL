//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 VMware, Inc. or its affiliates.
//
//	@filename:
//		CPartKeys.h
//
//	@doc:
//		A collection of partitioning keys for a partitioned table
//---------------------------------------------------------------------------
#ifndef GPOPT_CPartKeys_H
#define GPOPT_CPartKeys_H

#include "gpos/base.h"

#include "gpopt/base/CColRef.h"

namespace gpopt
{
using namespace gpos;

// fwd decl
class CColRefSet;
class CPartKeys;

// array of part keys
typedef CDynamicPtrArray<CPartKeys, CleanupRelease> CPartKeysArray;

//---------------------------------------------------------------------------
//	@class:
//		CPartKeys
//
//	@doc:
//		A collection of partitioning keys for a partitioned table
//
//---------------------------------------------------------------------------
class CPartKeys : public CRefCount, public DbgPrintMixin<CPartKeys>
{
private:
	// partitioning keys
	CColRef2dArray *m_pdrgpdrgpcr;

	// number of levels
	ULONG m_num_of_part_levels;

public:
	CPartKeys(const CPartKeys &) = delete;

	// ctor
	explicit CPartKeys(CColRef2dArray *pdrgpdrgpcr);

	// dtor
	~CPartKeys() override;

	// return key at a given level
	CColRef *PcrKey(ULONG ulLevel) const;

	// return array of keys
	CColRef2dArray *
	Pdrgpdrgpcr() const
	{
		return m_pdrgpdrgpcr;
	}

	// number of levels
	ULONG
	GetPartitioningLevel() const
	{
		return m_num_of_part_levels;
	}

	// copy part key into the given memory pool
	CPartKeys *PpartkeysCopy(CMemoryPool *mp);

	// check whether the key columns overlap the given column
	BOOL FOverlap(CColRefSet *pcrs) const;

	// create a new PartKeys object from the current one by remapping the
	// keys using the given hashmap
	CPartKeys *PpartkeysRemap(CMemoryPool *mp,
							  UlongToColRefMap *colref_mapping) const;

	// print
	IOstream &OsPrint(IOstream &os) const;

	// copy array of part keys into given memory pool
	static CPartKeysArray *PdrgppartkeysCopy(
		CMemoryPool *mp, const CPartKeysArray *pdrgppartkeys);

};	// CPartKeys

// shorthand for printing
inline IOstream &
operator<<(IOstream &os, CPartKeys &partkeys)
{
	return partkeys.OsPrint(os);
}

}  // namespace gpopt

#endif	// !GPOPT_CPartKeys_H

// EOF
