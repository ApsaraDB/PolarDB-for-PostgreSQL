//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 VMware, Inc. or its affiliates.
//	Copyright (C) 2021, Alibaba Group Holding Limited
//
//	@filename:
//		IMDRelationCtas.h
//
//	@doc:
//		Interface for CTAS relation entries in the metadata cache
//---------------------------------------------------------------------------

#ifndef GPMD_IMDRelationCTAS_H
#define GPMD_IMDRelationCTAS_H

#include "gpos/base.h"

#include "naucrates/md/IMDRelation.h"

namespace gpdxl
{
// fwd decl
class CDXLCtasStorageOptions;
}  // namespace gpdxl

namespace gpmd
{
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		IMDRelationCtas
//
//	@doc:
//		Interface for CTAS relation entries in the metadata cache
//
//---------------------------------------------------------------------------
class IMDRelationCtas : public IMDRelation
{
public:
	// schema name
	virtual CMDName *GetMdNameSchema() const = 0;

	// is this a partitioned table
	BOOL
	IsPartitioned() const override
	{
		return false;
	}

	// number of partition columns
	ULONG
	PartColumnCount() const override
	{
		return 0;
	}

	// number of partitions
	ULONG
	PartitionCount() const override
	{
		return 0;
	}

	// retrieve the partition column at the given position
	const IMDColumn *PartColAt(ULONG  // pos
	) const override
	{
		GPOS_ASSERT(!"CTAS tables have no partition columns");
		return nullptr;
	}

	// POLAR px
	ULONG PartSchemeCount() const override
	{
		return 0;
	}

	// POLAR px: retrieve the partition scheme at the given position
	ULONG PartSchemeAt(ULONG  // pos
	) const override
	{
		GPOS_ASSERT(!"CTAS tables have no part scheme");
		return 0;
	}

	// retrieve list of partition types
	CharPtrArray *
	GetPartitionTypes() const override
	{
		GPOS_ASSERT(!"CTAS tables have no partition types");
		return nullptr;
	}

	// retrieve the partition column at the given position
	CHAR PartTypeAtLevel(ULONG /*pos*/) const override
	{
		GPOS_ASSERT(!"CTAS tables have no partition types");
		return (CHAR) 0;
	}

	// return true if a hash distributed table needs to be considered as random
	BOOL
	ConvertHashToRandom() const override
	{
		return false;
	}

	// returns the number of key sets
	ULONG
	KeySetCount() const override
	{
		return 0;
	}

	// returns the key set at the specified position
	const ULongPtrArray *KeySetAt(ULONG	 // pos
	) const override
	{
		GPOS_ASSERT(!"CTAS tables have no keys");
		return nullptr;
	}

	// part constraint
	CDXLNode *
	MDPartConstraint() const override
	{
		return nullptr;
	}

	// CTAS storage options
	virtual CDXLCtasStorageOptions *GetDxlCtasStorageOption() const = 0;
};
}  // namespace gpmd

#endif	// !GPMD_IMDRelationCTAS_H

// EOF
