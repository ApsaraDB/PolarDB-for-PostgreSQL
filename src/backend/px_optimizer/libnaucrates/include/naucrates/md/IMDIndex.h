//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		IMDIndex.h
//
//	@doc:
//		Interface for indexes in the metadata cache
//---------------------------------------------------------------------------



#ifndef GPMD_IMDIndex_H
#define GPMD_IMDIndex_H

#include "gpos/base.h"

#include "naucrates/md/IMDCacheObject.h"

namespace gpmd
{
using namespace gpos;

// fwd decl
class IMDPartConstraint;
class IMDScalarOp;

//---------------------------------------------------------------------------
//	@class:
//		IMDIndex
//
//	@doc:
//		Interface for indexes in the metadata cache
//
//---------------------------------------------------------------------------
class IMDIndex : public IMDCacheObject
{
public:
	// index type
	enum EmdindexType
	{
		EmdindBtree,   // btree
		EmdindBitmap,  // bitmap
		EmdindGist,	   // gist using btree or bitmap
		EmdindGin,	   // gin using btree or bitmap
		EmdindBrin,	   // brin
		EmdindSentinel
	};

	// object type
	Emdtype
	MDType() const override
	{
		return EmdtInd;
	}

	// is the index clustered
	virtual BOOL IsClustered() const = 0;

	// is the index partitioned
	virtual BOOL IsPartitioned() const = 0;

	// index type
	virtual EmdindexType IndexType() const = 0;

	// number of keys
	virtual ULONG Keys() const = 0;

	// return the n-th key column
	virtual ULONG KeyAt(ULONG pos) const = 0;

	// return the position of the key column
	virtual ULONG GetKeyPos(ULONG pos) const = 0;

	// number of included columns
	virtual ULONG IncludedCols() const = 0;

	// return the n-th included column
	virtual ULONG IncludedColAt(ULONG pos) const = 0;

	// return the position of the included column
	virtual ULONG GetIncludedColPos(ULONG column) const = 0;

	// part constraint
	virtual IMDPartConstraint *MDPartConstraint() const = 0;

	// type id of items returned by the index
	virtual IMDId *GetIndexRetItemTypeMdid() const = 0;

	// check if given scalar comparison can be used with the index key
	// at the specified position
	virtual BOOL IsCompatible(const IMDScalarOp *md_scalar_op,
							  ULONG key_pos) const = 0;

	// child index oids
	virtual IMdIdArray *ChildIndexMdids() const = 0;

	// index type as a string value
	static const CWStringConst *GetDXLStr(EmdindexType index_type);
};
}  // namespace gpmd



#endif	// !GPMD_IMDIndex_H

// EOF
