//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		IMDTypeOid.h
//
//	@doc:
//		Interface for OID types in the metadata cache
//---------------------------------------------------------------------------

#ifndef GPMD_IMDTypeOid_H
#define GPMD_IMDTypeOid_H

#include "gpos/base.h"

#include "naucrates/md/IMDType.h"

// fwd decl
namespace gpnaucrates
{
class IDatumOid;
}

namespace gpmd
{
using namespace gpos;
using namespace gpnaucrates;


//---------------------------------------------------------------------------
//	@class:
//		IMDTypeOid
//
//	@doc:
//		Interface for OID types in the metadata cache
//
//---------------------------------------------------------------------------
class IMDTypeOid : public IMDType
{
public:
	// type id
	static ETypeInfo
	GetTypeInfo()
	{
		return EtiOid;
	}

	ETypeInfo
	GetDatumType() const override
	{
		return IMDTypeOid::GetTypeInfo();
	}

	// factory function for OID datums
	virtual IDatumOid *CreateOidDatum(CMemoryPool *mp, OID oid_value,
									  BOOL is_null) const = 0;
};
}  // namespace gpmd

#endif	// !GPMD_IMDTypeOid_H

// EOF
