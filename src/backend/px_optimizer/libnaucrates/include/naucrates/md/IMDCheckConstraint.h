//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		IMDCheckConstraint.h
//
//	@doc:
//		Interface class for check constraint in a metadata cache relation
//---------------------------------------------------------------------------

#ifndef GPMD_IMDCheckConstraint_H
#define GPMD_IMDCheckConstraint_H

#include "gpos/base.h"

#include "gpopt/base/CColRef.h"
#include "naucrates/md/IMDCacheObject.h"

// fwd decl
namespace gpdxl
{
class CDXLNode;
}

namespace gpopt
{
class CExpression;
class CMDAccessor;
}  // namespace gpopt

namespace gpmd
{
using namespace gpos;
using namespace gpopt;

//---------------------------------------------------------------------------
//	@class:
//		IMDCheckConstraint
//
//	@doc:
//		Interface class for check constraint in a metadata cache relation
//
//---------------------------------------------------------------------------
class IMDCheckConstraint : public IMDCacheObject
{
public:
	// object type
	Emdtype
	MDType() const override
	{
		return EmdtCheckConstraint;
	}

	// mdid of the relation
	virtual IMDId *GetRelMdId() const = 0;

	// the scalar expression of the check constraint
	virtual CExpression *GetCheckConstraintExpr(
		CMemoryPool *mp, CMDAccessor *md_accessor,
		CColRefArray *colref_array) const = 0;
};
}  // namespace gpmd

#endif	// !GPMD_IMDCheckConstraint_H

// EOF
