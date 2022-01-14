//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		IMDScalarOp.h
//
//	@doc:
//		Interface for scalar operators in the metadata cache
//---------------------------------------------------------------------------



#ifndef GPMD_IMDScalarOp_H
#define GPMD_IMDScalarOp_H

#include "gpos/base.h"

#include "naucrates/md/IMDCacheObject.h"
#include "naucrates/md/IMDType.h"

namespace gpmd
{
using namespace gpos;


//---------------------------------------------------------------------------
//	@class:
//		IMDScalarOp
//
//	@doc:
//		Interface for scalar operators in the metadata cache
//
//---------------------------------------------------------------------------
class IMDScalarOp : public IMDCacheObject
{
public:
	// object type
	Emdtype
	MDType() const override
	{
		return EmdtOp;
	}

	// type of left operand
	virtual IMDId *GetLeftMdid() const = 0;

	// type of right operand
	virtual IMDId *GetRightMdid() const = 0;

	// type of result operand
	virtual IMDId *GetResultTypeMdid() const = 0;

	// id of function which implements the operator
	virtual IMDId *FuncMdId() const = 0;

	// id of commute operator
	virtual IMDId *GetCommuteOpMdid() const = 0;

	// id of inverse operator
	virtual IMDId *GetInverseOpMdid() const = 0;

	// is this an equality operator
	virtual BOOL IsEqualityOp() const = 0;

	// does operator return NULL when all inputs are NULL?
	virtual BOOL ReturnsNullOnNullInput() const = 0;

	// preserves NDVs of its inputs?
	virtual BOOL IsNDVPreserving() const = 0;

	virtual IMDType::ECmpType ParseCmpType() const = 0;

	// operator name
	CMDName Mdname() const override = 0;

	// number of classes this operator belongs to
	virtual ULONG OpfamiliesCount() const = 0;

	// operator class at given position
	virtual IMDId *OpfamilyMdidAt(ULONG pos) const = 0;

	// compatible hash opfamily
	virtual IMDId *HashOpfamilyMdid() const = 0;
};
}  // namespace gpmd

#endif	// !GPMD_IMDScalarOp_H

// EOF
