//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 VMware, Inc. or its affiliates.
//
//	@filename:
//		CLogicalExternalGet.h
//
//	@doc:
//		Logical external get operator
//---------------------------------------------------------------------------
#ifndef GPOPT_CLogicalExternalGet_H
#define GPOPT_CLogicalExternalGet_H

#include "gpos/base.h"

#include "gpopt/operators/CLogicalGet.h"

namespace gpopt
{
// fwd declarations
class CTableDescriptor;
class CName;
class CColRefSet;

//---------------------------------------------------------------------------
//	@class:
//		CLogicalExternalGet
//
//	@doc:
//		Logical external get operator
//
//---------------------------------------------------------------------------
class CLogicalExternalGet : public CLogicalGet
{
private:
public:
	CLogicalExternalGet(const CLogicalExternalGet &) = delete;

	// ctors
	explicit CLogicalExternalGet(CMemoryPool *mp);

	CLogicalExternalGet(CMemoryPool *mp, const CName *pnameAlias,
						CTableDescriptor *ptabdesc);

	CLogicalExternalGet(CMemoryPool *mp, const CName *pnameAlias,
						CTableDescriptor *ptabdesc,
						CColRefArray *pdrgpcrOutput);

	// ident accessors
	EOperatorId
	Eopid() const override
	{
		return EopLogicalExternalGet;
	}

	// return a string for operator name
	const CHAR *
	SzId() const override
	{
		return "CLogicalExternalGet";
	}

	// match function
	BOOL Matches(COperator *pop) const override;

	// return a copy of the operator with remapped columns
	COperator *PopCopyWithRemappedColumns(CMemoryPool *mp,
										  UlongToColRefMap *colref_mapping,
										  BOOL must_exist) override;

	//-------------------------------------------------------------------------------------
	// Required Relational Properties
	//-------------------------------------------------------------------------------------

	// compute required stat columns of the n-th child
	CColRefSet *
	PcrsStat(CMemoryPool *,		   // mp,
			 CExpressionHandle &,  // exprhdl
			 CColRefSet *,		   // pcrsInput
			 ULONG				   // child_index
	) const override
	{
		GPOS_ASSERT(!"CLogicalExternalGet has no children");
		return nullptr;
	}

	//-------------------------------------------------------------------------------------
	// Transformations
	//-------------------------------------------------------------------------------------

	// candidate set of xforms
	CXformSet *PxfsCandidates(CMemoryPool *mp) const override;

	//-------------------------------------------------------------------------------------
	//-------------------------------------------------------------------------------------
	//-------------------------------------------------------------------------------------

	// conversion function
	static CLogicalExternalGet *
	PopConvert(COperator *pop)
	{
		GPOS_ASSERT(nullptr != pop);
		GPOS_ASSERT(EopLogicalExternalGet == pop->Eopid());

		return dynamic_cast<CLogicalExternalGet *>(pop);
	}

};	// class CLogicalExternalGet
}  // namespace gpopt

#endif	// !GPOPT_CLogicalExternalGet_H

// EOF
