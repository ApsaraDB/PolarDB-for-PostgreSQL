//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC, Corp.
//
//	@filename:
//		CDXLScalarSubqueryAny.h
//
//	@doc:
//		Class for representing ANY subqueries
//---------------------------------------------------------------------------

#ifndef GPDXL_CDXLScalarSubqueryAny_H
#define GPDXL_CDXLScalarSubqueryAny_H

#include "gpos/base.h"

#include "naucrates/dxl/operators/CDXLNode.h"
#include "naucrates/dxl/operators/CDXLScalarSubqueryQuantified.h"
#include "naucrates/md/CMDName.h"
#include "naucrates/md/IMDId.h"

namespace gpdxl
{
using namespace gpmd;

//---------------------------------------------------------------------------
//	@class:
//		CDXLScalarSubqueryAny
//
//	@doc:
//		Class for representing ANY subqueries
//
//---------------------------------------------------------------------------
class CDXLScalarSubqueryAny : public CDXLScalarSubqueryQuantified
{
private:
public:
	CDXLScalarSubqueryAny(CDXLScalarSubqueryAny &) = delete;

	// ctor
	CDXLScalarSubqueryAny(CMemoryPool *mp, IMDId *scalar_op_mdid,
						  CMDName *mdname, ULONG colid);

	// ident accessors
	Edxlopid GetDXLOperator() const override;

	// name of the operator
	const CWStringConst *GetOpNameStr() const override;

	// conversion function
	static CDXLScalarSubqueryAny *
	Cast(CDXLOperator *dxl_op)
	{
		GPOS_ASSERT(nullptr != dxl_op);
		GPOS_ASSERT(EdxlopScalarSubqueryAny == dxl_op->GetDXLOperator());

		return dynamic_cast<CDXLScalarSubqueryAny *>(dxl_op);
	}

	// does the operator return a boolean result
	BOOL
	HasBoolResult(CMDAccessor *	 //md_accessor
	) const override
	{
		return true;
	}
};
}  // namespace gpdxl


#endif	// !GPDXL_CDXLScalarSubqueryAny_H

// EOF
