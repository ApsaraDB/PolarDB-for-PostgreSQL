//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 Greenplum, Inc.
//
//	@filename:
//		CDXLScalarCast.h
//
//	@doc:
//		Class for representing DXL casting operation
//
//	@owner:
//
//
//	@test:
//
//---------------------------------------------------------------------------

#ifndef GPDXL_CDXLScalarCast_H
#define GPDXL_CDXLScalarCast_H

#include "gpos/base.h"

#include "naucrates/dxl/operators/CDXLScalar.h"
#include "naucrates/md/IMDId.h"

namespace gpdxl
{
using namespace gpos;
using namespace gpmd;

//---------------------------------------------------------------------------
//	@class:
//		CDXLScalarCast
//
//	@doc:
//		Class for representing DXL casting operator
//
//---------------------------------------------------------------------------
class CDXLScalarCast : public CDXLScalar
{
private:
	// catalog MDId of the column's type
	IMDId *m_mdid_type;

	// catalog MDId of the function implementing the casting
	IMDId *m_func_mdid;

public:
	CDXLScalarCast(const CDXLScalarCast &) = delete;

	// ctor/dtor
	CDXLScalarCast(CMemoryPool *mp, IMDId *mdid_type, IMDId *func_mdid);

	~CDXLScalarCast() override;

	// ident accessors
	Edxlopid GetDXLOperator() const override;

	IMDId *MdidType() const;
	IMDId *FuncMdId() const;

	// name of the DXL operator name
	const CWStringConst *GetOpNameStr() const override;

	// conversion function
	static CDXLScalarCast *
	Cast(CDXLOperator *dxl_op)
	{
		GPOS_ASSERT(nullptr != dxl_op);
		GPOS_ASSERT(EdxlopScalarCast == dxl_op->GetDXLOperator());

		return dynamic_cast<CDXLScalarCast *>(dxl_op);
	}

	// does the operator return a boolean result
	BOOL HasBoolResult(CMDAccessor *md_accessor) const override;

#ifdef GPOS_DEBUG
	// checks whether the operator has valid structure, i.e. number and
	// types of child nodes
	void AssertValid(const CDXLNode *dxlnode,
					 BOOL validate_children) const override;
#endif	// GPOS_DEBUG

	// serialize operator in DXL format
	void SerializeToDXL(CXMLSerializer *xml_serializer,
						const CDXLNode *dxlnode) const override;
};
}  // namespace gpdxl

#endif	// !GPDXL_CDXLScalarCast_H

// EOF
