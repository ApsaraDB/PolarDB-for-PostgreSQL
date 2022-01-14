//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//	Copyright (C) 2021, Alibaba Group Holding Limited
//
//	@filename:
//		CDXLScalarSwitch.h
//
//	@doc:
//
//		Class for representing DXL Switch (corresponds to Case (expr) ...)
//---------------------------------------------------------------------------

#ifndef GPDXL_CDXLScalarSwitch_H
#define GPDXL_CDXLScalarSwitch_H

#include "gpos/base.h"

#include "naucrates/dxl/operators/CDXLScalar.h"
#include "naucrates/md/IMDId.h"

namespace gpdxl
{
using namespace gpos;
using namespace gpmd;

//---------------------------------------------------------------------------
//	@class:
//		CDXLScalarSwitch
//
//	@doc:
//		Class for representing DXL Switch
//
//---------------------------------------------------------------------------
class CDXLScalarSwitch : public CDXLScalar
{
private:
	// return type
	IMDId *m_mdid_type;

	// POLAR px: this flag indicates whether this DXL node is translated
	// from the decode expression.
	BOOL m_is_decode_expr;

public:
	CDXLScalarSwitch(const CDXLScalarSwitch &) = delete;

	// ctor
	CDXLScalarSwitch(CMemoryPool *mp, IMDId *mdid_type, BOOL is_decode_expr);

	//dtor
	~CDXLScalarSwitch() override;

	// name of the operator
	const CWStringConst *GetOpNameStr() const override;

	// return type
	virtual IMDId *MdidType() const;

	// DXL Operator ID
	Edxlopid GetDXLOperator() const override;

	// serialize operator in DXL format
	void SerializeToDXL(CXMLSerializer *xml_serializer,
						const CDXLNode *dxlnode) const override;

	// conversion function
	static CDXLScalarSwitch *
	Cast(CDXLOperator *dxl_op)
	{
		GPOS_ASSERT(nullptr != dxl_op);
		GPOS_ASSERT(EdxlopScalarSwitch == dxl_op->GetDXLOperator());

		return dynamic_cast<CDXLScalarSwitch *>(dxl_op);
	}

	// does the operator return a boolean result
	BOOL HasBoolResult(CMDAccessor *md_accessor) const override;

	// POLAR px: this function is used to check whether the DXL node is transformed
	// from the decode expression.
	BOOL IsDecodeExpr() const
	{
		return m_is_decode_expr;
	}

#ifdef GPOS_DEBUG
	// checks whether the operator has valid structure, i.e. number and
	// types of child nodes
	void AssertValid(const CDXLNode *dxlnode,
					 BOOL validate_children) const override;
#endif	// GPOS_DEBUG
};
}  // namespace gpdxl
#endif	// !GPDXL_CDXLScalarSwitch_H

// EOF
