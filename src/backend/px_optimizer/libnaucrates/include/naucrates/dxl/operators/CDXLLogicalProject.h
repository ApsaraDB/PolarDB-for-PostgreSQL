//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 Greenplum, Inc.
//
//	@filename:
//		CDXLLogicalProject.h
//
//	@doc:
//		Class for representing DXL logical project operators
//
//---------------------------------------------------------------------------
#ifndef GPDXL_CDXLLogicalProject_H
#define GPDXL_CDXLLogicalProject_H

#include "gpos/base.h"

#include "naucrates/dxl/operators/CDXLLogical.h"

namespace gpdxl
{
//---------------------------------------------------------------------------
//	@class:
//		CDXLLogicalProject
//
//	@doc:
//		Class for representing DXL logical project operators
//
//---------------------------------------------------------------------------
class CDXLLogicalProject : public CDXLLogical
{
private:
	// alias name
	const CMDName *m_mdname_alias;

public:
	CDXLLogicalProject(CDXLLogicalProject &) = delete;

	// ctor
	explicit CDXLLogicalProject(CMemoryPool *);

	// accessors
	Edxlopid GetDXLOperator() const override;
	const CWStringConst *GetOpNameStr() const override;
	const CMDName *MdName() const;

	// serialize operator in DXL format
	void SerializeToDXL(CXMLSerializer *xml_serializer,
						const CDXLNode *dxl_node) const override;

	// set alias name
	void SetAliasName(CMDName *);

	// conversion function
	static CDXLLogicalProject *
	Cast(CDXLOperator *dxl_op)
	{
		GPOS_ASSERT(nullptr != dxl_op);
		GPOS_ASSERT(EdxlopLogicalProject == dxl_op->GetDXLOperator());

		return dynamic_cast<CDXLLogicalProject *>(dxl_op);
	}

#ifdef GPOS_DEBUG
	// checks whether the operator has valid structure, i.e. number and
	// types of child nodes
	void AssertValid(const CDXLNode *, BOOL validate_children) const override;
#endif	// GPOS_DEBUG
};
}  // namespace gpdxl
#endif	// !GPDXL_CDXLLogicalProject_H

// EOF
