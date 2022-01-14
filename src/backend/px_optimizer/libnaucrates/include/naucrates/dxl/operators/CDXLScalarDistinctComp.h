//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 Greenplum, Inc.
//
//	@filename:
//		CDXLScalarDistinctComp.h
//
//	@doc:
//		Class for representing DXL scalar "is distinct from" comparison operators.
//---------------------------------------------------------------------------

#ifndef GPDXL_CDXLScalarDistinctComp_H
#define GPDXL_CDXLScalarDistinctComp_H

#include "gpos/base.h"

#include "naucrates/dxl/operators/CDXLScalarComp.h"


namespace gpdxl
{
// indices of scalar distinct comparison elements in the children array
enum Edxlscdistcmp
{
	EdxlscdistcmpIndexLeft = 0,
	EdxlscdistcmpIndexRight,
	EdxlscdistcmpSentinel
};

//---------------------------------------------------------------------------
//	@class:
//		CDXLScalarDistinctComp
//
//	@doc:
//		Class for representing DXL scalar "is distinct from" comparison operators
//
//---------------------------------------------------------------------------
class CDXLScalarDistinctComp : public CDXLScalarComp
{
private:
public:
	CDXLScalarDistinctComp(CDXLScalarDistinctComp &) = delete;

	// ctor/dtor
	CDXLScalarDistinctComp(CMemoryPool *mp, IMDId *operator_mdid);

	// ident accessors
	Edxlopid GetDXLOperator() const override;

	// name of the DXL operator
	const CWStringConst *GetOpNameStr() const override;

	// serialize operator in DXL format
	void SerializeToDXL(CXMLSerializer *xml_serializer,
						const CDXLNode *node) const override;

	// conversion function
	static CDXLScalarDistinctComp *
	Cast(CDXLOperator *dxl_op)
	{
		GPOS_ASSERT(nullptr != dxl_op);
		GPOS_ASSERT(EdxlopScalarDistinct == dxl_op->GetDXLOperator());

		return dynamic_cast<CDXLScalarDistinctComp *>(dxl_op);
	}

	// does the operator return a boolean result
	BOOL
	HasBoolResult(CMDAccessor *	 //md_accessor
	) const override
	{
		return true;
	}

#ifdef GPOS_DEBUG
	// checks whether the operator has valid structure, i.e. number and
	// types of child nodes
	void AssertValid(const CDXLNode *node,
					 BOOL validate_children) const override;
#endif	// GPOS_DEBUG
};
}  // namespace gpdxl

#endif	// !GPDXL_CDXLScalarDistinctComp_H


// EOF
