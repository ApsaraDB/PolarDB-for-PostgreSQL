//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC, Corp.
//
//	@filename:
//		CDXLScalarSubqueryExists.cpp
//
//	@doc:
//		Implementation of EXISTS subqueries
//---------------------------------------------------------------------------

#include "naucrates/dxl/operators/CDXLScalarSubqueryExists.h"

#include "gpos/string/CWStringDynamic.h"

#include "naucrates/dxl/operators/CDXLNode.h"
#include "naucrates/dxl/xml/CXMLSerializer.h"

using namespace gpos;
using namespace gpdxl;

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarSubqueryExists::CDXLScalarSubqueryExists
//
//	@doc:
//		Constructor
//
//---------------------------------------------------------------------------
CDXLScalarSubqueryExists::CDXLScalarSubqueryExists(CMemoryPool *mp)
	: CDXLScalar(mp)
{
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarSubqueryExists::~CDXLScalarSubqueryExists
//
//	@doc:
//		Destructor
//
//---------------------------------------------------------------------------
CDXLScalarSubqueryExists::~CDXLScalarSubqueryExists() = default;

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarSubqueryExists::GetDXLOperator
//
//	@doc:
//		Operator type
//
//---------------------------------------------------------------------------
Edxlopid
CDXLScalarSubqueryExists::GetDXLOperator() const
{
	return EdxlopScalarSubqueryExists;
}


//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarSubqueryExists::GetOpNameStr
//
//	@doc:
//		Operator name
//
//---------------------------------------------------------------------------
const CWStringConst *
CDXLScalarSubqueryExists::GetOpNameStr() const
{
	return CDXLTokens::GetDXLTokenStr(EdxltokenScalarSubqueryExists);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarSubqueryExists::SerializeToDXL
//
//	@doc:
//		Serialize operator in DXL format
//
//---------------------------------------------------------------------------
void
CDXLScalarSubqueryExists::SerializeToDXL(CXMLSerializer *xml_serializer,
										 const CDXLNode *dxlnode) const
{
	const CWStringConst *element_name = GetOpNameStr();
	xml_serializer->OpenElement(
		CDXLTokens::GetDXLTokenStr(EdxltokenNamespacePrefix), element_name);
	dxlnode->SerializeChildrenToDXL(xml_serializer);
	xml_serializer->CloseElement(
		CDXLTokens::GetDXLTokenStr(EdxltokenNamespacePrefix), element_name);
}

#ifdef GPOS_DEBUG
//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarSubqueryExists::AssertValid
//
//	@doc:
//		Checks whether operator node is well-structured
//
//---------------------------------------------------------------------------
void
CDXLScalarSubqueryExists::AssertValid(const CDXLNode *dxlnode,
									  BOOL validate_children) const
{
	GPOS_ASSERT(1 == dxlnode->Arity());

	CDXLNode *child_dxlnode = (*dxlnode)[0];
	GPOS_ASSERT(EdxloptypeLogical ==
				child_dxlnode->GetOperator()->GetDXLOperatorType());

	dxlnode->AssertValid(validate_children);
}
#endif	// GPOS_DEBUG

// EOF
