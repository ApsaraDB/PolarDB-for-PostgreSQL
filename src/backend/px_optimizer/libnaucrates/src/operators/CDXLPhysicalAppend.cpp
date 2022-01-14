//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 Greenplum, Inc.
//
//	@filename:
//		CDXLPhysicalAppend.cpp
//
//	@doc:
//		Implementation of DXL physical Append operator
//---------------------------------------------------------------------------

#include "naucrates/dxl/operators/CDXLPhysicalAppend.h"

#include "gpos/common/CBitSetIter.h"

#include "naucrates/dxl/CDXLUtils.h"
#include "naucrates/dxl/operators/CDXLNode.h"
#include "naucrates/dxl/xml/CXMLSerializer.h"

using namespace gpos;
using namespace gpdxl;

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalAppend::CDXLPhysicalAppend
//
//	@doc:
//		Constructor
//
//---------------------------------------------------------------------------
CDXLPhysicalAppend::CDXLPhysicalAppend(CMemoryPool *mp, BOOL fIsTarget,
									   BOOL fIsZapped)
	: CDXLPhysical(mp), m_used_in_upd_del(fIsTarget), m_is_zapped(fIsZapped)
{
}

CDXLPhysicalAppend::CDXLPhysicalAppend(CMemoryPool *mp, BOOL fIsTarget,
									   BOOL fIsZapped, ULONG scan_id,
									   CDXLTableDescr *dxl_table_desc,
									   ULongPtrArray *selector_ids)
	: CDXLPhysical(mp),
	  m_used_in_upd_del(fIsTarget),
	  m_is_zapped(fIsZapped),
	  m_scan_id(scan_id),
	  m_dxl_table_descr(dxl_table_desc),
	  m_selector_ids(selector_ids)
{
}

CDXLPhysicalAppend::~CDXLPhysicalAppend()
{
	CRefCount::SafeRelease(m_dxl_table_descr);
	CRefCount::SafeRelease(m_selector_ids);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalAppend::GetDXLOperator
//
//	@doc:
//		Operator type
//
//---------------------------------------------------------------------------
Edxlopid
CDXLPhysicalAppend::GetDXLOperator() const
{
	return EdxlopPhysicalAppend;
}


//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalAppend::GetOpNameStr
//
//	@doc:
//		Operator name
//
//---------------------------------------------------------------------------
const CWStringConst *
CDXLPhysicalAppend::GetOpNameStr() const
{
	return CDXLTokens::GetDXLTokenStr(EdxltokenPhysicalAppend);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalAppend::IsUsedInUpdDel
//
//	@doc:
//		Is the append node updating a target relation
//
//---------------------------------------------------------------------------
BOOL
CDXLPhysicalAppend::IsUsedInUpdDel() const
{
	return m_used_in_upd_del;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalAppend::IsZapped
//
//	@doc:
//		Is the append node zapped
//
//---------------------------------------------------------------------------
BOOL
CDXLPhysicalAppend::IsZapped() const
{
	return m_is_zapped;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalAppend::SerializeToDXL
//
//	@doc:
//		Serialize operator in DXL format
//
//---------------------------------------------------------------------------
void
CDXLPhysicalAppend::SerializeToDXL(CXMLSerializer *xml_serializer,
								   const CDXLNode *dxlnode) const
{
	const CWStringConst *element_name = GetOpNameStr();

	xml_serializer->OpenElement(
		CDXLTokens::GetDXLTokenStr(EdxltokenNamespacePrefix), element_name);

	xml_serializer->AddAttribute(
		CDXLTokens::GetDXLTokenStr(EdxltokenAppendIsTarget), m_used_in_upd_del);
	xml_serializer->AddAttribute(
		CDXLTokens::GetDXLTokenStr(EdxltokenAppendIsZapped), m_is_zapped);

	if (m_scan_id != gpos::ulong_max)
	{
		xml_serializer->AddAttribute(
			CDXLTokens::GetDXLTokenStr(EdxltokenPartIndexId), m_scan_id);

		CWStringDynamic *serialized_selector_ids =
			CDXLUtils::Serialize(m_mp, m_selector_ids);
		xml_serializer->AddAttribute(
			CDXLTokens::GetDXLTokenStr(EdxltokenSelectorIds),
			serialized_selector_ids);
		GPOS_DELETE(serialized_selector_ids);
	}
	// serialize properties
	dxlnode->SerializePropertiesToDXL(xml_serializer);

	if (m_dxl_table_descr != nullptr)
	{
		GPOS_ASSERT(m_scan_id != gpos::ulong_max);
		m_dxl_table_descr->SerializeToDXL(xml_serializer);
	}


	// serialize children
	dxlnode->SerializeChildrenToDXL(xml_serializer);

	xml_serializer->CloseElement(
		CDXLTokens::GetDXLTokenStr(EdxltokenNamespacePrefix), element_name);
}


#ifdef GPOS_DEBUG
//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalAppend::AssertValid
//
//	@doc:
//		Checks whether operator node is well-structured
//
//---------------------------------------------------------------------------
void
CDXLPhysicalAppend::AssertValid(const CDXLNode *dxlnode,
								BOOL validate_children) const
{
	// assert proj list and filter are valid
	CDXLPhysical::AssertValid(dxlnode, validate_children);

	const ULONG ulChildren = dxlnode->Arity();
	for (ULONG ul = EdxlappendIndexFirstChild; ul < ulChildren; ul++)
	{
		CDXLNode *child_dxlnode = (*dxlnode)[ul];
		GPOS_ASSERT(EdxloptypePhysical ==
					child_dxlnode->GetOperator()->GetDXLOperatorType());

		if (validate_children)
		{
			child_dxlnode->GetOperator()->AssertValid(child_dxlnode,
													  validate_children);
		}
	}
}
#endif	// GPOS_DEBUG

// EOF
