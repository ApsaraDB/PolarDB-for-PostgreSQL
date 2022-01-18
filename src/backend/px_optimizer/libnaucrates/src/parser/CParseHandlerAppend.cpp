//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 Greenplum, Inc.
//
//	@filename:
//		CParseHandlerAppend.cpp
//
//	@doc:
//		Implementation of the SAX parse handler class for parsing Append operator.
//---------------------------------------------------------------------------

#include "naucrates/dxl/parser/CParseHandlerAppend.h"

#include "naucrates/dxl/operators/CDXLOperatorFactory.h"
#include "naucrates/dxl/parser/CParseHandlerFactory.h"
#include "naucrates/dxl/parser/CParseHandlerFilter.h"
#include "naucrates/dxl/parser/CParseHandlerProjList.h"
#include "naucrates/dxl/parser/CParseHandlerProperties.h"
#include "naucrates/dxl/parser/CParseHandlerScalarOp.h"
#include "naucrates/dxl/parser/CParseHandlerTableDescr.h"
#include "naucrates/dxl/parser/CParseHandlerUtils.h"

using namespace gpdxl;


XERCES_CPP_NAMESPACE_USE

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerAppend::CParseHandlerAppend
//
//	@doc:
//		Constructor
//
//---------------------------------------------------------------------------
CParseHandlerAppend::CParseHandlerAppend(
	CMemoryPool *mp, CParseHandlerManager *parse_handler_mgr,
	CParseHandlerBase *parse_handler_root)
	: CParseHandlerPhysicalOp(mp, parse_handler_mgr, parse_handler_root),
	  m_dxl_op(nullptr)
{
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerAppend::SetupInitialHandlers
//
//	@doc:
//		Setup initial set of handlers for append node
//
//---------------------------------------------------------------------------
void
CParseHandlerAppend::SetupInitialHandlers(const Attributes &attrs)
{
	// seeing a result tag
	GPOS_ASSERT(m_dxl_op == nullptr &&
				"Append dxl node should not have been created yet");
	GPOS_ASSERT(this->Length() == 0 &&
				"No handlers should have been added yet");

	m_dxl_op = (CDXLPhysicalAppend *) CDXLOperatorFactory::MakeDXLAppend(
		m_parse_handler_mgr->GetDXLMemoryManager(), attrs);

	// parse handler for the filter
	CParseHandlerBase *filter_parse_handler =
		CParseHandlerFactory::GetParseHandler(
			m_mp, CDXLTokens::XmlstrToken(EdxltokenScalarFilter),
			m_parse_handler_mgr, this);
	m_parse_handler_mgr->ActivateParseHandler(filter_parse_handler);

	// parse handler for the proj list
	CParseHandlerBase *proj_list_parse_handler =
		CParseHandlerFactory::GetParseHandler(
			m_mp, CDXLTokens::XmlstrToken(EdxltokenScalarProjList),
			m_parse_handler_mgr, this);
	m_parse_handler_mgr->ActivateParseHandler(proj_list_parse_handler);

	CParseHandlerBase *table_descr_parse_handler = nullptr;
	if (m_dxl_op->GetScanId() != gpos::ulong_max)
	{
		// parse handler for table descriptor
		table_descr_parse_handler = CParseHandlerFactory::GetParseHandler(
			m_mp, CDXLTokens::XmlstrToken(EdxltokenTableDescr),
			m_parse_handler_mgr, this);
		m_parse_handler_mgr->ActivateParseHandler(table_descr_parse_handler);
	}

	//parse handler for the properties of the operator
	CParseHandlerBase *prop_parse_handler =
		CParseHandlerFactory::GetParseHandler(
			m_mp, CDXLTokens::XmlstrToken(EdxltokenProperties),
			m_parse_handler_mgr, this);
	m_parse_handler_mgr->ActivateParseHandler(prop_parse_handler);

	this->Append(prop_parse_handler);
	if (m_dxl_op->GetScanId() != gpos::ulong_max)
	{
		GPOS_ASSERT(nullptr != table_descr_parse_handler);
		this->Append(table_descr_parse_handler);
	}
	this->Append(proj_list_parse_handler);
	this->Append(filter_parse_handler);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerAppend::StartElement
//
//	@doc:
//		Invoked by Xerces to process an opening tag
//
//---------------------------------------------------------------------------
void
CParseHandlerAppend::StartElement(const XMLCh *const element_uri,
								  const XMLCh *const element_local_name,
								  const XMLCh *const element_qname,
								  const Attributes &attrs)
{
	if (0 == XMLString::compareString(
				 CDXLTokens::XmlstrToken(EdxltokenPhysicalAppend),
				 element_local_name) &&
		nullptr == m_dxl_op)
	{
		// open a root Append element
		SetupInitialHandlers(attrs);
	}
	else if (nullptr != m_dxl_op)
	{
		// install a parse handler for a child node
		CParseHandlerBase *child_parse_handler =
			CParseHandlerFactory::GetParseHandler(
				m_mp, CDXLTokens::XmlstrToken(EdxltokenPhysical),
				m_parse_handler_mgr, this);
		m_parse_handler_mgr->ActivateParseHandler(child_parse_handler);

		this->Append(child_parse_handler);

		child_parse_handler->startElement(element_uri, element_local_name,
										  element_qname, attrs);
	}
	else
	{
		CWStringDynamic *str = CDXLUtils::CreateDynamicStringFromXMLChArray(
			m_parse_handler_mgr->GetDXLMemoryManager(), element_local_name);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag,
				   str->GetBuffer());
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerAppend::EndElement
//
//	@doc:
//		Invoked by Xerces to process a closing tag
//
//---------------------------------------------------------------------------
void
CParseHandlerAppend::EndElement(const XMLCh *const,	 // element_uri,
								const XMLCh *const element_local_name,
								const XMLCh *const	// element_qname
)
{
	if (0 != XMLString::compareString(
				 CDXLTokens::XmlstrToken(EdxltokenPhysicalAppend),
				 element_local_name))
	{
		CWStringDynamic *str = CDXLUtils::CreateDynamicStringFromXMLChArray(
			m_parse_handler_mgr->GetDXLMemoryManager(), element_local_name);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag,
				   str->GetBuffer());
	}

	ULONG child_index = 0;
	// construct node from the created child nodes
	CParseHandlerProperties *prop_parse_handler =
		dynamic_cast<CParseHandlerProperties *>((*this)[child_index++]);
	if (m_dxl_op->GetScanId() != gpos::ulong_max)
	{
		CParseHandlerTableDescr *table_descr_parse_handler =
			dynamic_cast<CParseHandlerTableDescr *>((*this)[child_index++]);
		CDXLTableDescr *table_descr =
			table_descr_parse_handler->GetDXLTableDescr();
		table_descr->AddRef();
		m_dxl_op->SetDXLTableDesc(table_descr);
	}
	CParseHandlerProjList *proj_list_parse_handler =
		dynamic_cast<CParseHandlerProjList *>((*this)[child_index++]);
	CParseHandlerFilter *filter_parse_handler =
		dynamic_cast<CParseHandlerFilter *>((*this)[child_index++]);

	m_dxl_node = GPOS_NEW(m_mp) CDXLNode(m_mp, m_dxl_op);
	CParseHandlerUtils::SetProperties(m_dxl_node, prop_parse_handler);

	// add constructed children
	AddChildFromParseHandler(proj_list_parse_handler);
	AddChildFromParseHandler(filter_parse_handler);

	GPOS_ASSERT(3 <= this->Length());

	const ULONG length = this->Length();
	// an append node can have variable number of children: add them one by one from the respective parse handlers
	for (; child_index < length; ++child_index)
	{
		CParseHandlerPhysicalOp *child_parse_handler =
			dynamic_cast<CParseHandlerPhysicalOp *>((*this)[child_index]);
		AddChildFromParseHandler(child_parse_handler);
	}

#ifdef GPOS_DEBUG
	m_dxl_op->AssertValid(m_dxl_node, false /* validate_children */);
#endif	// GPOS_DEBUG

	// deactivate handler
	m_parse_handler_mgr->DeactivateHandler();
}

// EOF
