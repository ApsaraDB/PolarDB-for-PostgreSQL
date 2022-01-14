//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CParseHandlerLogicalWindow.h
//
//	@doc:
//		Parse handler for parsing a logical window operator
//
//---------------------------------------------------------------------------
#ifndef GPDXL_CParseHandlerLogicalWindow_H
#define GPDXL_CParseHandlerLogicalWindow_H

#include "gpos/base.h"

#include "naucrates/dxl/operators/CDXLLogicalWindow.h"
#include "naucrates/dxl/operators/CDXLWindowSpec.h"
#include "naucrates/dxl/parser/CParseHandlerLogicalOp.h"

namespace gpdxl
{
using namespace gpos;

XERCES_CPP_NAMESPACE_USE

//---------------------------------------------------------------------------
//	@class:
//		CParseHandlerLogicalWindow
//
//	@doc:
//		Parse handler for parsing a logical window operator
//
//---------------------------------------------------------------------------
class CParseHandlerLogicalWindow : public CParseHandlerLogicalOp
{
private:
	// process the start of an element
	void StartElement(
		const XMLCh *const element_uri,			// URI of element's namespace
		const XMLCh *const element_local_name,	// local part of element's name
		const XMLCh *const element_qname,		// element's qname
		const Attributes &attr					// element's attributes
		) override;

	// process the end of an element
	void EndElement(
		const XMLCh *const element_uri,			// URI of element's namespace
		const XMLCh *const element_local_name,	// local part of element's name
		const XMLCh *const element_qname		// element's qname
		) override;

public:
	CParseHandlerLogicalWindow(const CParseHandlerLogicalWindow &) = delete;

	// ctor
	CParseHandlerLogicalWindow(CMemoryPool *mp,
							   CParseHandlerManager *parse_handler_mgr,
							   CParseHandlerBase *parse_handler_root);
};
}  // namespace gpdxl

#endif	// !GPDXL_CParseHandlerLogicalWindow_H

// EOF
