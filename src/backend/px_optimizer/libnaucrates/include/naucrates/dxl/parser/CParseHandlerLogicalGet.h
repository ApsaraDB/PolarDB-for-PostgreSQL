//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 Greenplum, Inc.
//
//	@filename:
//		CParseHandlerLogicalGet.h
//
//	@doc:
//		SAX parse handler class for parsing logical get operator nodes.
//---------------------------------------------------------------------------

#ifndef GPDXL_CParseHandlerLogicalGet_H
#define GPDXL_CParseHandlerLogicalGet_H

#include "gpos/base.h"

#include "naucrates/dxl/operators/CDXLLogicalGet.h"
#include "naucrates/dxl/parser/CParseHandlerLogicalOp.h"
#include "naucrates/dxl/xml/dxltokens.h"

namespace gpdxl
{
using namespace gpos;


XERCES_CPP_NAMESPACE_USE

//---------------------------------------------------------------------------
//	@class:
//		CParseHandlerLogicalGet
//
//	@doc:
//		Parse handler for parsing a logical get operator
//
//---------------------------------------------------------------------------
class CParseHandlerLogicalGet : public CParseHandlerLogicalOp
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

protected:
	// start element helper function
	void StartElement(const XMLCh *const element_local_name,
					  Edxltoken token_type);

	// end element helper function
	void EndElement(const XMLCh *const element_local_name,
					Edxltoken token_type);

public:
	CParseHandlerLogicalGet(const CParseHandlerLogicalGet &) = delete;

	// ctor
	CParseHandlerLogicalGet(CMemoryPool *mp,
							CParseHandlerManager *parse_handler_mgr,
							CParseHandlerBase *parse_handler_root);
};
}  // namespace gpdxl

#endif	// !GPDXL_CParseHandlerLogicalGet_H

// EOF
