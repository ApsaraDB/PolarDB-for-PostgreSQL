//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2017 VMware, Inc. or its affiliates.
//
//	@filename:
//		CParseHandlerScalarValuesList.h
//
//	@doc:
//		SAX parse handler class for parsing scalar value list.
//---------------------------------------------------------------------------

#ifndef GPDXL_CParseHandlerScalarValuesList_H
#define GPDXL_CParseHandlerScalarValuesList_H

#include "gpos/base.h"

#include "naucrates/dxl/parser/CParseHandlerOp.h"

namespace gpdxl
{
using namespace gpos;


XERCES_CPP_NAMESPACE_USE

// Parse handler for parsing a value list operator
class CParseHandlerScalarValuesList : public CParseHandlerOp
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
	CParseHandlerScalarValuesList(const CParseHandlerScalarValuesList &) =
		delete;

	// ctor/dtor
	CParseHandlerScalarValuesList(CMemoryPool *mp,
								  CParseHandlerManager *parse_handler_mgr,
								  CParseHandlerBase *parse_handler_root);
};
}  // namespace gpdxl

#endif	// !GPDXL_CParseHandlerScalarValuesList_H

// EOF
