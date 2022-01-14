//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 VMware, Inc. or its affiliates.
//
//	@filename:
//		CParseHandlerExternalScan.h
//
//	@doc:
//		SAX parse handler class for parsing external scan operator
//---------------------------------------------------------------------------

#ifndef GPDXL_CParseHandlerExternalScan_H
#define GPDXL_CParseHandlerExternalScan_H

#include "gpos/base.h"

#include "naucrates/dxl/operators/CDXLPhysicalExternalScan.h"
#include "naucrates/dxl/parser/CParseHandlerTableScan.h"


namespace gpdxl
{
using namespace gpos;

XERCES_CPP_NAMESPACE_USE

//---------------------------------------------------------------------------
//	@class:
//		CParseHandlerExternalScan
//
//	@doc:
//		Parse handler for parsing external scan operator
//
//---------------------------------------------------------------------------
class CParseHandlerExternalScan : public CParseHandlerTableScan
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
	CParseHandlerExternalScan(const CParseHandlerExternalScan &) = delete;

	// ctor
	CParseHandlerExternalScan(CMemoryPool *mp,
							  CParseHandlerManager *parse_handler_mgr,
							  CParseHandlerBase *parse_handler_root);
};
}  // namespace gpdxl

#endif	// !GPDXL_CParseHandlerExternalScan_H

// EOF
