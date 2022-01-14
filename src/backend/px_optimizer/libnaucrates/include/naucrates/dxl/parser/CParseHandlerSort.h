//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 Greenplum, Inc.
//
//	@filename:
//		CParseHandlerSort.h
//
//	@doc:
//		SAX parse handler class for parsing sort operator nodes.
//---------------------------------------------------------------------------

#ifndef GPDXL_CParseHandlerSort_H
#define GPDXL_CParseHandlerSort_H

#include "gpos/base.h"

#include "naucrates/dxl/operators/CDXLPhysicalSort.h"
#include "naucrates/dxl/parser/CParseHandlerPhysicalOp.h"

namespace gpdxl
{
using namespace gpos;

XERCES_CPP_NAMESPACE_USE

//---------------------------------------------------------------------------
//	@class:
//		CParseHandlerSort
//
//	@doc:
//		Parse handler for sort operators
//
//---------------------------------------------------------------------------
class CParseHandlerSort : public CParseHandlerPhysicalOp
{
private:
	// the sort operator
	CDXLPhysicalSort *m_dxl_op;

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
	CParseHandlerSort(const CParseHandlerSort &) = delete;

	// ctor/dtor
	CParseHandlerSort(CMemoryPool *mp, CParseHandlerManager *parse_handler_mgr,
					  CParseHandlerBase *parse_handler_root);
};
}  // namespace gpdxl

#endif	// !GPDXL_CParseHandlerSort_H

// EOF
