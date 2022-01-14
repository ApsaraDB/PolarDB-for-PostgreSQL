//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CParseHandlerWindowSpecList.h
//
//	@doc:
//		SAX parse handler class for parsing the list of window specifications
//---------------------------------------------------------------------------

#ifndef GPDXL_CParseHandlerWindowSpecList_H
#define GPDXL_CParseHandlerWindowSpecList_H

#include "gpos/base.h"

#include "naucrates/dxl/operators/CDXLWindowSpec.h"
#include "naucrates/dxl/parser/CParseHandlerBase.h"

namespace gpdxl
{
using namespace gpos;

XERCES_CPP_NAMESPACE_USE

//---------------------------------------------------------------------------
//	@class:
//		CParseHandlerWindowSpecList
//
//	@doc:
//		SAX parse handler class for parsing the list of window specifications
//
//---------------------------------------------------------------------------
class CParseHandlerWindowSpecList : public CParseHandlerBase
{
private:
	// list of window specifications
	CDXLWindowSpecArray *m_window_spec_array;

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
	CParseHandlerWindowSpecList(const CParseHandlerWindowSpecList &) = delete;

	// ctor
	CParseHandlerWindowSpecList(CMemoryPool *mp,
								CParseHandlerManager *parse_handler_mgr,
								CParseHandlerBase *pph);

	// list of window keys
	CDXLWindowSpecArray *
	GetDxlWindowSpecArray() const
	{
		return m_window_spec_array;
	}
};
}  // namespace gpdxl

#endif	// !GPDXL_CParseHandlerWindowSpecList_H

// EOF
