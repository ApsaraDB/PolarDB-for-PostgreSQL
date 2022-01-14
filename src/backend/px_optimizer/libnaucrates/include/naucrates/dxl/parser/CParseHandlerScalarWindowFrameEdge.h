//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CParseHandlerScalarWindowFrameEdge.h
//
//	@doc:
//		SAX parse handler class for parsing a window frame edge
//
//---------------------------------------------------------------------------
#ifndef GPDXL_CParseHandlerScalarWindowFrameEdge_H
#define GPDXL_CParseHandlerScalarWindowFrameEdge_H

#include "gpos/base.h"

#include "naucrates/dxl/parser/CParseHandlerScalarOp.h"


namespace gpdxl
{
using namespace gpos;

XERCES_CPP_NAMESPACE_USE

//---------------------------------------------------------------------------
//	@class:
//		CParseHandlerScalarWindowFrameEdge
//
//	@doc:
//		Parse handler for parsing a window frame edge
//
//---------------------------------------------------------------------------
class CParseHandlerScalarWindowFrameEdge : public CParseHandlerScalarOp
{
private:
	// process the start of an element
	void StartElement(const XMLCh *const element_uri,
					  const XMLCh *const element_local_name,
					  const XMLCh *const element_qname,
					  const Attributes &attr) override;

	// process the end of an element
	void EndElement(const XMLCh *const element_uri,
					const XMLCh *const element_local_name,
					const XMLCh *const element_qname) override;

public:
	CParseHandlerScalarWindowFrameEdge(
		const CParseHandlerScalarWindowFrameEdge &) = delete;

	// ctor
	CParseHandlerScalarWindowFrameEdge(CMemoryPool *mp,
									   CParseHandlerManager *parse_handler_mgr,
									   CParseHandlerBase *parse_handler_root);
};
}  // namespace gpdxl

#endif	// !GPDXL_CParseHandlerScalarWindowFrameEdge_H

//EOF
