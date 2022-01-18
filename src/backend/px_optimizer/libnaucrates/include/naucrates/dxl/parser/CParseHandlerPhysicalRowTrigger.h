//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CParseHandlerPhysicalRowTrigger.h
//
//	@doc:
//		Parse handler for parsing a physical row trigger operator
//---------------------------------------------------------------------------
#ifndef GPDXL_CParseHandlerPhysicalRowTrigger_H
#define GPDXL_CParseHandlerPhysicalRowTrigger_H

#include "gpos/base.h"

#include "naucrates/dxl/operators/CDXLPhysicalRowTrigger.h"
#include "naucrates/dxl/parser/CParseHandlerPhysicalOp.h"


namespace gpdxl
{
using namespace gpos;

XERCES_CPP_NAMESPACE_USE

//---------------------------------------------------------------------------
//	@class:
//		CParseHandlerPhysicalRowTrigger
//
//	@doc:
//		Parse handler for parsing a physical row trigger operator
//
//---------------------------------------------------------------------------
class CParseHandlerPhysicalRowTrigger : public CParseHandlerPhysicalOp
{
private:
	CDXLPhysicalRowTrigger *m_dxl_op;

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
	CParseHandlerPhysicalRowTrigger(const CParseHandlerPhysicalRowTrigger &) =
		delete;

	// ctor
	CParseHandlerPhysicalRowTrigger(CMemoryPool *mp,
									CParseHandlerManager *parse_handler_mgr,
									CParseHandlerBase *parse_handler_root);
};
}  // namespace gpdxl

#endif	// !GPDXL_CParseHandlerPhysicalRowTrigger_H

// EOF
