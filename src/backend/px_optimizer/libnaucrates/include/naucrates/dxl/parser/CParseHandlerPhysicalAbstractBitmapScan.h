//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 VMware, Inc. or its affiliates.
//
//	@filename:
//		CParseHandlerPhysicalAbstractBitmapScan.h
//
//	@doc:
//		SAX parse handler parent class for parsing bitmap scan operator nodes
//---------------------------------------------------------------------------

#ifndef GPDXL_CParseHandlerPhysicalAbstractBitmapScan_H
#define GPDXL_CParseHandlerPhysicalAbstractBitmapScan_H

#include "gpos/base.h"

#include "naucrates/dxl/parser/CParseHandlerPhysicalOp.h"
#include "naucrates/dxl/xml/dxltokens.h"

namespace gpdxl
{
using namespace gpos;

XERCES_CPP_NAMESPACE_USE

//---------------------------------------------------------------------------
//	@class:
//		CParseHandlerPhysicalAbstractBitmapScan
//
//	@doc:
//		Parse handler parent class for parsing bitmap scan operators
//
//---------------------------------------------------------------------------
class CParseHandlerPhysicalAbstractBitmapScan : public CParseHandlerPhysicalOp
{
private:
protected:
	// common StartElement functionality for child classes
	void StartElementHelper(const XMLCh *const element_local_name,
							Edxltoken token_type);

	// common EndElement functionality for child classes
	void EndElementHelper(const XMLCh *const element_local_name,
						  Edxltoken token_type);

public:
	CParseHandlerPhysicalAbstractBitmapScan(
		const CParseHandlerPhysicalAbstractBitmapScan &) = delete;

	// ctor
	CParseHandlerPhysicalAbstractBitmapScan(
		CMemoryPool *mp, CParseHandlerManager *parse_handler_mgr,
		CParseHandlerBase *parse_handler_root)
		: CParseHandlerPhysicalOp(mp, parse_handler_mgr, parse_handler_root)
	{
	}
};
}  // namespace gpdxl

#endif	// !GPDXL_CParseHandlerPhysicalAbstractBitmapScan_H

// EOF
