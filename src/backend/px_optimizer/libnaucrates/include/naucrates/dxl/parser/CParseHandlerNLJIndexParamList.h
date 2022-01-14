//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2018 VMware, Inc. or its affiliates.
//
//	@filename:
//		CParseHandlerNLJIndexParamList.h
//
//	@doc:
//
//		SAX parse handler class for parsing NLJ ParamList
//---------------------------------------------------------------------------

#ifndef GPDXL_CParseHandlerNLJIndexParamList_H
#define GPDXL_CParseHandlerNLJIndexParamList_H

#include "gpos/base.h"

#include "naucrates/dxl/operators/CDXLColRef.h"
#include "naucrates/dxl/parser/CParseHandlerScalarOp.h"

namespace gpdxl
{
using namespace gpos;

XERCES_CPP_NAMESPACE_USE

//---------------------------------------------------------------------------
//	@class:
//		CParseHandlerNLJIndexParamList
//
//	@doc:
//		Parse handler for parsing a scalar NLJ ParamList
//
//---------------------------------------------------------------------------
class CParseHandlerNLJIndexParamList : public CParseHandlerBase
{
private:
	BOOL m_is_param_list;

	// array of outer column references
	CDXLColRefArray *m_nest_params_colrefs_array;

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
	CParseHandlerNLJIndexParamList(const CParseHandlerNLJIndexParamList &) =
		delete;

	// ctor
	CParseHandlerNLJIndexParamList(CMemoryPool *mp,
								   CParseHandlerManager *parse_handler_mgr,
								   CParseHandlerBase *parse_handler_root);

	// dtor
	~CParseHandlerNLJIndexParamList() override;

	// return param column references
	CDXLColRefArray *
	GetNLParamsColRefs() const
	{
		return m_nest_params_colrefs_array;
	}
};

}  // namespace gpdxl
#endif	// GPDXL_CParseHandlerNLJIndexParamList_H

//EOF
