//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		CParseHandlerMetadata.h
//
//	@doc:
//		SAX parse handler class for parsing metadata from a DXL document.
//---------------------------------------------------------------------------

#ifndef GPDXL_CParseHandlerMetadata_H
#define GPDXL_CParseHandlerMetadata_H

#include "gpos/base.h"

#include "naucrates/dxl/parser/CParseHandlerBase.h"
#include "naucrates/dxl/xml/dxltokens.h"
#include "naucrates/md/IMDCacheObject.h"

namespace gpdxl
{
using namespace gpos;
using namespace gpmd;
using namespace gpnaucrates;

XERCES_CPP_NAMESPACE_USE


//---------------------------------------------------------------------------
//	@class:
//		CParseHandlerMetadata
//
//	@doc:
//		Parse handler for metadata.
//
//---------------------------------------------------------------------------
class CParseHandlerMetadata : public CParseHandlerBase
{
private:
	// list of parsed metadata objects
	IMDCacheObjectArray *m_mdid_cached_obj_array;

	// list of parsed mdids
	IMdIdArray *m_mdid_array;

	// list of parsed metatadata source system ids
	CSystemIdArray *m_system_id_array;

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

	// parse an array of system ids from the XML attributes
	CSystemIdArray *GetSrcSysIdArray(const Attributes &attr,
									 Edxltoken target_attr,
									 Edxltoken target_elem);


public:
	CParseHandlerMetadata(const CParseHandlerMetadata &) = delete;

	// ctor
	CParseHandlerMetadata(CMemoryPool *mp,
						  CParseHandlerManager *parse_handler_mgr,
						  CParseHandlerBase *parse_handler_root);

	// dtor
	~CParseHandlerMetadata() override;

	// parse hander type
	EDxlParseHandlerType GetParseHandlerType() const override;

	// return the list of parsed metadata objects
	IMDCacheObjectArray *GetMdIdCachedObjArray();

	// return the list of parsed mdids
	IMdIdArray *GetMdIdArray();

	// return the list of parsed system ids
	CSystemIdArray *GetSysidPtrArray();
};
}  // namespace gpdxl

#endif	// !GPDXL_CParseHandlerMetadata_H

// EOF
