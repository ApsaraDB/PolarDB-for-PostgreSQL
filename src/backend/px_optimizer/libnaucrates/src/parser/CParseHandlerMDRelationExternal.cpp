//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 EMC Corp.
//
//	@filename:
//		CParseHandlerMDRelationExternal.cpp
//
//	@doc:
//		Implementation of the SAX parse handler class for parsing external
//		relation metadata.
//---------------------------------------------------------------------------

#include "naucrates/dxl/parser/CParseHandlerMDRelationExternal.h"

#include "naucrates/dxl/operators/CDXLOperatorFactory.h"
#include "naucrates/dxl/parser/CParseHandlerFactory.h"
#include "naucrates/dxl/parser/CParseHandlerMDIndexInfoList.h"
#include "naucrates/dxl/parser/CParseHandlerManager.h"
#include "naucrates/dxl/parser/CParseHandlerMetadataColumns.h"
#include "naucrates/dxl/parser/CParseHandlerMetadataIdList.h"
#include "naucrates/dxl/parser/CParseHandlerScalarOp.h"

#define GPDXL_DEFAULT_REJLIMIT -1

using namespace gpdxl;


XERCES_CPP_NAMESPACE_USE

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerMDRelationExternal::CParseHandlerMDRelationExternal
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CParseHandlerMDRelationExternal::CParseHandlerMDRelationExternal(
	CMemoryPool *mp, CParseHandlerManager *parse_handler_mgr,
	CParseHandlerBase *parse_handler_root)
	: CParseHandlerMDRelation(mp, parse_handler_mgr, parse_handler_root),
	  m_reject_limit(GPDXL_DEFAULT_REJLIMIT),
	  m_is_rej_limit_in_rows(false),
	  m_mdid_fmt_err_table(nullptr)
{
	m_rel_storage_type = IMDRelation::ErelstorageExternal;
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerMDRelationExternal::StartElement
//
//	@doc:
//		Invoked by Xerces to process an opening tag
//
//---------------------------------------------------------------------------
void
CParseHandlerMDRelationExternal::StartElement(
	const XMLCh *const element_uri, const XMLCh *const element_local_name,
	const XMLCh *const element_qname, const Attributes &attrs)
{
	if (0 == XMLString::compareString(
				 CDXLTokens::XmlstrToken(EdxltokenRelDistrOpfamilies),
				 element_local_name))
	{
		// parse handler for check constraints
		m_opfamilies_parse_handler = CParseHandlerFactory::GetParseHandler(
			m_mp, CDXLTokens::XmlstrToken(EdxltokenMetadataIdList),
			m_parse_handler_mgr, this);
		m_parse_handler_mgr->ActivateParseHandler(m_opfamilies_parse_handler);
		this->Append(m_opfamilies_parse_handler);
		m_opfamilies_parse_handler->startElement(
			element_uri, element_local_name, element_qname, attrs);

		return;
	}

	if (0 != XMLString::compareString(
				 CDXLTokens::XmlstrToken(EdxltokenRelationExternal),
				 element_local_name))
	{
		CWStringDynamic *str = CDXLUtils::CreateDynamicStringFromXMLChArray(
			m_parse_handler_mgr->GetDXLMemoryManager(), element_local_name);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag,
				   str->GetBuffer());
	}

	// parse main relation attributes: name, id, distribution policy and keys
	ParseRelationAttributes(attrs, EdxltokenRelation);

	// parse reject limit
	const XMLCh *xml_str_reject_limit =
		attrs.getValue(CDXLTokens::XmlstrToken(EdxltokenExtRelRejLimit));
	if (nullptr != xml_str_reject_limit)
	{
		m_reject_limit = CDXLOperatorFactory::ConvertAttrValueToInt(
			m_parse_handler_mgr->GetDXLMemoryManager(), xml_str_reject_limit,
			EdxltokenExtRelRejLimit, EdxltokenRelationExternal);
		m_is_rej_limit_in_rows =
			CDXLOperatorFactory::ExtractConvertAttrValueToBool(
				m_parse_handler_mgr->GetDXLMemoryManager(), attrs,
				EdxltokenExtRelRejLimitInRows, EdxltokenRelationExternal);
	}

	// format error table id
	const XMLCh *xml_str_err_rel_id =
		attrs.getValue(CDXLTokens::XmlstrToken(EdxltokenExtRelFmtErrRel));
	if (nullptr != xml_str_err_rel_id)
	{
		m_mdid_fmt_err_table = CDXLOperatorFactory::MakeMdIdFromStr(
			m_parse_handler_mgr->GetDXLMemoryManager(), xml_str_err_rel_id,
			EdxltokenExtRelFmtErrRel, EdxltokenRelationExternal);
	}

	// parse whether a hash distributed relation needs to be considered as random distributed
	const XMLCh *xml_str_convert_hash_to_random =
		attrs.getValue(CDXLTokens::XmlstrToken(EdxltokenConvertHashToRandom));
	if (nullptr != xml_str_convert_hash_to_random)
	{
		m_convert_hash_to_random = CDXLOperatorFactory::ConvertAttrValueToBool(
			m_parse_handler_mgr->GetDXLMemoryManager(),
			xml_str_convert_hash_to_random, EdxltokenConvertHashToRandom,
			EdxltokenRelationExternal);
	}

	// parse children
	ParseChildNodes();
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerMDRelationExternal::EndElement
//
//	@doc:
//		Invoked by Xerces to process a closing tag
//
//---------------------------------------------------------------------------
void
CParseHandlerMDRelationExternal::EndElement(
	const XMLCh *const,	 // element_uri,
	const XMLCh *const element_local_name,
	const XMLCh *const	// element_qname
)
{
	if (0 != XMLString::compareString(
				 CDXLTokens::XmlstrToken(EdxltokenRelationExternal),
				 element_local_name))
	{
		CWStringDynamic *str = CDXLUtils::CreateDynamicStringFromXMLChArray(
			m_parse_handler_mgr->GetDXLMemoryManager(), element_local_name);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag,
				   str->GetBuffer());
	}

	// construct metadata object from the created child elements
	CParseHandlerMetadataColumns *md_cols_parse_handler =
		dynamic_cast<CParseHandlerMetadataColumns *>((*this)[0]);
	CParseHandlerMDIndexInfoList *md_index_info_list_parse_handler =
		dynamic_cast<CParseHandlerMDIndexInfoList *>((*this)[1]);
	CParseHandlerMetadataIdList *mdid_triggers_parse_list =
		dynamic_cast<CParseHandlerMetadataIdList *>((*this)[2]);
	CParseHandlerMetadataIdList *mdid_check_constraint_parse_handler =
		dynamic_cast<CParseHandlerMetadataIdList *>((*this)[3]);

	GPOS_ASSERT(nullptr != md_cols_parse_handler->GetMdColArray());
	GPOS_ASSERT(nullptr !=
				md_index_info_list_parse_handler->GetMdIndexInfoArray());
	GPOS_ASSERT(nullptr != mdid_check_constraint_parse_handler->GetMdIdArray());

	// refcount child objects
	CMDColumnArray *md_col_array = md_cols_parse_handler->GetMdColArray();
	CMDIndexInfoArray *md_index_info_array =
		md_index_info_list_parse_handler->GetMdIndexInfoArray();
	IMdIdArray *mdid_triggers_array = mdid_triggers_parse_list->GetMdIdArray();
	IMdIdArray *mdid_check_constraint_array =
		mdid_check_constraint_parse_handler->GetMdIdArray();

	md_col_array->AddRef();
	md_index_info_array->AddRef();
	mdid_triggers_array->AddRef();
	mdid_check_constraint_array->AddRef();

	IMdIdArray *distr_opfamilies = nullptr;
	if (m_rel_distr_policy == IMDRelation::EreldistrHash &&
		m_opfamilies_parse_handler != nullptr)
	{
		distr_opfamilies = dynamic_cast<CParseHandlerMetadataIdList *>(
							   m_opfamilies_parse_handler)
							   ->GetMdIdArray();
		distr_opfamilies->AddRef();
	}

	m_imd_obj = GPOS_NEW(m_mp) CMDRelationExternalGPDB(
		m_mp, m_mdid, m_mdname, m_rel_distr_policy, md_col_array,
		m_distr_col_array, distr_opfamilies, m_convert_hash_to_random,
		m_key_sets_arrays, md_index_info_array, mdid_triggers_array,
		mdid_check_constraint_array, m_reject_limit, m_is_rej_limit_in_rows,
		m_mdid_fmt_err_table);

	// deactivate handler
	m_parse_handler_mgr->DeactivateHandler();
}

// EOF
