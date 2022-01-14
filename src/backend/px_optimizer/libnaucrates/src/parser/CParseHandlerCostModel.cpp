//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 VMware, Inc. or its affiliates.
//	Copyright (C) 2021, Alibaba Group Holding Limited
//
//	@filename:
//		CParseHandlerCostModel.cpp
//
//	@doc:
//		Implementation of the SAX parse handler class for parsing cost model
//		config params
//---------------------------------------------------------------------------

#include "naucrates/dxl/parser/CParseHandlerCostModel.h"

#include "gpos/common/CBitSet.h"

#include "gpdbcost/CCostModelGPDB.h"
#include "naucrates/dxl/operators/CDXLOperatorFactory.h"
#include "naucrates/dxl/parser/CParseHandlerCostParams.h"
#include "naucrates/dxl/parser/CParseHandlerFactory.h"
#include "naucrates/dxl/parser/CParseHandlerManager.h"
#include "naucrates/dxl/xml/dxltokens.h"
#include "naucrates/traceflags/traceflags.h"
#include "gpdbcost/CCostModelPolarDB.h"

using namespace gpdxl;
using namespace gpdbcost;

XERCES_CPP_NAMESPACE_USE

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerCostModel::CParseHandlerCostModel
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CParseHandlerCostModel::CParseHandlerCostModel(
	CMemoryPool *mp, CParseHandlerManager *parse_handler_mgr,
	CParseHandlerBase *parse_handler_root)
	: CParseHandlerBase(mp, parse_handler_mgr, parse_handler_root),
	  m_num_of_segments(0),
	  m_cost_model(nullptr),
	  m_parse_handler_cost_params(nullptr)
{
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerCostModel::~CParseHandlerCostModel
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CParseHandlerCostModel::~CParseHandlerCostModel()
{
	CRefCount::SafeRelease(m_cost_model);
	GPOS_DELETE(m_parse_handler_cost_params);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerCostModel::StartElement
//
//	@doc:
//		Invoked by Xerces to process an opening tag
//
//---------------------------------------------------------------------------
void
CParseHandlerCostModel::StartElement(const XMLCh *const element_uri,
									 const XMLCh *const element_local_name,
									 const XMLCh *const element_qname,
									 const Attributes &attrs)
{
	if (0 == XMLString::compareString(
				 CDXLTokens::XmlstrToken(EdxltokenCostModelConfig),
				 element_local_name))
	{
		m_num_of_segments = CDXLOperatorFactory::ExtractConvertAttrValueToUlong(
			m_parse_handler_mgr->GetDXLMemoryManager(), attrs,
			EdxltokenSegmentsForCosting, EdxltokenCostModelConfig);

		m_cost_model_type = (ICostModel::ECostModelType)
			CDXLOperatorFactory::ExtractConvertAttrValueToUlong(
				m_parse_handler_mgr->GetDXLMemoryManager(), attrs,
				EdxltokenCostModelType, EdxltokenCostModelConfig);
	}
	else if (0 == XMLString::compareString(
					  CDXLTokens::XmlstrToken(EdxltokenCostParams),
					  element_local_name))
	{
		CParseHandlerBase *pphCostParams =
			CParseHandlerFactory::GetParseHandler(
				m_mp, CDXLTokens::XmlstrToken(EdxltokenCostParams),
				m_parse_handler_mgr, this);
		m_parse_handler_cost_params =
			static_cast<CParseHandlerCostParams *>(pphCostParams);
		m_parse_handler_mgr->ActivateParseHandler(pphCostParams);

		pphCostParams->startElement(element_uri, element_local_name,
									element_qname, attrs);
	}
	else
	{
		CWStringDynamic *str = CDXLUtils::CreateDynamicStringFromXMLChArray(
			m_parse_handler_mgr->GetDXLMemoryManager(), element_local_name);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag,
				   str->GetBuffer());
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerCostModel::EndElement
//
//	@doc:
//		Invoked by Xerces to process a closing tag
//
//---------------------------------------------------------------------------
void
CParseHandlerCostModel::EndElement(const XMLCh *const,	// element_uri,
								   const XMLCh *const element_local_name,
								   const XMLCh *const  // element_qname
)
{
	if (0 != XMLString::compareString(
				 CDXLTokens::XmlstrToken(EdxltokenCostModelConfig),
				 element_local_name))
	{
		CWStringDynamic *str = CDXLUtils::CreateDynamicStringFromXMLChArray(
			m_parse_handler_mgr->GetDXLMemoryManager(), element_local_name);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag,
				   str->GetBuffer());
	}

	switch (m_cost_model_type)
	{
		// FIXME: Remove ICostModel::ECostModelType
		// Right now, we use the same class for all cost models
		case ICostModel::EcmtGPDBLegacy:
		case ICostModel::EcmtGPDBExperimental:
		case ICostModel::EcmtGPDBCalibrated:
			CCostModelParamsGPDB *pcp;

			if (nullptr == m_parse_handler_cost_params)
			{
				pcp = nullptr;
			}
			else
			{
				pcp = dynamic_cast<CCostModelParamsGPDB *>(
					m_parse_handler_cost_params->GetCostModelParams());
				GPOS_ASSERT(nullptr != pcp);
				pcp->AddRef();
			}
			m_cost_model =
				GPOS_NEW(m_mp) CCostModelGPDB(m_mp, m_num_of_segments, pcp);
			break;
		case ICostModel::EcmtPolarDBCalibrated:
			CCostModelParamsPolarDB *pcpp;

			if (NULL == m_parse_handler_cost_params)
			{
				pcpp = NULL;
				GPOS_ASSERT(false && "CostModelParam handler not set");
			}
			else
			{
				pcpp = dynamic_cast<CCostModelParamsPolarDB *>(m_parse_handler_cost_params->GetCostModelParams());
				GPOS_ASSERT(NULL != pcpp);
				pcpp->AddRef();
			}
			m_cost_model = GPOS_NEW(m_mp) CCostModelPolarDB(m_mp, m_num_of_segments, pcpp);
			break;			
		case ICostModel::EcmtSentinel:
			GPOS_ASSERT(false && "Unexpected cost model type");
			break;
	}

	// deactivate handler
	m_parse_handler_mgr->DeactivateHandler();
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerCostModel::Pmc
//
//	@doc:
//		Returns the cost model config object
//
//---------------------------------------------------------------------------
ICostModel *
CParseHandlerCostModel::GetCostModel() const
{
	return m_cost_model;
}

// EOF
