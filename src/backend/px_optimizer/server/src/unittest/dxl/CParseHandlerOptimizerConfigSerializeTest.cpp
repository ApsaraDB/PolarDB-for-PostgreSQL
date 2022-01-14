//	Greenplum Database
//	Copyright (C) 2018 VMware, Inc. or its affiliates.

#include "unittest/dxl/CParseHandlerOptimizerConfigSerializeTest.h"

#include "gpos/io/COstreamString.h"

#include "gpopt/optimizer/COptimizerConfig.h"
#include "naucrates/dxl/CDXLUtils.h"
#include "naucrates/dxl/xml/CXMLSerializer.h"

#include "unittest/gpopt/CTestUtils.h"

namespace
{
class Fixture
{
private:
	const CAutoMemoryPool m_amp;
	CAutoRg<CHAR> m_szDXL;

public:
	Fixture() : m_amp(), m_szDXL(nullptr)
	{
	}

	CMemoryPool *
	Pmp() const
	{
		return m_amp.Pmp();
	}

	static const CHAR *
	SzValidationPath(BOOL fValidate)
	{
		if (fValidate)
		{
			return CTestUtils::m_szXSDPath;
		}
		else
		{
			return nullptr;
		}
	}

	const CHAR *
	SzDxl(const CHAR *dxl_filename)
	{
		m_szDXL = CDXLUtils::Read(Pmp(), dxl_filename);

		GPOS_CHECK_ABORT;

		return m_szDXL.Rgt();
	}
};
}  // namespace

static void
SerializeOptimizerConfig(CMemoryPool *mp, COptimizerConfig *optimizer_config,
						 COstream &oos, BOOL indentation)
{
	GPOS_ASSERT(nullptr != mp);
	GPOS_ASSERT(nullptr != optimizer_config);

	CXMLSerializer xml_serializer(mp, oos, indentation);

	// Add XML version and encoding, DXL document header, and namespace
	CDXLUtils::SerializeHeader(mp, &xml_serializer);

	// Make a dummy bitset
	CBitSet *pbs = GPOS_NEW(mp) CBitSet(mp, 256);

	optimizer_config->Serialize(mp, &xml_serializer, pbs);

	// Add DXL document footer
	CDXLUtils::SerializeFooter(&xml_serializer);

	pbs->Release();
	return;
}

namespace gpdxl
{
// Optimizer Config request file
const CHAR *dxl_filename = "../data/dxl/parse_tests/OptimizerConfig.xml";

// Parse an optimizer config and verify correctness of serialization.
// Serialization of COptimizerConfig is only done for writing to a DXL file as part of creating a minidump
GPOS_RESULT
CParseHandlerOptimizerConfigSerializeTest::EresUnittest()
{
	BOOL fValidate = false;
	Fixture f;
	CMemoryPool *mp = f.Pmp();
	// Valid input for this test requires DXL in the form of:
	// Please note that most editors will automatically add a newline at the end of the file
	// This will cause the test to fail, as we do a byte-wise string comparison as opposed to a
	// comparison of canonicalized XML
	const CHAR *dxl_string = f.SzDxl(dxl_filename);
	const CHAR *szValidationPath = Fixture::SzValidationPath(fValidate);

	CWStringDynamic str(mp);
	COstreamString oss(&str);

	COptimizerConfig *poc =
		CDXLUtils::ParseDXLToOptimizerConfig(mp, dxl_string, szValidationPath);

	GPOS_ASSERT(nullptr != poc);

	GPOS_CHECK_ABORT;

	// Though the serialization of an optimizer config will include a traceflags element
	// This test tests only the serializing of the traceflags element itself and not the traceflag values
	// The production code calls a method to get the traceflags from a global task context
	SerializeOptimizerConfig(mp, poc, oss, false);
	GPOS_CHECK_ABORT;

	CWStringDynamic strExpected(mp);
	strExpected.AppendFormat(GPOS_WSZ_LIT("%s"), dxl_string);

	GPOS_ASSERT(strExpected.Equals(&str));

	poc->Release();

	return GPOS_OK;
}
}  // namespace gpdxl

// EOF
