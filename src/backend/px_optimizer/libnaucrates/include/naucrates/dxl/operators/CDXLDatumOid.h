//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CDXLDatumOid.h
//
//	@doc:
//		Class for representing DXL oid datum
//
//	@owner:
//
//
//	@test:
//
//---------------------------------------------------------------------------

#ifndef GPDXL_CDXLDatumOid_H
#define GPDXL_CDXLDatumOid_H

#include "gpos/base.h"

#include "naucrates/dxl/operators/CDXLDatum.h"

namespace gpdxl
{
using namespace gpos;

// fwd decl
class CXMLSerializer;

//---------------------------------------------------------------------------
//	@class:
//		CDXLDatumOid
//
//	@doc:
//		Class for representing DXL oid datums
//
//---------------------------------------------------------------------------
class CDXLDatumOid : public CDXLDatum
{
private:
	// oid value
	OID m_oid_val;

public:
	CDXLDatumOid(const CDXLDatumOid &) = delete;

	// ctor
	CDXLDatumOid(CMemoryPool *mp, IMDId *mdid_type, BOOL is_null, OID oid_val);

	// dtor
	~CDXLDatumOid() override = default;

	// accessor of oid value
	OID OidValue() const;

	// serialize the datum as the given element
	void Serialize(CXMLSerializer *xml_serializer) override;

	// datum type
	EdxldatumType
	GetDatumType() const override
	{
		return CDXLDatum::EdxldatumOid;
	}

	// conversion function
	static CDXLDatumOid *
	Cast(CDXLDatum *dxl_datum)
	{
		GPOS_ASSERT(nullptr != dxl_datum);
		GPOS_ASSERT(CDXLDatum::EdxldatumOid == dxl_datum->GetDatumType());

		return dynamic_cast<CDXLDatumOid *>(dxl_datum);
	}
};
}  // namespace gpdxl

#endif	// !GPDXL_CDXLDatumOid_H

// EOF
