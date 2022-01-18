//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2017 VMware, Inc. or its affiliates.
//
//	@filename:
//		CDXLScalarCoerceBase.h
//
//	@doc:
//		Base class for representing DXL coerce operators
//
//	@owner:
//
//	@test:
//
//---------------------------------------------------------------------------

#ifndef GPDXL_CDXLScalarCoerceBase_H
#define GPDXL_CDXLScalarCoerceBase_H

#include "gpos/base.h"

#include "naucrates/dxl/operators/CDXLScalar.h"
#include "naucrates/md/IMDId.h"

namespace gpdxl
{
using namespace gpos;
using namespace gpmd;

//---------------------------------------------------------------------------
//	@class:
//		CDXLScalarCoerceBase
//
//	@doc:
//		Base class for representing DXL coerce operators
//
//---------------------------------------------------------------------------
class CDXLScalarCoerceBase : public CDXLScalar
{
private:
	// catalog MDId of the result type
	IMDId *m_result_type_mdid;

	// output type modifier
	INT m_type_modifier;

	// coercion form
	EdxlCoercionForm m_dxl_coerce_format;

	// location of token to be coerced
	INT m_location;

public:
	CDXLScalarCoerceBase(const CDXLScalarCoerceBase &) = delete;

	// ctor/dtor
	CDXLScalarCoerceBase(CMemoryPool *mp, IMDId *mdid_type, INT type_modifier,
						 EdxlCoercionForm dxl_coerce_format, INT location);

	~CDXLScalarCoerceBase() override;

	// return result type
	IMDId *
	GetResultTypeMdId() const
	{
		return m_result_type_mdid;
	}

	// return type modifier
	INT
	TypeModifier() const
	{
		return m_type_modifier;
	}

	// return coercion form
	EdxlCoercionForm
	GetDXLCoercionForm() const
	{
		return m_dxl_coerce_format;
	}

	// return token location
	INT
	GetLocation() const
	{
		return m_location;
	}

	// does the operator return a boolean result
	BOOL HasBoolResult(CMDAccessor *md_accessor) const override;

#ifdef GPOS_DEBUG
	// checks whether the operator has valid structure, i.e. number and
	// types of child nodes
	void AssertValid(const CDXLNode *node,
					 BOOL validate_children) const override;
#endif	// GPOS_DEBUG

	// serialize operator in DXL format
	void SerializeToDXL(CXMLSerializer *xml_serializer,
						const CDXLNode *node) const override;
};
}  // namespace gpdxl

#endif	// !GPDXL_CDXLScalarCoerceBase_H

// EOF
