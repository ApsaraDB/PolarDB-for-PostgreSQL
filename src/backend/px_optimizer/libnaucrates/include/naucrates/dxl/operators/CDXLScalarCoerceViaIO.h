//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 VMware, Inc. or its affiliates.
//
//	@filename:
//		CDXLScalarCoerceViaIO.h
//
//	@doc:
//		Class for representing DXL CoerceViaIO operation,
//		the operator captures coercing a value from one type to another, by
//		calling the output function of the argument type, and passing the
//		result to the input function of the result type.
//
//	@owner:
//
//	@test:
//
//---------------------------------------------------------------------------

#ifndef GPDXL_CDXLScalarCoerceViaIO_H
#define GPDXL_CDXLScalarCoerceViaIO_H

#include "gpos/base.h"

#include "naucrates/dxl/operators/CDXLScalarCoerceBase.h"
#include "naucrates/md/IMDId.h"

namespace gpdxl
{
using namespace gpos;
using namespace gpmd;

//---------------------------------------------------------------------------
//	@class:
//		CDXLScalarCoerceViaIO
//
//	@doc:
//		Class for representing DXL casting operator
//
//---------------------------------------------------------------------------
class CDXLScalarCoerceViaIO : public CDXLScalarCoerceBase
{
private:
public:
	CDXLScalarCoerceViaIO(const CDXLScalarCoerceViaIO &) = delete;

	// ctor/dtor
	CDXLScalarCoerceViaIO(CMemoryPool *mp, IMDId *mdid_type, INT type_modifier,
						  EdxlCoercionForm dxl_coerce_format, INT location);

	~CDXLScalarCoerceViaIO() override = default;

	// ident accessor
	Edxlopid
	GetDXLOperator() const override
	{
		return EdxlopScalarCoerceViaIO;
	}

	// name of the DXL operator name
	const CWStringConst *GetOpNameStr() const override;

	// conversion function
	static CDXLScalarCoerceViaIO *
	Cast(CDXLOperator *dxl_op)
	{
		GPOS_ASSERT(nullptr != dxl_op);
		GPOS_ASSERT(EdxlopScalarCoerceViaIO == dxl_op->GetDXLOperator());

		return dynamic_cast<CDXLScalarCoerceViaIO *>(dxl_op);
	}
};
}  // namespace gpdxl

#endif	// !GPDXL_CDXLScalarCoerceViaIO_H

// EOF
