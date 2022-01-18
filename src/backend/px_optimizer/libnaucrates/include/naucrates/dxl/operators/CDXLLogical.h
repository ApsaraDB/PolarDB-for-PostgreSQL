//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 Greenplum, Inc.
//
//	@filename:
//		CDXLLogical.h
//
//	@doc:
//		Base class for DXL logical operators.
//
//---------------------------------------------------------------------------

#ifndef GPDXL_CDXLLogical_H
#define GPDXL_CDXLLogical_H

#include "gpos/base.h"

#include "naucrates/dxl/operators/CDXLColRef.h"
#include "naucrates/dxl/operators/CDXLOperator.h"

namespace gpdxl
{
using namespace gpos;

// fwd decl
class CXMLSerializer;

//---------------------------------------------------------------------------
//	@class:
//		CDXLLogical
//
//	@doc:
//		Base class the DXL logical operators
//
//---------------------------------------------------------------------------
class CDXLLogical : public CDXLOperator
{
private:
public:
	CDXLLogical(const CDXLLogical &) = delete;

	// ctor/dtor
	explicit CDXLLogical(CMemoryPool *mp);

	// Get operator type
	Edxloptype GetDXLOperatorType() const override;
};
}  // namespace gpdxl

#endif	// !GPDXL_CDXLLogical_H

// EOF
