//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CScalarNullTest.cpp
//
//	@doc:
//		Implementation of scalar null test operator
//---------------------------------------------------------------------------

#include "gpopt/operators/CScalarNullTest.h"

#include "gpos/base.h"

#include "gpopt/base/CColRefSet.h"
#include "gpopt/base/CDrvdPropScalar.h"
#include "gpopt/base/COptCtxt.h"
#include "gpopt/operators/CExpressionHandle.h"
#include "naucrates/md/IMDTypeBool.h"

using namespace gpopt;
using namespace gpmd;


//---------------------------------------------------------------------------
//	@function:
//		CScalarNullTest::Matches
//
//	@doc:
//		Match function on operator level
//
//---------------------------------------------------------------------------
BOOL
CScalarNullTest::Matches(COperator *pop) const
{
	return pop->Eopid() == Eopid();
}


//---------------------------------------------------------------------------
//	@function:
//		CScalarNullTest::MdidType
//
//	@doc:
//		Expression type
//
//---------------------------------------------------------------------------
IMDId *
CScalarNullTest::MdidType() const
{
	CMDAccessor *md_accessor = COptCtxt::PoctxtFromTLS()->Pmda();

	return md_accessor->PtMDType<IMDTypeBool>()->MDId();
}


//---------------------------------------------------------------------------
//	@function:
//		CScalarNullTest::Eber
//
//	@doc:
//		Perform boolean expression evaluation
//
//---------------------------------------------------------------------------
CScalar::EBoolEvalResult
CScalarNullTest::Eber(ULongPtrArray *pdrgpulChildren) const
{
	GPOS_ASSERT(nullptr != pdrgpulChildren);
	GPOS_ASSERT(1 == pdrgpulChildren->Size());

	EBoolEvalResult eber = (EBoolEvalResult) * ((*pdrgpulChildren)[0]);
	switch (eber)
	{
		case EberNull:
			return EberTrue;

		case EberFalse:
		case EberTrue:
			return EberFalse;

		case EberNotTrue:
		default:
			return EberAny;
	}
}


// EOF
