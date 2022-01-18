//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 VMware, Inc. or its affiliates.
//
//	@filename:
//		CConstExprEvaluatorForDates.cpp
//
//	@doc:
//		Implementation of a constant expression evaluator for dates data
//
//	@owner:
//
//
//	@test:
//
//---------------------------------------------------------------------------

#include "unittest/gpopt/CConstExprEvaluatorForDates.h"

#include "gpopt/base/CUtils.h"
#include "gpopt/operators/CScalarCmp.h"
#include "naucrates/base/IDatum.h"
#include "naucrates/md/CMDIdGPDB.h"
#include "naucrates/md/IMDType.h"

using namespace gpnaucrates;
using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CConstExprEvaluatorForDates::PexprEval
//
//	@doc:
//		It expects that the given expression is a scalar comparison between
//		two date constants. It compares the two constants using their lint
//		stats mapping, which in the case of the date type gives a correct result.
//		If it gets an illegal expression, an assertion failure is raised in
//		debug mode.
//
//---------------------------------------------------------------------------
CExpression *
CConstExprEvaluatorForDates::PexprEval(CExpression *pexpr)
{
	GPOS_ASSERT(COperator::EopScalarCmp == pexpr->Pop()->Eopid());
	GPOS_ASSERT(COperator::EopScalarConst == (*pexpr)[0]->Pop()->Eopid());
	GPOS_ASSERT(COperator::EopScalarConst == (*pexpr)[1]->Pop()->Eopid());

	CScalarConst *popScalarLeft =
		dynamic_cast<CScalarConst *>((*pexpr)[0]->Pop());

	GPOS_ASSERT(
		CMDIdGPDB::m_mdid_date.Equals(popScalarLeft->GetDatum()->MDId()));
	CScalarConst *popScalarRight =
		dynamic_cast<CScalarConst *>((*pexpr)[1]->Pop());

	GPOS_ASSERT(
		CMDIdGPDB::m_mdid_date.Equals(popScalarRight->GetDatum()->MDId()));

	CScalarCmp *popScCmp = dynamic_cast<CScalarCmp *>(pexpr->Pop());
	LINT dLeft = popScalarLeft->GetDatum()->GetLINTMapping();
	LINT dRight = popScalarRight->GetDatum()->GetLINTMapping();
	BOOL result = false;
	switch (popScCmp->ParseCmpType())
	{
		case IMDType::EcmptEq:
			result = dLeft == dRight;
			break;
		case IMDType::EcmptNEq:
			result = dLeft != dRight;
			break;
		case IMDType::EcmptL:
			result = dLeft < dRight;
			break;
		case IMDType::EcmptLEq:
			result = dLeft <= dRight;
			break;
		case IMDType::EcmptG:
			result = dLeft > dRight;
			break;
		case IMDType::EcmptGEq:
			result = dLeft >= dRight;
			break;
		default:
			GPOS_ASSERT(false && "Unsupported comparison");
			return nullptr;
	}
	CExpression *pexprResult = CUtils::PexprScalarConstBool(m_mp, result);

	return pexprResult;
}

// EOF
