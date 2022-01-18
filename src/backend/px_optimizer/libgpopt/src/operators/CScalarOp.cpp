//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CScalarOp.cpp
//
//	@doc:
//		Implementation of general scalar operator
//---------------------------------------------------------------------------

#include "gpopt/operators/CScalarOp.h"

#include "gpos/base.h"

#include "gpopt/base/CColRefSet.h"
#include "gpopt/base/CDrvdPropScalar.h"
#include "gpopt/base/COptCtxt.h"
#include "gpopt/mdcache/CMDAccessorUtils.h"
#include "gpopt/operators/CExpressionHandle.h"
#include "naucrates/md/IMDScalarOp.h"


using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CScalarOp::CScalarOp
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CScalarOp::CScalarOp(CMemoryPool *mp, IMDId *mdid_op, IMDId *return_type_mdid,
					 const CWStringConst *pstrOp)
	: CScalar(mp),
	  m_mdid_op(mdid_op),
	  m_return_type_mdid(return_type_mdid),
	  m_pstrOp(pstrOp),
	  m_returns_null_on_null_input(false),
	  m_fBoolReturnType(false),
	  m_fCommutative(false)
{
	GPOS_ASSERT(mdid_op->IsValid());

	CMDAccessor *md_accessor = COptCtxt::PoctxtFromTLS()->Pmda();

	m_returns_null_on_null_input =
		CMDAccessorUtils::FScalarOpReturnsNullOnNullInput(md_accessor,
														  m_mdid_op);
	m_fCommutative =
		CMDAccessorUtils::FCommutativeScalarOp(md_accessor, m_mdid_op);
	m_fBoolReturnType =
		CMDAccessorUtils::FBoolType(md_accessor, m_return_type_mdid);
}


//---------------------------------------------------------------------------
//	@function:
//		CScalarOp::GetMDName
//
//	@doc:
//		Operator name
//
//---------------------------------------------------------------------------
const CWStringConst *
CScalarOp::Pstr() const
{
	return m_pstrOp;
}

//---------------------------------------------------------------------------
//	@function:
//		CScalarOp::MdIdOp
//
//	@doc:
//		Scalar operator metadata id
//
//---------------------------------------------------------------------------
IMDId *
CScalarOp::MdIdOp() const
{
	return m_mdid_op;
}

//---------------------------------------------------------------------------
//	@function:
//		CScalarOp::HashValue
//
//	@doc:
//		Operator specific hash function; combined hash of operator id and
//		metadata id
//
//---------------------------------------------------------------------------
ULONG
CScalarOp::HashValue() const
{
	return gpos::CombineHashes(COperator::HashValue(), m_mdid_op->HashValue());
}


//---------------------------------------------------------------------------
//	@function:
//		CScalarOp::Matches
//
//	@doc:
//		Match function on operator level
//
//---------------------------------------------------------------------------
BOOL
CScalarOp::Matches(COperator *pop) const
{
	if (pop->Eopid() == Eopid())
	{
		CScalarOp *pscop = CScalarOp::PopConvert(pop);

		// match if operator oid are identical
		return m_mdid_op->Equals(pscop->MdIdOp());
	}

	return false;
}

//---------------------------------------------------------------------------
//	@function:
//		CScalarOp::GetReturnTypeMdId
//
//	@doc:
//		Accessor to the return type
//
//---------------------------------------------------------------------------
IMDId *
CScalarOp::GetReturnTypeMdId() const
{
	return m_return_type_mdid;
}

//---------------------------------------------------------------------------
//	@function:
//		CScalarOp::MdidType
//
//	@doc:
//		Expression type
//
//---------------------------------------------------------------------------
IMDId *
CScalarOp::MdidType() const
{
	if (nullptr != m_return_type_mdid)
	{
		return m_return_type_mdid;
	}

	CMDAccessor *md_accessor = COptCtxt::PoctxtFromTLS()->Pmda();
	return md_accessor->RetrieveScOp(m_mdid_op)->GetResultTypeMdid();
}

//---------------------------------------------------------------------------
//	@function:
//		CScalarOp::FInputOrderSensitive
//
//	@doc:
//		Sensitivity to order of inputs
//
//---------------------------------------------------------------------------
BOOL
CScalarOp::FInputOrderSensitive() const
{
	return !m_fCommutative;
}


//---------------------------------------------------------------------------
//	@function:
//		CScalarOp::OsPrint
//
//	@doc:
//		debug print
//
//---------------------------------------------------------------------------
IOstream &
CScalarOp::OsPrint(IOstream &os) const
{
	os << SzId() << " (";
	os << Pstr()->GetBuffer();
	os << ")";

	return os;
}

//---------------------------------------------------------------------------
//	@function:
//		CScalarOp::Eber
//
//	@doc:
//		Perform boolean expression evaluation
//
//---------------------------------------------------------------------------
CScalar::EBoolEvalResult
CScalarOp::Eber(ULongPtrArray *pdrgpulChildren) const
{
	if (m_returns_null_on_null_input)
	{
		return EberNullOnAnyNullChild(pdrgpulChildren);
	}

	return EberAny;
}

// EOF
