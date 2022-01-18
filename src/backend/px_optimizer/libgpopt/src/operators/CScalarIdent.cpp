//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2009 Greenplum, Inc.
//
//	@filename:
//		CScalarIdent.cpp
//
//	@doc:
//		Implementation of scalar identity operator
//---------------------------------------------------------------------------

#include "gpopt/operators/CScalarIdent.h"

#include "gpos/base.h"

#include "gpopt/base/CColRefSet.h"
#include "gpopt/base/CColRefTable.h"
#include "gpopt/base/COptCtxt.h"
#include "gpopt/operators/CScalarFunc.h"


using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CScalarIdent::HashValue
//
//	@doc:
//		Hash value built from colref and Eop
//
//---------------------------------------------------------------------------
ULONG
CScalarIdent::HashValue() const
{
	return gpos::CombineHashes(COperator::HashValue(),
							   gpos::HashPtr<CColRef>(m_pcr));
}


//---------------------------------------------------------------------------
//	@function:
//		CScalarIdent::Matches
//
//	@doc:
//		Match function on operator level
//
//---------------------------------------------------------------------------
BOOL
CScalarIdent::Matches(COperator *pop) const
{
	if (pop->Eopid() == Eopid())
	{
		CScalarIdent *popIdent = CScalarIdent::PopConvert(pop);

		// match if column reference is same
		return Pcr() == popIdent->Pcr();
	}

	return false;
}


//---------------------------------------------------------------------------
//	@function:
//		CScalarIdent::FInputOrderSensitive
//
//	@doc:
//		Not called for leaf operators
//
//---------------------------------------------------------------------------
BOOL
CScalarIdent::FInputOrderSensitive() const
{
	GPOS_ASSERT(!"Unexpected call of function FInputOrderSensitive");
	return false;
}

//---------------------------------------------------------------------------
//	@function:
//		CScalarIdent::PopCopyWithRemappedColumns
//
//	@doc:
//		Return a copy of the operator with remapped columns
//
//---------------------------------------------------------------------------
COperator *
CScalarIdent::PopCopyWithRemappedColumns(CMemoryPool *mp,
										 UlongToColRefMap *colref_mapping,
										 BOOL must_exist)
{
	ULONG id = m_pcr->Id();
	CColRef *colref = colref_mapping->Find(&id);
	if (nullptr == colref)
	{
		if (must_exist)
		{
			// not found in hashmap, so create a new colref and add to hashmap
			CColumnFactory *col_factory = COptCtxt::PoctxtFromTLS()->Pcf();

			colref = col_factory->PcrCopy(m_pcr);

			BOOL result GPOS_ASSERTS_ONLY =
				colref_mapping->Insert(GPOS_NEW(mp) ULONG(id), colref);
			GPOS_ASSERT(result);
		}
		else
		{
			colref = const_cast<CColRef *>(m_pcr);
		}
	}

	return GPOS_NEW(mp) CScalarIdent(mp, colref);
}

//---------------------------------------------------------------------------
//	@function:
//		CScalarIdent::MdidType
//
//	@doc:
//		Expression type
//
//---------------------------------------------------------------------------
IMDId *
CScalarIdent::MdidType() const
{
	return m_pcr->RetrieveType()->MDId();
}

INT
CScalarIdent::TypeModifier() const
{
	return m_pcr->TypeModifier();
}

//---------------------------------------------------------------------------
//	@function:
//		CScalarIdent::FCastedScId
//
//	@doc:
// 		Is the given expression a scalar cast of a scalar identifier
//
//---------------------------------------------------------------------------
BOOL
CScalarIdent::FCastedScId(CExpression *pexpr)
{
	GPOS_ASSERT(nullptr != pexpr);

	// cast(col1)
	if (COperator::EopScalarCast == pexpr->Pop()->Eopid())
	{
		if (COperator::EopScalarIdent == (*pexpr)[0]->Pop()->Eopid())
		{
			return true;
		}
	}

	return false;
}

BOOL
CScalarIdent::FCastedScId(CExpression *pexpr, CColRef *colref)
{
	GPOS_ASSERT(nullptr != pexpr);
	GPOS_ASSERT(nullptr != colref);

	if (!FCastedScId(pexpr))
	{
		return false;
	}

	CScalarIdent *pScIdent = CScalarIdent::PopConvert((*pexpr)[0]->Pop());

	return colref == pScIdent->Pcr();
}

//---------------------------------------------------------------------------
//	@function:
//		CScalarIdent::FAllowedFuncScId
//
//	@doc:
// 		Is the given expression a scalar func (which is an increasing function
//		allowed for partition selection) of a scalar identifier
//
//---------------------------------------------------------------------------
BOOL
CScalarIdent::FAllowedFuncScId(CExpression *pexpr)
{
	GPOS_ASSERT(nullptr != pexpr);

	if (COperator::EopScalarFunc == pexpr->Pop()->Eopid() &&
		pexpr->Arity() > 0 &&
		COperator::EopScalarIdent == (*pexpr)[0]->Pop()->Eopid())
	{
		CScalarFunc *func = CScalarFunc::PopConvert(pexpr->Pop());
		CMDIdGPDB *funcmdid = CMDIdGPDB::CastMdid(func->FuncMdId());
		CMDAccessor *md_accessor = COptCtxt::PoctxtFromTLS()->Pmda();
		return md_accessor->RetrieveFunc(funcmdid)->IsAllowedForPS();
	}
	return false;
}

BOOL
CScalarIdent::FAllowedFuncScId(CExpression *pexpr, CColRef *colref)
{
	GPOS_ASSERT(nullptr != pexpr);
	GPOS_ASSERT(nullptr != colref);

	if (!FAllowedFuncScId(pexpr))
	{
		return false;
	}

	CScalarIdent *pScIdent = CScalarIdent::PopConvert((*pexpr)[0]->Pop());

	return colref == pScIdent->Pcr();
}
//---------------------------------------------------------------------------
//	@function:
//		CScalarIdent::OsPrint
//
//	@doc:
//		debug print
//
//---------------------------------------------------------------------------
IOstream &
CScalarIdent::OsPrint(IOstream &os) const
{
	os << SzId() << " ";
	m_pcr->OsPrint(os);

	return os;
}

// EOF
