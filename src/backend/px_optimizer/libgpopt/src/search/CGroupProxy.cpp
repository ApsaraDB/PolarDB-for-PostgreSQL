//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2008 - 2011 EMC Corp.
//
//	@filename:
//		CGroupProxy.cpp
//
//	@doc:
//		Implementation of proxy object for group access
//---------------------------------------------------------------------------

#include "gpopt/search/CGroupProxy.h"

#include "gpos/base.h"
#include "gpos/common/CSyncHashtableAccessByKey.h"

#include "gpopt/base/CDrvdPropRelational.h"
#include "gpopt/base/COptimizationContext.h"
#include "gpopt/search/CGroup.h"
#include "gpopt/search/CGroupExpression.h"
#include "gpopt/search/CJobGroup.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CGroupProxy::CGroupProxy
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CGroupProxy::CGroupProxy(CGroup *pgroup) : m_pgroup(pgroup)
{
	GPOS_ASSERT(nullptr != pgroup);
}


//---------------------------------------------------------------------------
//	@function:
//		CGroupProxy::~CGroupProxy
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CGroupProxy::~CGroupProxy() = default;


//---------------------------------------------------------------------------
//	@function:
//		CGroupProxy::Insert
//
//	@doc:
//		Insert group expression into group
//
//---------------------------------------------------------------------------
void
CGroupProxy::Insert(CGroupExpression *pgexpr)
{
	pgexpr->Init(m_pgroup, m_pgroup->m_ulGExprs++);

	GPOS_ASSERT(pgexpr->Pgroup() == m_pgroup);

	m_pgroup->Insert(pgexpr);
}


//---------------------------------------------------------------------------
//	@function:
//		CGroupProxy::MoveDuplicateGExpr
//
//	@doc:
//		Move duplicate group expression to duplicates list
//
//---------------------------------------------------------------------------
void
CGroupProxy::MoveDuplicateGExpr(CGroupExpression *pgexpr)
{
	GPOS_ASSERT(pgexpr->Pgroup() == m_pgroup);

#ifdef GPOS_DEBUG
	ULONG ulGExprsOld =
		m_pgroup->m_listGExprs.Size() + m_pgroup->m_listDupGExprs.Size();
#endif	// GPOS_DEBUG

	m_pgroup->MoveDuplicateGExpr(pgexpr);
	GPOS_ASSERT(ulGExprsOld == (m_pgroup->m_listGExprs.Size() +
								m_pgroup->m_listDupGExprs.Size()));
}

//---------------------------------------------------------------------------
//	@function:
//		CGroupProxy::InitProperties
//
//	@doc:
//		Initialize group's properties
//
//---------------------------------------------------------------------------
void
CGroupProxy::InitProperties(CDrvdProp *pdp)
{
	m_pgroup->InitProperties(pdp);
}


//---------------------------------------------------------------------------
//	@function:
//		CGroupProxy::InitStats
//
//	@doc:
//		Initialize group's stats
//
//---------------------------------------------------------------------------
void
CGroupProxy::InitStats(IStatistics *stats)
{
	m_pgroup->InitStats(stats);
}


//---------------------------------------------------------------------------
//	@function:
//		CGroupProxy::PgexprNext
//
//	@doc:
//		Retrieve next group expression;
//
//---------------------------------------------------------------------------
CGroupExpression *
CGroupProxy::PgexprNext(CGroupExpression *pgexpr)
{
	GPOS_ASSERT(nullptr != pgexpr);
	return m_pgroup->PgexprNext(pgexpr);
}


//---------------------------------------------------------------------------
//	@function:
//		CGroupProxy::PgexprFirst
//
//	@doc:
//		Retrieve first group expression;
//
//---------------------------------------------------------------------------
CGroupExpression *
CGroupProxy::PgexprFirst()
{
	return m_pgroup->PgexprFirst();
}


//---------------------------------------------------------------------------
//	@function:
//		CGroupProxy::PgexprSkip
//
//	@doc:
//		Skip group expressions starting from the given expression;
//		the type of group expressions to skip is determined by the passed
//		flag
//
//---------------------------------------------------------------------------
CGroupExpression *
CGroupProxy::PgexprSkip(CGroupExpression *pgexprStart, BOOL fSkipLogical)
{
	CGroupExpression *pgexpr = pgexprStart;
	while (nullptr != pgexpr && fSkipLogical == pgexpr->Pop()->FLogical())
	{
		pgexpr = PgexprNext(pgexpr);
	}

	return pgexpr;
}


//---------------------------------------------------------------------------
//	@function:
//		CGroupProxy::PgexprSkipLogical
//
//	@doc:
//		Retrieve the first non-logical group expression following the given
//		expression;
//
//---------------------------------------------------------------------------
CGroupExpression *
CGroupProxy::PgexprSkipLogical(CGroupExpression *pgexpr)
{
	if (nullptr == pgexpr)
	{
		return PgexprSkip(PgexprFirst(), true /*fSkipLogical*/);
	}

	return PgexprSkip(PgexprNext(pgexpr), true /*fSkipLogical*/);
}



//---------------------------------------------------------------------------
//	@function:
//		CGroupProxy::PgexprNextLogical
//
//	@doc:
//		Find the first logical group expression after the given expression
//
//---------------------------------------------------------------------------
CGroupExpression *
CGroupProxy::PgexprNextLogical(CGroupExpression *pgexpr)
{
	GPOS_ASSERT(!m_pgroup->FScalar());

	if (nullptr == pgexpr)
	{
		return PgexprSkip(PgexprFirst(), false /*fSkipLogical*/);
	}

	return PgexprSkip(PgexprNext(pgexpr), false /*fSkipLogical*/);
}


// EOF
