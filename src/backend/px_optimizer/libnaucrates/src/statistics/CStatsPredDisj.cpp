//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 EMC Corp.
//
//	@filename:
//		CStatsPredDisj.cpp
//
//	@doc:
//		Implementation of statistics Disjunctive filter
//---------------------------------------------------------------------------

#include "naucrates/statistics/CStatsPredDisj.h"

#include "naucrates/statistics/CStatisticsUtils.h"

using namespace gpnaucrates;
using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CStatsPrefDisj::CStatsPrefDisj
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CStatsPredDisj::CStatsPredDisj(CStatsPredPtrArry *disj_pred_stats_array)
	: CStatsPred(gpos::ulong_max),
	  m_disj_pred_stats_array(disj_pred_stats_array)
{
	GPOS_ASSERT(nullptr != disj_pred_stats_array);
	m_colid = CStatisticsUtils::GetColId(disj_pred_stats_array);
}

//---------------------------------------------------------------------------
//	@function:
//		CStatsPrefDisj::GetPredStats
//
//	@doc:
//		Return the point filter at a particular position
//
//---------------------------------------------------------------------------
CStatsPred *
CStatsPredDisj::GetPredStats(ULONG pos) const
{
	return (*m_disj_pred_stats_array)[pos];
}

//---------------------------------------------------------------------------
//	@function:
//		CStatsPrefDisj::Sort
//
//	@doc:
//		Sort the components of the disjunction
//
//---------------------------------------------------------------------------
void
CStatsPredDisj::Sort() const
{
	if (1 < GetNumPreds())
	{
		// sort the filters on column ids
		m_disj_pred_stats_array->Sort(CStatsPred::StatsPredSortCmpFunc);
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CStatsPrefDisj::GetColId
//
//	@doc:
//		Return the column identifier on which the predicates are on
//
//---------------------------------------------------------------------------
ULONG
CStatsPredDisj::GetColId() const
{
	return m_colid;
}

// EOF
