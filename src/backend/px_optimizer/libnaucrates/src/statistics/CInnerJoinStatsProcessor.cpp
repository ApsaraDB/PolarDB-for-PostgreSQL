//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright 2018 VMware, Inc. or its affiliates.
//
//	@filename:
//		CInnerJoinStatsProcessor.cpp
//
//	@doc:
//		Statistics helper routines for processing Inner Joins
//---------------------------------------------------------------------------

#include "naucrates/statistics/CInnerJoinStatsProcessor.h"

using namespace gpmd;

// return statistics object after performing inner join
CStatistics *
CInnerJoinStatsProcessor::CalcInnerJoinStatsStatic(
	CMemoryPool *mp, const IStatistics *outer_stats_input,
	const IStatistics *inner_stats_input, CStatsPredJoinArray *join_preds_stats)
{
	GPOS_ASSERT(nullptr != outer_stats_input);
	GPOS_ASSERT(nullptr != inner_stats_input);
	GPOS_ASSERT(nullptr != join_preds_stats);
	const CStatistics *outer_stats =
		dynamic_cast<const CStatistics *>(outer_stats_input);

	return CJoinStatsProcessor::SetResultingJoinStats(
		mp, outer_stats->GetStatsConfig(), outer_stats_input, inner_stats_input,
		join_preds_stats, IStatistics::EsjtInnerJoin,
		true /* DoIgnoreLASJHistComputation */
	);
}

// EOF
