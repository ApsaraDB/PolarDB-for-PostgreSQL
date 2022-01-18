//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 VMware, Inc. or its affiliates.
//
//	@filename:
//		CStatsPredUnsupported.h
//
//	@doc:
//		Class representing unsupported statistics filter
//---------------------------------------------------------------------------
#ifndef GPNAUCRATES_CStatsPredUnsupported_H
#define GPNAUCRATES_CStatsPredUnsupported_H

#include "gpos/base.h"
#include "gpos/common/CDouble.h"

#include "naucrates/statistics/CStatsPred.h"

namespace gpnaucrates
{
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CStatsPredUnsupported
//
//	@doc:
//		Class representing unsupported statistics filter
//---------------------------------------------------------------------------
class CStatsPredUnsupported : public CStatsPred
{
private:
	// predicate comparison type
	CStatsPred::EStatsCmpType m_stats_cmp_type;

	// scale factor of the predicate
	CDouble m_default_scale_factor;

	// initialize the scale factor of the predicate
	static CDouble InitScaleFactor();

public:
	CStatsPredUnsupported(const CStatsPredUnsupported &) = delete;

	// ctors
	CStatsPredUnsupported(ULONG colid,
						  CStatsPred::EStatsCmpType stats_pred_type);
	CStatsPredUnsupported(ULONG colid,
						  CStatsPred::EStatsCmpType stats_pred_type,
						  CDouble default_scale_factor);

	// filter type id
	CStatsPred::EStatsPredType
	GetPredStatsType() const override
	{
		return CStatsPred::EsptUnsupported;
	}

	// comparison types for stats computation
	virtual CStatsPred::EStatsCmpType
	GetStatsCmpType() const
	{
		return m_stats_cmp_type;
	}

	CDouble
	ScaleFactor() const
	{
		return m_default_scale_factor;
	}

	// conversion function
	static CStatsPredUnsupported *
	ConvertPredStats(CStatsPred *pred_stats)
	{
		GPOS_ASSERT(nullptr != pred_stats);
		GPOS_ASSERT(CStatsPred::EsptUnsupported ==
					pred_stats->GetPredStatsType());

		return dynamic_cast<CStatsPredUnsupported *>(pred_stats);
	}

};	// class CStatsPredUnsupported
}  // namespace gpnaucrates

#endif	// !GPNAUCRATES_CStatsPredUnsupported_H

// EOF
