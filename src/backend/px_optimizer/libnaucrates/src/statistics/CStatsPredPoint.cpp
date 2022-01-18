//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 EMC Corp.
//
//	@filename:
//		CStatsPredPoint.cpp
//
//	@doc:
//		Implementation of statistics filter that compares a column to a constant
//---------------------------------------------------------------------------

#include "naucrates/statistics/CStatsPredPoint.h"

#include "gpopt/base/CColRef.h"
#include "gpopt/base/CColRefTable.h"
#include "naucrates/md/CMDIdGPDB.h"

using namespace gpnaucrates;
using namespace gpopt;
using namespace gpmd;

//---------------------------------------------------------------------------
//	@function:
//		CStatsPredPoint::CStatisticsFilterPoint
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CStatsPredPoint::CStatsPredPoint(ULONG colid,
								 CStatsPred::EStatsCmpType stats_cmp_type,
								 CPoint *point)
	: CStatsPred(colid), m_stats_cmp_type(stats_cmp_type), m_pred_point(point)
{
	GPOS_ASSERT(nullptr != point);
}

//---------------------------------------------------------------------------
//	@function:
//		CStatsPredPoint::CStatisticsFilterPoint
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CStatsPredPoint::CStatsPredPoint(CMemoryPool *mp, const CColRef *colref,
								 CStatsPred::EStatsCmpType stats_cmp_type,
								 IDatum *datum)
	: CStatsPred(gpos::ulong_max),
	  m_stats_cmp_type(stats_cmp_type),
	  m_pred_point(nullptr)
{
	GPOS_ASSERT(nullptr != colref);
	GPOS_ASSERT(nullptr != datum);

	m_colid = colref->Id();
	IDatum *padded_datum = PreprocessDatum(mp, colref, datum);

	m_pred_point = GPOS_NEW(mp) CPoint(padded_datum);
}

//---------------------------------------------------------------------------
//		CStatsPredPoint::PreprocessDatum
//
//	@doc:
//		Add padding to datums when needed
//---------------------------------------------------------------------------
IDatum *
CStatsPredPoint::PreprocessDatum(CMemoryPool *mp, const CColRef *colref,
								 IDatum *datum)
{
	GPOS_ASSERT(nullptr != colref);
	GPOS_ASSERT(nullptr != datum);

	if (!datum->NeedsPadding() || CColRef::EcrtTable != colref->Ecrt() ||
		datum->IsNull())
	{
		// we do not pad datum for comparison against computed columns
		datum->AddRef();
		return datum;
	}

	const CColRefTable *colref_table =
		CColRefTable::PcrConvert(const_cast<CColRef *>(colref));

	return datum->MakePaddedDatum(mp, colref_table->Width());
}

// EOF
