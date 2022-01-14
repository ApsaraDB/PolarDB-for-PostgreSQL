//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//	Copyright (C) 2021, Alibaba Group Holding Limited
//
//	@filename:
//		CConstraintInterval.h
//
//	@doc:
//		Representation of an interval constraint. An interval contains a number
//		of ranges + "is null" and "is not null" flags. The interval can be interpreted
//		as the ORing of the ranges and the flags that are set
//---------------------------------------------------------------------------
#ifndef GPOPT_CConstraintInterval_H
#define GPOPT_CConstraintInterval_H

#include "gpos/base.h"
#include "gpos/error/CAutoTrace.h"
#include "gpos/task/CAutoTraceFlag.h"

#include "gpopt/base/CConstraint.h"
#include "gpopt/base/CRange.h"
#include "gpopt/operators/CScalarArrayCmp.h"
#include "gpopt/operators/CScalarConst.h"
#include "naucrates/dxl/xml/dxltokens.h"
#include "naucrates/traceflags/traceflags.h"

namespace gpopt
{
// range array
typedef CDynamicPtrArray<CRange, CleanupRelease> CRangeArray;

using namespace gpos;
using namespace gpmd;

//---------------------------------------------------------------------------
//	@class:
//		CConstraintInterval
//
//	@doc:
//		Representation of an interval constraint
//
//		If x has a CConstraintInterval C on it, this means that x is in the
//		ranges contained in C.
//
//---------------------------------------------------------------------------
class CConstraintInterval : public CConstraint
{
private:
	// column referenced in this constraint
	const CColRef *m_pcr;

	// array of ranges
	CRangeArray *m_pdrgprng;

	// does the interval include the null value
	BOOL m_fIncludesNull;

	/* POLAR px: used for hash partition by*/
	CExpression *m_pexprHash;

	// adds ranges from a source array to a destination array, starting
	// at the range with the given index
	static void AddRemainingRanges(CMemoryPool *mp, CRangeArray *pdrgprngSrc,
								   ULONG ulStart, CRangeArray *pdrgprngDest);

	// append the given range to the array or extend the last element
	static void AppendOrExtend(CMemoryPool *mp, CRangeArray *pdrgprng,
							   CRange *prange);

	// difference between two ranges on the left side only -
	// any difference on the right side is reported as residual range
	static CRange *PrangeDiffWithRightResidual(CMemoryPool *mp,
											   CRange *prangeFirst,
											   CRange *prangeSecond,
											   CRange **pprangeResidual,
											   CRangeArray *pdrgprngResidual);

	// type of this interval
	IMDId *MdidType();

	// construct scalar expression
	virtual CExpression *PexprConstructScalar(CMemoryPool *mp) const;

	virtual CExpression *PexprConstructArrayScalar(CMemoryPool *mp) const;

	// create interval from scalar comparison expression
	static CConstraintInterval *PciIntervalFromScalarCmp(
		CMemoryPool *mp, CExpression *pexpr, CColRef *colref,
		BOOL infer_nulls_as = false);

	static CConstraintInterval *PciIntervalFromScalarIDF(CMemoryPool *mp,
														 CExpression *pexpr,
														 CColRef *colref);

	// create interval from scalar bool operator
	static CConstraintInterval *PciIntervalFromScalarBoolOp(
		CMemoryPool *mp, CExpression *pexpr, CColRef *colref,
		BOOL infer_nulls_as = false);

	// create interval from scalar bool AND
	static CConstraintInterval *PciIntervalFromScalarBoolAnd(
		CMemoryPool *mp, CExpression *pexpr, CColRef *colref,
		BOOL infer_nulls_as = false);

	// create interval from scalar bool OR
	static CConstraintInterval *PciIntervalFromScalarBoolOr(
		CMemoryPool *mp, CExpression *pexpr, CColRef *colref,
		BOOL infer_nulls_as = false);

	// create interval from scalar null test
	static CConstraintInterval *PciIntervalFromScalarNullTest(
		CMemoryPool *mp, CExpression *pexpr, CColRef *colref);

	// creates a range like [x,x] where x is a constant
	static CRangeArray *PciRangeFromColConstCmp(CMemoryPool *mp,
												IMDType::ECmpType cmp_type,
												const CScalarConst *popScConst);

	// create an array IN or NOT IN expression
	CExpression *PexprConstructArrayScalar(CMemoryPool *mp, bool isIn) const;

	/* POLAR px: used for hash partition func */
	static CConstraintInterval *PciIntervalFromScalarFunc(CMemoryPool *mp,
														 CExpression *pexpr,
														 CColRef *colref);
public:
	CConstraintInterval(const CConstraintInterval &) = delete;

	// ctor
	CConstraintInterval(CMemoryPool *mp, const CColRef *colref,
						CRangeArray *pdrgprng, BOOL is_null);

	/* POLAR px: used for hash partition func */
	CConstraintInterval(CMemoryPool *mp, const CColRef *colref,
						CExpression *pexprHash);

	// dtor
	~CConstraintInterval() override;

	// constraint type accessor
	EConstraintType
	Ect() const override
	{
		return CConstraint::EctInterval;
	}

	// column referenced in constraint
	const CColRef *
	Pcr() const
	{
		return m_pcr;
	}

	// all ranges in interval
	CRangeArray *
	Pdrgprng() const
	{
		return m_pdrgprng;
	}

	// does the interval include the null value
	BOOL
	FIncludesNull() const
	{
		return m_fIncludesNull;
	}

	// is this constraint a contradiction
	BOOL FContradiction() const override;

	// is this interval unbounded
	BOOL IsConstraintUnbounded() const override;

	// check if there is a constraint on the given column
	BOOL
	FConstraint(const CColRef *colref) const override
	{
		return m_pcr == colref;
	}

	// check if constraint is on the gp_segment_id column
	BOOL
	FConstraintOnSegmentId() const override
	{
		return m_pcr->IsSystemCol() &&
			   m_pcr->Name().Equals(
				   CDXLTokens::GetDXLTokenStr(EdxltokenGpSegmentIdColName));
	}

	// return a copy of the constraint with remapped columns
	CConstraint *PcnstrCopyWithRemappedColumns(CMemoryPool *mp,
											   UlongToColRefMap *colref_mapping,
											   BOOL must_exist) override;

	// interval intersection
	CConstraintInterval *PciIntersect(CMemoryPool *mp,
									  CConstraintInterval *pci);

	// interval union
	CConstraintInterval *PciUnion(CMemoryPool *mp, CConstraintInterval *pci);

	// interval difference
	CConstraintInterval *PciDifference(CMemoryPool *mp,
									   CConstraintInterval *pci);

	// interval complement
	CConstraintInterval *PciComplement(CMemoryPool *mp);

	// does the current interval contain the given interval?
	BOOL FContainsInterval(CMemoryPool *mp, CConstraintInterval *pci);

	// scalar expression
	CExpression *PexprScalar(CMemoryPool *mp) override;

	// scalar expression  which will be a disjunction
	CExpression *PexprConstructDisjunctionScalar(CMemoryPool *mp) const;

	// return constraint on a given column
	CConstraint *Pcnstr(CMemoryPool *mp, const CColRef *colref) override;

	// return constraint on a given column set
	CConstraint *Pcnstr(CMemoryPool *mp, CColRefSet *pcrs) override;

	// return a clone of the constraint for a different column
	CConstraint *PcnstrRemapForColumn(CMemoryPool *mp,
									  CColRef *colref) const override;

	// converts to an array in expression
	bool FConvertsToNotIn() const;

	// converts to an array not in expression
	bool FConvertsToIn() const;

	// print
	IOstream &OsPrint(IOstream &os) const override;

	// create unbounded interval
	static CConstraintInterval *PciUnbounded(CMemoryPool *mp,
											 const CColRef *colref,
											 BOOL fIncludesNull);

	// create an unbounded interval on any column from the given set
	static CConstraintInterval *PciUnbounded(CMemoryPool *mp,
											 const CColRefSet *pcrs,
											 BOOL fIncludesNull);

	// helper for create interval from comparison between a column and a constant
	static CConstraintInterval *PciIntervalFromColConstCmp(
		CMemoryPool *mp, CColRef *colref, IMDType::ECmpType cmp_type,
		CScalarConst *popScConst, BOOL infer_nulls_as = false);

	// create interval from scalar expression
	static CConstraintInterval *PciIntervalFromScalarExpr(
		CMemoryPool *mp, CExpression *pexpr, CColRef *colref,
		BOOL infer_nulls_as = false);

	// create interval from any general constraint that references
	// only one column
	static CConstraintInterval *PciIntervalFromConstraint(
		CMemoryPool *mp, CConstraint *pcnstr, CColRef *colref = nullptr);

	// generate a ConstraintInterval from the given expression
	static CConstraintInterval *PcnstrIntervalFromScalarArrayCmp(
		CMemoryPool *mp, CExpression *pexpr, CColRef *colref,
		BOOL infer_nulls_as = false);

	/* POLAR px: used for hash partition func */
	CExpression *
	PExprHash()
	{
		return m_pexprHash;
	}

	/* POLAR px: used for hash partition func */
	BOOL
	FUsedForExprHash() const
	{
		return m_pexprHash != NULL;
	}

	/* POLAR px: hash interval intersection */
	CConstraintInterval *PciIntersectWithHash(CMemoryPool *mp,
											  CConstraintInterval *pci);
	/* POLAR px: check contain points */
	bool FContainPointsOnly() const;

	/* POLAR px: create an unbounded interval on any column from the given set */
	static CRangeArray *PciUnbounded(CMemoryPool *mp, const CColRef *colref);

};	// class CConstraintInterval

// shorthand for printing, reference
inline IOstream &
operator<<(IOstream &os, const CConstraintInterval &interval)
{
	return interval.OsPrint(os);
}

// shorthand for printing, pointer
inline IOstream &
operator<<(IOstream &os, const CConstraintInterval *interval)
{
	return interval->OsPrint(os);
}

typedef CDynamicPtrArray<CConstraintInterval, CleanupRelease>
	CConstraintIntervalArray;
}  // namespace gpopt

#endif	// !GPOPT_CConstraintInterval_H

// EOF
