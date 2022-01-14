//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CConstraint.h
//
//	@doc:
//		Base class for representing constraints
//---------------------------------------------------------------------------
#ifndef GPOPT_CConstraint_H
#define GPOPT_CConstraint_H

#include "gpos/base.h"
#include "gpos/common/CDynamicPtrArray.h"
#include "gpos/common/CHashMap.h"
#include "gpos/common/CRefCount.h"
#include "gpos/types.h"

#include "gpopt/base/CColRef.h"
#include "gpopt/base/CColRefSet.h"

namespace gpopt
{
using namespace gpos;

// fwd declaration
class CExpression;
class CConstraint;

// constraint array
typedef CDynamicPtrArray<CConstraint, CleanupRelease> CConstraintArray;

// hash map mapping CColRef -> CConstraintArray
typedef CHashMap<CColRef, CConstraintArray, CColRef::HashValue, CColRef::Equals,
				 CleanupNULL<CColRef>, CleanupRelease<CConstraintArray> >
	ColRefToConstraintArrayMap;

// mapping CConstraint -> BOOL to cache previous containment queries,
// we use pointer equality here for fast map lookup -- since we do shallow comparison, we do not take ownership
// of pointer values
typedef CHashMap<CConstraint, BOOL, gpos::HashPtr<CConstraint>,
				 gpos::EqualPtr<CConstraint>, CleanupNULL<CConstraint>,
				 CleanupNULL<BOOL> >
	ConstraintContainmentMap;

// hash map mapping ULONG -> CConstraint
typedef CHashMap<ULONG, CConstraint, gpos::HashValue<ULONG>,
				 gpos::Equals<ULONG>, CleanupDelete<ULONG>,
				 CleanupRelease<CConstraint> >
	UlongToConstraintMap;

//---------------------------------------------------------------------------
//	@class:
//		CConstraint
//
//	@doc:
//		Base class for representing constraints
//
//---------------------------------------------------------------------------
class CConstraint : public CRefCount, public DbgPrintMixin<CConstraint>
{
public:
	enum EConstraintType
	{
		EctInterval,	 // a single interval on a single columns
		EctConjunction,	 // a set of ANDed constraints
		EctDisjunction,	 // a set of ORed constraints
		EctNegation		 // a negated constraint
	};

private:
	// containment map
	ConstraintContainmentMap *m_phmcontain;

	// constant true
	static BOOL m_fTrue;

	// constant false
	static BOOL m_fFalse;

	// return address of static BOOL constant based on passed BOOL value
	static BOOL *
	PfVal(BOOL value)
	{
		if (value)
		{
			return &m_fTrue;
		}

		return &m_fFalse;
	}

	// add column as a new equivalence class, if it is not already in one of the
	// existing equivalence classes
	static void AddColumnToEquivClasses(CMemoryPool *mp, const CColRef *colref,
										CColRefSetArray *pdrgpcrs);

	// create constraint from scalar comparison
	static CConstraint *PcnstrFromScalarCmp(CMemoryPool *mp, CExpression *pexpr,
											CColRefSetArray **ppdrgpcrs,
											BOOL infer_nulls_as = false);

	// create constraint from scalar boolean expression
	static CConstraint *PcnstrFromScalarBoolOp(CMemoryPool *mp,
											   CExpression *pexpr,
											   CColRefSetArray **ppdrgpcrs,
											   BOOL infer_nulls_as = false);

	// create conjunction/disjunction from array of constraints
	static CConstraint *PcnstrConjDisj(CMemoryPool *mp,
									   CConstraintArray *pdrgpcnstr,
									   BOOL fConj);

protected:
	// memory pool -- used for local computations
	CMemoryPool *m_mp;

	// columns used in this constraint
	CColRefSet *m_pcrsUsed;

	// equivalent scalar expression
	CExpression *m_pexprScalar;

	// print
	IOstream &PrintConjunctionDisjunction(IOstream &os,
										  CConstraintArray *pdrgpcnstr) const;

	// construct a conjunction or disjunction scalar expression from an
	// array of constraints
	static CExpression *PexprScalarConjDisj(CMemoryPool *mp,
											CConstraintArray *pdrgpcnstr,
											BOOL fConj);

	// flatten an array of constraints to be used as constraint children
	static CConstraintArray *PdrgpcnstrFlatten(CMemoryPool *mp,
											   CConstraintArray *pdrgpcnstr,
											   EConstraintType ect);

	// combine any two or more constraints that reference only one particular column
	static CConstraintArray *PdrgpcnstrDeduplicate(CMemoryPool *mp,
												   CConstraintArray *pdrgpcnstr,
												   EConstraintType ect);

	// mapping between columns and arrays of constraints
	static ColRefToConstraintArrayMap *Phmcolconstr(
		CMemoryPool *mp, CColRefSet *pcrs, CConstraintArray *pdrgpcnstr);

	// return a copy of the conjunction/disjunction constraint for a different column
	static CConstraint *PcnstrConjDisjRemapForColumn(
		CMemoryPool *mp, CColRef *colref, CConstraintArray *pdrgpcnstr,
		BOOL fConj);

	// create constraint from scalar array comparison expression originally generated for
	// "scalar op ANY/ALL (array)" construct
	static CConstraint *PcnstrFromScalarArrayCmp(CMemoryPool *mp,
												 CExpression *pexpr,
												 CColRef *colref,
												 BOOL infer_nulls_as = false);

	static CColRefSet *PcrsFromConstraints(CMemoryPool *mp,
										   CConstraintArray *pdrgpcnstr);

public:
	CConstraint(const CConstraint &) = delete;

	// ctor
	explicit CConstraint(CMemoryPool *mp, CColRefSet *pcrsUsed);

	// dtor
	~CConstraint() override;

	// constraint type accessor
	virtual EConstraintType Ect() const = 0;

	// is this constraint a contradiction
	virtual BOOL FContradiction() const = 0;

	// is this constraint unbounded
	virtual BOOL
	IsConstraintUnbounded() const
	{
		return false;
	}

	// does the current constraint contain the given one
	virtual BOOL Contains(CConstraint *pcnstr);

	// equality function
	virtual BOOL Equals(CConstraint *pcnstr);

	// columns in this constraint
	virtual CColRefSet *
	PcrsUsed() const
	{
		return m_pcrsUsed;
	}

	// scalar expression
	virtual CExpression *PexprScalar(CMemoryPool *mp) = 0;

	// check if there is a constraint on the given column
	virtual BOOL FConstraint(const CColRef *colref) const = 0;

	virtual BOOL
	FConstraintOnSegmentId() const
	{
		return false;
	}

	// return a copy of the constraint with remapped columns
	virtual CConstraint *PcnstrCopyWithRemappedColumns(
		CMemoryPool *mp, UlongToColRefMap *colref_mapping, BOOL must_exist) = 0;

	// return constraint on a given column
	virtual CConstraint *
	Pcnstr(CMemoryPool *,	//mp,
		   const CColRef *	//colref
	)
	{
		return nullptr;
	}

	// return constraint on a given set of columns
	virtual CConstraint *
	Pcnstr(CMemoryPool *,  //mp,
		   CColRefSet *	   //pcrs
	)
	{
		return nullptr;
	}

	// return a clone of the constraint for a different column
	virtual CConstraint *PcnstrRemapForColumn(CMemoryPool *mp,
											  CColRef *colref) const = 0;

	// create constraint from scalar expression and pass back any discovered
	// equivalence classes
	static CConstraint *PcnstrFromScalarExpr(CMemoryPool *mp,
											 CExpression *pexpr,
											 CColRefSetArray **ppdrgpcrs,
											 BOOL infer_nulls_as = false);

	// create conjunction from array of constraints
	static CConstraint *PcnstrConjunction(CMemoryPool *mp,
										  CConstraintArray *pdrgpcnstr);

	// create disjunction from array of constraints
	static CConstraint *PcnstrDisjunction(CMemoryPool *mp,
										  CConstraintArray *pdrgpcnstr);

	// merge equivalence classes coming from children of a bool op
	static CColRefSetArray *PdrgpcrsMergeFromBoolOp(
		CMemoryPool *mp, CExpression *pexpr, CColRefSetArray *pdrgpcrsFst,
		CColRefSetArray *pdrgpcrsSnd);

	// subset of the given constraints, which reference the given column
	static CConstraintArray *PdrgpcnstrOnColumn(CMemoryPool *mp,
												CConstraintArray *pdrgpcnstr,
												CColRef *colref,
												BOOL fExclusive);
	virtual gpos::IOstream &OsPrint(gpos::IOstream &os) const = 0;

};	// class CConstraint

// shorthand for printing, pointer.
inline IOstream &
operator<<(IOstream &os, const CConstraint *cnstr)
{
	return cnstr->OsPrint(os);
}
// shorthand for printing
inline IOstream &
operator<<(IOstream &os, const CConstraint &cnstr)
{
	return cnstr.OsPrint(os);
}
}  // namespace gpopt

#endif	// !GPOPT_CConstraint_H

// EOF
