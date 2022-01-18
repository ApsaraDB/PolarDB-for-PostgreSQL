//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CSearchStage.h
//
//	@doc:
//		Search stage
//---------------------------------------------------------------------------
#ifndef GPOPT_CSearchStage_H
#define GPOPT_CSearchStage_H

#include "gpos/base.h"
#include "gpos/common/CDynamicPtrArray.h"
#include "gpos/common/CTimerUser.h"

#include "gpopt/xforms/CXform.h"


namespace gpopt
{
using namespace gpos;

// forward declarations
class CSearchStage;
class CExpression;

// definition of array of search stages
typedef CDynamicPtrArray<CSearchStage, CleanupDelete> CSearchStageArray;


//---------------------------------------------------------------------------
//	@class:
//		CSearchStage
//
//	@doc:
//		Search stage
//
//---------------------------------------------------------------------------
class CSearchStage : public DbgPrintMixin<CSearchStage>
{
private:
	// set of xforms to be applied during stage
	CXformSet *m_xforms;

	// time threshold in milliseconds
	ULONG m_time_threshold;

	// cost threshold
	CCost m_cost_threshold;

	// best plan found at the end of search stage
	CExpression *m_pexprBest;

	// cost of best plan found
	CCost m_costBest;

	// elapsed time
	CTimerUser m_timer;

public:
	// ctor
	CSearchStage(CXformSet *xform_set, ULONG ulTimeThreshold = gpos::ulong_max,
				 CCost costThreshold = CCost(0.0));

	// dtor
	virtual ~CSearchStage();

	// restart timer if time threshold is not default indicating don't timeout
	// Restart() is a costly method, so avoid calling unnecessarily
	void
	RestartTimer()
	{
		if (m_time_threshold != gpos::ulong_max)
			m_timer.Restart();
	}

	// is search stage timed-out?
	// if threshold is gpos::ulong_max, its the default and we need not time out
	// ElapsedMS() is a costly method, so avoid calling unnecesarily
	BOOL
	FTimedOut() const
	{
		if (m_time_threshold == gpos::ulong_max)
			return false;
		return m_timer.ElapsedMS() > m_time_threshold;
	}

	// return elapsed time (in millseconds) since timer was last restarted
	ULONG
	UlElapsedTime() const
	{
		return m_timer.ElapsedMS();
	}

	BOOL
	FAchievedReqdCost() const
	{
		return (nullptr != m_pexprBest && m_costBest <= m_cost_threshold);
	}

	// xforms set accessor
	CXformSet *
	GetXformSet() const
	{
		return m_xforms;
	}

	// time threshold accessor
	ULONG
	TimeThreshold() const
	{
		return m_time_threshold;
	}

	// cost threshold accessor
	CCost
	CostThreshold() const
	{
		return m_cost_threshold;
	}

	// set best plan found at the end of search stage
	void SetBestExpr(CExpression *pexpr);

	// best plan found accessor
	CExpression *
	PexprBest() const
	{
		return m_pexprBest;
	}

	// best plan cost accessor
	CCost
	CostBest() const
	{
		return m_costBest;
	}

	// print function
	IOstream &OsPrint(IOstream &) const;

	// generate default search strategy
	static CSearchStageArray *PdrgpssDefault(CMemoryPool *mp);
};

// shorthand for printing
inline IOstream &
operator<<(IOstream &os, CSearchStage &ss)
{
	return ss.OsPrint(os);
}

}  // namespace gpopt

#endif	// !GPOPT_CSearchStage_H


// EOF
