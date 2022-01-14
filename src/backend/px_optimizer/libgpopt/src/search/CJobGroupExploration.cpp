//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CJobGroupExploration.cpp
//
//	@doc:
//		Implementation of group exploration job
//---------------------------------------------------------------------------

#include "gpopt/search/CJobGroupExploration.h"

#include "gpopt/engine/CEngine.h"
#include "gpopt/search/CGroup.h"
#include "gpopt/search/CGroupExpression.h"
#include "gpopt/search/CGroupProxy.h"
#include "gpopt/search/CJobFactory.h"
#include "gpopt/search/CJobGroupExpressionExploration.h"
#include "gpopt/search/CJobQueue.h"
#include "gpopt/search/CScheduler.h"
#include "gpopt/search/CSchedulerContext.h"

using namespace gpopt;

// State transition diagram for group exploration job state machine;
//
// +------------------------+
// |    estInitialized:     |
// | EevtStartExploration() |
// +------------------------+
//   |
//   | eevStartedExploration
//   v
// +------------------------+   eevNewChildren
// | estExploringChildren:  | -----------------+
// | EevtExploreChildren()  |                  |
// |                        | <----------------+
// +------------------------+
//   |
//   | eevExplored
//   v
// +------------------------+
// |      estCompleted      |
// +------------------------+
//
const CJobGroupExploration::EEvent
	rgeev[CJobGroupExploration::estSentinel]
		 [CJobGroupExploration::estSentinel] = {
			 {// estInitialized
			  CJobGroupExploration::eevSentinel,
			  CJobGroupExploration::eevStartedExploration,
			  CJobGroupExploration::eevSentinel},
			 {// estExploringChildren
			  CJobGroupExploration::eevSentinel,
			  CJobGroupExploration::eevNewChildren,
			  CJobGroupExploration::eevExplored},
			 {// estCompleted
			  CJobGroupExploration::eevSentinel,
			  CJobGroupExploration::eevSentinel,
			  CJobGroupExploration::eevSentinel},
};

#ifdef GPOS_DEBUG

// names for states
const WCHAR
	rgwszStates[CJobGroupExploration::estSentinel][GPOPT_FSM_NAME_LENGTH] = {
		GPOS_WSZ_LIT("initialized"), GPOS_WSZ_LIT("children explored"),
		GPOS_WSZ_LIT("completed")};

// names for events
const WCHAR
	rgwszEvents[CJobGroupExploration::eevSentinel][GPOPT_FSM_NAME_LENGTH] = {
		GPOS_WSZ_LIT("started exploration"), GPOS_WSZ_LIT("exploring children"),
		GPOS_WSZ_LIT("finalized")};

#endif	// GPOS_DEBUG


//---------------------------------------------------------------------------
//	@function:
//		CJobGroupExploration::CJobGroupExploration
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CJobGroupExploration::CJobGroupExploration() = default;


//---------------------------------------------------------------------------
//	@function:
//		CJobGroupExploration::~CJobGroupExploration
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CJobGroupExploration::~CJobGroupExploration() = default;


//---------------------------------------------------------------------------
//	@function:
//		CJobGroupExploration::Init
//
//	@doc:
//		Initialize job
//
//---------------------------------------------------------------------------
void
CJobGroupExploration::Init(CGroup *pgroup)
{
	CJobGroup::Init(pgroup);

	m_jsm.Init(rgeev
#ifdef GPOS_DEBUG
			   ,
			   rgwszStates, rgwszEvents
#endif	// GPOS_DEBUG
	);

	// set job actions
	m_jsm.SetAction(estInitialized, EevtStartExploration);
	m_jsm.SetAction(estExploringChildren, EevtExploreChildren);

	SetJobQueue(pgroup->PjqExploration());

	CJob::SetInit();
}



//---------------------------------------------------------------------------
//	@function:
//		CJobGroupExploration::FScheduleGroupExpressions
//
//	@doc:
//		Schedule exploration jobs for all unexplored group expressions;
//		the function returns true if it could schedule any new jobs
//
//---------------------------------------------------------------------------
BOOL
CJobGroupExploration::FScheduleGroupExpressions(CSchedulerContext *psc)
{
	CGroupExpression *pgexprLast = m_pgexprLastScheduled;

	// iterate on expressions and schedule them as needed
	CGroupExpression *pgexpr = PgexprFirstUnsched();
	while (nullptr != pgexpr)
	{
		if (!pgexpr->FTransitioned(CGroupExpression::estExplored))
		{
			CJobGroupExpressionExploration::ScheduleJob(psc, pgexpr, this);
			pgexprLast = pgexpr;
		}

		// move to next expression
		{
			CGroupProxy gp(m_pgroup);
			pgexpr = gp.PgexprNext(pgexpr);
		}
	}

	BOOL fNewJobs = (m_pgexprLastScheduled != pgexprLast);

	// set last scheduled expression
	m_pgexprLastScheduled = pgexprLast;

	return fNewJobs;
}


//---------------------------------------------------------------------------
//	@function:
//		CJobGroupExploration::EevtStartExploration
//
//	@doc:
//		Start group exploration
//
//---------------------------------------------------------------------------
CJobGroupExploration::EEvent
CJobGroupExploration::EevtStartExploration(CSchedulerContext *,	 //psc
										   CJob *pjOwner)
{
	// get a job pointer
	CJobGroupExploration *pjge = PjConvert(pjOwner);
	CGroup *pgroup = pjge->m_pgroup;

	// move group to exploration state
	{
		CGroupProxy gp(pgroup);
		gp.SetState(CGroup::estExploring);
	}

	return eevStartedExploration;
}


//---------------------------------------------------------------------------
//	@function:
//		CJobGroupExploration::EevtExploreChildren
//
//	@doc:
//		Explore child group expressions
//
//---------------------------------------------------------------------------
CJobGroupExploration::EEvent
CJobGroupExploration::EevtExploreChildren(CSchedulerContext *psc, CJob *pjOwner)
{
	// get a job pointer
	CJobGroupExploration *pjge = PjConvert(pjOwner);
	if (pjge->FScheduleGroupExpressions(psc))
	{
		// new expressions have been added to group
		return eevNewChildren;
	}
	else
	{
		// no new expressions have been added to group, move to explored state
		{
			CGroupProxy gp(pjge->m_pgroup);
			gp.SetState(CGroup::estExplored);
		}

		// if this is the root, complete exploration phase
		if (psc->Peng()->FRoot(pjge->m_pgroup))
		{
			psc->Peng()->FinalizeExploration();
		}

		return eevExplored;
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CJobGroupExploration::FExecute
//
//	@doc:
//		Main job function
//
//---------------------------------------------------------------------------
BOOL
CJobGroupExploration::FExecute(CSchedulerContext *psc)
{
	GPOS_ASSERT(FInit());

	return m_jsm.FRun(psc, this);
}


//---------------------------------------------------------------------------
//	@function:
//		CJobGroupExploration::ScheduleJob
//
//	@doc:
//		Schedule a new group exploration job
//
//---------------------------------------------------------------------------
void
CJobGroupExploration::ScheduleJob(CSchedulerContext *psc, CGroup *pgroup,
								  CJob *pjParent)
{
	CJob *pj = psc->Pjf()->PjCreate(CJob::EjtGroupExploration);

	// initialize job
	CJobGroupExploration *pjge = PjConvert(pj);
	pjge->Init(pgroup);
	psc->Psched()->Add(pjge, pjParent);
}

#ifdef GPOS_DEBUG

//---------------------------------------------------------------------------
//	@function:
//		CJobGroupExploration::OsPrint
//
//	@doc:
//		Print function
//
//---------------------------------------------------------------------------
IOstream &
CJobGroupExploration::OsPrint(IOstream &os) const
{
	return m_jsm.OsHistory(os);
}

#endif	// GPOS_DEBUG

// EOF
