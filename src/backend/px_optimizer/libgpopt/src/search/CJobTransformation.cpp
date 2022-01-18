//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 Greenplum, Inc.
//
//	@filename:
//		CJobTransformation.cpp
//
//	@doc:
//		Implementation of group expression transformation job
//---------------------------------------------------------------------------

#include "gpopt/search/CJobTransformation.h"

#include "gpopt/engine/CEngine.h"
#include "gpopt/operators/CLogical.h"
#include "gpopt/search/CGroup.h"
#include "gpopt/search/CGroupExpression.h"
#include "gpopt/search/CJobFactory.h"
#include "gpopt/search/CScheduler.h"
#include "gpopt/search/CSchedulerContext.h"


using namespace gpopt;

// State transition diagram for transformation job state machine;
//
// +-----------------+
// | estInitialized: |
// | EevtTransform() |
// +-----------------+
//   |
//   | eevCompleted
//   v
// +-----------------+
// |  estCompleted   |
// +-----------------+
//
const CJobTransformation::EEvent
	rgeev[CJobTransformation::estSentinel][CJobTransformation::estSentinel] = {
		{// estInitialized
		 CJobTransformation::eevSentinel, CJobTransformation::eevCompleted},
		{// estCompleted
		 CJobTransformation::eevSentinel, CJobTransformation::eevSentinel},
};

#ifdef GPOS_DEBUG

// names for states
const WCHAR rgwszStates[CJobTransformation::estSentinel]
					   [GPOPT_FSM_NAME_LENGTH] = {GPOS_WSZ_LIT("initialized"),
												  GPOS_WSZ_LIT("completed")};

// names for events
const WCHAR rgwszEvents[CJobTransformation::eevSentinel]
					   [GPOPT_FSM_NAME_LENGTH] = {GPOS_WSZ_LIT("transforming")};

#endif	//GPOS_DEBUG

//---------------------------------------------------------------------------
//	@function:
//		CJobTransformation::CJobTransformation
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CJobTransformation::CJobTransformation() = default;


//---------------------------------------------------------------------------
//	@function:
//		CJobTransformation::~CJobTransformation
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CJobTransformation::~CJobTransformation() = default;


//---------------------------------------------------------------------------
//	@function:
//		CJobTransformation::Init
//
//	@doc:
//		Initialize job
//
//---------------------------------------------------------------------------
void
CJobTransformation::Init(CGroupExpression *pgexpr, CXform *pxform)
{
	GPOS_ASSERT(!FInit());
	GPOS_ASSERT(nullptr != pgexpr);
	GPOS_ASSERT(nullptr != pxform);

	m_pgexpr = pgexpr;
	m_xform = pxform;

	m_jsm.Init(rgeev
#ifdef GPOS_DEBUG
			   ,
			   rgwszStates, rgwszEvents
#endif	// GPOS_DEBUG
	);

	// set job actions
	m_jsm.SetAction(estInitialized, EevtTransform);

	// mark as initialized
	CJob::SetInit();
}


//---------------------------------------------------------------------------
//	@function:
//		CJobTransformation::EevtTransform
//
//	@doc:
//		Apply transformation action
//
//---------------------------------------------------------------------------
CJobTransformation::EEvent
CJobTransformation::EevtTransform(CSchedulerContext *psc, CJob *pjOwner)
{
	// get a job pointer
	CJobTransformation *pjt = PjConvert(pjOwner);
	CMemoryPool *pmpGlobal = psc->GetGlobalMemoryPool();
	CMemoryPool *pmpLocal = psc->PmpLocal();
	CGroupExpression *pgexpr = pjt->m_pgexpr;
	CXform *pxform = pjt->m_xform;

	// insert transformation results to memo
	CXformResult *pxfres = GPOS_NEW(pmpGlobal) CXformResult(pmpGlobal);
	ULONG ulElapsedTime = 0;
	ULONG ulNumberOfBindings = 0;
	pgexpr->Transform(pmpGlobal, pmpLocal, pxform, pxfres, &ulElapsedTime,
					  &ulNumberOfBindings);
	psc->Peng()->InsertXformResult(pgexpr->Pgroup(), pxfres, pxform->Exfid(),
								   pgexpr, ulElapsedTime, ulNumberOfBindings);
	pxfres->Release();

	return eevCompleted;
}


//---------------------------------------------------------------------------
//	@function:
//		CJobTransformation::FExecute
//
//	@doc:
//		Main job function
//
//---------------------------------------------------------------------------
BOOL
CJobTransformation::FExecute(CSchedulerContext *psc)
{
	GPOS_ASSERT(FInit());

	return m_jsm.FRun(psc, this);
}


//---------------------------------------------------------------------------
//	@function:
//		CJobTransformation::ScheduleJob
//
//	@doc:
//		Schedule a new transformation job
//
//---------------------------------------------------------------------------
void
CJobTransformation::ScheduleJob(CSchedulerContext *psc,
								CGroupExpression *pgexpr, CXform *pxform,
								CJob *pjParent)
{
	CJob *pj = psc->Pjf()->PjCreate(CJob::EjtTransformation);

	// initialize job
	CJobTransformation *pjt = PjConvert(pj);
	pjt->Init(pgexpr, pxform);
	psc->Psched()->Add(pjt, pjParent);
}

#ifdef GPOS_DEBUG

//---------------------------------------------------------------------------
//	@function:
//		CJobTransformation::OsPrint
//
//	@doc:
//		Print function
//
//---------------------------------------------------------------------------
IOstream &
CJobTransformation::OsPrint(IOstream &os) const
{
	return m_jsm.OsHistory(os);
}

#endif	// GPOS_DEBUG


// EOF
