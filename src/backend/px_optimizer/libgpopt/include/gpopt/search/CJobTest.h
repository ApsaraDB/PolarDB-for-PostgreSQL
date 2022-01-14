//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CJobTest.h
//
//	@doc:
//		Job implementation for testing purposes
//---------------------------------------------------------------------------
#ifndef GPOPT_CJobTest_H
#define GPOPT_CJobTest_H

#include "gpos/base.h"

#include "gpopt/search/CJob.h"
#include "gpopt/search/CJobQueue.h"

namespace gpopt
{
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CJobTest
//
//	@doc:
//		Job derivative for unittests
//
//---------------------------------------------------------------------------
class CJobTest : public CJob
{
	// friends
	friend class CJobFactory;

public:
	// job test type
	enum ETestType
	{
		EttSpawn,
		EttStartQueue,
		EttQueueu
	};

private:
	// test type
	ETestType m_ett{EttSpawn};

	// number of job spawning rounds
	ULONG m_ulRounds{gpos::ulong_max};

	// spawning fanout
	ULONG m_ulFanout{gpos::ulong_max};

	// CPU-burning iterations per job
	ULONG m_ulIters{gpos::ulong_max};

	// iteration counter
	static ULONG_PTR m_ulpCnt;

	// job queue
	CJobQueue *m_pjq;

	// test job spawning
	BOOL FSpawn(CSchedulerContext *psc);

	// start jobs to be queued
	BOOL FStartQueue(CSchedulerContext *psc);

	// test job queueing
	BOOL FQueue(CSchedulerContext *psc);

	// burn some CPU to simulate actual work
	void Loop() const;

public:
	// ctor
	CJobTest();

	// dtor
	~CJobTest() override;

	// execution
	BOOL FExecute(CSchedulerContext *psc) override;

#ifdef GPOS_DEBUG
	// printer
	IOstream &OsPrint(IOstream &) const override;
#endif	// GPOS_DEBUG

	// set execution parameters
	void
	Init(ETestType ett, ULONG ulRounds, ULONG ulFanout, ULONG ulIters,
		 CJobQueue *pjq)
	{
		m_ett = ett;
		m_ulRounds = ulRounds;
		m_ulFanout = ulFanout;
		m_ulIters = ulIters;
		m_pjq = pjq;
	}

	// copy execution parameters
	void
	Init(CJobTest *pjt)
	{
		Init(pjt->m_ett, pjt->m_ulRounds, pjt->m_ulFanout, pjt->m_ulIters,
			 pjt->m_pjq);
	}

	// reset
	static void
	ResetCnt()
	{
		m_ulpCnt = 0;
	}

	// conversion function
	static CJobTest *
	PjConvert(CJob *pj)
	{
		GPOS_ASSERT(nullptr != pj);
		GPOS_ASSERT(EjtTest == pj->Ejt());

		return dynamic_cast<CJobTest *>(pj);
	}
};
}  // namespace gpopt

#endif	// !GPOPT_CJobTest_H


// EOF
