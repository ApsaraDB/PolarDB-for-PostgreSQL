//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CJobStateMachine.h
//
//	@doc:
//		State machine to execute actions and maintain state of
//		optimization jobs;
//---------------------------------------------------------------------------
#ifndef GPOPT_CJobStateMachine_H
#define GPOPT_CJobStateMachine_H


#include "gpos/base.h"
#include "gpos/common/CEnumSet.h"
#include "gpos/types.h"

#include "gpopt/base/CStateMachine.h"
#include "gpopt/engine/CEngine.h"
#include "gpopt/search/CSchedulerContext.h"

namespace gpopt
{
using namespace gpos;

// prototypes
class CJob;
class CSchedulerContext;

//---------------------------------------------------------------------------
//	@class:
//		CJobStateMachine
//
//	@doc:
//		Optimization job state machine
//
//		The class CJobStateMachine has a templated definition of the GPOS class
//		CStateMachine. The template allows parameterizing the state machine
//		with states and events enumerations. The final (terminating) state in
//		the state machine is the last entry in the states enumeration.
//
//		Job actions:
//		The class CJobStateMachine also allows setting an action for each
//		state. An action is a function to be executed when the state machine is
//		in a given state. Setting actions happens inside the function
//		CJobStateMachine::SetAction(). This function is called from the Init()
//		function inside each job. The outcome of an action is an event, that
//		may cause the state machine to remain in the same state (self-loop on
//		current state), or move the state machine to a different state.
//
//		Basic job state machine execution:
//		The function that runs the state machine is CJobStateMachine::FRun().
//		This is a simple loop that iteratively calls the action that
//		corresponds to current state. Executing an action returns an event,
//		which is used as input to move the state machine to a new (or same)
//		state. The loop ends when the state machine reaches the final
//		(terminating) state.
//
//		State transitions:
//		Each optimization job inlines the definition of its state machine at
//		the top of its cpp file (see CJobGroupOptimization.cpp for an example).
//		The state machine definition is primarily given by a state transition
//		diagram represented as a square matrix of |states| x |states| entries.
//		Each matrix entry contains an event that causes the transition of state
//		machine from the state corresponding to the current row to the state
//		corresponding to the current column. For example the entry at [row 1,
//		column 2] contains an event e that causes the state machine to
//		transition from state 1 to state 2. When executing  the action defined
//		by state 1, if  the return result is the event e, the state machine
//		advances to state 2. The state transition diagram must be carefully
//		defined to avoid infinite loops that have no possible exists (e.g.,
//		state 1 → state 2 → state 1).
//
//---------------------------------------------------------------------------
template <class TEnumState, TEnumState estSentinel, class TEnumEvent,
		  TEnumEvent eevSentinel>
class CJobStateMachine
{
private:
	// pointer to job action function
	typedef TEnumEvent (*PFuncAction)(CSchedulerContext *psc, CJob *pjOwner);

	// shorthand for state machine
	typedef CStateMachine<TEnumState, estSentinel, TEnumEvent, eevSentinel> SM;

	// array of actions corresponding to states
	PFuncAction m_rgPfuncAction[estSentinel];

	// job state machine
	SM m_sm;

public:
	CJobStateMachine(const CJobStateMachine &) = delete;

	// ctor
	CJobStateMachine<TEnumState, estSentinel, TEnumEvent, eevSentinel>() =
		default;

	// dtor
	~CJobStateMachine() = default;

	// initialize state machine
	void
	Init(const TEnumEvent rgfTransitions[estSentinel][estSentinel]
#ifdef GPOS_DEBUG
		 ,
		 const WCHAR wszStates[estSentinel][GPOPT_FSM_NAME_LENGTH],
		 const WCHAR wszEvent[estSentinel][GPOPT_FSM_NAME_LENGTH]
#endif	// GPOS_DEBUG
	)
	{
		Reset();

		m_sm.Init(rgfTransitions
#ifdef GPOS_DEBUG
				  ,
				  wszStates, wszEvent
#endif	// GPOS_DEBUG
		);
	}

	// match action with state
	void
	SetAction(TEnumState est, PFuncAction pfAction)
	{
		GPOS_ASSERT(nullptr != pfAction);
		GPOS_ASSERT(nullptr == m_rgPfuncAction[est] &&
					"Action has been already set");

		m_rgPfuncAction[est] = pfAction;
	}

	// run the state machine
	BOOL
	FRun(CSchedulerContext *psc, CJob *pjOwner)
	{
		GPOS_ASSERT(nullptr != psc);
		GPOS_ASSERT(nullptr != pjOwner);

		TEnumState estCurrent = estSentinel;
		TEnumState estNext = estSentinel;
		do
		{
			// check if current search stage is timed-out
			if (psc->Peng()->PssCurrent()->FTimedOut())
			{
				// cleanup job state and terminate state machine
				pjOwner->Cleanup();
				return true;
			}

			// find current state
			estCurrent = m_sm.Estate();

			// get the function associated with current state
			PFuncAction pfunc = m_rgPfuncAction[estCurrent];
			GPOS_ASSERT(nullptr != pfunc);

			// execute the function to get an event
			TEnumEvent eev = pfunc(psc, pjOwner);

			// use the event to transition state machine
			estNext = estCurrent;
			BOOL fSucceeded GPOS_ASSERTS_ONLY = m_sm.FTransition(eev, estNext);

			GPOS_ASSERT(fSucceeded);
		} while (estNext != estCurrent && estNext != m_sm.TesFinal());

		return (estNext == m_sm.TesFinal());
	}

	// reset state machine
	void
	Reset()
	{
		m_sm.Reset();

		// initialize actions array
		for (ULONG i = 0; i < estSentinel; i++)
		{
			m_rgPfuncAction[i] = nullptr;
		}
	}

#ifdef GPOS_DEBUG
	// dump history
	IOstream &
	OsHistory(IOstream &os) const
	{
		m_sm.OsHistory(os);
		return os;
	}

	// dump state machine diagram in graphviz format
	IOstream &
	OsDiagramToGraphviz(CMemoryPool *mp, IOstream &os,
						const WCHAR *wszTitle) const
	{
		(void) m_sm.OsDiagramToGraphviz(mp, os, wszTitle);

		return os;
	}

	// compute unreachable states
	void
	Unreachable(CMemoryPool *mp, TEnumState **ppestate, ULONG *pulSize) const
	{
		m_sm.Unreachable(mp, ppestate, pulSize);
	}

#endif	// GPOS_DEBUG

};	// class CJobStateMachine
}  // namespace gpopt

#endif	// !GPOPT_CJobStateMachine_H


// EOF
