//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2008 - 2010 Greenplum, Inc.
//
//	@filename:
//		CWorker.cpp
//
//	@doc:
//		Worker abstraction, e.g. thread
//---------------------------------------------------------------------------

#include "gpos/task/CWorker.h"

#include "gpos/common/syslibwrapper.h"
#include "gpos/memory/CMemoryPoolManager.h"
#include "gpos/string/CWStringStatic.h"
#include "gpos/task/CWorkerPoolManager.h"

using namespace gpos;

// host system callback function to report abort requests
bool (*CWorker::abort_requested_by_system)(void);


//---------------------------------------------------------------------------
//	@function:
//		CWorker::CWorker
//
//	@doc:
//		ctor
//
//---------------------------------------------------------------------------
CWorker::CWorker(ULONG stack_size, ULONG_PTR stack_start)
	: m_task(nullptr), m_stack_size(stack_size), m_stack_start(stack_start)
{
	GPOS_ASSERT(stack_size >= 2 * 1024 &&
				"Worker has to have at least 2KB stack");

	// register worker
	GPOS_ASSERT(nullptr == Self() && "Found registered worker!");

	CWorkerPoolManager::WorkerPoolManager()->RegisterWorker(this);
	GPOS_ASSERT(this == CWorkerPoolManager::WorkerPoolManager()->Self());
}


//---------------------------------------------------------------------------
//	@function:
//		CWorker::~CWorker
//
//	@doc:
//		dtor
//
//---------------------------------------------------------------------------
CWorker::~CWorker()
{
	// unregister worker
	GPOS_ASSERT(this == Self() && "Unidentified worker found.");
	CWorkerPoolManager::WorkerPoolManager()->RemoveWorker();
}


//---------------------------------------------------------------------------
//	@function:
//		CWorker::Execute
//
//	@doc:
//		Execute single task
//
//---------------------------------------------------------------------------
void
CWorker::Execute(CTask *task)
{
	GPOS_ASSERT(task);
	GPOS_ASSERT(nullptr == m_task && "Another task is assigned to worker");

	m_task = task;
	GPOS_TRY
	{
		m_task->Execute();
		m_task = nullptr;
	}
	GPOS_CATCH_EX(ex)
	{
		m_task = nullptr;
		GPOS_RETHROW(ex);
	}
	GPOS_CATCH_END;
}


//---------------------------------------------------------------------------
//	@function:
//		CWorker::CheckForAbort
//
//	@doc:
//		Check pending abort flag; throw if abort is flagged
//
//---------------------------------------------------------------------------
void
CWorker::CheckForAbort(const CHAR *, ULONG)
{
	// check if there is a task assigned to worker,
	// task is still running and CFA is not suspended
	if (nullptr != m_task && m_task->IsRunning() && !m_task->IsAbortSuspended())
	{
		GPOS_ASSERT(!m_task->GetErrCtxt()->IsPending() &&
					"Check-For-Abort while an exception is pending");

		if ((nullptr != abort_requested_by_system &&
			 abort_requested_by_system()) ||
			m_task->IsCanceled())
		{
			// raise exception
			GPOS_ABORT;
		}
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CWorker::CheckStackSize
//
//	@doc:
//		Size of stack within context of this worker;
//		effectively calculates distance of local variable to stack start;
//		if stack space is exhausted we throw an exception;
//		else we check if requested space can fit in stack
//
//---------------------------------------------------------------------------
BOOL
CWorker::CheckStackSize(ULONG request) const
{
	ULONG_PTR ptr = 0;

	// get current stack size
	ULONG_PTR size = m_stack_start - (ULONG_PTR) &ptr;

	// check if we have exceeded stack space
	if (size >= m_stack_size)
	{
		// raise stack overflow exception
		GPOS_RAISE(CException::ExmaSystem, CException::ExmiOutOfStack);
	}

	// check if there is enough stack space for request
	if (size + request >= m_stack_size)
	{
		return false;
	}
	return true;
}


// EOF
