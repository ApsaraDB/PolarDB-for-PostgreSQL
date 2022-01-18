//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		CTaskScheduler.h
//
//	@doc:
//		Task scheduler using FIFO.
//---------------------------------------------------------------------------
#ifndef GPOS_CTaskSchedulerFifo_H
#define GPOS_CTaskSchedulerFifo_H

#include "gpos/base.h"
#include "gpos/common/CList.h"
#include "gpos/task/CTask.h"
#include "gpos/task/ITaskScheduler.h"

namespace gpos
{
//---------------------------------------------------------------------------
//	@class:
//		CTaskScheduler
//
//	@doc:
//		Task scheduler abstraction maintains collection of tasks waiting to
//		execute and decides which task will execute next. The scheduling
//		algorithm is FIFO, i.e. tasks are queued and scheduled in the order
//		they arrive to the scheduler.
//
//		Queue operations are not thread-safe. The caller (worker pool
//		manager) is responsible for synchronizing concurrent accesses to
//		task scheduler.
//
//---------------------------------------------------------------------------

class CTaskSchedulerFifo : public ITaskScheduler
{
private:
	// task queue
	CList<CTask> m_task_queue;

public:
	CTaskSchedulerFifo(const CTaskSchedulerFifo &) = delete;

	// ctor
	CTaskSchedulerFifo()
	{
		m_task_queue.Init(GPOS_OFFSET(CTask, m_task_scheduler_link));
	}

	// dtor
	~CTaskSchedulerFifo() override = default;

	// add task to waiting queue
	void Enqueue(CTask *task) override;

	// get next task to execute
	CTask *Dequeue() override;

	// check if task is waiting to be scheduled and remove it
	GPOS_RESULT Cancel(CTask *task) override;

	// get number of waiting tasks
	ULONG
	GetQueueSize() override
	{
		return m_task_queue.Size();
	}

	// check if task queue is empty
	BOOL
	IsEmpty() const override
	{
		return m_task_queue.IsEmpty();
	}

};	// class CTaskSchedulerFifo
}  // namespace gpos

#endif /* GPOS_CTaskSchedulerFifo_H */

// EOF
