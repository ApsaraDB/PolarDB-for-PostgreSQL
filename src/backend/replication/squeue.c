/*-------------------------------------------------------------------------
 *
 * squeue.c
 *
 *      Shared queue is for data exchange in shared memory between sessions,
 * one of which is a producer, providing data rows. Others are consumer agents -
 * sessions initiated from other datanodes, the main purpose of them is to read
 * rows from the shared queue and send then to the parent data node.
 *    The producer is usually a consumer at the same time, it sends back tuples
 * to the parent node without putting it to the queue.
 *
 * Copyright (c) 2020, Alibaba Group Holding Limited
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at

 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Copyright (c) 2012-2014, TransLattice, Inc.
 *
 * IDENTIFICATION
 *      $$
 *
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include <pthread.h>
#include "storage/s_lock.h"
#include "storage/spin.h"
#include "replication/squeue.h"


extern void ThreadSemaRecInitRec(ThreadSemaRec * sema, int32 init);
extern void ThreadSemaRecDownRec(ThreadSemaRec * sema);
extern void ThreadSemaRecUpRec(ThreadSemaRec * sema);

typedef slock_t pg_spin_lock;

static void spinlock_init(pg_spin_lock * lock);
static void spinlock_lock(pg_spin_lock * lock);
static void spinlock_unlock(pg_spin_lock * lock);

void
ThreadMutexInitRec(pthread_mutex_t *mutex)
{
	pthread_mutex_init(mutex, 0);
}

void
ThreadMutexLockRec(pthread_mutex_t *mutex)
{
	pthread_mutex_lock(mutex);
}

void
ThreadMutexUnlockRec(pthread_mutex_t *mutex)
{
	pthread_mutex_unlock(mutex);
}

void
ThreadSemaRecInitRec(ThreadSemaRec * sema, int32 init)
{
	if (sema)
	{
		sema->m_cnt = init;
		pthread_mutex_init(&sema->m_mutex, 0);
		pthread_cond_init(&sema->m_cond, 0);
	}
}

void
ThreadSemaRecDownRec(ThreadSemaRec * sema)
{
	if (sema)
	{
		(void) pthread_mutex_lock(&sema->m_mutex);

		if (--(sema->m_cnt) < 0)
		{
			/* thread goes to sleep */

			(void) pthread_cond_wait(&sema->m_cond, &sema->m_mutex);
		}

		(void) pthread_mutex_unlock(&sema->m_mutex);
	}
}

void
ThreadSemaRecUpRec(ThreadSemaRec * sema)
{
	if (sema)
	{
		(void) pthread_mutex_lock(&sema->m_mutex);

		if ((sema->m_cnt)++ < 0)
		{
			/* wake up sleeping thread */
			(void) pthread_cond_signal(&sema->m_cond);
		}


		(void) pthread_mutex_unlock(&sema->m_mutex);
	}
}

static void
spinlock_init(pg_spin_lock * lock)
{
	SpinLockInit(lock);
}

static void
spinlock_lock(pg_spin_lock * lock)
{
	if (lock)
	{
		SpinLockAcquire(lock);
	}
}

static void
spinlock_unlock(pg_spin_lock * lock)
{
	if (lock)
	{
		SpinLockRelease(lock);
	}
}


int32
CreateThreadRec(void *(*f) (void *), void *arg, int32 mode)
{

	pthread_attr_t attr;
	pthread_t	threadid;
	int			ret = 0;

	pthread_attr_init(&attr);
	switch (mode)
	{
		case MT_THR_JOINABLE:
			{
				pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);
				break;
			}
		case MT_THR_DETACHED:
			{
				pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);
				break;
			}
		default:
			{
				elog(ERROR, "invalid thread mode %d\n", mode);
			}
	}
	ret = pthread_create(&threadid, &attr, f, arg);
	return ret;
}


PGPipeRec *
CreatePipeRec(uint32 size)
{
	PGPipeRec	   *pPipe = NULL;

	pPipe = palloc0(sizeof(PGPipeRec));
	pPipe->m_List = (void **) palloc0(sizeof(void *) * size);
	pPipe->m_Length = size;
	pPipe->m_Head = 0;
	pPipe->m_Tail = 0;
	spinlock_init(&(pPipe->m_lock));
	return pPipe;
}
void
DestoryPipeRec(PGPipeRec * pPipe)
{
	if (pPipe)
	{
		pfree(pPipe->m_List);
		pfree(pPipe);
	}
}

void *
PipeGetRec(PGPipeRec * pPipe)
{
	void	   *ptr = NULL;

	spinlock_lock(&(pPipe->m_lock));
	if (pPipe->m_Head == pPipe->m_Tail)
	{
		spinlock_unlock(&(pPipe->m_lock));
		return NULL;
	}
	ptr = pPipe->m_List[pPipe->m_Head];
	pPipe->m_List[pPipe->m_Head] = NULL;
	pPipe->m_Head = (pPipe->m_Head + 1) % pPipe->m_Length;
	spinlock_unlock(&(pPipe->m_lock));
	return ptr;
}

/**
 * add p to the pipe, return 0 if successful, -1 if fail
 */
int
PipePutRec(PGPipeRec * pPipe, void *p)
{
	spinlock_lock(&(pPipe->m_lock));
	if ((pPipe->m_Tail + 1) % pPipe->m_Length == pPipe->m_Head)
	{
		spinlock_unlock(&(pPipe->m_lock));
		return -1;
	}
	pPipe->m_List[pPipe->m_Tail] = p;
	pPipe->m_Tail = (pPipe->m_Tail + 1) % pPipe->m_Length;
	spinlock_unlock(&(pPipe->m_lock));
	return 0;
}
bool
PipeIsFullRec(PGPipeRec * pPipe)
{
	spinlock_lock(&(pPipe->m_lock));
	if ((pPipe->m_Tail + 1) % pPipe->m_Length == pPipe->m_Head)
	{
		spinlock_unlock(&(pPipe->m_lock));
		return true;
	}
	else
	{
		spinlock_unlock(&(pPipe->m_lock));
		return false;
	}
}
bool
IsEmptyRec(PGPipeRec * pPipe)
{
	spinlock_lock(&(pPipe->m_lock));
	if (pPipe->m_Tail == pPipe->m_Head)
	{
		spinlock_unlock(&(pPipe->m_lock));
		return true;
	}
	else
	{
		spinlock_unlock(&(pPipe->m_lock));
		return false;
	}
}
int
PipeLengthRec(PGPipeRec * pPipe)
{
	int			len = -1;

	spinlock_lock(&(pPipe->m_lock));
	len = (pPipe->m_Tail - pPipe->m_Head + pPipe->m_Length) % pPipe->m_Length;
	spinlock_unlock(&(pPipe->m_lock));
	return len;
}
