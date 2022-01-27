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
 * Copyright (c) 2012-2014, TransLattice, Inc.
 *
 * IDENTIFICATION
 *      $$
 *
 *
 *-------------------------------------------------------------------------
 */

#include <sys/time.h>

#include "postgres.h"

#include "miscadmin.h"
#include "commands/prepare.h"
#include "executor/executor.h"
#include "nodes/pg_list.h"
#include "pgxc/locator.h"
#include "pgxc/nodemgr.h"
#include "pgxc/pgxc.h"
#include "pgxc/pgxcnode.h"
#include "pgxc/squeue.h"
#include "storage/latch.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "utils/hsearch.h"
#include "utils/resowner.h"
#include "pgstat.h"
#include <pthread.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include "storage/spin.h"
#include "storage/s_lock.h"
#include "miscadmin.h"
#include "libpq/libpq-be.h"
#include "utils/lsyscache.h"
#include "storage/fd.h"
#include "storage/shm_toc.h"
#include "access/parallel.h"
#include "postmaster/postmaster.h"
#include "access/printtup.h"
#include "catalog/pg_type.h"
#include "utils/typcache.h"
#include "access/htup_details.h"
#include "executor/execParallel.h"
#include "utils/memutils.h"
#include "utils/elog.h"
#include "commands/vacuum.h"

/*
typedef struct
{
    int             m_cnt;
    pthread_mutex_t m_mutex;
    pthread_cond_t  m_cond;
}ThreadSema;
*/
extern void ThreadSemaInit(ThreadSema *sema, int32 init);
extern void ThreadSemaDown(ThreadSema *sema);
extern void ThreadSemaUp(ThreadSema *sema);

typedef slock_t pg_spin_lock;

static void spinlock_init(pg_spin_lock *lock);
static void spinlock_lock(pg_spin_lock *lock);
static void spinlock_unlock(pg_spin_lock *lock);

void ThreadSemaInit(ThreadSema *sema, int32 init)
{
    if (sema)
    {
        sema->m_cnt = init;
        pthread_mutex_init(&sema->m_mutex, 0);
        pthread_cond_init(&sema->m_cond, 0);
    }
}

void ThreadSemaDown(ThreadSema *sema)
{
    if (sema)
    {
        (void)pthread_mutex_lock(&sema->m_mutex);
#if 1
        if (--(sema->m_cnt) < 0)
        {
            /* thread goes to sleep */

            (void)pthread_cond_wait(&sema->m_cond, &sema->m_mutex);
        }
#endif
        (void)pthread_mutex_unlock(&sema->m_mutex);
    }
}

void ThreadSemaUp(ThreadSema *sema)
{
    if (sema)
    {
        (void)pthread_mutex_lock(&sema->m_mutex);
#if 1
        if ((sema->m_cnt)++ < 0)
        {
            /*wake up sleeping thread*/
            (void)pthread_cond_signal(&sema->m_cond);
        }
#endif

        (void)pthread_mutex_unlock(&sema->m_mutex);
    }
}

void spinlock_init(pg_spin_lock *lock)
{
    SpinLockInit(lock);
}

void spinlock_lock(pg_spin_lock *lock)
{
    if (lock)
    {
        SpinLockAcquire(lock);
    }
}

void spinlock_unlock(pg_spin_lock *lock)
{
    if (lock)
    {
        SpinLockRelease(lock);
    }
}

int32 CreateThread(void *(*f)(void *), void *arg, int32 mode)
{

    pthread_attr_t attr;
    pthread_t threadid;
    int ret = 0;

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

PGPipe *CreatePipe(uint32 size)
{
    PGPipe *pPipe = NULL;
    pPipe = palloc0(sizeof(PGPipe));
    pPipe->m_List = (void **)palloc0(sizeof(void *) * size);
    pPipe->m_Length = size;
    pPipe->m_Head = 0;
    pPipe->m_Tail = 0;
    spinlock_init(&(pPipe->m_lock));
    return pPipe;
}
void DestoryPipe(PGPipe *pPipe)
{
    if (pPipe)
    {
        pfree(pPipe->m_List);
        pfree(pPipe);
    }
}
/*从队列中取出一个单元使用，如果队列为空就返回空指针*/
void *PipeGet(PGPipe *pPipe)
{
    void *ptr = NULL;
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
int PipePut(PGPipe *pPipe, void *p)
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
bool PipeIsFull(PGPipe *pPipe)
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
bool IsEmpty(PGPipe *pPipe)
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
int PipeLength(PGPipe *pPipe)
{
    int len = -1;
    spinlock_lock(&(pPipe->m_lock));
    len = (pPipe->m_Tail - pPipe->m_Head + pPipe->m_Length) % pPipe->m_Length;
    spinlock_unlock(&(pPipe->m_lock));
    return len;
}
