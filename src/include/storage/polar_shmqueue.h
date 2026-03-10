/*-------------------------------------------------------------------------
 *
 * polar_shmqueue.h
 *	  shared memory management structures
 *
 * Note:
 * The community has removed SHM_QUEUE by commit d137cb52 and use dlist or
 * dclist inside ilist.h. But the polardb procpool needs one simple double
 * link without head node in shared memory. So we just port back SHM_QUEUE
 * from old PG version.
 *
 * Portions Copyright (c) 1996-2022, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/storage/polar_shmqueue.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef POLAR_SHMQUEUE_H
#define POLAR_SHMQUEUE_H

typedef struct SHM_QUEUE
{
	struct SHM_QUEUE *prev;
	struct SHM_QUEUE *next;
} SHM_QUEUE;

extern void SHMQueueInit(SHM_QUEUE *queue);
extern void SHMQueueElemInit(SHM_QUEUE *queue);
extern void SHMQueueDelete(SHM_QUEUE *queue);
extern void SHMQueueInsertBefore(SHM_QUEUE *queue, SHM_QUEUE *elem);
extern void SHMQueueInsertAfter(SHM_QUEUE *queue, SHM_QUEUE *elem);
extern Pointer SHMQueueNext(const SHM_QUEUE *queue, const SHM_QUEUE *curElem,
							Size linkOffset);
extern Pointer SHMQueuePrev(const SHM_QUEUE *queue, const SHM_QUEUE *curElem,
							Size linkOffset);
extern bool SHMQueueEmpty(const SHM_QUEUE *queue);
extern bool SHMQueueIsDetached(const SHM_QUEUE *queue);

#endif							/* POLAR_SHMQUEUE_H */
