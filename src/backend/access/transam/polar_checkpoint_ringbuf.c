/*-------------------------------------------------------------------------
 * polar_checkpoint_ringbuf.c
 *      checkpoint ringbuf for parallel replay in standby mode.
 *
 * Copyright (c) 2020, Alibaba Group Holding Limited
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * IDENTIFICATION
 *      src/backend/access/transam/polar_checkpoint_ringbuf.c
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/polar_checkpoint_ringbuf.h"

void
polar_checkpoint_ringbuf_init(polar_checkpoint_ringbuf_t *ringbuf)
{
	Assert(ringbuf != NULL);

	LWLockRegisterTranche(LWTRANCHE_POLAR_CHECKPOINT_RINGBUF, "checkpoint_ringbuf");
	LWLockInitialize(&ringbuf->lock, LWTRANCHE_POLAR_CHECKPOINT_RINGBUF);
}

/*
 * Remove extra checkpoints in the stash checkpoint array
 *
 * It will remove the ckpt array head utill there is only one ckpt whose redo_lsn
 * is less than bg_replayed_lsn.
 */
void
polar_checkpoint_ringbuf_shrink(polar_checkpoint_ringbuf_t *ringbuf, XLogRecPtr replayed)
{
	int i;

	Assert(ringbuf != NULL);

	LWLockAcquire(&ringbuf->lock, LW_EXCLUSIVE);

	for (i = 1; i < ringbuf->size; i++)
		if (ringbuf->checkpoints[(ringbuf->head + i) % POLAR_CHECKPOINT_RINGBUF_CAPACITY].redo > replayed)
			break;

	if (i > 1)
	{
		i--;
		ringbuf->head = (ringbuf->head + i) % POLAR_CHECKPOINT_RINGBUF_CAPACITY;
		ringbuf->size -= i;
	}

	LWLockRelease(&ringbuf->lock);
}

/*
 * Insert a checkpoint at the tail of the stash checkpoint array
 * The redo of checkpoints in the ringbuf must be incremental.
 *
 * It will evict a ckpt if the array is full.
 */
void
polar_checkpoint_ringbuf_insert(polar_checkpoint_ringbuf_t *ringbuf,
                                XLogRecPtr checkpointRecPtr,
                                XLogRecPtr checkpointEndPtr,
                                const CheckPoint *checkpoint)
{
	int target, tail;

	Assert(ringbuf != NULL);

	LWLockAcquire(&ringbuf->lock, LW_EXCLUSIVE);

	tail = (ringbuf->head + ringbuf->size - 1) % POLAR_CHECKPOINT_RINGBUF_CAPACITY;

	if (ringbuf->checkpoints[tail].redo > checkpoint->redo)
	{
		elog(LOG, "%s: ignore the checkpoint whose redo is less than the tail.", __func__);
		return;
	}

	/*
	 * Evict a checkpoint if the array is full. Current eviction strategy is to
	 * evict the last ckpt.
	 *
	 * TODO: A better strategy is to evict a ckpt to keep the redo distance
	 * between every two adjacent ckpts balanced. But it also incurs extra
	 * ckpt copy overhead. It's a tradeoff here.
	 */
	if (ringbuf->size == POLAR_CHECKPOINT_RINGBUF_CAPACITY)
		ringbuf->size--;

	/* Assign the checkpoint to the target posistion */
	target = (ringbuf->head + ringbuf->size) % POLAR_CHECKPOINT_RINGBUF_CAPACITY;
	ringbuf->recptrs[target] = checkpointRecPtr;
	ringbuf->endptrs[target] = checkpointEndPtr;
	ringbuf->checkpoints[target] = *checkpoint;
	ringbuf->size++;

	LWLockRelease(&ringbuf->lock);
}

/*
 * Get the first ckpt from the stashed ckpt array
 *
 * if the ckpt array is empty, it will set the checkpointRecPtr as NULL
 *
 * When size is not 0, it is guaranteed to return a ckpt whose redo is smaller
 * than bg_replayed_lsn, because the redo of the first read ckpt is the starting
 * point of bg_replayed_lsn. And subsequent operations will always retain a ckpt
 * whose redo is smaller than the bg_replayed_lsn.
 */
void
polar_checkpoint_ringbuf_front(polar_checkpoint_ringbuf_t *ringbuf,
                               XLogRecPtr *checkpointRecPtr,
                               XLogRecPtr *checkpointEndPtr,
                               CheckPoint *checkpoint)
{
	int head;

	Assert(ringbuf != NULL);

	LWLockAcquire(&ringbuf->lock, LW_SHARED);;

	if (ringbuf->size)
	{
		head = ringbuf->head;
		if (checkpointRecPtr)
			*checkpointRecPtr = ringbuf->recptrs[head];
		if (checkpointEndPtr)
			*checkpointEndPtr = ringbuf->endptrs[head];
		if (checkpoint)
			*checkpoint = ringbuf->checkpoints[head];
		LWLockRelease(&ringbuf->lock);;
	}
	else
	{
		LWLockRelease(&ringbuf->lock);;
		if (checkpointRecPtr)
			*checkpointRecPtr = InvalidXLogRecPtr;
		if (checkpointEndPtr)
			*checkpointEndPtr = InvalidXLogRecPtr;
		if (checkpoint)
			*checkpoint = (CheckPoint) {0};
	}
}
