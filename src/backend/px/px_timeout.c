/*-------------------------------------------------------------------------
 *
 * px_timeout.c
 *	  Functions to cancel a PX query and release its acquired locks
 *	  if the lockmode conflicts with another lock request, after the
 *	  timeout specified by GUC variable.
 *
 * Copyright (c) 2020, Alibaba Group Holding Limited
 * 
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
 *	  src/backend/px/px_timeout.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "miscadmin.h"
#include "storage/lock.h"
#include "storage/proc.h"
#include "storage/procarray.h"

/*
 * The flag representing a PX wait lock timeout is triggered.
 * It is set by the px_wait_lock_timeout_handler(), and reset
 * after the wait lock situation is processed by px_cancel_wait_lock().
 */
volatile sig_atomic_t px_wait_lock_alert;

/*
 * PX wait lock timer ID returned by RegisterTimeout().
 */
int px_wait_lock_timer_id = -1;


static inline bool lock_is_granted(LockInstanceData *lock);
static inline bool lock_equal(LockInstanceData *lock1, LockInstanceData *lock2);
void px_cancel_wait_lock(void);
void px_wait_lock_timeout_handler(void);


/* Is the lock granted to the requesting process? */
static inline bool
lock_is_granted(LockInstanceData *lock)
{
	return lock->holdMask != 0;
}

static inline bool
lock_equal(LockInstanceData *lock1, LockInstanceData *lock2)
{
	LOCKTAG *tag1 = &lock1->locktag;
	LOCKTAG *tag2 = &lock2->locktag;

	return memcmp(tag1, tag2, sizeof(LOCKTAG)) == 0;
}

/*
 * Timeout occurs. If current process is requesting for a conflict
 * lock mode with a process which is executing PX, send signal SIGUSR1
 * to cancel the PX process.
 */
void
px_cancel_wait_lock(void)
{
	LockData* lockData;
	int waiter;
	int holder;
	
	/* Get lock manager's internal status */
	lockData = GetLockStatusData();

	for (waiter = 0; waiter < lockData->nelements; waiter++)
	{
		LockInstanceData *waiter_lock = &lockData->locks[waiter];

		/*
		 * The waiter should be the current process, and the lock
		 * should not be granted yet.
		 */
		if (waiter_lock->pid != MyProc->pid)
			continue;
		if (lock_is_granted(waiter_lock))
			continue;
		
		for (holder = 0; holder < lockData->nelements; holder++)
		{
			LockInstanceData *holder_lock = &lockData->locks[holder];
			PGPROC *holder_proc;

			/*
			 * Filtering condition:
			 *   1. waiter and holder should not be the same lock instance
			 *   2. holder lock should already been granted
			 *   3. waiter and holder should not be in the same process
			 *   4. waiter and holder should be waiting for the same lock tag
			 *   5. waiter's lock mode should be conflict with holder's
			 */
			if (holder == waiter)
				continue;
			if (!lock_is_granted(holder_lock))
				continue;
			if (holder_lock->pid == waiter_lock->pid)
				continue;
			if (!lock_equal(holder_lock, waiter_lock))
				continue;
			if (!polar_check_wait_lockmode_conflict_holdmask(waiter_lock->locktag,
															 waiter_lock->waitLockMode,
															 holder_lock->holdMask))
				continue;
			
			/* Get the PGPROC of holder process in shared memory */ 
			holder_proc = BackendPidGetProcWithLock(holder_lock->pid);
			if (holder_proc && !pg_atomic_unlocked_test_flag(&holder_proc->polar_px_is_executing))
			{
				elog(DEBUG3, "PX process %d hold the conflict lock for too long, send signal SIGUSR1 to cancel it",
					 holder_lock->pid);
				(void) SendProcSignal(holder_proc->pid,
									  PROCSIG_RECOVERY_CONFLICT_LOCK,
									  holder_proc->backendId);
			}
		}
	}
}

/*
 * Called after timeout triggered by timer. Set the flag to true,
 * and set the proc latch of current process to wake up current
 * process in ProcSleep().
 */
void
px_wait_lock_timeout_handler(void)
{
	int save_errno = errno;

	px_wait_lock_alert = true;
	SetLatch(MyLatch);

	errno = save_errno;
}
