/*-------------------------------------------------------------------------
 *
 * polar_priority_replication.c
 *
 * Polar priority replication
 *
 * Copyright (c) 2021, Alibaba Group Holding Limited
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
 *	  src/backend/replication/polar_priority_replication.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <unistd.h>

#include "access/xact.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "replication/polar_priority_replication.h"
#include "replication/syncrep.h"
#include "replication/walsender.h"
#include "replication/walsender_private.h"
#include "storage/pmsignal.h"
#include "storage/proc.h"
#include "tcop/tcopprot.h"
#include "utils/builtins.h"
#include "utils/ps_status.h"

#define POLAR_PRI_REP_VALID_MODE(mode) \
	((mode) == POLAR_PRI_REP_WAIT_FLUSH)

/* POLAR GUCs */
int			polar_priority_replication_mode = POLAR_PRI_REP_OFF;
bool		polar_priority_replication_force_wait = false;
char	   *polar_high_priority_replication_standby_names;
char	   *polar_low_priority_replication_standby_names;

/* POLAR: priority replication helper function */
static bool polar_exist_high_priority_replication_walsender(int mode);
static bool polar_priority_replication_waiter_need_wait(int mode);
static void polar_priority_replication_get_oldest_rec_ptr(XLogRecPtr *priRepPtr);

void
polar_priority_replication_wait_for_lsn(XLogRecPtr lsn)
{
	bool	am_low_pri_walsender = false;

	/* 
	 * for physical replication, only low priority walsender should wait 
	 * for high priority walsender.
	 */
	LWLockAcquire(SyncRepLock, LW_SHARED);
	am_low_pri_walsender = MyWalSnd->is_low_priority_replication_standby;
	LWLockRelease(SyncRepLock);

	if (!am_low_pri_walsender)
		return;
	
	polar_priority_replication_walsender_wait_for_lsn(POLAR_PRI_REP_WAIT_FLUSH, lsn);
}

void
polar_priority_replication_walsender_wait_for_lsn(int mode, XLogRecPtr lsn)
{
	char	   *new_status = NULL;
	const char *old_status;
	long		sleeptime;
	bool		priority_replication_required = false;

	Assert(POLAR_PRI_REP_VALID_MODE(mode));

	LWLockAcquire(SyncRepLock, LW_SHARED);
	priority_replication_required = 
		(POLAR_PRI_REP_ENABLE() && WalSndCtl->high_priority_replication_standbys_defined);
	LWLockRelease(SyncRepLock);

	if (!priority_replication_required)
		return;

	Assert(SHMQueueIsDetached(&(MyProc->syncRepLinks)));
	Assert(WalSndCtl != NULL);

	LWLockAcquire(SyncRepLock, LW_EXCLUSIVE);
	Assert(MyProc->syncRepState == SYNC_REP_NOT_WAITING);

	/*
	 * Check that the standby hasn't already replied. Unlikely race
	 * condition but we'll be fetching that cache line anyway so it's likely
	 * to be a low cost check.
	 */
	if (lsn <= WalSndCtl->lsn[mode])
	{
		LWLockRelease(SyncRepLock);
		return;
	}

	/*
	 * Set our waitLSN so WALSender will know when to wake us, and add
	 * ourselves to the queue.
	 */
	MyProc->waitLSN = lsn;
	MyProc->syncRepState = SYNC_REP_WAITING;
	SyncRepQueueInsert(mode);
	Assert(SyncRepQueueIsOrderedByLSN(mode));
	LWLockRelease(SyncRepLock);

	/* Alter ps display to show waiting for pri rep. */
	if (update_process_title)
	{
		int			len;

		old_status = get_ps_display(&len);
		new_status = (char *) palloc(len + 32 + 1);
		memcpy(new_status, old_status, len);
		sprintf(new_status + len, " waiting for %X/%X",
				(uint32) (lsn >> 32), (uint32) lsn);
		set_ps_display(new_status, false);
		new_status[len] = '\0'; /* truncate off " waiting ..." */
	}

	/*
	 * Wait for specified LSN to be confirmed.
	 *
	 * Each proc has its own wait latch, so we perform a normal latch
	 * check/wait loop here.
	 */
	for (;;)
	{
		/* Must reset the latch before testing state. */
		ResetLatch(MyLatch);

		/*
		 * Acquiring the lock is not needed, the latch ensures proper
		 * barriers. If it looks like we're done, we must really be done,
		 * because once walsender changes the state to SYNC_REP_WAIT_COMPLETE,
		 * it will never update it again, so we can't be seeing a stale value
		 * in that case.
		 */
		if (MyProc->syncRepState == SYNC_REP_WAIT_COMPLETE)
			break;

		/* Exit the loop first, we will process these signals later */
		if (!PostmasterIsAlive() || ProcDiePending || QueryCancelPending)
		{
			SyncRepCancelWait();
			break;
		}

		if (!polar_priority_replication_waiter_need_wait(mode))
		{
			SyncRepCancelWait();
			break;
		}

		polar_wal_snd_normal_check();

		sleeptime = polar_wal_snd_compute_sleep_time();
		
		WaitLatch(MyLatch, WL_LATCH_SET | WL_POSTMASTER_DEATH, sleeptime,
				  WAIT_EVENT_SYNC_REP);
	}

	/*
	 * High Pri Rep WalSender has checked our LSN and has removed us from 
	 * queue. Clean up state and leave.  It's OK to reset these shared memory 
	 * fields without holding SyncRepLock, because any walsenders will ignore 
	 * us anyway when we're not on the queue.  We need a read barrier to make 
	 * sure we see the changes to the queue link (this might be unnecessary 
	 * without assertions, but better safe than sorry).
	 */
	pg_read_barrier();
	Assert(SHMQueueIsDetached(&(MyProc->syncRepLinks)));
	MyProc->syncRepState = SYNC_REP_NOT_WAITING;
	MyProc->waitLSN = 0;

	if (new_status)
	{
		/* Reset ps display */
		set_ps_display(new_status, false);
		pfree(new_status);
	}

	/* 
	 * POLAR: different from syncrep.c for normal backend, in priority replication,
	 * we should kill the walsender if required, otherwise it will send some data 
	 * that shouldn't be sent.
	 * We must guarantee to exit the waiting queue before killing this walsender 
	 * process, so we check interrupts and postmaster death here.
	 */

	/*no cover begin*/
	if (!PostmasterIsAlive())
		exit(1);
	/*no cover end*/

	CHECK_FOR_INTERRUPTS();
}

void
polar_priority_replication_release_waiters(void)
{
	volatile WalSndCtlData *walsndctl = WalSndCtl;
	XLogRecPtr	flushPtr = MyWalSnd->flush;
	XLogRecPtr	priRepPtr;
	bool		am_high_pri_walsender = false;
	int			mode;

	/*
	 * If we are stil starting up, still running base backup or the current 
	 * flush position is still invalid, then leave quickly.  Streaming or 
	 * stopping WAL senders are allowed to release waiters.
	 */
	if ((MyWalSnd->state != WALSNDSTATE_STREAMING &&
		 MyWalSnd->state != WALSNDSTATE_STOPPING) ||
		XLogRecPtrIsInvalid(flushPtr))
		return;

	/* 
	 * If am not a high priority walsender, i can do nothing.
	 */
	LWLockAcquire(SyncRepLock, LW_SHARED);
	am_high_pri_walsender = MyWalSnd->is_high_priority_replication_standby;
	mode = WalSndCtl->priority_replication_mode;
	LWLockRelease(SyncRepLock);
	
	if (!am_high_pri_walsender)
		return;

	LWLockAcquire(SyncRepLock, LW_EXCLUSIVE);

	priRepPtr = flushPtr;

	if (mode == POLAR_PRI_REP_ALL)
		polar_priority_replication_get_oldest_rec_ptr(&priRepPtr);
	
	/*
	 * Set the lsn first so that when we wake backends they will release up to
	 * this location.
	 */
	if (MyWalSnd->is_high_priority_replication_standby &&
		walsndctl->lsn[POLAR_PRI_REP_WAIT_FLUSH] < priRepPtr)
	{
		walsndctl->lsn[POLAR_PRI_REP_WAIT_FLUSH] = priRepPtr;
		SyncRepWakeQueue(false, POLAR_PRI_REP_WAIT_FLUSH);
	}

	LWLockRelease(SyncRepLock);
}

/*
 * POLAR: for priority replication, return true if exist high priority
 * replication standbys. 
 * This function *must* be called with SyncRepLock in both shared or
 * exclusive mode.
 */
static bool
polar_exist_high_priority_replication_walsender(int mode)
{
	int		i;
	volatile WalSnd *walsnd;	/* Use volatile pointer to prevent code
								 * rearrangement */
	
	Assert(POLAR_PRI_REP_VALID_MODE(mode));

	for (i = 0; i < max_wal_senders; i++)
	{
		bool	is_high_pri = false;
		WalSndState state;
		int			pid;

		walsnd = &WalSndCtl->walsnds[i];

		SpinLockAcquire(&walsnd->mutex);
		pid = walsnd->pid;
		state = walsnd->state;
		SpinLockRelease(&walsnd->mutex);

		is_high_pri = walsnd->is_high_priority_replication_standby;

		/* Must be active */
		if (pid == 0)
			continue;

		/*no cover begin*/
		/* Must be streaming or stopping */
		if (state != WALSNDSTATE_STREAMING &&
			state != WALSNDSTATE_STOPPING)
			continue;
		/*no cover end*/

		if (mode == POLAR_PRI_REP_WAIT_FLUSH && is_high_pri)
			return true;
	}

	return false;
}

bool 
polar_priority_replication_waiter_need_wait(int mode)
{
	bool	need_wait = false;
	bool	am_low_pri_walsender;

	LWLockAcquire(SyncRepLock, LW_SHARED);
	need_wait = WalSndCtl->priority_replication_force_wait ||
				polar_exist_high_priority_replication_walsender(mode);
	am_low_pri_walsender = MyWalSnd->is_low_priority_replication_standby;
	LWLockRelease(SyncRepLock);
	
	if (!need_wait)
		return false;

	/* 
	 * POLAR: for priority replication, we can't wake up a subset of standby 
	 * that's been removed from polar_low_priority_replication_standby_names. 
	 * so they need to jump out of the waiting loop by themself.
	 */
	if (mode == POLAR_PRI_REP_WAIT_FLUSH)
		return am_low_pri_walsender;

	return true;
}

/*
 * POLAR: priority replication ALL mode, fetch oldest XLogRecPtr for
 * priority replication.
 * This function *must* be called with SyncRepLock in both shared or
 * exclusive mode.
 */ 
static void
polar_priority_replication_get_oldest_rec_ptr(XLogRecPtr *priRepPtr)
{
	int		i;
	volatile WalSnd *walsnd;	/* Use volatile pointer to prevent code
								 * rearrangement */

	for (i = 0; i < max_wal_senders; i++)
	{
		bool	is_high_pri = false;
		WalSndState state;
		int			pid;
		XLogRecPtr	flushPtr;

		walsnd = &WalSndCtl->walsnds[i];

		SpinLockAcquire(&walsnd->mutex);
		pid = walsnd->pid;
		state = walsnd->state;
		flushPtr = walsnd->flush;
		SpinLockRelease(&walsnd->mutex);

		is_high_pri = walsnd->is_high_priority_replication_standby;

		/* Must be active */
		if (pid == 0)
			continue;

		/*no cover begin*/
		/* Must be streaming or stopping */
		if (state != WALSNDSTATE_STREAMING &&
			state != WALSNDSTATE_STOPPING)
			continue;

		if (is_high_pri && flushPtr < *priRepPtr)
			*priRepPtr = flushPtr;
		/*no cover end*/
	}

}

/*
 * POLAR: update priority replication walsender's type.
 * Mark a walsender weather it's a high/low priority replication walsender.
 * This function *must not* called with SyncRepLock.
 */ 
void
polar_priority_replication_update_walsender_type(void)
{
	int		i;
	volatile WalSnd *walsnd;	/* Use volatile pointer to prevent code
								 * rearrangement */

	for (i = 0; i < max_wal_senders; i++)
	{
		WalSndState state;
		int			pid;
		bool		is_high_pri;
		bool		is_low_pri;

		walsnd = &WalSndCtl->walsnds[i];

		SpinLockAcquire(&walsnd->mutex);
		pid = walsnd->pid;
		state = walsnd->state;
		SpinLockRelease(&walsnd->mutex);

		/* Must be active */
		if (pid == 0)
			continue;

		/*no cover begin*/
		/* Must be streaming or stopping */
		if (state != WALSNDSTATE_STREAMING &&
			state != WALSNDSTATE_STOPPING)
			continue;
		/*no cover end*/
		
		polar_walsender_parse_attrs(pid, &is_high_pri, &is_low_pri);

		LWLockAcquire(SyncRepLock, LW_EXCLUSIVE);
		walsnd->is_low_priority_replication_standby = is_low_pri;
		walsnd->is_high_priority_replication_standby = is_high_pri;
		LWLockRelease(SyncRepLock);
	}
}

/*
 * POLAR: called by checkpointer, It's safe to check the current value
 * without the lock, because it's only ever updated by one process. But we
 * must take the lock to change it.
 */ 
void
polar_priority_replication_update_priority_replication_force_wait(void)
{
	if (polar_priority_replication_force_wait != WalSndCtl->priority_replication_force_wait)
	{
		LWLockAcquire(SyncRepLock, LW_EXCLUSIVE);

		WalSndCtl->priority_replication_force_wait = polar_priority_replication_force_wait;

		LWLockRelease(SyncRepLock);
	}
}

/*
 * POLAR: called by checkpointer, It's safe to check the current value
 * without the lock, because it's only ever updated by one process. But we
 * must take the lock to change it.
 */ 
void
polar_priority_replication_update_high_priority_replication_standbys_defined(void)
{	
	bool	high_pri_rep_standbys_defined = POLAR_HIGH_PRI_REP_STANDBYS_DEFINED();

	if (high_pri_rep_standbys_defined != WalSndCtl->high_priority_replication_standbys_defined)
	{
		LWLockAcquire(SyncRepLock, LW_EXCLUSIVE);

		if (!high_pri_rep_standbys_defined)
			SyncRepWakeQueue(true, POLAR_PRI_REP_WAIT_FLUSH);

		WalSndCtl->high_priority_replication_standbys_defined = high_pri_rep_standbys_defined;

		LWLockRelease(SyncRepLock);
	}
}

/*
 * POLAR: called by checkpointer, It's safe to check the current value
 * without the lock, because it's only ever updated by one process. But we
 * must take the lock to change it.
 */ 
void
polar_priority_replication_update_low_priority_replication_standbys_defined(void)
{
	bool	low_pri_rep_standbys_defined = POLAR_LOW_PRI_REP_STANDBYS_DEFINED();

	if (low_pri_rep_standbys_defined != WalSndCtl->low_priority_replication_standbys_defined)
	{
		LWLockAcquire(SyncRepLock, LW_EXCLUSIVE);

		/* 
		 * POLAR: We can't wake up a subset that's been removed from low priority,
		 * but we can wake them all up.
		 */
		if (!low_pri_rep_standbys_defined)
			SyncRepWakeQueue(true, POLAR_PRI_REP_WAIT_FLUSH);

		WalSndCtl->low_priority_replication_standbys_defined = low_pri_rep_standbys_defined;

		LWLockRelease(SyncRepLock);
	}
}

/*
 * POLAR: called by checkpointer, It's safe to check the current value
 * without the lock, because it's only ever updated by one process. But we
 * must take the lock to change it.
 */ 
void
polar_priority_replication_update_priority_replication_mode(void)
{
	int			priority_replication_mode = polar_priority_replication_mode;

	if (priority_replication_mode != WalSndCtl->priority_replication_mode)
	{
		LWLockAcquire(SyncRepLock, LW_EXCLUSIVE);

		if (priority_replication_mode == POLAR_PRI_REP_OFF)
			SyncRepWakeQueue(true, POLAR_PRI_REP_WAIT_FLUSH);
	
		WalSndCtl->priority_replication_mode = priority_replication_mode;

		LWLockRelease(SyncRepLock);
	}
}