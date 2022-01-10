/*-------------------------------------------------------------------------
 *
 * polar_logindex_bg_worker.c
 *  
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
 *    src/backend/access/logindex/polar_logindex_bg_worker.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <unistd.h>

#include "access/polar_logindex.h"
#include "access/polar_logindex_redo.h"
#include "libpq/pqsignal.h"
#include "miscadmin.h"
#include "postmaster/bgworker.h"
#include "storage/condition_variable.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/procarray.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/polar_coredump.h"
#include "utils/resowner.h"

/*
 * Flags set by interrupt handlers for later service in the main loop.
 */
static volatile sig_atomic_t got_SIGHUP = false;
static volatile sig_atomic_t shutdown_requested = false;
static volatile sig_atomic_t polar_online_promote_req = false;
/* Signal handlers */


/* SIGHUP: set flag to re-read config file at next convenient time */
static void
bg_sighup_handler(SIGNAL_ARGS)
{
	int         save_errno = errno;

	got_SIGHUP = true;
	SetLatch(MyLatch);

	errno = save_errno;
}

/* SIGTERM: set flag to shutdown and exit */
static void
bg_shutdown_handler(SIGNAL_ARGS)
{
	int         save_errno = errno;

	shutdown_requested = true;
	SetLatch(MyLatch);

	errno = save_errno;
}

/* POLAR: SIGUSR2 : used for online promote */
static void
online_promote_trigger(SIGNAL_ARGS)
{
	int save_errno = errno;

	polar_online_promote_req = true;
	SetLatch(MyLatch);

	errno = save_errno;
}


static void
bgworker_handle_online_promote(polar_logindex_redo_ctl_t instance)
{
	XLogRecPtr bg_lsn;

	/*
	 * Only run polar_logindex_redo_online_promote when promoting replica.
	 *
	 * Standby promoting doesn't need it to change logindex state, which is
	 * originally writable.
	 */
	polar_logindex_redo_online_promote(instance);
	bg_lsn = polar_bg_redo_get_replayed_lsn(instance);

	elog(LOG, "Before online promote bg_replayed_lsn=%lX", bg_lsn);

	polar_set_bg_redo_state(instance, POLAR_BG_WAITING_RESET);

	/* POLAR: Notify startup process that background replay state is changed */
	WakeupRecovery();
}

static void
set_logindex_bg_worker_latch(polar_logindex_redo_ctl_t instance)
{
	Assert(MyLatch);

	instance->bg_worker_latch = MyLatch;
	polar_logindex_set_writer_latch(instance->wal_logindex_snapshot, MyLatch);

	if (instance->fullpage_logindex_snapshot)
		polar_logindex_set_writer_latch(instance->fullpage_logindex_snapshot, MyLatch);
}

static polar_logindex_bg_redo_ctl_t *
create_logindex_bg_redo_ctl(polar_logindex_redo_ctl_t instance)
{
	polar_logindex_bg_redo_ctl_t *bg_redo_ctl = NULL;

	do
	{
		uint32 state;

		if (!polar_need_do_bg_replay(polar_logindex_redo_instance))
			break;

		state = polar_get_bg_redo_state(polar_logindex_redo_instance);

		switch (state)
		{
			case POLAR_BG_RO_BUF_REPLAYING:
			{
				bg_redo_ctl = polar_create_bg_redo_ctl(polar_logindex_redo_instance, false);
				polar_bg_replaying_process = POLAR_LOGINDEX_DISPATCHER;
				break;
			}

			case POLAR_BG_PARALLEL_REPLAYING:
			case POLAR_BG_ONLINE_PROMOTE:
			{
				bg_redo_ctl = polar_create_bg_redo_ctl(polar_logindex_redo_instance, true);
				polar_bg_replaying_process = POLAR_LOGINDEX_DISPATCHER;
				break;
			}

			case POLAR_BG_REDO_NOT_START:
			{
				if (polar_online_promote_req)
					return bg_redo_ctl;

				/* Else run the next case to WaitLatch and then check whether startup set new background redo state */
			}

			case POLAR_BG_WAITING_RESET:
			{
				int rc = WaitLatch(MyLatch,
								   WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
								   100 /* ms */, WAIT_EVENT_LOGINDEX_BG_MAIN);

				if (rc & WL_POSTMASTER_DEATH)
					exit(1);

				ResetLatch(MyLatch);
				break;
			}

			default:
				elog(PANIC, "Got unexpected bg_redo_state=%d", state);
		}

		CHECK_FOR_INTERRUPTS();
	}
	while (bg_redo_ctl == NULL && !shutdown_requested);

	return bg_redo_ctl;
}

void
polar_logindex_bg_worker_main(void)
{
	/* POLAR: Flags check whether finish online promote to avoid receive more than one times of SIGUSR2 */
	bool polar_online_promoting = false;
	polar_logindex_bg_redo_ctl_t *bg_redo_ctl = NULL;
	MemoryContext bgworker_context;

	/*
	 * Properly accept or ignore signals the postmaster might send us.
	 *
	 * bgwriter doesn't participate in ProcSignal signalling, but a SIGUSR1
	 * handler is still needed for latch wakeups.
	 */
	pqsignal(SIGHUP, bg_sighup_handler);    /* set flag to read config file */
	pqsignal(SIGINT, SIG_IGN);
	pqsignal(SIGTERM, bg_shutdown_handler); /* shutdown */
	pqsignal(SIGQUIT, polar_bg_quickdie); /* hard crash time */
	pqsignal(SIGALRM, SIG_IGN);
	pqsignal(SIGPIPE, SIG_IGN);
	pqsignal(SIGUSR1, polar_bg_sigusr1_handler);
	pqsignal(SIGUSR2, online_promote_trigger);

	/*
	 * Reset some signals that are accepted by postmaster but not here
	 */
	pqsignal(SIGCHLD, SIG_DFL);
	pqsignal(SIGTTIN, SIG_DFL);
	pqsignal(SIGTTOU, SIG_DFL);
	pqsignal(SIGCONT, SIG_DFL);
	pqsignal(SIGWINCH, SIG_DFL);

	/* POLAR : register for coredump print */
#ifndef _WIN32
#ifdef SIGILL
	pqsignal(SIGILL, polar_program_error_handler);
#endif
#ifdef SIGSEGV
	pqsignal(SIGSEGV, polar_program_error_handler);
#endif
#ifdef SIGBUS
	pqsignal(SIGBUS, polar_program_error_handler);
#endif
#endif  /* _WIN32 */
	/* POLAR: end */

	/* We allow SIGQUIT (quickdie) at all times */
	sigdelset(&BlockSig, SIGQUIT);

	/*
	 * Create a resource owner to keep track of our resources (currently only
	 * buffer pins).
	 */
	CurrentResourceOwner = ResourceOwnerCreate(NULL, "LogindexBgWorker");

	/*
	 * Create a memory context that we will do all our work in.  We do this so
	 * that we can reset the context during error recovery and thereby avoid
	 * possible memory leaks.  Formerly this code just ran in
	 * TopMemoryContext, but resetting that would be a really bad idea.
	 */
	bgworker_context = AllocSetContextCreate(TopMemoryContext,
											 "LogIndex background worker",
											 ALLOCSET_DEFAULT_SIZES);
	MemoryContextSwitchTo(bgworker_context);

	set_logindex_bg_worker_latch(polar_logindex_redo_instance);

	/*
	 * Unblock signals (they were blocked when the postmaster forked us)
	 */
	PG_SETMASK(&UnBlockSig);

	elog(LOG, "Start logindex background worker");
	bg_redo_ctl = create_logindex_bg_redo_ctl(polar_logindex_redo_instance);

	for (;;)
	{
		bool replay_done = true;
		bool flush_done = true;
		bool can_hold = false;
		long timeout = -1;

		int rc;

		/* Clear any already-pending wakeups */
		ResetLatch(MyLatch);

		if (unlikely(got_SIGHUP))
		{
			got_SIGHUP = false;
			ProcessConfigFile(PGC_SIGHUP);
		}

		if (unlikely(polar_online_promote_req))
		{
			if (!polar_online_promoting)
			{
				/* We release background redo control, and recreate it when startup process reset background replay lsn */
				if (bg_redo_ctl)
				{
					polar_release_bg_redo_ctl(bg_redo_ctl);
					bg_redo_ctl = NULL;
				}

				bgworker_handle_online_promote(polar_logindex_redo_instance);
				polar_online_promoting = true;
			}

			polar_online_promote_req = false;
		}

		if(unlikely(!bg_redo_ctl && polar_bg_redo_state_is_parallel(polar_logindex_redo_instance))) {
			elog(LOG, "Create polar parallel replay process pool");
			bg_redo_ctl = create_logindex_bg_redo_ctl(polar_logindex_redo_instance);
		}

		if (bg_redo_ctl)
		{
			replay_done = polar_logindex_redo_bg_replay(bg_redo_ctl, &can_hold);

			/* Release background redo control when online promote is finished */
			if (unlikely(polar_logindex_bg_promoted(bg_redo_ctl->instance)))
			{
				polar_release_bg_redo_ctl(bg_redo_ctl);
				bg_redo_ctl = NULL;
				if (polar_online_promoting) 
					polar_online_promoting = false;
			}
		}

		/*
		 * POLAR: Flush log index memory table which is full
		 */
		flush_done = polar_logindex_redo_bg_flush_data(polar_logindex_redo_instance);

		if (unlikely(shutdown_requested))
		{
			/* If in rw mode, we exit when finish online promote and flush all inactive logindex table */
			if (polar_in_replica_mode() || replay_done)
			{
				if (bg_redo_ctl)
					polar_release_bg_redo_ctl(bg_redo_ctl);

				/* Exit this process */
				break;
			}
		}

		if (flush_done)
		{
			if (replay_done || can_hold)
				timeout = 10; /* ms */
		}

		if (timeout > 0)
		{
			rc = WaitLatch(MyLatch,
						   WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
						   timeout /* ms */, WAIT_EVENT_LOGINDEX_BG_MAIN);

			/*
			 * Emergency bailout if postmaster has died.  This is to avoid the
			 * necessity for manual cleanup of all postmaster children.
			 */
			if (unlikely(rc & WL_POSTMASTER_DEATH))
				exit(1);
		}
	}

	elog(LOG, "Exit logindex background worker");
	/*
	 * From here on, elog(ERROR) should end with exit(1), not send
	 * control back to the sigsetjmp block above
	 */
	ExitOnAnyError = true;
	proc_exit(0);
}
