/*-------------------------------------------------------------------------
 *
 * polar_parallel_bgwriter.c
 *		Parallel background writer process manager.
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
 *	IDENTIFICATION
 *		src/backend/postmaster/polar_parallel_bgwriter.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <unistd.h>

#include "miscadmin.h"
#include "pgstat.h"

#include "access/xlog.h"
#include "libpq/pqsignal.h"
#include "postmaster/bgworker.h"
#include "postmaster/bgwriter.h"
#include "postmaster/polar_parallel_bgwriter.h"
#include "storage/buf_internals.h"
#include "storage/condition_variable.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/polar_bufmgr.h"
#include "storage/polar_fd.h"
#include "storage/polar_flushlist.h"
#include "storage/procsignal.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/polar_coredump.h"
#include "utils/timestamp.h"

#define HIBERNATE_FACTOR 50
#define ILLEGAL_GENERATION 0
#define ILLEGAL_SLOT -1

static int  check_running_bgwriter_workers(void);
static bool parallel_sync_buffer(WritebackContext *wb_context);
static int  find_one_unused_handle_slot(void);
static int  find_one_used_handle_slot(void);
static void reset_bgwriter_handle(ParallelBgwriterHandle *handle);
static bool handle_is_used(ParallelBgwriterHandle *handle);
static bool handle_is_unused(ParallelBgwriterHandle *handle);

static void worker_sighup_handler(SIGNAL_ARGS);
static void worker_sigterm_handler(SIGNAL_ARGS);
static void worker_sigusr1_handler(SIGNAL_ARGS);
static void worker_quit_handler(SIGNAL_ARGS);

/* Flags set by signal handlers */
static volatile sig_atomic_t got_sighup = false;
static volatile sig_atomic_t shutdown_requested = false;

ParallelBgwriterInfo *polar_parallel_bgwriter_info = NULL;

/*
 * Initialize parallel bgwriter manager.
 *
 * This is called once during shared-memory initialization (either in the
 * postmaster, or in a standalone backend).
 */
void
polar_init_parallel_bgwriter(void)
{
	bool found;
	int  i;

	if (!polar_parallel_bgwriter_enabled())
		return;

	polar_parallel_bgwriter_info = (ParallelBgwriterInfo *) ShmemInitStruct(
		"Parallel bgwriter info",
		sizeof(ParallelBgwriterInfo),
		&found);

	if (!found)
	{
		/* Init background worker handle */
		for (i = 0; i < MAX_NUM_OF_PARALLEL_BGWRITER; i++)
			reset_bgwriter_handle(&polar_parallel_bgwriter_info->handles[i]);
		pg_atomic_init_u32(&polar_parallel_bgwriter_info->current_workers, 0);
	}
}

Size
polar_parallel_bgwriter_shmem_size(void)
{
	Size		size = 0;

	if (!polar_parallel_bgwriter_enabled())
		return size;

	size = add_size(size, sizeof(ParallelBgwriterInfo));

	return size;
}

/* Start parallel background writer workers. */
void
polar_launch_parallel_bgwriter_workers(void)
{
	uint32	current_workers;
	int		running_workers;
	int		launch_workers = 0;
	int		guc_workers;
	bool	update = false;

	/*
	 * Now, only master launch parallel background writers. If we want replica
	 * or standby to launch them, please DO NOT forget to change the worker's
	 * bgw_start_time.
	 */
	if (!polar_parallel_bgwriter_enabled())
		return;

	if ((polar_is_master_in_recovery() &&
		!polar_enable_early_launch_parallel_bgwriter) || polar_in_replica_mode() || polar_is_standby())
		return;

	current_workers = CURRENT_PARALLEL_WORKERS;
	running_workers = check_running_bgwriter_workers();
	guc_workers = polar_parallel_bgwriter_workers;

	Assert(running_workers <= current_workers);

	/* polar_parallel_bgwriter_workers is greater than #current_workers */
	if (current_workers < guc_workers)
	{
		launch_workers = guc_workers - current_workers;
		update = true;
	}
	else if (current_workers > POLAR_MAX_BGWRITER_WORKERS)
	{
		launch_workers = POLAR_MAX_BGWRITER_WORKERS - current_workers;
		elog(LOG,
			 "Current parallel background workers %d is greater than the max %d, try to stop %d worker.",
			 current_workers, POLAR_MAX_BGWRITER_WORKERS, abs(launch_workers));
	}
	/* Some workers are stopped, it is time to start new */
	else if (current_workers > running_workers)
		launch_workers = current_workers - running_workers;

	if (launch_workers > 0)
		polar_register_parallel_bgwriter_workers(launch_workers, update);
	else if (launch_workers < 0)
		polar_shutdown_parallel_bgwriter_workers(abs(launch_workers));
}

static int
check_running_bgwriter_workers(void)
{
	int                    i;
	ParallelBgwriterHandle *handle;
	BgwHandleStatus        status;
	pid_t                  pid;
	int                    workers = 0;

	for(i = 0; i < MAX_NUM_OF_PARALLEL_BGWRITER; i++)
	{
		handle = &polar_parallel_bgwriter_info->handles[i];

		if (handle_is_unused(handle))
			continue;

		status = GetBackgroundWorkerPid((BackgroundWorkerHandle *)handle, &pid);

		/* Already stopped */
		if (status == BGWH_STOPPED)
			reset_bgwriter_handle(handle);
		else
			workers++;
	}

	return workers;
}

/* Register #workers background writer workers. */
void
polar_register_parallel_bgwriter_workers(int workers, bool update_workers)
{
	BackgroundWorker worker;
	uint32           current_workers;
	int              slot;
	int              i;

	Assert(polar_parallel_bgwriter_enabled());

	memset(&worker, 0, sizeof(BackgroundWorker));
	worker.bgw_flags = BGWORKER_SHMEM_ACCESS;
	worker.bgw_start_time = BgWorkerStart_PostmasterStart;
	worker.bgw_restart_time = BGW_NEVER_RESTART;
	sprintf(worker.bgw_library_name, "postgres");
	sprintf(worker.bgw_function_name, "polar_parallel_bgwriter_worker_main");
	snprintf(worker.bgw_name, BGW_MAXLEN, "polar parallel bgwriter");
	snprintf(worker.bgw_type, BGW_MAXLEN, "polar parallel bgwriter");
	worker.bgw_notify_pid = MyProcPid;

	current_workers = CURRENT_PARALLEL_WORKERS;

	/* Start workers */
	for (i = 0; i < workers; i++)
	{
		BackgroundWorkerHandle *worker_handle;

		/* Check the total number of parallel background workers. */
		if (current_workers >= POLAR_MAX_BGWRITER_WORKERS)
		{
			ereport(WARNING,
					(errmsg(
						"Can not start new parallel background workers, current workers: %d, at most workers %d",
						current_workers,
						POLAR_MAX_BGWRITER_WORKERS)));
			break;
		}

		/* Find an empty handle slot to register one background writer. */
		slot = find_one_unused_handle_slot();
		if (slot == -1)
		{
			ereport(WARNING,
					(errmsg(
						"Can not find an empty background worker handle slot to register a worker, total number of workers is %d",
						current_workers)));
			break;
		}

		if (RegisterDynamicBackgroundWorker(&worker, &worker_handle))
		{
			polar_set_parallel_bgwriter_handle(worker_handle,
											   &polar_parallel_bgwriter_info->handles[slot]);

			current_workers++;
			pfree(worker_handle);
		}
		else
		{
			ereport(WARNING,
					(errcode(ERRCODE_INSUFFICIENT_RESOURCES),
						errmsg("could not register background process"),
						errhint("You may need to increase max_worker_processes.")));
			continue;
		}

		if (polar_enable_debug)
			ereport(LOG,
					(errmsg(
						"Register one background writer worker, its handle slot index is %d",
						slot)));
	}

	/* Set current parallel workers */
	if (update_workers)
		pg_atomic_write_u32(&polar_parallel_bgwriter_info->current_workers, current_workers);
}

/*
 * polar_parallel_bgwriter_worker_main - main entry point for the parallel
 * bgwriter worker.
 *
 * Based on BackgroundWriterMain.
 */
void
polar_parallel_bgwriter_worker_main(Datum main_arg)
{
	sigjmp_buf	local_sigjmp_buf;
	MemoryContext bgwriter_context;
	bool		prev_hibernate;
	WritebackContext wb_context;

	/*
	 * Properly accept or ignore signals the postmaster might send us.
	 *
	 * bgwriter doesn't participate in ProcSignal signalling, but a SIGUSR1
	 * handler is still needed for latch wakeups.
	 */
	pqsignal(SIGHUP, worker_sighup_handler);	/* set flag to read config file */
	pqsignal(SIGINT, SIG_IGN);
	pqsignal(SIGTERM, worker_sigterm_handler);	/* shutdown */
	pqsignal(SIGQUIT, worker_quit_handler); /* hard crash time */
	pqsignal(SIGALRM, SIG_IGN);
	pqsignal(SIGPIPE, SIG_IGN);
	pqsignal(SIGUSR1, worker_sigusr1_handler);
	pqsignal(SIGUSR2, SIG_IGN);

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
#endif	/* _WIN32 */
	/* POLAR: end */

	/* We allow SIGQUIT (quickdie) at all times */
	sigdelset(&BlockSig, SIGQUIT);

	/* We are also bgwriter, for stat use */
	MyAuxProcType = BgWriterProcess;

	/*
	 * Create a resource owner to keep track of our resources (currently only
	 * buffer pins).
	 */
	CurrentResourceOwner = ResourceOwnerCreate(NULL, "Parallel Background Writer");

	/* Add bg writer into backends for showing them in pg_stat_activity */
	polar_init_dynamic_bgworker_in_backends();

	/*
	 * Create a memory context that we will do all our work in.  We do this so
	 * that we can reset the context during error recovery and thereby avoid
	 * possible memory leaks.  Formerly this code just ran in
	 * TopMemoryContext, but resetting that would be a really bad idea.
	 */
	bgwriter_context = AllocSetContextCreate(TopMemoryContext,
											 "Parallel Background Writer",
											 ALLOCSET_DEFAULT_SIZES);
	MemoryContextSwitchTo(bgwriter_context);

	WritebackContextInit(&wb_context, &bgwriter_flush_after);

	/*
	 * If an exception is encountered, processing resumes here.
	 *
	 * See notes in postgres.c about the design of this coding.
	 */
	if (sigsetjmp(local_sigjmp_buf, 1) != 0)
	{
		/* Since not using PG_TRY, must reset error stack by hand */
		error_context_stack = NULL;

		/* Prevent interrupts while cleaning up */
		HOLD_INTERRUPTS();

		/* Report the error to the server log */
		EmitErrorReport();

		/*
		 * These operations are really just a minimal subset of
		 * AbortTransaction().  We don't have very many resources to worry
		 * about in bgwriter, but we do have LWLocks, buffers, and temp files.
		 */
		LWLockReleaseAll();
		ConditionVariableCancelSleep();
		AbortBufferIO();
		UnlockBuffers();
		/* buffer pins are released here: */
		ResourceOwnerRelease(CurrentResourceOwner,
							 RESOURCE_RELEASE_BEFORE_LOCKS,
							 false, true);
		/* we needn't bother with the other ResourceOwnerRelease phases */
		AtEOXact_Buffers(false);
		AtEOXact_SMgr();
		AtEOXact_Files(false);
		AtEOXact_HashTables(false);

		/*
		 * Now return to normal top-level context and clear ErrorContext for
		 * next time.
		 */
		MemoryContextSwitchTo(bgwriter_context);
		FlushErrorState();

		/* Flush any leaked data in the top-level context */
		MemoryContextResetAndDeleteChildren(bgwriter_context);

		/* re-initialize to avoid repeated errors causing problems */
		WritebackContextInit(&wb_context, &bgwriter_flush_after);

		/* Now we can allow interrupts again */
		RESUME_INTERRUPTS();

		/*
		 * Sleep at least 1 second after any error.  A write error is likely
		 * to be repeated, and we don't want to be filling the error logs as
		 * fast as we can.
		 */
		pg_usleep(1000000L);

		/*
		 * Close all open files after any error.  This is helpful on Windows,
		 * where holding deleted files open causes various strange errors.
		 * It's not clear we need it elsewhere, but shouldn't hurt.
		 */
		smgrcloseall();

		/* Report wait end here, when there is no further possibility of wait */
		pgstat_report_wait_end();
	}

	/* We can now handle ereport(ERROR) */
	PG_exception_stack = &local_sigjmp_buf;

	/*
	 * Unblock signals (they were blocked when the postmaster forked us)
	 */
	PG_SETMASK(&UnBlockSig);

	/*
	 * Reset hibernation state after any error.
	 */
	prev_hibernate = false;

	/*
	 * Loop forever
	 */
	for (;;)
	{
		bool		can_hibernate;
		int			rc;

		/* Clear any already-pending wakeups */
		ResetLatch(MyLatch);

		if (got_sighup)
		{
			got_sighup = false;
			ProcessConfigFile(PGC_SIGHUP);
		}
		if (shutdown_requested)
		{
			/*
			 * From here on, elog(ERROR) should end with exit(1), not send
			 * control back to the sigsetjmp block above
			 */
			ExitOnAnyError = true;
			/* Normal exit from the bgwriter is here */
			proc_exit(0);		/* done */
		}

		/*
		 * Do one cycle of dirty-buffer writing.
		 */
		can_hibernate = parallel_sync_buffer(&wb_context);

		/*
		 * Send off activity statistics to the stats collector
		 */
		pgstat_send_bgwriter();

		if (FirstCallSinceLastCheckpoint())
		{
			/*
			 * After any checkpoint, close all smgr files.  This is so we
			 * won't hang onto smgr references to deleted files indefinitely.
			 */
			smgrcloseall();
		}

		/*
		 * Sleep until we are signaled or BgWriterDelay has elapsed.
		 */
		rc = WaitLatch(MyLatch,
					   WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
					   polar_parallel_bgwriter_delay /* ms */ ,
					   WAIT_EVENT_BGWRITER_MAIN);

		if (rc == WL_TIMEOUT && can_hibernate && prev_hibernate)
			rc = WaitLatch(MyLatch,
						   WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
						   polar_parallel_bgwriter_delay * HIBERNATE_FACTOR,
						   WAIT_EVENT_BGWRITER_HIBERNATE);

		/*
		 * Emergency bailout if postmaster has died.  This is to avoid the
		 * necessity for manual cleanup of all postmaster children.
		 */
		if (rc & WL_POSTMASTER_DEATH)
			exit(1);

		prev_hibernate = can_hibernate;
	}
}

/*
 * Parallel background writer only flush buffer from flush list, do not flush
 * buffer from copy buffer and do not update the consistent lsn, only the normal
 * bgwriter does these.
 */
static bool
parallel_sync_buffer(WritebackContext *wb_context)
{
	XLogRecPtr	consistent_lsn = InvalidXLogRecPtr;
	return polar_buffer_sync(wb_context, &consistent_lsn, false, 0);
}

/* Shut down #workers parallel background workers. */
void
polar_shutdown_parallel_bgwriter_workers(int workers)
{
	int             used_slot;
	int             i;
	ParallelBgwriterHandle *handle;

	Assert(polar_parallel_bgwriter_enabled());

	for (i = 0; i < workers; i++)
	{
		used_slot = find_one_used_handle_slot();
		if (used_slot == -1)
		{
			ereport(WARNING,
					(errmsg(
						"Can not find a used background worker handle slot to shutdown a worker, total number of workers is %d",
						CURRENT_PARALLEL_WORKERS)));
			break;
		}

		handle = &polar_parallel_bgwriter_info->handles[used_slot];

		TerminateBackgroundWorker((BackgroundWorkerHandle *) handle);

		reset_bgwriter_handle(handle);
		pg_atomic_fetch_sub_u32(&polar_parallel_bgwriter_info->current_workers, 1);

		if (polar_enable_debug)
		{
			ereport(DEBUG1,
					(errmsg(
						"Shut down one background writer worker, its handle slot index is %d",
						used_slot)));
		}
	}
}

/* Check the parallel background writers, if some writers are stopped, start some. */
void
polar_check_parallel_bgwriter_worker(void)
{
	static TimestampTz last_check_ts = 0;

	TimestampTz timeout = 0;
	TimestampTz now;

	if (!polar_parallel_bgwriter_enabled() ||
		polar_in_replica_mode())
		return;

	if (last_check_ts == 0)
	{
		last_check_ts = GetCurrentTimestamp();
		return;
	}

	now = GetCurrentTimestamp();
	timeout = TimestampTzPlusMilliseconds(last_check_ts,
										  polar_parallel_bgwriter_check_interval * 1000L);
	if (now >= timeout)
	{
		polar_launch_parallel_bgwriter_workers();
		last_check_ts = now;
	}
}

static int
find_one_unused_handle_slot(void)
{
	int i;

	for (i = 0; i < MAX_NUM_OF_PARALLEL_BGWRITER; i++)
	{
		if (handle_is_unused(&polar_parallel_bgwriter_info->handles[i]))
			return i;
	}

	/* Return an illegal value. */
	return -1;
}

static int
find_one_used_handle_slot(void)
{
	int i;

	for (i = 0; i < MAX_NUM_OF_PARALLEL_BGWRITER; i++)
	{
		if (handle_is_used(&polar_parallel_bgwriter_info->handles[i]))
			return i;
	}

	/* Return an illegal value. */
	return -1;
}

static bool
handle_is_unused(ParallelBgwriterHandle *handle)
{
	bool res = handle->slot == ILLEGAL_SLOT ||
		handle->generation == ILLEGAL_GENERATION;

	if (res)
	{
		Assert(handle->slot == ILLEGAL_SLOT);
		Assert(handle->generation == ILLEGAL_GENERATION);
	}

	return res;
}

static bool
handle_is_used(ParallelBgwriterHandle *handle)
{
	return handle->slot != ILLEGAL_SLOT &&
		handle->generation != ILLEGAL_GENERATION;
}

static void
reset_bgwriter_handle(ParallelBgwriterHandle *handle)
{
	handle->slot = ILLEGAL_SLOT;
	handle->generation = ILLEGAL_GENERATION;
}

/* Signal handler for SIGTERM */
static void
worker_sigterm_handler(SIGNAL_ARGS)
{
	int			save_errno = errno;

	shutdown_requested = true;
	SetLatch(MyLatch);

	errno = save_errno;
}

/* Signal handler for SIGHUP */
static void
worker_sighup_handler(SIGNAL_ARGS)
{
	int			save_errno = errno;

	got_sighup = true;
	SetLatch(MyLatch);

	errno = save_errno;
}

/* SIGUSR1: used for latch wakeups */
static void
worker_sigusr1_handler(SIGNAL_ARGS)
{
	int			save_errno = errno;

	latch_sigusr1_handler();

	errno = save_errno;
}

static void
worker_quit_handler(SIGNAL_ARGS)
{
	_exit(2);
}
