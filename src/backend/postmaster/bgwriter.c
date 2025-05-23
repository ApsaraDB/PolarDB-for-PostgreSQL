/*-------------------------------------------------------------------------
 *
 * bgwriter.c
 *
 * The background writer (bgwriter) is new as of Postgres 8.0.  It attempts
 * to keep regular backends from having to write out dirty shared buffers
 * (which they would only do when needing to free a shared buffer to read in
 * another page).  In the best scenario all writes from shared buffers will
 * be issued by the background writer process.  However, regular backends are
 * still empowered to issue writes if the bgwriter fails to maintain enough
 * clean shared buffers.
 *
 * As of Postgres 9.2 the bgwriter no longer handles checkpoints.
 *
 * Normal termination is by SIGTERM, which instructs the bgwriter to exit(0).
 * Emergency termination is by SIGQUIT; like any backend, the bgwriter will
 * simply abort and exit on SIGQUIT.
 *
 * If the bgwriter exits unexpectedly, the postmaster treats that the same
 * as a backend crash: shared memory may be corrupted, so remaining backends
 * should be killed by SIGQUIT and then a recovery cycle started.
 *
 *
 * Portions Copyright (c) 2024, Alibaba Group Holding Limited
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 *
 *
 * IDENTIFICATION
 *	  src/backend/postmaster/bgwriter.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

/* POLAR */
#include <math.h>

#include "access/xlog.h"
#include "access/xlog_internal.h"
#include "libpq/pqsignal.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "postmaster/bgwriter.h"
#include "postmaster/interrupt.h"
#include "storage/buf_internals.h"
#include "storage/bufmgr.h"
#include "storage/condition_variable.h"
#include "storage/fd.h"
#include "storage/ipc.h"
#include "storage/lwlock.h"
#include "storage/proc.h"
#include "storage/procsignal.h"
#include "storage/shmem.h"
#include "storage/smgr.h"
#include "storage/spin.h"
#include "storage/standby.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/resowner.h"
#include "utils/timestamp.h"
#include "utils/timeout.h"

/* POLAR */
#include "postmaster/polar_parallel_bgwriter.h"
#include "storage/polar_bufmgr.h"
#include "storage/polar_fd.h"
#include "storage/polar_flush.h"

/*
 * GUC parameters
 */
int			BgWriterDelay = 200;

/*
 * Multiplier to apply to BgWriterDelay when we decide to hibernate.
 * (Perhaps this needs to be configurable?)
 */
#define HIBERNATE_FACTOR			50

/*
 * Interval in which standby snapshots are logged into the WAL stream, in
 * milliseconds.
 */
#define LOG_SNAPSHOT_INTERVAL_MS 15000

/*
 * LSN and timestamp at which we last issued a LogStandbySnapshot(), to avoid
 * doing so too often or repeatedly if there has been no other write activity
 * in the system.
 */
static TimestampTz last_snapshot_ts;
static XLogRecPtr last_snapshot_lsn = InvalidXLogRecPtr;

static bool polar_flush_dispatcher(void);
static bool polar_flush_dispatch_task(XLogRecPtr consistent_lag, int lru_ahead_lap);

/*
 * Main entry point for bgwriter process
 *
 * This is invoked from AuxiliaryProcessMain, which has already created the
 * basic execution environment, but not enabled signals yet.
 */
void
BackgroundWriterMain(void)
{
	sigjmp_buf	local_sigjmp_buf;
	MemoryContext bgwriter_context;
	bool		prev_hibernate;
	WritebackContext wb_context;

	/*
	 * Properly accept or ignore signals that might be sent to us.
	 */
	pqsignal(SIGHUP, SignalHandlerForConfigReload);
	pqsignal(SIGINT, SIG_IGN);
	pqsignal(SIGTERM, SignalHandlerForShutdownRequest);
	/* SIGQUIT handler was already set up by InitPostmasterChild */
	pqsignal(SIGALRM, SIG_IGN);
	pqsignal(SIGPIPE, SIG_IGN);
	pqsignal(SIGUSR1, procsignal_sigusr1_handler);
	pqsignal(SIGUSR2, SIG_IGN);

	/*
	 * Reset some signals that are accepted by postmaster but not here
	 */
	pqsignal(SIGCHLD, SIG_DFL);

	/*
	 * POLAR: Establishes SIGALRM handler and initialize parameters to
	 * facilitate the running of scheduled tasks. Some scheduled tasks will
	 * cause assertion errors when parameters are not initialized.
	 */
	InitializeTimeouts();

	/*
	 * We just started, assume there has been either a shutdown or
	 * end-of-recovery snapshot.
	 */
	last_snapshot_ts = GetCurrentTimestamp();

	/*
	 * Create a memory context that we will do all our work in.  We do this so
	 * that we can reset the context during error recovery and thereby avoid
	 * possible memory leaks.  Formerly this code just ran in
	 * TopMemoryContext, but resetting that would be a really bad idea.
	 */
	bgwriter_context = AllocSetContextCreate(TopMemoryContext,
											 "Background Writer",
											 ALLOCSET_DEFAULT_SIZES);
	MemoryContextSwitchTo(bgwriter_context);

	WritebackContextInit(&wb_context, &bgwriter_flush_after);

	/*
	 * If an exception is encountered, processing resumes here.
	 *
	 * You might wonder why this isn't coded as an infinite loop around a
	 * PG_TRY construct.  The reason is that this is the bottom of the
	 * exception stack, and so with PG_TRY there would be no exception handler
	 * in force at all during the CATCH part.  By leaving the outermost setjmp
	 * always active, we have at least some chance of recovering from an error
	 * during error recovery.  (If we get into an infinite loop thereby, it
	 * will soon be stopped by overflow of elog.c's internal state stack.)
	 *
	 * Note that we use sigsetjmp(..., 1), so that the prevailing signal mask
	 * (to wit, BlockSig) will be restored when longjmp'ing to here.  Thus,
	 * signals other than SIGQUIT will be blocked until we complete error
	 * recovery.  It might seem that this policy makes the HOLD_INTERRUPTS()
	 * call redundant, but it is not since InterruptPending might be set
	 * already.
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
		UnlockBuffers();
		ReleaseAuxProcessResources(false);
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

		/* Report wait end here, when there is no further possibility of wait */
		pgstat_report_wait_end();
	}

	/* We can now handle ereport(ERROR) */
	PG_exception_stack = &local_sigjmp_buf;

	/*
	 * Unblock signals (they were blocked when the postmaster forked us)
	 */
	PG_SETMASK(&UnBlockSig);

	/* POLAR: start parallel background writer workers. */
	polar_launch_parallel_bgwriter_workers();

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

		HandleMainLoopInterrupts();

		if (!polar_enable_flush_dispatcher ||
			polar_normal_buffer_sync_enabled() ||
			polar_is_standby())
		{
			/*
			 * Do one cycle of dirty-buffer writing.
			 */
			can_hibernate = polar_bg_buffer_sync(&wb_context, 0);
		}
		else
		{
			/* POLAR: dispatcher flush task */
			can_hibernate = polar_flush_dispatcher();
		}

		/* Report pending statistics to the cumulative stats system */
		pgstat_report_bgwriter();
		pgstat_report_wal(true);

		if (FirstCallSinceLastCheckpoint())
		{
			/*
			 * After any checkpoint, free all smgr objects.  Otherwise we
			 * would never do so for dropped relations, as the bgwriter does
			 * not process shared invalidation messages or call
			 * AtEOXact_SMgr().
			 */
			smgrdestroyall();
		}

		/*
		 * Log a new xl_running_xacts every now and then so replication can
		 * get into a consistent state faster (think of suboverflowed
		 * snapshots) and clean up resources (locks, KnownXids*) more
		 * frequently. The costs of this are relatively low, so doing it 4
		 * times (LOG_SNAPSHOT_INTERVAL_MS) a minute seems fine.
		 *
		 * We assume the interval for writing xl_running_xacts is
		 * significantly bigger than BgWriterDelay, so we don't complicate the
		 * overall timeout handling but just assume we're going to get called
		 * often enough even if hibernation mode is active. It's not that
		 * important that LOG_SNAPSHOT_INTERVAL_MS is met strictly. To make
		 * sure we're not waking the disk up unnecessarily on an idle system
		 * we check whether there has been any WAL inserted since the last
		 * time we've logged a running xacts.
		 *
		 * We do this logging in the bgwriter as it is the only process that
		 * is run regularly and returns to its mainloop all the time. E.g.
		 * Checkpointer, when active, is barely ever in its mainloop and thus
		 * makes it hard to log regularly.
		 */
		if (XLogStandbyInfoActive() && !RecoveryInProgress())
		{
			TimestampTz timeout = 0;
			TimestampTz now = GetCurrentTimestamp();

			timeout = TimestampTzPlusMilliseconds(last_snapshot_ts,
												  LOG_SNAPSHOT_INTERVAL_MS);

			/*
			 * Only log if enough time has passed and interesting records have
			 * been inserted since the last snapshot.  Have to compare with <=
			 * instead of < because GetLastImportantRecPtr() points at the
			 * start of a record, whereas last_snapshot_lsn points just past
			 * the end of the record.
			 */
			if (now >= timeout &&
				last_snapshot_lsn <= GetLastImportantRecPtr())
			{
				last_snapshot_lsn = LogStandbySnapshot();
				last_snapshot_ts = now;
			}
		}

		/*
		 * POLAR: Check the parallel background writers, if some writers are
		 * stopped, start some.
		 */
		polar_check_parallel_bgwriter_worker();

		/*
		 * Sleep until we are signaled or BgWriterDelay has elapsed.
		 *
		 * Note: the feedback control loop in BgBufferSync() expects that we
		 * will call it every BgWriterDelay msec.  While it's not critical for
		 * correctness that that be exact, the feedback loop might misbehave
		 * if we stray too far from that.  Hence, avoid loading this process
		 * down with latch events that are likely to happen frequently during
		 * normal operation.
		 */
		rc = WaitLatch(MyLatch,
					   WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
					   BgWriterDelay /* ms */ , WAIT_EVENT_BGWRITER_MAIN);

		/*
		 * If no latch event and BgBufferSync says nothing's happening, extend
		 * the sleep in "hibernation" mode, where we sleep for much longer
		 * than bgwriter_delay says.  Fewer wakeups save electricity.  When a
		 * backend starts using buffers again, it will wake us up by setting
		 * our latch.  Because the extra sleep will persist only as long as no
		 * buffer allocations happen, this should not distort the behavior of
		 * BgBufferSync's control loop too badly; essentially, it will think
		 * that the system-wide idle interval didn't exist.
		 *
		 * There is a race condition here, in that a backend might allocate a
		 * buffer between the time BgBufferSync saw the alloc count as zero
		 * and the time we call StrategyNotifyBgWriter.  While it's not
		 * critical that we not hibernate anyway, we try to reduce the odds of
		 * that by only hibernating when BgBufferSync says nothing's happening
		 * for two consecutive cycles.  Also, we mitigate any possible
		 * consequences of a missed wakeup by not hibernating forever.
		 */
		if (rc == WL_TIMEOUT && can_hibernate && prev_hibernate)
		{
			/* Ask for notification at next buffer allocation */
			StrategyNotifyBgWriter(MyProc->pgprocno);
			/* Sleep ... */
			(void) WaitLatch(MyLatch,
							 WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
							 BgWriterDelay * HIBERNATE_FACTOR,
							 WAIT_EVENT_BGWRITER_HIBERNATE);
			/* Reset the notification request in case we timed out */
			StrategyNotifyBgWriter(-1);
		}

		prev_hibernate = can_hibernate;
	}
}

static bool
polar_flush_dispatcher(void)
{
	bool		res = false;
	XLogRecPtr	consistent_lag;
	int			lru_ahead_lap = 0;

	consistent_lag = polar_calculate_consistent_lsn_lag();
	lru_ahead_lap = polar_calculate_lru_lap();

	res = polar_flush_dispatch_task(consistent_lag, lru_ahead_lap);

	/*
	 * Normal background writer will create or close some parallel background
	 * workers.
	 */
	polar_adjust_parallel_bgwriters(consistent_lag, lru_ahead_lap);

	return res;
}

static bool
polar_flush_add_type_task(int flush_type, int flush_task)
{
	int			write_worker_idx,
				read_worker_idx;
	int			pre_flush_task = flush_task;
	int			task_count = 0;
	bool		success = true;

	SpinLockAcquire(&polar_parallel_bgwriter_info->lock);

	read_worker_idx = polar_parallel_bgwriter_info->read_worker_idx;
	write_worker_idx = polar_parallel_bgwriter_info->write_worker_idx;

	while (flush_task > 0)
	{
		if ((write_worker_idx + 1) % MAX_NUM_OF_PARALLEL_BGWRITER == read_worker_idx)
			break;

		polar_parallel_bgwriter_info->flush_task[write_worker_idx] = flush_type;
		flush_task--;
		task_count++;

		if (++write_worker_idx >= MAX_NUM_OF_PARALLEL_BGWRITER)
			write_worker_idx = 0;
	}

	polar_parallel_bgwriter_info->write_worker_idx = write_worker_idx;

	SpinLockRelease(&polar_parallel_bgwriter_info->lock);

	if (pre_flush_task > 0 && task_count <= 0)
		success = false;

	return success;
}

static bool
polar_task_been_consumed(void)
{
	int			read_worker_idx,
				write_worker_idx;
	int			current_works;
	int			idle_workers;
	uint32		flushlist_workers,
				lru_workers;

	SpinLockAcquire(&polar_parallel_bgwriter_info->lock);
	read_worker_idx = polar_parallel_bgwriter_info->read_worker_idx;
	write_worker_idx = polar_parallel_bgwriter_info->write_worker_idx;

	if (read_worker_idx != write_worker_idx)
	{
		SpinLockRelease(&polar_parallel_bgwriter_info->lock);
		return false;
	}
	SpinLockRelease(&polar_parallel_bgwriter_info->lock);

	current_works = CURRENT_PARALLEL_WORKERS;
	flushlist_workers = pg_atomic_read_u32(&polar_parallel_bgwriter_info->flush_workers[FLUSHLIST_TASK]);
	lru_workers = pg_atomic_read_u32(&polar_parallel_bgwriter_info->flush_workers[FLUSHLRU_TASK]);

	/* idle_works */
	idle_workers = current_works - (flushlist_workers + lru_workers);

	if (idle_workers <= 0)
		return false;

	return true;
}

static bool
polar_flush_generate_task(XLogRecPtr consistent_lag, int lru_ahead_lap, int *flush_task)
{
	uint32		flush_works[MAX_FLUSH_TASK] = {0};
	int			idle_workers;

	int			lru_sleep_lap;
	XLogRecPtr	consistent_sleep_lag;
	XLogRecPtr	threshold_lag;
	int			current_works;

	int			lru_limit,
				flushlist_limit;
	bool		flushlist_behind,
				lru_behind;

	bool		lru_no_task;

	current_works = CURRENT_PARALLEL_WORKERS;
	flush_works[FLUSHLIST_TASK] = pg_atomic_read_u32(&polar_parallel_bgwriter_info->flush_workers[FLUSHLIST_TASK]);
	flush_works[FLUSHLRU_TASK] = pg_atomic_read_u32(&polar_parallel_bgwriter_info->flush_workers[FLUSHLRU_TASK]);

	idle_workers = current_works - (flush_works[FLUSHLIST_TASK] + flush_works[FLUSHLRU_TASK]);

	threshold_lag = polar_parallel_new_bgwriter_threshold_lag * 1024 * 1024L;

	flushlist_behind = consistent_lag >= threshold_lag;
	lru_behind = lru_ahead_lap == LRU_BUFFER_BEHIND;

	if ((flushlist_behind && lru_behind) ||
		(!flushlist_behind && !lru_behind))
	{
		/*
		 * flushlist lag delay more than threshold_lag and lru flush behind
		 * buffer alloc, or flushlist lag delay less than threshold_lag and
		 * lru flush ahead buffer alloc
		 */
		flush_task[FLUSHLRU_TASK] = ceil((double) idle_workers / 2);
		flush_task[FLUSHLIST_TASK] = ceil((double) idle_workers / 2);
	}
	else if (flushlist_behind && !lru_behind)
	{
		/*
		 * flushlist lag delay more than threshold_lag and lru flush ahead
		 * buffer alloc
		 */
		flush_task[FLUSHLRU_TASK] = 1;
		flush_task[FLUSHLIST_TASK] = idle_workers;
	}
	else if (lru_behind && !flushlist_behind)
	{
		/*
		 * flushlist lag delay less than threshold_lag and lru flush behind
		 * buffer alloc
		 */
		flush_task[FLUSHLRU_TASK] = idle_workers;
		flush_task[FLUSHLIST_TASK] = 1;
	}

	lru_limit = (int) (current_works * polar_lru_works_threshold);
	flushlist_limit = ceil((double) current_works * (1 - polar_lru_works_threshold));

	lru_no_task = (lru_ahead_lap == NBuffers || lru_limit <= 0);

	/* lru buffer not alloc or ahead one pass or close lru */
	if (lru_no_task)
		flush_task[FLUSHLRU_TASK] = 0;

	lru_sleep_lap = polar_bgwriter_sleep_lru_lap > (NBuffers / 2) ? (NBuffers / 2) : polar_bgwriter_sleep_lru_lap;
	consistent_sleep_lag = polar_bgwriter_sleep_lsn_lag * 1024 * 1024L;

	/* limit lru */
	if ((flush_works[FLUSHLRU_TASK] + flush_task[FLUSHLRU_TASK]) >= lru_limit)
	{
		flush_task[FLUSHLRU_TASK] = lru_limit - flush_works[FLUSHLRU_TASK] > 0 ? lru_limit - flush_works[FLUSHLRU_TASK] : 0;

		if (idle_workers > flush_task[FLUSHLIST_TASK] && consistent_lag > consistent_sleep_lag)
			flush_task[FLUSHLIST_TASK] = idle_workers;
	}

	/* limit flushlist */
	if (flush_works[FLUSHLIST_TASK] + flush_task[FLUSHLIST_TASK] >= flushlist_limit)
		flush_task[FLUSHLIST_TASK] = flushlist_limit - flush_works[FLUSHLIST_TASK] > 0 ? flushlist_limit - flush_works[FLUSHLIST_TASK] : 0;

	/* lru sleep */
	if (lru_ahead_lap >= lru_sleep_lap && !lru_no_task)
		flush_task[FLUSHLRU_TASK] = flush_works[FLUSHLRU_TASK] >= 1 ? 0 : 1;

	/* flushlist sleep */
	if (consistent_lag <= consistent_sleep_lag)
		flush_task[FLUSHLIST_TASK] = flush_works[FLUSHLIST_TASK] >= 1 ? 0 : 1;

	if (unlikely(polar_enable_lru_log))
		elog(LOG, "lru-flushlist task %d-%d, lru-flushlist works %d-%d",
			 flush_task[FLUSHLRU_TASK], flush_task[FLUSHLIST_TASK],
			 flush_works[FLUSHLRU_TASK], flush_works[FLUSHLIST_TASK]);

	return (lru_ahead_lap >= lru_sleep_lap) && (consistent_lag <= consistent_sleep_lag);
}

static bool
polar_flush_dispatch_task(XLogRecPtr consistent_lag, int lru_ahead_lap)
{
	int			flush_task[MAX_FLUSH_TASK] = {0};
	int			i;
	bool		flush_delay = false;
	parallel_flush_type_t type;

	/*
	 * If the task has not been consumed, it means that the all flush process
	 * is still working.
	 */
	if (!polar_task_been_consumed())
		return flush_delay;

	/* generate task by consistent_lag and lru_ahead_lap */
	flush_delay = polar_flush_generate_task(consistent_lag, lru_ahead_lap, flush_task);

	/* add type task by generate task */
	for (type = FLUSHLIST_TASK; type < MAX_FLUSH_TASK; type++)
	{
		if (!polar_flush_add_type_task(type, flush_task[type]))
			elog(WARNING, "Add flush type %d, task num %d fail", type, flush_task[type]);
	}

	for (i = 0; i < MAX_NUM_OF_PARALLEL_BGWRITER; i++)
	{
		if (polar_parallel_bgwriter_info->polar_flush_work_info[i].latch != NULL)
			SetLatch(polar_parallel_bgwriter_info->polar_flush_work_info[i].latch);
	}

	return flush_delay;
}
