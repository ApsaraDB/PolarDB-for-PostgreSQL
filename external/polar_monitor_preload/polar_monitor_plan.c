/*-------------------------------------------------------------------------
 *
 * polar_monitor_plan.c
 *	  Routines for interprocess signalling
 *
 *
 * Portions Copyright (c) 2021, Alibaba Group Holding Limited
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  external/polar_monitor_preload/polar_monitor_plan.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <signal.h>
#include <unistd.h>
#include "commands/async.h"
#include "miscadmin.h"
#include "storage/proc.h"
#include "storage/procarray.h"
#include "tcop/pquery.h"
#include "commands/explain.h"
#include "polar_monitor_plan.h"

/*
 * polar_log_current_plan
 *		Signal a backend process to log the plan of running
 *		query.
 *
 * Only superusers are allowed to signal to log the plan because
 * allowing any users to issue this request at an unbounded rate
 * would cause lots of log messages and which can lead to denial
 * of service.
 *
 * On receipt of this signal, a backend sets the flag in the signal
 * handler, which causes the next CHECK_FOR_INTERRUPTS() to log the
 * plan.
 */
PG_FUNCTION_INFO_V1(polar_log_current_plan);
Datum
polar_log_current_plan(PG_FUNCTION_ARGS)
{
	int			pid = PG_GETARG_INT32(0);
	PGPROC	   *proc = BackendPidGetProc(pid);

	/*
	 * BackendPidGetProc returns NULL if the pid isn't valid; but by the time
	 * we reach kill(), a process for which we get a valid proc here might
	 * have terminated on its own.  There's no way to acquire a lock on an
	 * arbitrary process to prevent that. But since this mechanism is usually
	 * used to look into the plan of long running query, that it might end on
	 * its own first and its plan is not logged is not a problem.
	 */
	if (proc == NULL)
	{
		/*
		 * This is just a warning so a loop-through-resultset will not abort
		 * if one backend terminated on its own during the run.
		 */
		ereport(WARNING,
				(errmsg("PID %d is not a PostgreSQL server process", pid)));
		PG_RETURN_BOOL(false);
	}

	/* Only allow superusers to log the plan of running query. */
	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("must be a superuser to log the plan of running query")));

	if (SendProcSignal(pid, PROCSIG_LOG_CURRENT_PLAN, proc->backendId) < 0)
	{
		/* Again, just a warning to allow loops */
		ereport(WARNING,
				(errmsg("could not send signal to process %d: %m", pid)));
		PG_RETURN_BOOL(false);
	}

	PG_RETURN_BOOL(true);
}

/*
 * HandleLogCurrentPlanInterrupt
 *		Handle receipt of an interrupt indicating logging the
 *		plan of a running query.
 *
 * All the actual work is deferred to ProcessLogExplainInterrupt(),
 * because we cannot safely emit a log message inside the signal handler.
 */
void
HandleLogCurrentPlanInterrupt(void)
{
	InterruptPending = true;
	LogCurrentPlanPending = true;
	/* latch will be set by procsignal_sigusr1_handler */
}

/*
 * ProcessLogCurrentPlanInterrupt
 * 		Perform logging the plan of running query on this
 * 		backend process.
 *
 * Any backend that participates in ProcSignal signaling must arrange
 * to call this function if we see LogCurrentPlanPending set.
 * It is called from CHECK_FOR_INTERRUPTS(), which is enough because
 * the target process for logging the plan of running query.
 */
void
ProcessLogCurrentPlanInterrupt(void)
{
	ExplainState *es = NewExplainState();

	LogCurrentPlanPending = false;

	es->format = EXPLAIN_FORMAT_TEXT;
	es->summary = true;

	if (ActivePortal && ActivePortal->queryDesc != NULL)
	{
		ExplainQueryText(es, ActivePortal->queryDesc);
		ExplainPrintPlan(es, ActivePortal->queryDesc);

		/* Remove last line break */
		if (es->str->len > 0 && es->str->data[es->str->len - 1] == '\n')
			es->str->data[--es->str->len] = '\0';

		ereport(LOG,
				(errmsg("logging the plan of running query on PID %d\n%s",
					MyProcPid,
					es->str->data),
				 errhidestmt(true)));
	}
	else
		ereport(LOG,
				(errmsg("PID %d is not running a query now",
					MyProcPid)));
}
