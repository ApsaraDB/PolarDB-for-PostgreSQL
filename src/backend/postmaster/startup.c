/*-------------------------------------------------------------------------
 *
 * startup.c
 *
 * The Startup process initialises the server and performs any recovery
 * actions that have been specified. Notice that there is no "main loop"
 * since the Startup process ends as soon as initialisation is complete.
 * (in standby mode, one can think of the replay loop as a main loop,
 * though.)
 *
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 *
 *
 * IDENTIFICATION
 *	  src/backend/postmaster/startup.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <signal.h>
#include <sys/stat.h>
#include <unistd.h>

#include "access/xlog.h"
#include "libpq/pqsignal.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "postmaster/startup.h"
#include "storage/fd.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/pmsignal.h"
#include "storage/standby.h"
#include "utils/elog.h"
#include "utils/guc.h"
#include "utils/polar_coredump.h"
#include "utils/timeout.h"
#include "replication/walreceiver.h"

/* POLAR */
#include "storage/bufmgr.h"
/* POLAR end */

/* POLAR: keep in sync with PROMOTE_SIGNAL_FILE of xlog.c */
#define PROMOTE_SIGNAL_FILE		"promote"
/* POLAR end */

/* POLAR */
#include "polar_datamax/polar_datamax.h"
/* POLAR end */

/*
 * Flags set by interrupt handlers for later service in the redo loop.
 */
static volatile sig_atomic_t got_SIGHUP = false;
static volatile sig_atomic_t shutdown_requested = false;
static volatile sig_atomic_t promote_triggered = false;

/*
 * Flag set when executing a restore command, to tell SIGTERM signal handler
 * that it's safe to just proc_exit.
 */
static volatile sig_atomic_t in_restore_command = false;

/* Signal handlers */
static void startupproc_quickdie(SIGNAL_ARGS);
static void StartupProcSigUsr1Handler(SIGNAL_ARGS);
static void StartupProcTriggerHandler(SIGNAL_ARGS);
static void StartupProcSigHupHandler(SIGNAL_ARGS);


/* --------------------------------
 *		signal handler routines
 * --------------------------------
 */

/*
 * startupproc_quickdie() occurs when signalled SIGQUIT by the postmaster.
 *
 * Some backend has bought the farm,
 * so we need to stop what we're doing and exit.
 */
static void
startupproc_quickdie(SIGNAL_ARGS)
{
	/*
	 * We DO NOT want to run proc_exit() or atexit() callbacks -- we're here
	 * because shared memory may be corrupted, so we don't want to try to
	 * clean up our transaction.  Just nail the windows shut and get out of
	 * town.  The callbacks wouldn't be safe to run from a signal handler,
	 * anyway.
	 *
	 * Note we do _exit(2) not _exit(0).  This is to force the postmaster into
	 * a system reset cycle if someone sends a manual SIGQUIT to a random
	 * backend.  This is necessary precisely because we don't clean up our
	 * shared memory state.  (The "dead man switch" mechanism in pmsignal.c
	 * should ensure the postmaster sees this as a crash, too, but no harm in
	 * being doubly sure.)
	 */
	_exit(2);
}


/* SIGUSR1: let latch facility handle the signal */
static void
StartupProcSigUsr1Handler(SIGNAL_ARGS)
{
	int			save_errno = errno;

	latch_sigusr1_handler();

	errno = save_errno;
}

/* SIGUSR2: set flag to finish recovery */
static void
StartupProcTriggerHandler(SIGNAL_ARGS)
{
	int			save_errno = errno;
	/* POLAR */
	struct stat stat_buf;
	/* POLAR end */
	
	promote_triggered = true;

	/*
	 * POLAR: notify walrcv promote is triggered 
	 * no need when force promote or in replica mode
	 */
	if (polar_enable_promote_wait_for_walreceive_done 
		&& stat(POLAR_FORCE_PROMOTE_SIGNAL_FILE, &stat_buf) != 0
		&& !polar_in_replica_mode())
	{
		if (WalRcvStreaming())
		{
			elog(LOG,"notify walrcv promote is triggered");
			POLAR_SET_RECEIVE_PROMOTE_TRIGGER();
			POLAR_RESET_PROMOTE_ALLOWED_STATE();
			POLAR_SET_END_LSN_INVALID();
		}
	}
	/* 
	 * POLAR: WakeupRecovery after promote is ready
	 * wakeup now when force promote or in replica mode 
	 */
	else
		WakeupRecovery();
	/* POLAR end */

	errno = save_errno;
}

/* SIGHUP: set flag to re-read config file at next convenient time */
static void
StartupProcSigHupHandler(SIGNAL_ARGS)
{
	int			save_errno = errno;

	got_SIGHUP = true;
	WakeupRecovery();

	errno = save_errno;
}

/* SIGTERM: set flag to abort redo and exit */
static void
StartupProcShutdownHandler(SIGNAL_ARGS)
{
	int			save_errno = errno;

	/* POLAR: set datamax_shutdown flag when in datamax mode */
	if (polar_is_datamax())
		polar_datamax_shutdown_requested = true;
	/* POLAR end */

	if (in_restore_command)
		proc_exit(1);
	else
		shutdown_requested = true;
	WakeupRecovery();

	errno = save_errno;
}

/* Handle SIGHUP and SIGTERM signals of startup process */
void
HandleStartupProcInterrupts(void)
{
	/*
	 * Check if we were requested to re-read config file.
	 */
	if (got_SIGHUP)
	{
		got_SIGHUP = false;
		ProcessConfigFile(PGC_SIGHUP);
	}

	/*
	 * Check if we were requested to exit without finishing recovery.
	 */
	if (shutdown_requested)
		proc_exit(1);

	/*
	 * Emergency bailout if postmaster has died.  This is to avoid the
	 * necessity for manual cleanup of all postmaster children.
	 */
	if (IsUnderPostmaster && !PostmasterIsAlive())
		exit(1);
}


/* ----------------------------------
 *	Startup Process main entry point
 * ----------------------------------
 */
void
StartupProcessMain(void)
{
	/*
	 * Properly accept or ignore signals the postmaster might send us.
	 */
	pqsignal(SIGHUP, StartupProcSigHupHandler); /* reload config file */
	pqsignal(SIGINT, SIG_IGN);	/* ignore query cancel */
	pqsignal(SIGTERM, StartupProcShutdownHandler);	/* request shutdown */
	pqsignal(SIGQUIT, startupproc_quickdie);	/* hard crash time */
	InitializeTimeouts();		/* establishes SIGALRM handler */
	pqsignal(SIGPIPE, SIG_IGN);
	pqsignal(SIGUSR1, StartupProcSigUsr1Handler);
	pqsignal(SIGUSR2, StartupProcTriggerHandler);

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

	/*
	 * Register timeouts needed for standby mode
	 */
	RegisterTimeout(STANDBY_DEADLOCK_TIMEOUT, StandbyDeadLockHandler);
	RegisterTimeout(STANDBY_TIMEOUT, StandbyTimeoutHandler);
	RegisterTimeout(STANDBY_LOCK_TIMEOUT, StandbyLockTimeoutHandler);

	/*
	 * POLAR: Initialize the local directories, copy some directories
	 * from shared storage to local before unblock signals if needed
	 */
	polar_init_local_dir();

	/*
	 * Unblock signals (they were blocked when the postmaster forked us)
	 */
	PG_SETMASK(&UnBlockSig);

	/*
	 * Do what we came for.
	 */
	StartupXLOG();

	/*
	 * Exit normally. Exit code 0 tells postmaster that we completed recovery
	 * successfully.
	 */
	proc_exit(0);
}

void
PreRestoreCommand(void)
{
	/*
	 * Set in_restore_command to tell the signal handler that we should exit
	 * right away on SIGTERM. We know that we're at a safe point to do that.
	 * Check if we had already received the signal, so that we don't miss a
	 * shutdown request received just before this.
	 */
	in_restore_command = true;
	if (shutdown_requested)
		proc_exit(1);
}

void
PostRestoreCommand(void)
{
	in_restore_command = false;
}

bool
IsPromoteTriggered(void)
{
	return promote_triggered;
}

void
ResetPromoteTriggered(void)
{
	promote_triggered = false;
}

/*
 * POLAR: when polar worker receive SIGTERM signal, we need
 * set shutdown_requested flag, so polar worker can exit
 */
void
polar_set_shutdown_requested_flag(void)
{
	shutdown_requested = true;
}

/* 
 * POLAR: create promote_not_allowed file and unlink promote file 
 * when promote is not allowed 
 */
void
polar_clear_promote_file(void)
{
	struct stat stat_buf;
	FILE *file = NULL;

	/* reset promote trigger and unlink promote file */
	if (stat(PROMOTE_SIGNAL_FILE, &stat_buf) == 0)
	{
		ResetPromoteTriggered();
		unlink(PROMOTE_SIGNAL_FILE);
	}

	/* create promote_not_allowed file for pg_ctl */
	file = AllocateFile(POLAR_PROMOTE_NOT_ALLOWED_FILE, "w");
	if (file == NULL)
		ereport(LOG,
				(errcode_for_file_access(),
				 errmsg("could not create polar_promote_not_allowed file \"%s\": %m",
						POLAR_PROMOTE_NOT_ALLOWED_FILE)));
	else
		FreeFile(file);

}

/* POLAR: judge if promote can be executed right now */
bool
polar_is_promote_ready(void)
{
	/* promote is enabled when polar_enable_promote_wait_for_walreceive_done = off or in replica mode */
	if (!polar_enable_promote_wait_for_walreceive_done || polar_in_replica_mode())
		return true;

	/* disable promote when upstream node is not alive */
	if (!polar_upstream_node_is_alive())
	{
		ereport(WARNING,
				(errmsg("promote is not allowed"),
				 errdetail("Promote is not allowed when polar_enable_promote_wait_for_walreceive_done = on and primary/datamax/standby node can't be connected. "
							"Use promote -f to execute force promote")));
		/* reset promote trigger and unlink promote file */
		polar_clear_promote_file();
		return false;
	}
	/* upstream node is alive and walreceiver is ready, result based on the reply of walsender */
	else
	{
		/* return true when promote is enabled, WakeupRecovery now */
		if (POLAR_IS_PROMOTE_ALLOWED())
		{
			WakeupRecovery();
			return true;
		}
		if (POLAR_IS_PROMOTE_NOT_ALLOWED())
		{
			ereport(WARNING,
					(errmsg("promote is not allowed"),
					 errdetail("Promote is not allowed when polar_enable_promote_wait_for_walreceive_done = on and primary is running normally. "
								"Use promote -f to execute force promote")));
			/* reset promote trigger and unlink promote file */ 
			polar_clear_promote_file();
			return false;
		}
	}
	return false;
}

void
polar_startup_interrupt_with_pinned_buf(int buf)
{
	if (shutdown_requested)
		polar_unpin_buffer_proc_exit(buf);

	HandleStartupProcInterrupts();
}
/* POLAR end */
