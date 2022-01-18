/*
 * fork_process.c
 *	 A simple wrapper on top of fork(). This does not handle the
 *	 EXEC_BACKEND case; it might be extended to do so, but it would be
 *	 considerably more complex.
 *
 * Copyright (c) 1996-2018, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/postmaster/fork_process.c
 */
#include "postgres.h"
#include "postmaster/fork_process.h"

#include <fcntl.h>
#include <time.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <unistd.h>
#ifdef USE_OPENSSL
#include <openssl/rand.h>
#endif

#ifndef WIN32

static pid_t polar_fetch_tpid(pid_t pid);

/*
 * Wrapper for fork(). Return values are the same as those for fork():
 * -1 if the fork failed, 0 in the child process, and the PID of the
 * child in the parent process.
 */
pid_t
fork_process(void)
{
	pid_t		result;
	const char *oomfilename;

#ifdef LINUX_PROFILE
	struct itimerval prof_itimer;
#endif

	/*
	 * Flush stdio channels just before fork, to avoid double-output problems.
	 * Ideally we'd use fflush(NULL) here, but there are still a few non-ANSI
	 * stdio libraries out there (like SunOS 4.1.x) that coredump if we do.
	 * Presently stdout and stderr are the only stdio output channels used by
	 * the postmaster, so fflush'ing them should be sufficient.
	 */
	fflush(stdout);
	fflush(stderr);

#ifdef LINUX_PROFILE

	/*
	 * Linux's fork() resets the profiling timer in the child process. If we
	 * want to profile child processes then we need to save and restore the
	 * timer setting.  This is a waste of time if not profiling, however, so
	 * only do it if commanded by specific -DLINUX_PROFILE switch.
	 */
	getitimer(ITIMER_PROF, &prof_itimer);
#endif

	result = fork();
	if (result == 0)
	{
		/* fork succeeded, in child */
#ifdef LINUX_PROFILE
		setitimer(ITIMER_PROF, &prof_itimer, NULL);
#endif

		/*
		 * By default, Linux tends to kill the postmaster in out-of-memory
		 * situations, because it blames the postmaster for the sum of child
		 * process sizes *including shared memory*.  (This is unbelievably
		 * stupid, but the kernel hackers seem uninterested in improving it.)
		 * Therefore it's often a good idea to protect the postmaster by
		 * setting its OOM score adjustment negative (which has to be done in
		 * a root-owned startup script).  Since the adjustment is inherited by
		 * child processes, this would ordinarily mean that all the
		 * postmaster's children are equally protected against OOM kill, which
		 * is not such a good idea.  So we provide this code to allow the
		 * children to change their OOM score adjustments again.  Both the
		 * file name to write to and the value to write are controlled by
		 * environment variables, which can be set by the same startup script
		 * that did the original adjustment.
		 */
		oomfilename = getenv("PG_OOM_ADJUST_FILE");

		if (oomfilename != NULL)
		{
			/*
			 * Use open() not stdio, to ensure we control the open flags. Some
			 * Linux security environments reject anything but O_WRONLY.
			 */
			int			fd = open(oomfilename, O_WRONLY, 0);

			/* We ignore all errors */
			if (fd >= 0)
			{
				const char *oomvalue = getenv("PG_OOM_ADJUST_VALUE");
				int			rc;

				if (oomvalue == NULL)	/* supply a useful default */
					oomvalue = "0";

				rc = write(fd, oomvalue, strlen(oomvalue));
				(void) rc;
				close(fd);
			}
		}

		/*
		 * Make sure processes do not share OpenSSL randomness state.
		 */
#ifdef USE_OPENSSL
		RAND_cleanup();
#endif
	}
	else if(result > 0)
	{
		pid_t polar_tpid = polar_fetch_tpid(result);
		if (polar_tpid >= 0)
			ereport(LOG, (errmsg("forked new process, pid is %d, true pid is %d", result, polar_tpid)));
	}

	return result;
}

/*
 * POLAR: fetch pid from file /proc/[pid]/sched.
 * The pattern of first line in sched is as follows:
 *    "process_name (tpid, #threads: integer)"
 * The tpid is just what we need.
 * The process_name's limit length is 16 bytes and pid is usually lower than
 * 65536 in linux. So it's enough to set POLAR_PROC_MAX_LEN to 64.
 * So does the sched's path.
 *
 * -1 if fetch work failed, otherwise tpid will be returned.
 */
static pid_t
polar_fetch_tpid(pid_t pid)
{
#define POLAR_PROC_MAX_LEN 63
#define STRINGIFY_HELPER(X) #X
#define STRINGIFY(X) STRINGIFY_HELPER(X)
	char sched[POLAR_PROC_MAX_LEN + 1] = {0x0};
	char buf[POLAR_PROC_MAX_LEN + 1] = {0x0};
	FILE *fp;
	pid_t tpid = -1;
	snprintf(sched, POLAR_PROC_MAX_LEN, "/proc/%d/sched", pid);
	if (!(fp = fopen(sched, "r")))
	{
		ereport(WARNING, (errmsg("failed to open file %s\n", sched)));
		return tpid;
	}
	else if (fscanf(fp, "%"STRINGIFY(POLAR_PROC_MAX_LEN)"[^0-9]%d", buf, &tpid) != 2)
		ereport(WARNING, (errmsg("failed to read pid from file %s\n", sched)));
	fclose(fp);
	return tpid;
}

#endif							/* ! WIN32 */
