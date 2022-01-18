/*-------------------------------------------------------------------------
 *
 * px_threadlog.c
 *	  Functions to write to the log, when using threads
 *
 *
 * Portions Copyright (c) 2005-2008, Greenplum inc
 * Portions Copyright (c) 2012-Present Pivotal Software, Inc.
 * Portions Copyright (c) 2021, Alibaba Group Holding Limited
 *
 *
 * IDENTIFICATION
 *	    src/backend/px/px_threadlog.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <limits.h>
#include <pthread.h>

#include "libpq/libpq-be.h"
#include "miscadmin.h"
#include "pgtime.h"
#include "postmaster/syslogger.h"

#include "px/px_vars.h"

/*
 * We can't use elog to write to the log if we are running in a thread.
 *
 * So, write some thread-safe routines to write to the log.
 *
 * Ugly:  This write in a fixed format, and ignore what the log_prefix guc says.
 */
static pthread_mutex_t send_mutex = PTHREAD_MUTEX_INITIALIZER;

#ifdef WIN32
static void
			write_eventlog(int level, const char *line);

/*
 * Write a message line to the windows event log
 */
static void
write_eventlog(int level, const char *line)
{
	int			eventlevel = EVENTLOG_ERROR_TYPE;
	static HANDLE evtHandle = INVALID_HANDLE_VALUE;

	if (evtHandle == INVALID_HANDLE_VALUE)
	{
		evtHandle = RegisterEventSource(NULL, "PostgreSQL");
		if (evtHandle == NULL)
		{
			evtHandle = INVALID_HANDLE_VALUE;
			return;
		}
	}

	ReportEvent(evtHandle,
				eventlevel,
				0,
				0,				/* All events are Id 0 */
				NULL,
				1,
				0,
				&line,
				NULL);
}
#endif							/* WIN32 */

static void
get_timestamp(char *strfbuf, int length)
{
	pg_time_t	stamp_time;
	char		msbuf[8];
	struct timeval tv;

	gettimeofday(&tv, NULL);
	stamp_time = tv.tv_sec;

	pg_strftime(strfbuf, length,
	/* leave room for microseconds... */
	/* Win32 timezone names are too long so don't print them */
#ifndef WIN32
				"%Y-%m-%d %H:%M:%S        %Z",
#else
				"%Y-%m-%d %H:%M:%S        ",
#endif
				pg_localtime(&stamp_time, log_timezone));

	/* 'paste' milliseconds into place... */
	sprintf(msbuf, ".%06d", (int) (tv.tv_usec));
	strncpy(strfbuf + 19, msbuf, 7);
}

void
write_log(const char *fmt,...)
{
	char		logprefix[1024];
	char		tempbuf[25];
	va_list		ap;

	fmt = _(fmt);

	va_start(ap, fmt);

	get_timestamp(logprefix, sizeof(logprefix));
	strcat(logprefix, "|");
	if (MyProcPort)
	{
		const char *username = MyProcPort->user_name;

		if (username == NULL || *username == '\0')
			username = "";
		strcat(logprefix, username);	/* user */
	}

	strcat(logprefix, "|");
	if (MyProcPort)
	{
		const char *dbname = MyProcPort->database_name;

		if (dbname == NULL || *dbname == '\0')
			dbname = "";
		strcat(logprefix, dbname);
	}
	strcat(logprefix, "|");
	sprintf(tempbuf, "%d", MyProcPid);
	strcat(logprefix, tempbuf); /* pid */
	strcat(logprefix, "|");
	sprintf(tempbuf, "con%d cmd%d", px_session_id, px_command_count);
	strcat(logprefix, tempbuf);

	strcat(logprefix, "|");
	strcat(logprefix, ":-THREAD ");

	strcat(logprefix, ":  ");

	strcat(logprefix, fmt);

	if (fmt[strlen(fmt) - 1] != '\n')
		strcat(logprefix, "\n");

	/*
	 * We don't trust that vfprintf won't get confused if it is being run by
	 * two threads at the same time, which could cause interleaved messages.
	 * Let's play it safe, and make sure only one thread is doing this at a
	 * time.
	 */
	pthread_mutex_lock(&send_mutex);
#ifndef WIN32
	/* On Unix, we just fprintf to stderr */
	vfprintf(stderr, logprefix, ap);
	fflush(stderr);
#else

	/*
	 * On Win32, we print to stderr if running on a console, or write to
	 * eventlog if running as a service
	 */
	if (pgwin32_is_service())	/* Running as a service */
	{
		char		errbuf[2048];	/* Arbitrary size? */

		vsnprintf(errbuf, sizeof(errbuf), logprefix, ap);

		write_eventlog(EVENTLOG_ERROR_TYPE, errbuf);
	}
	else
	{
		/* Not running as service, write to stderr */
		vfprintf(stderr, logprefix, ap);
		fflush(stderr);
	}
#endif
	pthread_mutex_unlock(&send_mutex);
	va_end(ap);
}
