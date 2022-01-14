/*-------------------------------------------------------------------------
 *
 * pqsignal.c
 *	  reliable BSD-style signal(2) routine stolen from RWW who stole it
 *	  from Stevens...
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/port/pqsignal.c
 *
 *	We now assume that all Unix-oid systems have POSIX sigaction(2)
 *	with support for restartable signals (SA_RESTART).  We used to also
 *	support BSD-style signal(2), but there really shouldn't be anything
 *	out there anymore that doesn't have the POSIX API.
 *
 *	Windows, of course, is resolutely in a class by itself.  In the backend,
 *	we don't use this file at all; src/backend/port/win32/signal.c provides
 *	pqsignal() for the backend environment.  Frontend programs can use
 *	this version of pqsignal() if they wish, but beware that this does
 *	not provide restartable signals on Windows.
 *
 * ------------------------------------------------------------------------
 */

#include "c.h"

#include <signal.h>

#if !defined(WIN32) || defined(FRONTEND)

/*
 * Set up a signal handler, with SA_RESTART, for signal "signo"
 *
 * Returns the previous handler.
 */
pqsigfunc
pqsignal(int signo, pqsigfunc func)
{
#ifndef WIN32
	struct sigaction act,
				oact;

	act.sa_handler = func;
	sigemptyset(&act.sa_mask);
	act.sa_flags = SA_RESTART;
#ifdef SA_NOCLDSTOP
	if (signo == SIGCHLD)
		act.sa_flags |= SA_NOCLDSTOP;
#endif
	if (sigaction(signo, &act, &oact) < 0)
		return SIG_ERR;
	return oact.sa_handler;
#else							/* WIN32 */
	return signal(signo, func);
#endif
}

/* POLAR: Use pqsignal_with_sigsets api to disable some signal. */
pqsigfunc
pqsignal_with_sigsets(int signo, pqsigfunc func, int signum1, int signum2)
{
#ifndef WIN32
	struct sigaction act,
				oact;

	act.sa_handler = func;
	sigemptyset(&act.sa_mask);
	act.sa_flags = SA_RESTART;
	sigaddset(&act.sa_mask, signum1);
	sigaddset(&act.sa_mask, signum2);
#ifdef SA_NOCLDSTOP
	if (signo == SIGCHLD)
		act.sa_flags |= SA_NOCLDSTOP;
#endif
	if (sigaction(signo, &act, &oact) < 0)
		return SIG_ERR;
	return oact.sa_handler;
#else							/* WIN32 */
	return signal(signo, func);
#endif
}

/*
 * Set up a signal handler, without SA_RESTART, for signal "signo"
 *
 * Returns the previous handler.
 *
 * On Windows, this would be identical to pqsignal(), so don't bother.
 */
#ifndef WIN32

pqsigfunc
pqsignal_no_restart(int signo, pqsigfunc func)
{
	struct sigaction act,
				oact;

	act.sa_handler = func;
	sigemptyset(&act.sa_mask);
	act.sa_flags = 0;
#ifdef SA_NOCLDSTOP
	if (signo == SIGCHLD)
		act.sa_flags |= SA_NOCLDSTOP;
#endif
	if (sigaction(signo, &act, &oact) < 0)
		return SIG_ERR;
	return oact.sa_handler;
}

#endif							/* !WIN32 */

#endif							/* !defined(WIN32) || defined(FRONTEND) */
