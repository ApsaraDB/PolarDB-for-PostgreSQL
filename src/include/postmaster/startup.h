/*-------------------------------------------------------------------------
 *
 * startup.h
 *	  Exports from postmaster/startup.c.
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 *
 * src/include/postmaster/startup.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef _STARTUP_H
#define _STARTUP_H

extern void HandleStartupProcInterrupts(void);
extern void StartupProcessMain(void) pg_attribute_noreturn();
extern void PreRestoreCommand(void);
extern void PostRestoreCommand(void);
extern bool IsPromoteTriggered(void);
extern void ResetPromoteTriggered(void);

/* POLAR */
extern void polar_set_shutdown_requested_flag(void);
extern void polar_clear_promote_file(void);
extern bool polar_is_promote_ready(void);
extern void polar_startup_interrupt_with_pinned_buf(int buf);

#endif							/* _STARTUP_H */
