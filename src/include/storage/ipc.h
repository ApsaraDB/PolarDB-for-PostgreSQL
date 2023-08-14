/*-------------------------------------------------------------------------
 *
 * ipc.h
 *	  POSTGRES inter-process communication definitions.
 *
 * This file is misnamed, as it no longer has much of anything directly
 * to do with IPC.  The functionality here is concerned with managing
 * exit-time cleanup for either a postmaster or a backend.
 *
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/storage/ipc.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef IPC_H
#define IPC_H

typedef void (*pg_on_exit_callback) (int code, Datum arg);
typedef void (*shmem_startup_hook_type) (void);

/* POLAR: hook for backend memory context */
typedef enum
{
	POLAR_SET_SIGNAL_MCTX,
	POLAR_CHECK_SIGNAL_MCTX,
	POLAR_SET_LOGGING_PLAN_OF_RUNNING_QUERY,
	POLAR_CHECK_LOGGING_PLAN_OF_RUNNING_QUERY,
	POLAR_SS_CHECK_SIGNAL_MCTX
} PolarHookActionType;
typedef void (*polar_monitor_hook_type) (PolarHookActionType action, void *args);

typedef void (*polar_heap_profile_hook_type) (void);

/*----------
 * API for handling cleanup that must occur during either ereport(ERROR)
 * or ereport(FATAL) exits from a block of code.  (Typical examples are
 * undoing transient changes to shared-memory state.)
 *
 *		PG_ENSURE_ERROR_CLEANUP(cleanup_function, arg);
 *		{
 *			... code that might throw ereport(ERROR) or ereport(FATAL) ...
 *		}
 *		PG_END_ENSURE_ERROR_CLEANUP(cleanup_function, arg);
 *
 * where the cleanup code is in a function declared per pg_on_exit_callback.
 * The Datum value "arg" can carry any information the cleanup function
 * needs.
 *
 * This construct ensures that cleanup_function() will be called during
 * either ERROR or FATAL exits.  It will not be called on successful
 * exit from the controlled code.  (If you want it to happen then too,
 * call the function yourself from just after the construct.)
 *
 * Note: the macro arguments are multiply evaluated, so avoid side-effects.
 *----------
 */
#define PG_ENSURE_ERROR_CLEANUP(cleanup_function, arg)	\
	do { \
		before_shmem_exit(cleanup_function, arg); \
		PG_TRY()

#define PG_END_ENSURE_ERROR_CLEANUP(cleanup_function, arg)	\
		cancel_before_shmem_exit(cleanup_function, arg); \
		PG_CATCH(); \
		{ \
			cancel_before_shmem_exit(cleanup_function, arg); \
			cleanup_function (0, arg); \
			PG_RE_THROW(); \
		} \
		PG_END_TRY(); \
	} while (0)


/* ipc.c */
extern PGDLLIMPORT bool proc_exit_inprogress;
extern PGDLLIMPORT bool shmem_exit_inprogress;

extern void proc_exit(int code) pg_attribute_noreturn();
extern void shmem_exit(int code);
extern void on_proc_exit(pg_on_exit_callback function, Datum arg);
extern void on_shmem_exit(pg_on_exit_callback function, Datum arg);
extern void before_shmem_exit(pg_on_exit_callback function, Datum arg);
extern void cancel_before_shmem_exit(pg_on_exit_callback function, Datum arg);
extern void on_exit_reset(void);

/* ipci.c */
extern PGDLLIMPORT shmem_startup_hook_type shmem_startup_hook;

extern void CreateSharedMemoryAndSemaphores(int port);

/* POLAR */
extern PGDLLIMPORT polar_monitor_hook_type polar_monitor_hook;
extern PGDLLIMPORT polar_heap_profile_hook_type polar_heap_profile_hook;

extern void polar_unlink_shmem_stat_file(int status, Datum arg);
extern bool polar_check_before_shmem_exit(pg_on_exit_callback function, Datum arg, bool print_backtrace);
/* POLAR end */

#endif							/* IPC_H */
