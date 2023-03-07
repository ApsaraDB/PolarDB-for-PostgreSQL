/*-------------------------------------------------------------------------
 *
 * walsender.h
 *	  Exports from replication/walsender.c.
 *
 * Portions Copyright (c) 2010-2018, PostgreSQL Global Development Group
 *
 * src/include/replication/walsender.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef _WALSENDER_H
#define _WALSENDER_H

#include <signal.h>

#include "fmgr.h"

/* POLAR */
#include "access/xlogdefs.h"

/* POLAR */
#define STR_EXPAND(x) #x
#define STR(x) STR_EXPAND(x)

#define POLAR_STREAM_REPLICATION_VERSION 2  /* polar customized stream replication protocol version  */
/* POLAR end */

/*
 * What to do with a snapshot in create replication slot command.
 */
typedef enum
{
	CRS_EXPORT_SNAPSHOT,
	CRS_NOEXPORT_SNAPSHOT,
	CRS_USE_SNAPSHOT
} CRSSnapshotAction;

/* POLAR: customized replication mode */
typedef enum polar_repl_mode_t
{
	POLAR_REPL_DEFAULT,	/* Default mode, as original one */
	POLAR_REPL_REPLICA,	/* Replica mode */
	POLAR_REPL_STANDBY,	/* Standby mode */
	POLAR_REPL_SA_DATAMAX,	/* Standalone DataMax mode */
	POLAR_REPL_DMA_DATA,	/* DMA mode */
	POLAR_REPL_DMA_LOGGER	/* DMA mode */
} polar_repl_mode_t;
/* POLAR end */

/* global state */
extern bool am_walsender;
extern bool am_cascading_walsender;
extern bool am_db_walsender;
extern bool wake_wal_senders;

/* user-settable parameters */
extern int	max_wal_senders;
extern int	wal_sender_timeout;
extern bool log_replication_commands;

/* POLAR */
extern int polar_wal_snd_reserved_for_superuser;
extern bool polar_enable_physical_repl_non_super_wal_snd;
extern int polar_shutdown_walsnd_wait_replication_kind;
extern bool polar_shutdown_walsnd_wait_non_super;
/* POLAR: We will wait walsender whose replication is as follows during shutdown. */
#define POLAR_REPLICATION_KIND_INVALID -1
/* POLAR: REPLICATION_KIND_LOGICAL and REPLICATION_KIND_PHYSICAL are defined in replnodes.h. */
#define POLAR_REPLICATION_KIND_ALL -2
extern bool polar_dma_non_committed_walsender;
extern bool polar_dma_consistent_replication;
/* POLAR: end */

extern void InitWalSender(void);
extern bool exec_replication_command(const char *query_string);
extern void WalSndErrorCleanup(void);
extern void WalSndSignals(void);
extern Size WalSndShmemSize(void);
extern void WalSndShmemInit(void);
extern void WalSndWakeup(void);
extern void WalSndInitStopping(void);
extern void WalSndWaitStopping(void);
extern void HandleWalSndInitStopping(void);
extern void WalSndRqstFileReload(void);

/* POLAR */
extern void polar_wal_snd_normal_check(void);
extern long polar_wal_snd_compute_sleep_time(void);
extern polar_repl_mode_t polar_gen_replication_mode(void);
extern const char * polar_replication_mode_str(polar_repl_mode_t mode);
extern void polar_process_standby_promote(void);
extern void polar_send_promote_reply(void);
extern void polar_clear_walsender_promote(void);
/* POLAR end */

/*
 * Remember that we want to wakeup walsenders later
 *
 * This is separated from doing the actual wakeup because the writeout is done
 * while holding contended locks.
 */
#define WalSndWakeupRequest() \
	do { wake_wal_senders = true; } while (0)

/*
 * wakeup walsenders if there is work to be done
 */
#define WalSndWakeupProcessRequests()		\
	do										\
	{										\
		if (wake_wal_senders)				\
		{									\
			wake_wal_senders = false;		\
			if (max_wal_senders > 0)		\
				WalSndWakeup();				\
		}									\
	} while (0)

#endif							/* _WALSENDER_H */
