/*-------------------------------------------------------------------------
 *
 * walsender.c
 *
 * The WAL sender process (walsender) is new as of Postgres 9.0. It takes
 * care of sending XLOG from the primary server to a single recipient.
 * (Note that there can be more than one walsender process concurrently.)
 * It is started by the postmaster when the walreceiver of a standby server
 * connects to the primary server and requests XLOG streaming replication.
 *
 * A walsender is similar to a regular backend, ie. there is a one-to-one
 * relationship between a connection and a walsender process, but instead
 * of processing SQL queries, it understands a small set of special
 * replication-mode commands. The START_REPLICATION command begins streaming
 * WAL to the client. While streaming, the walsender keeps reading XLOG
 * records from the disk and sends them to the standby server over the
 * COPY protocol, until either side ends the replication by exiting COPY
 * mode (or until the connection is closed).
 *
 * Normal termination is by SIGTERM, which instructs the walsender to
 * close the connection and exit(0) at the next convenient moment. Emergency
 * termination is by SIGQUIT; like any backend, the walsender will simply
 * abort and exit on SIGQUIT. A close of the connection and a FATAL error
 * are treated as not a crash but approximately normal termination;
 * the walsender will exit quickly without sending any more XLOG records.
 *
 * If the server is shut down, checkpointer sends us
 * PROCSIG_WALSND_INIT_STOPPING after all regular backends have exited.  If
 * the backend is idle or runs an SQL query this causes the backend to
 * shutdown, if logical replication is in progress all existing WAL records
 * are processed followed by a shutdown.  Otherwise this causes the walsender
 * to switch to the "stopping" state. In this state, the walsender will reject
 * any further replication commands. The checkpointer begins the shutdown
 * checkpoint once all walsenders are confirmed as stopping. When the shutdown
 * checkpoint finishes, the postmaster sends us SIGUSR2. This instructs
 * walsender to send any outstanding WAL, including the shutdown checkpoint
 * record, wait for it to be replicated to the standby, and then exit.
 *
 * Portions Copyright (c) 2020, Alibaba Group Holding Limited
 * Portions Copyright (c) 2010-2018, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/replication/walsender.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <signal.h>
#include <unistd.h>

#include "access/printtup.h"
#include "access/timeline.h"
#include "access/transam.h"
#include "access/xact.h"
#include "access/xlog_internal.h"
#include "access/xlogutils.h"

#include "catalog/pg_authid.h"
#include "catalog/pg_type.h"
#include "commands/dbcommands.h"
#include "commands/defrem.h"
#include "funcapi.h"
#include "libpq/libpq.h"
#include "libpq/pqformat.h"
#include "miscadmin.h"
#include "nodes/replnodes.h"
#include "pgstat.h"
#include "port/atomics.h"
#include "replication/basebackup.h"
#include "replication/decode.h"
#include "replication/logical.h"
#include "replication/logicalfuncs.h"
#include "replication/slot.h"
#include "replication/snapbuild.h"
#include "replication/syncrep.h"
#include "replication/walreceiver.h"
#include "replication/walsender.h"
#include "replication/walsender_private.h"
#include "storage/condition_variable.h"
#include "storage/fd.h"
#include "storage/ipc.h"
#include "storage/pmsignal.h"
#include "storage/proc.h"
#include "storage/procarray.h"
#include "tcop/dest.h"
#include "tcop/tcopprot.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/pg_lsn.h"
#include "utils/portal.h"
#include "utils/ps_status.h"
#include "utils/resowner.h"
#include "utils/timeout.h"
#include "utils/timestamp.h"

#ifdef ENABLE_DISTRIBUTED_TRANSACTION
#include "access/transam.h"
#endif
#ifdef ENABLE_REMOTE_RECOVERY
#include "replication/remote_recovery.h"
#endif

/* POLAR */
#include "polar_datamax/polar_datamax.h"
#include "polar_dma/polar_dma.h"

/*
 * Maximum data payload in a WAL data message.  Must be >= XLOG_BLCKSZ.
 *
 * We don't have a good idea of what a good value would be; there's some
 * overhead per message in both walsender and walreceiver, but on the other
 * hand sending large batches makes walsender less responsive to signals
 * because signals are checked only between messages.  128kB (with
 * default 8k blocks) seems like a reasonable guess for now.
 */
#define MAX_SEND_SIZE (XLOG_BLCKSZ * 16)

/* POLAR: dma replication mode */
#define POLAR_IS_DMA_REPL(mode) \
	((mode) == POLAR_REPL_DMA_DATA || (mode) == POLAR_REPL_DMA_LOGGER)
/* POLAR end */

/* Array of WalSnds in shared memory */
WalSndCtlData *WalSndCtl = NULL;

/* My slot in the shared memory array */
WalSnd	   *MyWalSnd = NULL;

/* Global state */
bool		am_walsender = false;	/* Am I a walsender process? */
bool		am_cascading_walsender = false; /* Am I cascading WAL to another
											 * standby? */
bool		am_db_walsender = false;	/* Connected to a database? */

/* User-settable parameters for walsender */
int			max_wal_senders = 0;	/* the maximum number of concurrent
									 * walsenders */
int			wal_sender_timeout = 60 * 1000; /* maximum time to send one WAL
											 * data message */
bool		log_replication_commands = false;

/*
 * State for WalSndWakeupRequest
 */
bool		wake_wal_senders = false;

/*
 * These variables are used similarly to openLogFile/SegNo/Off,
 * but for walsender to read the XLOG.
 */
static int	sendFile = -1;
static XLogSegNo sendSegNo = 0;
static uint32 sendOff = 0;

/* Timeline ID of the currently open file */
static TimeLineID curFileTimeLine = 0;

/*
 * These variables keep track of the state of the timeline we're currently
 * sending. sendTimeLine identifies the timeline. If sendTimeLineIsHistoric,
 * the timeline is not the latest timeline on this server, and the server's
 * history forked off from that timeline at sendTimeLineValidUpto.
 */
static TimeLineID sendTimeLine = 0;
static TimeLineID sendTimeLineNextTLI = 0;
static bool sendTimeLineIsHistoric = false;
static XLogRecPtr sendTimeLineValidUpto = InvalidXLogRecPtr;

/*
 * How far have we sent WAL already? This is also advertised in
 * MyWalSnd->sentPtr.  (Actually, this is the next WAL location to send.)
 */
static XLogRecPtr sentPtr = 0;

/* Buffers for constructing outgoing messages and processing reply messages. */
static StringInfoData output_message;
static StringInfoData reply_message;
static StringInfoData tmpbuf;

/* Timestamp of last ProcessRepliesIfAny(). */
static TimestampTz last_processing = 0;

/*
 * Timestamp of last ProcessRepliesIfAny() that saw a reply from the
 * standby. Set to 0 if wal_sender_timeout doesn't need to be active.
 */
static TimestampTz last_reply_timestamp = 0;

/* Have we sent a heartbeat message asking for reply, since last reply? */
static bool waiting_for_ping_response = false;

/*
 * While streaming WAL in Copy mode, streamingDoneSending is set to true
 * after we have sent CopyDone. We should not send any more CopyData messages
 * after that. streamingDoneReceiving is set to true when we receive CopyDone
 * from the other end. When both become true, it's time to exit Copy mode.
 */
static bool streamingDoneSending;
static bool streamingDoneReceiving;

/* Are we there yet? */
static bool WalSndCaughtUp = false;

/* Flags set by signal handlers for later service in main loop */
static volatile sig_atomic_t got_SIGUSR2 = false;
static volatile sig_atomic_t got_STOPPING = false;

/*
 * This is set while we are streaming. When not set
 * PROCSIG_WALSND_INIT_STOPPING signal will be handled like SIGTERM. When set,
 * the main loop is responsible for checking got_STOPPING and terminating when
 * it's set (after streaming any remaining WAL).
 */
static volatile sig_atomic_t replication_active = false;

static LogicalDecodingContext *logical_decoding_ctx = NULL;
static XLogRecPtr logical_startptr = InvalidXLogRecPtr;

/* A sample associating a WAL location with the time it was written. */
typedef struct
{
	XLogRecPtr	lsn;
	TimestampTz time;
} WalTimeSample;

/* The size of our buffer of time samples. */
#define LAG_TRACKER_BUFFER_SIZE 8192

/* A mechanism for tracking replication lag. */
static struct
{
	XLogRecPtr	last_lsn;
	WalTimeSample buffer[LAG_TRACKER_BUFFER_SIZE];
	int			write_head;
	int			read_heads[NUM_SYNC_REP_WAIT_MODE];
	WalTimeSample last_read[NUM_SYNC_REP_WAIT_MODE];
} LagTracker;

/* Signal handlers */
static void WalSndLastCycleHandler(SIGNAL_ARGS);

/* Prototypes for private functions */
typedef void (*WalSndSendDataCallback) (void);
static void WalSndLoop(WalSndSendDataCallback send_data);
static void InitWalSenderSlot(void);
static void WalSndKill(int code, Datum arg);
static void WalSndShutdown(void) pg_attribute_noreturn();
static void XLogSendPhysical(void);
static void XLogSendLogical(void);
static void WalSndDone(WalSndSendDataCallback send_data);
static XLogRecPtr GetStandbyFlushRecPtr(void);
static void IdentifySystem(void);
static void CreateReplicationSlot(CreateReplicationSlotCmd *cmd);
static void DropReplicationSlot(DropReplicationSlotCmd *cmd);
static void StartReplication(StartReplicationCmd *cmd);
#ifdef ENABLE_REMOTE_RECOVERY
static void StartFetchPageReplication(StartReplicationCmd *cmd);
static void ProcessPrimaryMessage(void);
static void ProcessFetchPageMessage(void);
#endif
static void StartLogicalReplication(StartReplicationCmd *cmd);
static void ProcessStandbyMessage(void);
static void ProcessStandbyReplyMessage(void);
static void ProcessStandbyHSFeedbackMessage(void);
static void ProcessRepliesIfAny(void);
static void WalSndKeepalive(bool requestReply);
static void WalSndKeepaliveIfNecessary(void);
static void WalSndCheckTimeOut(void);
static long WalSndComputeSleeptime(TimestampTz now);
static void WalSndPrepareWrite(LogicalDecodingContext *ctx, XLogRecPtr lsn, TransactionId xid, bool last_write);
static void WalSndWriteData(LogicalDecodingContext *ctx, XLogRecPtr lsn, TransactionId xid, bool last_write);
static void WalSndUpdateProgress(LogicalDecodingContext *ctx, XLogRecPtr lsn, TransactionId xid);
static XLogRecPtr WalSndWaitForWal(XLogRecPtr loc);
static void LagTrackerWrite(XLogRecPtr lsn, TimestampTz local_flush_time);
static TimeOffset LagTrackerRead(int head, XLogRecPtr lsn, TimestampTz now);
static bool TransactionIdInRecentPast(TransactionId xid, uint32 epoch);

static void XLogRead(char *buf, XLogRecPtr startptr, Size count, polar_repl_mode_t polar_replication_mode);

/* POLAR */
static void XLogSendPhysicalExt(polar_repl_mode_t polar_replication_mode);
static void polar_dma_logger_xlog_send_physical(void);
static void polar_dma_data_xlog_send_physical(void);
static XLogRecPtr polar_dma_get_flush_lsn(void);

/* Initialize walsender process before entering the main command loop */
void
InitWalSender(void)
{
	am_cascading_walsender = RecoveryInProgress();

	/* Create a per-walsender data structure in shared memory */
	InitWalSenderSlot();

	/* Set up resource owner */
	CurrentResourceOwner = ResourceOwnerCreate(NULL, "walsender top-level resource owner");

	/*
	 * Let postmaster know that we're a WAL sender. Once we've declared us as
	 * a WAL sender process, postmaster will let us outlive the bgwriter and
	 * kill us last in the shutdown sequence, so we get a chance to stream all
	 * remaining WAL at shutdown, including the shutdown checkpoint. Note that
	 * there's no going back, and we mustn't write any WAL records after this.
	 */
	MarkPostmasterChildWalSender();
	SendPostmasterSignal(PMSIGNAL_ADVANCE_STATE_MACHINE);

	/* Initialize empty timestamp buffer for lag tracking. */
	memset(&LagTracker, 0, sizeof(LagTracker));
}

/*
 * Clean up after an error.
 *
 * WAL sender processes don't use transactions like regular backends do.
 * This function does any cleanup required after an error in a WAL sender
 * process, similar to what transaction abort does in a regular backend.
 */
void
WalSndErrorCleanup(void)
{
	LWLockReleaseAll();
	ConditionVariableCancelSleep();
	pgstat_report_wait_end();

	if (sendFile >= 0)
	{
		close(sendFile);
		sendFile = -1;
	}

	if (MyReplicationSlot != NULL)
		ReplicationSlotRelease();

	ReplicationSlotCleanup();

	replication_active = false;

	if (got_STOPPING || got_SIGUSR2)
		proc_exit(0);

	/* Revert back to startup state */
	WalSndSetState(WALSNDSTATE_STARTUP);
}

/*
 * Handle a client's connection abort in an orderly manner.
 */
static void
WalSndShutdown(void)
{
	int			i;

	/*
	 * Reset whereToSendOutput to prevent ereport from attempting to send any
	 * more messages to the standby.
	 */
	if (whereToSendOutput == DestRemote)
		whereToSendOutput = DestNone;

	/*
	 * check all sender is shutdown, and set WalSndCtl->sender_running for max
	 * available falg.
	 */
	bool		all_shutdown = true;

	if (WalSndCtl->sender_running == true)
	{
		/*
		 * Find a free walsender slot and reserve it. If this fails, we must
		 * be out of WalSnd structures.
		 */
		for (i = 0; i < max_wal_senders; i++)
		{
			WalSnd	   *walsnd = &WalSndCtl->walsnds[i];

			SpinLockAcquire(&walsnd->mutex);
			if ((walsnd->pid != 0) && (walsnd->pid != MyWalSnd->pid))
			{
				SpinLockRelease(&walsnd->mutex);
				all_shutdown = false;
				break;
			}
			else
			{
				SpinLockRelease(&walsnd->mutex);
			}
		}

		if (all_shutdown)
		{
			LWLockAcquire(SyncRepLock, LW_EXCLUSIVE);
			WalSndCtl->sender_running = false;
			LWLockRelease(SyncRepLock);
		}
	}

	proc_exit(0);
	abort();					/* keep the compiler quiet */
}

/*
 * Handle the IDENTIFY_SYSTEM command.
 */
static void
IdentifySystem(void)
{
	char		sysid[32];
	char		xloc[MAXFNAMELEN];
	XLogRecPtr	logptr;
	char	   *dbname = NULL;
	DestReceiver *dest;
	TupOutputState *tstate;
	TupleDesc	tupdesc;
	Datum		values[4];
	bool		nulls[4];

	/*
	 * Reply with a result set with one row, four columns. First col is system
	 * ID, second is timeline ID, third is current xlog location and the
	 * fourth contains the database name if we are connected to one.
	 */

	snprintf(sysid, sizeof(sysid), UINT64_FORMAT,
			 GetSystemIdentifier());

	am_cascading_walsender = RecoveryInProgress();
	if (am_cascading_walsender)
	{
		/* this also updates ThisTimeLineID */
		if (polar_enable_dma)
			logptr = polar_dma_get_flush_lsn();
		else
			logptr = GetStandbyFlushRecPtr();
	}
	else
		logptr = GetFlushRecPtr();

	snprintf(xloc, sizeof(xloc), "%X/%X", (uint32) (logptr >> 32), (uint32) logptr);

	if (MyDatabaseId != InvalidOid)
	{
		MemoryContext cur = CurrentMemoryContext;

		/* syscache access needs a transaction env. */
		StartTransactionCommand();
		/* make dbname live outside TX context */
		MemoryContextSwitchTo(cur);
		dbname = get_database_name(MyDatabaseId);
		CommitTransactionCommand();
		/* CommitTransactionCommand switches to TopMemoryContext */
		MemoryContextSwitchTo(cur);
	}

	dest = CreateDestReceiver(DestRemoteSimple);
	MemSet(nulls, false, sizeof(nulls));

	/* need a tuple descriptor representing four columns */
	tupdesc = CreateTemplateTupleDesc(4, false);
	TupleDescInitBuiltinEntry(tupdesc, (AttrNumber) 1, "systemid",
							  TEXTOID, -1, 0);
	TupleDescInitBuiltinEntry(tupdesc, (AttrNumber) 2, "timeline",
							  INT4OID, -1, 0);
	TupleDescInitBuiltinEntry(tupdesc, (AttrNumber) 3, "xlogpos",
							  TEXTOID, -1, 0);
	TupleDescInitBuiltinEntry(tupdesc, (AttrNumber) 4, "dbname",
							  TEXTOID, -1, 0);

	/* prepare for projection of tuples */
	tstate = begin_tup_output_tupdesc(dest, tupdesc);

	/* column 1: system identifier */
	values[0] = CStringGetTextDatum(sysid);

	/* column 2: timeline */
	values[1] = Int32GetDatum(ThisTimeLineID);

	/* column 3: wal location */
	values[2] = CStringGetTextDatum(xloc);

	/* column 4: database name, or NULL if none */
	if (dbname)
		values[3] = CStringGetTextDatum(dbname);
	else
		nulls[3] = true;

	/* send it to dest */
	do_tup_output(tstate, values, nulls);

	end_tup_output(tstate);
}


/*
 * Handle TIMELINE_HISTORY command.
 */
static void
SendTimeLineHistory(TimeLineHistoryCmd *cmd)
{
	StringInfoData buf;
	char		histfname[MAXFNAMELEN];
	char		path[MAXPGPATH];
	int			fd;
	off_t		histfilelen;
	off_t		bytesleft;
	Size		len;

	/*
	 * Reply with a result set with one row, and two columns. The first col is
	 * the name of the history file, 2nd is the contents.
	 */

	TLHistoryFileName(histfname, cmd->timeline);
	polar_is_dma_logger_mode = polar_is_dma_logger_node();	/* POLAR: Enter datamax
															 * Mode. */
	TLHistoryFilePath(path, cmd->timeline);
	polar_is_dma_logger_mode = false;	/* POLAR: Leave datamax mode. */

	/* Send a RowDescription message */
	pq_beginmessage(&buf, 'T');
	pq_sendint16(&buf, 2);		/* 2 fields */

	/* first field */
	pq_sendstring(&buf, "filename");	/* col name */
	pq_sendint32(&buf, 0);		/* table oid */
	pq_sendint16(&buf, 0);		/* attnum */
	pq_sendint32(&buf, TEXTOID);	/* type oid */
	pq_sendint16(&buf, -1);		/* typlen */
	pq_sendint32(&buf, 0);		/* typmod */
	pq_sendint16(&buf, 0);		/* format code */

	/* second field */
	pq_sendstring(&buf, "content"); /* col name */
	pq_sendint32(&buf, 0);		/* table oid */
	pq_sendint16(&buf, 0);		/* attnum */
	pq_sendint32(&buf, BYTEAOID);	/* type oid */
	pq_sendint16(&buf, -1);		/* typlen */
	pq_sendint32(&buf, 0);		/* typmod */
	pq_sendint16(&buf, 0);		/* format code */
	pq_endmessage(&buf);

	/* Send a DataRow message */
	pq_beginmessage(&buf, 'D');
	pq_sendint16(&buf, 2);		/* # of columns */
	len = strlen(histfname);
	pq_sendint32(&buf, len);	/* col1 len */
	pq_sendbytes(&buf, histfname, len);

	fd = OpenTransientFile(path, O_RDONLY | PG_BINARY);
	if (fd < 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not open file \"%s\": %m", path)));

	/* Determine file length and send it to client */
	histfilelen = lseek(fd, 0, SEEK_END);
	if (histfilelen < 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not seek to end of file \"%s\": %m", path)));
	if (lseek(fd, 0, SEEK_SET) != 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not seek to beginning of file \"%s\": %m", path)));

	pq_sendint32(&buf, histfilelen);	/* col2 len */

	bytesleft = histfilelen;
	while (bytesleft > 0)
	{
		PGAlignedBlock rbuf;
		int			nread;

		pgstat_report_wait_start(WAIT_EVENT_WALSENDER_TIMELINE_HISTORY_READ);
		nread = read(fd, rbuf.data, sizeof(rbuf));
		pgstat_report_wait_end();
		if (nread <= 0)
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not read file \"%s\": %m",
							path)));
		pq_sendbytes(&buf, rbuf.data, nread);
		bytesleft -= nread;
	}
	CloseTransientFile(fd);

	pq_endmessage(&buf);
}

#ifdef ENABLE_REMOTE_RECOVERY
static char *page_image;

/*
 * Regular fetch page request from primary.
 */
static void
ProcessFetchPageMessage(void)
{
	RelFileNode rnode;
	ForkNumber	forknum;
	BlockNumber blkno;
	Buffer		buf;
	Page		page;
	int			r;

	/* the caller already consumed the msgtype byte */
	rnode.spcNode = (Oid) pq_getmsgint(&reply_message, 4);
	rnode.dbNode = (Oid) pq_getmsgint(&reply_message, 4);
	rnode.relNode = (Oid) pq_getmsgint(&reply_message, 4);
	forknum = pq_getmsgint(&reply_message, 4);
	blkno = (BlockNumber) pq_getmsgint(&reply_message, 4);

	/* Read the request page */
	buf = XLogReadBufferExtended(rnode, forknum, blkno,
								 RBM_NORMAL);

	resetStringInfo(&output_message);
	pq_sendbyte(&output_message, 'w');

	if (BufferIsValid(buf))
	{
		pq_sendint(&output_message, BLCKSZ, 4); /* len first */
		enlargeStringInfo(&output_message, BLCKSZ);
		LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);

		page = BufferGetPage(buf);
		memcpy(&output_message.data[output_message.len], page, BLCKSZ);
		output_message.len += BLCKSZ;
		output_message.data[output_message.len] = '\0';

		UnlockReleaseBuffer(buf);

		if (enable_remote_recovery_print)
			elog(LOG, "REMOTE RECOVERY valid page on standby spcnode %d dbnode %d relnode %d forknum %d blkno %d",
				 rnode.spcNode, rnode.dbNode, rnode.relNode, forknum, blkno);
	}
	else
	{
		pq_sendint(&output_message, 0, 4);	/* len first */
		elog(LOG, "REMOTE RECOVERY no valid page on standby spcnode %d dbnode %d relnode %d forknum %d blkno %d",
			 rnode.spcNode, rnode.dbNode, rnode.relNode, forknum, blkno);
	}

	pq_putmessage_noblock('d', output_message.data, output_message.len);

	r = pq_flush();
	if (r != 0)
	{
		elog(LOG, "REMOTE RECOVERY cannot flush data");
		proc_exit(1);
	}
}


/*
 * Process a status update message received from standby.
 */
static void
ProcessPrimaryMessage(void)
{
	char		msgtype;

	/*
	 * Check message type from the first byte.
	 */
	msgtype = pq_getmsgbyte(&reply_message);
	if (enable_remote_recovery_print)
		elog(LOG, "process primary message %c", msgtype);

	switch (msgtype)
	{
		case 'f':
			ProcessFetchPageMessage();
			break;

		default:
			ereport(COMMERROR,
					(errcode(ERRCODE_PROTOCOL_VIOLATION),
					 errmsg("unexpected message type \"%c\"", msgtype)));
			proc_exit(0);
	}
}

#define FETCH_SLEEP_INTERVAL (1 * 1000 * 1000)

/*
 * Handle START_REPLICATION command.
 *
 * At the moment, this never returns, but an ereport(ERROR) will take us back
 * to the main loop.
 *
 * Loop to handle remote page fetching requests from primary.
 */
static void
StartFetchPageReplication(StartReplicationCmd *cmd)
{
	StringInfoData buf;
	int			r;

	/* Send a CopyBothResponse message, and start streaming */
	pq_beginmessage(&buf, 'W');
	pq_sendbyte(&buf, 0);
	pq_sendint16(&buf, 0);
	pq_endmessage(&buf);
	pq_flush();

	page_image = (char *) palloc(BLCKSZ);

	streamingDoneSending = streamingDoneReceiving = false;
	elog(LOG, "start fetch page replication %X/%X timeline %u",
		 (uint32) (cmd->startpoint >> 32),
		 (uint32) (cmd->startpoint),
		 cmd->timeline);

	/*
	 * if we are standby, we must wait for the replay process to pass the
	 * target checkpoint.
	 */
	if (RecoveryInProgress())
	{
		XLogRecPtr	replayPtr;
		TimeLineID	replayTLI;
		int			sleep_count = 0;

		while (1)
		{
			replayPtr = GetXLogReplayRecPtr(&replayTLI);

			if (cmd->timeline != replayTLI)
				elog(ERROR, "request different timeline %u %u", cmd->timeline, replayTLI);

			if (cmd->startpoint > replayPtr)
			{
				if (!sleep_count)
					elog(LOG, "REMOTE RECOVERY wait for replay process to pass the targert ckpt %X/%X replayPtr %X/%X",
						 (uint32) (cmd->startpoint >> 32),
						 (uint32) (cmd->startpoint),
						 (uint32) (replayPtr >> 32),
						 (uint32) (replayPtr));



				pg_usleep(FETCH_SLEEP_INTERVAL);
				sleep_count++;
			}
			else
			{
				elog(LOG, "REMOTE RECOVERY replay process passed the targert ckpt %X/%X replayPtr %X/%X",
					 (uint32) (cmd->startpoint >> 32),
					 (uint32) (cmd->startpoint),
					 (uint32) (replayPtr >> 32),
					 (uint32) (replayPtr));
				break;
			}
		}
	}

	for (;;)
	{
		pq_startmsgread();
		r = pq_getbyte();
		if (r == EOF)
		{
			/* unexpected error or EOF */
			ereport(COMMERROR,
					(errcode(ERRCODE_PROTOCOL_VIOLATION),
					 errmsg("unexpected EOF on primary connection")));
			proc_exit(0);
		}
		elog(DEBUG6, "REMOTE RECOVERY receives command %c", r);

		/* Read the message contents */
		resetStringInfo(&reply_message);
		if (pq_getmessage(&reply_message, 0))
		{
			ereport(COMMERROR,
					(errcode(ERRCODE_PROTOCOL_VIOLATION),
					 errmsg("unexpected EOF on standby connection")));
			proc_exit(0);
		}

		/* Handle the very limited subset of commands expected in this phase */
		switch (r)
		{
				/*
				 * 'd' means a standby reply wrapped in a CopyData packet.
				 */
			case 'd':
				ProcessPrimaryMessage();
				break;

				/*
				 * CopyDone means the primary requested to finish streaming.
				 * Reply with CopyDone, if we had not sent that already.
				 */
			case 'c':
				if (!streamingDoneSending)
				{
					pq_putmessage_noblock('c', NULL, 0);
					streamingDoneSending = true;
				}

				streamingDoneReceiving = true;
				elog(LOG, "REMOTE RECOVERY streaming copy done");

				break;

				/*
				 * 'X' means that the primary is closing down the socket.
				 */
			case 'X':
				elog(LOG, "REMOTE RECOVERY primary disconnected");
				proc_exit(0);

			default:
				ereport(FATAL,
						(errcode(ERRCODE_PROTOCOL_VIOLATION),
						 errmsg("invalid primary message type \"%c\"",
								r)));
		}
		pq_endmsgread();

		if (streamingDoneReceiving)
			break;
	}

	elog(LOG, "REMOTE RECOVERY streaming done");

	/* Send CommandComplete message */
	EndCommand("COPY 0", DestRemote);
}
#endif
/*
 * Handle START_REPLICATION command.
 *
 * At the moment, this never returns, but an ereport(ERROR) will take us back
 * to the main loop.
 */
static void
StartReplication(StartReplicationCmd *cmd)
{
	StringInfoData buf;
	XLogRecPtr	FlushPtr;

	if (ThisTimeLineID == 0)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("IDENTIFY_SYSTEM has not been run before START_REPLICATION")));

	/* POLAR: do not allow DMA replication if not in DMA mode */
	if (!polar_enable_dma && POLAR_IS_DMA_REPL(cmd->polar_repl_mode))
		ereport(ERROR,
				(errmsg("DMA replication is illegally requested if not in DMA mode.")));

	/*
	 * We assume here that we're logging enough information in the WAL for
	 * log-shipping, since this is checked in PostmasterMain().
	 *
	 * NOTE: wal_level can only change at shutdown, so in most cases it is
	 * difficult for there to be WAL data that we can still see that was
	 * written at wal_level='minimal'.
	 */
	if (cmd->slotname)
	{
		ReplicationSlotAcquire(cmd->slotname, true);
		if (SlotIsLogical(MyReplicationSlot))
			ereport(ERROR,
					(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
					 (errmsg("cannot use a logical replication slot for physical replication"))));
	}

	if (cmd->polar_repl_mode == POLAR_REPL_DMA_LOGGER)
	{
		/*
		 * POLAR: if startpoint from DMA logger is invalid, send from old wal
		 * files of start timeline.
		 */
		if (XLogRecPtrIsInvalid(cmd->startpoint))
		{
			XLogRecPtr	restart_lsn = polar_set_initial_datamax_restart_lsn(NULL);

			/*
			 * Select the maximum of current timeline's switch point and
			 * oldest wal file
			 */
			if (cmd->timeline > 1)
			{
				List	   *history;
				XLogRecPtr	switchpoint;
				TimeLineID	tli;

				polar_is_dma_logger_mode = polar_is_dma_logger_node();	/* POLAR: set datamax
																		 * branch for xlog file
																		 * path */
				history = readTimeLineHistory(cmd->timeline);
				polar_is_dma_logger_mode = false;	/* POLAR: reset */

				switchpoint = tliSwitchPoint(cmd->timeline - 1, history, &tli);
				if (switchpoint > restart_lsn)
					restart_lsn = switchpoint;

				list_free_deep(history);
			}

			cmd->startpoint =
				restart_lsn - XLogSegmentOffset(restart_lsn, wal_segment_size);
		}
	}
	/* POLAR end */

	/*
	 * Select the timeline. If it was given explicitly by the client, use
	 * that. Otherwise use the timeline of the last replayed record, which is
	 * kept in ThisTimeLineID.
	 */
	if (am_cascading_walsender)
	{
		/* this also updates ThisTimeLineID */
		if (polar_enable_dma)
			FlushPtr = polar_dma_get_flush_lsn();
		else
			FlushPtr = GetStandbyFlushRecPtr();
	}
	else
		FlushPtr = GetFlushRecPtr();

	if (cmd->timeline != 0)
	{
		XLogRecPtr	switchpoint;

		sendTimeLine = cmd->timeline;
		if (sendTimeLine == ThisTimeLineID)
		{
			sendTimeLineIsHistoric = false;
			sendTimeLineValidUpto = InvalidXLogRecPtr;
		}
		else
		{
			List	   *timeLineHistory;

			sendTimeLineIsHistoric = true;

			/*
			 * Check that the timeline the client requested exists, and the
			 * requested start location is on that timeline.
			 */
			polar_is_dma_logger_mode = polar_is_dma_logger_node();	/* POLAR: set datamax
																	 * branch for xlog file
																	 * path */
			timeLineHistory = readTimeLineHistory(ThisTimeLineID);
			polar_is_dma_logger_mode = false;	/* POLAR: reset */
			switchpoint = tliSwitchPoint(cmd->timeline, timeLineHistory,
										 &sendTimeLineNextTLI);
			list_free_deep(timeLineHistory);

			/*
			 * Found the requested timeline in the history. Check that
			 * requested startpoint is on that timeline in our history.
			 *
			 * This is quite loose on purpose. We only check that we didn't
			 * fork off the requested timeline before the switchpoint. We
			 * don't check that we switched *to* it before the requested
			 * starting point. This is because the client can legitimately
			 * request to start replication from the beginning of the WAL
			 * segment that contains switchpoint, but on the new timeline, so
			 * that it doesn't end up with a partial segment. If you ask for
			 * too old a starting point, you'll get an error later when we
			 * fail to find the requested WAL segment in pg_wal.
			 *
			 * XXX: we could be more strict here and only allow a startpoint
			 * that's older than the switchpoint, if it's still in the same
			 * WAL segment.
			 */
			if (!XLogRecPtrIsInvalid(switchpoint) &&
				switchpoint < cmd->startpoint)
			{
				ereport(ERROR,
						(errmsg("requested starting point %X/%X on timeline %u is not in this server's history",
								(uint32) (cmd->startpoint >> 32),
								(uint32) (cmd->startpoint),
								cmd->timeline),
						 errdetail("This server's history forked from timeline %u at %X/%X.",
								   cmd->timeline,
								   (uint32) (switchpoint >> 32),
								   (uint32) (switchpoint))));
			}
			sendTimeLineValidUpto = switchpoint;
		}
	}
	else
	{
		sendTimeLine = ThisTimeLineID;
		sendTimeLineValidUpto = InvalidXLogRecPtr;
		sendTimeLineIsHistoric = false;
	}

	streamingDoneSending = streamingDoneReceiving = false;

	/* If there is nothing to stream, don't even enter COPY mode */
	if (!sendTimeLineIsHistoric || cmd->startpoint < sendTimeLineValidUpto)
	{
		/*
		 * When we first start replication the standby will be behind the
		 * primary. For some applications, for example synchronous
		 * replication, it is important to have a clear state for this initial
		 * catchup mode, so we can trigger actions when we change streaming
		 * state later. We may stay in this state for a long time, which is
		 * exactly why we want to be able to monitor whether or not we are
		 * still here.
		 */
		WalSndSetState(WALSNDSTATE_CATCHUP);

		/* Send a CopyBothResponse message, and start streaming */
		pq_beginmessage(&buf, 'W');
		pq_sendbyte(&buf, 0);
		pq_sendint16(&buf, 0);
		pq_endmessage(&buf);
		pq_flush();

		/*
		 * Don't allow a request to stream from a future point in WAL that
		 * hasn't been flushed to disk in this server yet.
		 */
		if (FlushPtr < cmd->startpoint)
		{
			ereport(ERROR,
					(errmsg("requested starting point %X/%X is ahead of the WAL flush position of this server %X/%X",
							(uint32) (cmd->startpoint >> 32),
							(uint32) (cmd->startpoint),
							(uint32) (FlushPtr >> 32),
							(uint32) (FlushPtr))));
		}

		/* Start streaming from the requested point */
		sentPtr = cmd->startpoint;

		/* Initialize shared memory status, too */
		SpinLockAcquire(&MyWalSnd->mutex);
		MyWalSnd->sentPtr = sentPtr;
		SpinLockRelease(&MyWalSnd->mutex);

		SyncRepInitConfig();

		/* Main loop of walsender */
		replication_active = true;

		if (cmd->polar_repl_mode == POLAR_REPL_DMA_LOGGER)
			WalSndLoop(polar_dma_logger_xlog_send_physical);
		else if (cmd->polar_repl_mode == POLAR_REPL_DMA_DATA)
			WalSndLoop(polar_dma_data_xlog_send_physical);
		else
			WalSndLoop(XLogSendPhysical);

		replication_active = false;
		if (got_STOPPING)
			proc_exit(0);
		WalSndSetState(WALSNDSTATE_STARTUP);

		Assert(streamingDoneSending && streamingDoneReceiving);
	}

	if (cmd->slotname)
		ReplicationSlotRelease();

	/*
	 * Copy is finished now. Send a single-row result set indicating the next
	 * timeline.
	 */
	if (sendTimeLineIsHistoric)
	{
		char		startpos_str[8 + 1 + 8 + 1];
		DestReceiver *dest;
		TupOutputState *tstate;
		TupleDesc	tupdesc;
		Datum		values[2];
		bool		nulls[2];

		snprintf(startpos_str, sizeof(startpos_str), "%X/%X",
				 (uint32) (sendTimeLineValidUpto >> 32),
				 (uint32) sendTimeLineValidUpto);

		dest = CreateDestReceiver(DestRemoteSimple);
		MemSet(nulls, false, sizeof(nulls));

		/*
		 * Need a tuple descriptor representing two columns. int8 may seem
		 * like a surprising data type for this, but in theory int4 would not
		 * be wide enough for this, as TimeLineID is unsigned.
		 */
		tupdesc = CreateTemplateTupleDesc(2, false);
		TupleDescInitBuiltinEntry(tupdesc, (AttrNumber) 1, "next_tli",
								  INT8OID, -1, 0);
		TupleDescInitBuiltinEntry(tupdesc, (AttrNumber) 2, "next_tli_startpos",
								  TEXTOID, -1, 0);

		/* prepare for projection of tuple */
		tstate = begin_tup_output_tupdesc(dest, tupdesc);

		values[0] = Int64GetDatum((int64) sendTimeLineNextTLI);
		values[1] = CStringGetTextDatum(startpos_str);

		/* send it to dest */
		do_tup_output(tstate, values, nulls);

		end_tup_output(tstate);
	}

	/* Send CommandComplete message */
	pq_puttextmessage('C', "START_STREAMING");
}

/*
 * read_page callback for logical decoding contexts, as a walsender process.
 *
 * Inside the walsender we can do better than logical_read_local_xlog_page,
 * which has to do a plain sleep/busy loop, because the walsender's latch gets
 * set every time WAL is flushed.
 */
static int
logical_read_xlog_page(XLogReaderState *state, XLogRecPtr targetPagePtr, int reqLen,
					   XLogRecPtr targetRecPtr, char *cur_page, TimeLineID *pageTLI)
{
	XLogRecPtr	flushptr;
	int			count;

	XLogReadDetermineTimeline(state, targetPagePtr, reqLen);
	sendTimeLineIsHistoric = (state->currTLI != ThisTimeLineID);
	sendTimeLine = state->currTLI;
	sendTimeLineValidUpto = state->currTLIValidUntil;
	sendTimeLineNextTLI = state->nextTLI;

	/* make sure we have enough WAL available */
	flushptr = WalSndWaitForWal(targetPagePtr + reqLen);

	/* fail if not (implies we are going to shut down) */
	if (flushptr < targetPagePtr + reqLen)
		return -1;

	if (targetPagePtr + XLOG_BLCKSZ <= flushptr)
		count = XLOG_BLCKSZ;	/* more than one block available */
	else
		count = flushptr - targetPagePtr;	/* part of the page available */

	/* now actually read the data, we know it's there */
	XLogRead(cur_page, targetPagePtr, XLOG_BLCKSZ, POLAR_REPL_DEFAULT);

	return count;
}

/*
 * Process extra options given to CREATE_REPLICATION_SLOT.
 */
static void
parseCreateReplSlotOptions(CreateReplicationSlotCmd *cmd,
						   bool *reserve_wal,
						   CRSSnapshotAction *snapshot_action)
{
	ListCell   *lc;
	bool		snapshot_action_given = false;
	bool		reserve_wal_given = false;

	/* Parse options */
	foreach(lc, cmd->options)
	{
		DefElem    *defel = (DefElem *) lfirst(lc);

		if (strcmp(defel->defname, "export_snapshot") == 0)
		{
			if (snapshot_action_given || cmd->kind != REPLICATION_KIND_LOGICAL)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options")));

			snapshot_action_given = true;
			*snapshot_action = defGetBoolean(defel) ? CRS_EXPORT_SNAPSHOT :
				CRS_NOEXPORT_SNAPSHOT;
		}
		else if (strcmp(defel->defname, "use_snapshot") == 0)
		{
			if (snapshot_action_given || cmd->kind != REPLICATION_KIND_LOGICAL)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options")));

			snapshot_action_given = true;
			*snapshot_action = CRS_USE_SNAPSHOT;
		}
		else if (strcmp(defel->defname, "reserve_wal") == 0)
		{
			if (reserve_wal_given || cmd->kind != REPLICATION_KIND_PHYSICAL)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options")));

			reserve_wal_given = true;
			*reserve_wal = true;
		}
		else
			elog(ERROR, "unrecognized option: %s", defel->defname);
	}
}

/*
 * Create a new replication slot.
 */
static void
CreateReplicationSlot(CreateReplicationSlotCmd *cmd)
{
	const char *snapshot_name = NULL;
	char		xloc[MAXFNAMELEN];
	char	   *slot_name;
	bool		reserve_wal = false;
#ifdef ENABLE_DISTRIBUTED_TRANSACTION
	GlobalTimestamp snapshot_start_ts = InvalidGlobalTimestamp;
#endif
	CRSSnapshotAction snapshot_action = CRS_EXPORT_SNAPSHOT;
	DestReceiver *dest;
	TupOutputState *tstate;
	TupleDesc	tupdesc;
#ifdef ENABLE_DISTRIBUTED_TRANSACTION
	Datum		values[5];
	bool		nulls[5];
#else
	Datum		values[4];
	bool		nulls[4];
#endif
	Assert(!MyReplicationSlot);

	parseCreateReplSlotOptions(cmd, &reserve_wal, &snapshot_action);

	/* setup state for XLogReadPage */
	sendTimeLineIsHistoric = false;
	sendTimeLine = ThisTimeLineID;

	if (cmd->kind == REPLICATION_KIND_PHYSICAL)
	{
		ReplicationSlotCreate(cmd->slotname, false,
							  cmd->temporary ? RS_TEMPORARY : RS_PERSISTENT);
	}
	else
	{
		CheckLogicalDecodingRequirements();

		/*
		 * Initially create persistent slot as ephemeral - that allows us to
		 * nicely handle errors during initialization because it'll get
		 * dropped if this transaction fails. We'll make it persistent at the
		 * end. Temporary slots can be created as temporary from beginning as
		 * they get dropped on error as well.
		 */
		ReplicationSlotCreate(cmd->slotname, true,
							  cmd->temporary ? RS_TEMPORARY : RS_EPHEMERAL);
	}

	if (cmd->kind == REPLICATION_KIND_LOGICAL)
	{
		LogicalDecodingContext *ctx;
		bool		need_full_snapshot = false;

		/*
		 * Do options check early so that we can bail before calling the
		 * DecodingContextFindStartpoint which can take long time.
		 */
		if (snapshot_action == CRS_EXPORT_SNAPSHOT)
		{
			if (IsTransactionBlock())
				ereport(ERROR,
						(errmsg("CREATE_REPLICATION_SLOT ... EXPORT_SNAPSHOT "
								"must not be called inside a transaction")));

			need_full_snapshot = true;
		}
		else if (snapshot_action == CRS_USE_SNAPSHOT)
		{
			if (!IsTransactionBlock())
				ereport(ERROR,
						(errmsg("CREATE_REPLICATION_SLOT ... USE_SNAPSHOT "
								"must be called inside a transaction")));

			if (XactIsoLevel != XACT_REPEATABLE_READ)
				ereport(ERROR,
						(errmsg("CREATE_REPLICATION_SLOT ... USE_SNAPSHOT "
								"must be called in REPEATABLE READ isolation mode transaction")));

			if (FirstSnapshotSet)
				ereport(ERROR,
						(errmsg("CREATE_REPLICATION_SLOT ... USE_SNAPSHOT "
								"must be called before any query")));

			if (IsSubTransaction())
				ereport(ERROR,
						(errmsg("CREATE_REPLICATION_SLOT ... USE_SNAPSHOT "
								"must not be called in a subtransaction")));

			need_full_snapshot = true;
		}

		ctx = CreateInitDecodingContext(cmd->plugin, NIL, need_full_snapshot,
										logical_read_xlog_page,
										WalSndPrepareWrite, WalSndWriteData,
										WalSndUpdateProgress);

		/*
		 * Signal that we don't need the timeout mechanism. We're just
		 * creating the replication slot and don't yet accept feedback
		 * messages or send keepalives. As we possibly need to wait for
		 * further WAL the walsender would otherwise possibly be killed too
		 * soon.
		 */
		last_reply_timestamp = 0;

		/* build initial snapshot, might take a while */
		DecodingContextFindStartpoint(ctx);

		/*
		 * Export or use the snapshot if we've been asked to do so.
		 *
		 * NB. We will convert the snapbuild.c kind of snapshot to normal
		 * snapshot when doing this.
		 */
		if (snapshot_action == CRS_EXPORT_SNAPSHOT)
		{
			snapshot_name = SnapBuildExportSnapshot(ctx->snapshot_builder);
		}
		else if (snapshot_action == CRS_USE_SNAPSHOT)
		{
			Snapshot	snap;
#ifdef ENABLE_DISTRIBUTED_TRANSACTION
			Snapshot	cur_snap;
#endif
			snap = SnapBuildInitialSnapshot(ctx->snapshot_builder);
			RestoreTransactionSnapshot(snap, MyProc);
#ifdef ENABLE_DISTRIBUTED_TRANSACTION
			if (!IsolationUsesXactSnapshot())
				elog(ERROR, "Isolation level should be larger than Repeatable Read");

			cur_snap = GetTransactionSnapshot();
			snapshot_start_ts = cur_snap->snapshotcsn;
			if (!COMMITSEQNO_IS_NORMAL(snapshot_start_ts))
				elog(ERROR, "invalid snapshot start ts " UINT64_FORMAT, snapshot_start_ts);

			if (enable_distri_print)
				elog(LOG, "logical replication sends snapshot start ts " UINT64_FORMAT, snapshot_start_ts);
#endif
		}

		/* don't need the decoding context anymore */
		FreeDecodingContext(ctx);

		if (!cmd->temporary)
			ReplicationSlotPersist();
	}
	else if (cmd->kind == REPLICATION_KIND_PHYSICAL && reserve_wal)
	{
		ReplicationSlotReserveWal();

		ReplicationSlotMarkDirty();

		/* Write this slot to disk if it's a permanent one. */
		if (!cmd->temporary)
			ReplicationSlotSave();
	}

	snprintf(xloc, sizeof(xloc), "%X/%X",
			 (uint32) (MyReplicationSlot->data.confirmed_flush >> 32),
			 (uint32) MyReplicationSlot->data.confirmed_flush);

	dest = CreateDestReceiver(DestRemoteSimple);
	MemSet(nulls, false, sizeof(nulls));

	/*----------
	 * Need a tuple descriptor representing four columns:
	 * - first field: the slot name
	 * - second field: LSN at which we became consistent
	 * - third field: exported snapshot's name
	 * - fourth field: output plugin
	 * - fifth filed: snapshot start_ts added for CTS-based logical replication
	 *----------
	 */
#ifdef ENABLE_DISTRIBUTED_TRANSACTION
	tupdesc = CreateTemplateTupleDesc(5, false);
#else
	tupdesc = CreateTemplateTupleDesc(4, false);
#endif

	TupleDescInitBuiltinEntry(tupdesc, (AttrNumber) 1, "slot_name",
							  TEXTOID, -1, 0);
	TupleDescInitBuiltinEntry(tupdesc, (AttrNumber) 2, "consistent_point",
							  TEXTOID, -1, 0);
	TupleDescInitBuiltinEntry(tupdesc, (AttrNumber) 3, "snapshot_name",
							  TEXTOID, -1, 0);
	TupleDescInitBuiltinEntry(tupdesc, (AttrNumber) 4, "output_plugin",
							  TEXTOID, -1, 0);

#ifdef ENABLE_DISTRIBUTED_TRANSACTION
	TupleDescInitBuiltinEntry(tupdesc, (AttrNumber) 5, "snapshot_start_ts",
							  INT8OID, -1, 0);
#endif

	/* prepare for projection of tuples */
	tstate = begin_tup_output_tupdesc(dest, tupdesc);

	/* slot_name */
	slot_name = NameStr(MyReplicationSlot->data.name);
	values[0] = CStringGetTextDatum(slot_name);

	/* consistent wal location */
	values[1] = CStringGetTextDatum(xloc);

	/* snapshot name, or NULL if none */
	if (snapshot_name != NULL)
		values[2] = CStringGetTextDatum(snapshot_name);
	else
		nulls[2] = true;

	/* plugin, or NULL if none */
	if (cmd->plugin != NULL)
		values[3] = CStringGetTextDatum(cmd->plugin);
	else
		nulls[3] = true;

#ifdef ENABLE_DISTRIBUTED_TRANSACTION
	if (snapshot_start_ts != InvalidGlobalTimestamp)
		values[4] = Int64GetDatum(snapshot_start_ts);
	else
		nulls[4] = true;

	if (enable_distri_print)
		elog(LOG, "logical replication sends snapshot start ts " UINT64_FORMAT, snapshot_start_ts);
#endif
	/* send it to dest */
	do_tup_output(tstate, values, nulls);
	end_tup_output(tstate);

	ReplicationSlotRelease();
}

/*
 * Get rid of a replication slot that is no longer wanted.
 */
static void
DropReplicationSlot(DropReplicationSlotCmd *cmd)
{
	ReplicationSlotDrop(cmd->slotname, !cmd->wait);
	EndCommand("DROP_REPLICATION_SLOT", DestRemote);
}

/*
 * Load previously initiated logical slot and prepare for sending data (via
 * WalSndLoop).
 */
static void
StartLogicalReplication(StartReplicationCmd *cmd)
{
	StringInfoData buf;

	/* make sure that our requirements are still fulfilled */
	CheckLogicalDecodingRequirements();

	Assert(!MyReplicationSlot);

	ReplicationSlotAcquire(cmd->slotname, true);

	/*
	 * Force a disconnect, so that the decoding code doesn't need to care
	 * about an eventual switch from running in recovery, to running in a
	 * normal environment. Client code is expected to handle reconnects.
	 */
	if (am_cascading_walsender && !RecoveryInProgress())
	{
		ereport(LOG,
				(errmsg("terminating walsender process after promotion")));
		got_STOPPING = true;
	}

	/*
	 * Create our decoding context, making it start at the previously ack'ed
	 * position.
	 *
	 * Do this before sending CopyBoth, so that any errors are reported early.
	 */
	logical_decoding_ctx =
		CreateDecodingContext(cmd->startpoint, cmd->options, false,
							  logical_read_xlog_page,
							  WalSndPrepareWrite, WalSndWriteData,
							  WalSndUpdateProgress);


	WalSndSetState(WALSNDSTATE_CATCHUP);

	/* Send a CopyBothResponse message, and start streaming */
	pq_beginmessage(&buf, 'W');
	pq_sendbyte(&buf, 0);
	pq_sendint16(&buf, 0);
	pq_endmessage(&buf);
	pq_flush();


	/* Start reading WAL from the oldest required WAL. */
	logical_startptr = MyReplicationSlot->data.restart_lsn;

	/*
	 * Report the location after which we'll send out further commits as the
	 * current sentPtr.
	 */
	sentPtr = MyReplicationSlot->data.confirmed_flush;

	/* Also update the sent position status in shared memory */
	SpinLockAcquire(&MyWalSnd->mutex);
	MyWalSnd->sentPtr = MyReplicationSlot->data.restart_lsn;
	SpinLockRelease(&MyWalSnd->mutex);

	replication_active = true;

	SyncRepInitConfig();

	/* Main loop of walsender */
	WalSndLoop(XLogSendLogical);

	FreeDecodingContext(logical_decoding_ctx);
	ReplicationSlotRelease();

	replication_active = false;
	if (got_STOPPING)
		proc_exit(0);
	WalSndSetState(WALSNDSTATE_STARTUP);

	/* Get out of COPY mode (CommandComplete). */
	EndCommand("COPY 0", DestRemote);
}

/*
 * LogicalDecodingContext 'prepare_write' callback.
 *
 * Prepare a write into a StringInfo.
 *
 * Don't do anything lasting in here, it's quite possible that nothing will be done
 * with the data.
 */
static void
WalSndPrepareWrite(LogicalDecodingContext *ctx, XLogRecPtr lsn, TransactionId xid, bool last_write)
{
	/* can't have sync rep confused by sending the same LSN several times */
	if (!last_write)
		lsn = InvalidXLogRecPtr;

	resetStringInfo(ctx->out);

	pq_sendbyte(ctx->out, 'w');
	pq_sendint64(ctx->out, lsn);	/* dataStart */
	pq_sendint64(ctx->out, lsn);	/* walEnd */

	/*
	 * Fill out the sendtime later, just as it's done in XLogSendPhysical, but
	 * reserve space here.
	 */
	pq_sendint64(ctx->out, 0);	/* sendtime */
}

/*
 * LogicalDecodingContext 'write' callback.
 *
 * Actually write out data previously prepared by WalSndPrepareWrite out to
 * the network. Take as long as needed, but process replies from the other
 * side and check timeouts during that.
 */
static void
WalSndWriteData(LogicalDecodingContext *ctx, XLogRecPtr lsn, TransactionId xid,
				bool last_write)
{
	TimestampTz now;

	/* output previously gathered data in a CopyData packet */
	pq_putmessage_noblock('d', ctx->out->data, ctx->out->len);

	/*
	 * Fill the send timestamp last, so that it is taken as late as possible.
	 * This is somewhat ugly, but the protocol is set as it's already used for
	 * several releases by streaming physical replication.
	 */
	resetStringInfo(&tmpbuf);
	now = GetCurrentTimestamp();
	pq_sendint64(&tmpbuf, now);
	memcpy(&ctx->out->data[1 + sizeof(int64) + sizeof(int64)],
		   tmpbuf.data, sizeof(int64));

	CHECK_FOR_INTERRUPTS();

	/* Try to flush pending output to the client */
	if (pq_flush_if_writable() != 0)
		WalSndShutdown();

	/* Try taking fast path unless we get too close to walsender timeout. */
	if (now < TimestampTzPlusMilliseconds(last_reply_timestamp,
										  wal_sender_timeout / 2) &&
		!pq_is_send_pending())
	{
		return;
	}

	/* If we have pending write here, go to slow path */
	for (;;)
	{
		int			wakeEvents;
		long		sleeptime;

		/* Check for input from the client */
		ProcessRepliesIfAny();

		/* die if timeout was reached */
		WalSndCheckTimeOut();

		/* Send keepalive if the time has come */
		WalSndKeepaliveIfNecessary();

		if (!pq_is_send_pending())
			break;

		sleeptime = WalSndComputeSleeptime(GetCurrentTimestamp());

		wakeEvents = WL_LATCH_SET | WL_POSTMASTER_DEATH |
			WL_SOCKET_WRITEABLE | WL_SOCKET_READABLE | WL_TIMEOUT;

		/* Sleep until something happens or we time out */
		WaitLatchOrSocket(MyLatch, wakeEvents,
						  MyProcPort->sock, sleeptime,
						  WAIT_EVENT_WAL_SENDER_WRITE_DATA);

		/*
		 * Emergency bailout if postmaster has died.  This is to avoid the
		 * necessity for manual cleanup of all postmaster children.
		 */
		if (!PostmasterIsAlive())
			exit(1);

		/* Clear any already-pending wakeups */
		ResetLatch(MyLatch);

		CHECK_FOR_INTERRUPTS();

		/* Process any requests or signals received recently */
		if (ConfigReloadPending)
		{
			ConfigReloadPending = false;
			ProcessConfigFile(PGC_SIGHUP);
			SyncRepInitConfig();
		}

		/* Try to flush pending output to the client */
		if (pq_flush_if_writable() != 0)
			WalSndShutdown();
	}

	/* reactivate latch so WalSndLoop knows to continue */
	SetLatch(MyLatch);
}

/*
 * LogicalDecodingContext 'update_progress' callback.
 *
 * Write the current position to the lag tracker (see XLogSendPhysical).
 */
static void
WalSndUpdateProgress(LogicalDecodingContext *ctx, XLogRecPtr lsn, TransactionId xid)
{
	static TimestampTz sendTime = 0;
	TimestampTz now = GetCurrentTimestamp();

	/*
	 * Track lag no more than once per WALSND_LOGICAL_LAG_TRACK_INTERVAL_MS to
	 * avoid flooding the lag tracker when we commit frequently.
	 */
#define WALSND_LOGICAL_LAG_TRACK_INTERVAL_MS	1000
	if (!TimestampDifferenceExceeds(sendTime, now,
									WALSND_LOGICAL_LAG_TRACK_INTERVAL_MS))
		return;

	LagTrackerWrite(lsn, now);
	sendTime = now;
}

/*
 * Wait till WAL < loc is flushed to disk so it can be safely sent to client.
 *
 * Returns end LSN of flushed WAL.  Normally this will be >= loc, but
 * if we detect a shutdown request (either from postmaster or client)
 * we will return early, so caller must always check.
 */
static XLogRecPtr
WalSndWaitForWal(XLogRecPtr loc)
{
	int			wakeEvents;
	static XLogRecPtr RecentFlushPtr = InvalidXLogRecPtr;


	/*
	 * Fast path to avoid acquiring the spinlock in case we already know we
	 * have enough WAL available. This is particularly interesting if we're
	 * far behind.
	 */
	if (RecentFlushPtr != InvalidXLogRecPtr &&
		loc <= RecentFlushPtr)
		return RecentFlushPtr;

	/* Get a more recent flush pointer. */
	if (!RecoveryInProgress())
		RecentFlushPtr = GetFlushRecPtr();
	else
		RecentFlushPtr = GetXLogReplayRecPtr(NULL);

	for (;;)
	{
		long		sleeptime;

		/*
		 * Emergency bailout if postmaster has died.  This is to avoid the
		 * necessity for manual cleanup of all postmaster children.
		 */
		if (!PostmasterIsAlive())
			exit(1);

		/* Clear any already-pending wakeups */
		ResetLatch(MyLatch);

		CHECK_FOR_INTERRUPTS();

		/* Process any requests or signals received recently */
		if (ConfigReloadPending)
		{
			ConfigReloadPending = false;
			ProcessConfigFile(PGC_SIGHUP);
			SyncRepInitConfig();
		}

		/* Check for input from the client */
		ProcessRepliesIfAny();

		/*
		 * If we're shutting down, trigger pending WAL to be written out,
		 * otherwise we'd possibly end up waiting for WAL that never gets
		 * written, because walwriter has shut down already.
		 */
		if (got_STOPPING)
			XLogBackgroundFlush();

		/* Update our idea of the currently flushed position. */
		if (!RecoveryInProgress())
			RecentFlushPtr = GetFlushRecPtr();
		else
			RecentFlushPtr = GetXLogReplayRecPtr(NULL);

		/*
		 * If postmaster asked us to stop, don't wait anymore.
		 *
		 * It's important to do this check after the recomputation of
		 * RecentFlushPtr, so we can send all remaining data before shutting
		 * down.
		 */
		if (got_STOPPING)
			break;

		/*
		 * We only send regular messages to the client for full decoded
		 * transactions, but a synchronous replication and walsender shutdown
		 * possibly are waiting for a later location. So we send pings
		 * containing the flush location every now and then.
		 */
		if (MyWalSnd->flush < sentPtr &&
			MyWalSnd->write < sentPtr &&
			!waiting_for_ping_response)
		{
			WalSndKeepalive(false);
			waiting_for_ping_response = true;
		}

		/* check whether we're done */
		if (loc <= RecentFlushPtr)
			break;

		/* Waiting for new WAL. Since we need to wait, we're now caught up. */
		WalSndCaughtUp = true;

		/*
		 * Try to flush any pending output to the client.
		 */
		if (pq_flush_if_writable() != 0)
			WalSndShutdown();

		/*
		 * If we have received CopyDone from the client, sent CopyDone
		 * ourselves, and the output buffer is empty, it's time to exit
		 * streaming, so fail the current WAL fetch request.
		 */
		if (streamingDoneReceiving && streamingDoneSending &&
			!pq_is_send_pending())
			break;

		/* die if timeout was reached */
		WalSndCheckTimeOut();

		/* Send keepalive if the time has come */
		WalSndKeepaliveIfNecessary();

		/*
		 * Sleep until something happens or we time out.  Also wait for the
		 * socket becoming writable, if there's still pending output.
		 * Otherwise we might sit on sendable output data while waiting for
		 * new WAL to be generated.  (But if we have nothing to send, we don't
		 * want to wake on socket-writable.)
		 */
		sleeptime = WalSndComputeSleeptime(GetCurrentTimestamp());

		wakeEvents = WL_LATCH_SET | WL_POSTMASTER_DEATH |
			WL_SOCKET_READABLE | WL_TIMEOUT;

		if (pq_is_send_pending())
			wakeEvents |= WL_SOCKET_WRITEABLE;

		WaitLatchOrSocket(MyLatch, wakeEvents,
						  MyProcPort->sock, sleeptime,
						  WAIT_EVENT_WAL_SENDER_WAIT_WAL);
	}

	/* reactivate latch so WalSndLoop knows to continue */
	SetLatch(MyLatch);
	return RecentFlushPtr;
}

/*
 * Execute an incoming replication command.
 *
 * Returns true if the cmd_string was recognized as WalSender command, false
 * if not.
 */
bool
exec_replication_command(const char *cmd_string)
{
	int			parse_rc;
	Node	   *cmd_node;
	MemoryContext cmd_context;
	MemoryContext old_context;

	/*
	 * If WAL sender has been told that shutdown is getting close, switch its
	 * status accordingly to handle the next replication commands correctly.
	 */
	if (got_STOPPING)
		WalSndSetState(WALSNDSTATE_STOPPING);

	/*
	 * Throw error if in stopping mode.  We need prevent commands that could
	 * generate WAL while the shutdown checkpoint is being written.  To be
	 * safe, we just prohibit all new commands.
	 */
	if (MyWalSnd->state == WALSNDSTATE_STOPPING)
		ereport(ERROR,
				(errmsg("cannot execute new commands while WAL sender is in stopping mode")));

	/*
	 * CREATE_REPLICATION_SLOT ... LOGICAL exports a snapshot until the next
	 * command arrives. Clean up the old stuff if there's anything.
	 */
	SnapBuildClearExportedSnapshot();

	CHECK_FOR_INTERRUPTS();

	cmd_context = AllocSetContextCreate(CurrentMemoryContext,
										"Replication command context",
										ALLOCSET_DEFAULT_SIZES);
	old_context = MemoryContextSwitchTo(cmd_context);

	replication_scanner_init(cmd_string);
	parse_rc = replication_yyparse();
	if (parse_rc != 0)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 (errmsg_internal("replication command parser returned %d",
								  parse_rc))));

	cmd_node = replication_parse_result;

	/*
	 * Log replication command if log_replication_commands is enabled. Even
	 * when it's disabled, log the command with DEBUG1 level for backward
	 * compatibility. Note that SQL commands are not logged here, and will be
	 * logged later if log_statement is enabled.
	 */
	if (cmd_node->type != T_SQLCmd)
		ereport(log_replication_commands ? LOG : DEBUG1,
				(errmsg("received replication command: %s", cmd_string)));

	/*
	 * CREATE_REPLICATION_SLOT ... LOGICAL exports a snapshot. If it was
	 * called outside of transaction the snapshot should be cleared here.
	 */
	if (!IsTransactionBlock())
		SnapBuildClearExportedSnapshot();

	/*
	 * For aborted transactions, don't allow anything except pure SQL, the
	 * exec_simple_query() will handle it correctly.
	 */
	if (IsAbortedTransactionBlockState() && !IsA(cmd_node, SQLCmd))
		ereport(ERROR,
				(errcode(ERRCODE_IN_FAILED_SQL_TRANSACTION),
				 errmsg("current transaction is aborted, "
						"commands ignored until end of transaction block")));

	CHECK_FOR_INTERRUPTS();

	/*
	 * Allocate buffers that will be used for each outgoing and incoming
	 * message.  We do this just once per command to reduce palloc overhead.
	 */
	initStringInfo(&output_message);
	initStringInfo(&reply_message);
	initStringInfo(&tmpbuf);

	/* Report to pgstat that this process is running */
	pgstat_report_activity(STATE_RUNNING, NULL);

	switch (cmd_node->type)
	{
		case T_IdentifySystemCmd:
			IdentifySystem();
			break;

		case T_BaseBackupCmd:
			PreventInTransactionBlock(true, "BASE_BACKUP");
			SendBaseBackup((BaseBackupCmd *) cmd_node);
			break;

		case T_CreateReplicationSlotCmd:
			CreateReplicationSlot((CreateReplicationSlotCmd *) cmd_node);
			break;

		case T_DropReplicationSlotCmd:
			DropReplicationSlot((DropReplicationSlotCmd *) cmd_node);
			break;

		case T_StartReplicationCmd:
			{
				StartReplicationCmd *cmd = (StartReplicationCmd *) cmd_node;

				PreventInTransactionBlock(true, "START_REPLICATION");
#ifdef ENABLE_REMOTE_RECOVERY
				if (cmd->kind == REPLICATION_KIND_FETCH_PAGE)
				{
					StartFetchPageReplication(cmd);
				}
				else
#endif
				if (cmd->kind == REPLICATION_KIND_PHYSICAL)
					StartReplication(cmd);
				else
					StartLogicalReplication(cmd);
				break;
			}

		case T_TimeLineHistoryCmd:
			PreventInTransactionBlock(true, "TIMELINE_HISTORY");
			SendTimeLineHistory((TimeLineHistoryCmd *) cmd_node);
			break;

		case T_VariableShowStmt:
			{
				DestReceiver *dest = CreateDestReceiver(DestRemoteSimple);
				VariableShowStmt *n = (VariableShowStmt *) cmd_node;

				GetPGVariable(n->name, dest);
			}
			break;

		case T_SQLCmd:
			if (MyDatabaseId == InvalidOid)
				ereport(ERROR,
						(errmsg("cannot execute SQL commands in WAL sender for physical replication")));

			/* Report to pgstat that this process is now idle */
			pgstat_report_activity(STATE_IDLE, NULL);

			/* Tell the caller that this wasn't a WalSender command. */
			return false;

		default:
			elog(ERROR, "unrecognized replication command node tag: %u",
				 cmd_node->type);
	}

	/* done */
	MemoryContextSwitchTo(old_context);
	MemoryContextDelete(cmd_context);

	/* Send CommandComplete message */
	EndCommand("SELECT", DestRemote);

	/* Report to pgstat that this process is now idle */
	pgstat_report_activity(STATE_IDLE, NULL);

	return true;
}

/*
 * Process any incoming messages while streaming. Also checks if the remote
 * end has closed the connection.
 */
static void
ProcessRepliesIfAny(void)
{
	unsigned char firstchar;
	int			r;
	bool		received = false;

	last_processing = GetCurrentTimestamp();

	for (;;)
	{
		pq_startmsgread();
		r = pq_getbyte_if_available(&firstchar);
		if (r < 0)
		{
			/* unexpected error or EOF */
			ereport(COMMERROR,
					(errcode(ERRCODE_PROTOCOL_VIOLATION),
					 errmsg("unexpected EOF on standby connection")));
			proc_exit(0);
		}
		if (r == 0)
		{
			/* no data available without blocking */
			pq_endmsgread();
			break;
		}

		/* Read the message contents */
		resetStringInfo(&reply_message);
		if (pq_getmessage(&reply_message, 0))
		{
			ereport(COMMERROR,
					(errcode(ERRCODE_PROTOCOL_VIOLATION),
					 errmsg("unexpected EOF on standby connection")));
			proc_exit(0);
		}

		/*
		 * If we already received a CopyDone from the frontend, the frontend
		 * should not send us anything until we've closed our end of the COPY.
		 * XXX: In theory, the frontend could already send the next command
		 * before receiving the CopyDone, but libpq doesn't currently allow
		 * that.
		 */
		if (streamingDoneReceiving && firstchar != 'X')
			ereport(FATAL,
					(errcode(ERRCODE_PROTOCOL_VIOLATION),
					 errmsg("unexpected standby message type \"%c\", after receiving CopyDone",
							firstchar)));

		/* Handle the very limited subset of commands expected in this phase */
		switch (firstchar)
		{
				/*
				 * 'd' means a standby reply wrapped in a CopyData packet.
				 */
			case 'd':
				ProcessStandbyMessage();
				received = true;
				break;

				/*
				 * CopyDone means the standby requested to finish streaming.
				 * Reply with CopyDone, if we had not sent that already.
				 */
			case 'c':
				if (!streamingDoneSending)
				{
					pq_putmessage_noblock('c', NULL, 0);
					streamingDoneSending = true;
				}

				streamingDoneReceiving = true;
				received = true;
				break;

				/*
				 * 'X' means that the standby is closing down the socket.
				 */
			case 'X':
				proc_exit(0);

			default:
				ereport(FATAL,
						(errcode(ERRCODE_PROTOCOL_VIOLATION),
						 errmsg("invalid standby message type \"%c\"",
								firstchar)));
		}
	}

	/*
	 * Save the last reply timestamp if we've received at least one reply.
	 */
	if (received)
	{
		last_reply_timestamp = last_processing;
		waiting_for_ping_response = false;
	}
}

/*
 * Process a status update message received from standby.
 */
static void
ProcessStandbyMessage(void)
{
	char		msgtype;

	/*
	 * Check message type from the first byte.
	 */
	msgtype = pq_getmsgbyte(&reply_message);

	switch (msgtype)
	{
		case 'r':
			ProcessStandbyReplyMessage();
			break;

		case 'h':
			ProcessStandbyHSFeedbackMessage();
			break;

		default:
			ereport(COMMERROR,
					(errcode(ERRCODE_PROTOCOL_VIOLATION),
					 errmsg("unexpected message type \"%c\"", msgtype)));
			proc_exit(0);
	}
}

/*
 * Remember that a walreceiver just confirmed receipt of lsn `lsn`.
 */
static void
PhysicalConfirmReceivedLocation(XLogRecPtr lsn)
{
	bool		changed = false;
	ReplicationSlot *slot = MyReplicationSlot;

	Assert(lsn != InvalidXLogRecPtr);
	SpinLockAcquire(&slot->mutex);
	if (slot->data.restart_lsn != lsn)
	{
		changed = true;
		slot->data.restart_lsn = lsn;
	}
	SpinLockRelease(&slot->mutex);

	if (changed)
	{
		ReplicationSlotMarkDirty();
		ReplicationSlotsComputeRequiredLSN();
	}

	/*
	 * One could argue that the slot should be saved to disk now, but that'd
	 * be energy wasted - the worst lost information can do here is give us
	 * wrong information in a statistics view - we'll just potentially be more
	 * conservative in removing files.
	 */
}

/*
 * Regular reply from standby advising of WAL locations on standby server.
 */
static void
ProcessStandbyReplyMessage(void)
{
	XLogRecPtr	writePtr,
				flushPtr,
				applyPtr;
	bool		replyRequested;
	TimeOffset	writeLag,
				flushLag,
				applyLag;
	bool		clearLagTimes;
	TimestampTz now;

	static bool fullyAppliedLastTime = false;

	/* the caller already consumed the msgtype byte */
	writePtr = pq_getmsgint64(&reply_message);
	flushPtr = pq_getmsgint64(&reply_message);
	applyPtr = pq_getmsgint64(&reply_message);
	(void) pq_getmsgint64(&reply_message);	/* sendTime; not used ATM */
	replyRequested = pq_getmsgbyte(&reply_message);

	elog(DEBUG2, "write %X/%X flush %X/%X apply %X/%X%s",
		 (uint32) (writePtr >> 32), (uint32) writePtr,
		 (uint32) (flushPtr >> 32), (uint32) flushPtr,
		 (uint32) (applyPtr >> 32), (uint32) applyPtr,
		 replyRequested ? " (reply requested)" : "");

	if (enable_distri_print)
		elog(DEBUG2, "write %X/%X flush %X/%X apply %X/%X%s",
			 (uint32) (writePtr >> 32), (uint32) writePtr,
			 (uint32) (flushPtr >> 32), (uint32) flushPtr,
			 (uint32) (applyPtr >> 32), (uint32) applyPtr,
			 replyRequested ? " (reply requested)" : "");

	/* See if we can compute the round-trip lag for these positions. */
	now = GetCurrentTimestamp();
	writeLag = LagTrackerRead(SYNC_REP_WAIT_WRITE, writePtr, now);
	flushLag = LagTrackerRead(SYNC_REP_WAIT_FLUSH, flushPtr, now);
	applyLag = LagTrackerRead(SYNC_REP_WAIT_APPLY, applyPtr, now);

	/*
	 * If the standby reports that it has fully replayed the WAL in two
	 * consecutive reply messages, then the second such message must result
	 * from wal_receiver_status_interval expiring on the standby.  This is a
	 * convenient time to forget the lag times measured when it last
	 * wrote/flushed/applied a WAL record, to avoid displaying stale lag data
	 * until more WAL traffic arrives.
	 */
	clearLagTimes = false;
	if (applyPtr == sentPtr)
	{
		if (fullyAppliedLastTime)
			clearLagTimes = true;
		fullyAppliedLastTime = true;
	}
	else
		fullyAppliedLastTime = false;

	/* Send a reply if the standby requested one. */
	if (replyRequested)
		WalSndKeepalive(false);

	/*
	 * Update shared state for this WalSender process based on reply data from
	 * standby.
	 */
	{
		WalSnd	   *walsnd = MyWalSnd;

		SpinLockAcquire(&walsnd->mutex);
		walsnd->write = writePtr;
		walsnd->flush = flushPtr;
		walsnd->apply = applyPtr;
		if (writeLag != -1 || clearLagTimes)
			walsnd->writeLag = writeLag;
		if (flushLag != -1 || clearLagTimes)
			walsnd->flushLag = flushLag;
		if (applyLag != -1 || clearLagTimes)
			walsnd->applyLag = applyLag;
		SpinLockRelease(&walsnd->mutex);
	}

	if (!am_cascading_walsender)
		SyncRepReleaseWaiters();

	/*
	 * Advance our local xmin horizon when the client confirmed a flush.
	 */
	if (MyReplicationSlot && flushPtr != InvalidXLogRecPtr)
	{
		if (SlotIsLogical(MyReplicationSlot))
			LogicalConfirmReceivedLocation(flushPtr);
		else
			PhysicalConfirmReceivedLocation(flushPtr);
	}
}

/* compute new replication slot xmin horizon if needed */
static void
PhysicalReplicationSlotNewXmin(TransactionId feedbackXmin, TransactionId feedbackCatalogXmin)
{
	bool		changed = false;
	ReplicationSlot *slot = MyReplicationSlot;

	SpinLockAcquire(&slot->mutex);
	MyPgXact->xmin = InvalidTransactionId;

	/*
	 * For physical replication we don't need the interlock provided by xmin
	 * and effective_xmin since the consequences of a missed increase are
	 * limited to query cancellations, so set both at once.
	 */
	if (!TransactionIdIsNormal(slot->data.xmin) ||
		!TransactionIdIsNormal(feedbackXmin) ||
		TransactionIdPrecedes(slot->data.xmin, feedbackXmin))
	{
		changed = true;
		slot->data.xmin = feedbackXmin;
		slot->effective_xmin = feedbackXmin;
	}
	if (!TransactionIdIsNormal(slot->data.catalog_xmin) ||
		!TransactionIdIsNormal(feedbackCatalogXmin) ||
		TransactionIdPrecedes(slot->data.catalog_xmin, feedbackCatalogXmin))
	{
		changed = true;
		slot->data.catalog_xmin = feedbackCatalogXmin;
		slot->effective_catalog_xmin = feedbackCatalogXmin;
	}
	SpinLockRelease(&slot->mutex);

	if (changed)
	{
		ReplicationSlotMarkDirty();
		ReplicationSlotsComputeRequiredXmin(false);
	}
}

/* POLAR end */

/*
 * Check that the provided xmin/epoch are sane, that is, not in the future
 * and not so far back as to be already wrapped around.
 *
 * Epoch of nextXid should be same as standby, or if the counter has
 * wrapped, then one greater than standby.
 *
 * This check doesn't care about whether clog exists for these xids
 * at all.
 */
static bool
TransactionIdInRecentPast(TransactionId xid, uint32 epoch)
{
	TransactionId nextXid;
	uint32		nextEpoch;

	/*
	 * POLAR: get primary's nextXid and epoch from polar_datamax_ctl when in
	 * datamax mode
	 */
	if (!polar_is_dma_logger_node())
		GetNextXidAndEpoch(&nextXid, &nextEpoch);
	else
	{
		nextXid = pg_atomic_read_u32(&polar_datamax_ctl->polar_primary_next_xid);
		nextEpoch = pg_atomic_read_u32(&polar_datamax_ctl->polar_primary_epoch);
	}
	/* POLAR end */

	if (xid <= nextXid)
	{
		if (epoch != nextEpoch)
			return false;
	}
	else
	{
		if (epoch + 1 != nextEpoch)
			return false;
	}

	if (!TransactionIdPrecedesOrEquals(xid, nextXid))
		return false;			/* epoch OK, but it's wrapped around */

	return true;
}

/*
 * Hot Standby feedback
 */
static void
ProcessStandbyHSFeedbackMessage(void)
{
	TransactionId feedbackXmin;
	uint32		feedbackEpoch;
	TransactionId feedbackCatalogXmin;
	uint32		feedbackCatalogEpoch;

	/*
	 * Decipher the reply message. The caller already consumed the msgtype
	 * byte. See XLogWalRcvSendHSFeedback() in walreceiver.c for the creation
	 * of this message.
	 */
	(void) pq_getmsgint64(&reply_message);	/* sendTime; not used ATM */
	feedbackXmin = pq_getmsgint(&reply_message, 4);
	feedbackEpoch = pq_getmsgint(&reply_message, 4);
	feedbackCatalogXmin = pq_getmsgint(&reply_message, 4);
	feedbackCatalogEpoch = pq_getmsgint(&reply_message, 4);

	elog(DEBUG2, "hot standby feedback xmin %u epoch %u, catalog_xmin %u epoch %u",
		 feedbackXmin,
		 feedbackEpoch,
		 feedbackCatalogXmin,
		 feedbackCatalogEpoch);

	/*
	 * Unset WalSender's xmins if the feedback message values are invalid.
	 * This happens when the downstream turned hot_standby_feedback off.
	 */
	if (!TransactionIdIsNormal(feedbackXmin)
		&& !TransactionIdIsNormal(feedbackCatalogXmin))
	{
		MyPgXact->xmin = InvalidTransactionId;
		if (MyReplicationSlot != NULL)
			PhysicalReplicationSlotNewXmin(feedbackXmin, feedbackCatalogXmin);
		return;
	}

	/*
	 * Check that the provided xmin/epoch are sane, that is, not in the future
	 * and not so far back as to be already wrapped around.  Ignore if not.
	 */
	if (TransactionIdIsNormal(feedbackXmin) &&
		!TransactionIdInRecentPast(feedbackXmin, feedbackEpoch))
		return;

	if (TransactionIdIsNormal(feedbackCatalogXmin) &&
		!TransactionIdInRecentPast(feedbackCatalogXmin, feedbackCatalogEpoch))
		return;

	/*
	 * Set the WalSender's xmin equal to the standby's requested xmin, so that
	 * the xmin will be taken into account by GetOldestXmin.  This will hold
	 * back the removal of dead rows and thereby prevent the generation of
	 * cleanup conflicts on the standby server.
	 *
	 * There is a small window for a race condition here: although we just
	 * checked that feedbackXmin precedes nextXid, the nextXid could have
	 * gotten advanced between our fetching it and applying the xmin below,
	 * perhaps far enough to make feedbackXmin wrap around.  In that case the
	 * xmin we set here would be "in the future" and have no effect.  No point
	 * in worrying about this since it's too late to save the desired data
	 * anyway.  Assuming that the standby sends us an increasing sequence of
	 * xmins, this could only happen during the first reply cycle, else our
	 * own xmin would prevent nextXid from advancing so far.
	 *
	 * We don't bother taking the ProcArrayLock here.  Setting the xmin field
	 * is assumed atomic, and there's no real need to prevent a concurrent
	 * GetOldestXmin.  (If we're moving our xmin forward, this is obviously
	 * safe, and if we're moving it backwards, well, the data is at risk
	 * already since a VACUUM could have just finished calling GetOldestXmin.)
	 *
	 * If we're using a replication slot we reserve the xmin via that,
	 * otherwise via the walsender's PGXACT entry. We can only track the
	 * catalog xmin separately when using a slot, so we store the least of the
	 * two provided when not using a slot.
	 *
	 * XXX: It might make sense to generalize the ephemeral slot concept and
	 * always use the slot mechanism to handle the feedback xmin.
	 */
	if (MyReplicationSlot != NULL)	/* XXX: persistency configurable? */
		PhysicalReplicationSlotNewXmin(feedbackXmin, feedbackCatalogXmin);

	/*
	 * POLAR: no need to update MyPgXact->xmin in datamax mode MyPgXact->xmin
	 * is used to compute oldestxmin for vacuum there is no primary data in
	 * datamax mode
	 */
	else if (!polar_is_dma_logger_node())
	{
		if (TransactionIdIsNormal(feedbackCatalogXmin)
			&& TransactionIdPrecedes(feedbackCatalogXmin, feedbackXmin))
			MyPgXact->xmin = feedbackCatalogXmin;
		else
			MyPgXact->xmin = feedbackXmin;
	}
	/* POLAR end */
}

/*
 * Compute how long send/receive loops should sleep.
 *
 * If wal_sender_timeout is enabled we want to wake up in time to send
 * keepalives and to abort the connection if wal_sender_timeout has been
 * reached.
 */
static long
WalSndComputeSleeptime(TimestampTz now)
{
	long		sleeptime = 10000;	/* 10 s */

	if (wal_sender_timeout > 0 && last_reply_timestamp > 0)
	{
		TimestampTz wakeup_time;
		long		sec_to_timeout;
		int			microsec_to_timeout;

		/*
		 * At the latest stop sleeping once wal_sender_timeout has been
		 * reached.
		 */
		wakeup_time = TimestampTzPlusMilliseconds(last_reply_timestamp,
												  wal_sender_timeout);

		/*
		 * If no ping has been sent yet, wakeup when it's time to do so.
		 * WalSndKeepaliveIfNecessary() wants to send a keepalive once half of
		 * the timeout passed without a response.
		 */
		if (!waiting_for_ping_response)
			wakeup_time = TimestampTzPlusMilliseconds(last_reply_timestamp,
													  wal_sender_timeout / 2);

		/* Compute relative time until wakeup. */
		TimestampDifference(now, wakeup_time,
							&sec_to_timeout, &microsec_to_timeout);

		sleeptime = sec_to_timeout * 1000 +
			microsec_to_timeout / 1000;
	}

	return sleeptime;
}

/*
 * Check whether there have been responses by the client within
 * wal_sender_timeout and shutdown if not.  Using last_processing as the
 * reference point avoids counting server-side stalls against the client.
 * However, a long server-side stall can make WalSndKeepaliveIfNecessary()
 * postdate last_processing by more than wal_sender_timeout.  If that happens,
 * the client must reply almost immediately to avoid a timeout.  This rarely
 * affects the default configuration, under which clients spontaneously send a
 * message every standby_message_timeout = wal_sender_timeout/6 = 10s.  We
 * could eliminate that problem by recognizing timeout expiration at
 * wal_sender_timeout/2 after the keepalive.
 */
static void
WalSndCheckTimeOut(void)
{
	TimestampTz timeout;

	/* don't bail out if we're doing something that doesn't require timeouts */
	if (last_reply_timestamp <= 0)
		return;

	timeout = TimestampTzPlusMilliseconds(last_reply_timestamp,
										  wal_sender_timeout);

	if (wal_sender_timeout > 0 && last_processing >= timeout)
	{
		/*
		 * Since typically expiration of replication timeout means
		 * communication problem, we don't send the error message to the
		 * standby.
		 */
		ereport(COMMERROR,
				(errmsg("terminating walsender process due to replication timeout")));

		WalSndShutdown();
	}
}

/* Main loop of walsender process that streams the WAL over Copy messages. */
static void
WalSndLoop(WalSndSendDataCallback send_data)
{
	/*
	 * Initialize the last reply timestamp. That enables timeout processing
	 * from hereon.
	 */
	last_reply_timestamp = GetCurrentTimestamp();
	waiting_for_ping_response = false;

	/*
	 * Loop until we reach the end of this timeline or the client requests to
	 * stop streaming.
	 */
	for (;;)
	{
		/*
		 * Emergency bailout if postmaster has died.  This is to avoid the
		 * necessity for manual cleanup of all postmaster children.
		 */
		if (!PostmasterIsAlive())
			exit(1);

		/* Clear any already-pending wakeups */
		ResetLatch(MyLatch);

		CHECK_FOR_INTERRUPTS();

		/* Process any requests or signals received recently */
		if (ConfigReloadPending)
		{
			ConfigReloadPending = false;
			ProcessConfigFile(PGC_SIGHUP);
			SyncRepInitConfig();
		}

		/* Check for input from the client */
		ProcessRepliesIfAny();

		/*
		 * If we have received CopyDone from the client, sent CopyDone
		 * ourselves, and the output buffer is empty, it's time to exit
		 * streaming.
		 */
		if (streamingDoneReceiving && streamingDoneSending &&
			!pq_is_send_pending())
			break;

		/*
		 * If we don't have any pending data in the output buffer, try to send
		 * some more.  If there is some, we don't bother to call send_data
		 * again until we've flushed it ... but we'd better assume we are not
		 * caught up.
		 */
		if (!pq_is_send_pending())
			send_data();
		else
			WalSndCaughtUp = false;

		/* Try to flush pending output to the client */
		if (pq_flush_if_writable() != 0)
			WalSndShutdown();

		/* If nothing remains to be sent right now ... */
		if (WalSndCaughtUp && !pq_is_send_pending())
		{
			/*
			 * If we're in catchup state, move to streaming.  This is an
			 * important state change for users to know about, since before
			 * this point data loss might occur if the primary dies and we
			 * need to failover to the standby. The state change is also
			 * important for synchronous replication, since commits that
			 * started to wait at that point might wait for some time.
			 */
			if (MyWalSnd->state == WALSNDSTATE_CATCHUP)
			{
				ereport(DEBUG1,
						(errmsg("\"%s\" has now caught up with upstream server",
								application_name)));
				WalSndSetState(WALSNDSTATE_STREAMING);
			}

			/*
			 * When SIGUSR2 arrives, we send any outstanding logs up to the
			 * shutdown checkpoint record (i.e., the latest record), wait for
			 * them to be replicated to the standby, and exit. This may be a
			 * normal termination at shutdown, or a promotion, the walsender
			 * is not sure which.
			 */
			if (got_SIGUSR2)
				WalSndDone(send_data);
		}

		/* Check for replication timeout. */
		WalSndCheckTimeOut();

		/* Send keepalive if the time has come */
		WalSndKeepaliveIfNecessary();

		/*
		 * We don't block if not caught up, unless there is unsent data
		 * pending in which case we'd better block until the socket is
		 * write-ready.  This test is only needed for the case where the
		 * send_data callback handled a subset of the available data but then
		 * pq_flush_if_writable flushed it all --- we should immediately try
		 * to send more.
		 */
		if ((WalSndCaughtUp && !streamingDoneSending) || pq_is_send_pending())
		{
			long		sleeptime;
			int			wakeEvents;

			wakeEvents = WL_LATCH_SET | WL_POSTMASTER_DEATH | WL_TIMEOUT |
				WL_SOCKET_READABLE;

			/*
			 * Use fresh timestamp, not last_processed, to reduce the chance
			 * of reaching wal_sender_timeout before sending a keepalive.
			 */
			sleeptime = WalSndComputeSleeptime(GetCurrentTimestamp());

			if (pq_is_send_pending())
				wakeEvents |= WL_SOCKET_WRITEABLE;

			/* Sleep until something happens or we time out */
			WaitLatchOrSocket(MyLatch, wakeEvents,
							  MyProcPort->sock, sleeptime,
							  WAIT_EVENT_WAL_SENDER_MAIN);
		}
	}
	return;
}

/* Initialize a per-walsender data structure for this walsender process */
static void
InitWalSenderSlot(void)
{
	int			i;

	/*
	 * WalSndCtl should be set up already (we inherit this by fork() or
	 * EXEC_BACKEND mechanism from the postmaster).
	 */
	Assert(WalSndCtl != NULL);
	Assert(MyWalSnd == NULL);

	/*
	 * Find a free walsender slot and reserve it. If this fails, we must be
	 * out of WalSnd structures.
	 */
	for (i = 0; i < max_wal_senders; i++)
	{
		WalSnd	   *walsnd = &WalSndCtl->walsnds[i];

		SpinLockAcquire(&walsnd->mutex);

		if (walsnd->pid != 0)
		{
			SpinLockRelease(&walsnd->mutex);
			continue;
		}
		else
		{
			/*
			 * Found a free slot. Reserve it for us.
			 */
			walsnd->pid = MyProcPid;
			walsnd->sentPtr = InvalidXLogRecPtr;
			walsnd->write = InvalidXLogRecPtr;
			walsnd->flush = InvalidXLogRecPtr;
			walsnd->apply = InvalidXLogRecPtr;
			walsnd->writeLag = -1;
			walsnd->flushLag = -1;
			walsnd->applyLag = -1;
			walsnd->state = WALSNDSTATE_STARTUP;
			walsnd->latch = &MyProc->procLatch;
			SpinLockRelease(&walsnd->mutex);
			/* don't need the lock anymore */
			MyWalSnd = (WalSnd *) walsnd;

			if (WalSndCtl->sender_running == false)
			{
				LWLockAcquire(SyncRepLock, LW_EXCLUSIVE);
				WalSndCtl->sender_running = true;
				LWLockRelease(SyncRepLock);
			}

			break;
		}
	}
	if (MyWalSnd == NULL)
		ereport(FATAL,
				(errcode(ERRCODE_TOO_MANY_CONNECTIONS),
				 errmsg("number of requested standby connections "
						"exceeds max_wal_senders (currently %d)",
						max_wal_senders)));

	/* Arrange to clean up at walsender exit */
	on_shmem_exit(WalSndKill, 0);
}

/* Destroy the per-walsender data structure for this walsender process */
static void
WalSndKill(int code, Datum arg)
{
	WalSnd	   *walsnd = MyWalSnd;

	Assert(walsnd != NULL);

	MyWalSnd = NULL;

	SpinLockAcquire(&walsnd->mutex);
	/* clear latch while holding the spinlock, so it can safely be read */
	walsnd->latch = NULL;
	/* Mark WalSnd struct as no longer being in use. */
	walsnd->pid = 0;
	SpinLockRelease(&walsnd->mutex);
}

/*
 * Read 'count' bytes from WAL into 'buf', starting at location 'startptr'
 *
 * XXX probably this should be improved to suck data directly from the
 * WAL buffers when possible.
 *
 * Will open, and keep open, one WAL segment stored in the global file
 * descriptor sendFile. This means if XLogRead is used once, there will
 * always be one descriptor left open until the process ends, but never
 * more than one.
 */
static void
XLogRead(char *buf, XLogRecPtr startptr, Size count, polar_repl_mode_t polar_replication_mode)
{
	char	   *p;
	XLogRecPtr	recptr;
	Size		nbytes;
	XLogSegNo	segno;

retry:
	p = buf;
	recptr = startptr;
	nbytes = count;

	while (nbytes > 0)
	{
		uint32		startoff;
		int			segbytes;
		int			readbytes;

		startoff = XLogSegmentOffset(recptr, wal_segment_size);

		if (sendFile < 0 || !XLByteInSeg(recptr, sendSegNo, wal_segment_size))
		{
			char		path[MAXPGPATH];

			/* Switch to another logfile segment */
			if (sendFile >= 0)
				close(sendFile);

			XLByteToSeg(recptr, sendSegNo, wal_segment_size);

			/*-------
			 * When reading from a historic timeline, and there is a timeline
			 * switch within this segment, read from the WAL segment belonging
			 * to the new timeline.
			 *
			 * For example, imagine that this server is currently on timeline
			 * 5, and we're streaming timeline 4. The switch from timeline 4
			 * to 5 happened at 0/13002088. In pg_wal, we have these files:
			 *
			 * ...
			 * 000000040000000000000012
			 * 000000040000000000000013
			 * 000000050000000000000013
			 * 000000050000000000000014
			 * ...
			 *
			 * In this situation, when requested to send the WAL from
			 * segment 0x13, on timeline 4, we read the WAL from file
			 * 000000050000000000000013. Archive recovery prefers files from
			 * newer timelines, so if the segment was restored from the
			 * archive on this server, the file belonging to the old timeline,
			 * 000000040000000000000013, might not exist. Their contents are
			 * equal up to the switchpoint, because at a timeline switch, the
			 * used portion of the old segment is copied to the new file.
			 *-------
			 */
			curFileTimeLine = sendTimeLine;
			if (!POLAR_IS_DMA_REPL(polar_replication_mode) && sendTimeLineIsHistoric)
			{
				XLogSegNo	endSegNo;

				XLByteToSeg(sendTimeLineValidUpto, endSegNo, wal_segment_size);
				if (sendSegNo == endSegNo)
					curFileTimeLine = sendTimeLineNextTLI;
			}

			polar_is_dma_logger_mode = polar_is_dma_logger_node();	/* POLAR: set datamax
																	 * branch for xlog file
																	 * path */
			XLogFilePath(path, curFileTimeLine, sendSegNo, wal_segment_size);
			polar_is_dma_logger_mode = false;	/* POLAR: reset */

			sendFile = BasicOpenFile(path, O_RDONLY | PG_BINARY);
			if (sendFile < 0)
			{
				/*
				 * If the file is not found, assume it's because the standby
				 * asked for a too old WAL segment that has already been
				 * removed or recycled.
				 */
				if (errno == ENOENT)
					ereport(ERROR,
							(errcode_for_file_access(),
							 errmsg("requested WAL segment %s has already been removed",
									XLogFileNameP(curFileTimeLine, sendSegNo))));
				else
					ereport(ERROR,
							(errcode_for_file_access(),
							 errmsg("could not open file \"%s\": %m",
									path)));
			}
			sendOff = 0;
		}

		/* Need to seek in the file? */
		if (sendOff != startoff)
		{
			if (lseek(sendFile, (off_t) startoff, SEEK_SET) < 0)
				ereport(ERROR,
						(errcode_for_file_access(),
						 errmsg("could not seek in log segment %s to offset %u: %m",
								XLogFileNameP(curFileTimeLine, sendSegNo),
								startoff)));
			sendOff = startoff;
		}

		/* How many bytes are within this segment? */
		if (nbytes > (wal_segment_size - startoff))
			segbytes = wal_segment_size - startoff;
		else
			segbytes = nbytes;

		pgstat_report_wait_start(WAIT_EVENT_WAL_READ);
		readbytes = read(sendFile, p, segbytes);
		pgstat_report_wait_end();
		if (readbytes <= 0)
		{
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not read from log segment %s, offset %u, length %lu: %m",
							XLogFileNameP(curFileTimeLine, sendSegNo),
							sendOff, (unsigned long) segbytes)));
		}

		/* Update state for read */
		recptr += readbytes;

		sendOff += readbytes;
		nbytes -= readbytes;
		p += readbytes;
	}

	/*
	 * After reading into the buffer, check that what we read was valid. We do
	 * this after reading, because even though the segment was present when we
	 * opened it, it might get recycled or removed while we read it. The
	 * read() succeeds in that case, but the data we tried to read might
	 * already have been overwritten with new WAL records.
	 */
	XLByteToSeg(startptr, segno, wal_segment_size);
	CheckXLogRemoved(segno, ThisTimeLineID);

	/*
	 * During recovery, the currently-open WAL file might be replaced with the
	 * file of the same name retrieved from archive. So we always need to
	 * check what we read was valid after reading into the buffer. If it's
	 * invalid, we try to open and read the file again.
	 */
	if (am_cascading_walsender)
	{
		WalSnd	   *walsnd = MyWalSnd;
		bool		reload;

		SpinLockAcquire(&walsnd->mutex);
		reload = walsnd->needreload;
		walsnd->needreload = false;
		SpinLockRelease(&walsnd->mutex);

		if (reload && sendFile >= 0)
		{
			close(sendFile);
			sendFile = -1;

			goto retry;
		}
	}
}

/*
 * Send out the WAL in its normal physical/stored form.
 *
 * Read up to MAX_SEND_SIZE bytes of WAL that's been flushed to disk,
 * but not yet sent to the client, and buffer it in the libpq output
 * buffer.
 *
 * If there is no unsent WAL remaining, WalSndCaughtUp is set to true,
 * otherwise WalSndCaughtUp is set to false.
 */
static void
XLogSendPhysical(void)
{
	XLogSendPhysicalExt(POLAR_REPL_DEFAULT);
}

/*
 * POLAR: Eextend function for support polar replica mode and polar datamax mode
 */
static void
XLogSendPhysicalExt(polar_repl_mode_t polar_replication_mode)
{
	XLogRecPtr	SendRqstPtr;
	XLogRecPtr	startptr;
	XLogRecPtr	endptr;
	Size		nbytes;

	/* POLAR */
	TransactionId polar_primary_next_xid;	/* record primary next_xid */
	uint32		polar_primary_epoch;	/* record primary epoch */
	XLogSegNo	polar_last_removed_segno;	/* record primary's last removed
											 * segno */

	/* If requested switch the WAL sender to the stopping state. */
	if (got_STOPPING)
		WalSndSetState(WALSNDSTATE_STOPPING);

	if (streamingDoneSending)
	{
		WalSndCaughtUp = true;
		return;
	}

	/* Figure out how far we can safely send the WAL. */
	if (sendTimeLineIsHistoric)
	{
		/*
		 * Streaming an old timeline that's in this server's history, but is
		 * not the one we're currently inserting or replaying. It can be
		 * streamed up to the point where we switched off that timeline.
		 */
		SendRqstPtr = sendTimeLineValidUpto;
	}
	else if (am_cascading_walsender)
	{
		/*
		 * Streaming the latest timeline on a standby.
		 *
		 * Attempt to send all WAL that has already been replayed, so that we
		 * know it's valid. If we're receiving WAL through streaming
		 * replication, it's also OK to send any WAL that has been received
		 * but not replayed.
		 *
		 * The timeline we're recovering from can change, or we can be
		 * promoted. In either case, the current timeline becomes historic. We
		 * need to detect that so that we don't try to stream past the point
		 * where we switched to another timeline. We check for promotion or
		 * timeline switch after calculating FlushPtr, to avoid a race
		 * condition: if the timeline becomes historic just after we checked
		 * that it was still current, it's still be OK to stream it up to the
		 * FlushPtr that was calculated before it became historic.
		 */
		bool		becameHistoric = false;

		if (polar_enable_dma)
			SendRqstPtr = polar_dma_get_flush_lsn();
		else
			SendRqstPtr = GetStandbyFlushRecPtr();

		if (!RecoveryInProgress())
		{
			/*
			 * We have been promoted. RecoveryInProgress() updated
			 * ThisTimeLineID to the new current timeline.
			 */
			am_cascading_walsender = false;

			/*
			 * POLAR: if in DMA mode, ThisTimeLineID will be advance to new
			 * timeline in polar_dma_get_flush_lsn and new timeline will be
			 * sent before exit from recovery status.
			 */
			if (!POLAR_IS_DMA_REPL(polar_replication_mode) || sendTimeLine != ThisTimeLineID)
				becameHistoric = true;
		}
		else
		{
			/*
			 * Still a cascading standby. But is the timeline we're sending
			 * still the one recovery is recovering from? ThisTimeLineID was
			 * updated by the GetStandbyFlushRecPtr() call above.
			 */
			if (sendTimeLine != ThisTimeLineID)
				becameHistoric = true;
		}

		if (becameHistoric)
		{
			/*
			 * The timeline we were sending has become historic. Read the
			 * timeline history file of the new timeline to see where exactly
			 * we forked off from the timeline we were sending.
			 */
			List	   *history;

			polar_is_dma_logger_mode = polar_is_dma_logger_node();	/* POLAR: set datamax
																	 * branch for xlog file
																	 * path */
			history = readTimeLineHistory(ThisTimeLineID);
			polar_is_dma_logger_mode = false;	/* POLAR: Leave datamax mode. */
			sendTimeLineValidUpto = tliSwitchPoint(sendTimeLine, history, &sendTimeLineNextTLI);

			Assert(sendTimeLine < sendTimeLineNextTLI);
			list_free_deep(history);

			sendTimeLineIsHistoric = true;

			SendRqstPtr = sendTimeLineValidUpto;
		}
	}
	else
	{
		/*
		 * Streaming the current timeline on a master.
		 *
		 * Attempt to send all data that's already been written out and
		 * fsync'd to disk.  We cannot go further than what's been written out
		 * given the current implementation of XLogRead().  And in any case
		 * it's unsafe to send WAL that is not securely down to disk on the
		 * master: if the master subsequently crashes and restarts, standbys
		 * must not have applied any WAL that got lost on the master.
		 */
		SendRqstPtr = GetFlushRecPtr();
	}

	/*
	 * Record the current system time as an approximation of the time at which
	 * this WAL location was written for the purposes of lag tracking.
	 *
	 * In theory we could make XLogFlush() record a time in shmem whenever WAL
	 * is flushed and we could get that time as well as the LSN when we call
	 * GetFlushRecPtr() above (and likewise for the cascading standby
	 * equivalent), but rather than putting any new code into the hot WAL path
	 * it seems good enough to capture the time here.  We should reach this
	 * after XLogFlush() runs WalSndWakeupProcessRequests(), and although that
	 * may take some time, we read the WAL flush pointer and take the time
	 * very close to together here so that we'll get a later position if it is
	 * still moving.
	 *
	 * Because LagTrackerWriter ignores samples when the LSN hasn't advanced,
	 * this gives us a cheap approximation for the WAL flush time for this
	 * LSN.
	 *
	 * Note that the LSN is not necessarily the LSN for the data contained in
	 * the present message; it's the end of the WAL, which might be further
	 * ahead.  All the lag tracking machinery cares about is finding out when
	 * that arbitrary LSN is eventually reported as written, flushed and
	 * applied, so that it can measure the elapsed time.
	 */
	LagTrackerWrite(SendRqstPtr, GetCurrentTimestamp());

	/*
	 * If this is a historic timeline and we've reached the point where we
	 * forked to the next timeline, stop streaming.
	 *
	 * Note: We might already have sent WAL > sendTimeLineValidUpto. The
	 * startup process will normally replay all WAL that has been received
	 * from the master, before promoting, but if the WAL streaming is
	 * terminated at a WAL page boundary, the valid portion of the timeline
	 * might end in the middle of a WAL record. We might've already sent the
	 * first half of that partial WAL record to the cascading standby, so that
	 * sentPtr > sendTimeLineValidUpto. That's OK; the cascading standby can't
	 * replay the partial WAL record either, so it can still follow our
	 * timeline switch.
	 */
	if (sendTimeLineIsHistoric && sendTimeLineValidUpto <= sentPtr)
	{
		/* close the current file. */
		if (sendFile >= 0)
			close(sendFile);
		sendFile = -1;

		/* Send CopyDone */
		pq_putmessage_noblock('c', NULL, 0);
		streamingDoneSending = true;

		WalSndCaughtUp = true;

		elog(DEBUG1, "walsender reached end of timeline at %X/%X (sent up to %X/%X)",
			 (uint32) (sendTimeLineValidUpto >> 32), (uint32) sendTimeLineValidUpto,
			 (uint32) (sentPtr >> 32), (uint32) sentPtr);
		return;
	}

	/* Do we have any work to do? */
	Assert(POLAR_IS_DMA_REPL(polar_replication_mode) || sentPtr <= SendRqstPtr);
	if (SendRqstPtr <= sentPtr)
	{
		WalSndCaughtUp = true;
		return;
	}

	/*
	 * Figure out how much to send in one message. If there's no more than
	 * MAX_SEND_SIZE bytes to send, send everything. Otherwise send
	 * MAX_SEND_SIZE bytes, but round back to logfile or page boundary.
	 *
	 * The rounding is not only for performance reasons. Walreceiver relies on
	 * the fact that we never split a WAL record across two messages. Since a
	 * long WAL record is split at page boundary into continuation records,
	 * page boundary is always a safe cut-off point. We also assume that
	 * SendRqstPtr never points to the middle of a WAL record.
	 */
	startptr = sentPtr;
	endptr = startptr;
	endptr += MAX_SEND_SIZE;

	/* if we went beyond SendRqstPtr, back off */
	if (SendRqstPtr <= endptr)
	{
		endptr = SendRqstPtr;
		if (sendTimeLineIsHistoric)
			WalSndCaughtUp = false;
		else
			WalSndCaughtUp = true;
	}
	else
	{
		/* round down to page boundary. */
		endptr -= (endptr % XLOG_BLCKSZ);
		WalSndCaughtUp = false;
	}

	nbytes = endptr - startptr;
	Assert(nbytes <= MAX_SEND_SIZE);

	/*
	 * OK to read and send the slice.
	 */
	resetStringInfo(&output_message);

	/*
	 * POLAR: when downstream mode is datamax we need to send nextXid and
	 * Epoch to verify if xmin/epoch of standby is sane we also send
	 * last_valid_lsn to ensure xlog consistency between primary and datamax
	 */
	if (polar_replication_mode == POLAR_REPL_DMA_LOGGER)
		pq_sendbyte(&output_message, 'e');
	else
		pq_sendbyte(&output_message, 'w');

	pq_sendint64(&output_message, startptr);	/* dataStart */
	pq_sendint64(&output_message, SendRqstPtr); /* walEnd */

	{
		/*
		 * POLAR: when downstream mode is datamax, send 1) nextxid and
		 * nextepoch to verify the sanity of standby xmin in datamax node 2)
		 * current last valid lsn to avoid wal inconsistency between upstream
		 * and downstream 3) last_remove_segno to avoid datamax removing wal
		 * which haven't been removed in upstream
		 */
		if (polar_replication_mode == POLAR_REPL_DMA_LOGGER)
		{
			if (!polar_is_dma_logger_node())
				GetNextXidAndEpoch(&polar_primary_next_xid, &polar_primary_epoch);

			/*
			 * get next_xid and next_epoch from polar_datamax_ctl when in
			 * datamax mode
			 */
			else
			{
				polar_primary_next_xid = pg_atomic_read_u32(&polar_datamax_ctl->polar_primary_next_xid);
				polar_primary_epoch = pg_atomic_read_u32(&polar_datamax_ctl->polar_primary_epoch);
			}
			pq_sendint32(&output_message, polar_primary_next_xid);	/* current next xid */
			pq_sendint32(&output_message, polar_primary_epoch); /* current epoch */
			polar_last_removed_segno = XLogGetLastRemovedSegno();	/* current last removed
																	 * segno */
			pq_sendint64(&output_message, polar_last_removed_segno);
		}
		/* POLAR end */
		pq_sendint64(&output_message, 0);	/* sendtime, filled in last */

		/*
		 * Read the log directly into the output buffer to avoid extra memcpy
		 * calls.
		 */
		enlargeStringInfo(&output_message, nbytes);
		XLogRead(&output_message.data[output_message.len], startptr, nbytes, polar_replication_mode);
		output_message.len += nbytes;
		output_message.data[output_message.len] = '\0';

		/*
		 * Fill the send timestamp last, so that it is taken as late as
		 * possible.
		 */
		resetStringInfo(&tmpbuf);
		pq_sendint64(&tmpbuf, GetCurrentTimestamp());
		memcpy(&output_message.data[1 + sizeof(int64) + sizeof(int64)],
			   tmpbuf.data, sizeof(int64));
	}

	pq_putmessage_noblock('d', output_message.data, output_message.len);

	sentPtr = endptr;

	/* Update shared memory status */
	{
		WalSnd	   *walsnd = MyWalSnd;

		SpinLockAcquire(&walsnd->mutex);
		walsnd->sentPtr = sentPtr;
		SpinLockRelease(&walsnd->mutex);
	}

	if (update_process_title)
	{
		char		activitymsg[50];

		snprintf(activitymsg, sizeof(activitymsg), "streaming %X/%X",
				 (uint32) (sentPtr >> 32), (uint32) sentPtr);
		set_ps_display(activitymsg, false);
	}

	return;
}

/*
 * Stream out logically decoded data.
 */
static void
XLogSendLogical(void)
{
	XLogRecord *record;
	char	   *errm;

	/*
	 * Don't know whether we've caught up yet. We'll set WalSndCaughtUp to
	 * true in WalSndWaitForWal, if we're actually waiting. We also set to
	 * true if XLogReadRecord() had to stop reading but WalSndWaitForWal
	 * didn't wait - i.e. when we're shutting down.
	 */
	WalSndCaughtUp = false;

	record = XLogReadRecord(logical_decoding_ctx->reader, logical_startptr, &errm);
	logical_startptr = InvalidXLogRecPtr;

	/* xlog record was invalid */
	if (errm != NULL)
		elog(ERROR, "%s", errm);

	if (record != NULL)
	{
		/* XXX: Note that logical decoding cannot be used while in recovery */
		XLogRecPtr	flushPtr = GetFlushRecPtr();

		/*
		 * Note the lack of any call to LagTrackerWrite() which is handled by
		 * WalSndUpdateProgress which is called by output plugin through
		 * logical decoding write api.
		 */
		LogicalDecodingProcessRecord(logical_decoding_ctx, logical_decoding_ctx->reader);

		sentPtr = logical_decoding_ctx->reader->EndRecPtr;

		/*
		 * If we have sent a record that is at or beyond the flushed point, we
		 * have caught up.
		 */
		if (sentPtr >= flushPtr)
			WalSndCaughtUp = true;
	}
	else
	{
		/*
		 * If the record we just wanted read is at or beyond the flushed
		 * point, then we're caught up.
		 */
		if (logical_decoding_ctx->reader->EndRecPtr >= GetFlushRecPtr())
		{
			WalSndCaughtUp = true;

			/*
			 * Have WalSndLoop() terminate the connection in an orderly
			 * manner, after writing out all the pending data.
			 */
			if (got_STOPPING)
				got_SIGUSR2 = true;
		}
	}

	/* Update shared memory status */
	{
		WalSnd	   *walsnd = MyWalSnd;

		SpinLockAcquire(&walsnd->mutex);
		walsnd->sentPtr = sentPtr;
		SpinLockRelease(&walsnd->mutex);
	}
}

/*
 * Shutdown if the sender is caught up.
 *
 * NB: This should only be called when the shutdown signal has been received
 * from postmaster.
 *
 * Note that if we determine that there's still more data to send, this
 * function will return control to the caller.
 */
static void
WalSndDone(WalSndSendDataCallback send_data)
{
	XLogRecPtr	replicatedPtr;

	/* ... let's just be real sure we're caught up ... */
	send_data();

	/*
	 * To figure out whether all WAL has successfully been replicated, check
	 * flush location if valid, write otherwise. Tools like pg_receivewal will
	 * usually (unless in synchronous mode) return an invalid flush location.
	 */
	replicatedPtr = XLogRecPtrIsInvalid(MyWalSnd->flush) ?
		MyWalSnd->write : MyWalSnd->flush;

	if (WalSndCaughtUp && sentPtr == replicatedPtr &&
		!pq_is_send_pending())
	{
		/* Inform the standby that XLOG streaming is done */
		EndCommand("COPY 0", DestRemote);
		pq_flush();

		proc_exit(0);
	}
	if (!waiting_for_ping_response)
	{
		WalSndKeepalive(true);
		waiting_for_ping_response = true;
	}
}

/*
 * Returns the latest point in WAL that has been safely flushed to disk, and
 * can be sent to the standby. This should only be called when in recovery,
 * ie. we're streaming to a cascaded standby.
 *
 * As a side-effect, ThisTimeLineID is updated to the TLI of the last
 * replayed WAL record.
 */
static XLogRecPtr
GetStandbyFlushRecPtr(void)
{
	XLogRecPtr	replayPtr;
	TimeLineID	replayTLI;
	XLogRecPtr	receivePtr;
	TimeLineID	receiveTLI;
	XLogRecPtr	result;

	/*
	 * We can safely send what's already been replayed. Also, if walreceiver
	 * is streaming WAL from the same timeline, we can send anything that it
	 * has streamed, but hasn't been replayed yet.
	 */
	receivePtr = GetWalRcvWriteRecPtr(NULL, &receiveTLI);
	replayPtr = GetXLogReplayRecPtr(&replayTLI);

	ThisTimeLineID = replayTLI;

	result = replayPtr;
	if (receiveTLI == ThisTimeLineID && receivePtr > replayPtr)
		result = receivePtr;

	return result;
}

/*
 * Request walsenders to reload the currently-open WAL file
 */
void
WalSndRqstFileReload(void)
{
	int			i;

	for (i = 0; i < max_wal_senders; i++)
	{
		WalSnd	   *walsnd = &WalSndCtl->walsnds[i];

		SpinLockAcquire(&walsnd->mutex);
		if (walsnd->pid == 0)
		{
			SpinLockRelease(&walsnd->mutex);
			continue;
		}
		walsnd->needreload = true;
		SpinLockRelease(&walsnd->mutex);
	}
}

/*
 * Handle PROCSIG_WALSND_INIT_STOPPING signal.
 */
void
HandleWalSndInitStopping(void)
{
	Assert(am_walsender);

	/*
	 * If replication has not yet started, die like with SIGTERM. If
	 * replication is active, only set a flag and wake up the main loop. It
	 * will send any outstanding WAL, wait for it to be replicated to the
	 * standby, and then exit gracefully.
	 */
	if (!replication_active)
		kill(MyProcPid, SIGTERM);
	else
		got_STOPPING = true;
}

/*
 * SIGUSR2: set flag to do a last cycle and shut down afterwards. The WAL
 * sender should already have been switched to WALSNDSTATE_STOPPING at
 * this point.
 */
static void
WalSndLastCycleHandler(SIGNAL_ARGS)
{
	int			save_errno = errno;

	got_SIGUSR2 = true;
	SetLatch(MyLatch);

	errno = save_errno;
}

/* Set up signal handlers */
void
WalSndSignals(void)
{
	/* Set up signal handlers */
	pqsignal(SIGHUP, PostgresSigHupHandler);	/* set flag to read config
												 * file */
	pqsignal(SIGINT, StatementCancelHandler);	/* query cancel */
	pqsignal(SIGTERM, die);		/* request shutdown */
	pqsignal(SIGQUIT, quickdie);	/* hard crash time */
	InitializeTimeouts();		/* establishes SIGALRM handler */
	pqsignal(SIGPIPE, SIG_IGN);
	pqsignal(SIGUSR1, procsignal_sigusr1_handler);
	pqsignal(SIGUSR2, WalSndLastCycleHandler);	/* request a last cycle and
												 * shutdown */

	/* Reset some signals that are accepted by postmaster but not here */
	pqsignal(SIGCHLD, SIG_DFL);
	pqsignal(SIGTTIN, SIG_DFL);
	pqsignal(SIGTTOU, SIG_DFL);
	pqsignal(SIGCONT, SIG_DFL);
	pqsignal(SIGWINCH, SIG_DFL);
}

/* Report shared-memory space needed by WalSndShmemInit */
Size
WalSndShmemSize(void)
{
	Size		size = 0;

	size = offsetof(WalSndCtlData, walsnds);
	size = add_size(size, mul_size(max_wal_senders, sizeof(WalSnd)));

	return size;
}

/* Allocate and initialize walsender-related shared memory */
void
WalSndShmemInit(void)
{
	bool		found;
	int			i;

	WalSndCtl = (WalSndCtlData *)
		ShmemInitStruct("Wal Sender Ctl", WalSndShmemSize(), &found);

	if (!found)
	{
		/* First time through, so initialize */
		MemSet(WalSndCtl, 0, WalSndShmemSize());

		WalSndCtl->sender_running = false;

		for (i = 0; i < NUM_SYNC_REP_WAIT_MODE; i++)
			SHMQueueInit(&(WalSndCtl->SyncRepQueue[i]));

		for (i = 0; i < max_wal_senders; i++)
		{
			WalSnd	   *walsnd = &WalSndCtl->walsnds[i];

			SpinLockInit(&walsnd->mutex);
		}
	}
}

/*
 * Wake up all walsenders
 *
 * This will be called inside critical sections, so throwing an error is not
 * advisable.
 */
void
WalSndWakeup(void)
{
	int			i;

	for (i = 0; i < max_wal_senders; i++)
	{
		Latch	   *latch;
		WalSnd	   *walsnd = &WalSndCtl->walsnds[i];

		/*
		 * Get latch pointer with spinlock held, for the unlikely case that
		 * pointer reads aren't atomic (as they're 8 bytes).
		 */
		SpinLockAcquire(&walsnd->mutex);
		latch = walsnd->latch;
		SpinLockRelease(&walsnd->mutex);

		if (latch != NULL)
			SetLatch(latch);
	}
}

/*
 * Signal all walsenders to move to stopping state.
 *
 * This will trigger walsenders to move to a state where no further WAL can be
 * generated. See this file's header for details.
 */
void
WalSndInitStopping(void)
{
	int			i;

	for (i = 0; i < max_wal_senders; i++)
	{
		WalSnd	   *walsnd = &WalSndCtl->walsnds[i];
		pid_t		pid;

		SpinLockAcquire(&walsnd->mutex);
		pid = walsnd->pid;
		SpinLockRelease(&walsnd->mutex);

		if (pid == 0)
			continue;

		SendProcSignal(pid, PROCSIG_WALSND_INIT_STOPPING, InvalidBackendId);
	}
}

/*
 * Wait that all the WAL senders have quit or reached the stopping state. This
 * is used by the checkpointer to control when the shutdown checkpoint can
 * safely be performed.
 */
void
WalSndWaitStopping(void)
{
	for (;;)
	{
		int			i;
		bool		all_stopped = true;

		for (i = 0; i < max_wal_senders; i++)
		{
			WalSnd	   *walsnd = &WalSndCtl->walsnds[i];

			SpinLockAcquire(&walsnd->mutex);

			if (walsnd->pid == 0)
			{
				SpinLockRelease(&walsnd->mutex);
				continue;
			}

			if (walsnd->state != WALSNDSTATE_STOPPING)
			{
				all_stopped = false;
				SpinLockRelease(&walsnd->mutex);
				break;
			}
			SpinLockRelease(&walsnd->mutex);
		}

		/* safe to leave if confirmation is done for all WAL senders */
		if (all_stopped)
			return;

		pg_usleep(10000L);		/* wait for 10 msec */
	}
}

/* Set state for current walsender (only called in walsender) */
void
WalSndSetState(WalSndState state)
{
	WalSnd	   *walsnd = MyWalSnd;

	Assert(am_walsender);

	if (walsnd->state == state)
		return;

	SpinLockAcquire(&walsnd->mutex);
	walsnd->state = state;
	SpinLockRelease(&walsnd->mutex);
}

/*
 * Return a string constant representing the state. This is used
 * in system views, and should *not* be translated.
 */
static const char *
WalSndGetStateString(WalSndState state)
{
	switch (state)
	{
		case WALSNDSTATE_STARTUP:
			return "startup";
		case WALSNDSTATE_BACKUP:
			return "backup";
		case WALSNDSTATE_CATCHUP:
			return "catchup";
		case WALSNDSTATE_STREAMING:
			return "streaming";
		case WALSNDSTATE_STOPPING:
			return "stopping";
	}
	return "UNKNOWN";
}

static Interval *
offset_to_interval(TimeOffset offset)
{
	Interval   *result = palloc(sizeof(Interval));

	result->month = 0;
	result->day = 0;
	result->time = offset;

	return result;
}

/*
 * Returns activity of walsenders, including pids and xlog locations sent to
 * standby servers.
 */
Datum
pg_stat_get_wal_senders(PG_FUNCTION_ARGS)
{
#define PG_STAT_GET_WAL_SENDERS_COLS	11
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	TupleDesc	tupdesc;
	Tuplestorestate *tupstore;
	MemoryContext per_query_ctx;
	MemoryContext oldcontext;
	List	   *sync_standbys;
	int			i;

	/* check to see if caller supports us returning a tuplestore */
	if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("set-valued function called in context that cannot accept a set")));
	if (!(rsinfo->allowedModes & SFRM_Materialize))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("materialize mode required, but it is not " \
						"allowed in this context")));

	/* Build a tuple descriptor for our result type */
	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
	oldcontext = MemoryContextSwitchTo(per_query_ctx);

	tupstore = tuplestore_begin_heap(true, false, work_mem);
	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = tupstore;
	rsinfo->setDesc = tupdesc;

	MemoryContextSwitchTo(oldcontext);

	/*
	 * Get the currently active synchronous standbys.
	 */
	LWLockAcquire(SyncRepLock, LW_SHARED);
	sync_standbys = SyncRepGetSyncStandbys(NULL);
	LWLockRelease(SyncRepLock);

	for (i = 0; i < max_wal_senders; i++)
	{
		WalSnd	   *walsnd = &WalSndCtl->walsnds[i];
		XLogRecPtr	sentPtr;
		XLogRecPtr	write;
		XLogRecPtr	flush;
		XLogRecPtr	apply;
		TimeOffset	writeLag;
		TimeOffset	flushLag;
		TimeOffset	applyLag;
		int			priority;
		int			pid;
		WalSndState state;
		Datum		values[PG_STAT_GET_WAL_SENDERS_COLS];
		bool		nulls[PG_STAT_GET_WAL_SENDERS_COLS];

		SpinLockAcquire(&walsnd->mutex);
		if (walsnd->pid == 0)
		{
			SpinLockRelease(&walsnd->mutex);
			continue;
		}
		pid = walsnd->pid;
		sentPtr = walsnd->sentPtr;
		state = walsnd->state;
		write = walsnd->write;
		flush = walsnd->flush;
		apply = walsnd->apply;
		writeLag = walsnd->writeLag;
		flushLag = walsnd->flushLag;
		applyLag = walsnd->applyLag;
		priority = walsnd->sync_standby_priority;
		SpinLockRelease(&walsnd->mutex);

		memset(nulls, 0, sizeof(nulls));
		values[0] = Int32GetDatum(pid);

		if (!is_member_of_role(GetUserId(), DEFAULT_ROLE_READ_ALL_STATS))
		{
			/*
			 * Only superusers and members of pg_read_all_stats can see
			 * details. Other users only get the pid value to know it's a
			 * walsender, but no details.
			 */
			MemSet(&nulls[1], true, PG_STAT_GET_WAL_SENDERS_COLS - 1);
		}
		else
		{
			values[1] = CStringGetTextDatum(WalSndGetStateString(state));

			if (XLogRecPtrIsInvalid(sentPtr))
				nulls[2] = true;
			values[2] = LSNGetDatum(sentPtr);

			if (XLogRecPtrIsInvalid(write))
				nulls[3] = true;
			values[3] = LSNGetDatum(write);

			if (XLogRecPtrIsInvalid(flush))
				nulls[4] = true;
			values[4] = LSNGetDatum(flush);

			if (XLogRecPtrIsInvalid(apply))
				nulls[5] = true;
			values[5] = LSNGetDatum(apply);

			/*
			 * Treat a standby such as a pg_basebackup background process
			 * which always returns an invalid flush location, as an
			 * asynchronous standby.
			 */
			priority = XLogRecPtrIsInvalid(flush) ? 0 : priority;

			if (writeLag < 0)
				nulls[6] = true;
			else
				values[6] = IntervalPGetDatum(offset_to_interval(writeLag));

			if (flushLag < 0)
				nulls[7] = true;
			else
				values[7] = IntervalPGetDatum(offset_to_interval(flushLag));

			if (applyLag < 0)
				nulls[8] = true;
			else
				values[8] = IntervalPGetDatum(offset_to_interval(applyLag));

			values[9] = Int32GetDatum(priority);

			/*
			 * More easily understood version of standby state. This is purely
			 * informational.
			 *
			 * In quorum-based sync replication, the role of each standby
			 * listed in synchronous_standby_names can be changing very
			 * frequently. Any standbys considered as "sync" at one moment can
			 * be switched to "potential" ones at the next moment. So, it's
			 * basically useless to report "sync" or "potential" as their sync
			 * states. We report just "quorum" for them.
			 */
			if (priority == 0)
				values[10] = CStringGetTextDatum("async");
			else if (list_member_int(sync_standbys, i))
				values[10] = SyncRepConfig->syncrep_method == SYNC_REP_PRIORITY ?
					CStringGetTextDatum("sync") : CStringGetTextDatum("quorum");
			else
				values[10] = CStringGetTextDatum("potential");
		}

		tuplestore_putvalues(tupstore, tupdesc, values, nulls);
	}

	/* clean up and return the tuplestore */
	tuplestore_donestoring(tupstore);

	return (Datum) 0;
}

/*
  * This function is used to send a keepalive message to standby.
  * If requestReply is set, sets a flag in the message requesting the standby
  * to send a message back to us, for heartbeat purposes.
  */
static void
WalSndKeepalive(bool requestReply)
{
	elog(DEBUG2, "sending replication keepalive");

	/* construct the message... */
	resetStringInfo(&output_message);
	pq_sendbyte(&output_message, 'k');
	pq_sendint64(&output_message, sentPtr);
	pq_sendint64(&output_message, GetCurrentTimestamp());
	pq_sendbyte(&output_message, requestReply ? 1 : 0);

	/* ... and send it wrapped in CopyData */
	pq_putmessage_noblock('d', output_message.data, output_message.len);
}

/*
 * Send keepalive message if too much time has elapsed.
 */
static void
WalSndKeepaliveIfNecessary(void)
{
	TimestampTz ping_time;

	/*
	 * Don't send keepalive messages if timeouts are globally disabled or
	 * we're doing something not partaking in timeouts.
	 */
	if (wal_sender_timeout <= 0 || last_reply_timestamp <= 0)
		return;

	if (waiting_for_ping_response)
		return;

	/*
	 * If half of wal_sender_timeout has lapsed without receiving any reply
	 * from the standby, send a keep-alive message to the standby requesting
	 * an immediate reply.
	 */
	ping_time = TimestampTzPlusMilliseconds(last_reply_timestamp,
											wal_sender_timeout / 2);
	if (last_processing >= ping_time)
	{
		WalSndKeepalive(true);
		waiting_for_ping_response = true;

		/* Try to flush pending output to the client */
		if (pq_flush_if_writable() != 0)
			WalSndShutdown();
	}
}

/*
 * Record the end of the WAL and the time it was flushed locally, so that
 * LagTrackerRead can compute the elapsed time (lag) when this WAL location is
 * eventually reported to have been written, flushed and applied by the
 * standby in a reply message.
 */
static void
LagTrackerWrite(XLogRecPtr lsn, TimestampTz local_flush_time)
{
	bool		buffer_full;
	int			new_write_head;
	int			i;

	if (!am_walsender)
		return;

	/*
	 * If the lsn hasn't advanced since last time, then do nothing.  This way
	 * we only record a new sample when new WAL has been written.
	 */
	if (LagTracker.last_lsn == lsn)
		return;
	LagTracker. last_lsn = lsn;

	/*
	 * If advancing the write head of the circular buffer would crash into any
	 * of the read heads, then the buffer is full.  In other words, the
	 * slowest reader (presumably apply) is the one that controls the release
	 * of space.
	 */
	new_write_head = (LagTracker.write_head + 1) %LAG_TRACKER_BUFFER_SIZE;
	buffer_full = false;
	for (i = 0; i < NUM_SYNC_REP_WAIT_MODE; ++i)
	{
		if (new_write_head == LagTracker.read_heads[i])
			buffer_full = true;
	}

	/*
	 * If the buffer is full, for now we just rewind by one slot and overwrite
	 * the last sample, as a simple (if somewhat uneven) way to lower the
	 * sampling rate.  There may be better adaptive compaction algorithms.
	 */
	if (buffer_full)
	{
		new_write_head = LagTracker.write_head;

		if (LagTracker.write_head > 0)
			LagTracker. write_head--;

		else
			LagTracker. write_head = LAG_TRACKER_BUFFER_SIZE - 1;
	}

	/* Store a sample at the current write head position. */
	LagTracker. buffer[LagTracker.write_head].lsn = lsn;
	LagTracker. buffer[LagTracker.write_head].time = local_flush_time;
	LagTracker. write_head = new_write_head;
}

/*
 * Find out how much time has elapsed between the moment WAL location 'lsn'
 * (or the highest known earlier LSN) was flushed locally and the time 'now'.
 * We have a separate read head for each of the reported LSN locations we
 * receive in replies from standby; 'head' controls which read head is
 * used.  Whenever a read head crosses an LSN which was written into the
 * lag buffer with LagTrackerWrite, we can use the associated timestamp to
 * find out the time this LSN (or an earlier one) was flushed locally, and
 * therefore compute the lag.
 *
 * Return -1 if no new sample data is available, and otherwise the elapsed
 * time in microseconds.
 */
static TimeOffset
LagTrackerRead(int head, XLogRecPtr lsn, TimestampTz now)
{
	TimestampTz time = 0;

	/* Read all unread samples up to this LSN or end of buffer. */
	while (LagTracker.read_heads[head] != LagTracker.write_head &&
		   LagTracker.buffer[LagTracker.read_heads[head]].lsn <= lsn)
	{
		time = LagTracker.buffer[LagTracker.read_heads[head]].time;
		LagTracker. last_read[head] =
		LagTracker.buffer[LagTracker.read_heads[head]];
		LagTracker. read_heads[head] =
		(LagTracker.read_heads[head] + 1) %LAG_TRACKER_BUFFER_SIZE;
	}

	/*
	 * If the lag tracker is empty, that means the standby has processed
	 * everything we've ever sent so we should now clear 'last_read'.  If we
	 * didn't do that, we'd risk using a stale and irrelevant sample for
	 * interpolation at the beginning of the next burst of WAL after a period
	 * of idleness.
	 */
	if (LagTracker.read_heads[head] == LagTracker.write_head)
		LagTracker. last_read[head].time = 0;

	if (time > now)
	{
		/* If the clock somehow went backwards, treat as not found. */
		return -1;
	}
	else if (time == 0)
	{
		/*
		 * We didn't cross a time.  If there is a future sample that we
		 * haven't reached yet, and we've already reached at least one sample,
		 * let's interpolate the local flushed time.  This is mainly useful
		 * for reporting a completely stuck apply position as having
		 * increasing lag, since otherwise we'd have to wait for it to
		 * eventually start moving again and cross one of our samples before
		 * we can show the lag increasing.
		 */
		if (LagTracker.read_heads[head] == LagTracker.write_head)
		{
			/* There are no future samples, so we can't interpolate. */
			return -1;
		}
		else if (LagTracker.last_read[head].time != 0)
		{
			/* We can interpolate between last_read and the next sample. */
			double		fraction;
			WalTimeSample prev = LagTracker.last_read[head];
			WalTimeSample next = LagTracker.buffer[LagTracker.read_heads[head]];

			if (lsn < prev.lsn)
			{
				/*
				 * Reported LSNs shouldn't normally go backwards, but it's
				 * possible when there is a timeline change.  Treat as not
				 * found.
				 */
				return -1;
			}

			Assert(prev.lsn < next.lsn);

			if (prev.time > next.time)
			{
				/* If the clock somehow went backwards, treat as not found. */
				return -1;
			}

			/* See how far we are between the previous and next samples. */
			fraction =
				(double) (lsn - prev.lsn) / (double) (next.lsn - prev.lsn);

			/* Scale the local flush time proportionally. */
			time = (TimestampTz)
				((double) prev.time + (next.time - prev.time) * fraction);
		}
		else
		{
			/*
			 * We have only a future sample, implying that we were entirely
			 * caught up but and now there is a new burst of WAL and the
			 * standby hasn't processed the first sample yet.  Until the
			 * standby reaches the future sample the best we can do is report
			 * the hypothetical lag if that sample were to be replayed now.
			 */
			time = LagTracker.buffer[LagTracker.read_heads[head]].time;
		}
	}

	/* Return the elapsed time since local flush time in microseconds. */
	Assert(time != 0);
	return now - time;
}

polar_repl_mode_t
polar_gen_replication_mode(void)
{
	if (polar_is_dma_data_node())
		return POLAR_REPL_DMA_DATA;
	else if (polar_is_dma_logger_node())
		return POLAR_REPL_DMA_LOGGER;
	else
		return POLAR_REPL_DEFAULT;
}

const char *
polar_replication_mode_str(polar_repl_mode_t mode)
{
	switch (mode)
	{
		case POLAR_REPL_DEFAULT:
			return "default";
		case POLAR_REPL_DMA_LOGGER:
			return "dma_logger";
		case POLAR_REPL_DMA_DATA:
			return "dma_data";
		default:
			return "unknown";
	}
}

/* POLAR: send wal data when downstream is dma logger */
static void
polar_dma_logger_xlog_send_physical(void)
{
	XLogSendPhysicalExt(POLAR_REPL_DMA_LOGGER);
}

/* POLAR: send wal data when downstream is dma logger */
static void
polar_dma_data_xlog_send_physical(void)
{
	XLogSendPhysicalExt(POLAR_REPL_DMA_DATA);
}

/*
 * POLAR: Returns the latest point in WAL that has been safely flushed, and
 * can be sent to the standby. This should only be called when in recovery,
 * ie. we're streaming to a cascaded standby.
 *
 * As a side-effect, ThisTimeLineID is updated to the TLI of the last
 * replayed WAL record.
 */
static XLogRecPtr
polar_dma_get_flush_lsn(void)
{
	XLogRecPtr	replayPtr;
	TimeLineID	replayTLI;
	XLogRecPtr	receivePtr;
	TimeLineID	receiveTLI;
	XLogRecPtr	result;

	/*
	 * POLAR: in DMA mode, advance to new timeline from consensus flushed
	 * point. otherwise, the new timeline cannot be sent before exit from
	 * recovery status.
	 */
	ConsensusGetXLogFlushedLSN(&receivePtr, &receiveTLI);

	ThisTimeLineID = receiveTLI;
	result = receivePtr;

	if (!polar_is_dma_logger_node())
	{
		replayPtr = GetXLogReplayRecPtr(&replayTLI);
		if (replayTLI == ThisTimeLineID && replayPtr > receivePtr)
			result = replayPtr;
	}

	return result;
}

/* POLAR end */
