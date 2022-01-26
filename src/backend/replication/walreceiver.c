/*-------------------------------------------------------------------------
 *
 * walreceiver.c
 *
 * The WAL receiver process (walreceiver) is new as of Postgres 9.0. It
 * is the process in the standby server that takes charge of receiving
 * XLOG records from a primary server during streaming replication.
 *
 * When the startup process determines that it's time to start streaming,
 * it instructs postmaster to start walreceiver. Walreceiver first connects
 * to the primary server (it will be served by a walsender process
 * in the primary server), and then keeps receiving XLOG records and
 * writing them to the disk as long as the connection is alive. As XLOG
 * records are received and flushed to disk, it updates the
 * WalRcv->receivedUpto variable in shared memory, to inform the startup
 * process of how far it can proceed with XLOG replay.
 *
 * If the primary server ends streaming, but doesn't disconnect, walreceiver
 * goes into "waiting" mode, and waits for the startup process to give new
 * instructions. The startup process will treat that the same as
 * disconnection, and will rescan the archive/pg_wal directory. But when the
 * startup process wants to try streaming replication again, it will just
 * nudge the existing walreceiver process that's waiting, instead of launching
 * a new one.
 *
 * Normal termination is by SIGTERM, which instructs the walreceiver to
 * exit(0). Emergency termination is by SIGQUIT; like any postmaster child
 * process, the walreceiver will simply abort and exit on SIGQUIT. A close
 * of the connection and a FATAL error are treated not as a crash but as
 * normal operation.
 *
 * This file contains the server-facing parts of walreceiver. The libpq-
 * specific parts are in the libpqwalreceiver module. It's loaded
 * dynamically to avoid linking the server with libpq.
 *
 * Portions Copyright (c) 2020, Alibaba Group Holding Limited
 * Portions Copyright (c) 2010-2018, PostgreSQL Global Development Group
 *
 *
 * IDENTIFICATION
 *	  src/backend/replication/walreceiver.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <signal.h>
#include <unistd.h>
#include <sys/stat.h>

#include "access/htup_details.h"
#include "access/timeline.h"
#include "access/transam.h"
#include "access/xlog_internal.h"
#include "catalog/pg_authid.h"
#include "catalog/pg_type.h"
#include "common/ip.h"
#include "funcapi.h"
#include "lib/ilist.h"
#include "libpq/pqformat.h"
#include "libpq/pqsignal.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "postmaster/startup.h"
#include "replication/walreceiver.h"
#include "replication/walsender.h"
#include "replication/walsender_private.h"
#include "storage/ipc.h"
#include "storage/pmsignal.h"
#include "storage/procarray.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/pg_lsn.h"
#include "utils/ps_status.h"
#include "utils/resowner.h"
#include "utils/timestamp.h"
#include "replication/squeue.h"

/* POLAR */
#include "polar_datamax/polar_datamax.h"
#include "utils/guc.h"
#include "polar_dma/polar_dma.h"

/* GUC variables */
int			wal_receiver_status_interval;
int			wal_receiver_timeout;
bool		hot_standby_feedback;

/* libpqwalreceiver connection */
static WalReceiverConn *wrconn = NULL;
WalReceiverFunctionsType *WalReceiverFunctions = NULL;

#define NAPTIME_PER_CYCLE 100	/* max sleep time between cycles (100ms) */

/*
 * These variables are used similarly to openLogFile/SegNo/Off,
 * but for walreceiver to write the XLOG. recvFileTLI is the TimeLineID
 * corresponding the filename of recvFile.
 */
static int	recvFile = -1;
static TimeLineID recvFileTLI = 0;
static XLogSegNo recvSegNo = 0;
static uint32 recvOff = 0;

/* POLAR: local flag for datamax */
static bool polar_is_initial_datamax = false;

/* POLAR end */
/*
 * Flags set by interrupt handlers of walreceiver for later service in the
 * main loop.
 */
static volatile sig_atomic_t got_SIGHUP = false;
static volatile sig_atomic_t got_SIGTERM = false;

/*
 * LogstreamResult indicates the byte positions that we have already
 * written/fsynced.
 */
static struct
{
	XLogRecPtr	Receive;
	XLogRecPtr	Send;
	XLogRecPtr	Write;			/* last byte + 1 written out in the standby */
	XLogRecPtr	Flush;			/* last byte + 1 flushed in the standby */
}			LogstreamResult;

static StringInfoData reply_message;
static StringInfoData incoming_message;

bool		EnableParallelWalreceiver = false;
int			max_parallel_write_thread = 10;
int			max_parallel_write_pipe_len = 10000;
int			wal_receiver_reply_threshold = 100;

typedef enum
{
	COMMAND_NONE = 0,
	COMMAND_WRITE = 1,
	COMMAND_FLUSH = 2
} WALReceiverAsyncCmd;

typedef struct
{
	int32		threadIndex;
}			WALReceiverWorkerThreadParam;

/* Concurrently write wal log info */
typedef struct
{
	WALReceiverAsyncCmd cmd;
	XLogRecPtr	Write;
	XLogRecPtr	start;
	int			recvFile;
	TimeLineID	recvFileTLI;
	XLogSegNo	recvSegNo;
	int			recvFileFlush;
	XLogSegNo	recvSegNoFlush;
	TimeLineID	recvFileTLIFlush;
	XLogRecPtr	Flush;
	int			len;
	char	   *buf;
	bool		needClose;
}			WALParallelWriteReq;

typedef struct
{
	slock_t		m_lock;			/* protect Write and Flush */
	XLogRecPtr	Write;
	XLogRecPtr	Flush;
	int			recvFile;
	TimeLineID	recvFileTLI;
	XLogSegNo	recvSegNo;
	uint32		recvOff;

}			LogFlushResultPerThread;

typedef struct
{
	XLogRecPtr	threadWrite;
	XLogRecPtr	threadFlush;
	int			threadRecvFile;
	TimeLineID	threadRecvFileTLI;
	XLogSegNo	threadRecvSegNo;
}			CurrentState;

typedef struct
{
	PGPipeRec	  **request;		/* request pipe */
	PGPipeRec	  **response;
	ThreadSemaRec *sem;			/* response sem */
	LogFlushResultPerThread *results;
	CurrentState *state;
	pthread_mutex_t m_mutex;	/* thread lock it to create new xlog file */
	int			messageCnt;
}			WALReceiverParallelControl;

static WALReceiverParallelControl rcv_ParallelWriteControl;

/*
 * About SIGTERM handling:
 *
 * We can't just exit(1) within SIGTERM signal handler, because the signal
 * might arrive in the middle of some critical operation, like while we're
 * holding a spinlock. We also can't just set a flag in signal handler and
 * check it in the main loop, because we perform some blocking operations
 * like libpqrcv_PQexec(), which can take a long time to finish.
 *
 * We use a combined approach: When WalRcvImmediateInterruptOK is true, it's
 * safe for the signal handler to elog(FATAL) immediately. Otherwise it just
 * sets got_SIGTERM flag, which is checked in the main loop when convenient.
 *
 * This is very much like what regular backends do with ImmediateInterruptOK,
 * ProcessInterrupts() etc.
 */
static volatile bool WalRcvImmediateInterruptOK = false;

/* Prototypes for private functions */
static void ProcessWalRcvInterrupts(void);
static void EnableWalRcvImmediateExit(void);
static void DisableWalRcvImmediateExit(void);
static void WalRcvFetchTimeLineHistoryFiles(TimeLineID first, TimeLineID last);
static void WalRcvWaitForStartPosition(XLogRecPtr *startpoint, TimeLineID *startpointTLI);
static void WalRcvDie(int code, Datum arg);
static void XLogWalRcvProcessMsg(unsigned char type, char *buf, Size len);
static void XLogWalRcvWrite(char *buf, Size nbytes, XLogRecPtr recptr);
static void XLogWalRcvFlush(bool dying);
static void XLogWalRcvSendReply(bool force, bool requestReply);
static void XLogWalRcvSendHSFeedback(bool immed);
static void ProcessWalSndrMessage(XLogRecPtr walEnd, TimestampTz sendTime);

/* Signal handlers */
static void WalRcvSigHupHandler(SIGNAL_ARGS);
static void WalRcvSigUsr1Handler(SIGNAL_ARGS);
static void WalRcvShutdownHandler(SIGNAL_ARGS);
static void WalRcvQuickDieHandler(SIGNAL_ARGS);

static void walreceiver_init_parallel_control(WALReceiverParallelControl * control, int threadCount);
static void *walrecevier_async_write_management_thread(void *arg);
static void WalRcvParallelWrite(char *buf, Size nbytes, XLogRecPtr recptr);
static void WalRcvParallelFlush(XLogRecPtr flush);
static void WalRcvParallelSync(void);
static void XLogWalRcvParallelFlush(LogFlushResultPerThread * result);
static void XLogWalRcvParallelWrite(LogFlushResultPerThread * result, WALParallelWriteReq * request, int threadIndex);
static void XLogWalRcvWakeup(void);
static int	XLogFileParallelInit(XLogSegNo logsegno, bool *use_existent, bool use_lock, int threadIndex);

static void
walreceiver_init_parallel_control(WALReceiverParallelControl * control, int threadCount)
{
	int			i;

	control->request = (PGPipeRec * *) palloc0(threadCount * sizeof(PGPipeRec *));
	control->response = (PGPipeRec * *) palloc0(threadCount * sizeof(PGPipeRec *));
	control->sem = (ThreadSemaRec *) palloc0(threadCount * sizeof(ThreadSemaRec));
	control->results = (LogFlushResultPerThread *)
		palloc0(threadCount * sizeof(LogFlushResultPerThread));
	control->state = (CurrentState *) palloc0(threadCount * sizeof(CurrentState));
	control->messageCnt = 0;
	for (i = 0; i < threadCount; i++)
	{
		SpinLockInit(&(control->results[i].m_lock));
		control->results[i].recvFile = -1;
		control->state[i].threadRecvFile = -1;
	}
	ThreadMutexInitRec(&control->m_mutex);
}

static void
ProcessWalRcvInterrupts(void)
{
	/*
	 * Although walreceiver interrupt handling doesn't use the same scheme as
	 * regular backends, call CHECK_FOR_INTERRUPTS() to make sure we receive
	 * any incoming signals on Win32.
	 */
	CHECK_FOR_INTERRUPTS();

	if (got_SIGTERM)
	{
		WalRcvImmediateInterruptOK = false;
		ereport(FATAL,
				(errcode(ERRCODE_ADMIN_SHUTDOWN),
				 errmsg("terminating walreceiver process due to administrator command")));
	}
}

static void
EnableWalRcvImmediateExit(void)
{
	WalRcvImmediateInterruptOK = true;
	ProcessWalRcvInterrupts();
}

static void
DisableWalRcvImmediateExit(void)
{
	WalRcvImmediateInterruptOK = false;
	ProcessWalRcvInterrupts();
}

/* Main entry point for walreceiver process */
void
WalReceiverMain(void)
{
	char		conninfo[MAXCONNINFO];
	char	   *tmp_conninfo;
	char		slotname[NAMEDATALEN];
	XLogRecPtr	startpoint;
	TimeLineID	startpointTLI;
	TimeLineID	primaryTLI;
	bool		first_stream;
	WalRcvData *walrcv = WalRcv;
	TimestampTz last_recv_timestamp;
	TimestampTz now;
	bool		ping_sent;
	char	   *err;
	char	   *sender_host = NULL;
	int			sender_port = 0;
	WALReceiverWorkerThreadParam threadParam[max_parallel_write_thread];
	int			ret;
	int			message_cnt = 0;
	int			i;

	/*
	 * WalRcv should be set up already (if we are a backend, we inherit this
	 * by fork() or EXEC_BACKEND mechanism from the postmaster).
	 */
	Assert(walrcv != NULL);

	now = GetCurrentTimestamp();

	/*
	 * Mark walreceiver as running in shared memory.
	 *
	 * Do this as early as possible, so that if we fail later on, we'll set
	 * state to STOPPED. If we die before this, the startup process will keep
	 * waiting for us to start up, until it times out.
	 */
	SpinLockAcquire(&walrcv->mutex);
	Assert(walrcv->pid == 0);
	switch (walrcv->walRcvState)
	{
		case WALRCV_STOPPING:
			/* If we've already been requested to stop, don't start up. */
			walrcv->walRcvState = WALRCV_STOPPED;
			/* fall through */

		case WALRCV_STOPPED:
			SpinLockRelease(&walrcv->mutex);
			proc_exit(1);
			break;

		case WALRCV_STARTING:
			/* The usual case */
			break;

		case WALRCV_WAITING:
		case WALRCV_STREAMING:
		case WALRCV_RESTARTING:
		default:
			/* Shouldn't happen */
			SpinLockRelease(&walrcv->mutex);
			elog(PANIC, "walreceiver still running according to shared memory state");
	}
	/* Advertise our PID so that the startup process can kill us */
	walrcv->pid = MyProcPid;
	walrcv->walRcvState = WALRCV_STREAMING;

	/* Fetch information required to start streaming */
	walrcv->ready_to_display = false;
	strlcpy(conninfo, (char *) walrcv->conninfo, MAXCONNINFO);
	strlcpy(slotname, (char *) walrcv->slotname, NAMEDATALEN);
	startpoint = walrcv->receiveStart;
	startpointTLI = walrcv->receiveStartTLI;

	/* Initialise to a sanish value */
	walrcv->lastMsgSendTime =
		walrcv->lastMsgReceiptTime = walrcv->latestWalEndTime = now;

	/* Report the latch to use to awaken this process */
	walrcv->latch = &MyProc->procLatch;

	SpinLockRelease(&walrcv->mutex);

	/* Arrange to clean up at walreceiver exit */
	on_shmem_exit(WalRcvDie, 0);

	/* Properly accept or ignore signals the postmaster might send us */
	pqsignal(SIGHUP, WalRcvSigHupHandler);	/* set flag to read config file */
	pqsignal(SIGINT, SIG_IGN);
	pqsignal(SIGTERM, WalRcvShutdownHandler);	/* request shutdown */
	pqsignal(SIGQUIT, WalRcvQuickDieHandler);	/* hard crash time */
	pqsignal(SIGALRM, SIG_IGN);
	pqsignal(SIGPIPE, SIG_IGN);
	pqsignal(SIGUSR1, WalRcvSigUsr1Handler);
	pqsignal(SIGUSR2, SIG_IGN);

	/* Reset some signals that are accepted by postmaster but not here */
	pqsignal(SIGCHLD, SIG_DFL);
	pqsignal(SIGTTIN, SIG_DFL);
	pqsignal(SIGTTOU, SIG_DFL);
	pqsignal(SIGCONT, SIG_DFL);
	pqsignal(SIGWINCH, SIG_DFL);

	/* We allow SIGQUIT (quickdie) at all times */
	sigdelset(&BlockSig, SIGQUIT);

	/* Load the libpq-specific functions */
	load_file("libpqwalreceiver", false);
	if (WalReceiverFunctions == NULL)
		elog(ERROR, "libpqwalreceiver didn't initialize correctly");

	/*
	 * Create a resource owner to keep track of our resources (not clear that
	 * we need this, but may as well have one).
	 */
	CurrentResourceOwner = ResourceOwnerCreate(NULL, "Wal Receiver");

	/* Unblock signals (they were blocked when the postmaster forked us) */
	PG_SETMASK(&UnBlockSig);

	/* Establish the connection to the primary for XLOG streaming */
	EnableWalRcvImmediateExit();
	wrconn = walrcv_connect(conninfo, false, "walreceiver", &err);
	if (!wrconn)
		ereport(ERROR,
				(errmsg("could not connect to the primary server: %s", err)));
	DisableWalRcvImmediateExit();

	/*
	 * Save user-visible connection string.  This clobbers the original
	 * conninfo, for security. Also save host and port of the sender server
	 * this walreceiver is connected to.
	 */
	tmp_conninfo = walrcv_get_conninfo(wrconn);
	walrcv_get_senderinfo(wrconn, &sender_host, &sender_port);
	SpinLockAcquire(&walrcv->mutex);
	memset(walrcv->conninfo, 0, MAXCONNINFO);
	if (tmp_conninfo)
		strlcpy((char *) walrcv->conninfo, tmp_conninfo, MAXCONNINFO);

	memset(walrcv->sender_host, 0, NI_MAXHOST);
	if (sender_host)
		strlcpy((char *) walrcv->sender_host, sender_host, NI_MAXHOST);

	walrcv->sender_port = sender_port;
	walrcv->ready_to_display = true;
	SpinLockRelease(&walrcv->mutex);

	if (tmp_conninfo)
		pfree(tmp_conninfo);

	if (sender_host)
		pfree(sender_host);

	first_stream = true;
	if (EnableParallelWalreceiver)
	{
		walreceiver_init_parallel_control(&rcv_ParallelWriteControl, max_parallel_write_thread);
		for (i = 0; i < max_parallel_write_thread; i++)
		{
			rcv_ParallelWriteControl.request[i] = CreatePipeRec(max_parallel_write_pipe_len);
			rcv_ParallelWriteControl.response[i] = CreatePipeRec(max_parallel_write_pipe_len);
			ThreadSemaRecInit(&rcv_ParallelWriteControl.sem[i], 0);
			threadParam[i].threadIndex = i;
			ret = CreateThreadRec(walrecevier_async_write_management_thread, (void *) &threadParam[i], MT_THR_DETACHED);
			if (ret)
			{
				elog(ERROR, "create walreceiver parallel write manage thread failed");
			}
		}
	}
	for (;;)
	{
		char	   *primary_sysid;
		char		standby_sysid[32];
		int			server_version;
		WalRcvStreamOptions options;

		/*
		 * Check that we're connected to a valid server using the
		 * IDENTIFY_SYSTEM replication command.
		 */
		EnableWalRcvImmediateExit();
		primary_sysid = walrcv_identify_system(wrconn, &primaryTLI,
											   &server_version);

		snprintf(standby_sysid, sizeof(standby_sysid), UINT64_FORMAT,
				 GetSystemIdentifier());
		if (strcmp(primary_sysid, standby_sysid) != 0)
		{
			ereport(ERROR,
					(errmsg("database system identifier differs between the primary and standby"),
					 errdetail("The primary's identifier is %s, the standby's identifier is %s.",
							   primary_sysid, standby_sysid)));
		}
		DisableWalRcvImmediateExit();

		/* POLAR: Update Logger timeline if current is a initial one. */
		if (polar_is_dma_logger_node() && polar_enable_dma && startpointTLI == POLAR_INVALID_TIMELINE_ID)
		{
			Assert(startpointTLI == POLAR_INVALID_TIMELINE_ID);

			polar_is_initial_datamax = true;

			if (startpointTLI == POLAR_INVALID_TIMELINE_ID)
			{
				/*
				 * POLAR: If we are in Logger mode and requested timeline is
				 * invalid, it means that we are a initial one, so we need to
				 * fetch as much as possible  WAL from Primary's current
				 * timeline, so update walreceiver's receiveStartTLI with
				 * primary's current one.
				 */
				SpinLockAcquire(&walrcv->mutex);
				walrcv->receiveStartTLI = startpointTLI = primaryTLI;
				SpinLockRelease(&walrcv->mutex);
				ereport(LOG,
						(errmsg("initial Logger node update requested streaming timeline with primary's current one %d",
								primaryTLI)));
			}
			else
				ereport(ERROR,
						(errmsg("initial datamax requested with certain timeline id: %d", startpointTLI)));
		}
		else
			polar_is_initial_datamax = false;
		/* POLAR end */

		/*
		 * Confirm that the current timeline of the primary is the same or
		 * ahead of ours.
		 */
		if (primaryTLI < startpointTLI)
			ereport(ERROR,
					(errmsg("highest timeline %u of the primary is behind recovery timeline %u",
							primaryTLI, startpointTLI)));

		/*
		 * Get any missing history files. We do this always, even when we're
		 * not interested in that timeline, so that if we're promoted to
		 * become the master later on, we don't select the same timeline that
		 * was already used in the current master. This isn't bullet-proof -
		 * you'll need some external software to manage your cluster if you
		 * need to ensure that a unique timeline id is chosen in every case,
		 * but let's avoid the confusion of timeline id collisions where we
		 * can.
		 */
		WalRcvFetchTimeLineHistoryFiles(startpointTLI, primaryTLI);

		/*
		 * Start streaming.
		 *
		 * We'll try to start at the requested starting point and timeline,
		 * even if it's different from the server's latest timeline. In case
		 * we've already reached the end of the old timeline, the server will
		 * finish the streaming immediately, and we will go back to await
		 * orders from the startup process. If recovery_target_timeline is
		 * 'latest', the startup process will scan pg_wal and find the new
		 * history file, bump recovery target timeline, and ask us to restart
		 * on the new timeline.
		 */
		options.logical = false;
		options.startpoint = startpoint;
		options.slotname = slotname[0] != '\0' ? slotname : NULL;
		options.proto.physical.startpointTLI = startpointTLI;
		/* POLAR: Set current replication mode */
		options.polar_repl_mode = polar_gen_replication_mode();
		/* POLAR end */
		ThisTimeLineID = startpointTLI;
		if (walrcv_startstreaming(wrconn, &options))
		{
			if (first_stream)
				ereport(LOG,
						(errmsg("started streaming WAL from primary at %X/%X on timeline %u",
								(uint32) (startpoint >> 32), (uint32) startpoint,
								startpointTLI)));
			else
				ereport(LOG,
						(errmsg("restarted WAL streaming at %X/%X on timeline %u",
								(uint32) (startpoint >> 32), (uint32) startpoint,
								startpointTLI)));
			first_stream = false;

			/*
			 * Initialize LogstreamResult and buffers for processing messages
			 * POLAR: Initialize LogstreamResult as last received lsn when in
			 * datamax mode otherwise, when datamax_replay_lsn > received_lsn
			 * and we set flush_lsn = datamax_replay_lsn the wal received
			 * won't be flushed to disk because flush_lsn > received_lsn(which
			 * is write lsn) in this case
			 */
			if (!polar_is_dma_logger_node())
				LogstreamResult.Receive = LogstreamResult.Send = LogstreamResult.Write = LogstreamResult.Flush = GetXLogReplayRecPtr(NULL);
			else
				LogstreamResult.Receive = LogstreamResult.Send = LogstreamResult.Write = LogstreamResult.Flush = polar_dma_get_received_lsn();

			initStringInfo(&reply_message);
			initStringInfo(&incoming_message);

			/* Initialize the last recv timestamp */
			last_recv_timestamp = GetCurrentTimestamp();
			ping_sent = false;

			/* Loop until end-of-streaming or error */
			for (;;)
			{
				char	   *buf;
				int			len;
				bool		endofwal = false;
				pgsocket	wait_fd = PGINVALID_SOCKET;
				int			rc;

				/*
				 * Exit walreceiver if we're not in recovery. This should not
				 * happen, but cross-check the status here.
				 */
				if (!RecoveryInProgress())
					ereport(FATAL,
							(errmsg("cannot continue WAL streaming, recovery has already ended")));

				/* Process any requests or signals received recently */
				ProcessWalRcvInterrupts();

				if (got_SIGHUP)
				{
					got_SIGHUP = false;
					ProcessConfigFile(PGC_SIGHUP);
					XLogWalRcvSendHSFeedback(true);
				}

				/* See if we can read data immediately */
				len = walrcv_receive(wrconn, &buf, &wait_fd);
				if (len != 0)
				{
					/*
					 * Process the received data, and any subsequent data we
					 * can read without blocking.
					 */
					for (;;)
					{
						if (len > 0)
						{
							/*
							 * Something was received from master, so reset
							 * timeout
							 */
							last_recv_timestamp = GetCurrentTimestamp();
							ping_sent = false;
							XLogWalRcvProcessMsg(buf[0], &buf[1], len - 1);
						}
						else if (len == 0)
							break;
						else if (len < 0)
						{
							ereport(LOG,
									(errmsg("replication terminated by primary server"),
									 errdetail("End of WAL reached on timeline %u at %X/%X.",
											   startpointTLI,
											   (uint32) (LogstreamResult.Write >> 32), (uint32) LogstreamResult.Write)));
							endofwal = true;
							break;
						}
						if (EnableParallelWalreceiver &&
							((++message_cnt) > wal_receiver_reply_threshold))
						{
							XLogWalRcvSendReply(false, false);
							WalRcvParallelFlush(LogstreamResult.Receive);
							WalRcvParallelSync();
							message_cnt = 0;
						}
						len = walrcv_receive(wrconn, &buf, &wait_fd);
					}

					if (EnableParallelWalreceiver)
					{
						if (rcv_ParallelWriteControl.messageCnt > max_parallel_write_pipe_len)
							elog(LOG, "wait sync complete, write cnt: %d",
								 rcv_ParallelWriteControl.messageCnt);
						XLogWalRcvSendReply(false, false);
						message_cnt = 0;
						WalRcvParallelFlush(LogstreamResult.Receive);
						WalRcvParallelSync();
					}
					else
					{
						/* Let the master know that we received some data. */
						XLogWalRcvSendReply(false, false);

						/*
						 * If we've written some records, flush them to disk
						 * and let the startup process and primary server know
						 * about them.
						 */
						XLogWalRcvFlush(false);
					}
				}
				if (EnableParallelWalreceiver && (!endofwal) && rcv_ParallelWriteControl.messageCnt > 0)
				{
					pg_usleep(1L);
					WalRcvParallelSync();
					continue;
				}

				/* Check if we need to exit the streaming loop. */
				if (endofwal)
					break;

				/*
				 * Ideally we would reuse a WaitEventSet object repeatedly
				 * here to avoid the overheads of WaitLatchOrSocket on epoll
				 * systems, but we can't be sure that libpq (or any other
				 * walreceiver implementation) has the same socket (even if
				 * the fd is the same number, it may have been closed and
				 * reopened since the last time).  In future, if there is a
				 * function for removing sockets from WaitEventSet, then we
				 * could add and remove just the socket each time, potentially
				 * avoiding some system calls.
				 */
				Assert(wait_fd != PGINVALID_SOCKET);
				rc = WaitLatchOrSocket(walrcv->latch,
									   WL_POSTMASTER_DEATH | WL_SOCKET_READABLE |
									   WL_TIMEOUT | WL_LATCH_SET,
									   wait_fd,
									   NAPTIME_PER_CYCLE,
									   WAIT_EVENT_WAL_RECEIVER_MAIN);
				if (rc & WL_LATCH_SET)
				{
					ResetLatch(walrcv->latch);
					if (EnableParallelWalreceiver)
						WalRcvParallelSync();
					if (walrcv->force_reply)
					{
						/*
						 * The recovery process has asked us to send apply
						 * feedback now.  Make sure the flag is really set to
						 * false in shared memory before sending the reply, so
						 * we don't miss a new request for a reply.
						 */
						walrcv->force_reply = false;
						pg_memory_barrier();
						XLogWalRcvSendReply(true, false);
					}
				}
				if (rc & WL_POSTMASTER_DEATH)
				{
					/*
					 * Emergency bailout if postmaster has died.  This is to
					 * avoid the necessity for manual cleanup of all
					 * postmaster children.
					 */
					exit(1);
				}
				if (rc & WL_TIMEOUT)
				{
					/*
					 * We didn't receive anything new. If we haven't heard
					 * anything from the server for more than
					 * wal_receiver_timeout / 2, ping the server. Also, if
					 * it's been longer than wal_receiver_status_interval
					 * since the last update we sent, send a status update to
					 * the master anyway, to report any progress in applying
					 * WAL.
					 */
					bool		requestReply = false;

					/*
					 * Check if time since last receive from standby has
					 * reached the configured limit.
					 */
					if (wal_receiver_timeout > 0)
					{
						TimestampTz now = GetCurrentTimestamp();
						TimestampTz timeout;

						timeout =
							TimestampTzPlusMilliseconds(last_recv_timestamp,
														wal_receiver_timeout);

						if (now >= timeout)
							ereport(ERROR,
									(errmsg("terminating walreceiver due to timeout")));

						/*
						 * We didn't receive anything new, for half of
						 * receiver replication timeout. Ping the server.
						 */
						if (!ping_sent)
						{
							timeout = TimestampTzPlusMilliseconds(last_recv_timestamp,
																  (wal_receiver_timeout / 2));
							if (now >= timeout)
							{
								requestReply = true;
								ping_sent = true;
							}
						}
					}

					if (EnableParallelWalreceiver)
						WalRcvParallelSync();
					XLogWalRcvSendReply(requestReply, requestReply);
					XLogWalRcvSendHSFeedback(false);
				}
			}

			if (EnableParallelWalreceiver)
			{
				while (rcv_ParallelWriteControl.messageCnt != 0)
				{
					elog(LOG, "wait sync complete, write cnt: %d",
						 rcv_ParallelWriteControl.messageCnt);
					WalRcvParallelSync();
				}
			}

			/*
			 * The backend finished streaming. Exit streaming COPY-mode from
			 * our side, too.
			 */
			EnableWalRcvImmediateExit();
			walrcv_endstreaming(wrconn, &primaryTLI);
			DisableWalRcvImmediateExit();

			/*
			 * If the server had switched to a new timeline that we didn't
			 * know about when we began streaming, fetch its timeline history
			 * file now.
			 */
			WalRcvFetchTimeLineHistoryFiles(startpointTLI, primaryTLI);
		}
		else
			ereport(LOG,
					(errmsg("primary server contains no more WAL on requested timeline %u",
							startpointTLI)));

		/*
		 * End of WAL reached on the requested timeline. Close the last
		 * segment, and await for new orders from the startup process.
		 */
		if (recvFile >= 0)
		{
			char		xlogfname[MAXFNAMELEN];

			XLogWalRcvFlush(false);
			if (close(recvFile) != 0)
				ereport(PANIC,
						(errcode_for_file_access(),
						 errmsg("could not close log segment %s: %m",
								XLogFileNameP(recvFileTLI, recvSegNo))));

			/*
			 * Create .done file forcibly to prevent the streamed segment from
			 * being archived later.
			 */
			polar_is_dma_logger_mode = polar_is_dma_logger_node();	/* POLAR: Enter dma
																	 * logger Mode. */
			XLogFileName(xlogfname, recvFileTLI, recvSegNo, wal_segment_size);
			if (XLogArchiveMode != ARCHIVE_MODE_ALWAYS)
				XLogArchiveForceDone(xlogfname);
			else if (polar_enable_dma)
				polar_dma_xlog_archive_notify(xlogfname, true);
			else
				XLogArchiveNotify(xlogfname);
			polar_is_dma_logger_mode = false;	/* POLAR: Leave dma logger
												 * mode. */
		}
		recvFile = -1;

		elog(DEBUG1, "walreceiver ended streaming and awaits new instructions");
		WalRcvWaitForStartPosition(&startpoint, &startpointTLI);
	}
	/* not reached */
}

/*
 * Wait for startup process to set receiveStart and receiveStartTLI.
 */
static void
WalRcvWaitForStartPosition(XLogRecPtr *startpoint, TimeLineID *startpointTLI)
{
	WalRcvData *walrcv = WalRcv;
	int			state;

	SpinLockAcquire(&walrcv->mutex);
	state = walrcv->walRcvState;
	if (state != WALRCV_STREAMING)
	{
		SpinLockRelease(&walrcv->mutex);
		if (state == WALRCV_STOPPING)
			proc_exit(0);
		else
			elog(FATAL, "unexpected walreceiver state");
	}
	walrcv->walRcvState = WALRCV_WAITING;
	walrcv->receiveStart = InvalidXLogRecPtr;
	walrcv->receiveStartTLI = 0;
	SpinLockRelease(&walrcv->mutex);

	if (update_process_title)
		set_ps_display("idle", false);

	/*
	 * nudge startup process to notice that we've stopped streaming and are
	 * now waiting for instructions.
	 */
	WakeupRecovery();
	for (;;)
	{
		ResetLatch(walrcv->latch);

		/*
		 * Emergency bailout if postmaster has died.  This is to avoid the
		 * necessity for manual cleanup of all postmaster children.
		 */
		if (!PostmasterIsAlive())
			exit(1);

		ProcessWalRcvInterrupts();

		SpinLockAcquire(&walrcv->mutex);
		Assert(walrcv->walRcvState == WALRCV_RESTARTING ||
			   walrcv->walRcvState == WALRCV_WAITING ||
			   walrcv->walRcvState == WALRCV_STOPPING);
		if (walrcv->walRcvState == WALRCV_RESTARTING)
		{
			/* we don't expect primary_conninfo to change */
			*startpoint = walrcv->receiveStart;
			*startpointTLI = walrcv->receiveStartTLI;
			walrcv->walRcvState = WALRCV_STREAMING;
			SpinLockRelease(&walrcv->mutex);
			break;
		}
		if (walrcv->walRcvState == WALRCV_STOPPING)
		{
			/*
			 * We should've received SIGTERM if the startup process wants us
			 * to die, but might as well check it here too.
			 */
			SpinLockRelease(&walrcv->mutex);
			exit(1);
		}
		SpinLockRelease(&walrcv->mutex);

		WaitLatch(walrcv->latch, WL_LATCH_SET | WL_POSTMASTER_DEATH, 0,
				  WAIT_EVENT_WAL_RECEIVER_WAIT_START);
	}

	if (update_process_title)
	{
		char		activitymsg[50];

		snprintf(activitymsg, sizeof(activitymsg), "restarting at %X/%X",
				 (uint32) (*startpoint >> 32),
				 (uint32) *startpoint);
		set_ps_display(activitymsg, false);
	}
}

/*
 * Fetch any missing timeline history files between 'first' and 'last'
 * (inclusive) from the server.
 */
static void
WalRcvFetchTimeLineHistoryFiles(TimeLineID first, TimeLineID last)
{
	TimeLineID	tli;

	polar_is_dma_logger_mode = polar_is_dma_logger_node();	/* POLAR: Enter datamax
															 * Mode. */
	for (tli = first; tli <= last; tli++)
	{
		/* there's no history file for timeline 1 */
		if (tli != 1 && !existsTimeLineHistory(tli))
		{
			char	   *fname;
			char	   *content;
			int			len;
			char		expectedfname[MAXFNAMELEN];

			ereport(LOG,
					(errmsg("fetching timeline history file for timeline %u from primary server",
							tli)));

			EnableWalRcvImmediateExit();
			walrcv_readtimelinehistoryfile(wrconn, tli, &fname, &content, &len);
			DisableWalRcvImmediateExit();

			/*
			 * Check that the filename on the master matches what we
			 * calculated ourselves. This is just a sanity check, it should
			 * always match.
			 */
			TLHistoryFileName(expectedfname, tli);
			if (strcmp(fname, expectedfname) != 0)
				ereport(ERROR,
						(errcode(ERRCODE_PROTOCOL_VIOLATION),
						 errmsg_internal("primary reported unexpected file name for timeline history file of timeline %u",
										 tli)));

			/*
			 * Write the file to pg_wal.
			 */
			writeTimeLineHistoryFile(tli, content, len);

			/*
			 * In DMA mode, archive history file for standby
			 */
			if (polar_enable_dma)
			{
				if (XLogArchiveMode != ARCHIVE_MODE_ALWAYS)
					XLogArchiveForceDone(expectedfname);
				else
					polar_dma_xlog_archive_notify(expectedfname, true);
			}

			pfree(fname);
			pfree(content);
		}
	}
	polar_is_dma_logger_mode = false;	/* POLAR: Leave datamax mode. */
}

/*
 * Mark us as STOPPED in shared memory at exit.
 */
static void
WalRcvDie(int code, Datum arg)
{
	WalRcvData *walrcv = WalRcv;

	/* Ensure that all WAL records received are flushed to disk */
	XLogWalRcvFlush(true);

	/* Mark ourselves inactive in shared memory */
	SpinLockAcquire(&walrcv->mutex);
	Assert(walrcv->walRcvState == WALRCV_STREAMING ||
		   walrcv->walRcvState == WALRCV_RESTARTING ||
		   walrcv->walRcvState == WALRCV_STARTING ||
		   walrcv->walRcvState == WALRCV_WAITING ||
		   walrcv->walRcvState == WALRCV_STOPPING);
	Assert(walrcv->pid == MyProcPid);
	walrcv->walRcvState = WALRCV_STOPPED;
	walrcv->pid = 0;
	walrcv->ready_to_display = false;
	walrcv->latch = NULL;
	SpinLockRelease(&walrcv->mutex);

	/* Terminate the connection gracefully. */
	if (wrconn != NULL)
		walrcv_disconnect(wrconn);

	/* Wake up the startup process to notice promptly that we're gone */
	WakeupRecovery();
}

/* SIGHUP: set flag to re-read config file at next convenient time */
static void
WalRcvSigHupHandler(SIGNAL_ARGS)
{
	got_SIGHUP = true;
}


/* SIGUSR1: used by latch mechanism */
static void
WalRcvSigUsr1Handler(SIGNAL_ARGS)
{
	int			save_errno = errno;

	latch_sigusr1_handler();

	errno = save_errno;
}

/* SIGTERM: set flag for main loop, or shutdown immediately if safe */
static void
WalRcvShutdownHandler(SIGNAL_ARGS)
{
	int			save_errno = errno;

	got_SIGTERM = true;

	if (WalRcv->latch)
		SetLatch(WalRcv->latch);

	/* Don't joggle the elbow of proc_exit */
	if (!proc_exit_inprogress && WalRcvImmediateInterruptOK)
		ProcessWalRcvInterrupts();

	errno = save_errno;
}

/*
 * WalRcvQuickDieHandler() occurs when signalled SIGQUIT by the postmaster.
 *
 * Some backend has bought the farm, so we need to stop what we're doing and
 * exit.
 */
static void
WalRcvQuickDieHandler(SIGNAL_ARGS)
{
	/*
	 * We DO NOT want to run proc_exit() or atexit() callbacks -- we're here
	 * because shared memory may be corrupted, so we don't want to try to
	 * clean up our transaction.  Just nail the windows shut and get out of
	 * town.  The callbacks wouldn't be safe to run from a signal handler,
	 * anyway.
	 *
	 * Note we use _exit(2) not _exit(0).  This is to force the postmaster
	 * into a system reset cycle if someone sends a manual SIGQUIT to a random
	 * backend.  This is necessary precisely because we don't clean up our
	 * shared memory state.  (The "dead man switch" mechanism in pmsignal.c
	 * should ensure the postmaster sees this as a crash, too, but no harm in
	 * being doubly sure.)
	 */
	_exit(2);
}

/*
 * Accept the message from XLOG stream, and process it.
 */
static void
XLogWalRcvProcessMsg(unsigned char type, char *buf, Size len)
{
	int			hdrlen;
	XLogRecPtr	dataStart;
	XLogRecPtr	walEnd;
	TimestampTz sendTime;
	bool		replyRequested;

	/* POLAR */
	TransactionId polar_primary_next_xid;
	uint32		polar_primary_epoch;
	XLogSegNo	polar_upstream_last_removed_segno;

	/* POLAR end */

	resetStringInfo(&incoming_message);

	switch (type)
	{
		case 'w':				/* WAL records */
			{
				/* copy message to StringInfo */
				hdrlen = sizeof(int64) + sizeof(int64) + sizeof(int64);
				if (len < hdrlen)
					ereport(ERROR,
							(errcode(ERRCODE_PROTOCOL_VIOLATION),
							 errmsg_internal("invalid WAL message received from primary")));
				appendBinaryStringInfo(&incoming_message, buf, hdrlen);

				/* read the fields */
				dataStart = pq_getmsgint64(&incoming_message);
				walEnd = pq_getmsgint64(&incoming_message);
				sendTime = pq_getmsgint64(&incoming_message);
				ProcessWalSndrMessage(walEnd, sendTime);

				buf += hdrlen;
				len -= hdrlen;
				if (EnableParallelWalreceiver)
				{
					LogstreamResult.Receive = dataStart + len;
					WalRcvParallelWrite(buf, len, dataStart);
				}
				else
				{
					XLogWalRcvWrite(buf, len, dataStart);
				}
				break;
			}
		case 'k':				/* Keepalive */
			{
				/* copy message to StringInfo */
				hdrlen = sizeof(int64) + sizeof(int64) + sizeof(char);
				if (len != hdrlen)
					ereport(ERROR,
							(errcode(ERRCODE_PROTOCOL_VIOLATION),
							 errmsg_internal("invalid keepalive message received from primary")));
				appendBinaryStringInfo(&incoming_message, buf, hdrlen);

				/* read the fields */
				walEnd = pq_getmsgint64(&incoming_message);
				sendTime = pq_getmsgint64(&incoming_message);
				replyRequested = pq_getmsgbyte(&incoming_message);

				ProcessWalSndrMessage(walEnd, sendTime);

				/* If the primary requested a reply, send one immediately */
				if (replyRequested)
					XLogWalRcvSendReply(true, false);
				break;
			}

			/*
			 * POLAR: with next_xid and epoch of primary, needed when feedback
			 * standby xmin in datamax mode with last_valid_lsn of primary,
			 * used to keep xlog consistency also with last_removed_segno of
			 * primary, used to keep wal file those haven't been removed in
			 * primary
			 */
		case 'e':
			{
				/* copy message to StringInfo */
				hdrlen = sizeof(int64) + sizeof(int64) + sizeof(int32) + sizeof(int32) + sizeof(int64) + sizeof(int64);
				if (len < hdrlen)
					/* no cover begin */
					ereport(ERROR,
							(errcode(ERRCODE_PROTOCOL_VIOLATION),
							 errmsg_internal("invalid WAL message received from primary")));
				/* no cover end */
				appendBinaryStringInfo(&incoming_message, buf, hdrlen);

				/* read the fields */
				dataStart = pq_getmsgint64(&incoming_message);
				walEnd = pq_getmsgint64(&incoming_message);
				polar_primary_next_xid = pq_getmsgint(&incoming_message, 4);
				polar_primary_epoch = pq_getmsgint(&incoming_message, 4);
				polar_upstream_last_removed_segno = pq_getmsgint64(&incoming_message);
				sendTime = pq_getmsgint64(&incoming_message);
				ProcessWalSndrMessage(walEnd, sendTime);

				/* record next_xid and epoch of primary */
				POLAR_DATAMAX_SET_PRIMARY_NEXTXID(polar_primary_next_xid);
				POLAR_DATAMAX_SET_PRIMARY_NEXTEPOCH(polar_primary_epoch);
				/* record polar_upstream_last_removed_segno */
				polar_datamax_update_upstream_last_removed_segno(polar_datamax_ctl, polar_upstream_last_removed_segno);

				buf += hdrlen;
				len -= hdrlen;
				XLogWalRcvWrite(buf, len, dataStart);

				/* init timeline id and lsn if intial datamax */
				if (unlikely(polar_is_initial_datamax))
				{
					/* POLAR: init timeline and lsn */
					polar_datamax_update_min_received_info(polar_datamax_ctl, ThisTimeLineID, dataStart);
					/* POLAR: write meta to storage */
					polar_datamax_write_meta(polar_datamax_ctl, true);

					polar_is_initial_datamax = false;
				}
				break;
			}
			/* POLAR end */
		default:
			ereport(ERROR,
					(errcode(ERRCODE_PROTOCOL_VIOLATION),
					 errmsg_internal("invalid replication message type %d",
									 type)));
	}
}

/*
 * Write XLOG data to disk.
 */
static void
XLogWalRcvWrite(char *buf, Size nbytes, XLogRecPtr recptr)
{
	int			startoff;
	int			byteswritten;

	while (nbytes > 0)
	{
		int			segbytes;

		if (recvFile < 0 || !XLByteInSeg(recptr, recvSegNo, wal_segment_size))
		{
			bool		use_existent;

			/*
			 * fsync() and close current file before we switch to next one. We
			 * would otherwise have to reopen this file to fsync it later
			 */
			if (recvFile >= 0)
			{
				char		xlogfname[MAXFNAMELEN];

				XLogWalRcvFlush(false);

				/*
				 * XLOG segment files will be re-read by recovery in startup
				 * process soon, so we don't advise the OS to release cache
				 * pages associated with the file like XLogFileClose() does.
				 */
				if (close(recvFile) != 0)
					ereport(PANIC,
							(errcode_for_file_access(),
							 errmsg("could not close log segment %s: %m",
									XLogFileNameP(recvFileTLI, recvSegNo))));

				/*
				 * Create .done file forcibly to prevent the streamed segment
				 * from being archived later.
				 */
				XLogFileName(xlogfname, recvFileTLI, recvSegNo, wal_segment_size);
				polar_is_dma_logger_mode = polar_is_dma_logger_node();	/* POLAR: Enter datamax
																		 * Mode. */
				if (XLogArchiveMode != ARCHIVE_MODE_ALWAYS)
					XLogArchiveForceDone(xlogfname);
				else if (polar_enable_dma)
					polar_dma_xlog_archive_notify(xlogfname, true);
				else
					XLogArchiveNotify(xlogfname);
				polar_is_dma_logger_mode = false;	/* POLAR: Leave datamax
													 * mode. */
			}
			recvFile = -1;

			/* Create/use new log file */
			XLByteToSeg(recptr, recvSegNo, wal_segment_size);
			use_existent = true;
			polar_is_dma_logger_mode = polar_is_dma_logger_node();	/* POLAR: Enter datamax
																	 * Mode. */
			recvFile = XLogFileInit(recvSegNo, &use_existent, true);
			polar_is_dma_logger_mode = false;	/* POLAR: Leave datamax mode. */
			recvFileTLI = ThisTimeLineID;
			recvOff = 0;
		}

		/* Calculate the start offset of the received logs */
		startoff = XLogSegmentOffset(recptr, wal_segment_size);

		if (startoff + nbytes > wal_segment_size)
			segbytes = wal_segment_size - startoff;
		else
			segbytes = nbytes;

		/* Need to seek in the file? */
		if (recvOff != startoff)
		{
			if (lseek(recvFile, (off_t) startoff, SEEK_SET) < 0)
				ereport(PANIC,
						(errcode_for_file_access(),
						 errmsg("could not seek in log segment %s to offset %u: %m",
								XLogFileNameP(recvFileTLI, recvSegNo),
								startoff)));
			recvOff = startoff;
		}

		/* OK to write the logs */
		errno = 0;

		byteswritten = write(recvFile, buf, segbytes);
		if (byteswritten <= 0)
		{
			/* if write didn't set errno, assume no disk space */
			if (errno == 0)
				errno = ENOSPC;
			ereport(PANIC,
					(errcode_for_file_access(),
					 errmsg("could not write to log segment %s "
							"at offset %u, length %lu: %m",
							XLogFileNameP(recvFileTLI, recvSegNo),
							recvOff, (unsigned long) segbytes)));
		}

		/* Update state for write */
		recptr += byteswritten;

		recvOff += byteswritten;
		nbytes -= byteswritten;
		buf += byteswritten;

		LogstreamResult.Write = recptr;
	}
}

/*
 * ParallelWrite XLOG data to disk.
 */
static void
XLogWalRcvParallelWrite(LogFlushResultPerThread * result, WALParallelWriteReq * request, int threadIndex)
{
	int			startoff;
	int			byteswritten;
	char	   *buf = request->buf;
	int			nbytes = request->len;
	XLogRecPtr	recptr = request->start;

	while (nbytes > 0)
	{
		int			segbytes;

		if (result->recvFile < 0 || !XLByteInSeg(recptr, result->recvSegNo, wal_segment_size))
		{
			bool		use_existent;

			/*
			 * fsync() and close current file before we switch to next one. We
			 * would otherwise have to reopen this file to fsync it later
			 */
			if (result->recvFile >= 0)
			{
				request->recvFileFlush = result->recvFile;
				request->recvSegNoFlush = result->recvSegNo;
				request->recvFileTLIFlush = result->recvFileTLI;
				request->needClose = true;
				XLogWalRcvParallelFlush(result);
				result->Flush = result->Write;
				request->Flush = result->Flush;
			}
			result->recvFile = -1;

			/* Create/use new log file */
			XLByteToSeg(recptr, result->recvSegNo, wal_segment_size);
			use_existent = true;
			result->recvFile = XLogFileParallelInit(result->recvSegNo, &use_existent, true, threadIndex);
			result->recvFileTLI = ThisTimeLineID;
			result->recvOff = 0;
		}

		/* Calculate the start offset of the received logs */
		startoff = XLogSegmentOffset(recptr, wal_segment_size);

		if (startoff + nbytes > wal_segment_size)
			segbytes = wal_segment_size - startoff;
		else
			segbytes = nbytes;

		/* Need to seek in the file? */
		if (result->recvOff != startoff)
		{
			if (lseek(result->recvFile, (off_t) startoff, SEEK_SET) < 0)
				ereport(PANIC,
						(errcode_for_file_access(),
						 errmsg("could not seek in log segment %s to offset %u: %m",
								XLogFileNameP(result->recvFileTLI, result->recvSegNo),
								startoff)));
			result->recvOff = startoff;
		}

		/* OK to write the logs */
		errno = 0;

		byteswritten = write(result->recvFile, buf, segbytes);
		if (byteswritten <= 0)
		{
			/* if write didn't set errno, assume no disk space */
			if (errno == 0)
				errno = ENOSPC;
			ereport(PANIC,
					(errcode_for_file_access(),
					 errmsg("could not write to log segment %s "
							"at offset %u, length %lu: %m",
							XLogFileNameP(result->recvFileTLI, result->recvSegNo),
							result->recvOff, (unsigned long) segbytes)));
		}

		/* Update state for write */
		recptr += byteswritten;

		result->recvOff += byteswritten;
		nbytes -= byteswritten;
		buf += byteswritten;

		result->Write = recptr;
	}
}

/*
 * Flush the log to disk.
 *
 * If we're in the midst of dying, it's unwise to do anything that might throw
 * an error, so we skip sending a reply in that case.
 */
static void
XLogWalRcvFlush(bool dying)
{
	if (LogstreamResult.Flush < LogstreamResult.Write)
	{
		WalRcvData *walrcv = WalRcv;

		issue_xlog_fsync(recvFile, recvSegNo);

		LogstreamResult.Flush = LogstreamResult.Write;

		/* Update shared-memory status */
		SpinLockAcquire(&walrcv->mutex);
		if (walrcv->receivedUpto < LogstreamResult.Flush)
		{
			walrcv->latestChunkStart = walrcv->receivedUpto;
			walrcv->receivedUpto = LogstreamResult.Flush;
			walrcv->receivedTLI = ThisTimeLineID;
		}
		SpinLockRelease(&walrcv->mutex);

		/* POLAR: update received WAL lsn */
		if (polar_enable_dma)
			ConsensusSetXLogFlushedLSN(LogstreamResult.Flush, ThisTimeLineID, false);
		/* POLAR end */

		/* Signal the startup process and walsender that new WAL has arrived */
		WakeupRecovery();
		if (AllowCascadeReplication())
			WalSndWakeup();

		/* Report XLOG streaming progress in PS display */
		if (update_process_title)
		{
			char		activitymsg[50];

			snprintf(activitymsg, sizeof(activitymsg), "streaming %X/%X",
					 (uint32) (LogstreamResult.Write >> 32),
					 (uint32) LogstreamResult.Write);
			set_ps_display(activitymsg, false);
		}

		/* Also let the master know that we made some progress */
		if (!dying)
		{
			XLogWalRcvSendReply(false, false);
			XLogWalRcvSendHSFeedback(false);
		}
	}
}

/*
 * Parallel Flush the log to disk.
 */
static void
XLogWalRcvParallelFlush(LogFlushResultPerThread * result)
{
	issue_xlog_fsync(result->recvFile, result->recvSegNo);
}

/*
 * Parallel  sync the info to walecvData.
 */
static void
XLogWalRcvWakeup(void)
{
	WalRcvData *walrcv = WalRcv;

	/* Update shared-memory status */
	SpinLockAcquire(&walrcv->mutex);
	if (walrcv->receivedUpto < LogstreamResult.Flush)
	{
		walrcv->latestChunkStart = walrcv->receivedUpto;
		walrcv->receivedUpto = LogstreamResult.Flush;
		walrcv->receivedTLI = ThisTimeLineID;
	}
	SpinLockRelease(&walrcv->mutex);

	/* Signal the startup process and walsender that new WAL has arrived */
	WakeupRecovery();
	if (AllowCascadeReplication())
		WalSndWakeup();

	/* Report XLOG streaming progress in PS display */
	if (update_process_title)
	{
		char		activitymsg[50];

		snprintf(activitymsg, sizeof(activitymsg), "streaming %X/%X",
				 (uint32) (LogstreamResult.Flush >> 32),
				 (uint32) LogstreamResult.Flush);
		set_ps_display(activitymsg, false);
	}
}

/*
 * Send reply message to primary, indicating our current WAL locations, oldest
 * xmin and the current time.
 *
 * If 'force' is not set, the message is only sent if enough time has
 * passed since last status update to reach wal_receiver_status_interval.
 * If wal_receiver_status_interval is disabled altogether and 'force' is
 * false, this is a no-op.
 *
 * If 'requestReply' is true, requests the server to reply immediately upon
 * receiving this message. This is used for heartbearts, when approaching
 * wal_receiver_timeout.
 */
static void
XLogWalRcvSendReply(bool force, bool requestReply)
{
	static XLogRecPtr writePtr = 0;
	static XLogRecPtr flushPtr = 0;
	XLogRecPtr	applyPtr;
	static TimestampTz sendTime = 0;
	TimestampTz now;

	/*
	 * If the user doesn't want status to be reported to the master, be sure
	 * to exit before doing anything at all.
	 */
	if (!force && wal_receiver_status_interval <= 0)
		return;

	/* Get current timestamp. */
	now = GetCurrentTimestamp();

	/*
	 * We can compare the write and flush positions to the last message we
	 * sent without taking any lock, but the apply position requires a spin
	 * lock, so we don't check that unless something else has changed or 10
	 * seconds have passed.  This means that the apply WAL location will
	 * appear, from the master's point of view, to lag slightly, but since
	 * this is only for reporting purposes and only on idle systems, that's
	 * probably OK.
	 */
	if (EnableParallelWalreceiver)
	{
		if (!force
			&& writePtr == LogstreamResult.Receive
			&& flushPtr == LogstreamResult.Flush
			&& !TimestampDifferenceExceeds(sendTime, now,
										   wal_receiver_status_interval * 1000))
			return;
	}
	else
	{

		if (!force
			&& writePtr == LogstreamResult.Write
			&& flushPtr == LogstreamResult.Flush
			&& !TimestampDifferenceExceeds(sendTime, now,
										   wal_receiver_status_interval * 1000))
			return;

	}
	sendTime = now;

	/* Construct a new message */
	if (EnableParallelWalreceiver)
		writePtr = LogstreamResult.Receive;
	else
		writePtr = LogstreamResult.Write;
	flushPtr = LogstreamResult.Flush;
	applyPtr = polar_is_dma_logger_node() ? LogstreamResult.Flush : GetXLogReplayRecPtr(NULL);

	resetStringInfo(&reply_message);
	pq_sendbyte(&reply_message, 'r');
	pq_sendint64(&reply_message, writePtr);
	pq_sendint64(&reply_message, flushPtr);
	pq_sendint64(&reply_message, applyPtr);
	pq_sendint64(&reply_message, GetCurrentTimestamp());
	pq_sendbyte(&reply_message, requestReply ? 1 : 0);

	/* Send it */
	elog(DEBUG2, "sending write %X/%X flush %X/%X apply %X/%X%s",
		 (uint32) (writePtr >> 32), (uint32) writePtr,
		 (uint32) (flushPtr >> 32), (uint32) flushPtr,
		 (uint32) (applyPtr >> 32), (uint32) applyPtr,
		 requestReply ? " (reply requested)" : "");

	walrcv_send(wrconn, reply_message.data, reply_message.len);
}

/*
 * Send hot standby feedback message to primary, plus the current time,
 * in case they don't have a watch.
 *
 * If the user disables feedback, send one final message to tell sender
 * to forget about the xmin on this standby. We also send this message
 * on first connect because a previous connection might have set xmin
 * on a replication slot. (If we're not using a slot it's harmless to
 * send a feedback message explicitly setting InvalidTransactionId).
 */
static void
XLogWalRcvSendHSFeedback(bool immed)
{
	TimestampTz now;
	TransactionId nextXid;
	uint32		xmin_epoch,
				catalog_xmin_epoch;
	TransactionId xmin,
				catalog_xmin;
	static TimestampTz sendTime = 0;

	/* initially true so we always send at least one feedback message */
	static bool master_has_standby_xmin = true;

	/*
	 * If the user doesn't want status to be reported to the master, be sure
	 * to exit before doing anything at all.
	 */
	if ((wal_receiver_status_interval <= 0 || !hot_standby_feedback) &&
		!master_has_standby_xmin)
		return;

	/* Get current timestamp. */
	now = GetCurrentTimestamp();

	if (!immed)
	{
		/*
		 * Send feedback at most once per wal_receiver_status_interval.
		 */
		if (!TimestampDifferenceExceeds(sendTime, now,
										wal_receiver_status_interval * 1000))
			return;
		sendTime = now;
	}

	/*
	 * If Hot Standby is not yet accepting connections there is nothing to
	 * send. Check this after the interval has expired to reduce number of
	 * calls.
	 *
	 * Bailing out here also ensures that we don't send feedback until we've
	 * read our own replication slot state, so we don't tell the master to
	 * discard needed xmin or catalog_xmin from any slots that may exist on
	 * this replica.
	 */
	/* POLAR: Allow to send feedback in datamax mode */
	if (!HotStandbyActive() && !polar_is_dma_logger_node())
		return;

	/*
	 * Make the expensive call to get the oldest xmin once we are certain
	 * everything else has been checked.
	 */
	if (hot_standby_feedback)
	{
		TransactionId slot_xmin;

		/*
		 * Usually GetOldestXmin() would include both global replication slot
		 * xmin and catalog_xmin in its calculations, but we want to derive
		 * separate values for each of those. So we ask for an xmin that
		 * excludes the catalog_xmin.
		 */
		xmin = GetOldestXmin(NULL,
							 PROCARRAY_FLAGS_DEFAULT | PROCARRAY_SLOTS_XMIN);

		ProcArrayGetReplicationSlotXmin(&slot_xmin, &catalog_xmin);

		if (TransactionIdIsValid(slot_xmin) &&
			TransactionIdPrecedes(slot_xmin, xmin))
			xmin = slot_xmin;

		/*
		 * POLAR: Don't consider oldestXmin in datamax mode otherwise when
		 * datamax_oldestXmin < slot_xmin, datamax_oldestXmin will be sent to
		 * primary, which will infect the vacuum process of primary, but
		 * primary only cares about the xmin of standby in fact datamax just
		 * records and sends them to primary
		 */
		if (polar_is_dma_logger_node())
			xmin = slot_xmin;
		/* POLAR end */
	}
	else
	{
		xmin = InvalidTransactionId;
		catalog_xmin = InvalidTransactionId;
	}

	/*
	 * Get epoch and adjust if nextXid and oldestXmin are different sides of
	 * the epoch boundary.
	 */

	/*
	 * POLAR: get nextXid and epoch from polar_datamax_ctl when in datamax
	 * mode we have checked the sanity of xmin feedbacked by standby in
	 * ProcessStandbyHSFeedbackMessage func and there is no primary's data in
	 * datamax node so we can feedback the epoch according to primary's epoch
	 * recorded in polar_datamax_ctl
	 */
	if (!polar_is_dma_logger_node())
		GetNextXidAndEpoch(&nextXid, &xmin_epoch);
	else
	{
		nextXid = pg_atomic_read_u32(&polar_datamax_ctl->polar_primary_next_xid);
		xmin_epoch = pg_atomic_read_u32(&polar_datamax_ctl->polar_primary_epoch);
	}
	/* POLAR end */
	catalog_xmin_epoch = xmin_epoch;
	if (nextXid < xmin)
		xmin_epoch--;
	if (nextXid < catalog_xmin)
		catalog_xmin_epoch--;

	elog(DEBUG2, "sending hot standby feedback xmin %u epoch %u catalog_xmin %u catalog_xmin_epoch %u",
		 xmin, xmin_epoch, catalog_xmin, catalog_xmin_epoch);

	/* Construct the message and send it. */
	resetStringInfo(&reply_message);
	pq_sendbyte(&reply_message, 'h');
	pq_sendint64(&reply_message, GetCurrentTimestamp());
	pq_sendint32(&reply_message, xmin);
	pq_sendint32(&reply_message, xmin_epoch);
	pq_sendint32(&reply_message, catalog_xmin);
	pq_sendint32(&reply_message, catalog_xmin_epoch);
	walrcv_send(wrconn, reply_message.data, reply_message.len);
	if (TransactionIdIsValid(xmin) || TransactionIdIsValid(catalog_xmin))
		master_has_standby_xmin = true;
	else
		master_has_standby_xmin = false;
}

/*
 * Update shared memory status upon receiving a message from primary.
 *
 * 'walEnd' and 'sendTime' are the end-of-WAL and timestamp of the latest
 * message, reported by primary.
 */
static void
ProcessWalSndrMessage(XLogRecPtr walEnd, TimestampTz sendTime)
{
	WalRcvData *walrcv = WalRcv;

	TimestampTz lastMsgReceiptTime = GetCurrentTimestamp();

	/* Update shared-memory status */
	SpinLockAcquire(&walrcv->mutex);
	if (walrcv->latestWalEnd < walEnd)
		walrcv->latestWalEndTime = sendTime;
	walrcv->latestWalEnd = walEnd;
	walrcv->lastMsgSendTime = sendTime;
	walrcv->lastMsgReceiptTime = lastMsgReceiptTime;
	SpinLockRelease(&walrcv->mutex);

	if (log_min_messages <= DEBUG2)
	{
		char	   *sendtime;
		char	   *receipttime;
		int			applyDelay;

		/* Copy because timestamptz_to_str returns a static buffer */
		sendtime = pstrdup(timestamptz_to_str(sendTime));
		receipttime = pstrdup(timestamptz_to_str(lastMsgReceiptTime));
		applyDelay = GetReplicationApplyDelay();

		/* apply delay is not available */
		if (applyDelay == -1)
			elog(DEBUG2, "sendtime %s receipttime %s replication apply delay (N/A) transfer latency %d ms",
				 sendtime,
				 receipttime,
				 GetReplicationTransferLatency());
		else
			elog(DEBUG2, "sendtime %s receipttime %s replication apply delay %d ms transfer latency %d ms",
				 sendtime,
				 receipttime,
				 applyDelay,
				 GetReplicationTransferLatency());

		pfree(sendtime);
		pfree(receipttime);
	}
}

/*
 * Wake up the walreceiver main loop.
 *
 * This is called by the startup process whenever interesting xlog records
 * are applied, so that walreceiver can check if it needs to send an apply
 * notification back to the master which may be waiting in a COMMIT with
 * synchronous_commit = remote_apply.
 */
void
WalRcvForceReply(void)
{
	Latch	   *latch;

	WalRcv->force_reply = true;
	/* fetching the latch pointer might not be atomic, so use spinlock */
	SpinLockAcquire(&WalRcv->mutex);
	latch = WalRcv->latch;
	SpinLockRelease(&WalRcv->mutex);
	if (latch)
		SetLatch(latch);
}

/*
 * Return a string constant representing the state. This is used
 * in system functions and views, and should *not* be translated.
 */
static const char *
WalRcvGetStateString(WalRcvState state)
{
	switch (state)
	{
		case WALRCV_STOPPED:
			return "stopped";
		case WALRCV_STARTING:
			return "starting";
		case WALRCV_STREAMING:
			return "streaming";
		case WALRCV_WAITING:
			return "waiting";
		case WALRCV_RESTARTING:
			return "restarting";
		case WALRCV_STOPPING:
			return "stopping";
	}
	return "UNKNOWN";
}

/*
 * Returns activity of WAL receiver, including pid, state and xlog locations
 * received from the WAL sender of another server.
 */
Datum
pg_stat_get_wal_receiver(PG_FUNCTION_ARGS)
{
	TupleDesc	tupdesc;
	Datum	   *values;
	bool	   *nulls;
	int			pid;
	bool		ready_to_display;
	WalRcvState state;
	XLogRecPtr	receive_start_lsn;
	TimeLineID	receive_start_tli;
	XLogRecPtr	received_lsn;
	TimeLineID	received_tli;
	TimestampTz last_send_time;
	TimestampTz last_receipt_time;
	XLogRecPtr	latest_end_lsn;
	TimestampTz latest_end_time;
	char		sender_host[NI_MAXHOST];
	int			sender_port = 0;
	char		slotname[NAMEDATALEN];
	char		conninfo[MAXCONNINFO];

	/* Take a lock to ensure value consistency */
	SpinLockAcquire(&WalRcv->mutex);
	pid = (int) WalRcv->pid;
	ready_to_display = WalRcv->ready_to_display;
	state = WalRcv->walRcvState;
	receive_start_lsn = WalRcv->receiveStart;
	receive_start_tli = WalRcv->receiveStartTLI;
	received_lsn = WalRcv->receivedUpto;
	received_tli = WalRcv->receivedTLI;
	last_send_time = WalRcv->lastMsgSendTime;
	last_receipt_time = WalRcv->lastMsgReceiptTime;
	latest_end_lsn = WalRcv->latestWalEnd;
	latest_end_time = WalRcv->latestWalEndTime;
	strlcpy(slotname, (char *) WalRcv->slotname, sizeof(slotname));
	strlcpy(sender_host, (char *) WalRcv->sender_host, sizeof(sender_host));
	sender_port = WalRcv->sender_port;
	strlcpy(conninfo, (char *) WalRcv->conninfo, sizeof(conninfo));
	SpinLockRelease(&WalRcv->mutex);

	/*
	 * No WAL receiver (or not ready yet), just return a tuple with NULL
	 * values
	 */
	if (pid == 0 || !ready_to_display)
		PG_RETURN_NULL();

	/* determine result type */
	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	values = palloc0(sizeof(Datum) * tupdesc->natts);
	nulls = palloc0(sizeof(bool) * tupdesc->natts);

	/* Fetch values */
	values[0] = Int32GetDatum(pid);

	if (!is_member_of_role(GetUserId(), DEFAULT_ROLE_READ_ALL_STATS))
	{
		/*
		 * Only superusers and members of pg_read_all_stats can see details.
		 * Other users only get the pid value to know whether it is a WAL
		 * receiver, but no details.
		 */
		MemSet(&nulls[1], true, sizeof(bool) * (tupdesc->natts - 1));
	}
	else
	{
		values[1] = CStringGetTextDatum(WalRcvGetStateString(state));

		if (XLogRecPtrIsInvalid(receive_start_lsn))
			nulls[2] = true;
		else
			values[2] = LSNGetDatum(receive_start_lsn);
		values[3] = Int32GetDatum(receive_start_tli);
		if (XLogRecPtrIsInvalid(received_lsn))
			nulls[4] = true;
		else
			values[4] = LSNGetDatum(received_lsn);
		values[5] = Int32GetDatum(received_tli);
		if (last_send_time == 0)
			nulls[6] = true;
		else
			values[6] = TimestampTzGetDatum(last_send_time);
		if (last_receipt_time == 0)
			nulls[7] = true;
		else
			values[7] = TimestampTzGetDatum(last_receipt_time);
		if (XLogRecPtrIsInvalid(latest_end_lsn))
			nulls[8] = true;
		else
			values[8] = LSNGetDatum(latest_end_lsn);
		if (latest_end_time == 0)
			nulls[9] = true;
		else
			values[9] = TimestampTzGetDatum(latest_end_time);
		if (*slotname == '\0')
			nulls[10] = true;
		else
			values[10] = CStringGetTextDatum(slotname);
		if (*sender_host == '\0')
			nulls[11] = true;
		else
			values[11] = CStringGetTextDatum(sender_host);
		if (sender_port == 0)
			nulls[12] = true;
		else
			values[12] = Int32GetDatum(sender_port);
		if (*conninfo == '\0')
			nulls[13] = true;
		else
			values[13] = CStringGetTextDatum(conninfo);
	}

	/* Returns the record as Datum */
	PG_RETURN_DATUM(HeapTupleGetDatum(heap_form_tuple(tupdesc, values, nulls)));
}

/*
 * Thread that will write wal log
 */
static void *
walrecevier_async_write_management_thread(void *arg)
{
	int32		threadIndex = 0;
	WALParallelWriteReq *request = NULL;
	LogFlushResultPerThread *result;

	threadIndex = ((WALReceiverWorkerThreadParam *) arg)->threadIndex;
	result = &rcv_ParallelWriteControl.results[threadIndex];

	while (1)
	{
		/* wait for signal */
		ThreadSemaRecDown(&rcv_ParallelWriteControl.sem[threadIndex]);

		/* create connect as needed */
		request = (WALParallelWriteReq *) PipeGetRec(rcv_ParallelWriteControl.request[threadIndex]);
		if (request)
		{

			switch (request->cmd)
			{
				case COMMAND_WRITE:
					{
						XLogWalRcvParallelWrite(result, request, threadIndex);
						request->Write = result->Write;
						request->recvFile = result->recvFile;
						request->recvSegNo = result->recvSegNo;
						request->recvFileTLI = result->recvFileTLI;
						break;
					}
				case COMMAND_FLUSH:
					{
						if (result->Flush < result->Write)
						{
							XLogWalRcvParallelFlush(result);
						}
						result->Flush = result->Write;
						request->Flush = result->Flush;
						break;
					}
				default:
					{
						abort();
					}
			}

			while (-1 == PipePutRec(rcv_ParallelWriteControl.response[threadIndex], request))
			{
				pg_usleep(1000L);
			}

		}
	}
	return NULL;
}

static void
WalRcvParallelWrite(char *buf, Size nbytes, XLogRecPtr recptr)
{
	WALParallelWriteReq *writeReq = NULL;
	int			threadIndex;
	int			round = 1;
	int			startoff;
	int			segbytes;
	XLogSegNo	segNo;

	while (nbytes > 0)
	{
		XLByteToSeg(recptr, segNo, wal_segment_size);
		threadIndex = segNo % max_parallel_write_thread;

		startoff = XLogSegmentOffset(recptr, wal_segment_size);

		if (startoff + nbytes > wal_segment_size)
			segbytes = wal_segment_size - startoff;
		else
			segbytes = nbytes;

		writeReq = (WALParallelWriteReq *) palloc0(sizeof(WALParallelWriteReq));
		writeReq->buf = palloc(segbytes);
		memcpy(writeReq->buf, buf, segbytes);

		writeReq->len = segbytes;
		writeReq->start = recptr;
		writeReq->cmd = COMMAND_WRITE;

		while (-1 == PipePutRec(rcv_ParallelWriteControl.request[threadIndex], (void *) writeReq))
		{
			round++;
		}
		rcv_ParallelWriteControl.messageCnt++;
		ThreadSemaRecUp(&rcv_ParallelWriteControl.sem[threadIndex]);

		recptr += segbytes;
		nbytes -= segbytes;
		buf += segbytes;
	}
}

static void
WalRcvParallelFlush(XLogRecPtr flush)
{
	WALParallelWriteReq *flushReq = NULL;
	int			threadIndex = 0;
	int			round = 1;
	XLogSegNo	segNo;

	if (LogstreamResult.Send >= LogstreamResult.Receive)
		return;
	XLByteToSeg(flush, segNo, wal_segment_size);
	threadIndex = segNo % max_parallel_write_thread;

	flushReq = (WALParallelWriteReq *) palloc0(sizeof(WALParallelWriteReq));
	flushReq->cmd = COMMAND_FLUSH;


	while (-1 == PipePutRec(rcv_ParallelWriteControl.request[threadIndex], (void *) flushReq))
	{
		round++;
	}
	rcv_ParallelWriteControl.messageCnt++;
	ThreadSemaRecUp(&rcv_ParallelWriteControl.sem[threadIndex]);
	LogstreamResult.Send = LogstreamResult.Receive;
}

static void
WalRcvParallelSync(void)
{
	int			threadIndex = 0;
	int			responseCnt = 0;
	WALParallelWriteReq *wrsp = NULL;
	XLogRecPtr	minWrite = 0;
	XLogRecPtr	minFlush = 0;
	int			minRecvFile = -1;
	TimeLineID	minRecvFileTLI = 0;
	XLogSegNo	minRecvSegNo = 0;
	CurrentState *state = NULL;
	int			minWriteThread = -1;
	int			minFlushThread = -1;

	if (rcv_ParallelWriteControl.messageCnt == 0)
		return;

	while (threadIndex < max_parallel_write_thread)
	{
		state = &(rcv_ParallelWriteControl.state[threadIndex]);
		for (responseCnt = 0; responseCnt < max_parallel_write_pipe_len; responseCnt++)
		{
			wrsp = (WALParallelWriteReq *) PipeGetRec(rcv_ParallelWriteControl.response[threadIndex]);
			if (wrsp)
			{
				switch (wrsp->cmd)
				{
					case COMMAND_WRITE:
						{
							state->threadWrite = wrsp->Write;
							state->threadRecvFile = wrsp->recvFile;
							state->threadRecvSegNo = wrsp->recvSegNo;
							state->threadRecvFileTLI = wrsp->recvFileTLI;

							if (wrsp->needClose)
							{
								char		xlogfname[MAXFNAMELEN];

								state->threadFlush = wrsp->Flush;
								if (close(wrsp->recvFileFlush) != 0)
									ereport(PANIC,
											(errcode_for_file_access(),
											 errmsg("could not close log segment %s: %m",
													XLogFileNameP(wrsp->recvFileFlush, wrsp->recvSegNoFlush))));
								XLogFileName(xlogfname, wrsp->recvFileTLIFlush, wrsp->recvSegNoFlush, wal_segment_size);
								polar_is_dma_logger_mode = polar_is_dma_logger_node();	/* POLAR: Enter datamax
																						 * Mode. */
								if (XLogArchiveMode != ARCHIVE_MODE_ALWAYS)
									XLogArchiveForceDone(xlogfname);
								else if (polar_enable_dma)
									polar_dma_xlog_archive_notify(xlogfname, true);
								else
									XLogArchiveNotify(xlogfname);
								polar_is_dma_logger_mode = false;	/* POLAR: Leave datamax
																	 * mode. */
							}

							if (wrsp->buf)
								pfree(wrsp->buf);
							pfree(wrsp);
							rcv_ParallelWriteControl.messageCnt--;
							break;
						}
					case COMMAND_FLUSH:
						{
							state->threadFlush = wrsp->Flush;
							pfree(wrsp);
							rcv_ParallelWriteControl.messageCnt--;
							break;
						}
					default:
						{
							abort();
						}
				}
			}
			else
				break;

		}
		threadIndex++;

		if (state->threadWrite == 0)
		{
			state->threadWrite = LogstreamResult.Write;
			state->threadFlush = LogstreamResult.Flush;
		}
		if ((minWrite == 0) || (minWrite > state->threadWrite))
		{
			minWrite = state->threadWrite;
			minRecvFile = state->threadRecvFile;
			minRecvSegNo = state->threadRecvSegNo;
			minRecvFileTLI = state->threadRecvFileTLI;
			minWriteThread = threadIndex - 1;
		}
		if ((minFlush == 0) || (minFlush > state->threadFlush))
		{
			minFlush = state->threadFlush;
			minFlushThread = threadIndex - 1;
		}
	}
	if (rcv_ParallelWriteControl.messageCnt != 0)
	{
		CurrentState *writeState = NULL;
		CurrentState *flushState = NULL;
		bool		needContinueWrite = (minWrite % wal_segment_size == 0);
		bool		needContinueFlush = (minFlush % wal_segment_size == 0);
		XLogRecPtr	minWriteTmp = minWrite;
		XLogRecPtr	minFlushTmp = minFlush;

		threadIndex = 0;
		while (threadIndex < max_parallel_write_thread - 1)
		{
			writeState = &(rcv_ParallelWriteControl.state[(minWriteThread + threadIndex + 1) % max_parallel_write_thread]);
			flushState = &(rcv_ParallelWriteControl.state[(minFlushThread + threadIndex + 1) % max_parallel_write_thread]);
			threadIndex++;
			if (needContinueWrite)
			{
				if (writeState->threadWrite > (minWriteTmp + wal_segment_size))
				{
					needContinueWrite = false;
				}
				else if (writeState->threadWrite == (minWriteTmp + wal_segment_size))
				{
					minWriteTmp = minWriteTmp + wal_segment_size;
					minWrite = writeState->threadWrite;
					minRecvFile = writeState->threadRecvFile;
					minRecvSegNo = writeState->threadRecvSegNo;
					minRecvFileTLI = writeState->threadRecvFileTLI;
				}
				else if (writeState->threadWrite > minWriteTmp)
				{
					needContinueWrite = false;
					minWrite = writeState->threadWrite;
					minRecvFile = writeState->threadRecvFile;
					minRecvSegNo = writeState->threadRecvSegNo;
					minRecvFileTLI = writeState->threadRecvFileTLI;
				}
			}
			if (needContinueFlush)
			{
				if (flushState->threadFlush > (minFlushTmp + wal_segment_size))
				{
					needContinueFlush = false;
				}
				else if (flushState->threadFlush == (minFlushTmp + wal_segment_size))
				{
					minFlushTmp = minFlushTmp + wal_segment_size;
					minFlush = flushState->threadFlush;
				}
				else if (flushState->threadFlush > minFlushTmp)
				{
					needContinueWrite = false;
					minFlush = flushState->threadFlush;
				}
			}
		}
	}
	else
	{
		threadIndex = 0;
		while (threadIndex < max_parallel_write_thread)
		{
			state = &(rcv_ParallelWriteControl.state[threadIndex]);
			if ((minWrite == 0) || (minWrite < state->threadWrite))
			{
				minWrite = state->threadWrite;
				minRecvFile = state->threadRecvFile;
				minRecvSegNo = state->threadRecvSegNo;
				minRecvFileTLI = state->threadRecvFileTLI;
			}
			if ((minFlush == 0) || (minFlush < state->threadFlush))
			{
				minFlush = state->threadFlush;
			}
			threadIndex++;
		}

	}
	if (minWrite > LogstreamResult.Write)
	{
		LogstreamResult.Write = minWrite;
		recvFile = minRecvFile;
		recvSegNo = minRecvSegNo;
		recvFileTLI = minRecvFileTLI;
	}
	if (minFlush > LogstreamResult.Flush)
	{
		LogstreamResult.Flush = minFlush;

		/* POLAR: update received WAL lsn */
		if (polar_enable_dma)
			ConsensusSetXLogFlushedLSN(LogstreamResult.Flush, ThisTimeLineID, false);
		/* POLAR end */

		XLogWalRcvWakeup();
		XLogWalRcvSendReply(false, false);
		XLogWalRcvSendHSFeedback(false);
	}
}

static int
XLogFileParallelInit(XLogSegNo logsegno, bool *use_existent, bool use_lock, int threadIndex)
{
	char		path[MAXPGPATH];
	char		tmppath[MAXPGPATH];
	PGAlignedXLogBlock zbuffer;
	XLogSegNo	installed_segno;
	XLogSegNo	max_segno;
	int			fd;
	int			nbytes;

	XLogFilePath(path, ThisTimeLineID, logsegno, wal_segment_size);

	if (*use_existent)
	{
		fd = BasicOpenFile(path, O_RDWR | PG_BINARY | GetSyncBit());
		if (fd < 0)
		{
			if (errno != ENOENT)
				ereport(ERROR,
						(errcode_for_file_access(),
						 errmsg("could not open file \"%s\": %m", path)));
		}
		else
			return fd;
	}

	snprintf(tmppath, MAXPGPATH, XLOGDIR "/xlogtemp.%d.%d", (int) getpid(), threadIndex);

	unlink(tmppath);

	fd = BasicOpenFile(tmppath, O_RDWR | O_CREAT | O_EXCL | PG_BINARY);
	if (fd < 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not create file \"%s\": %m", tmppath)));

#if defined(HAVE_POSIX_FALLOCATE) && defined(__linux__)
	if (wal_warmup)
#endif
	{
		memset(zbuffer.data, 0, XLOG_BLCKSZ);
		for (nbytes = 0; nbytes < wal_segment_size; nbytes += XLOG_BLCKSZ)
		{
			errno = 0;
			if ((int) write(fd, zbuffer.data, XLOG_BLCKSZ) != (int) XLOG_BLCKSZ)
			{
				int			save_errno = errno;

				unlink(tmppath);
				close(fd);

				errno = save_errno ? save_errno : ENOSPC;

				ereport(ERROR,
						(errcode_for_file_access(),
						 errmsg("could not write to file \"%s\": %m", tmppath)));
			}
		}
	}
#if defined(HAVE_POSIX_FALLOCATE) && defined(__linux__)
	else
	{
#if defined(HAVE_POSIX_FALLOCATE) && defined(__linux__)
		errno = posix_fallocate(fd, 0, wal_segment_size);
		if (errno != 0)
		{
			close(fd);
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not write to file \"%s\": %m", tmppath)));
		}
#else
		elog(ERROR, "POSIX FALLOCATE is not supported");
#endif
	}
#endif
	if (pg_fsync(fd) != 0)
	{
		int			save_errno = errno;

		close(fd);
		errno = save_errno;
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not fsync file \"%s\": %m", tmppath)));
	}

	if (close(fd))
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not close file \"%s\": %m", tmppath)));

	installed_segno = logsegno;

	max_segno = logsegno + CheckPointSegments;

	{
		char		path[MAXPGPATH];
		struct stat stat_buf;

		XLogFilePath(path, ThisTimeLineID, installed_segno, wal_segment_size);

		/*
		 * We want to be sure that only one process does this at a time.
		 */
		ThreadMutexLockRec(&rcv_ParallelWriteControl.m_mutex);

		LWLockAcquire(ControlFileLock, LW_EXCLUSIVE);

		/* Find a free slot to put it in */
		while (stat(path, &stat_buf) == 0)
		{
			if (installed_segno >= max_segno)
			{
				/* Failed to find a free slot within specified range */
				LWLockRelease(ControlFileLock);
				ThreadMutexUnlockRec(&rcv_ParallelWriteControl.m_mutex);
				return false;
			}
			installed_segno++;
			XLogFilePath(path, ThisTimeLineID, installed_segno, wal_segment_size);
		}

		/*
		 * Perform the rename using link if available, paranoidly trying to
		 * avoid overwriting an existing file (there shouldn't be one).
		 */
		if (durable_link_or_rename(tmppath, path, LOG) != 0)
		{
			LWLockRelease(ControlFileLock);
			ThreadMutexUnlockRec(&rcv_ParallelWriteControl.m_mutex);
			/* durable_link_or_rename already emitted log message */
			return false;
		}

		LWLockRelease(ControlFileLock);
		ThreadMutexUnlockRec(&rcv_ParallelWriteControl.m_mutex);
	}
	*use_existent = false;

	fd = BasicOpenFile(path, O_RDWR | PG_BINARY | GetSyncBit());
	if (fd < 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not open file \"%s\": %m", path)));
	return fd;
}

/*
 * POLAR: return received LSN in DMA mode
 */
XLogRecPtr
polar_dma_get_received_lsn(void)
{
	XLogRecPtr	receivePtr;
	TimeLineID	receiveTLI;

	ConsensusGetXLogFlushedLSN(&receivePtr, &receiveTLI);

	return receivePtr;
}

/* POLAR end */
