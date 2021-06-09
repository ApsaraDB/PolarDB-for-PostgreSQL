/*-------------------------------------------------------------------------
 *
 * walreceiver.h
 *	  Exports from replication/walreceiverfuncs.c.
 *
 * Portions Copyright (c) 2020, Alibaba Group Holding Limited
 * Portions Copyright (c) 2010-2018, PostgreSQL Global Development Group
 *
 * src/include/replication/walreceiver.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef _WALRECEIVER_H
#define _WALRECEIVER_H

#include "access/xlog.h"
#include "access/xlogdefs.h"
#include "fmgr.h"
#include "getaddrinfo.h"		/* for NI_MAXHOST */
#include "replication/logicalproto.h"
#include "replication/walsender.h"
#include "storage/latch.h"
#include "storage/spin.h"
#include "pgtime.h"
#include "utils/tuplestore.h"

/* user-settable parameters */
extern int	wal_receiver_status_interval;
extern int	wal_receiver_timeout;
extern bool hot_standby_feedback;

/*
 * MAXCONNINFO: maximum size of a connection string.
 *
 * XXX: Should this move to pg_config_manual.h?
 */
#define MAXCONNINFO		1024

/* Can we allow the standby to accept replication connection from another standby? */
#define AllowCascadeReplication() (EnableHotStandby && max_wal_senders > 0)

/*
 * Values for WalRcv->walRcvState.
 */
typedef enum
{
	WALRCV_STOPPED,				/* stopped and mustn't start up again */
	WALRCV_STARTING,			/* launched, but the process hasn't
								 * initialized yet */
	WALRCV_STREAMING,			/* walreceiver is streaming */
	WALRCV_WAITING,				/* stopped streaming, waiting for orders */
	WALRCV_RESTARTING,			/* asked to restart streaming */
	WALRCV_STOPPING				/* requested to stop, but still running */
} WalRcvState;

/* Shared memory area for management of walreceiver process */
typedef struct
{
	/*
	 * PID of currently active walreceiver process, its current state and
	 * start time (actually, the time at which it was requested to be
	 * started).
	 */
	pid_t		pid;
	WalRcvState walRcvState;
	pg_time_t	startTime;

	/*
	 * receiveStart and receiveStartTLI indicate the first byte position and
	 * timeline that will be received. When startup process starts the
	 * walreceiver, it sets these to the point where it wants the streaming to
	 * begin.
	 */
	XLogRecPtr	receiveStart;
	TimeLineID	receiveStartTLI;

	/*
	 * receivedUpto-1 is the last byte position that has already been
	 * received, and receivedTLI is the timeline it came from.  At the first
	 * startup of walreceiver, these are set to receiveStart and
	 * receiveStartTLI. After that, walreceiver updates these whenever it
	 * flushes the received WAL to disk.
	 */
	XLogRecPtr	receivedUpto;
	TimeLineID	receivedTLI;

	/*
	 * latestChunkStart is the starting byte position of the current "batch"
	 * of received WAL.  It's actually the same as the previous value of
	 * receivedUpto before the last flush to disk.  Startup process can use
	 * this to detect whether it's keeping up or not.
	 */
	XLogRecPtr	latestChunkStart;

	/*
	 * Time of send and receive of any message received.
	 */
	TimestampTz lastMsgSendTime;
	TimestampTz lastMsgReceiptTime;

	/*
	 * Latest reported end of WAL on the sender
	 */
	XLogRecPtr	latestWalEnd;
	TimestampTz latestWalEndTime;

	/*
	 * connection string; initially set to connect to the primary, and later
	 * clobbered to hide security-sensitive fields.
	 */
	char		conninfo[MAXCONNINFO];

	/*
	 * Host name (this can be a host name, an IP address, or a directory path)
	 * and port number of the active replication connection.
	 */
	char		sender_host[NI_MAXHOST];
	int			sender_port;

	/*
	 * replication slot name; is also used for walreceiver to connect with the
	 * primary
	 */
	char		slotname[NAMEDATALEN];

	/* set true once conninfo is ready to display (obfuscated pwds etc) */
	bool		ready_to_display;

	/*
	 * Latch used by startup process to wake up walreceiver after telling it
	 * where to start streaming (after setting receiveStart and
	 * receiveStartTLI), and also to tell it to send apply feedback to the
	 * primary whenever specially marked commit records are applied. This is
	 * normally mapped to procLatch when walreceiver is running.
	 */
	Latch	   *latch;

	slock_t		mutex;			/* locks shared variables shown above */

	/*
	 * force walreceiver reply?  This doesn't need to be locked; memory
	 * barriers for ordering are sufficient.  But we do need atomic fetch and
	 * store semantics, so use sig_atomic_t.
	 */
	sig_atomic_t force_reply;	/* used as a bool */
} WalRcvData;

extern WalRcvData *WalRcv;

typedef struct
{
	bool		logical;		/* True if this is logical replication stream,
								 * false if physical stream.  */
	char	   *slotname;		/* Name of the replication slot or NULL. */
	XLogRecPtr	startpoint;		/* LSN of starting point. */

	union
	{
		struct
		{
			TimeLineID	startpointTLI;	/* Starting timeline */
		}			physical;
		struct
		{
			uint32		proto_version;	/* Logical protocol version */
			List	   *publication_names;	/* String list of publications */
		}			logical;
	}			proto;
	polar_repl_mode_t polar_repl_mode;
} WalRcvStreamOptions;

struct WalReceiverConn;
typedef struct WalReceiverConn WalReceiverConn;

/*
 * Status of walreceiver query execution.
 *
 * We only define statuses that are currently used.
 */
typedef enum
{
	WALRCV_ERROR,				/* There was error when executing the query. */
	WALRCV_OK_COMMAND,			/* Query executed utility or replication
								 * command. */
	WALRCV_OK_TUPLES,			/* Query returned tuples. */
	WALRCV_OK_COPY_IN,			/* Query started COPY FROM. */
	WALRCV_OK_COPY_OUT,			/* Query started COPY TO. */
	WALRCV_OK_COPY_BOTH			/* Query started COPY BOTH replication
								 * protocol. */
} WalRcvExecStatus;

/*
 * Return value for walrcv_query, returns the status of the execution and
 * tuples if any.
 */
typedef struct WalRcvExecResult
{
	WalRcvExecStatus status;
	char	   *err;
	Tuplestorestate *tuplestore;
	TupleDesc	tupledesc;
} WalRcvExecResult;

/* libpqwalreceiver hooks */
typedef WalReceiverConn *(*walrcv_connect_fn) (const char *conninfo, bool logical,
											   const char *appname,
											   char **err);
typedef void (*walrcv_check_conninfo_fn) (const char *conninfo);
typedef char *(*walrcv_get_conninfo_fn) (WalReceiverConn *conn);
typedef void (*walrcv_get_senderinfo_fn) (WalReceiverConn *conn,
										  char **sender_host,
										  int *sender_port);
typedef char *(*walrcv_identify_system_fn) (WalReceiverConn *conn,
											TimeLineID *primary_tli,
											int *server_version);
typedef void (*walrcv_readtimelinehistoryfile_fn) (WalReceiverConn *conn,
												   TimeLineID tli,
												   char **filename,
												   char **content, int *size);
typedef bool (*walrcv_startstreaming_fn) (WalReceiverConn *conn,
										  const WalRcvStreamOptions *options);
typedef void (*walrcv_endstreaming_fn) (WalReceiverConn *conn,
										TimeLineID *next_tli);
#ifdef ENABLE_REMOTE_RECOVERY
typedef bool (*walrcv_startfetchpage_fn) (WalReceiverConn *conn,
										  const WalRcvStreamOptions *options);
typedef void (*walrcv_endfetchpage_fn) (WalReceiverConn *conn);
typedef int (*walrcv_fetchpage_fn) (WalReceiverConn *conn, char **buffer);
#endif
typedef int (*walrcv_receive_fn) (WalReceiverConn *conn, char **buffer,
								  pgsocket *wait_fd);
typedef void (*walrcv_send_fn) (WalReceiverConn *conn, const char *buffer,
								int nbytes);
#ifdef ENABLE_DISTRIBUTED_TRANSACTION
typedef char *(*walrcv_create_slot_fn) (WalReceiverConn *conn,
										const char *slotname, bool temporary,
										CRSSnapshotAction snapshot_action,
										XLogRecPtr *lsn, GlobalTimestamp * snapshot_start_ts);
#else
typedef char *(*walrcv_create_slot_fn) (WalReceiverConn *conn,
										const char *slotname, bool temporary,
										CRSSnapshotAction snapshot_action,
										XLogRecPtr *lsn);
#endif
typedef WalRcvExecResult *(*walrcv_exec_fn) (WalReceiverConn *conn,
											 const char *query,
											 const int nRetTypes,
											 const Oid *retTypes);
typedef void (*walrcv_disconnect_fn) (WalReceiverConn *conn);

typedef struct WalReceiverFunctionsType
{
	walrcv_connect_fn walrcv_connect;
	walrcv_check_conninfo_fn walrcv_check_conninfo;
	walrcv_get_conninfo_fn walrcv_get_conninfo;
	walrcv_get_senderinfo_fn walrcv_get_senderinfo;
	walrcv_identify_system_fn walrcv_identify_system;
	walrcv_readtimelinehistoryfile_fn walrcv_readtimelinehistoryfile;
	walrcv_startstreaming_fn walrcv_startstreaming;
	walrcv_endstreaming_fn walrcv_endstreaming;
#ifdef ENABLE_REMOTE_RECOVERY
	walrcv_startfetchpage_fn walrcv_startfetchpage;
	walrcv_endfetchpage_fn walrcv_endfetchpage;
	walrcv_fetchpage_fn walrcv_fetchpage;
#endif
	walrcv_receive_fn walrcv_receive;
	walrcv_send_fn walrcv_send;
	walrcv_create_slot_fn walrcv_create_slot;
	walrcv_exec_fn walrcv_exec;
	walrcv_disconnect_fn walrcv_disconnect;
} WalReceiverFunctionsType;

extern PGDLLIMPORT WalReceiverFunctionsType *WalReceiverFunctions;

#define walrcv_connect(conninfo, logical, appname, err) \
	WalReceiverFunctions->walrcv_connect(conninfo, logical, appname, err)
#define walrcv_check_conninfo(conninfo) \
	WalReceiverFunctions->walrcv_check_conninfo(conninfo)
#define walrcv_get_conninfo(conn) \
	WalReceiverFunctions->walrcv_get_conninfo(conn)
#define walrcv_get_senderinfo(conn, sender_host, sender_port) \
	WalReceiverFunctions->walrcv_get_senderinfo(conn, sender_host, sender_port)
#define walrcv_identify_system(conn, primary_tli, server_version) \
	WalReceiverFunctions->walrcv_identify_system(conn, primary_tli, server_version)
#define walrcv_readtimelinehistoryfile(conn, tli, filename, content, size) \
	WalReceiverFunctions->walrcv_readtimelinehistoryfile(conn, tli, filename, content, size)
#define walrcv_startstreaming(conn, options) \
	WalReceiverFunctions->walrcv_startstreaming(conn, options)
#define walrcv_endstreaming(conn, next_tli) \
	WalReceiverFunctions->walrcv_endstreaming(conn, next_tli)
#ifdef ENABLE_REMOTE_RECOVERY
#define walrcv_startfetchpage(conn, options) \
	WalReceiverFunctions->walrcv_startfetchpage(conn, options)
#define walrcv_endfetchpage(conn) \
	WalReceiverFunctions->walrcv_endfetchpage(conn)
#define walrcv_fetchpage(conn, buffer) \
	WalReceiverFunctions->walrcv_fetchpage(conn, buffer)
#endif
#define walrcv_receive(conn, buffer, wait_fd) \
	WalReceiverFunctions->walrcv_receive(conn, buffer, wait_fd)
#define walrcv_send(conn, buffer, nbytes) \
	WalReceiverFunctions->walrcv_send(conn, buffer, nbytes)
#ifdef ENABLE_DISTRIBUTED_TRANSACTION
#define walrcv_create_slot(conn, slotname, temporary, snapshot_action, lsn, snapshot_start_ts) \
	WalReceiverFunctions->walrcv_create_slot(conn, slotname, temporary, snapshot_action, lsn, snapshot_start_ts)
#else
#define walrcv_create_slot(conn, slotname, temporary, snapshot_action, lsn) \
	WalReceiverFunctions->walrcv_create_slot(conn, slotname, temporary, snapshot_action, lsn)
#endif
#define walrcv_exec(conn, exec, nRetTypes, retTypes) \
	WalReceiverFunctions->walrcv_exec(conn, exec, nRetTypes, retTypes)
#define walrcv_disconnect(conn) \
	WalReceiverFunctions->walrcv_disconnect(conn)

static inline void
walrcv_clear_result(WalRcvExecResult *walres)
{
	if (!walres)
		return;

	if (walres->err)
		pfree(walres->err);

	if (walres->tuplestore)
		tuplestore_end(walres->tuplestore);

	if (walres->tupledesc)
		FreeTupleDesc(walres->tupledesc);

	pfree(walres);
}

/* prototypes for functions in walreceiver.c */
extern void WalReceiverMain(void) pg_attribute_noreturn();

/* prototypes for functions in walreceiverfuncs.c */
extern Size WalRcvShmemSize(void);
extern void WalRcvShmemInit(void);
extern void ShutdownWalRcv(void);
extern bool WalRcvStreaming(void);
extern bool WalRcvRunning(void);
extern void RequestXLogStreaming(TimeLineID tli, XLogRecPtr recptr,
					 const char *conninfo, const char *slotname);
extern XLogRecPtr GetWalRcvWriteRecPtr(XLogRecPtr *latestChunkStart, TimeLineID *receiveTLI);
extern int	GetReplicationApplyDelay(void);
extern int	GetReplicationTransferLatency(void);
extern void WalRcvForceReply(void);

extern int	max_parallel_write_thread;
extern int	max_parallel_write_pipe_len;
extern int	wal_receiver_reply_threshold;
extern bool EnableParallelWalreceiver;

/* POLAR */
extern XLogRecPtr polar_dma_get_received_lsn(void);

/* POLAR end */

#endif							/* _WALRECEIVER_H */
