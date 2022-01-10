/*-------------------------------------------------------------------------
 *
 * dest.c
 *	  support for communication destinations
 *
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/tcop/dest.c
 *
 *-------------------------------------------------------------------------
 */
/*
 *	 INTERFACE ROUTINES
 *		BeginCommand - initialize the destination at start of command
 *		CreateDestReceiver - create tuple receiver object for destination
 *		EndCommand - clean up the destination at end of command
 *		NullCommand - tell dest that an empty query string was recognized
 *		ReadyForQuery - tell dest that we are ready for a new query
 *
 *	 NOTES
 *		These routines do the appropriate work before and after
 *		tuples are returned by a query to keep the backend and the
 *		"destination" portals synchronized.
 */

#include "postgres.h"

#include "access/printsimple.h"
#include "access/printtup.h"
#include "access/xact.h"
#include "commands/copy.h"
#include "commands/createas.h"
#include "commands/matview.h"
#include "executor/functions.h"
#include "executor/tqueue.h"
#include "executor/tstoreReceiver.h"
#include "libpq/libpq.h"
#include "libpq/pqformat.h"
#include "utils/portal.h"

/* POLAR */
#include "access/xact.h"
#include "access/xlog.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "storage/procarray.h"
#include "utils/guc.h"
/* POLAR end */

/* POLAR px */
#include "utils/snapmgr.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "px/px_vars.h"
#include "px/px_gang.h"
#include "px/px_snapshot.h"
/* POLAR end */

/* POLAR */
extern void px_check_qc_connection_alive(void);
#define SPIN_SLEEP_MSEC		10	/* 10ms */

/* ----------------
 *		dummy DestReceiver functions
 * ----------------
 */
static bool
donothingReceive(TupleTableSlot *slot, DestReceiver *self)
{
	return true;
}

static void
donothingStartup(DestReceiver *self, int operation, TupleDesc typeinfo)
{
}

static void
donothingCleanup(DestReceiver *self)
{
	/* this is used for both shutdown and destroy methods */
}

/* POLAR: send proxy info, including lsn and xact split info, also collects stats */
static void
polar_send_proxy_info(StringInfo buf)
{
	/* POLAR: send lsn to maxscale if needed */
	if (MyProcPort->polar_send_lsn)
	{
		if (RecoveryInProgress())
			pq_sendint64(buf, (uint64)GetXLogReplayRecPtr(NULL));
		else
			pq_sendint64(buf, (uint64)GetXLogInsertRecPtr());
	}

	if (unlikely(polar_enable_xact_split_debug) && !RecoveryInProgress())
	{
		char *xids = polar_xact_split_xact_info();

		elog(LOG, "current xids : %s", xids);
		elog(LOG, "current xact able to split: %s",
					polar_xact_split_splittable() ? "Yes" : "No");

		if (xids)
			pfree(xids);
	}

	polar_stat_update_proxy_info(polar_stat_proxy->proxy_total);

	/* POLAR: send xact split info to mxs if needed */
	if (polar_enable_xact_split &&
		XactIsoLevel == XACT_READ_COMMITTED &&
		MyProcPort->polar_send_xact &&
		!RecoveryInProgress())
	{
		if (polar_xact_split_splittable())
		{
			char *xids = polar_xact_split_xact_info();

			if (xids != NULL)
			{
				/* POLAR: send xids info */
				pq_sendbyte(buf, 'x');
				pq_sendstring(buf, xids);
				pfree(xids);
			}
			polar_stat_update_proxy_info(polar_stat_proxy->proxy_splittable);
		}
		else
			polar_stat_update_proxy_info(polar_stat_proxy->proxy_unsplittable);

		switch (polar_unable_to_split_reason)
		{
		case POLAR_UNSPLITTABLE_FOR_ERROR:
			polar_stat_update_proxy_info(polar_stat_proxy->proxy_error);
			break;
		case POLAR_UNSPLITTABLE_FOR_LOCK:
			polar_stat_update_proxy_info(polar_stat_proxy->proxy_lock);
			break;
		case POLAR_UNSPLITTABLE_FOR_COMBOCID:
			polar_stat_update_proxy_info(polar_stat_proxy->proxy_combocid);
			break;
		case POLAR_UNSPLITTABLE_FOR_AUTOXACT:
			polar_stat_update_proxy_info(polar_stat_proxy->proxy_autoxact);
			break;
		default:
			break;
		}
	}
	else
		polar_stat_update_proxy_info(polar_stat_proxy->proxy_disablesplit);

	if (RecoveryInProgress() && MyProcPort->polar_proxy)
		polar_stat_update_ro_proxy_info();
	polar_stat_need_update_proxy_info = false;
}

/* ----------------
 *		static DestReceiver structs for dest types needing no local state
 * ----------------
 */
static DestReceiver donothingDR = {
	donothingReceive, donothingStartup, donothingCleanup, donothingCleanup,
	DestNone
};

static DestReceiver debugtupDR = {
	debugtup, debugStartup, donothingCleanup, donothingCleanup,
	DestDebug
};

static DestReceiver printsimpleDR = {
	printsimple, printsimple_startup, donothingCleanup, donothingCleanup,
	DestRemoteSimple
};

static DestReceiver spi_printtupDR = {
	spi_printtup, spi_dest_startup, donothingCleanup, donothingCleanup,
	DestSPI
};

/* Globally available receiver for DestNone */
DestReceiver *None_Receiver = &donothingDR;


/* ----------------
 *		BeginCommand - initialize the destination at start of command
 * ----------------
 */
void
BeginCommand(const char *commandTag, CommandDest dest)
{
	/* Nothing to do at present */
}

/* ----------------
 *		CreateDestReceiver - return appropriate receiver function set for dest
 * ----------------
 */
DestReceiver *
CreateDestReceiver(CommandDest dest)
{
	switch (dest)
	{
		case DestRemote:
		case DestRemoteExecute:
			return printtup_create_DR(dest);

		case DestRemoteSimple:
			return &printsimpleDR;

		case DestNone:
			return &donothingDR;

		case DestDebug:
			return &debugtupDR;

		case DestSPI:
			return &spi_printtupDR;

		case DestTuplestore:
			return CreateTuplestoreDestReceiver();

		case DestIntoRel:
			return CreateIntoRelDestReceiver(NULL);

		case DestCopyOut:
			return CreateCopyDestReceiver();

		case DestSQLFunction:
			return CreateSQLFunctionDestReceiver();

		case DestTransientRel:
			return CreateTransientRelDestReceiver(InvalidOid);

		case DestTupleQueue:
			return CreateTupleQueueDestReceiver(NULL);
	}

	/* should never get here */
	return &donothingDR;
}

/* ----------------
 *		EndCommand - clean up the destination at end of command
 * ----------------
 */
void
EndCommand(const char *commandTag, CommandDest dest)
{
	switch (dest)
	{
		case DestRemote:
		case DestRemoteExecute:
		case DestRemoteSimple:

			/*
			 * We assume the commandTag is plain ASCII and therefore requires
			 * no encoding conversion.
			 */
			pq_putmessage('C', commandTag, strlen(commandTag) + 1);
			break;

		case DestNone:
		case DestDebug:
		case DestSPI:
		case DestTuplestore:
		case DestIntoRel:
		case DestCopyOut:
		case DestSQLFunction:
		case DestTransientRel:
		case DestTupleQueue:
			break;
	}
}

/* ----------------
 *		NullCommand - tell dest that an empty query string was recognized
 *
 *		In FE/BE protocol version 1.0, this hack is necessary to support
 *		libpq's crufty way of determining whether a multiple-command
 *		query string is done.  In protocol 2.0 it's probably not really
 *		necessary to distinguish empty queries anymore, but we still do it
 *		for backwards compatibility with 1.0.  In protocol 3.0 it has some
 *		use again, since it ensures that there will be a recognizable end
 *		to the response to an Execute message.
 * ----------------
 */
void
NullCommand(CommandDest dest)
{
	switch (dest)
	{
		case DestRemote:
		case DestRemoteExecute:
		case DestRemoteSimple:

			/*
			 * tell the fe that we saw an empty query string.  In protocols
			 * before 3.0 this has a useless empty-string message body.
			 */
			if (PG_PROTOCOL_MAJOR(FrontendProtocol) >= 3)
				pq_putemptymessage('I');
			else
				pq_putmessage('I', "", 1);
			break;

		case DestNone:
		case DestDebug:
		case DestSPI:
		case DestTuplestore:
		case DestIntoRel:
		case DestCopyOut:
		case DestSQLFunction:
		case DestTransientRel:
		case DestTupleQueue:
			break;
	}
}

/* ----------------
 *		ReadyForQuery - tell dest that we are ready for a new query
 *
 *		The ReadyForQuery message is sent so that the FE can tell when
 *		we are done processing a query string.
 *		In versions 3.0 and up, it also carries a transaction state indicator.
 *
 *		Note that by flushing the stdio buffer here, we can avoid doing it
 *		most other places and thus reduce the number of separate packets sent.
 * ----------------
 */
void
ReadyForQuery(CommandDest dest)
{
	switch (dest)
	{
		case DestRemote:
		case DestRemoteExecute:
		case DestRemoteSimple:
			if (PG_PROTOCOL_MAJOR(FrontendProtocol) >= 3)
			{
				StringInfoData buf;

				pq_beginmessage(&buf, 'Z');
				pq_sendbyte(&buf, TransactionBlockStatusCode());
				polar_send_proxy_info(&buf);
				pq_endmessage(&buf);
			}
			else
				pq_putemptymessage('Z');
			/* Flush output at end of cycle in any case. */
			pq_flush();
			break;

		case DestNone:
		case DestDebug:
		case DestSPI:
		case DestTuplestore:
		case DestIntoRel:
		case DestCopyOut:
		case DestSQLFunction:
		case DestTransientRel:
		case DestTupleQueue:
			break;
	}
}

/* POLAR px */
/*
 * Send a px libpq message.
 *
 * This sends a message identical to that used when sending values of
 * GUC_REPORT gucs to the client (see ReportGUCOption()). The motion
 * listener port is sent as if there was a GUC called "px_listener_port".
 */
void
sendPXDetails(void)
{
	StringInfoData msgbuf;
	char		port_str[11];
	Snapshot	xact_snap;
	Snapshot	snap;
	char	   *serialized_snap;
	int			serialized_snap_size;
	char	   *encoded_snap;
	int 		retry_count = 0;

	/* POLAR */
	XLogRecPtr max_valid_lsn;

	snprintf(port_str, sizeof(port_str), "%u", px_listener_port);

	/*
	 * POLAR: if px_enable_replay_wait is on, all the mpp queries in PXs should
	 * wait for the wal replayed the QC query begin.
	 */
	if (XLogRecPtrIsInvalid(px_sql_wal_lsn) && polar_in_replica_mode())
		elog(ERROR, "polardb px enabled, but current sql wal lsn is invalid");

	while (cached_px_enable_replay_wait && !polar_is_master())
	{
		max_valid_lsn = GetXLogReplayRecPtr(NULL);
		if (max_valid_lsn >= px_sql_wal_lsn)
		{
			elog((retry_count > 10000 ? LOG : DEBUG1), "before exec px query on node: px sessid %d, max valid lsn %lX, current sql wal lsn %lX, use time %dus", 
				px_session_id, max_valid_lsn, 
				px_sql_wal_lsn, retry_count * 100);
			break;
		}

		pg_usleep(SPIN_SLEEP_MSEC * 10);
		retry_count++;
		px_check_qc_connection_alive();

		/* large then 1s, print trace log every 1s */
		if (retry_count > 10000 && retry_count % 10000 == 1)
			elog(LOG, "before exec px query on node: px sessid %d, max valid lsn %lX, current sql wal lsn %lX, try time %dus", 
				px_session_id, max_valid_lsn, px_sql_wal_lsn, retry_count * 100);
	}

	xact_snap = GetTransactionSnapshot();
	Assert(xact_snap);

	snap = palloc(sizeof(SnapshotData));
	memcpy(snap, xact_snap, sizeof(SnapshotData));
	pxsn_set_snapshot(snap);
	serialized_snap = pxsn_get_serialized_snapshot();
	serialized_snap_size = pxsn_get_serialized_snapshot_size();

	encoded_snap = (char *) palloc(serialized_snap_size * 2 + 1);
	hex_encode(serialized_snap, serialized_snap_size, encoded_snap);
	encoded_snap[serialized_snap_size * 2] = '\0';

	if (px_info_debug)
		elog(LOG, "px_snapshot_encode: %d sendPXDetails: encoded_snap: %s", 
			PxIdentity.dbid, encoded_snap);

	pq_beginmessage(&msgbuf, 'S');
	pq_sendstring(&msgbuf, "px_listener_port");
	pq_sendstring(&msgbuf, port_str);
	pq_endmessage(&msgbuf);

	pq_beginmessage(&msgbuf, 'S');
	pq_sendstring(&msgbuf, "snapshot");
	pq_sendstring(&msgbuf, encoded_snap);
	pq_endmessage(&msgbuf);

	pxsn_set_snapshot(InvalidSnapshot);
	pfree(encoded_snap);
}
