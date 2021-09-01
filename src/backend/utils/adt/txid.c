/*-------------------------------------------------------------------------
 * txid.c
 *
 *	Export internal transaction IDs to user level.
 *
 * Note that only top-level transaction IDs are ever converted to TXID.
 * This is important because TXIDs frequently persist beyond the global
 * xmin horizon, or may even be shipped to other machines, so we cannot
 * rely on being able to correlate subtransaction IDs with their parents
 * via functions such as SubTransGetTopmostTransaction().
 *
 *
 *	Copyright (c) 2003-2018, PostgreSQL Global Development Group
 *	Author: Jan Wieck, Afilias USA INC.
 *	64-bit txids: Marko Kreen, Skype Technologies
 *
 *	src/backend/utils/adt/txid.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/clog.h"
#include "access/mvccvars.h"
#include "access/transam.h"
#include "access/xact.h"
#include "access/xlog.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "libpq/pqformat.h"
#include "postmaster/postmaster.h"
#include "storage/lwlock.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/snapmgr.h"


/* txid will be signed int8 in database, so must limit to 63 bits */
#define MAX_TXID   ((uint64) PG_INT64_MAX)

/* Use unsigned variant internally */
typedef uint64 txid;

/* sprintf format code for uint64 */
#define TXID_FMT UINT64_FORMAT

/*
 * If defined, use bsearch() function for searching for txids in snapshots
 * that have more than the specified number of values.
 */
#define USE_BSEARCH_IF_NXIP_GREATER 30


/*
 * Snapshot containing 8byte txids.
 *
 * FIXME: this could be a fixed-length datatype now.
 */
typedef struct
{
	/*
	 * 4-byte length hdr, should not be touched directly.
	 *
	 * Explicit embedding is ok as we want always correct alignment anyway.
	 */
	int32		__varsz;

	txid		xmax;
	/*
	 * FIXME: this is change in on-disk format if someone created a column
	 * with txid datatype. Dump+reload won't load either.
	 */
	CommitSeqNo	snapshotcsn;
} TxidSnapshot;

#define TXID_SNAPSHOT_SIZE \
	(offsetof(TxidSnapshot, snapshotcsn) + sizeof(CommitSeqNo))

/*
 * Epoch values from xact.c
 */
typedef struct
{
	TransactionId last_xid;
	uint32		epoch;
} TxidEpoch;


/*
 * Fetch epoch data from xact.c.
 */
static void
load_xid_epoch(TxidEpoch *state)
{
	GetNextXidAndEpoch(&state->last_xid, &state->epoch);
}

/*
 * Helper to get a TransactionId from a 64-bit xid with wraparound detection.
 *
 * It is an ERROR if the xid is in the future.  Otherwise, returns true if
 * the transaction is still new enough that we can determine whether it
 * committed and false otherwise.  If *extracted_xid is not NULL, it is set
 * to the low 32 bits of the transaction ID (i.e. the actual XID, without the
 * epoch).
 *
 * The caller must hold CLogTruncationLock since it's dealing with arbitrary
 * XIDs, and must continue to hold it until it's done with any clog lookups
 * relating to those XIDs.
 */
static bool
TransactionIdInRecentPast(uint64 xid_with_epoch, TransactionId *extracted_xid)
{
	uint32		xid_epoch = (uint32) (xid_with_epoch >> 32);
	TransactionId xid = (TransactionId) xid_with_epoch;
	uint32		now_epoch;
	TransactionId now_epoch_last_xid;

	GetNextXidAndEpoch(&now_epoch_last_xid, &now_epoch);

	if (extracted_xid != NULL)
		*extracted_xid = xid;

	if (!TransactionIdIsValid(xid))
		return false;

	/* For non-normal transaction IDs, we can ignore the epoch. */
	if (!TransactionIdIsNormal(xid))
		return true;

	/* If the transaction ID is in the future, throw an error. */
	if (xid_epoch > now_epoch
		|| (xid_epoch == now_epoch && xid > now_epoch_last_xid))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("transaction ID %s is in the future",
						psprintf(UINT64_FORMAT, xid_with_epoch))));

	/*
	 * ShmemVariableCache->oldestClogXid is protected by CLogTruncationLock,
	 * but we don't acquire that lock here.  Instead, we require the caller to
	 * acquire it, because the caller is presumably going to look up the
	 * returned XID.  If we took and released the lock within this function, a
	 * CLOG truncation could occur before the caller finished with the XID.
	 */
	Assert(LWLockHeldByMe(CLogTruncationLock));

	/*
	 * If the transaction ID has wrapped around, it's definitely too old to
	 * determine the commit status.  Otherwise, we can compare it to
	 * ShmemVariableCache->oldestClogXid to determine whether the relevant
	 * CLOG entry is guaranteed to still exist.
	 */
	if (xid_epoch + 1 < now_epoch
		|| (xid_epoch + 1 == now_epoch && xid < now_epoch_last_xid)
		|| TransactionIdPrecedes(xid, ShmemVariableCache->oldestClogXid))
		return false;

	return true;
}

/*
 * do a TransactionId -> txid conversion for an XID near the given epoch
 */
static txid
convert_xid(TransactionId xid, const TxidEpoch *state)
{
	uint64		epoch;

	/* return special xid's as-is */
	if (!TransactionIdIsNormal(xid))
		return (txid) xid;

	/* xid can be on either side when near wrap-around */
	epoch = (uint64) state->epoch;
	if (xid > state->last_xid &&
		TransactionIdPrecedes(xid, state->last_xid))
		epoch--;
	else if (xid < state->last_xid &&
			 TransactionIdFollows(xid, state->last_xid))
		epoch++;

	return (epoch << 32) | xid;
}

/*
 * check txid visibility.
 */
static bool
is_visible_txid(txid value, const TxidSnapshot *snap)
{
#ifdef BROKEN
	if (value < snap->xmin)
		return true;
	else if (value >= snap->xmax)
		return false;
#ifdef USE_BSEARCH_IF_NXIP_GREATER
	else if (snap->nxip > USE_BSEARCH_IF_NXIP_GREATER)
	{
		void	   *res;

		res = bsearch(&value, snap->xip, snap->nxip, sizeof(txid), cmp_txid);
		/* if found, transaction is still in progress */
		return (res) ? false : true;
	}
#endif
	else
	{
		uint32		i;

		for (i = 0; i < snap->nxip; i++)
		{
			if (value == snap->xip[i])
				return false;
		}
		return true;
	}
#endif
	return false;
}

/*
 * simple number parser.
 *
 * We return 0 on error, which is invalid value for txid.
 */
static txid
str2txid(const char *s, const char **endp)
{
	txid		val = 0;
	txid		cutoff = MAX_TXID / 10;
	txid		cutlim = MAX_TXID % 10;

	for (; *s; s++)
	{
		unsigned	d;

		if (*s < '0' || *s > '9')
			break;
		d = *s - '0';

		/*
		 * check for overflow
		 */
		if (val > cutoff || (val == cutoff && d > cutlim))
		{
			val = 0;
			break;
		}

		val = val * 10 + d;
	}
	if (endp)
		*endp = s;
	return val;
}

/*
 * parse snapshot from cstring
 */
static TxidSnapshot *
parse_snapshot(const char *str)
{
	const char *str_start = str;
	const char *endp;
	TxidSnapshot *snap;
	uint32		csn_hi,
				csn_lo;

	snap = palloc0(TXID_SNAPSHOT_SIZE);
	SET_VARSIZE(snap, TXID_SNAPSHOT_SIZE);

	snap->xmax = str2txid(str, &endp);
	if (*endp != ':')
		goto bad_format;
	str = endp + 1;

	/* it should look sane */
	if (snap->xmax == 0)
		goto bad_format;

	if (sscanf(str, "%X/%X", &csn_hi, &csn_lo) != 2)
		goto bad_format;
	snap->snapshotcsn = ((uint64) csn_hi) << 32 | csn_lo;

	return snap;

bad_format:
	ereport(ERROR,
			(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
			 errmsg("invalid input syntax for type %s: \"%s\"",
					"txid_snapshot", str_start)));
	return NULL;				/* keep compiler quiet */
}

/*
 * Public functions.
 *
 * txid_current() and txid_current_snapshot() are the only ones that
 * communicate with core xid machinery.  All the others work on data
 * returned by them.
 */

/*
 * txid_current() returns int8
 *
 *	Return the current toplevel transaction ID as TXID
 *	If the current transaction does not have one, one is assigned.
 *
 *	This value has the epoch as the high 32 bits and the 32-bit xid
 *	as the low 32 bits.
 */
Datum
txid_current(PG_FUNCTION_ARGS)
{
	txid		val;
	TxidEpoch	state;

	/*
	 * Must prevent during recovery because if an xid is not assigned we try
	 * to assign one, which would fail. Programs already rely on this function
	 * to always return a valid current xid, so we should not change this to
	 * return NULL or similar invalid xid.
	 */
	PreventCommandDuringRecovery("txid_current()");

	load_xid_epoch(&state);

	val = convert_xid(GetTopTransactionId(), &state);

	PG_RETURN_INT64(val);
}

/*
 * Same as txid_current() but doesn't assign a new xid if there isn't one
 * yet.
 */
Datum
txid_current_if_assigned(PG_FUNCTION_ARGS)
{
	txid		val;
	TxidEpoch	state;
	TransactionId topxid = GetTopTransactionIdIfAny();

	if (topxid == InvalidTransactionId)
		PG_RETURN_NULL();

	load_xid_epoch(&state);

	val = convert_xid(topxid, &state);

	PG_RETURN_INT64(val);
}

/*
 * txid_current_snapshot() returns txid_snapshot
 *
 *		Return current snapshot in TXID format
 *
 * Note that only top-transaction XIDs are included in the snapshot.
 */
Datum
txid_current_snapshot(PG_FUNCTION_ARGS)
{
	TxidSnapshot *snap;
	TxidEpoch	state;
	Snapshot	cur;

	cur = GetActiveSnapshot();
	if (cur == NULL)
		elog(ERROR, "no active snapshot set");

	load_xid_epoch(&state);

	/* allocate */
	snap = palloc(TXID_SNAPSHOT_SIZE);
	SET_VARSIZE(snap, TXID_SNAPSHOT_SIZE);

	/* fill */
	snap->xmax = convert_xid(cur->xmax, &state);
	snap->snapshotcsn = cur->snapshotcsn;

	PG_RETURN_POINTER(snap);
}

/*
 * txid_snapshot_in(cstring) returns txid_snapshot
 *
 *		input function for type txid_snapshot
 */
Datum
txid_snapshot_in(PG_FUNCTION_ARGS)
{
	char	   *str = PG_GETARG_CSTRING(0);
	TxidSnapshot *snap;

	snap = parse_snapshot(str);

	PG_RETURN_POINTER(snap);
}

/*
 * txid_snapshot_out(txid_snapshot) returns cstring
 *
 *		output function for type txid_snapshot
 */
Datum
txid_snapshot_out(PG_FUNCTION_ARGS)
{
	TxidSnapshot *snap = (TxidSnapshot *) PG_GETARG_VARLENA_P(0);
	StringInfoData str;

	initStringInfo(&str);

	appendStringInfo(&str, TXID_FMT ":", snap->xmax);
	appendStringInfo(&str, "%X/%X", (uint32) (snap->snapshotcsn >> 32),
					 (uint32) snap->snapshotcsn);

	PG_RETURN_CSTRING(str.data);
}

/*
 * txid_snapshot_recv(internal) returns txid_snapshot
 *
 *		binary input function for type txid_snapshot
 *
 *		format: int4 nxip, int8 xmin, int8 xmax, int8 xip
 */
Datum
txid_snapshot_recv(PG_FUNCTION_ARGS)
{
#ifdef BROKEN
	StringInfo	buf = (StringInfo) PG_GETARG_POINTER(0);
	TxidSnapshot *snap;
	txid		last = 0;
	int			nxip;
	int			i;
	txid		xmin,
				xmax;

	xmin = pq_getmsgint64(buf);
	xmax = pq_getmsgint64(buf);
	if (xmin == 0 || xmax == 0 || xmin > xmax || xmax > MAX_TXID)
		goto bad_format;

	snap = palloc(TXID_SNAPSHOT_SIZE(nxip));
	snap->xmin = xmin;
	snap->xmax = xmax;

	for (i = 0; i < nxip; i++)
	{
		txid		cur = pq_getmsgint64(buf);

		if (cur < last || cur < xmin || cur >= xmax)
			goto bad_format;

		/* skip duplicate xips */
		if (cur == last)
		{
			i--;
			nxip--;
			continue;
		}

		snap->xip[i] = cur;
		last = cur;
	}
	snap->nxip = nxip;
	SET_VARSIZE(snap, TXID_SNAPSHOT_SIZE(nxip));
	PG_RETURN_POINTER(snap);

bad_format:
#endif
	ereport(ERROR,
			(errcode(ERRCODE_INVALID_BINARY_REPRESENTATION),
			 errmsg("invalid external txid_snapshot data")));
	PG_RETURN_POINTER(NULL);	/* keep compiler quiet */
}

/*
 * txid_snapshot_send(txid_snapshot) returns bytea
 *
 *		binary output function for type txid_snapshot
 *
 *		format: int4 nxip, int8 xmin, int8 xmax, int8 xip
 */
Datum
txid_snapshot_send(PG_FUNCTION_ARGS)
{
	TxidSnapshot *snap = (TxidSnapshot *) PG_GETARG_VARLENA_P(0);
	StringInfoData buf;

	pq_begintypsend(&buf);
#ifdef BROKEN
	pq_sendint64(&buf, snap->xmin);
	pq_sendint64(&buf, snap->xmax);
#endif
	pq_sendint64(&buf, snap->snapshotcsn);
	PG_RETURN_BYTEA_P(pq_endtypsend(&buf));
}

/*
 * txid_visible_in_snapshot(int8, txid_snapshot) returns bool
 *
 *		is txid visible in snapshot ?
 */
Datum
txid_visible_in_snapshot(PG_FUNCTION_ARGS)
{
	txid		value = PG_GETARG_INT64(0);
	TxidSnapshot *snap = (TxidSnapshot *) PG_GETARG_VARLENA_P(1);

	PG_RETURN_BOOL(is_visible_txid(value, snap));
}

/*
 * txid_snapshot_xmin(txid_snapshot) returns int8
 *
 *             return snapshot's xmin
 */
Datum
txid_snapshot_xmin(PG_FUNCTION_ARGS)
{
	/* FIXME: we don't store xmin in the TxidSnapshot anymore. Maybe we still should? */
#ifdef BROKEN
	TxidSnapshot *snap = (TxidSnapshot *) PG_GETARG_VARLENA_P(0);

	PG_RETURN_INT64(snap->xmin);
#endif
	PG_RETURN_INT64(0);
}

/*
 * txid_snapshot_xmax(txid_snapshot) returns int8
 *
 *		return snapshot's xmax
 */
Datum
txid_snapshot_xmax(PG_FUNCTION_ARGS)
{
	TxidSnapshot *snap = (TxidSnapshot *) PG_GETARG_VARLENA_P(0);

	PG_RETURN_INT64(snap->xmax);
}
/*
 * Report the status of a recent transaction ID, or null for wrapped,
 * truncated away or otherwise too old XIDs.
 *
 * The passed epoch-qualified xid is treated as a normal xid, not a
 * multixact id.
 *
 * If it points to a committed subxact the result is the subxact status even
 * though the parent xact may still be in progress or may have aborted.
 */
Datum
txid_status(PG_FUNCTION_ARGS)
{
	const char *status;
	uint64		xid_with_epoch = PG_GETARG_INT64(0);
	TransactionId xid;

	/*
	 * We must protect against concurrent truncation of clog entries to avoid
	 * an I/O error on SLRU lookup.
	 */
	LWLockAcquire(CLogTruncationLock, LW_SHARED);
	if (TransactionIdInRecentPast(xid_with_epoch, &xid))
	{
		Assert(TransactionIdIsValid(xid));

		if (TransactionIdIsCurrentTransactionId(xid))
			status = "in progress";
		else if (TransactionIdDidCommit(xid))
			status = "committed";
		else if (TransactionIdDidAbort(xid))
			status = "aborted";
		else
		{
			/*
			 * The xact is not marked as either committed or aborted in clog.
			 *
			 * It could be a transaction that ended without updating clog or
			 * writing an abort record due to a crash. We can safely assume
			 * it's aborted if it isn't committed and is older than our
			 * snapshot xmin.
			 *
			 * Otherwise it must be in-progress (or have been at the time we
			 * checked commit/abort status).
			 */
			if (TransactionIdPrecedes(xid, GetActiveSnapshot()->xmin))
				status = "aborted";
			else
				status = "in progress";
		}
	}
	else
	{
		status = NULL;
	}
	LWLockRelease(CLogTruncationLock);

	if (status == NULL)
		PG_RETURN_NULL();
	else
		PG_RETURN_TEXT_P(cstring_to_text(status));
}
