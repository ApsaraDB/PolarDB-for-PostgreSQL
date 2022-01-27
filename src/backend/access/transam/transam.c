/*-------------------------------------------------------------------------
 *
 * transam.c
 *	  postgres transaction (commit) log interface routines
 *
 * Support CTS-based transactions.
 * Author: Junbin Kang
 *
 * Portions Copyright (c) 2020, Alibaba Group Holding Limited
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/access/transam/transam.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/clog.h"
#include "access/ctslog.h"
#include "access/mvccvars.h"
#include "access/subtrans.h"
#include "access/transam.h"
#include "distributed_txn/txn_timestamp.h"
#include "storage/lmgr.h"
#include "utils/snapmgr.h"
#ifdef POLARDB_X
#include "access/twophase.h"
#include "utils/builtins.h"
#include "pgxc/transam/txn_coordinator.h"
#endif

/*
 * Single-item cache for results of TransactionIdGetCommitSeqNo.  It's worth
 * having
 * such a cache because we frequently find ourselves repeatedly checking the
 * same XID, for example when scanning a table just after a bulk insert,
 * update, or delete.
 */
static TransactionId cachedFetchXid = InvalidTransactionId;
static CommitSeqNo cachedCSN;

/*
 * Also have a (separate) cache for CLogGetCommitLSN()
 */
static TransactionId cachedLSNFetchXid = InvalidTransactionId;
static XLogRecPtr cachedCommitLSN;

#ifdef POLARDB_X
PG_FUNCTION_INFO_V1(polardbx_get_transaction_status);
#endif

/*
 * TransactionIdGetCommitSeqNo --- fetch CSN of specified transaction id
 */
CommitSeqNo
TransactionIdGetCommitSeqNo(TransactionId transactionId)
{
	CommitSeqNo csn;

	/*
	 * Before going to the commit log manager, check our single item cache to
	 * see if we didn't just check the transaction status a moment ago.
	 */
	if (TransactionIdEquals(transactionId, cachedFetchXid))
		return cachedCSN;

	/*
	 * Also, check to see if the transaction ID is a permanent one.
	 */
	if (!TransactionIdIsNormal(transactionId))
	{
		if (TransactionIdEquals(transactionId, BootstrapTransactionId))
			return COMMITSEQNO_FROZEN;
		if (TransactionIdEquals(transactionId, FrozenTransactionId))
			return COMMITSEQNO_FROZEN;
		return COMMITSEQNO_ABORTED;
	}

	/*
	 * If the XID is older than TransactionXmin, check the clog. Otherwise
	 * check the csnlog.
	 */
#ifndef ENABLE_DISTRIBUTED_TRANSACTION
	Assert(TransactionIdIsValid(TransactionXmin));
	if (TransactionIdPrecedes(transactionId, TransactionXmin))
	{
		XLogRecPtr	lsn;

		if (CLogGetStatus(transactionId, &lsn) == CLOG_XID_STATUS_COMMITTED)
			csn = COMMITSEQNO_FROZEN;
		else
			csn = COMMITSEQNO_ABORTED;
	}
	else
#endif
	{
		csn = CTSLogGetCommitTs(transactionId);

		if (csn == COMMITSEQNO_COMMITTING)
		{
			/*
			 * If the transaction is committing at this very instant, and
			 * hasn't set its CSN yet, wait for it to finish doing so.
			 *
			 * XXX: Alternatively, we could wait on the heavy-weight lock on
			 * the XID. that'd make TransactionIdCommitTree() slightly
			 * cheaper, as it wouldn't need to acquire CommitSeqNoLock (even
			 * in shared mode).
			 */

#ifdef ENABLE_DISTRIBUTED_TRANSACTION
			/*
			 * We choose to wait for the specific transaction to run to
			 * completion, so as the transaction commit path does not need to
			 * acquire CommitSeqNoLock.
			 */
#ifdef ENABLE_DISTR_DEBUG
			if (enable_timestamp_debug_print)
				elog(LOG, "wait for committing transaction xid %d to complete", transactionId);
#endif
			XactLockTableWait(transactionId, NULL, NULL, XLTW_None);
#else
			LWLockAcquire(CommitSeqNoLock, LW_EXCLUSIVE);
			LWLockRelease(CommitSeqNoLock);
#endif

			csn = CTSLogGetCommitTs(transactionId);
			Assert(csn != COMMITSEQNO_COMMITTING);
		}
	}

	/*
	 * Cache it, but DO NOT cache status for unfinished transactions! We only
	 * cache status that is guaranteed not to change.
	 */
	if (COMMITSEQNO_IS_COMMITTED(csn) ||
		COMMITSEQNO_IS_ABORTED(csn))
	{
		cachedFetchXid = transactionId;
		cachedCSN = csn;
	}

	return csn;
}

/*
 * TransactionIdDidCommit
 *		True iff transaction associated with the identifier did commit.
 *
 * Note:
 *		Assumes transaction identifier is valid and exists in clog.
 */
bool							/* true if given transaction committed */
TransactionIdDidCommit(TransactionId transactionId)
{
	CommitSeqNo csn;

	csn = TransactionIdGetCommitSeqNo(transactionId);

#ifdef ENABLE_DISTR_DEBUG
	/* perform check against clog for debugging purpose */
	{
		if (COMMITSEQNO_IS_COMMITTED(csn))
		{
			XLogRecPtr	result;
			CLogXidStatus clogstatus;

			clogstatus = CLogGetStatus(transactionId, &result);

			if (clogstatus != CLOG_XID_STATUS_COMMITTED)
				elog(PANIC, "cts log status " UINT64_FORMAT " does not match clog %d",
					 csn, clogstatus);
		}
		else if (COMMITSEQNO_IS_ABORTED(csn))
		{
			XLogRecPtr	result;
			CLogXidStatus clogstatus;

			clogstatus = CLogGetStatus(transactionId, &result);

			if (clogstatus != CLOG_XID_STATUS_ABORTED)
				elog(PANIC, "cts log status " UINT64_FORMAT " does not match clog %d",
					 csn, clogstatus);
		}
	}
#endif

	if (COMMITSEQNO_IS_COMMITTED(csn))
		return true;
	else
		return false;
}

/*
 * TransactionIdDidAbort
 *		True iff transaction associated with the identifier did abort.
 *
 * Note:
 *		Assumes transaction identifier is valid and exists in clog.
 */
bool							/* true if given transaction aborted */
TransactionIdDidAbort(TransactionId transactionId)
{
	CommitSeqNo csn;

	csn = TransactionIdGetCommitSeqNo(transactionId);

#ifdef ENABLE_DISTR_DEBUG
	/* perform check against clog for debugging purpose */
	{
		if (COMMITSEQNO_IS_ABORTED(csn))
		{
			XLogRecPtr	result;
			CLogXidStatus clogstatus;

			clogstatus = CLogGetStatus(transactionId, &result);

			if (clogstatus != CLOG_XID_STATUS_ABORTED)
				elog(PANIC, "cts log status " UINT64_FORMAT " does not match clog %d",
					 csn, clogstatus);
		}
		else if (COMMITSEQNO_IS_COMMITTED(csn))
		{
			XLogRecPtr	result;
			CLogXidStatus clogstatus;

			clogstatus = CLogGetStatus(transactionId, &result);

			if (clogstatus != CLOG_XID_STATUS_COMMITTED)
				elog(PANIC, "cts log status " UINT64_FORMAT " does not match clog %d",
					 csn, clogstatus);
		}
	}
#endif

	if (COMMITSEQNO_IS_ABORTED(csn))
		return true;
	else
		return false;
}

/*
 * Returns the status of the tranaction.
 *
 * Note that this treats a a crashed transaction as still in-progress,
 * until it falls off the xmin horizon.
 */
TransactionIdStatus
TransactionIdGetStatus(TransactionId xid)
{
	CommitSeqNo csn;
	TransactionIdStatus status;

	csn = TransactionIdGetCommitSeqNo(xid);

	if (COMMITSEQNO_IS_COMMITTED(csn))
		status = XID_COMMITTED;
	else if (COMMITSEQNO_IS_ABORTED(csn))
		status = XID_ABORTED;
	else
		status = XID_INPROGRESS;

#ifdef ENABLE_DISTR_DEBUG
	/* perform check against clog for debugging purpose */
	{
		XLogRecPtr	result;
		CLogXidStatus clogstatus;

		clogstatus = CLogGetStatus(xid, &result);

		switch (status)
		{
			case XID_COMMITTED:
				if (clogstatus != CLOG_XID_STATUS_COMMITTED)
					elog(PANIC, "cts log status %d does not match clog %d",
						 status, clogstatus);
				break;
			case XID_ABORTED:
				if (clogstatus != CLOG_XID_STATUS_ABORTED)
					elog(PANIC, "cts log status %d does not match clog %d",
						 status, clogstatus);
				break;
		}
	}
#endif

	return status;
}

/*
 * TransactionIdCommitTree
 *		Marks the given transaction and children as committed
 *
 * "xid" is a toplevel transaction commit, and the xids array contains its
 * committed subtransactions.
 */
void
TransactionIdCommitTree(TransactionId xid, int nxids, TransactionId *xids)
{
	TransactionIdAsyncCommitTree(xid, nxids, xids, InvalidXLogRecPtr);
}

/*
 * TransactionIdAsyncCommitTree
 *		Same as above, but for async commits.
 *
 * "xid" is a toplevel transaction commit, and the xids array contains its
 * committed subtransactions.
 */
void
TransactionIdAsyncCommitTree(TransactionId xid, int nxids, TransactionId *xids,
							 XLogRecPtr lsn)
{
#ifndef ENABLE_DISTRIBUTED_TRANSACTION
	CommitSeqNo csn;
#endif
	TransactionId latestXid;
	TransactionId currentLatestCompletedXid;

	latestXid = TransactionIdLatest(xid, nxids, xids);

	/*
	 * First update the clog, then CSN log. oldestActiveXid advances based on
	 * CSN log content (see AdvanceOldestActiveXid), and it should not become
	 * greater than our xid before we set the clog status. Otherwise other
	 * transactions could see us as aborted for some time after we have
	 * written to CSN log, and somebody advanced the oldest active xid past
	 * our xid, but before we write to clog.
	 */
#ifdef ENABLE_DISTR_DEBUG
	CLogSetTreeStatus(xid, nxids, xids,
					  CLOG_XID_STATUS_COMMITTED,
					  lsn);
#endif

	/*
	 * Grab the CommitSeqNoLock, in shared mode. This is only used to provide
	 * a way for a concurrent transaction to wait for us to complete (see
	 * TransactionIdGetCommitSeqNo()).
	 *
	 * XXX: We could reduce the time the lock is held, by only setting the CSN
	 * on the top-XID while holding the lock, and updating the sub-XIDs later.
	 * But it doesn't matter much, because we're only holding it in shared
	 * mode, and it's rare for it to be acquired in exclusive mode.
	 */
	LWLockAcquire(CommitSeqNoLock, LW_SHARED);

	/*
	 * First update latestCompletedXid to cover this xid. We do this before
	 * assigning a CSN, so that if someone acquires a new snapshot at the same
	 * time, the xmax it computes is sure to cover our XID.
	 */
	currentLatestCompletedXid = pg_atomic_read_u32(&ShmemVariableCache->latestCompletedXid);
	while (TransactionIdFollows(latestXid, currentLatestCompletedXid))
	{
		if (pg_atomic_compare_exchange_u32(&ShmemVariableCache->latestCompletedXid,
										   &currentLatestCompletedXid,
										   latestXid))
			break;
	}
#ifndef ENABLE_DISTRIBUTED_TRANSACTION

	/*
	 * Mark our top transaction id as commit-in-progress.
	 */
	CSNLogSetCommitSeqNo(xid, 0, NULL, lsn, true, COMMITSEQNO_COMMITTING);

	/* Get our CSN and increment */
	csn = pg_atomic_fetch_add_u64(&ShmemVariableCache->nextCommitSeqNo, 1);
	Assert(csn >= COMMITSEQNO_FIRST_NORMAL);

	/* Stamp this XID (and sub-XIDs) with the CSN */
	CSNLogSetCommitSeqNo(xid, nxids, xids, lsn, true, csn);

#endif
	LWLockRelease(CommitSeqNoLock);
}

/*
 * TransactionIdAbortTree
 *		Marks the given transaction and children as aborted.
 *
 * "xid" is a toplevel transaction commit, and the xids array contains its
 * committed subtransactions.
 *
 * We don't need to worry about the non-atomic behavior, since any onlookers
 * will consider all the xacts as not-yet-committed anyway.
 */
void
TransactionIdAbortTree(TransactionId xid, int nxids, TransactionId *xids)
{
	TransactionId latestXid;
	TransactionId currentLatestCompletedXid;

	latestXid = TransactionIdLatest(xid, nxids, xids);

	currentLatestCompletedXid = pg_atomic_read_u32(&ShmemVariableCache->latestCompletedXid);
	while (TransactionIdFollows(latestXid, currentLatestCompletedXid))
	{
		if (pg_atomic_compare_exchange_u32(&ShmemVariableCache->latestCompletedXid,
										   &currentLatestCompletedXid,
										   latestXid))
			break;
	}
	if (enable_timestamp_debug_print)
		elog(LOG, "abort transaction xid %d", xid);

#ifdef ENABLE_DISTR_DEBUG
	CLogSetTreeStatus(xid, nxids, xids,
					  CLOG_XID_STATUS_ABORTED, InvalidXLogRecPtr);
#endif

	CTSLogSetCommitTs(xid, nxids, xids, InvalidXLogRecPtr, false, COMMITSEQNO_ABORTED);
}

/*
 * TransactionIdPrecedes --- is id1 logically < id2?
 */
bool
TransactionIdPrecedes(TransactionId id1, TransactionId id2)
{
	/*
	 * If either ID is a permanent XID then we can just do unsigned
	 * comparison.  If both are normal, do a modulo-2^32 comparison.
	 */
	int32		diff;

	if (!TransactionIdIsNormal(id1) || !TransactionIdIsNormal(id2))
		return (id1 < id2);

	diff = (int32) (id1 - id2);
	return (diff < 0);
}

/*
 * TransactionIdPrecedesOrEquals --- is id1 logically <= id2?
 */
bool
TransactionIdPrecedesOrEquals(TransactionId id1, TransactionId id2)
{
	int32		diff;

	if (!TransactionIdIsNormal(id1) || !TransactionIdIsNormal(id2))
		return (id1 <= id2);

	diff = (int32) (id1 - id2);
	return (diff <= 0);
}

/*
 * TransactionIdFollows --- is id1 logically > id2?
 */
bool
TransactionIdFollows(TransactionId id1, TransactionId id2)
{
	int32		diff;

	if (!TransactionIdIsNormal(id1) || !TransactionIdIsNormal(id2))
		return (id1 > id2);

	diff = (int32) (id1 - id2);
	return (diff > 0);
}

/*
 * TransactionIdFollowsOrEquals --- is id1 logically >= id2?
 */
bool
TransactionIdFollowsOrEquals(TransactionId id1, TransactionId id2)
{
	int32		diff;

	if (!TransactionIdIsNormal(id1) || !TransactionIdIsNormal(id2))
		return (id1 >= id2);

	diff = (int32) (id1 - id2);
	return (diff >= 0);
}


/*
 * TransactionIdLatest --- get latest XID among a main xact and its children
 */
TransactionId
TransactionIdLatest(TransactionId mainxid,
					int nxids, const TransactionId *xids)
{
	TransactionId result;

	/*
	 * In practice it is highly likely that the xids[] array is sorted, and so
	 * we could save some cycles by just taking the last child XID, but this
	 * probably isn't so performance-critical that it's worth depending on
	 * that assumption.  But just to show we're not totally stupid, scan the
	 * array back-to-front to avoid useless assignments.
	 */
	result = mainxid;
	while (--nxids >= 0)
	{
		if (TransactionIdPrecedes(result, xids[nxids]))
			result = xids[nxids];
	}
	return result;
}


/*
 * TransactionIdGetCommitLSN
 *
 * This function returns an LSN that is late enough to be able
 * to guarantee that if we flush up to the LSN returned then we
 * will have flushed the transaction's commit record to disk.
 *
 * The result is not necessarily the exact LSN of the transaction's
 * commit record!  For example, for long-past transactions (those whose
 * clog pages already migrated to disk), we'll return InvalidXLogRecPtr.
 * Also, because we group transactions on the same clog page to conserve
 * storage, we might return the LSN of a later transaction that falls into
 * the same group.
 */
XLogRecPtr
TransactionIdGetCommitLSN(TransactionId xid)
{
	XLogRecPtr	result;

	/*
	 * Currently, all uses of this function are for xids that were just
	 * reported to be committed by TransactionLogFetch, so we expect that
	 * checking TransactionLogFetch's cache will usually succeed and avoid an
	 * extra trip to shared memory.
	 */
	if (TransactionIdEquals(xid, cachedLSNFetchXid))
		return cachedCommitLSN;

	/* Special XIDs are always known committed */
	if (!TransactionIdIsNormal(xid))
		return InvalidXLogRecPtr;

	/*
	 * Get the transaction status.
	 */
#ifdef ENABLE_DISTR_DEBUG
	result = CLogGetLSN(xid);
#endif

	result = CTSLogGetLSN(xid);

	cachedLSNFetchXid = xid;
	cachedCommitLSN = result;

	return result;
}

#ifdef POLARDB_X
/*
 * For given gid, check if transaction is committed, aborted, inprogress, or failed to get localxid from 2pc file.
 * 1. get localxid by gid
 * 2. see localxid is committed?
 */
Datum
polardbx_get_transaction_status(PG_FUNCTION_ARGS)
{
	char *gid = text_to_cstring(PG_GETARG_TEXT_PP(0));
	TransactionId localxid = InvalidTransactionId;
	TransactionIdStatus   xidstatus = XID_INPROGRESS;

	localxid = GetTwoPhaseXactLocalxid(gid);

	if (InvalidTransactionId == localxid)
	{
		if (enable_twophase_recover_debug_print)
			elog(DEBUG_2PC, "gid:%s, no coresponding 2pc file found.", gid);
		PG_RETURN_INT32(POLARDBX_TRANSACTION_TWOPHASE_FILE_NOT_FOUND);
	}
	else
	{
		xidstatus = TransactionIdGetStatus(localxid);
	}

	if (xidstatus == XID_COMMITTED)
	{
		if (enable_twophase_recover_debug_print)
			elog(DEBUG_2PC, "polardbx_get_transaction_status: gid:%s is committed.", gid);
		PG_RETURN_INT32(POLARDBX_TRANSACTION_COMMITED);
	}
	else if (xidstatus == XID_ABORTED)
	{
		if (enable_twophase_recover_debug_print)
			elog(DEBUG_2PC, "polardbx_get_transaction_status: gid:%s is aborted.", gid);
		PG_RETURN_INT32(POLARDBX_TRANSACTION_ABORTED);
	}
	else if (xidstatus == XID_INPROGRESS)
	{
		if (enable_twophase_recover_debug_print)
			elog(DEBUG_2PC, "polardbx_get_transaction_status: gid:%s status:%d.", gid, xidstatus);
		PG_RETURN_INT32(POLARDBX_TRANSACTION_INPROGRESS);
	}
	
	PG_RETURN_NULL();
}
#endif