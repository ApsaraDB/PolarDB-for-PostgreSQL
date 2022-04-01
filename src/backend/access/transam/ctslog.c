/*-------------------------------------------------------------------------
 *
 * ctslog.c based on csnlog.c
 *		Tracking Commit-Timestamps and in-progress subtransactions
 *
 * The pg_ctslog manager is a pg_clog-like manager that stores the commit
 * sequence number, or parent transaction Id, for each transaction.  It is
 * a fundamental part of distributed MVCC.
 *
 * To support distributed transaction, XLOG is added for csnlog to preserve
 * data across crashes. During database startup, we would apply xlog records
 * to csnlog.
 * Author:  , 2020-01-11
 *
 * Implement the scalable CTS (Commit Timestamp Store) that adopts multi-partition LRU
 * and use lock-free algorithms as far as possible, i.e., Get/Set commit timestamp
 * in LRU cached pages with only shared-lock being held.
 * Author:  , 2020-06-19
 *
 * Portions Copyright (c) 2020, Alibaba Group Holding Limited
 * Portions Copyright (c) 1996-2014, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/backend/access/transam/ctslog.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/ctslog.h"
#include "access/mvccvars.h"
#include "access/lru.h"
#include "access/subtrans.h"
#include "access/transam.h"
#include "distributed_txn/txn_timestamp.h"
#include "miscadmin.h"
#include "pg_trace.h"
#include "utils/snapmgr.h"
#include "funcapi.h"
#include "utils/timestamp.h"


/*
 * Defines for CTSLOG page sizes.  A page is the same BLCKSZ as is used
 * everywhere else in Postgres.
 *
 * Note: because TransactionIds are 32 bits and wrap around at 0xFFFFFFFF,
 * CTSLOG page numbering also wraps around at 0xFFFFFFFF/CSNLOG_XACTS_PER_PAGE,
 * and CSNLOG segment numbering at
 * 0xFFFFFFFF/CLOG_XACTS_PER_PAGE/SLRU_PAGES_PER_SEGMENT.  We need take no
 * explicit notice of that fact in this module, except when comparing segment
 * and page numbers in TruncateCSNLOG (see CSNLOGPagePrecedes).
 */

/* We store the commit LSN for each xid */
#define CTSLOG_XACTS_PER_PAGE (BLCKSZ / sizeof(CommitTs))

#define TransactionIdToPage(xid)	((xid) / (TransactionId) CTSLOG_XACTS_PER_PAGE)
#define TransactionIdToPgIndex(xid) ((xid) % (TransactionId) CTSLOG_XACTS_PER_PAGE)

#ifdef ENABLE_DISTRIBUTED_TRANSACTION
/*
 * add WAL for CSNLog to support distributed transactions
 * written by  
 */

/* We store the latest async LSN for each group of transactions */
#define CTSLOG_XACTS_PER_LSN_GROUP	32	/* keep this a power of 2 */
#define CTSLOG_LSNS_PER_PAGE	(CTSLOG_XACTS_PER_PAGE / CTSLOG_XACTS_PER_LSN_GROUP)

#define GetLSNIndex(slotno, xid)	((slotno) * CTSLOG_LSNS_PER_PAGE + \
	((xid) % (TransactionId) CTSLOG_XACTS_PER_PAGE) / CTSLOG_XACTS_PER_LSN_GROUP)

#endif
/* We allocate new log pages in batches */
#define BATCH_SIZE 128

/*
 * Link to shared-memory data structures for CLOG control
 */
static LruCtlData CtslogCtlData;

#define CtslogCtl (&CtslogCtlData)

#ifdef ENABLE_DISTRIBUTED_TRANSACTION
/* local xlog stuff */
static void WriteZeroPageXlogRec(int pageno);
static void WriteTruncateXlogRec(int pageno, TransactionId oldestXact);
static XLogRecPtr WriteSetTimestampXlogRec(TransactionId mainxid, int nsubxids,
						 TransactionId *subxids, CommitTs csn);
#endif

static int	ZeroCTSLOGPage(int pageno, int partitionno, bool writeXlog);
static bool CTSLOGPagePrecedes(int page1, int page2);
static void CTSLogSetPageStatus(TransactionId xid, int nsubxids,
					TransactionId *subxids,
					CommitTs cts, XLogRecPtr lsn, int pageno);
static void CTSLogSetCSN(TransactionId xid, int partitionno, CommitTs cts, XLogRecPtr lsn, int slotno);
static CommitTs InternalGetCommitTs(TransactionId xid, int partitionno);
static CommitTs RecursiveGetCommitTs(TransactionId xid, int partitionno);

/*
 * CSNLogSetCommitSeqNo
 *
 * Record the status and CSN of transaction entries in the commit log for a
 * transaction and its subtransaction tree. Take care to ensure this is
 * efficient, and as atomic as possible.
 *
 * xid is a single xid to set status for. This will typically be the
 * top level transactionid for a top level commit or abort. It can
 * also be a subtransaction when we record transaction aborts.
 *
 * subxids is an array of xids of length nsubxids, representing subtransactions
 * in the tree of xid. In various cases nsubxids may be zero.
 *
 * csn is the commit sequence number of the transaction. It should be
 * InvalidCommitSeqNo for abort cases.
 *
 * Note: This doesn't guarantee atomicity. The caller can use the
 * COMMITSEQNO_COMMITTING special value for that.
 */
void
CTSLogSetCommitTs(TransactionId xid, int nsubxids,
				  TransactionId *subxids, XLogRecPtr lsn, bool write_xlog, CommitTs cts)
{
	int			nextSubxid;
	int			topPage;
	TransactionId topXid;
	XLogRecPtr	max_lsn = lsn;

	if (cts == InvalidCommitSeqNo || xid == BootstrapTransactionId)
	{
		if (IsBootstrapProcessingMode())
			cts = COMMITSEQNO_FROZEN;
		else
			elog(ERROR, "cannot mark transaction committed without CSN");
	}

#ifdef ENABLE_DISTRIBUTED_TRANSACTION

	/*
	 * Comply with the WAL-before-data rule: if caller specified it wants this
	 * value to be recorded in WAL, do so before touching the data. write xlog
	 * is only needed when the modification is not recorded in xlog before
	 * calling this function.
	 */
	if (write_xlog)
	{
		max_lsn = WriteSetTimestampXlogRec(xid, nsubxids, subxids, cts);
		if (lsn > max_lsn)
		{
			max_lsn = lsn;
		}
	}
#endif

	/*
	 * We set the status of child transaction before the status of parent
	 * transactions, so that another process can correctly determine the
	 * resulting status of a child transaction. See RecursiveGetCommitSeqNo().
	 */
	topXid = InvalidTransactionId;
	topPage = TransactionIdToPage(xid);
	nextSubxid = nsubxids - 1;
	do
	{
		int			currentPage = topPage;
		int			subxidsOnPage = 0;

		for (; nextSubxid >= 0; nextSubxid--)
		{
			int			subxidPage = TransactionIdToPage(subxids[nextSubxid]);

			if (subxidsOnPage == 0)
				currentPage = subxidPage;

			if (currentPage != subxidPage)
				break;

			subxidsOnPage++;
		}

		if (currentPage == topPage)
		{
			Assert(topXid == InvalidTransactionId);
			topXid = xid;
		}

		CTSLogSetPageStatus(topXid, subxidsOnPage, subxids + nextSubxid + 1,
							cts, max_lsn, currentPage);
	}
	while (nextSubxid >= 0);

	if (topXid == InvalidTransactionId)
	{
		/*
		 * No subxids were on the same page as the main xid; we have to update
		 * it separately
		 */

		CTSLogSetPageStatus(xid, 0, NULL, cts, max_lsn, topPage);
	}
}

#ifdef ENABLE_DISTRIBUTED_TRANSACTION
CommitTs
CTSLogAssignCommitTs(TransactionId xid, int nxids, TransactionId *xids, bool fromCoordinator)
{
	CommitTs	cts;
	TransactionId latestXid;
	TransactionId currentLatestCompletedXid;

	latestXid = TransactionIdLatest(xid, nxids, xids);

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

	/*
	 * Mark our top transaction id as commit-in-progress.
	 */
	if (false == fromCoordinator)
	{
		CTSLogSetCommitTs(xid, 0, NULL, InvalidXLogRecPtr, false, COMMITSEQNO_COMMITTING);
	}

#ifdef ENABLE_DISTRIBUTED_TRANSACTION
	cts = TxnGetOrGenerateCommitTs(fromCoordinator);
	Assert(!COMMITSEQNO_IS_SUBTRANS(cts));

	if (!COMMITSEQNO_IS_NORMAL(cts))
		elog(ERROR, "invalid commit ts " UINT64_FORMAT, cts);

	TxnSetReplyTimestamp(cts);
#else
	/* Get our CSN and increment */
	CSNLogSetCommitSeqNo(xid, 0, NULL, InvalidXLogRecPtr, false, COMMITSEQNO_COMMITTING);
	csn = pg_atomic_fetch_add_u64(&ShmemVariableCache->nextCommitSeqNo, 1);
#endif

	Assert(cts >= COMMITSEQNO_FIRST_NORMAL);
	if (enable_timestamp_debug_print)
		elog(LOG, "xid %d assign commit timestamp " UINT64_FORMAT, xid, cts);
	return cts;
}
#endif
/*
 * Record the final state of transaction entries in the csn log for
 * all entries on a single page.  Atomic only on this page.
 *
 * Otherwise API is same as TransactionIdSetTreeStatus()
 */
static void
CTSLogSetPageStatus(TransactionId xid, int nsubxids,
					TransactionId *subxids,
					CommitTs cts, XLogRecPtr lsn, int pageno)
{
	int			slotno;
	int			i;
	int			partitionno;
	LWLock	   *partitionLock;	/* buffer partition lock for it */


	partitionno = PagenoMappingPartitionno(CtslogCtl, pageno);
	partitionLock = GetPartitionLock(CtslogCtl, partitionno);
	LWLockAcquire(partitionLock, LW_SHARED);

	/* Try to find the page while holding only shared lock */

	slotno = LruReadPage_ReadOnly_Locked(CtslogCtl, partitionno, pageno, XLogRecPtrIsInvalid(lsn), xid);

	/*
	 * We set the status of child transaction before the status of parent
	 * transactions, so that another process can correctly determine the
	 * resulting status of a child transaction. See RecursiveGetCommitSeqNo().
	 */
	for (i = nsubxids - 1; i >= 0; i--)
	{
		Assert(CtslogCtl->shared[partitionno]->page_number[slotno] == TransactionIdToPage(subxids[i]));
		CTSLogSetCSN(subxids[i], partitionno, cts, lsn, slotno);
		pg_write_barrier();
	}

	if (TransactionIdIsValid(xid))
		CTSLogSetCSN(xid, partitionno, cts, lsn, slotno);

	CtslogCtl->shared[partitionno]->page_dirty[slotno] = true;

	LWLockRelease(partitionLock);

}



/*
 * Record the parent of a subtransaction in the subtrans log.
 *
 * In some cases we may need to overwrite an existing value.
 */
void
SubTransSetParent(TransactionId xid, TransactionId parent)
{
	int			pageno = TransactionIdToPage(xid);
	int			entryno = TransactionIdToPgIndex(xid);
	int			slotno;
	CommitTs   *ptr;
	CommitTs	newcts;
	int			partitionno = PagenoMappingPartitionno(CtslogCtl, pageno);
	LWLock	   *partitionLock = GetPartitionLock(CtslogCtl, partitionno);


	Assert(TransactionIdIsValid(parent));
	Assert(TransactionIdFollows(xid, parent));

	newcts = CSN_SUBTRANS_BIT | (uint64) parent;

	/*
	 * Shared page access is enough to set the subtransaction parent. It is
	 * set when the subtransaction is assigned an xid, and can be read only
	 * later, after the subtransaction have modified some tuples.
	 */
	slotno = LruReadPage_ReadOnly(CtslogCtl, partitionno, pageno, xid);
	ptr = (CommitSeqNo *) CtslogCtl->shared[partitionno]->page_buffer[slotno];
	ptr += entryno;

	/*
	 * It's possible we'll try to set the parent xid multiple times but we
	 * shouldn't ever be changing the xid from one valid xid to another valid
	 * xid, which would corrupt the data structure.
	 */
	if (*ptr != newcts)
	{
		Assert(*ptr == COMMITSEQNO_INPROGRESS);
		*ptr = newcts;
		CtslogCtl->shared[partitionno]->page_dirty[slotno] = true;
	}


	LWLockRelease(partitionLock);
}

/*
 * Interrogate the parent of a transaction in the csnlog.
 */
TransactionId
SubTransGetParent(TransactionId xid)
{
	CommitTs	cts;
	int			pageno = TransactionIdToPage(xid);
	int			partitionno = PagenoMappingPartitionno(CtslogCtl, pageno);
	LWLock	   *partitionLock = GetPartitionLock(CtslogCtl, partitionno);

	LWLockAcquire(partitionLock, LW_SHARED);

	cts = InternalGetCommitTs(xid, partitionno);

	LWLockRelease(partitionLock);

	if (COMMITSEQNO_IS_SUBTRANS(cts))
		return (TransactionId) (cts & 0xFFFFFFFF);
	else
		return InvalidTransactionId;
}

/*
 * SubTransGetTopmostTransaction
 *
 * Returns the topmost transaction of the given transaction id.
 *
 * Because we cannot look back further than TransactionXmin, it is possible
 * that this function will lie and return an intermediate subtransaction ID
 * instead of the true topmost parent ID.  This is OK, because in practice
 * we only care about detecting whether the topmost parent is still running
 * or is part of a current snapshot's list of still-running transactions.
 * Therefore, any XID before TransactionXmin is as good as any other.
 */
TransactionId
SubTransGetTopmostTransaction(TransactionId xid)
{
	TransactionId parentXid = xid,
				previousXid = xid;

	/* Can't ask about stuff that might not be around anymore */
	Assert(TransactionIdFollowsOrEquals(xid, TransactionXmin));

	while (TransactionIdIsValid(parentXid))
	{
		previousXid = parentXid;
		if (TransactionIdPrecedes(parentXid, TransactionXmin))
			break;
		parentXid = SubTransGetParent(parentXid);

		/*
		 * By convention the parent xid gets allocated first, so should always
		 * precede the child xid. Anything else points to a corrupted data
		 * structure that could lead to an infinite loop, so exit.
		 */
		if (!TransactionIdPrecedes(parentXid, previousXid))
			elog(ERROR, "pg_csnlog contains invalid entry: xid %u points to parent xid %u",
				 previousXid, parentXid);
	}

	Assert(TransactionIdIsValid(previousXid));

	return previousXid;
}


/*
 * Sets the commit status of a single transaction.
 *
 * Must be called with CSNLogControlLock held
 */
static void
CTSLogSetCSN(TransactionId xid, int partitionno, CommitTs cts, XLogRecPtr lsn, int slotno)
{
	int			entryno = TransactionIdToPgIndex(xid);
	CommitTs   *ptr;

#ifdef ENABLE_DISTRIBUTED_TRANSACTION

	/*
	 * Update the group LSN if the transaction completion LSN is higher.
	 *
	 * Note: lsn will be invalid when supplied during InRecovery processing,
	 * so we don't need to do anything special to avoid LSN updates during
	 * recovery. After recovery completes the next csnlog change will set the
	 * LSN correctly.
	 *
	 * We must first set LSN before CTS is written so that we can guarantee
	 * that CTSLogGetLSN() always reflects the fresh LSN corresponding to the
	 * asynchronous commit xid.
	 *
	 * In other words, if we see a async committed xid, the corresponding LSN
	 * has already been updated.
	 *
	 * The consideration is due to lack of locking protection when setting and
	 * fetching LSN.
	 *
	 * Written by  , 2020-09-03
	 *
	 */

	if (!XLogRecPtrIsInvalid(lsn))
	{
		int			lsnindex = GetLSNIndex(slotno, xid);

		if (CtslogCtl->shared[partitionno]->group_lsn[lsnindex] < lsn)
			CtslogCtl->shared[partitionno]->group_lsn[lsnindex] = lsn;
	}
#endif

	ptr = (CommitTs *) (CtslogCtl->shared[partitionno]->page_buffer[slotno] + entryno * sizeof(XLogRecPtr));

	/*
	 * Current state change should be from 0 to target state. (Allow setting
	 * it again to same value.)
	 */
	Assert(COMMITSEQNO_IS_INPROGRESS(*ptr) ||
		   COMMITSEQNO_IS_COMMITTING(*ptr) ||
		   COMMITSEQNO_IS_SUBTRANS(*ptr) ||
		   COMMITSEQNO_IS_PREPARED(*ptr) ||
		   *ptr == cts);

	*ptr = cts;

}

/*
 * Interrogate the state of a transaction in the commit log.
 *
 * Aside from the actual commit status, this function returns (into *lsn)
 * an LSN that is late enough to be able to guarantee that if we flush up to
 * that LSN then we will have flushed the transaction's commit record to disk.
 * The result is not necessarily the exact LSN of the transaction's commit
 * record!	For example, for long-past transactions (those whose clog pages
 * already migrated to disk), we'll return InvalidXLogRecPtr.  Also, because
 * we group transactions on the same clog page to conserve storage, we might
 * return the LSN of a later transaction that falls into the same group.
 *
 * NB: this is a low-level routine and is NOT the preferred entry point
 * for most uses; TransactionIdGetCommitSeqNo() in transam.c is the intended caller.
 */
CommitTs
CTSLogGetCommitTs(TransactionId xid)
{
	int			pageno = TransactionIdToPage(xid);
	int			partitionno = PagenoMappingPartitionno(CtslogCtl, pageno);
	LWLock	   *partitionLock = GetPartitionLock(CtslogCtl, partitionno);
	CommitTs	cts;


	LWLockAcquire(partitionLock, LW_SHARED);

	cts = RecursiveGetCommitTs(xid, partitionno);

	LWLockRelease(partitionLock);

	/*
	 * As the cts status of crashed transactions may not be set, it would be
	 * regarding as in-progress by mistaken. Note that
	 * TransactionIdIsInProgress() is removed by CSN to avoid proc array
	 * walking. As a result, we need to perform further checking: If the xid
	 * is below TransactionXmin and does not have cts, it should be crashed or
	 * aborted transaction. Written by  , 2020.06.22
	 */

	if (TransactionIdPrecedes(xid, CtslogCtl->global_shared->oldestActiveStartupXid))
	{
		if (!COMMITSEQNO_IS_COMMITTED(cts))
			cts = COMMITSEQNO_ABORTED;
	}

	return cts;
}

/* Determine the CSN of a transaction, walking the subtransaction tree if needed */
static CommitTs
RecursiveGetCommitTs(TransactionId xid, int partitionno)
{
	CommitTs	cts;

	cts = InternalGetCommitTs(xid, partitionno);

	if (COMMITSEQNO_IS_SUBTRANS(cts))
	{
		TransactionId parentXid = cts & ~CSN_SUBTRANS_BIT;
		int			parentPageno = TransactionIdToPage(parentXid);
		int			parentPartitionno = PagenoMappingPartitionno(CtslogCtl, parentPageno);
		LWLock	   *partitionLock = GetPartitionLock(CtslogCtl, partitionno);
		LWLock	   *parentPartitionLock = GetPartitionLock(CtslogCtl, parentPartitionno);
		CommitTs	parentCts;

		if (parentPartitionno != partitionno)
		{
			LWLockRelease(partitionLock);
			LWLockAcquire(parentPartitionLock, LW_SHARED);
		}

		parentCts = RecursiveGetCommitTs(parentXid, parentPartitionno);

		if (parentPartitionno != partitionno)
		{
			LWLockRelease(parentPartitionLock);
			LWLockAcquire(partitionLock, LW_SHARED);
		}

		Assert(!COMMITSEQNO_IS_SUBTRANS(parentCts));

		/*
		 * The parent and child transaction status update is not atomic. We
		 * must take care not to use the updated parent status with the old
		 * child status, or else we can wrongly see a committed subtransaction
		 * as aborted. This happens when the parent is already marked as
		 * committed and the child is not yet marked.
		 */
		pg_read_barrier();

		cts = InternalGetCommitTs(xid, partitionno);

		if (COMMITSEQNO_IS_SUBTRANS(cts))
		{
			if (COMMITSEQNO_IS_PREPARED(parentCts))
				cts = MASK_PREPARE_BIT(cts);
			else if (COMMITSEQNO_IS_INPROGRESS(parentCts))
				cts = COMMITSEQNO_INPROGRESS;
			else if (COMMITSEQNO_IS_COMMITTING(parentCts))
				cts = COMMITSEQNO_COMMITTING;
			else
			{
				elog(ERROR, "unexpected parent status "INT64_FORMAT, parentCts);
				Assert(false);
			}
		}
	}

	return cts;
}

/*
 * Get the raw CSN value.
 */
static CommitTs
InternalGetCommitTs(TransactionId xid, int partitionno)
{
	int			pageno = TransactionIdToPage(xid);
	int			entryno = TransactionIdToPgIndex(xid);
	int			slotno;

	/* Can't ask about stuff that might not be around anymore */
	/* Assert(TransactionIdFollowsOrEquals(xid, TransactionXmin)); */

	if (!TransactionIdIsNormal(xid))
	{
		if (xid == InvalidTransactionId)
			return COMMITSEQNO_ABORTED;
		if (xid == FrozenTransactionId || xid == BootstrapTransactionId)
			return COMMITSEQNO_FROZEN;
	}

	slotno = LruReadPage_ReadOnly_Locked(CtslogCtl, partitionno, pageno, true, xid);
	return *(CommitTs *) (CtslogCtl->shared[partitionno]->page_buffer[slotno]
						  + entryno * sizeof(XLogRecPtr));
}

XLogRecPtr
CTSLogGetLSN(TransactionId xid)
{
	int			pageno = TransactionIdToPage(xid);
	int			partitionno = PagenoMappingPartitionno(CtslogCtl, pageno);
	LWLock	   *partitionLock = GetPartitionLock(CtslogCtl, partitionno);
	int			lsnindex;
	int			slotno;
	XLogRecPtr	lsn;

	LWLockAcquire(partitionLock, LW_SHARED);

	slotno = LruLookupSlotno_Locked(CtslogCtl, partitionno, pageno);

	/*
	 * We do not need to really read the page. If the page is not buffered, it
	 * indicates it is written out to disk. Under such situation, the xlog
	 * records of the async transactions have been durable.
	 */
	if (slotno == -1)
	{
		LWLockRelease(partitionLock);
		return InvalidXLogRecPtr;
	}

	lsnindex = GetLSNIndex(slotno, xid);

	/*
	 * We asume 8-byte atomic CPU to guarantee correctness with no locking.
	 */
	lsn = CtslogCtl->shared[partitionno]->group_lsn[lsnindex];

	LWLockRelease(partitionLock);

	return lsn;
}

/*
 * Find the next xid that is in progress.
 * We do not care about the subtransactions, they are accounted for
 * by their respective top-level transactions.
 */
TransactionId
CTSLogGetNextActiveXid(TransactionId xid,
					   TransactionId end)
{
	int			saved_partitionno = -1;
	LWLock	   *partitionLock = NULL;

	Assert(TransactionIdIsValid(TransactionXmin));


	for (;;)
	{
		int			pageno;
		int			partitionno;
		int			slotno;
		int			entryno;

		if (!TransactionIdPrecedes(xid, end))
			goto end;

		pageno = TransactionIdToPage(xid);
		partitionno = PagenoMappingPartitionno(CtslogCtl, pageno);

		if (partitionno != saved_partitionno)
		{
			if (saved_partitionno >= 0)
				LWLockRelease(partitionLock);

			partitionLock = GetPartitionLock(CtslogCtl, partitionno);
			saved_partitionno = partitionno;
			LWLockAcquire(partitionLock, LW_SHARED);
		}

		slotno = LruReadPage_ReadOnly_Locked(CtslogCtl, partitionno, pageno, true, xid);

		for (entryno = TransactionIdToPgIndex(xid); entryno < CTSLOG_XACTS_PER_PAGE;
			 entryno++)
		{
			CommitTs	cts;

			if (!TransactionIdPrecedes(xid, end))
				goto end;

			cts = *(XLogRecPtr *) (CtslogCtl->shared[partitionno]->page_buffer[slotno] + entryno * sizeof(XLogRecPtr));

			if (COMMITSEQNO_IS_INPROGRESS(cts)
				|| COMMITSEQNO_IS_PREPARED(cts)
				|| COMMITSEQNO_IS_COMMITTING(cts))			
			{
				goto end;
			}

			TransactionIdAdvance(xid);
		}


	}

end:
	if (saved_partitionno >= 0)
		LWLockRelease(partitionLock);

	return xid;
}

/*
 * Number of shared CSNLOG buffers.
 */
Size
CTSLOGShmemBuffers(void)
{
	return Min(1024, Max(BATCH_SIZE, NBuffers / 512));
}

/*
 * Initialization of shared memory for CSNLOG
 */
Size
CTSLOGShmemSize(void)
{
	int			hash_table_size = NUM_PARTITIONS * CTSLOGShmemBuffers() + NUM_PARTITIONS;

	return NUM_PARTITIONS * (LruShmemSize(CTSLOGShmemBuffers(), CTSLOG_LSNS_PER_PAGE)) +
		MAXALIGN(sizeof(GlobalLruSharedData)) + LruBufTableShmemSize(hash_table_size);
}

void
CTSLOGShmemInit(void)
{
	int			hash_table_size = NUM_PARTITIONS * CTSLOGShmemBuffers() + NUM_PARTITIONS;

	CtslogCtl->PagePrecedes = CTSLOGPagePrecedes;

	LruInit(CtslogCtl, "CTSLOG Ctl", CTSLOGShmemBuffers(), CTSLOG_LSNS_PER_PAGE, hash_table_size,
			CTSLogControlLock, "pg_ctslog",
			LWTRANCHE_CTSLOG_BUFFERS);
}

/*
 * This func must be called ONCE on system install.  It creates
 * the initial CSNLOG segment.  (The pg_csnlog directory is assumed to
 * have been created by initdb, and CSNLOGShmemInit must have been
 * called already.)
 */
void
BootStrapCTSLOG(void)
{
	int			slotno;
	int			pageno = 0;
	int			partitionno = PagenoMappingPartitionno(CtslogCtl, pageno);
	LWLock	   *partitionLock = GetPartitionLock(CtslogCtl, partitionno);

	LWLockAcquire(partitionLock, LW_EXCLUSIVE);

	/* Create and zero the first page of the commit log */
	slotno = ZeroCTSLOGPage(0, partitionno, false);

	/* Make sure it's written out */
	LruWritePage(CtslogCtl, partitionno, slotno);
	Assert(!CtslogCtl->shared[partitionno]->page_dirty[slotno]);

	LWLockRelease(partitionLock);
}


/*
 * Initialize (or reinitialize) a page of CLOG to zeroes.
 * If writeXlog is TRUE, also emit an XLOG record saying we did this.
 *
 * The page is not actually written, just set up in shared memory.
 * The slot number of the new page is returned.
 *
 * Control lock must be held at entry, and will be held at exit.
 */
static int
ZeroCTSLOGPage(int pageno, int partitionno, bool writeXlog)
{
	int			slotno;

	slotno = LruZeroPage(CtslogCtl, partitionno, pageno);

#ifdef ENABLE_DISTRIBUTED_TRANSACTION
	if (writeXlog)
		WriteZeroPageXlogRec(pageno);
#endif

	return slotno;
}

void
RecoverCTSLOG(TransactionId oldestActiveXID)
{
	elog(LOG, "start recover CTS log oldestActivXid %u nextXid %u", oldestActiveXID,
		 ShmemVariableCache->nextXid);

	/*
	 * For standby promotion, we must reset the oldestActiveStartupXid
	 * correctly.
	 */
	LWLockAcquire(CTSLogControlLock, LW_EXCLUSIVE);
	CtslogCtl->global_shared->oldestActiveStartupXid = oldestActiveXID;
	LWLockRelease(CTSLogControlLock);

	if (TransactionIdIsNormal(oldestActiveXID))
	{
		int			start_xid = oldestActiveXID;
		int			end_xid = ShmemVariableCache->nextXid;
		CommitTs	cts;

		/*
		 * For each xid within [oldestActiveXid, nextXid), in-progress status
		 * in CTSLog indicates it must be crash aborted.
		 */
		while (TransactionIdPrecedes(start_xid, end_xid))
		{
			cts = CTSLogGetCommitTs(start_xid);
			if (cts == COMMITSEQNO_INPROGRESS)
			{
				CTSLogSetCommitTs(start_xid, 0, NULL, InvalidXLogRecPtr, false, COMMITSEQNO_ABORTED);
				elog(LOG, "recover crash aborted xid %d next xid %d", start_xid, end_xid);
			}

			TransactionIdAdvance(start_xid);
		}
	}

}

/*
 * This must be called ONCE during postmaster or standalone-backend startup,
 * after StartupXLOG has initialized ShmemVariableCache->nextXid.
 *
 * oldestActiveXID is the oldest XID of any prepared transaction, or nextXid
 * if there are none.
 */
void
StartupCTSLOG(TransactionId oldestActiveXID)
{

#ifdef ENABLE_DISTRIBUTED_TRANSACTION
	TransactionId xid = ShmemVariableCache->nextXid;
	int			pageno = TransactionIdToPage(xid);
	int			partitionno = PagenoMappingPartitionno(CtslogCtl, pageno);
	LWLock	   *partitionlock;

	/*
	 * Initialize our idea of the latest page number.
	 */
	LWLockAcquire(CTSLogControlLock, LW_EXCLUSIVE);
	CtslogCtl->global_shared->latest_page_number = pageno;
	CtslogCtl->global_shared->oldestActiveStartupXid = oldestActiveXID;
	LWLockRelease(CTSLogControlLock);

	partitionlock = GetPartitionLock(CtslogCtl, partitionno);
	LWLockAcquire(partitionlock, LW_EXCLUSIVE);
	CtslogCtl->shared[partitionno]->latest_page_number = pageno;
	LWLockRelease(partitionlock);

#ifdef ENABLE_DISTR_DEBUG
	elog(LOG, "Startup debug version CTS");
#else
	elog(LOG, "Startup multi-core scaling CTS");
#endif

#else
	int			startPage;
	int			endPage;

	/*
	 * Since we don't expect pg_csnlog to be valid across crashes, we
	 * initialize the currently-active page(s) to zeroes during startup.
	 * Whenever we advance into a new page, ExtendCSNLOG will likewise zero
	 * the new page without regard to whatever was previously on disk.
	 */
	LWLockAcquire(CSNLogControlLock, LW_EXCLUSIVE);

	startPage = TransactionIdToPage(oldestActiveXID);
	endPage = TransactionIdToPage(ShmemVariableCache->nextXid);
	endPage = ((endPage + BATCH_SIZE - 1) / BATCH_SIZE) * BATCH_SIZE;

	while (startPage != endPage)
	{
		(void) ZeroCSNLOGPage(startPage);
		startPage++;
		/* must account for wraparound */
		if (startPage > TransactionIdToPage(MaxTransactionId))
			startPage = 0;
	}
	(void) ZeroCSNLOGPage(startPage);

	LWLockRelease(CSNLogControlLock);
#endif

}

/*
 * This must be called ONCE during postmaster or standalone-backend shutdown
 */
void
ShutdownCTSLOG(void)
{
	/*
	 * Flush dirty CLOG pages to disk
	 *
	 * This is not actually necessary from a correctness point of view. We do
	 * it merely as a debugging aid.
	 */
	LruFlush(CtslogCtl, false);

	/*
	 * fsync pg_csnlog to ensure that any files flushed previously are durably
	 * on disk.
	 */
	fsync_fname("pg_ctslog", true);
}

/*
 * This must be called ONCE at the end of startup/recovery.
 */
void
TrimCTSLOG(void)
{
	TransactionId xid = ShmemVariableCache->nextXid;
	int			pageno = TransactionIdToPage(xid);
	int			partitionno = PagenoMappingPartitionno(CtslogCtl, pageno);
	LWLock	   *partitionlock;

	LWLockAcquire(CtslogCtl->global_shared->ControlLock, LW_EXCLUSIVE);
	CtslogCtl->global_shared->latest_page_number = pageno;
	LWLockRelease(CtslogCtl->global_shared->ControlLock);

	/*
	 * Re-Initialize our idea of the latest page number.
	 */
	partitionlock = GetPartitionLock(CtslogCtl, partitionno);
	LWLockAcquire(partitionlock, LW_EXCLUSIVE);
	CtslogCtl->shared[partitionno]->latest_page_number = pageno;

	/*
	 * Zero out the remainder of the current clog page.  Under normal
	 * circumstances it should be zeroes already, but it seems at least
	 * theoretically possible that XLOG replay will have settled on a nextXID
	 * value that is less than the last XID actually used and marked by the
	 * previous database lifecycle (since subtransaction commit writes clog
	 * but makes no WAL entry).  Let's just be safe. (We need not worry about
	 * pages beyond the current one, since those will be zeroed when first
	 * used.  For the same reason, there is no need to do anything when
	 * nextXid is exactly at a page boundary; and it's likely that the
	 * "current" page doesn't exist yet in that case.)
	 */
	if (TransactionIdToPgIndex(xid) != 0)
	{
		int			entryno = TransactionIdToPgIndex(xid);
		int			byteno = entryno * sizeof(XLogRecPtr);
		int			slotno;
		char	   *byteptr;


		slotno = LruReadPage(CtslogCtl, partitionno, pageno, false, xid);

		byteptr = CtslogCtl->shared[partitionno]->page_buffer[slotno] + byteno;

		/* Zero the rest of the page */
		MemSet(byteptr, 0, BLCKSZ - byteno);
		elog(LOG, "Trim ctslog start from %d size %d next xid %u", byteno, BLCKSZ - byteno, xid);

		CtslogCtl->shared[partitionno]->page_dirty[slotno] = true;
	}

	LWLockRelease(partitionlock);
}

/*
 * Perform a checkpoint --- either during shutdown, or on-the-fly
 */
void
CheckPointCTSLOG(void)
{
	/*
	 * Flush dirty CLOG pages to disk
	 *
	 * This is not actually necessary from a correctness point of view. We do
	 * it merely to improve the odds that writing of dirty pages is done by
	 * the checkpoint process and not by backends.
	 */
	LruFlush(CtslogCtl, true);
#ifdef ENABLE_DISTRIBUTED_TRANSACTION

	/*
	 * fsync pg_csnlog to ensure that any files flushed previously are durably
	 * on disk.
	 */
	fsync_fname("pg_ctslog", true);
#endif
}


/*
 * Make sure that CSNLOG has room for a newly-allocated XID.
 *
 * NB: this is called while holding XidGenLock.  We want it to be very fast
 * most of the time; even when it's not so fast, no actual I/O need happen
 * unless we're forced to write out a dirty clog or xlog page to make room
 * in shared memory.
 */
void
ExtendCTSLOG(TransactionId newestXact)
{

	int			pageno;
	int			partitionno;
#ifdef ENABLE_BATCH
	int			pre_partitionno = -1;
	int			i;
#endif
	LWLock	   *partitionlock;

	/*
	 * No work except at first XID of a page.  But beware: just after
	 * wraparound, the first XID of page zero is FirstNormalTransactionId.
	 */
	if (TransactionIdToPgIndex(newestXact) != 0 &&
		!TransactionIdEquals(newestXact, FirstNormalTransactionId))
		return;

	pageno = TransactionIdToPage(newestXact);
#ifdef ENABLE_BATCH
	if (pageno % BATCH_SIZE)
		return;

	/*
	 * Acquire global lock to protect global latest page number that would be
	 * modified in ZeroCTSLOGPage(). We keep the locking order of global lock
	 * -> partition lock.
	 */

	LWLockAcquire(CtslogCtl->global_shared->ControlLock, LW_EXCLUSIVE);
	for (i = pageno; i < pageno + BATCH_SIZE; i++)
	{
		partitionno = PagenoMappingPartitionno(CtslogCtl, i);
		if (partitionno != pre_partitionno)
		{
			if (pre_partitionno >= 0)
				LWLockRelease(partitionlock);
			partitionlock = GetPartitionLock(CtslogCtl, partitionno);
			pre_partitionno = partitionno;
			LWLockAcquire(partitionlock, LW_EXCLUSIVE);
		}

		/* Zero the page and make an XLOG entry about it */
		ZeroCTSLOGPage(i, partitionno, true);
	}

	if (pre_partitionno >= 0)
		LWLockRelease(partitionlock);
	LWLockRelease(CtslogCtl->global_shared->ControlLock);
#else
	LWLockAcquire(CtslogCtl->global_shared->ControlLock, LW_EXCLUSIVE);
	partitionno = PagenoMappingPartitionno(CtslogCtl, pageno);
	partitionlock = GetPartitionLock(CtslogCtl, partitionno);

	LWLockAcquire(partitionlock, LW_EXCLUSIVE);
	ZeroCTSLOGPage(pageno, partitionno, true);
	LWLockRelease(partitionlock);
	LWLockRelease(CtslogCtl->global_shared->ControlLock);
#endif


}


/*
 * Remove all CSNLOG segments before the one holding the passed transaction ID
 *
 * This is normally called during checkpoint, with oldestXact being the
 * oldest TransactionXmin of any running transaction.
 */
void
TruncateCTSLOG(TransactionId oldestXact)
{
	int			cutoffPage;

	/*
	 * The cutoff point is the start of the segment containing oldestXact. We
	 * pass the *page* containing oldestXact to SimpleLruTruncate.
	 */
	cutoffPage = TransactionIdToPage(oldestXact);

	/* Check to see if there's any files that could be removed */
	if (!LruScanDirectory(CtslogCtl, LruScanDirCbReportPresence, &cutoffPage))
		return;					/* nothing to remove */

	/*
	 * Write XLOG record and flush XLOG to disk. We record the oldest xid
	 * we're keeping information about here so we can ensure that it's always
	 * ahead of clog truncation in case we crash, and so a standby finds out
	 * the new valid xid before the next checkpoint.
	 */
	WriteTruncateXlogRec(cutoffPage, oldestXact);

	elog(LOG, "truncate cutoffpage %d", cutoffPage);
	LruTruncate(CtslogCtl, cutoffPage);
}


/*
 * Decide which of two CLOG page numbers is "older" for truncation purposes.
 *
 * We need to use comparison of TransactionIds here in order to do the right
 * thing with wraparound XID arithmetic.  However, if we are asked about
 * page number zero, we don't want to hand InvalidTransactionId to
 * TransactionIdPrecedes: it'll get weird about permanent xact IDs.  So,
 * offset both xids by FirstNormalTransactionId to avoid that.
 */
static bool
CTSLOGPagePrecedes(int page1, int page2)
{
	TransactionId xid1;
	TransactionId xid2;

	xid1 = ((TransactionId) page1) * CTSLOG_XACTS_PER_PAGE;
	xid1 += FirstNormalTransactionId;
	xid2 = ((TransactionId) page2) * CTSLOG_XACTS_PER_PAGE;
	xid2 += FirstNormalTransactionId;

	return TransactionIdPrecedes(xid1, xid2);
}

#ifdef ENABLE_DISTRIBUTED_TRANSACTION


PG_FUNCTION_INFO_V1(pg_xact_get_cts);
/*
 * function api to get csn for given xid
 */
Datum
pg_xact_get_cts(PG_FUNCTION_ARGS)
{
	TransactionId xid = PG_GETARG_UINT32(0);
	CommitTs	cts;
	StringInfoData str;

	initStringInfo(&str);

	cts = CTSLogGetCommitTs(xid);

	appendStringInfo(&str, "csn: " UINT64_FORMAT, cts);

	PG_RETURN_CSTRING(str.data);
}

/*
 * Write a ZEROPAGE xlog record
 */
static void
WriteZeroPageXlogRec(int pageno)
{
	XLogBeginInsert();
	XLogRegisterData((char *) (&pageno), sizeof(int));
	(void) XLogInsert(RM_CTSLOG_ID, CTSLOG_ZEROPAGE);
}

/*
 * Write a TRUNCATE xlog record
 *
 * We must flush the xlog record to disk before returning --- see notes
 * in TruncateCLOG().
 */
static void
WriteTruncateXlogRec(int pageno, TransactionId oldestXact)
{
	XLogRecPtr	recptr;
	xl_ctslog_truncate xlrec;

	xlrec.pageno = pageno;
	xlrec.oldestXact = oldestXact;

	XLogBeginInsert();
	XLogRegisterData((char *) (&xlrec), sizeof(xl_ctslog_truncate));
	recptr = XLogInsert(RM_CTSLOG_ID, CTSLOG_TRUNCATE);
	XLogFlush(recptr);
}

/*
 * Write a SETCSN xlog record
 */
static XLogRecPtr
WriteSetTimestampXlogRec(TransactionId mainxid, int nsubxids,
						 TransactionId *subxids, CommitSeqNo csn)
{
	xl_cts_set	record;

	record.cts = csn;
	record.mainxid = mainxid;

	XLogBeginInsert();
	XLogRegisterData((char *) &record,
					 SizeOfCtsSet);
	XLogRegisterData((char *) subxids, nsubxids * sizeof(TransactionId));
	return XLogInsert(RM_CTSLOG_ID, CTSLOG_SETCSN);
}


/*
 * CSNLOG resource manager's routines
 */
void
ctslog_redo(XLogReaderState *record)
{
	uint8		info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

	/* Backup blocks are not used in clog records */
	Assert(!XLogRecHasAnyBlockRefs(record));

	if (info == CTSLOG_ZEROPAGE)
	{
		int			pageno;
		int			slotno;
		int			partitionno;
		LWLock	   *partitionlock;

		memcpy(&pageno, XLogRecGetData(record), sizeof(int));

		partitionno = PagenoMappingPartitionno(CtslogCtl, pageno);

		partitionlock = GetPartitionLock(CtslogCtl, partitionno);
		elog(DEBUG1, "redo committs: zero partitionno %d pageno %d", partitionno, pageno);
		LWLockAcquire(partitionlock, LW_EXCLUSIVE);
		slotno = ZeroCTSLOGPage(pageno, partitionno, false);
		LruWritePage(CtslogCtl, partitionno, slotno);
		Assert(!CtslogCtl->shared[partitionno]->page_dirty[slotno]);

		LWLockRelease(partitionlock);
	}
	else if (info == CTSLOG_TRUNCATE)
	{
		int			partitionno;
		xl_ctslog_truncate xlrec;

		memcpy(&xlrec, XLogRecGetData(record), sizeof(xl_ctslog_truncate));

		partitionno = PagenoMappingPartitionno(CtslogCtl, xlrec.pageno);

		/*
		 * During XLOG replay, latest_page_number isn't set up yet; insert a
		 * suitable value to bypass the sanity test in SimpleLruTruncate.
		 */
		CtslogCtl->shared[partitionno]->latest_page_number = xlrec.pageno;
		CtslogCtl->global_shared->latest_page_number = xlrec.pageno;

		LruTruncate(CtslogCtl, xlrec.pageno);
	}
	else if (info == CTSLOG_SETCSN)
	{
		xl_cts_set *setcts = (xl_cts_set *) XLogRecGetData(record);
		int			nsubxids;
		TransactionId *subxids;

		nsubxids = ((XLogRecGetDataLen(record) - SizeOfCtsSet) /
					sizeof(TransactionId));

		if (nsubxids > 0)
		{
			subxids = palloc(sizeof(TransactionId) * nsubxids);
			memcpy(subxids,
				   XLogRecGetData(record) + SizeOfCtsSet,
				   sizeof(TransactionId) * nsubxids);
		}
		else
			subxids = NULL;

		CTSLogSetCommitTs(setcts->mainxid, nsubxids, subxids, InvalidXLogRecPtr, false, setcts->cts);
		elog(DEBUG1, "csnlog_redo: set xid %d csn " INT64_FORMAT, setcts->mainxid, setcts->cts);
		if (subxids)
			pfree(subxids);
	}
	else
		elog(PANIC, "csnlog_redo: unknown op code %u", info);
}
#endif
