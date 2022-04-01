/*-------------------------------------------------------------------------
 *
 * csnlog.c
 *		Tracking Commit-Sequence-Numbers and in-progress subtransactions
 *
 * The pg_csnlog manager is a pg_clog-like manager that stores the commit
 * sequence number, or parent transaction Id, for each transaction.  It is
 * a fundamental part of MVCC.
 *
 * The csnlog serves two purposes:
 *
 * 1. While a transaction is in progress, it stores the parent transaction
 * Id for each in-progress subtransaction. A main transaction has a parent
 * of InvalidTransactionId, and each subtransaction has its immediate
 * parent. The tree can easily be walked from child to parent, but not in
 * the opposite direction.
 *
 * 2. After a transaction has committed, it stores the Commit Sequence
 * Number of the commit.
 *
 * We can use the same structure for both, because we don't care about the
 * parent-child relationships subtransaction after commit.
 *
 * This code is based on clog.c, but the robustness requirements
 * are completely different from pg_clog, because we only need to remember
 * pg_csnlog information for currently-open and recently committed
 * transactions.  Thus, there is no need to preserve data over a crash and
 * restart.
 *
 * There are no XLOG interactions since we do not care about preserving
 * data across crashes.  During database startup, we simply force the
 * currently-active page of CSNLOG to zeroes.
 *
 * To support distributed transaction, XLOG is added for csnlog to preserve
 * data across crashes. During database startup, we would apply xlog records
 * to csnlog.
 * Written by  , 2020-01-11
 *
 * Portions Copyright (c) 2020, Alibaba Group Holding Limited
 * Portions Copyright (c) 1996-2014, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/backend/access/transam/csnlog.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/csnlog.h"
#include "access/mvccvars.h"
#include "access/slru.h"
#include "access/subtrans.h"
#include "access/transam.h"
#include "distributed_txn/txn_timestamp.h"
#include "miscadmin.h"
#include "pg_trace.h"
#include "utils/snapmgr.h"
#include "funcapi.h"
#include "utils/timestamp.h"


#ifdef ENABLE_DISTRIBUTED_TRANSACTION
/*GUC parameter */
int			delay_before_set_committing_status = 0;
int			delay_after_set_committing_status = 0;
#endif


/*
 * Defines for CSNLOG page sizes.  A page is the same BLCKSZ as is used
 * everywhere else in Postgres.
 *
 * Note: because TransactionIds are 32 bits and wrap around at 0xFFFFFFFF,
 * CSNLOG page numbering also wraps around at 0xFFFFFFFF/CSNLOG_XACTS_PER_PAGE,
 * and CSNLOG segment numbering at
 * 0xFFFFFFFF/CLOG_XACTS_PER_PAGE/SLRU_PAGES_PER_SEGMENT.  We need take no
 * explicit notice of that fact in this module, except when comparing segment
 * and page numbers in TruncateCSNLOG (see CSNLOGPagePrecedes).
 */

/* We store the commit LSN for each xid */
#define CSNLOG_XACTS_PER_PAGE (BLCKSZ / sizeof(CommitSeqNo))

#define TransactionIdToPage(xid)	((xid) / (TransactionId) CSNLOG_XACTS_PER_PAGE)
#define TransactionIdToPgIndex(xid) ((xid) % (TransactionId) CSNLOG_XACTS_PER_PAGE)

#ifdef ENABLE_DISTRIBUTED_TRANSACTION
/*
 * add WAL for CSNLog to support distributed transactions
 * written by  
 */

/* We store the latest async LSN for each group of transactions */
#define CSNLOG_XACTS_PER_LSN_GROUP	32	/* keep this a power of 2 */
#define CSNLOG_LSNS_PER_PAGE	(CSNLOG_XACTS_PER_PAGE / CSNLOG_XACTS_PER_LSN_GROUP)

#define GetLSNIndex(slotno, xid)	((slotno) * CSNLOG_LSNS_PER_PAGE + \
	((xid) % (TransactionId) CSNLOG_XACTS_PER_PAGE) / CSNLOG_XACTS_PER_LSN_GROUP)

#endif
/* We allocate new log pages in batches */
#ifdef ENABLE_DISTRIBUTED_TRANSACTION
#define BATCH_SIZE 1
#else
#define BATCH_SIZE 128
#endif
/*
 * Link to shared-memory data structures for CLOG control
 */
static SlruCtlData CsnlogCtlData;

#define CsnlogCtl (&CsnlogCtlData)

#ifdef ENABLE_DISTRIBUTED_TRANSACTION
/* local xlog stuff */
static void WriteZeroPageXlogRec(int pageno);
static void WriteTruncateXlogRec(int pageno, TransactionId oldestXact);
static XLogRecPtr WriteSetTimestampXlogRec(TransactionId mainxid, int nsubxids,
						 TransactionId *subxids, CommitSeqNo csn);
#endif

static int	ZeroCSNLOGPage(int pageno, bool writeXlog);
static bool CSNLOGPagePrecedes(int page1, int page2);
static void CSNLogSetPageStatus(TransactionId xid, int nsubxids,
					TransactionId *subxids,
					CommitSeqNo csn, XLogRecPtr lsn, int pageno);
static void CSNLogSetCSN(TransactionId xid, CommitSeqNo csn, XLogRecPtr lsn, int slotno);
static CommitSeqNo InternalGetCommitSeqNo(TransactionId xid);
static CommitSeqNo RecursiveGetCommitSeqNo(TransactionId xid);

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
CSNLogSetCommitSeqNo(TransactionId xid, int nsubxids,
					 TransactionId *subxids, XLogRecPtr lsn, bool write_xlog, CommitSeqNo csn)
{
	int			nextSubxid;
	int			topPage;
	TransactionId topXid;
#ifndef ENABLE_DISTRIBUTED_TRANSACTION
	TransactionId oldestActiveXid = pg_atomic_read_u32(
													   &ShmemVariableCache->oldestActiveXid);
#endif
	XLogRecPtr	max_lsn = lsn;

	if (csn == InvalidCommitSeqNo || xid == BootstrapTransactionId)
	{
		if (IsBootstrapProcessingMode())
			csn = COMMITSEQNO_FROZEN;
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
		max_lsn = WriteSetTimestampXlogRec(xid, nsubxids, subxids, csn);
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

		CSNLogSetPageStatus(topXid, subxidsOnPage, subxids + nextSubxid + 1,
							csn, max_lsn, currentPage);
	}
	while (nextSubxid >= 0);

	if (topXid == InvalidTransactionId)
	{
		/*
		 * No subxids were on the same page as the main xid; we have to update
		 * it separately
		 */

		CSNLogSetPageStatus(xid, 0, NULL, csn, max_lsn, topPage);
	}
}

#ifdef ENABLE_DISTRIBUTED_TRANSACTION
CommitSeqNo
CSNLogAssignCommitSeqNo(TransactionId xid, int nxids, TransactionId *xids, bool fromCoordinator)
{
	CommitSeqNo csn;
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
#ifdef ENABLE_DISTR_DEBUG
		if (delay_before_set_committing_status)
			pg_usleep(delay_before_set_committing_status * 1000);
#endif

		CSNLogSetCommitSeqNo(xid, 0, NULL, InvalidXLogRecPtr, false, COMMITSEQNO_COMMITTING);

#ifdef ENABLE_DISTR_DEBUG
		if (delay_after_set_committing_status)
			pg_usleep(delay_after_set_committing_status * 1000);
#endif
	}

#ifdef ENABLE_DISTRIBUTED_TRANSACTION
	csn = TxnGetOrGenerateCommitTs(fromCoordinator);
	Assert(!COMMITSEQNO_IS_SUBTRANS(csn));

	if (!COMMITSEQNO_IS_NORMAL(csn))
		elog(ERROR, "invalid commit ts " UINT64_FORMAT, csn);

	TxnSetReplyTimestamp(csn);
#else
	/* Get our CSN and increment */
	CSNLogSetCommitSeqNo(xid, 0, NULL, InvalidXLogRecPtr, false, COMMITSEQNO_COMMITTING);
	csn = pg_atomic_fetch_add_u64(&ShmemVariableCache->nextCommitSeqNo, 1);
#endif

	Assert(csn >= COMMITSEQNO_FIRST_NORMAL);
	if (enable_timestamp_debug_print)
		elog(LOG, "xid %d assign commit timestamp " UINT64_FORMAT, xid, csn);
	return csn;
}
#endif
/*
 * Record the final state of transaction entries in the csn log for
 * all entries on a single page.  Atomic only on this page.
 *
 * Otherwise API is same as TransactionIdSetTreeStatus()
 */
static void
CSNLogSetPageStatus(TransactionId xid, int nsubxids,
					TransactionId *subxids,
					CommitSeqNo csn, XLogRecPtr lsn, int pageno)
{
	int			slotno;
	int			i;

	LWLockAcquire(CSNLogControlLock, LW_SHARED);

	slotno = SimpleLruReadPage_ReadOnly_Locked(CsnlogCtl, pageno, lsn, xid);

	/*
	 * We set the status of child transaction before the status of parent
	 * transactions, so that another process can correctly determine the
	 * resulting status of a child transaction. See RecursiveGetCommitSeqNo().
	 */
	for (i = nsubxids - 1; i >= 0; i--)
	{
		Assert(CsnlogCtl->shared->page_number[slotno] == TransactionIdToPage(subxids[i]));
		CSNLogSetCSN(subxids[i], csn, lsn, slotno);
		pg_write_barrier();
	}

	if (TransactionIdIsValid(xid))
		CSNLogSetCSN(xid, csn, lsn, slotno);

	CsnlogCtl->shared->page_dirty[slotno] = true;

	LWLockRelease(CSNLogControlLock);
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
	CommitSeqNo *ptr;
	CommitSeqNo newcsn;

	Assert(TransactionIdIsValid(parent));
	Assert(TransactionIdFollows(xid, parent));

	newcsn = CSN_SUBTRANS_BIT | (uint64) parent;

	/*
	 * Shared page access is enough to set the subtransaction parent. It is
	 * set when the subtransaction is assigned an xid, and can be read only
	 * later, after the subtransaction have modified some tuples.
	 */
	slotno = SimpleLruReadPage_ReadOnly(CsnlogCtl, pageno, xid);
	ptr = (CommitSeqNo *) CsnlogCtl->shared->page_buffer[slotno];
	ptr += entryno;

	/*
	 * It's possible we'll try to set the parent xid multiple times but we
	 * shouldn't ever be changing the xid from one valid xid to another valid
	 * xid, which would corrupt the data structure.
	 */
	if (*ptr != newcsn)
	{
		Assert(*ptr == COMMITSEQNO_INPROGRESS);
		*ptr = newcsn;
		CsnlogCtl->shared->page_dirty[slotno] = true;
	}


	LWLockRelease(CSNLogControlLock);
}

/*
 * Interrogate the parent of a transaction in the csnlog.
 */
TransactionId
SubTransGetParent(TransactionId xid)
{
	CommitSeqNo csn;

	LWLockAcquire(CSNLogControlLock, LW_SHARED);

	csn = InternalGetCommitSeqNo(xid);

	LWLockRelease(CSNLogControlLock);

	if (COMMITSEQNO_IS_SUBTRANS(csn))
		return (TransactionId) (csn & 0xFFFFFFFF);
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
CSNLogSetCSN(TransactionId xid, CommitSeqNo csn, XLogRecPtr lsn, int slotno)
{
	int			entryno = TransactionIdToPgIndex(xid);
	CommitSeqNo *ptr;

	ptr = (CommitSeqNo *) (CsnlogCtl->shared->page_buffer[slotno] + entryno * sizeof(XLogRecPtr));

	/*
	 * Current state change should be from 0 to target state. (Allow setting
	 * it again to same value.)
	 */
	Assert(COMMITSEQNO_IS_INPROGRESS(*ptr) ||
		   COMMITSEQNO_IS_COMMITTING(*ptr) ||
		   COMMITSEQNO_IS_SUBTRANS(*ptr) ||
		   COMMITSEQNO_IS_PREPARED(*ptr) ||
		   *ptr == csn);

	*ptr = csn;

#ifdef ENABLE_DISTRIBUTED_TRANSACTION

	/*
	 * Update the group LSN if the transaction completion LSN is higher.
	 *
	 * Note: lsn will be invalid when supplied during InRecovery processing,
	 * so we don't need to do anything special to avoid LSN updates during
	 * recovery. After recovery completes the next csnlog change will set the
	 * LSN correctly.
	 */
	if (!XLogRecPtrIsInvalid(lsn))
	{
		int			lsnindex = GetLSNIndex(slotno, xid);

		/*
		 * As csnlog only holds shared control lock, we should use a seperate
		 * spin lock to protect group lsns from concurrent updates. Read
		 * access in SlruPhysicalWritePage would be protected by
		 * CSNLogControlLock. It is safe enough to use group_lsn_lock for
		 * update only.
		 */

		if (CsnlogCtl->shared->group_lsn[lsnindex] < lsn)
			CsnlogCtl->shared->group_lsn[lsnindex] = lsn;
	}
#endif
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
CommitSeqNo
CSNLogGetCommitSeqNo(TransactionId xid)
{
	CommitSeqNo csn;

	LWLockAcquire(CSNLogControlLock, LW_SHARED);

	csn = RecursiveGetCommitSeqNo(xid);

	LWLockRelease(CSNLogControlLock);

	/*
	 * As the cts status of crashed transactions may not be set, it would be
	 * regarding as in-progress by mistaken. Note that
	 * TransactionIdIsInProgress() is removed by CSN to avoid proc array
	 * walking. As a result, we need to perform further checking: If the xid
	 * is below TransactionXmin and does not have cts, it should be crashed or
	 * aborted transaction. Written by  , 2020.06.22
	 */

	if (TransactionIdPrecedes(xid, CsnlogCtl->shared->oldestActiveStartupXid))
	{
		if (!COMMITSEQNO_IS_COMMITTED(csn))
			csn = COMMITSEQNO_ABORTED;
	}

	return csn;
}

/* Determine the CSN of a transaction, walking the subtransaction tree if needed */
static CommitSeqNo
RecursiveGetCommitSeqNo(TransactionId xid)
{
	CommitSeqNo csn;

	csn = InternalGetCommitSeqNo(xid);

	if (COMMITSEQNO_IS_SUBTRANS(csn))
	{
		TransactionId parentXid = csn & ~CSN_SUBTRANS_BIT;
		CommitSeqNo parentCsn = RecursiveGetCommitSeqNo(parentXid);

		Assert(!COMMITSEQNO_IS_SUBTRANS(parentCsn));

		/*
		 * The parent and child transaction status update is not atomic. We
		 * must take care not to use the updated parent status with the old
		 * child status, or else we can wrongly see a committed subtransaction
		 * as aborted. This happens when the parent is already marked as
		 * committed and the child is not yet marked.
		 */
		pg_read_barrier();

		csn = InternalGetCommitSeqNo(xid);

		if (COMMITSEQNO_IS_SUBTRANS(csn))
		{
			if (COMMITSEQNO_IS_ABORTED(parentCsn)
				|| COMMITSEQNO_IS_COMMITTED(parentCsn))
			{
				csn = COMMITSEQNO_ABORTED;
			}
			else if (COMMITSEQNO_IS_INPROGRESS(parentCsn))
				csn = COMMITSEQNO_INPROGRESS;
			else if (COMMITSEQNO_IS_COMMITTING(parentCsn))
				csn = COMMITSEQNO_COMMITTING;
			else
				Assert(false);
		}
	}

	return csn;
}

/*
 * Get the raw CSN value.
 */
static CommitSeqNo
InternalGetCommitSeqNo(TransactionId xid)
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

	slotno = SimpleLruReadPage_ReadOnly_Locked(CsnlogCtl, pageno, InvalidXLogRecPtr, xid);
	return *(CommitSeqNo *) (CsnlogCtl->shared->page_buffer[slotno]
							 + entryno * sizeof(XLogRecPtr));
}

/*
 * Find the next xid that is in progress.
 * We do not care about the subtransactions, they are accounted for
 * by their respective top-level transactions.
 */
TransactionId
CSNLogGetNextActiveXid(TransactionId xid,
					   TransactionId end)
{
	Assert(TransactionIdIsValid(TransactionXmin));

	LWLockAcquire(CSNLogControlLock, LW_SHARED);

	for (;;)
	{
		int			pageno;
		int			slotno;
		int			entryno;

		if (!TransactionIdPrecedes(xid, end))
			goto end;

		pageno = TransactionIdToPage(xid);
		slotno = SimpleLruReadPage_ReadOnly_Locked(CsnlogCtl, pageno, InvalidXLogRecPtr, xid);

		for (entryno = TransactionIdToPgIndex(xid); entryno < CSNLOG_XACTS_PER_PAGE;
			 entryno++)
		{
			CommitSeqNo csn;

			if (!TransactionIdPrecedes(xid, end))
				goto end;

			csn = *(XLogRecPtr *) (CsnlogCtl->shared->page_buffer[slotno] + entryno * sizeof(XLogRecPtr));

			if (COMMITSEQNO_IS_INPROGRESS(csn)
				|| COMMITSEQNO_IS_COMMITTING(csn))
			{
				goto end;
			}

			TransactionIdAdvance(xid);
		}
	}

end:
	LWLockRelease(CSNLogControlLock);

	return xid;
}

/*
 * Number of shared CSNLOG buffers.
 */
Size
CSNLOGShmemBuffers(void)
{
	return Min(81920, Max(BATCH_SIZE, NBuffers / 512));
}

/*
 * Initialization of shared memory for CSNLOG
 */
Size
CSNLOGShmemSize(void)
{
	return SimpleLruShmemSize(CSNLOGShmemBuffers(), CSNLOG_LSNS_PER_PAGE);
}

void
CSNLOGShmemInit(void)
{
	CsnlogCtl->PagePrecedes = CSNLOGPagePrecedes;
	SimpleLruInit(CsnlogCtl, "CSNLOG Ctl", CSNLOGShmemBuffers(), CSNLOG_LSNS_PER_PAGE,
				  CSNLogControlLock, "pg_csnlog", LWTRANCHE_CSNLOG_BUFFERS);
}

/*
 * This func must be called ONCE on system install.  It creates
 * the initial CSNLOG segment.  (The pg_csnlog directory is assumed to
 * have been created by initdb, and CSNLOGShmemInit must have been
 * called already.)
 */
void
BootStrapCSNLOG(void)
{
	int			slotno;

	LWLockAcquire(CSNLogControlLock, LW_EXCLUSIVE);

	/* Create and zero the first page of the commit log */
	slotno = ZeroCSNLOGPage(0, false);

	/* Make sure it's written out */
	SimpleLruWritePage(CsnlogCtl, slotno);
	Assert(!CsnlogCtl->shared->page_dirty[slotno]);

	LWLockRelease(CSNLogControlLock);
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
ZeroCSNLOGPage(int pageno, bool writeXlog)
{
	int			slotno;

	slotno = SimpleLruZeroPage(CsnlogCtl, pageno);

#ifdef ENABLE_DISTRIBUTED_TRANSACTION
	if (writeXlog)
		WriteZeroPageXlogRec(pageno);
#endif

	return slotno;
}

/*
 * This must be called ONCE during postmaster or standalone-backend startup,
 * after StartupXLOG has initialized ShmemVariableCache->nextXid.
 *
 * oldestActiveXID is the oldest XID of any prepared transaction, or nextXid
 * if there are none.
 */
void
StartupCSNLOG(TransactionId oldestActiveXID)
{

#ifdef ENABLE_DISTRIBUTED_TRANSACTION
	TransactionId xid = ShmemVariableCache->nextXid;
	int			pageno = TransactionIdToPage(xid);

	LWLockAcquire(CSNLogControlLock, LW_EXCLUSIVE);

	/*
	 * Initialize our idea of the latest page number.
	 */
	CsnlogCtl->shared->latest_page_number = pageno;

	CsnlogCtl->shared->oldestActiveStartupXid = oldestActiveXID;

	LWLockRelease(CSNLogControlLock);


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
ShutdownCSNLOG(void)
{
	/*
	 * Flush dirty CLOG pages to disk
	 *
	 * This is not actually necessary from a correctness point of view. We do
	 * it merely as a debugging aid.
	 */
	TRACE_POSTGRESQL_CSNLOG_CHECKPOINT_START(false);
	SimpleLruFlush(CsnlogCtl, false);

#ifdef ENABLE_DISTRIBUTED_TRANSACTION

	/*
	 * fsync pg_csnlog to ensure that any files flushed previously are durably
	 * on disk.
	 */
	fsync_fname("pg_csnlog", true);
#endif
	TRACE_POSTGRESQL_CSNLOG_CHECKPOINT_DONE(false);
}

/*
 * This must be called ONCE at the end of startup/recovery.
 */
void
TrimCSNLOG(void)
{
	TransactionId xid = ShmemVariableCache->nextXid;
	int			pageno = TransactionIdToPage(xid);

	LWLockAcquire(CSNLogControlLock, LW_EXCLUSIVE);

	/*
	 * Re-Initialize our idea of the latest page number.
	 */
	CsnlogCtl->shared->latest_page_number = pageno;

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

		slotno = SimpleLruReadPage(CsnlogCtl, pageno, false, xid);

		byteptr = CsnlogCtl->shared->page_buffer[slotno] + byteno;

		/* Zero the rest of the page */
		MemSet(byteptr, 0, BLCKSZ - byteno);
		elog(LOG, "Trim csnlog start from %d size %d next xid %u", byteno, BLCKSZ - byteno, xid);

		CsnlogCtl->shared->page_dirty[slotno] = true;
	}

	LWLockRelease(CSNLogControlLock);
}

/*
 * Perform a checkpoint --- either during shutdown, or on-the-fly
 */
void
CheckPointCSNLOG(void)
{
	/*
	 * Flush dirty CLOG pages to disk
	 *
	 * This is not actually necessary from a correctness point of view. We do
	 * it merely to improve the odds that writing of dirty pages is done by
	 * the checkpoint process and not by backends.
	 */
	TRACE_POSTGRESQL_CSNLOG_CHECKPOINT_START(true);
	SimpleLruFlush(CsnlogCtl, true);
#ifdef ENABLE_DISTRIBUTED_TRANSACTION

	/*
	 * fsync pg_csnlog to ensure that any files flushed previously are durably
	 * on disk.
	 */
	fsync_fname("pg_csnlog", true);
#endif
	TRACE_POSTGRESQL_CSNLOG_CHECKPOINT_DONE(true);
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
ExtendCSNLOG(TransactionId newestXact)
{
	int			i;
	int			pageno;

	/*
	 * No work except at first XID of a page.  But beware: just after
	 * wraparound, the first XID of page zero is FirstNormalTransactionId.
	 */
	if (TransactionIdToPgIndex(newestXact) != 0 &&
		!TransactionIdEquals(newestXact, FirstNormalTransactionId))
		return;

	pageno = TransactionIdToPage(newestXact);

	if (pageno % BATCH_SIZE)
		return;
	LWLockAcquire(CSNLogControlLock, LW_EXCLUSIVE);

#ifdef ENABLE_DISTRIBUTED_TRANSACTION
	/* Zero the page and make an XLOG entry about it */
	for (i = pageno; i < pageno + BATCH_SIZE; i++)
		ZeroCSNLOGPage(i, true);
#endif

	LWLockRelease(CSNLogControlLock);
}


/*
 * Remove all CSNLOG segments before the one holding the passed transaction ID
 *
 * This is normally called during checkpoint, with oldestXact being the
 * oldest TransactionXmin of any running transaction.
 */
void
TruncateCSNLOG(TransactionId oldestXact)
{
	int			cutoffPage;

	/*
	 * The cutoff point is the start of the segment containing oldestXact. We
	 * pass the *page* containing oldestXact to SimpleLruTruncate.
	 */
	cutoffPage = TransactionIdToPage(oldestXact);

	/* Check to see if there's any files that could be removed */
	if (!SlruScanDirectory(CsnlogCtl, SlruScanDirCbReportPresence, &cutoffPage))
		return;					/* nothing to remove */

	/*
	 * Write XLOG record and flush XLOG to disk. We record the oldest xid
	 * we're keeping information about here so we can ensure that it's always
	 * ahead of clog truncation in case we crash, and so a standby finds out
	 * the new valid xid before the next checkpoint.
	 */
#ifdef ENABLE_DISTRIBUTED_TRANSACTION
	WriteTruncateXlogRec(cutoffPage, oldestXact);
#endif

	SimpleLruTruncate(CsnlogCtl, cutoffPage);
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
CSNLOGPagePrecedes(int page1, int page2)
{
	TransactionId xid1;
	TransactionId xid2;

	xid1 = ((TransactionId) page1) * CSNLOG_XACTS_PER_PAGE;
	xid1 += FirstNormalTransactionId;
	xid2 = ((TransactionId) page2) * CSNLOG_XACTS_PER_PAGE;
	xid2 += FirstNormalTransactionId;

	return TransactionIdPrecedes(xid1, xid2);
}

#ifdef ENABLE_DISTRIBUTED_TRANSACTION


PG_FUNCTION_INFO_V1(pg_xact_get_csn);
/*
 * function api to get csn for given xid
 */
Datum
pg_xact_get_csn(PG_FUNCTION_ARGS)
{
	TransactionId xid = PG_GETARG_UINT32(0);
	CommitSeqNo csn;
	StringInfoData str;

	initStringInfo(&str);

	csn = CSNLogGetCommitSeqNo(xid);

	appendStringInfo(&str, "csn: " UINT64_FORMAT, csn);

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
	(void) XLogInsert(RM_CSNLOG_ID, CSNLOG_ZEROPAGE);
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
	xl_csnlog_truncate xlrec;

	xlrec.pageno = pageno;
	xlrec.oldestXact = oldestXact;

	XLogBeginInsert();
	XLogRegisterData((char *) (&xlrec), sizeof(xl_csnlog_truncate));
	recptr = XLogInsert(RM_CSNLOG_ID, CSNLOG_TRUNCATE);
	XLogFlush(recptr);
}

/*
 * Write a SETCSN xlog record
 */
static XLogRecPtr
WriteSetTimestampXlogRec(TransactionId mainxid, int nsubxids,
						 TransactionId *subxids, CommitSeqNo csn)
{
	xl_csn_set	record;

	record.csn = csn;
	record.mainxid = mainxid;

	XLogBeginInsert();
	XLogRegisterData((char *) &record,
					 SizeOfCsnSet);
	XLogRegisterData((char *) subxids, nsubxids * sizeof(TransactionId));
	return XLogInsert(RM_CSNLOG_ID, CSNLOG_SETCSN);
}


/*
 * CSNLOG resource manager's routines
 */
void
csnlog_redo(XLogReaderState *record)
{
	uint8		info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

	/* Backup blocks are not used in clog records */
	Assert(!XLogRecHasAnyBlockRefs(record));

	if (info == CSNLOG_ZEROPAGE)
	{
		int			pageno;
		int			slotno;

		memcpy(&pageno, XLogRecGetData(record), sizeof(int));

		LWLockAcquire(CSNLogControlLock, LW_EXCLUSIVE);

		slotno = ZeroCSNLOGPage(pageno, false);
		SimpleLruWritePage(CsnlogCtl, slotno);
		Assert(!CsnlogCtl->shared->page_dirty[slotno]);

		LWLockRelease(CSNLogControlLock);
	}
	else if (info == CSNLOG_TRUNCATE)
	{
		xl_csnlog_truncate xlrec;

		memcpy(&xlrec, XLogRecGetData(record), sizeof(xl_csnlog_truncate));

		/*
		 * During XLOG replay, latest_page_number isn't set up yet; insert a
		 * suitable value to bypass the sanity test in SimpleLruTruncate.
		 */
		CsnlogCtl->shared->latest_page_number = xlrec.pageno;

		SimpleLruTruncate(CsnlogCtl, xlrec.pageno);
	}
	else if (info == CSNLOG_SETCSN)
	{
		xl_csn_set *setcsn = (xl_csn_set *) XLogRecGetData(record);
		int			nsubxids;
		TransactionId *subxids;

		nsubxids = ((XLogRecGetDataLen(record) - SizeOfCsnSet) /
					sizeof(TransactionId));

		if (nsubxids > 0)
		{
			subxids = palloc(sizeof(TransactionId) * nsubxids);
			memcpy(subxids,
				   XLogRecGetData(record) + SizeOfCsnSet,
				   sizeof(TransactionId) * nsubxids);
		}
		else
			subxids = NULL;

		CSNLogSetCommitSeqNo(setcsn->mainxid, nsubxids, subxids, InvalidXLogRecPtr, false, setcsn->csn);
		elog(DEBUG1, "csnlog_redo: set xid %d csn " INT64_FORMAT, setcsn->mainxid, setcsn->csn);
		if (subxids)
			pfree(subxids);
	}
	else
		elog(PANIC, "csnlog_redo: unknown op code %u", info);
}
#endif
