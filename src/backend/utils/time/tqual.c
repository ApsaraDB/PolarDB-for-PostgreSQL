/*-------------------------------------------------------------------------
 *
 * tqual.c
 *	  POSTGRES "time qualification" code, ie, tuple visibility rules.
 *
 * NOTE: all the HeapTupleSatisfies routines will update the tuple's
 * "hint" status bits if we see that the inserting or deleting transaction
 * has now committed or aborted (and it is safe to set the hint bits).
 * If the hint bits are changed, MarkBufferDirtyHint is called on
 * the passed-in buffer.  The caller must hold not only a pin, but at least
 * shared buffer content lock on the buffer containing the tuple.
 *
 * Summary of visibility functions:
 *
 *	 HeapTupleSatisfiesMVCC()
 *		  visible to supplied snapshot, excludes current command
 *	 HeapTupleSatisfiesUpdate()
 *		  visible to instant snapshot, with user-supplied command
 *		  counter and more complex result
 *	 HeapTupleSatisfiesSelf()
 *		  visible to instant snapshot and current command
 *	 HeapTupleSatisfiesDirty()
 *		  like HeapTupleSatisfiesSelf(), but includes open transactions
 *	 HeapTupleSatisfiesVacuum()
 *		  visible to any running transaction, used by VACUUM
 *	 HeapTupleSatisfiesNonVacuumable()
 *		  Snapshot-style API for HeapTupleSatisfiesVacuum
 *	 HeapTupleSatisfiesToast()
 *		  visible unless part of interrupted vacuum, used for TOAST
 *	 HeapTupleSatisfiesAny()
 *		  all tuples are visible
 *
 *   ----------------------------------------------------------------------------------
 * 	 We design a timestamp based protocol to generate consistent distributed 
 *   snapshot across a cluster to provide read commited/repeatable read level 
 *   isolation between distributed transactions.
 * 
 *   Main functions on MVCC visibility are as follows:
 *   XidVisibleInSnapshotDistri()
 *   XminVisibleInSnapshotByTimestamp()
 *   XmaxVisibleInSnapshotByTimestamp()
 *   HeapTupleSatisfiesMVCC()
 *   SetHintBits()
 *   
 *   We use CTS log to store commit timestamps and prepare states for distributed
 *   transactions and as each access to CTS log is cost we cache the commit ts to 
 *   speed up subsequent access in the tuple header when it is read from the CTS log.
 *   We add WAL for CTS log to perserve data across crashes so as to support recovery. 
 *   For more details on CTS log, please see ctslog.c
 * 
 *   Author:  , 2020-01-15
 *   ------------------------------------------------------------------------------------
 *
 * Portions Copyright (c) 2020, Alibaba Group Holding Limited
 * Portions Copyright (C) 2019 THL A29 Limited, a Tencent company.  All rights reserved.
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/utils/time/tqual.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/htup_details.h"
#include "access/multixact.h"
#include "access/subtrans.h"
#include "access/xact.h"
#include "access/xlog.h"
#include "storage/bufmgr.h"
#include "storage/procarray.h"
#include "utils/builtins.h"
#include "utils/combocid.h"
#include "utils/snapmgr.h"
#include "utils/tqual.h"
#include "storage/lmgr.h"
#include "access/ctslog.h"
#include "access/clog.h"
#include "storage/buf_internals.h"
#ifdef POLARDB_X
#include "miscadmin.h"
#include "pgxc/transam/txn_coordinator.h"
#endif


/* Static variables representing various special snapshot semantics */
SnapshotData SnapshotSelfData = {HeapTupleSatisfiesSelf};
SnapshotData SnapshotAnyData = {HeapTupleSatisfiesAny};

/* local functions */
#ifndef ENABLE_DISTRIBUTED_TRANSACTION
static bool CommittedXidVisibleInSnapshot(TransactionId xid, Snapshot snapshot);
#endif
static bool IsMovedTupleVisible(HeapTuple htup, Buffer buffer);

#ifdef ENABLE_DISTRIBUTED_TRANSACTION
/* guc parameter */
bool enable_distri_visibility_print = false;
bool enable_distri_print = false;
bool enable_global_cutoffts = false;

/* local functions for distributed transaction implemetation */
static bool
XidVisibleInSnapshotDistri(HeapTupleHeader tuple, 
							TransactionId xid, 
							Snapshot snapshot, 
							Buffer buffer, 
                            TransactionIdStatus *hintstatus,
							bool *retry);
static bool
XminVisibleInSnapshotByTimestamp(HeapTupleHeader tuple, 
								Snapshot snapshot, 
								Buffer buffer, 
                                bool *retry);
static bool
XmaxVisibleInSnapshotByTimestamp(HeapTupleHeader tuple, 
								Snapshot snapshot, 
								Buffer buffer, 
                                bool *retry);


CommitSeqNo HeapTupleHderGetXminTimestampAtomic(HeapTupleHeader tuple)
{
    if (HEAP_XMIN_TIMESTAMP_IS_UPDATED(tuple->t_infomask2))
        return HeapTupleHeaderGetXminTimestamp(tuple);
    else
        return InvalidCommitSeqNo;

}

CommitSeqNo HeapTupleHderGetXmaxTimestampAtomic(HeapTupleHeader tuple)
{
    if (HEAP_XMAX_TIMESTAMP_IS_UPDATED(tuple->t_infomask2))
        return HeapTupleHeaderGetXmaxTimestamp(tuple);
    else
        return InvalidCommitSeqNo;

}

void HeapTupleHderSetXminTimestampAtomic(HeapTupleHeader tuple, CommitSeqNo committs)
{
    HeapTupleHeaderSetXminTimestamp(tuple, committs);
	/* Add memory barrier to prevent CPU out of order execution */
	pg_write_barrier();
    tuple->t_infomask2 |= HEAP_XMIN_TIMESTAMP_UPDATED;
}

void HeapTupleHderSetXmaxTimestampAtomic(HeapTupleHeader tuple, CommitSeqNo committs)
{
    HeapTupleHeaderSetXmaxTimestamp(tuple, committs);
	/* Add memory barrier to prevent CPU out of order execution */
	pg_write_barrier();
    tuple->t_infomask2 |= HEAP_XMAX_TIMESTAMP_UPDATED;
}
#endif

/*
 * SetHintBits()
 *
 * Set commit/abort hint bits on a tuple, if appropriate at this time.
 *
 * It is only safe to set a transaction-committed hint bit if we know the
 * transaction's commit record is guaranteed to be flushed to disk before the
 * buffer, or if the table is temporary or unlogged and will be obliterated by
 * a crash anyway.  We cannot change the LSN of the page here, because we may
 * hold only a share lock on the buffer, so we can only use the LSN to
 * interlock this if the buffer's LSN already is newer than the commit LSN;
 * otherwise we have to just refrain from setting the hint bit until some
 * future re-examination of the tuple.
 *
 * We can always set hint bits when marking a transaction aborted.  (Some
 * code in heapam.c relies on that!)
 *
 * Also, if we are cleaning up HEAP_MOVED_IN or HEAP_MOVED_OFF entries, then
 * we can always set the hint bits, since pre-9.0 VACUUM FULL always used
 * synchronous commits and didn't move tuples that weren't previously
 * hinted.  (This is not known by this subroutine, but is applied by its
 * callers.)  Note: old-style VACUUM FULL is gone, but we have to keep this
 * module's support for MOVED_OFF/MOVED_IN flag bits for as long as we
 * support in-place update from pre-9.0 databases.
 *
 * Normal commits may be asynchronous, so for those we need to get the LSN
 * of the transaction and then check whether this is flushed.
 *
 * The caller should pass xid as the XID of the transaction to check, or
 * InvalidTransactionId if no check is needed.
 */
static inline void
SetHintBits(HeapTupleHeader tuple, Buffer buffer,
			uint16 infomask, TransactionId xid)
{
#ifdef ENABLE_DISTRIBUTED_TRANSACTION
    TransactionId xmin;
    TransactionId xmax;
    CommitSeqNo 	committs;
#endif

	if (TransactionIdIsValid(xid))
	{
		/* NB: xid must be known committed here! */
		XLogRecPtr		commitLSN = TransactionIdGetCommitLSN(xid);

		if (BufferIsPermanent(buffer) && XLogNeedsFlush(commitLSN) &&
			BufferGetLSNAtomic(buffer) < commitLSN)
		{
			/* not flushed and no LSN interlock, so don't set hint */
			return;
		}
	}

#ifdef ENABLE_DISTRIBUTED_TRANSACTION
	/*
	 * We cache commit timestamp in tuple header to speed up
	 * visibility validation. See XminVisibleInSnapshotByTimestamp()
	 * and XmaxVisibleInSnapshotByTimestamp().
	 */ 
    if (infomask & HEAP_XMIN_COMMITTED)
    {
        if (!COMMITSEQNO_IS_COMMITTED(HeapTupleHderGetXminTimestampAtomic(tuple)))
        {
            xmin = HeapTupleHeaderGetRawXmin(tuple);

			committs = CTSLogGetCommitTs(xmin);
			if (COMMITSEQNO_IS_COMMITTED(committs))
				HeapTupleHderSetXminTimestampAtomic(tuple, committs);
			else
				elog(PANIC, "xmin %d should have commit ts but not "UINT64_FORMAT, 
									xmin, committs);
        }
    }
    
	if (infomask & HEAP_XMAX_COMMITTED)
    {
        if (!COMMITSEQNO_IS_COMMITTED(HeapTupleHderGetXmaxTimestampAtomic(tuple)))
        {
			Page		page = BufferGetPage(buffer);

            xmax = HeapTupleHeaderGetRawXmax(tuple);
			committs = CTSLogGetCommitTs(xmax);
			if (COMMITSEQNO_IS_COMMITTED(committs))
				HeapTupleHderSetXmaxTimestampAtomic(tuple, committs);
			else
				elog(PANIC, "xmax %d should have commit ts but not commit ts "UINT64_FORMAT, 
									xmax, committs);
			/* set page prunable hint */
			PageSetPrunableTs(page, committs);
        }
    }
#endif

	tuple->t_infomask |= infomask;
	MarkBufferDirtyHint(buffer, true);
}

/*
 * HeapTupleSetHintBits --- exported version of SetHintBits()
 *
 * This must be separate because of C99's brain-dead notions about how to
 * implement inline functions.
 */
void
HeapTupleSetHintBits(HeapTupleHeader tuple, Buffer buffer,
					 uint16 infomask, TransactionId xid)
{
	SetHintBits(tuple, buffer, infomask, xid);
}


/*
 * HeapTupleSatisfiesSelf
 *		True iff heap tuple is valid "for itself".
 *
 *	Here, we consider the effects of:
 *		all committed transactions (as of the current instant)
 *		previous commands of this transaction
 *		changes made by the current command
 *
 * Note:
 *		Assumes heap tuple is valid.
 *
 * The satisfaction of "itself" requires the following:
 *
 * ((Xmin == my-transaction &&				the row was updated by the current transaction, and
 *		(Xmax is null						it was not deleted
 *		 [|| Xmax != my-transaction)])			[or it was deleted by another transaction]
 * ||
 *
 * (Xmin is committed &&					the row was modified by a committed transaction, and
 *		(Xmax is null ||					the row has not been deleted, or
 *			(Xmax != my-transaction &&			the row was deleted by another transaction
 *			 Xmax is not committed)))			that has not been committed
 */
bool
HeapTupleSatisfiesSelf(HeapTuple htup, Snapshot snapshot, Buffer buffer)
{
	HeapTupleHeader tuple = htup->t_data;
	bool		visible;
	TransactionIdStatus	hintstatus;
#ifdef ENABLE_DISTRIBUTED_TRANSACTION
	bool		retry;

l1:
	retry = false;
#endif

	Assert(ItemPointerIsValid(&htup->t_self));
	Assert(htup->t_tableOid != InvalidOid);

	if (!HeapTupleHeaderXminCommitted(tuple))
	{
		if (HeapTupleHeaderXminInvalid(tuple))
			return false;

		/* Used by pre-9.0 binary upgrades */
		if (tuple->t_infomask & HEAP_MOVED)
			return IsMovedTupleVisible(htup, buffer);

		if (TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetRawXmin(tuple)))
		{
			if (tuple->t_infomask & HEAP_XMAX_INVALID)	/* xid invalid */
				return true;

			if (HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask))	/* not deleter */
				return true;

			if (tuple->t_infomask & HEAP_XMAX_IS_MULTI)
			{
				TransactionId xmax;

				xmax = HeapTupleGetUpdateXid(tuple);

				/* not LOCKED_ONLY, so it has to have an xmax */
				Assert(TransactionIdIsValid(xmax));

				/* updating subtransaction must have aborted */
				if (!TransactionIdIsCurrentTransactionId(xmax))
					return true;
				else
					return false;
			}

			if (!TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetRawXmax(tuple)))
			{
				/* deleting subtransaction must have aborted */
				SetHintBits(tuple, buffer, HEAP_XMAX_INVALID,
							InvalidTransactionId);
				return true;
			}

			return false;
		}
		else
		{
			#ifdef ENABLE_DISTRIBUTED_TRANSACTION
			visible = XidVisibleInSnapshotDistri(tuple, HeapTupleHeaderGetRawXmin(tuple),
														snapshot, buffer, &hintstatus, &retry);
			if (retry)
				goto l1;
			#else
			visible = XidVisibleInSnapshot(HeapTupleHeaderGetRawXmin(tuple), snapshot, &hintstatus);
			#endif
			if (hintstatus == XID_COMMITTED)
				SetHintBits(tuple, buffer, HEAP_XMIN_COMMITTED,
							HeapTupleHeaderGetRawXmin(tuple));
			if (hintstatus == XID_ABORTED)
				SetHintBits(tuple, buffer, HEAP_XMIN_INVALID,
							InvalidTransactionId);
			if (!visible)
				return false;
		}
	}

	/* by here, the inserting transaction has committed */

	if (tuple->t_infomask & HEAP_XMAX_INVALID)	/* xid invalid or aborted */
		return true;

	if (tuple->t_infomask & HEAP_XMAX_COMMITTED)
	{
		if (HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask))
			return true;
		return false;			/* updated by other */
	}

	if (tuple->t_infomask & HEAP_XMAX_IS_MULTI)
	{
		TransactionId xmax;

		if (HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask))
			return true;

		xmax = HeapTupleGetUpdateXid(tuple);

		/* not LOCKED_ONLY, so it has to have an xmax */
		Assert(TransactionIdIsValid(xmax));

		if (TransactionIdIsCurrentTransactionId(xmax))
			return false;

		#ifdef ENABLE_DISTRIBUTED_TRANSACTION
		visible = XidVisibleInSnapshotDistri(tuple, xmax,
													snapshot, buffer, &hintstatus, &retry);
		if (retry)
			goto l1;
		#else
		visible = XidVisibleInSnapshot(xmax, snapshot, &hintstatus);
		#endif
		if (!visible)
		{
			/* it must have aborted or crashed */
			return true;
		}
	}

	if (TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetRawXmax(tuple)))
	{
		if (HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask))
			return true;
		return false;
	}

	#ifdef ENABLE_DISTRIBUTED_TRANSACTION
	visible = XidVisibleInSnapshotDistri(tuple, HeapTupleHeaderGetRawXmax(tuple),
												snapshot, buffer, &hintstatus, &retry);
	if (retry)
		goto l1;
	#else
	visible = XidVisibleInSnapshot(HeapTupleHeaderGetRawXmax(tuple), snapshot, &hintstatus);
	#endif
	if (hintstatus == XID_ABORTED)
	{
		/* it must have aborted or crashed */
		SetHintBits(tuple, buffer, HEAP_XMAX_INVALID,
					InvalidTransactionId);
	}
	if (!visible)
		return true;

	/* xmax transaction committed */

	if (HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask))
	{
		SetHintBits(tuple, buffer, HEAP_XMAX_INVALID,
					InvalidTransactionId);
		return true;
	}

	SetHintBits(tuple, buffer, HEAP_XMAX_COMMITTED,
				HeapTupleHeaderGetRawXmax(tuple));
	return false;
}

/*
 * HeapTupleSatisfiesAny
 *		Dummy "satisfies" routine: any tuple satisfies SnapshotAny.
 */
bool
HeapTupleSatisfiesAny(HeapTuple htup, Snapshot snapshot, Buffer buffer)
{
	return true;
}

/*
 * HeapTupleSatisfiesToast
 *		True iff heap tuple is valid as a TOAST row.
 *
 * This is a simplified version that only checks for VACUUM moving conditions.
 * It's appropriate for TOAST usage because TOAST really doesn't want to do
 * its own time qual checks; if you can see the main table row that contains
 * a TOAST reference, you should be able to see the TOASTed value.  However,
 * vacuuming a TOAST table is independent of the main table, and in case such
 * a vacuum fails partway through, we'd better do this much checking.
 *
 * Among other things, this means you can't do UPDATEs of rows in a TOAST
 * table.
 */
bool
HeapTupleSatisfiesToast(HeapTuple htup, Snapshot snapshot,
						Buffer buffer)
{
	HeapTupleHeader tuple = htup->t_data;

	Assert(ItemPointerIsValid(&htup->t_self));
	Assert(htup->t_tableOid != InvalidOid);

	if (!HeapTupleHeaderXminCommitted(tuple))
	{
		if (HeapTupleHeaderXminInvalid(tuple))
			return false;

		/* Used by pre-9.0 binary upgrades */
		if (tuple->t_infomask & HEAP_MOVED)
			return IsMovedTupleVisible(htup, buffer);

		/*
		 * An invalid Xmin can be left behind by a speculative insertion that
		 * is canceled by super-deleting the tuple.  This also applies to
		 * TOAST tuples created during speculative insertion.
		 */
		if (!TransactionIdIsValid(HeapTupleHeaderGetXmin(tuple)))
			return false;
	}

	/* otherwise assume the tuple is valid for TOAST. */
	return true;
}

/*
 * HeapTupleSatisfiesUpdate
 *
 *	This function returns a more detailed result code than most of the
 *	functions in this file, since UPDATE needs to know more than "is it
 *	visible?".  It also allows for user-supplied CommandId rather than
 *	relying on CurrentCommandId.
 *
 *	The possible return codes are:
 *
 *	HeapTupleInvisible: the tuple didn't exist at all when the scan started,
 *	e.g. it was created by a later CommandId.
 *
 *	HeapTupleMayBeUpdated: The tuple is valid and visible, so it may be
 *	updated.
 *
 *	HeapTupleSelfUpdated: The tuple was updated by the current transaction,
 *	after the current scan started.
 *
 *	HeapTupleUpdated: The tuple was updated by a committed transaction.
 *
 *	HeapTupleBeingUpdated: The tuple is being updated by an in-progress
 *	transaction other than the current transaction.  (Note: this includes
 *	the case where the tuple is share-locked by a MultiXact, even if the
 *	MultiXact includes the current transaction.  Callers that want to
 *	distinguish that case must test for it themselves.)
 */
HTSU_Result
HeapTupleSatisfiesUpdate(HeapTuple htup, CommandId curcid,
						 Buffer buffer)
{
	HeapTupleHeader tuple = htup->t_data;
	TransactionIdStatus	xidstatus;

	Assert(ItemPointerIsValid(&htup->t_self));
	Assert(htup->t_tableOid != InvalidOid);

	if (!HeapTupleHeaderXminCommitted(tuple))
	{
		if (HeapTupleHeaderXminInvalid(tuple))
			return HeapTupleInvisible;

		/* Used by pre-9.0 binary upgrades */
		if (tuple->t_infomask & HEAP_MOVED)
		{
			if (IsMovedTupleVisible(htup, buffer))
				return HeapTupleMayBeUpdated;
			else
				return HeapTupleInvisible;
		}

		if (TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetRawXmin(tuple)))
		{
			if (HeapTupleHeaderGetCmin(tuple) >= curcid)
				return HeapTupleInvisible;	/* inserted after scan started */

			if (tuple->t_infomask & HEAP_XMAX_INVALID)	/* xid invalid */
				return HeapTupleMayBeUpdated;

			if (HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask))
			{
				TransactionId xmax;

				xmax = HeapTupleHeaderGetRawXmax(tuple);

				/*
				 * Careful here: even though this tuple was created by our own
				 * transaction, it might be locked by other transactions, if
				 * the original version was key-share locked when we updated
				 * it.
				 */

				if (tuple->t_infomask & HEAP_XMAX_IS_MULTI)
				{
					if (MultiXactIdIsRunning(xmax, true))
						return HeapTupleBeingUpdated;
					else
						return HeapTupleMayBeUpdated;
				}

				/*
				 * If the locker is gone, then there is nothing of interest
				 * left in this Xmax; otherwise, report the tuple as
				 * locked/updated.
				 */
				xidstatus = TransactionIdGetStatus(xmax);
				if (xidstatus != XID_INPROGRESS)
					return HeapTupleMayBeUpdated;
				else
					return HeapTupleBeingUpdated;
			}

			if (tuple->t_infomask & HEAP_XMAX_IS_MULTI)
			{
				TransactionId xmax;

				xmax = HeapTupleGetUpdateXid(tuple);

				/* not LOCKED_ONLY, so it has to have an xmax */
				Assert(TransactionIdIsValid(xmax));

				/* deleting subtransaction must have aborted */
				if (!TransactionIdIsCurrentTransactionId(xmax))
				{
					if (MultiXactIdIsRunning(HeapTupleHeaderGetRawXmax(tuple),
											 false))
						return HeapTupleBeingUpdated;
					return HeapTupleMayBeUpdated;
				}
				else
				{
					if (HeapTupleHeaderGetCmax(tuple) >= curcid)
						return HeapTupleSelfUpdated;	/* updated after scan
														 * started */
					else
						return HeapTupleInvisible;	/* updated before scan
													 * started */
				}
			}

			if (!TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetRawXmax(tuple)))
			{
				/* deleting subtransaction must have aborted */
				SetHintBits(tuple, buffer, HEAP_XMAX_INVALID,
							InvalidTransactionId);
				return HeapTupleMayBeUpdated;
			}

			if (HeapTupleHeaderGetCmax(tuple) >= curcid)
				return HeapTupleSelfUpdated;	/* updated after scan started */
			else
				return HeapTupleInvisible;	/* updated before scan started */
		}
		else
		{
			xidstatus = TransactionIdGetStatus(HeapTupleHeaderGetRawXmin(tuple));
			if (xidstatus == XID_COMMITTED)
			{
				SetHintBits(tuple, buffer, HEAP_XMIN_COMMITTED,
							HeapTupleHeaderGetXmin(tuple));
			}
			else
			{
				if (xidstatus == XID_ABORTED)
					SetHintBits(tuple, buffer, HEAP_XMIN_INVALID,
								InvalidTransactionId);
				return HeapTupleInvisible;
			}
		}
	}

	/* by here, the inserting transaction has committed */

	if (tuple->t_infomask & HEAP_XMAX_INVALID)	/* xid invalid or aborted */
		return HeapTupleMayBeUpdated;

	if (tuple->t_infomask & HEAP_XMAX_COMMITTED)
	{
		if (HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask))
			return HeapTupleMayBeUpdated;
		return HeapTupleUpdated;	/* updated by other */
	}

	if (tuple->t_infomask & HEAP_XMAX_IS_MULTI)
	{
		TransactionId xmax;

		if (HEAP_LOCKED_UPGRADED(tuple->t_infomask))
			return HeapTupleMayBeUpdated;

		if (HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask))
		{
			if (MultiXactIdIsRunning(HeapTupleHeaderGetRawXmax(tuple), true))
				return HeapTupleBeingUpdated;

			SetHintBits(tuple, buffer, HEAP_XMAX_INVALID, InvalidTransactionId);
			return HeapTupleMayBeUpdated;
		}

		xmax = HeapTupleGetUpdateXid(tuple);
		if (!TransactionIdIsValid(xmax))
		{
			if (MultiXactIdIsRunning(HeapTupleHeaderGetRawXmax(tuple), false))
				return HeapTupleBeingUpdated;
		}

		/* not LOCKED_ONLY, so it has to have an xmax */
		Assert(TransactionIdIsValid(xmax));

		if (TransactionIdIsCurrentTransactionId(xmax))
		{
			if (HeapTupleHeaderGetCmax(tuple) >= curcid)
				return HeapTupleSelfUpdated;	/* updated after scan started */
			else
				return HeapTupleInvisible;	/* updated before scan started */
		}

		xidstatus = TransactionIdGetStatus(xmax);
		switch (xidstatus)
		{
			case XID_INPROGRESS:
				return HeapTupleBeingUpdated;
			case XID_COMMITTED:
				return HeapTupleUpdated;
			case XID_ABORTED:
				break;
		}

		/*
		 * By here, the update in the Xmax is either aborted or crashed, but
		 * what about the other members?
		 */
		if (!MultiXactIdIsRunning(HeapTupleHeaderGetRawXmax(tuple), false))
		{
			/*
			 * There's no member, even just a locker, alive anymore, so we can
			 * mark the Xmax as invalid.
			 */
			SetHintBits(tuple, buffer, HEAP_XMAX_INVALID,
						InvalidTransactionId);
			return HeapTupleMayBeUpdated;
		}
		else
		{
			/* There are lockers running */
			return HeapTupleBeingUpdated;
		}
	}

	if (TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetRawXmax(tuple)))
	{
		if (HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask))
			return HeapTupleBeingUpdated;
		if (HeapTupleHeaderGetCmax(tuple) >= curcid)
			return HeapTupleSelfUpdated;	/* updated after scan started */
		else
			return HeapTupleInvisible;	/* updated before scan started */
	}

	xidstatus = TransactionIdGetStatus(HeapTupleHeaderGetRawXmax(tuple));
	switch (xidstatus)
	{
		case XID_INPROGRESS:
			return HeapTupleBeingUpdated;
		case XID_ABORTED:
			/* it must have aborted or crashed */
			SetHintBits(tuple, buffer, HEAP_XMAX_INVALID,
						InvalidTransactionId);
			return HeapTupleMayBeUpdated;
		case XID_COMMITTED:
			break;
	}

	/* xmax transaction committed */

	if (HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask))
	{
		SetHintBits(tuple, buffer, HEAP_XMAX_INVALID,
					InvalidTransactionId);
		return HeapTupleMayBeUpdated;
	}

	SetHintBits(tuple, buffer, HEAP_XMAX_COMMITTED,
				HeapTupleHeaderGetRawXmax(tuple));
	return HeapTupleUpdated;	/* updated by other */
}

/*
 * HeapTupleSatisfiesDirty
 *		True iff heap tuple is valid including effects of open transactions.
 *
 *	Here, we consider the effects of:
 *		all committed and in-progress transactions (as of the current instant)
 *		previous commands of this transaction
 *		changes made by the current command
 *
 * This is essentially like HeapTupleSatisfiesSelf as far as effects of
 * the current transaction and committed/aborted xacts are concerned.
 * However, we also include the effects of other xacts still in progress.
 *
 * A special hack is that the passed-in snapshot struct is used as an
 * output argument to return the xids of concurrent xacts that affected the
 * tuple.  snapshot->xmin is set to the tuple's xmin if that is another
 * transaction that's still in progress; or to InvalidTransactionId if the
 * tuple's xmin is committed good, committed dead, or my own xact.
 * Similarly for snapshot->xmax and the tuple's xmax.  If the tuple was
 * inserted speculatively, meaning that the inserter might still back down
 * on the insertion without aborting the whole transaction, the associated
 * token is also returned in snapshot->speculativeToken.
 */
bool
HeapTupleSatisfiesDirty(HeapTuple htup, Snapshot snapshot,
						Buffer buffer)
{
	HeapTupleHeader tuple = htup->t_data;
	TransactionIdStatus xidstatus;

	Assert(ItemPointerIsValid(&htup->t_self));
	Assert(htup->t_tableOid != InvalidOid);

	snapshot->xmin = snapshot->xmax = InvalidTransactionId;
	snapshot->speculativeToken = 0;

	if (!HeapTupleHeaderXminCommitted(tuple))
	{
		if (HeapTupleHeaderXminInvalid(tuple))
			return false;

		/* Used by pre-9.0 binary upgrades */
		if (tuple->t_infomask & HEAP_MOVED)
			return IsMovedTupleVisible(htup, buffer);

		if (TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetRawXmin(tuple)))
		{
			if (tuple->t_infomask & HEAP_XMAX_INVALID)	/* xid invalid */
				return true;

			if (HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask))	/* not deleter */
				return true;

			if (tuple->t_infomask & HEAP_XMAX_IS_MULTI)
			{
				TransactionId xmax;

				xmax = HeapTupleGetUpdateXid(tuple);

				/* not LOCKED_ONLY, so it has to have an xmax */
				Assert(TransactionIdIsValid(xmax));

				/* updating subtransaction must have aborted */
				if (!TransactionIdIsCurrentTransactionId(xmax))
					return true;
				else
					return false;
			}

			if (!TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetRawXmax(tuple)))
			{
				/* deleting subtransaction must have aborted */
				SetHintBits(tuple, buffer, HEAP_XMAX_INVALID,
							InvalidTransactionId);
				return true;
			}

			return false;
		}
		else
		{
			xidstatus = TransactionIdGetStatus(HeapTupleHeaderGetRawXmin(tuple));
			switch (xidstatus)
			{
				case XID_INPROGRESS:
					/*
					 * Return the speculative token to caller.  Caller can worry about
					 * xmax, since it requires a conclusively locked row version, and
					 * a concurrent update to this tuple is a conflict of its
					 * purposes.
					 */
					if (HeapTupleHeaderIsSpeculative(tuple))
					{
						snapshot->speculativeToken =
							HeapTupleHeaderGetSpeculativeToken(tuple);

						Assert(snapshot->speculativeToken != 0);
					}

					snapshot->xmin = HeapTupleHeaderGetRawXmin(tuple);
					/* XXX shouldn't we fall through to look at xmax? */
					return true;		/* in insertion by other */
				case XID_COMMITTED:
					SetHintBits(tuple, buffer, HEAP_XMIN_COMMITTED,
								HeapTupleHeaderGetRawXmin(tuple));
					break;
				case XID_ABORTED:
					/* it must have aborted or crashed */
					SetHintBits(tuple, buffer, HEAP_XMIN_INVALID,
								InvalidTransactionId);
				return false;
			}
		}
	}

	/* by here, the inserting transaction has committed */

	if (tuple->t_infomask & HEAP_XMAX_INVALID)	/* xid invalid or aborted */
		return true;

	if (tuple->t_infomask & HEAP_XMAX_COMMITTED)
	{
		if (HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask))
			return true;
		return false;			/* updated by other */
	}

	if (tuple->t_infomask & HEAP_XMAX_IS_MULTI)
	{
		TransactionId xmax;

		if (HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask))
			return true;

		xmax = HeapTupleGetUpdateXid(tuple);

		/* not LOCKED_ONLY, so it has to have an xmax */
		Assert(TransactionIdIsValid(xmax));

		if (TransactionIdIsCurrentTransactionId(xmax))
			return false;

		xidstatus = TransactionIdGetStatus(xmax);
		switch (xidstatus)
		{
			case XID_INPROGRESS:
				snapshot->xmax = xmax;
				return true;
			case XID_COMMITTED:
				return false;
			case XID_ABORTED:
				/* it must have aborted or crashed */
				return true;
		}
	}

	if (TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetRawXmax(tuple)))
	{
		if (HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask))
			return true;
		return false;
	}

	xidstatus = TransactionIdGetStatus(HeapTupleHeaderGetRawXmax(tuple));
	switch (xidstatus)
	{
		case XID_INPROGRESS:
			if (!HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask))
				snapshot->xmax = HeapTupleHeaderGetRawXmax(tuple);
			return true;
		case XID_ABORTED:
			/* it must have aborted or crashed */
			SetHintBits(tuple, buffer, HEAP_XMAX_INVALID,
						InvalidTransactionId);
			return true;
		case XID_COMMITTED:
			break;
	}

	/* xmax transaction committed */

	if (HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask))
	{
		SetHintBits(tuple, buffer, HEAP_XMAX_INVALID,
					InvalidTransactionId);
		return true;
	}

	SetHintBits(tuple, buffer, HEAP_XMAX_COMMITTED,
				HeapTupleHeaderGetRawXmax(tuple));
	return false;				/* updated by other */
}


#ifdef ENABLE_DISTRIBUTED_TRANSACTION
/*
 * A timestamp based protocol to generate consistent distributed snapshot 
 * across a cluster to provide RC/RR level isolation between distributed transactions.
 * We cache the commit ts of xmin and xmax in the tuple header to speed up visibility
 * validating as it is cost to access the CTS log. 
 */
static bool
XminVisibleInSnapshotByTimestamp(HeapTupleHeader tuple, 
								Snapshot snapshot, 
								Buffer buffer, 
                                bool *retry)
{// #lizard forgives
    CommitSeqNo 		committs;
    TransactionId xid = HeapTupleHeaderGetRawXmin(tuple);
    
	committs = HeapTupleHderGetXminTimestampAtomic(tuple);
	if (!TransactionIdIsNormal(xid) || !COMMITSEQNO_IS_COMMITTED(committs))
	{	
		bool visible;
		TransactionIdStatus	hintstatus;

		visible =  XidVisibleInSnapshotDistri(tuple, xid, snapshot, buffer, &hintstatus, retry);
		//if (hintstatus == XID_COMMITTED)
		//	SetHintBits(tuple, buffer, HEAP_XMIN_COMMITTED, xid);
		return visible;
	}
    
	if (snapshot->snapshotcsn >= committs)
	{
		if (enable_distri_visibility_print)
			elog(LOG, "snapshot ts " UINT64_FORMAT " true xid %d committs "UINT64_FORMAT" 21.", 
								snapshot->snapshotcsn, xid, committs);
		return true;
	}
	else
	{    
		if (enable_distri_visibility_print)
			elog(LOG, "snapshot ts " UINT64_FORMAT " false xid %d committs "UINT64_FORMAT" 22.", 
								snapshot->snapshotcsn, xid, committs);
		return false;
	}
    
	/* 
	 * should not be here
	 * just keep complier quiet
	 */
	Assert(0);
	return false;
}

static bool
XmaxVisibleInSnapshotByTimestamp(HeapTupleHeader tuple, 
								Snapshot snapshot, 
								Buffer buffer, 
                                bool *retry)
{// #lizard forgives
    CommitSeqNo 		committs;
    TransactionId xid = HeapTupleHeaderGetRawXmax(tuple);

	committs = HeapTupleHderGetXmaxTimestampAtomic(tuple);
	if (!TransactionIdIsNormal(xid) || !COMMITSEQNO_IS_COMMITTED(committs))
	{
		bool visible;
		TransactionIdStatus	hintstatus;

		visible =  XidVisibleInSnapshotDistri(tuple, xid, snapshot, buffer, &hintstatus, retry);
		//if (hintstatus == XID_COMMITTED)
		//	SetHintBits(tuple, buffer, HEAP_XMAX_COMMITTED, xid);
		return visible;
	}
    
	if (snapshot->snapshotcsn >= committs)
	{
		if (enable_distri_visibility_print)
			elog(LOG, "snapshot ts " UINT64_FORMAT " true xid %d committs "UINT64_FORMAT" 11.", 
								snapshot->snapshotcsn, xid, committs);
		return true;
	}
	else
	{
		if (enable_distri_visibility_print)
			elog(LOG, "snapshot ts " UINT64_FORMAT " false xid %d committs "UINT64_FORMAT" 12.",
								snapshot->snapshotcsn, xid, committs);
		return false;
	}
	/* 
	 * should not be here
	 * just keep complier quiet
	 */
	Assert(0);
	return false;
}
#endif
/*
 * HeapTupleSatisfiesMVCC
 *		True iff heap tuple is valid for the given MVCC snapshot.
 *
 *	Here, we consider the effects of:
 *		all transactions committed as of the time of the given snapshot
 *		previous commands of this transaction
 *
 *	Does _not_ include:
 *		transactions shown as in-progress by the snapshot
 *		transactions started after the snapshot was taken
 *		changes made by the current command
 */
bool
HeapTupleSatisfiesMVCC(HeapTuple htup, Snapshot snapshot,
					   Buffer buffer)
{
	HeapTupleHeader tuple = htup->t_data;
	bool		visible;
	TransactionIdStatus	hintstatus;

#ifdef ENABLE_DISTRIBUTED_TRANSACTION
	bool		retry;
l1:
	retry = false;
#endif

	Assert(ItemPointerIsValid(&htup->t_self));
	Assert(htup->t_tableOid != InvalidOid);

	if (!HeapTupleHeaderXminCommitted(tuple))
	{
		if (HeapTupleHeaderXminInvalid(tuple))
			return false;

		/* Used by pre-9.0 binary upgrades */
		if (tuple->t_infomask & HEAP_MOVED)
			return IsMovedTupleVisible(htup, buffer);

		if (TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetRawXmin(tuple)))
		{
			if (HeapTupleHeaderGetCmin(tuple) >= snapshot->curcid)
				return false;	/* inserted after scan started */

			if (tuple->t_infomask & HEAP_XMAX_INVALID)	/* xid invalid */
				return true;

			if (HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask))	/* not deleter */
				return true;

			if (tuple->t_infomask & HEAP_XMAX_IS_MULTI)
			{
				TransactionId xmax;

				xmax = HeapTupleGetUpdateXid(tuple);

				/* not LOCKED_ONLY, so it has to have an xmax */
				Assert(TransactionIdIsValid(xmax));

				/* updating subtransaction must have aborted */
				if (!TransactionIdIsCurrentTransactionId(xmax))
					return true;
				else if (HeapTupleHeaderGetCmax(tuple) >= snapshot->curcid)
					return true;	/* updated after scan started */
				else
					return false;	/* updated before scan started */
			}

			if (!TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetRawXmax(tuple)))
			{
				/* deleting subtransaction must have aborted */
				SetHintBits(tuple, buffer, HEAP_XMAX_INVALID,
							InvalidTransactionId);
				return true;
			}

			if (HeapTupleHeaderGetCmax(tuple) >= snapshot->curcid)
				return true;	/* deleted after scan started */
			else
				return false;	/* deleted before scan started */
		}
		else
		{
			#ifdef ENABLE_DISTRIBUTED_TRANSACTION
			visible = XidVisibleInSnapshotDistri(tuple, HeapTupleHeaderGetXmin(tuple),
														snapshot, buffer, &hintstatus, &retry);
			if (retry)
				goto l1;
			if (hintstatus == XID_COMMITTED)
				SetHintBits(tuple, buffer, HEAP_XMIN_COMMITTED,
							HeapTupleHeaderGetRawXmin(tuple));
			if (hintstatus == XID_ABORTED)
				SetHintBits(tuple, buffer, HEAP_XMIN_INVALID,
							InvalidTransactionId);
			if (!visible)
				return false;

			#else
			visible = XidVisibleInSnapshot(HeapTupleHeaderGetXmin(tuple),
										   snapshot, &hintstatus);
			if (hintstatus == XID_COMMITTED)
				SetHintBits(tuple, buffer, HEAP_XMIN_COMMITTED,
							HeapTupleHeaderGetRawXmin(tuple));
			if (hintstatus == XID_ABORTED)
				SetHintBits(tuple, buffer, HEAP_XMIN_INVALID,
							InvalidTransactionId);
			if (!visible)
				return false;
			#endif
		}
	}
	else
	{
		/* xmin is committed, but maybe not according to our snapshot */
		if (!HeapTupleHeaderXminFrozen(tuple))
		{
			#ifdef ENABLE_DISTRIBUTED_TRANSACTION
			visible = XminVisibleInSnapshotByTimestamp(tuple, snapshot, buffer,	&retry);
			if (retry)
				goto l1;
			#else
			visible = CommittedXidVisibleInSnapshot(HeapTupleHeaderGetRawXmin(tuple), snapshot);
			#endif
			if (!visible)
				return false;		/* treat as still in progress */
		}
	}

	/* by here, the inserting transaction has committed */

	if (tuple->t_infomask & HEAP_XMAX_INVALID)	/* xid invalid or aborted */
		return true;

	if (HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask))
		return true;

	if (tuple->t_infomask & HEAP_XMAX_IS_MULTI)
	{
		TransactionId xmax;

		/* already checked above */
		Assert(!HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask));

		xmax = HeapTupleGetUpdateXid(tuple);

		/* not LOCKED_ONLY, so it has to have an xmax */
		Assert(TransactionIdIsValid(xmax));

		if (TransactionIdIsCurrentTransactionId(xmax))
		{
			if (HeapTupleHeaderGetCmax(tuple) >= snapshot->curcid)
				return true;	/* deleted after scan started */
			else
				return false;	/* deleted before scan started */
		}
		#ifdef ENABLE_DISTRIBUTED_TRANSACTION
		visible = XidVisibleInSnapshotDistri(tuple, xmax, snapshot, buffer,	
													&hintstatus, &retry);
		if (retry)
			goto l1;
		#else
		visible = XidVisibleInSnapshot(xmax, snapshot, &hintstatus);
		#endif
		if (visible)
			return false;		/* updating transaction committed */
		else
		{
			/* it must have aborted or crashed */
			return true;
		}
	}

	if (!(tuple->t_infomask & HEAP_XMAX_COMMITTED))
	{
		if (TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetRawXmax(tuple)))
		{
			if (HeapTupleHeaderGetCmax(tuple) >= snapshot->curcid)
				return true;	/* deleted after scan started */
			else
				return false;	/* deleted before scan started */
		}
		#ifdef ENABLE_DISTRIBUTED_TRANSACTION
		visible = XidVisibleInSnapshotDistri(tuple, HeapTupleHeaderGetRawXmax(tuple), 
													snapshot, buffer, &hintstatus, &retry);
		if (retry)
			goto l1;
		#else
		visible = XidVisibleInSnapshot(HeapTupleHeaderGetRawXmax(tuple),
									   snapshot, &hintstatus);
		#endif
		if (hintstatus == XID_COMMITTED)
		{
			/* xmax transaction committed */
			SetHintBits(tuple, buffer, HEAP_XMAX_COMMITTED,
						HeapTupleHeaderGetRawXmax(tuple));
		}
		if (hintstatus == XID_ABORTED)
		{
			/* it must have aborted or crashed */
			SetHintBits(tuple, buffer, HEAP_XMAX_INVALID,
						InvalidTransactionId);
		}
		if (!visible)
			return true;		/* treat as still in progress */
	}
	else
	{
		/* xmax is committed, but maybe not according to our snapshot */
		#ifdef ENABLE_DISTRIBUTED_TRANSACTION
		visible = XmaxVisibleInSnapshotByTimestamp(tuple, snapshot, buffer,	&retry);
		if (retry)
			goto l1;
		#else
		visible = CommittedXidVisibleInSnapshot(HeapTupleHeaderGetRawXmax(tuple), snapshot);
		#endif
		if (!visible)
			return true;		/* treat as still in progress */
	}

	/* xmax transaction committed */

	return false;
}

/*
 * HeapTupleSatisfiesVacuum
 *
 *	Determine the status of tuples for VACUUM purposes.  Here, what
 *	we mainly want to know is if a tuple is potentially visible to *any*
 *	running transaction.  If so, it can't be removed yet by VACUUM.
 *
 * OldestSnapshot is a cutoff snapshot (obtained from GetOldestSnapshot()).
 * Tuples deleted by XIDs that are still visible to OldestSnapshot are deemed
 * "recently dead"; they might still be visible to some open transaction,
 * so we can't remove them, even if we see that the deleting transaction
 * has committed.
 *
 * Note: predicate.c calls this with a current snapshot, rather than one obtained
 * from GetOldestSnapshot(). So even if this function determines that a tuple
 * is not visible to anyone anymore, we can't "kill" the tuple right here.
 */
HTSV_Result
HeapTupleSatisfiesVacuum(HeapTuple htup, TransactionId OldestXmin,
						 Buffer buffer)
{
	HeapTupleHeader tuple = htup->t_data;
	TransactionIdStatus	xidstatus;

	Assert(ItemPointerIsValid(&htup->t_self));
	Assert(htup->t_tableOid != InvalidOid);

	/*
	 * Has inserting transaction committed?
	 *
	 * If the inserting transaction aborted, then the tuple was never visible
	 * to any other transaction, so we can delete it immediately.
	 */
	if (!HeapTupleHeaderXminCommitted(tuple))
	{
		if (HeapTupleHeaderXminInvalid(tuple))
			return HEAPTUPLE_DEAD;

		/* Used by pre-9.0 binary upgrades */
		if (tuple->t_infomask & HEAP_MOVED)
		{
			if (IsMovedTupleVisible(htup, buffer))
				return HEAPTUPLE_LIVE;
			else
				return HEAPTUPLE_DEAD;
		}

		if (TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetRawXmin(tuple)))
		{
			if (tuple->t_infomask & HEAP_XMAX_INVALID)	/* xid invalid */
				return HEAPTUPLE_INSERT_IN_PROGRESS;
			/* only locked? run infomask-only check first, for performance */
			if (HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask) ||
				HeapTupleHeaderIsOnlyLocked(tuple))
				return HEAPTUPLE_INSERT_IN_PROGRESS;
			/* inserted and then deleted by same xact */
			if (TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetUpdateXid(tuple)))
				return HEAPTUPLE_DELETE_IN_PROGRESS;
			/* deleting subtransaction must have aborted */
			return HEAPTUPLE_INSERT_IN_PROGRESS;
		}

		xidstatus = TransactionIdGetStatus(HeapTupleHeaderGetRawXmin(tuple));

		if (xidstatus == XID_INPROGRESS)
		{
			/*
			 * It'd be possible to discern between INSERT/DELETE in progress
			 * here by looking at xmax - but that doesn't seem beneficial for
			 * the majority of callers and even detrimental for some. We'd
			 * rather have callers look at/wait for xmin than xmax. It's
			 * always correct to return INSERT_IN_PROGRESS because that's
			 * what's happening from the view of other backends.
			 */
			return HEAPTUPLE_INSERT_IN_PROGRESS;
		}
		else if (xidstatus == XID_COMMITTED)
			SetHintBits(tuple, buffer, HEAP_XMIN_COMMITTED,
						HeapTupleHeaderGetRawXmin(tuple));
		else
		{
			/*
			 * Not in Progress, Not Committed, so either Aborted or crashed
			 */
			SetHintBits(tuple, buffer, HEAP_XMIN_INVALID,
						InvalidTransactionId);
			return HEAPTUPLE_DEAD;
		}

		/*
		 * At this point the xmin is known committed, but we might not have
		 * been able to set the hint bit yet; so we can no longer Assert that
		 * it's set.
		 */
	}

	/*
	 * Okay, the inserter committed, so it was good at some point.  Now what
	 * about the deleting transaction?
	 */
	if (tuple->t_infomask & HEAP_XMAX_INVALID)
		return HEAPTUPLE_LIVE;

	if (HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask))
	{
		/*
		 * "Deleting" xact really only locked it, so the tuple is live in any
		 * case.  However, we should make sure that either XMAX_COMMITTED or
		 * XMAX_INVALID gets set once the xact is gone, to reduce the costs of
		 * examining the tuple for future xacts.
		 */
		if (!(tuple->t_infomask & HEAP_XMAX_COMMITTED))
		{
			if (tuple->t_infomask & HEAP_XMAX_IS_MULTI)
			{
				/*
				 * If it's a pre-pg_upgrade tuple, the multixact cannot
				 * possibly be running; otherwise have to check.
				 */
				if (!HEAP_LOCKED_UPGRADED(tuple->t_infomask) &&
					MultiXactIdIsRunning(HeapTupleHeaderGetRawXmax(tuple),
										 true))
					return HEAPTUPLE_LIVE;
				SetHintBits(tuple, buffer, HEAP_XMAX_INVALID, InvalidTransactionId);
			}
			else
			{
				xidstatus = TransactionIdGetStatus(HeapTupleHeaderGetRawXmax(tuple));
				if (xidstatus == XID_INPROGRESS)
					return HEAPTUPLE_LIVE;
				SetHintBits(tuple, buffer, HEAP_XMAX_INVALID,
							InvalidTransactionId);
			}
		}

		/*
		 * We don't really care whether xmax did commit, abort or crash. We
		 * know that xmax did lock the tuple, but it did not and will never
		 * actually update it.
		 */

		return HEAPTUPLE_LIVE;
	}

	if (tuple->t_infomask & HEAP_XMAX_IS_MULTI)
	{
		TransactionId xmax = HeapTupleGetUpdateXid(tuple);
		CommitSeqNo committs;

		/* already checked above */
		Assert(!HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask));

		/* not LOCKED_ONLY, so it has to have an xmax */
		Assert(TransactionIdIsValid(xmax));

		/*
		 * TransactionIdIsActive() does not consider subtransactions and
		 * prepared transactions, which would however mark running or 
		 * committed transactions as aborted by mistaken.
		 * 
		 * BUG:  diverged version chain found under the mixed TPCC workload
		 * and consistency checking.
		 * 
		 * FIX: Use the CTS status as the ground truth for transaction status.
		 * 
		 * Improve: Only fetch CTS once for both status and prunable check.
		 * 
		 * Written by  , 2020-09-02.
		 */ 
		committs = TransactionIdGetCommitSeqNo(xmax);

		Assert(COMMITSEQNO_IS_INPROGRESS(committs)
				|| COMMITSEQNO_IS_PREPARED(committs)
				|| COMMITSEQNO_IS_COMMITTED(committs)
				|| COMMITSEQNO_IS_ABORTED(committs));


		if (COMMITSEQNO_IS_INPROGRESS(committs)
			|| COMMITSEQNO_IS_PREPARED(committs))
			return HEAPTUPLE_DELETE_IN_PROGRESS;
		else if (COMMITSEQNO_IS_COMMITTED(committs))
		{
			/*
			 * The multixact might still be running due to lockers.  If the
			 * updater is below the xid horizon, we have to return DEAD
			 * regardless -- otherwise we could end up with a tuple where the
			 * updater has to be removed due to the horizon, but is not pruned
			 * away.  It's not a problem to prune that tuple, because any
			 * remaining lockers will also be present in newer tuple versions.
			 */
			if (!TransactionIdPrecedes(xmax, OldestXmin))
				return HEAPTUPLE_RECENTLY_DEAD;

			#ifdef ENABLE_DISTRIBUTED_TRANSACTION
			{
				if (!COMMITSEQNO_IS_COMMITTED(committs))
				{
					elog(ERROR, "xmax %d should have committs "UINT64_FORMAT, xmax, committs);
				}

				if (!CommittsSatisfiesVacuum(committs))
					return HEAPTUPLE_RECENTLY_DEAD;
			}
			#endif
			return HEAPTUPLE_DEAD;
		}
		else if (!MultiXactIdIsRunning(HeapTupleHeaderGetRawXmax(tuple), false))
		{
			/*
			 * Not in Progress, Not Committed, so either Aborted or crashed.
			 * Mark the Xmax as invalid.
			 */
			SetHintBits(tuple, buffer, HEAP_XMAX_INVALID, InvalidTransactionId);
		}

		return HEAPTUPLE_LIVE;
	}

	if (!(tuple->t_infomask & HEAP_XMAX_COMMITTED))
	{
		xidstatus = TransactionIdGetStatus(HeapTupleHeaderGetRawXmax(tuple));

		if (xidstatus == XID_INPROGRESS)
			return HEAPTUPLE_DELETE_IN_PROGRESS;
		else if (xidstatus == XID_COMMITTED)
			SetHintBits(tuple, buffer, HEAP_XMAX_COMMITTED,
						HeapTupleHeaderGetRawXmax(tuple));
		else
		{
			/*
			 * Not in Progress, Not Committed, so either Aborted or crashed
			 */
			SetHintBits(tuple, buffer, HEAP_XMAX_INVALID,
						InvalidTransactionId);
			return HEAPTUPLE_LIVE;
		}

		/*
		 * At this point the xmax is known committed, but we might not have
		 * been able to set the hint bit yet; so we can no longer Assert that
		 * it's set.
		 */
	}

	/*
	 * Deleter committed, but perhaps it was recent enough that some open
	 * transactions could still see the tuple.
	 */
	if (!TransactionIdPrecedes(HeapTupleHeaderGetRawXmax(tuple), OldestXmin))
		return HEAPTUPLE_RECENTLY_DEAD;
	
#ifdef ENABLE_DISTRIBUTED_TRANSACTION	
	{
		CommitSeqNo committs = HeapTupleHderGetXmaxTimestampAtomic(tuple);

		if (!COMMITSEQNO_IS_COMMITTED(committs))
		{
			committs = TransactionIdGetCommitSeqNo(HeapTupleHeaderGetRawXmax(tuple));
		}

		if (!COMMITSEQNO_IS_COMMITTED(committs))
		{
			elog(ERROR, "xmax %d should have committs "UINT64_FORMAT, HeapTupleHeaderGetRawXmax(tuple), committs);
		}

		if (!CommittsSatisfiesVacuum(committs))
			return HEAPTUPLE_RECENTLY_DEAD;
	}
#endif

	/* Otherwise, it's dead and removable */
	return HEAPTUPLE_DEAD;
}


/*
 * HeapTupleSatisfiesNonVacuumable
 *
 *	True if tuple might be visible to some transaction; false if it's
 *	surely dead to everyone, ie, vacuumable.
 *
 *	This is an interface to HeapTupleSatisfiesVacuum that meets the
 *	SnapshotSatisfiesFunc API, so it can be used through a Snapshot.
 *	snapshot->xmin must have been set up with the xmin horizon to use.
 */
bool
HeapTupleSatisfiesNonVacuumable(HeapTuple htup, Snapshot snapshot,
								Buffer buffer)
{
	return HeapTupleSatisfiesVacuum(htup, snapshot->xmin, buffer)
		!= HEAPTUPLE_DEAD;
}


/*
 * HeapTupleIsSurelyDead
 *
 *	Cheaply determine whether a tuple is surely dead to all onlookers.
 *	We sometimes use this in lieu of HeapTupleSatisfiesVacuum when the
 *	tuple has just been tested by another visibility routine (usually
 *	HeapTupleSatisfiesMVCC) and, therefore, any hint bits that can be set
 *	should already be set.  We assume that if no hint bits are set, the xmin
 *	or xmax transaction is still running.  This is therefore faster than
 *	HeapTupleSatisfiesVacuum, because we don't consult PGXACT nor CLOG.
 *	It's okay to return false when in doubt, but we must return true only
 *	if the tuple is removable.
 */
bool
HeapTupleIsSurelyDead(HeapTuple htup, TransactionId OldestXmin)
{
	HeapTupleHeader tuple = htup->t_data;

	Assert(ItemPointerIsValid(&htup->t_self));
	Assert(htup->t_tableOid != InvalidOid);

	/*
	 * If the inserting transaction is marked invalid, then it aborted, and
	 * the tuple is definitely dead.  If it's marked neither committed nor
	 * invalid, then we assume it's still alive (since the presumption is that
	 * all relevant hint bits were just set moments ago).
	 */
	if (!HeapTupleHeaderXminCommitted(tuple))
		return HeapTupleHeaderXminInvalid(tuple) ? true : false;

	/*
	 * If the inserting transaction committed, but any deleting transaction
	 * aborted, the tuple is still alive.
	 */
	if (tuple->t_infomask & HEAP_XMAX_INVALID)
		return false;

	/*
	 * If the XMAX is just a lock, the tuple is still alive.
	 */
	if (HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask))
		return false;

	/*
	 * If the Xmax is a MultiXact, it might be dead or alive, but we cannot
	 * know without checking pg_multixact.
	 */
	if (tuple->t_infomask & HEAP_XMAX_IS_MULTI)
		return false;

	/* If deleter isn't known to have committed, assume it's still running. */
	if (!(tuple->t_infomask & HEAP_XMAX_COMMITTED))
		return false;

	/* Deleter committed, so tuple is dead if the XID is old enough. */
#ifdef ENABLE_DISTRIBUTED_TRANSACTION	
	if (!TransactionIdPrecedes(HeapTupleHeaderGetRawXmax(tuple), OldestXmin))
		return false;
	else
	{
		CommitSeqNo committs = HeapTupleHderGetXmaxTimestampAtomic(tuple);

		if (!COMMITSEQNO_IS_COMMITTED(committs))
		{
			committs = TransactionIdGetCommitSeqNo(HeapTupleHeaderGetRawXmax(tuple));
		}

		if (!COMMITSEQNO_IS_COMMITTED(committs))
		{
			elog(ERROR, "xmax %d should have committs "UINT64_FORMAT, HeapTupleHeaderGetRawXmax(tuple), committs);
		}

		if (!CommittsSatisfiesVacuum(committs))
			return false;
		else
			return true;
	}
#else
	return TransactionIdPrecedes(HeapTupleHeaderGetRawXmax(tuple), OldestXmin);
#endif
}

/*
 * XidVisibleInSnapshot
 *		Is the given XID visible according to the snapshot?
 *
 * On return, *hintstatus is set to indicate if the transaction had committed,
 * or aborted, whether or not it's not visible to us.
 */
bool
XidVisibleInSnapshot(TransactionId xid, Snapshot snapshot,
					 TransactionIdStatus *hintstatus)
{
	CommitSeqNo csn;

	*hintstatus = XID_INPROGRESS;

	/*
	 * Any xid >= xmax is in-progress (or aborted, but we don't distinguish
	 * that here).
	 *
	 * We can't do anything useful with xmin, because the xmin only tells us
	 * whether we see it as completed. We have to check the transaction log to
	 * see if the transaction committed or aborted, in any case.
	 */
	if (TransactionIdFollowsOrEquals(xid, snapshot->xmax))
		return false;

	csn = TransactionIdGetCommitSeqNo(xid);

	if (COMMITSEQNO_IS_COMMITTED(csn))
	{
		*hintstatus = XID_COMMITTED;
		if (csn <= snapshot->snapshotcsn)
			return true;
		else
			return false;
	}
	else
	{
		if (csn == COMMITSEQNO_ABORTED)
			*hintstatus = XID_ABORTED;
		return false;
	}
}

/*
 * CommittedXidVisibleInSnapshot
 *		Is the given XID visible according to the snapshot?
 *
 * This is the same as XidVisibleInSnapshot, but the caller knows that the
 * given XID committed. The only question is whether it's visible to our
 * snapshot or not.
 */
#ifndef ENABLE_DISTRIBUTED_TRANSACTION
static bool
CommittedXidVisibleInSnapshot(TransactionId xid, Snapshot snapshot)
{
	CommitSeqNo csn;

	/*
	 * Make a quick range check to eliminate most XIDs without looking at the
	 * CSN log.
	 */
	if (TransactionIdPrecedes(xid, snapshot->xmin))
		return true;

	/*
	 * Any xid >= xmax is in-progress (or aborted, but we don't distinguish
	 * that here.
	 */
	if (TransactionIdFollowsOrEquals(xid, snapshot->xmax))
		return false;

	csn = TransactionIdGetCommitSeqNo(xid);

	if (!COMMITSEQNO_IS_COMMITTED(csn))
	{
		elog(WARNING, "transaction %u was hinted as committed, but was not marked as committed in the transaction log", xid);
		/*
		 * We have contradicting evidence on whether the transaction committed or
		 * not. Let's assume that it did. That seems better than erroring out.
		 */
		return true;
	}

	if (csn <= snapshot->snapshotcsn)
		return true;
	else
		return false;
}
#endif

#ifdef ENABLE_DISTRIBUTED_TRANSACTION
/*
 * A timestamp based protocol to generate consistent distributed snapshot 
 * across a cluster to provide RC/RR level isolation between distributed transactions.
 */
static bool
XidVisibleInSnapshotDistri(HeapTupleHeader tuple, 
							TransactionId xid, 
							Snapshot snapshot, 
							Buffer buffer, 
                            TransactionIdStatus *hintstatus,
							bool *retry)
{
	CommitSeqNo committs;

	*retry = false;
	*hintstatus = XID_INPROGRESS;
	committs = TransactionIdGetCommitSeqNo(xid);
	
	/* 
	 * For local transactions, we check whether
	 * it is committing.
	 */
	if (COMMITSEQNO_IS_COMMITTING(committs))
	{
	   /* 
		* We choose to wait for the specifc transaction to run to 
		* completion, so as the transaction commit path does not need
		* to acquire CommitSeqNoLock.
		*/

		XactLockTableWait(xid, NULL, NULL, XLTW_None);
		committs = CTSLogGetCommitTs(xid);
		Assert(committs != COMMITSEQNO_COMMITTING);
	}
	else if (COMMITSEQNO_IS_SUBTRANS(committs))
	{
		return false;
	}
	else if (COMMITSEQNO_IS_PREPARED(committs))
	{
		CommitSeqNo preparets = UNMASK_PREPARE_BIT(committs);
		BufferDesc 	*buf;
		int 		lock_type;

#ifdef POLARDB_X
		if (IsInitProcessingMode())
		{
			if (enable_twophase_recover_debug_print)
				elog(DEBUG_2PC, "Prepared tuple invisible in InitProcessingMode.xid:%d", xid);
			return false;
		}
#endif

		/* 
		 * Prepare ts may be zero for recovered prepared transaction
		 */ 
		if (preparets && !COMMITSEQNO_IS_NORMAL(preparets))
			elog(ERROR, "prepare ts is invalid "UINT64_FORMAT, preparets);

		#ifdef ENABLE_DISTR_DEBUG
		if (!preparets)
			elog(LOG, "snapshot ts "INT64_FORMAT" recovered prepared transaction %d", snapshot->snapshotcsn, xid);
		#endif

		/* avoid unnecessary wait */
		if (snapshot->snapshotcsn < preparets)
		{
			if(enable_distri_visibility_print)
				elog(LOG, "snapshot ts " INT64_FORMAT " false xid %d preparets "UINT64_FORMAT" committs "UINT64_FORMAT" .", 
															snapshot->snapshotcsn, xid, preparets, committs);
			return false;
		}
		else
		{
			if(enable_distri_visibility_print)
				elog(LOG, "snapshot ts " INT64_FORMAT " wait xid %d preparets "UINT64_FORMAT" committs "UINT64_FORMAT" .", 
															snapshot->snapshotcsn, xid, preparets, committs);

		}
		
		
		/* 
		 * When xid is prepared, we should wait for its completion.
		 * To avoid blocking concurrent updates to the scanned buffer,
		 * we unlock the buffer and lock it again upon completion.
		 * As the tuple header may be changed, we retry the HeapTupleSatisfiesMVCC
		 * routine. Some optimization can be adopted to avoid unneccessary retries.
		 * 
		 * Note that although the buffer is unlocked, the tuples on the buffer 
		 * would not be compacted as the buffer is pinned. 
		 * Hence, it is safe enough to reaccess the tuple header again without
		 * refetching the tuple through its tuple item pointer.
		 */ 

		buf = GetBufferDescriptor(buffer - 1);
		/* 
		 * Remember the lock type although it usually should be shared lock.
		 */
		if(LWLockHeldByMeInMode(BufferDescriptorGetContentLock(buf),
									LW_EXCLUSIVE))
		{
			lock_type = BUFFER_LOCK_EXCLUSIVE;
		}
		else if(LWLockHeldByMeInMode(BufferDescriptorGetContentLock(buf),
									LW_SHARED))
		{
			lock_type = BUFFER_LOCK_SHARE;
		}
		else
		{
			elog(ERROR, "Buffer should be locked during scan");
		}
		
		LockBuffer(buffer, BUFFER_LOCK_UNLOCK);    

		XactLockTableWait(xid, NULL, NULL, XLTW_None);
		
		LockBuffer(buffer, lock_type);
		/* Avoid deadlock */
		if(TransactionIdDidAbort(xid))
		{
			/* 
			 * Don't set hint bit as the buffer lock is re-acquired.
			 */
			//*hintstatus = XID_ABORTED;
			if(enable_distri_visibility_print)
				elog(LOG, "abort snapshot ts " INT64_FORMAT " false xid %d .", snapshot->snapshotcsn, xid);
			return false;
		}
		
		*retry = true;
		return true;
	}

	if (COMMITSEQNO_IS_COMMITTED(committs))
	{
		*hintstatus = XID_COMMITTED;

		if(snapshot->snapshotcsn >= committs)
		{
			if(enable_distri_visibility_print)
				elog(LOG, "snapshot ts " INT64_FORMAT " true xid %d committs "UINT64_FORMAT" 3.", 
															snapshot->snapshotcsn, xid, committs);
			
			return true;
		}
		else
		{
			if(enable_distri_visibility_print)
				elog(LOG, "snapshot ts " INT64_FORMAT " false xid %d committs "UINT64_FORMAT" 4.", 
															snapshot->snapshotcsn, xid, committs);
			return false;
		}
		/*
		 * just keep compliler quiet
		 */
		Assert(0);
		return true;
	}
	else if (COMMITSEQNO_IS_ABORTED(committs))
	{
		*hintstatus = XID_ABORTED;
		if(enable_distri_visibility_print)
			elog(LOG, "snapshot ts "INT64_FORMAT" false xid %d abort", snapshot->snapshotcsn, xid);
		
		return false;
	}

	/*
     * It must be non-prepared transaction or crashed transaction.
	 * For non-prepared transaction, its commit timestamp must be larger than
     * the current running transaction/statement's start timestamp.
     * This is because that as T1's prepare timestamp does not be acquired, 
     * T2.start_ts < T1.commit_ts is always being held.
     */
 	if(enable_distri_visibility_print)
        elog(LOG, "snapshot ts " UINT64_FORMAT " false xid %d 5.", snapshot->snapshotcsn, xid);
    
	return false;
}
#endif

/*
 * Is the tuple really only locked?  That is, is it not updated?
 *
 * It's easy to check just infomask bits if the locker is not a multi; but
 * otherwise we need to verify that the updating transaction has not aborted.
 *
 * This function is here because it follows the same time qualification rules
 * laid out at the top of this file.
 */
bool
HeapTupleHeaderIsOnlyLocked(HeapTupleHeader tuple)
{
	TransactionId xmax;
	TransactionIdStatus	xidstatus;

	/* if there's no valid Xmax, then there's obviously no update either */
	if (tuple->t_infomask & HEAP_XMAX_INVALID)
		return true;

	if (tuple->t_infomask & HEAP_XMAX_LOCK_ONLY)
		return true;

	/* invalid xmax means no update */
	if (!TransactionIdIsValid(HeapTupleHeaderGetRawXmax(tuple)))
		return true;

	/*
	 * if HEAP_XMAX_LOCK_ONLY is not set and not a multi, then this must
	 * necessarily have been updated
	 */
	if (!(tuple->t_infomask & HEAP_XMAX_IS_MULTI))
		return false;

	/* ... but if it's a multi, then perhaps the updating Xid aborted. */
	xmax = HeapTupleGetUpdateXid(tuple);

	/* not LOCKED_ONLY, so it has to have an xmax */
	Assert(TransactionIdIsValid(xmax));

	if (TransactionIdIsCurrentTransactionId(xmax))
		return false;

	xidstatus = TransactionIdGetStatus(xmax);
	if (xidstatus == XID_INPROGRESS)
		return false;
	if (xidstatus == XID_COMMITTED)
		return false;

	/*
	 * not current, not in progress, not committed -- must have aborted or
	 * crashed
	 */
	return true;
}

/*
 * check whether the transaction id 'xid' is in the pre-sorted array 'xip'.
 */
static bool
TransactionIdInArray(TransactionId xid, TransactionId *xip, Size num)
{
	return bsearch(&xid, xip, num,
				   sizeof(TransactionId), xidComparator) != NULL;
}

/*
 * See the comments for HeapTupleSatisfiesMVCC for the semantics this function
 * obeys.
 *
 * Only usable on tuples from catalog tables!
 *
 * We don't need to support HEAP_MOVED_(IN|OFF) for now because we only support
 * reading catalog pages which couldn't have been created in an older version.
 *
 * We don't set any hint bits in here as it seems unlikely to be beneficial as
 * those should already be set by normal access and it seems to be too
 * dangerous to do so as the semantics of doing so during timetravel are more
 * complicated than when dealing "only" with the present.
 */
bool
HeapTupleSatisfiesHistoricMVCC(HeapTuple htup, Snapshot snapshot,
							   Buffer buffer)
{
	HeapTupleHeader tuple = htup->t_data;
	TransactionId xmin = HeapTupleHeaderGetXmin(tuple);
	TransactionId xmax = HeapTupleHeaderGetRawXmax(tuple);
	TransactionIdStatus hintstatus;
#ifdef ENABLE_DISTRIBUTED_TRANSACTION
	bool 		  retry;
l1:
#endif

	Assert(ItemPointerIsValid(&htup->t_self));
	Assert(htup->t_tableOid != InvalidOid);

	/* inserting transaction aborted */
	if (HeapTupleHeaderXminInvalid(tuple))
	{
		Assert(!TransactionIdDidCommit(xmin));
		return false;
	}
	/* check if it's one of our txids, toplevel is also in there */
	else if (TransactionIdInArray(xmin, snapshot->this_xip, snapshot->this_xcnt))
	{
		bool		resolved;
		CommandId	cmin = HeapTupleHeaderGetRawCommandId(tuple);
		CommandId	cmax = InvalidCommandId;

		/*
		 * another transaction might have (tried to) delete this tuple or
		 * cmin/cmax was stored in a combocid. So we need to lookup the actual
		 * values externally.
		 */
		resolved = ResolveCminCmaxDuringDecoding(HistoricSnapshotGetTupleCids(),
												 snapshot,
												 htup, buffer,
												 &cmin, &cmax);

		if (!resolved)
			elog(ERROR, "could not resolve cmin/cmax of catalog tuple");

		Assert(cmin != InvalidCommandId);

		if (cmin >= snapshot->curcid)
			return false;		/* inserted after scan started */
		/* fall through */
	}
	/*
	 * it's not "this" transaction. Do a normal visibility check using the
	 * snapshot.
	 */
	#ifdef ENABLE_DISTRIBUTED_TRANSACTION
	else if (!XidVisibleInSnapshotDistri(tuple, xmin, snapshot, buffer, &hintstatus, &retry))
	{
		if (retry)
			goto l1;
		return false;
	}
	#else
	else if (!XidVisibleInSnapshot(xmin, snapshot, &hintstatus))
	{
		return false;
	}
	#endif

	/* at this point we know xmin is visible, go on to check xmax */

	/* xid invalid or aborted */
	if (tuple->t_infomask & HEAP_XMAX_INVALID)
		return true;
	/* locked tuples are always visible */
	else if (HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask))
		return true;

	/*
	 * We can see multis here if we're looking at user tables or if somebody
	 * SELECT ... FOR SHARE/UPDATE a system table.
	 */
	else if (tuple->t_infomask & HEAP_XMAX_IS_MULTI)
	{
		xmax = HeapTupleGetUpdateXid(tuple);
	}

	/* check if it's one of our txids, toplevel is also in there */
	if (TransactionIdInArray(xmax, snapshot->this_xip, snapshot->this_xcnt))
	{
		bool		resolved;
		CommandId	cmin;
		CommandId	cmax = HeapTupleHeaderGetRawCommandId(tuple);

		/* Lookup actual cmin/cmax values */
		resolved = ResolveCminCmaxDuringDecoding(HistoricSnapshotGetTupleCids(),
												 snapshot,
												 htup, buffer,
												 &cmin, &cmax);

		if (!resolved)
			elog(ERROR, "could not resolve combocid to cmax");

		Assert(cmax != InvalidCommandId);

		if (cmax >= snapshot->curcid)
			return true;		/* deleted after scan started */
		else
			return false;		/* deleted before scan started */
	}
	/*
	 * it's not "this" transaction. Do a normal visibility check using the
	 * snapshot.
	 */
	#ifdef ENABLE_DISTRIBUTED_TRANSACTION
	if (XidVisibleInSnapshotDistri(tuple, xmax, snapshot, buffer, &hintstatus, &retry))
	{
		if (retry)
			goto l1;
		return false;
	}
	else
	{
		return true;
	}
	#else
	if (XidVisibleInSnapshot(xmax, snapshot, &hintstatus))
		return false;
	else
		return true;
	#endif
}


/*
 * Check the visibility on a tuple with HEAP_MOVED flags set.
 *
 * Returns true if the tuple is visible, false otherwise. These flags are
 * no longer used, any such tuples must've come from binary upgrade of a
 * pre-9.0 system, so we can assume that the xid is long finished by now.
 */
static bool
IsMovedTupleVisible(HeapTuple htup, Buffer buffer)
{
	HeapTupleHeader tuple = htup->t_data;
	TransactionId xvac = HeapTupleHeaderGetXvac(tuple);
	TransactionIdStatus xidstatus;

	/*
	 * Check that the xvac is not a live transaction. This should never
	 * happen, because HEAP_MOVED flags are not set by current code.
	 */
	if (TransactionIdIsCurrentTransactionId(xvac))
		elog(ERROR, "HEAP_MOVED tuple with in-progress xvac: %u", xvac);

	xidstatus = TransactionIdGetStatus(xvac);

	if (tuple->t_infomask & HEAP_MOVED_OFF)
	{
		if (xidstatus == XID_COMMITTED)
		{
			SetHintBits(tuple, buffer, HEAP_XMIN_INVALID,
						InvalidTransactionId);
			return false;
		}
		else
		{
			SetHintBits(tuple, buffer, HEAP_XMIN_COMMITTED,
						InvalidTransactionId);
			return true;
		}
	}
	/* Used by pre-9.0 binary upgrades */
	else if (tuple->t_infomask & HEAP_MOVED_IN)
	{
		if (xidstatus == XID_COMMITTED)
		{
			SetHintBits(tuple, buffer, HEAP_XMIN_COMMITTED,
						InvalidTransactionId);
			return true;
		}
		else
		{
			SetHintBits(tuple, buffer, HEAP_XMIN_INVALID,
						InvalidTransactionId);
			return false;
		}
	}
	else
	{
		elog(ERROR, "IsMovedTupleVisible() called on a non-moved tuple");
		return true; /* keep compiler quiet */
	}
}
