/*-------------------------------------------------------------------------
 *
 * procarray.c
 *	  POSTGRES process array code.
 *
 *
 * This module maintains arrays of the PGPROC and PGXACT structures for all
 * active backends.  Although there are several uses for this, the principal
 * one is as a means of determining the set of currently running transactions.
 *
 * Because of various subtle race conditions it is critical that a backend
 * hold the correct locks while setting or clearing its MyPgXact->xid field.
 * See notes in src/backend/access/transam/README.
 *
 * The process arrays now also include structures representing prepared
 * transactions.  The xid fields of these are valid, as are the
 * myProcLocks lists.  They can be distinguished from regular backend PGPROCs
 * at need by checking for pid == 0.
 *
 * During hot standby, we update latestCompletedXid, oldestActiveXid, and
 * latestObservedXid, as we replay transaction commit/abort and standby WAL
 * records. Note that in hot standby, the PGPROC array represents standby
 * processes, which by definition are not running transactions that have XIDs.
 *
 *
 * We design a timestamp based MVCC garbage collection algorithm to
 * make the gc not to vacuum the tuple versions that are visible to
 * concurrent and pending transactions.
 *
 * The algorithm consists of two parts.
 * The first part is the admission of transaction execution. We reject
 * the transaction with global snapshot that may access the tuple versions
 * garbage collected.
 *
 * Specifically, we reject the snapshot with start ts < maxCommitTs - gc_interval
 * in order to resolve the conflict with vacuum and hot-chain cleanup.
 *
 * The parameter gc_interval represents the allowed maximum delay from
 * the snapshot generated on the coordinator to its arrival on data node.
 *
 * The second part is the determining of the oldest committs before which the tuple
 * versions can be pruned. See CommittsSatisfiesVacuum().
 * The oldest committs computation is implemented in GetRecentGlobalXmin() and GetOldestXmin().
 *
 * We also develop a global MVCC garbage collection which periodically collects
 * the minimal snapshot timestamp on each node to determine the globally minimal
 * timestamp.
 * Then we can use the globally minimal timestamp to vacuum stale tuple versions within
 * the cluster.
 *
 * Written by Junbin Kang, 2020.01.18
 *
 * Portions Copyright (c) 2020, Alibaba Group Holding Limited
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/storage/ipc/procarray.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <signal.h>

#include "access/clog.h"
#include "access/ctslog.h"
#include "access/mvccvars.h"
#include "access/transam.h"
#include "access/twophase.h"
#include "access/xact.h"
#include "access/xlog.h"
#include "catalog/catalog.h"
#include "distributed_txn/txn_timestamp.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "storage/proc.h"
#include "storage/procarray.h"
#include "storage/spin.h"
#include "utils/builtins.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"

#ifdef ENABLE_DISTRIBUTED_TRANSACTION
#include "utils/tqual.h"

#define 	SUBTRACT_GC_INTERVAL(ts, delta) ((ts) - ((delta) << 16))
#define 	SHIFT_GC_INTERVAL(delta) ((unsigned long)(delta) << 16)
/* User-settable GUC parameters */
int			gc_interval;
int			snapshot_delay;
static CommitSeqNo GetGlobalSnapshot(void);
static bool GlobalSnapshotIsAdmitted(Snapshot snapshot, CommitSeqNo * cutoffTs);
#endif

/* Our shared memory area */
typedef struct ProcArrayStruct
{
	int			numProcs;		/* number of valid procs entries */
	int			maxProcs;		/* allocated size of procs array */

	/* oldest xmin of any replication slot */
	TransactionId replication_slot_xmin;
	/* oldest catalog xmin of any replication slot */
	TransactionId replication_slot_catalog_xmin;

	/* indexes into allPgXact[], has PROCARRAY_MAXPROCS entries */
	int			pgprocnos[FLEXIBLE_ARRAY_MEMBER];
} ProcArrayStruct;

static ProcArrayStruct *procArray;

static PGPROC *allProcs;
static PGXACT * allPgXact;

/*
 * Cached values for GetRecentGlobalXmin().
 *
 * RecentGlobalXmin and RecentGlobalDataXmin are initialized to
 * InvalidTransactionId, to ensure that no one tries to use a stale
 * value. Readers should ensure that it has been set to something else
 * before using it.
 */
static int	XminCacheResetCounter = 0;
static TransactionId RecentGlobalXmin = InvalidTransactionId;
static TransactionId RecentGlobalDataXmin = InvalidTransactionId;
#ifdef ENABLE_DISTRIBUTED_TRANSACTION
static CommitSeqNo GlobalCutoffTs = InvalidCommitSeqNo;
CommitSeqNo RecentGlobalTs = InvalidCommitSeqNo;
bool		txnUseGlobalSnapshot = false;
#endif

/*
 * Bookkeeping for tracking transactions in recovery
 */
static TransactionId latestObservedXid = InvalidTransactionId;

#ifdef ENABLE_DISTRIBUTED_TRANSACTION
bool
CommittsSatisfiesVacuum(CommitSeqNo committs)
{
#ifdef ENABLE_DISTR_DEBUG
	if (enable_timestamp_debug_print)
		elog(LOG, "committs satisfy vacuum res %d cts " UINT64_FORMAT " cutoff " UINT64_FORMAT " MyTmin " UINT64_FORMAT,
			 committs < GlobalCutoffTs,
			 committs, GlobalCutoffTs,
			 pg_atomic_read_u64(&MyPgXact->tmin));
#endif

	if (committs < GlobalCutoffTs)
		return true;
	else
		return false;
}
#endif
/*
 * Report shared-memory space needed by CreateSharedProcArray.
 */
Size
ProcArrayShmemSize(void)
{
	Size		size;

	/* Size of the ProcArray structure itself */
#define PROCARRAY_MAXPROCS	(MaxBackends + max_prepared_xacts)

	size = offsetof(ProcArrayStruct, pgprocnos);
	size = add_size(size, mul_size(sizeof(int), PROCARRAY_MAXPROCS));

	return size;
}

/*
 * Initialize the shared PGPROC array during postmaster startup.
 */
void
CreateSharedProcArray(void)
{
	bool		found;

	/* Create or attach to the ProcArray shared structure */
	procArray = (ProcArrayStruct *)
		ShmemInitStruct("Proc Array",
						add_size(offsetof(ProcArrayStruct, pgprocnos),
								 mul_size(sizeof(int),
										  PROCARRAY_MAXPROCS)),
						&found);

	if (!found)
	{
		/*
		 * We're the first - initialize.
		 */
		procArray->numProcs = 0;
		procArray->maxProcs = PROCARRAY_MAXPROCS;
		procArray->replication_slot_xmin = InvalidTransactionId;
		procArray->replication_slot_catalog_xmin = InvalidTransactionId;
	}

	allProcs = ProcGlobal->allProcs;
	allPgXact = ProcGlobal->allPgXact;

	/* Register and initialize fields of ProcLWLockTranche */
	LWLockRegisterTranche(LWTRANCHE_PROC, "proc");
}

/*
 * Add the specified PGPROC to the shared array.
 */
void
ProcArrayAdd(PGPROC *proc)
{
	ProcArrayStruct *arrayP = procArray;
	int			index;

	LWLockAcquire(ProcArrayLock, LW_EXCLUSIVE);

	if (arrayP->numProcs >= arrayP->maxProcs)
	{
		/*
		 * Oops, no room.  (This really shouldn't happen, since there is a
		 * fixed supply of PGPROC structs too, and so we should have failed
		 * earlier.)
		 */
		LWLockRelease(ProcArrayLock);
		ereport(FATAL,
				(errcode(ERRCODE_TOO_MANY_CONNECTIONS),
				 errmsg("sorry, too many clients already")));
	}

	/*
	 * Keep the procs array sorted by (PGPROC *) so that we can utilize
	 * locality of references much better. This is useful while traversing the
	 * ProcArray because there is an increased likelihood of finding the next
	 * PGPROC structure in the cache.
	 *
	 * Since the occurrence of adding/removing a proc is much lower than the
	 * access to the ProcArray itself, the overhead should be marginal
	 */
	for (index = 0; index < arrayP->numProcs; index++)
	{
		/*
		 * If we are the first PGPROC or if we have found our right position
		 * in the array, break
		 */
		if ((arrayP->pgprocnos[index] == -1) || (arrayP->pgprocnos[index] > proc->pgprocno))
			break;
	}

	memmove(&arrayP->pgprocnos[index + 1], &arrayP->pgprocnos[index],
			(arrayP->numProcs - index) * sizeof(int));
	arrayP->pgprocnos[index] = proc->pgprocno;
	arrayP->numProcs++;

	LWLockRelease(ProcArrayLock);
}

/*
 * Remove the specified PGPROC from the shared array.
 */
void
ProcArrayRemove(PGPROC *proc)
{
	ProcArrayStruct *arrayP = procArray;
	int			index;

	LWLockAcquire(ProcArrayLock, LW_EXCLUSIVE);

	for (index = 0; index < arrayP->numProcs; index++)
	{
		if (arrayP->pgprocnos[index] == proc->pgprocno)
		{
			/* Keep the PGPROC array sorted. See notes above */
			memmove(&arrayP->pgprocnos[index], &arrayP->pgprocnos[index + 1],
					(arrayP->numProcs - index - 1) * sizeof(int));
			arrayP->pgprocnos[arrayP->numProcs - 1] = -1;	/* for debugging */
			arrayP->numProcs--;
			LWLockRelease(ProcArrayLock);
			return;
		}
	}

	/* Oops */
	LWLockRelease(ProcArrayLock);

	elog(LOG, "failed to find proc %p in ProcArray", proc);
}

static void
resetGlobalXminCache(void)
{
	if (++XminCacheResetCounter == 13)
	{
		XminCacheResetCounter = 0;
		RecentGlobalXmin = InvalidTransactionId;
		RecentGlobalDataXmin = InvalidTransactionId;
	}
}

/*
 * ProcArrayEndTransaction -- mark a transaction as no longer running
 *
 * This is used interchangeably for commit and abort cases.  The transaction
 * commit/abort must already be reported to WAL and pg_xact.
 *
 * proc is currently always MyProc, but we pass it explicitly for flexibility.
 */
void
ProcArrayEndTransaction(PGPROC *proc)
{
	PGXACT	   *pgxact = &allPgXact[proc->pgprocno];
	TransactionId myXid;

	myXid = pgxact->xid;

	/* A shared lock is enough to modify our own fields */
	LWLockAcquire(ProcArrayLock, LW_SHARED);
	pgxact->xid = InvalidTransactionId;
	proc->lxid = InvalidLocalTransactionId;
	pgxact->xmin = InvalidTransactionId;
#ifdef ENABLE_DISTRIBUTED_TRANSACTION
	pg_atomic_write_u64(&pgxact->tmin, InvalidCommitSeqNo);
#endif
	pgxact->snapshotcsn = InvalidCommitSeqNo;
	/* must be cleared with xid/xmin/snapshotcsn: */
	pgxact->vacuumFlags &= ~PROC_VACUUM_STATE_MASK;
	pgxact->delayChkpt = false; /* be sure this is cleared in abort */
	proc->recoveryConflictPending = false;

	LWLockRelease(ProcArrayLock);

	/* If we were the oldest active XID, advance oldestXid */
	if (TransactionIdIsValid(myXid))
		AdvanceOldestActiveXid(myXid);

	/* Reset cached variables */
	resetGlobalXminCache();
}

void
ProcArrayResetXmin(PGPROC *proc)
{
	PGXACT	   *pgxact = &allPgXact[proc->pgprocno];

	/*
	 * Note we can do this without locking because we assume that storing an
	 * Xid is atomic.
	 */
	pgxact->xmin = InvalidTransactionId;
	/* Reset cached variables */
	resetGlobalXminCache();
}

/*
 * ProcArrayClearTransaction -- clear the transaction fields
 *
 * This is used after successfully preparing a 2-phase transaction.  We are
 * not actually reporting the transaction's XID as no longer running --- it
 * will still appear as running because the 2PC's gxact is in the ProcArray
 * too.  We just have to clear out our own PGXACT.
 */
void
ProcArrayClearTransaction(PGPROC *proc)
{
	PGXACT	   *pgxact = &allPgXact[proc->pgprocno];

	/*
	 * We can skip locking ProcArrayLock here, because this action does not
	 * actually change anyone's view of the set of running XIDs: our entry is
	 * duplicate with the gxact that has already been inserted into the
	 * ProcArray.
	 */
	pgxact->xid = InvalidTransactionId;
	proc->lxid = InvalidLocalTransactionId;
	pgxact->xmin = InvalidTransactionId;
	pgxact->snapshotcsn = InvalidCommitSeqNo;
	proc->recoveryConflictPending = false;

	/* redundant, but just in case */
	pgxact->vacuumFlags &= ~PROC_VACUUM_STATE_MASK;
	pgxact->delayChkpt = false;

	/*
	 * We don't need to update oldestActiveXid, because the gxact entry in the
	 * procarray is still running with the same XID.
	 */

	/* Reset cached variables */
	RecentGlobalXmin = InvalidTransactionId;
	RecentGlobalDataXmin = InvalidTransactionId;
#ifdef ENABLE_DISTRIBUTED_TRANSACTION
	GlobalCutoffTs = InvalidCommitSeqNo;
	pg_atomic_write_u64(&pgxact->tmin, InvalidCommitSeqNo);
#endif
}

/*
 * ProcArrayInitRecovery -- initialize recovery xid mgmt environment
 *
 * Remember up to where the startup process initialized the CLOG and CSNLOG
 * so we can ensure it's initialized gaplessly up to the point where necessary
 * while in recovery.
 */
void
ProcArrayInitRecovery(TransactionId oldestActiveXID, TransactionId initializedUptoXID)
{
	Assert(standbyState == STANDBY_INITIALIZED);
	Assert(TransactionIdIsNormal(initializedUptoXID));

	/*
	 * we set latestObservedXid to the xid SUBTRANS (XXX csnlog?) has been
	 * initialized up to, so we can extend it from that point onwards in
	 * RecordKnownAssignedTransactionIds, and when we get consistent in
	 * ProcArrayApplyRecoveryInfo().
	 */
	latestObservedXid = initializedUptoXID;
	TransactionIdRetreat(latestObservedXid);

	/* also initialize oldestActiveXid */
	pg_atomic_write_u32(&ShmemVariableCache->oldestActiveXid, oldestActiveXID);
}

/*
 * ProcArrayApplyRecoveryInfo -- apply recovery info about xids
 *
 * Takes us through 3 states: Initialized, Pending and Ready.
 * Normal case is to go all the way to Ready straight away, though there
 * are atypical cases where we need to take it in steps.
 *
 * Use the data about running transactions on master to create the initial
 * state of KnownAssignedXids. We also use these records to regularly prune
 * KnownAssignedXids because we know it is possible that some transactions
 * with FATAL errors fail to write abort records, which could cause eventual
 * overflow.
 *
 * See comments for LogStandbySnapshot().
 */
void
ProcArrayApplyRecoveryInfo(RunningTransactions running)
{
	TransactionId nextXid;

	Assert(standbyState >= STANDBY_INITIALIZED);
	Assert(TransactionIdIsValid(running->nextXid));
	Assert(TransactionIdIsValid(running->oldestRunningXid));

	/*
	 * Remove stale locks, if any.
	 */
	StandbyReleaseOldLocks(running->oldestRunningXid);

	/*
	 * If our snapshot is already valid, nothing else to do...
	 */
	if (standbyState == STANDBY_SNAPSHOT_READY)
		return;

	Assert(standbyState == STANDBY_INITIALIZED);

	/*
	 * OK, we need to initialise from the RunningTransactionsData record.
	 *
	 * NB: this can be reached at least twice, so make sure new code can deal
	 * with that.
	 */

	/*
	 * latestObservedXid is at least set to the point where CSNLOG was started
	 * up to (c.f. ProcArrayInitRecovery()) or to the biggest xid
	 * RecordKnownAssignedTransactionIds() (FIXME: gone!) was called for.
	 * Initialize csnlog from thereon, up to nextXid - 1.
	 *
	 * We need to duplicate parts of RecordKnownAssignedTransactionId() here,
	 * because we've just added xids to the known assigned xids machinery that
	 * haven't gone through RecordKnownAssignedTransactionId().
	 */
	Assert(TransactionIdIsNormal(latestObservedXid));
	TransactionIdAdvance(latestObservedXid);
	while (TransactionIdPrecedes(latestObservedXid, running->nextXid))
	{
#ifdef ENABLE_DISTRIBUTED_TRANSACTION
		/*
		 * Each time the transaction/subtransaction id is allocated, we have
		 * written CTS extend WAL entry. As a result, no extra CTS extend is
		 * needed here to avoid nested WAL. Written by Junbin Kang, 2020.06.16
		 */
#else
		ExtendCSNLOG(latestObservedXid);
#endif
		TransactionIdAdvance(latestObservedXid);
	}
	TransactionIdRetreat(latestObservedXid);	/* = running->nextXid - 1 */

	/*
	 * ShmemVariableCache->nextXid must be beyond any observed xid.
	 *
	 * We don't expect anyone else to modify nextXid, hence we don't need to
	 * hold a lock while examining it.  We still acquire the lock to modify
	 * it, though.
	 */
	nextXid = latestObservedXid;
	TransactionIdAdvance(nextXid);
	if (TransactionIdFollows(nextXid, ShmemVariableCache->nextXid))
	{
		LWLockAcquire(XidGenLock, LW_EXCLUSIVE);
		ShmemVariableCache->nextXid = nextXid;
		LWLockRelease(XidGenLock);
	}

	Assert(TransactionIdIsValid(ShmemVariableCache->nextXid));

	standbyState = STANDBY_SNAPSHOT_READY;
	elog(trace_recovery(DEBUG1), "recovery snapshots are now enabled");
}

/*
 * TransactionIdIsActive -- is xid the top-level XID of an active backend?
 *
 * This ignores prepared transactions and subtransactions, since that's not
 * needed for current uses.
 */
bool
TransactionIdIsActive(TransactionId xid)
{
	bool		result = false;
	ProcArrayStruct *arrayP = procArray;
	int			i;

	LWLockAcquire(ProcArrayLock, LW_SHARED);

	for (i = 0; i < arrayP->numProcs; i++)
	{
		int			pgprocno = arrayP->pgprocnos[i];
		volatile PGPROC *proc = &allProcs[pgprocno];
		volatile	PGXACT *pgxact = &allPgXact[pgprocno];
		TransactionId pxid;

		/* Fetch xid just once - see GetNewTransactionId */
		pxid = pgxact->xid;

		if (!TransactionIdIsValid(pxid))
			continue;

		if (proc->pid == 0)
			continue;			/* ignore prepared transactions */

		if (TransactionIdEquals(pxid, xid))
		{
			result = true;
			break;
		}
	}

	LWLockRelease(ProcArrayLock);

	return result;
}

/*
 * AdvanceOldestActiveXid --
 *
 * Advance oldestActiveXid. 'oldXid' is the current value, and it's known to be
 * finished now.
 */
void
AdvanceOldestActiveXid(TransactionId myXid)
{
	TransactionId nextXid;
	TransactionId xid;
	TransactionId oldValue;

	oldValue = pg_atomic_read_u32(&ShmemVariableCache->oldestActiveXid);

	/* Quick exit if we were not the oldest active XID. */
	if (myXid != oldValue)
		return;

	xid = myXid;
	TransactionIdAdvance(xid);

	for (;;)
	{
		/*
		 * Current nextXid is the upper bound, if there are no transactions
		 * active at all.
		 */
		/* assume we can read nextXid atomically without holding XidGenlock. */
		nextXid = ShmemVariableCache->nextXid;
		/* Scan the CSN Log for the next active xid */
		xid = CTSLogGetNextActiveXid(xid, nextXid);

		if (xid == oldValue)
		{
			/* nothing more to do */
			break;
		}

		/*
		 * Update oldestActiveXid with that value.
		 */
		if (!pg_atomic_compare_exchange_u32(&ShmemVariableCache->oldestActiveXid,
											&oldValue,
											xid))
		{
			/*
			 * Someone beat us to it. This can happen if we hit the race
			 * condition described below. That's OK. We're no longer the
			 * oldest active XID in that case, so we're done.
			 */
			Assert(TransactionIdFollows(oldValue, myXid));
			break;
		}

		/*
		 * We're not necessarily done yet. It's possible that the XID that we
		 * saw as still running committed just before we updated
		 * oldestActiveXid. She didn't see herself as the oldest transaction,
		 * so she wouldn't update oldestActiveXid. Loop back to check the XID
		 * that we saw as the oldest in-progress one is still in-progress, and
		 * if not, update oldestActiveXid again, on behalf of that
		 * transaction.
		 */
		oldValue = xid;
	}
}


/*
 * This is like GetOldestXmin(NULL, true), but can return slightly stale, cached value.
 *
 * --------------------------------------------------------------------------------------
 * We design a timestamp based MVCC garbage collection algorithm to
 * make the gc not to vacuum the tuple versions that are visible to
 * concurrent and pending transactions.
 *
 * The algorithm consists of two parts.
 * The first part is the admission of transaction execution. We reject
 * the transaction with global snapshot that may access the tuple versions
 * garbage collected.
 *
 * Specifically, we reject the snapshot with start ts < maxCommitTs - gc_interval
 * in order to resolve the conflict with vacuum and hot-chain cleanup.
 *
 * The parameter gc_interval represents the allowed maximum delay from
 * the snapshot generated on the coordinator to its arrival on data node.
 *
 * The second part is the determining of the oldest committs before which the tuple
 * versions can be pruned. See CommittsSatisfiesVacuum().
 * The oldest committs computation is implemented in GetRecentGlobalXmin() and GetOldestXmin().
 * --------------------------------------------------------------------------------------
 * Written by Junbin Kang, 2020.01.18
 */
TransactionId
GetRecentGlobalXmin(void)
{
	TransactionId globalXmin;
	ProcArrayStruct *arrayP = procArray;
	int			index;
	volatile TransactionId replication_slot_xmin = InvalidTransactionId;
	volatile TransactionId replication_slot_catalog_xmin = InvalidTransactionId;
#ifdef ENABLE_DISTRIBUTED_TRANSACTION
	CommitSeqNo cutoffTs;
#endif

	if (TransactionIdIsValid(RecentGlobalXmin))
		return RecentGlobalXmin;

#ifdef ENABLE_DISTRIBUTED_TRANSACTION
	SpinLockAcquire(&ShmemVariableCache->ts_lock);
	cutoffTs = ShmemVariableCache->maxCommitTs;
	SpinLockRelease(&ShmemVariableCache->ts_lock);
	if (cutoffTs < SHIFT_GC_INTERVAL(gc_interval))
		elog(ERROR, "maxCommitTs " UINT64_FORMAT " is smaller than gc_interval " UINT64_FORMAT,
			 cutoffTs, SHIFT_GC_INTERVAL(gc_interval));
	cutoffTs = cutoffTs - SHIFT_GC_INTERVAL(gc_interval);
	pg_memory_barrier();
	if (enable_timestamp_debug_print)
		elog(LOG, "GetRecentGlobalXmin: Caculate cutoff ts " UINT64_FORMAT, cutoffTs);
#endif

	LWLockAcquire(ProcArrayLock, LW_SHARED);

	/*
	 * We initialize the MIN() calculation with oldestActiveXid. This is a
	 * lower bound for the XIDs that might appear in the ProcArray later, and
	 * so protects us against overestimating the result due to future
	 * additions.
	 */
	globalXmin = pg_atomic_read_u32(&ShmemVariableCache->oldestActiveXid);
	Assert(TransactionIdIsNormal(globalXmin));

	for (index = 0; index < arrayP->numProcs; index++)
	{
		int			pgprocno = arrayP->pgprocnos[index];
		volatile	PGXACT *pgxact = &allPgXact[pgprocno];
		TransactionId xmin = pgxact->xmin;
#ifdef ENABLE_DISTRIBUTED_TRANSACTION
		CommitSeqNo tmin;
#endif


		/*
		 * Backend is doing logical decoding which manages xmin separately,
		 * check below.
		 */
		if (pgxact->vacuumFlags & PROC_IN_LOGICAL_DECODING)
			continue;

		if (pgxact->vacuumFlags & PROC_IN_VACUUM)
			continue;

		/*
		 * Consider the transaction's Xmin, if set.
		 */
		if (TransactionIdIsNormal(xmin) &&
			NormalTransactionIdPrecedes(xmin, globalXmin))
			globalXmin = xmin;


#ifdef ENABLE_DISTRIBUTED_TRANSACTION
		tmin = pg_atomic_read_u64(&pgxact->tmin);

		if (COMMITSEQNO_IS_NORMAL(tmin) && tmin < cutoffTs)
		{
			cutoffTs = tmin;
			if (enable_timestamp_debug_print)
				elog(LOG, "GetRecentGlobalXmin: update cutoff ts " UINT64_FORMAT, tmin);
		}
#endif

	}

	/* fetch into volatile var while ProcArrayLock is held */
	replication_slot_xmin = procArray->replication_slot_xmin;
	replication_slot_catalog_xmin = procArray->replication_slot_catalog_xmin;

	LWLockRelease(ProcArrayLock);

	/* Update cached variables */
	RecentGlobalXmin = globalXmin - vacuum_defer_cleanup_age;
	if (!TransactionIdIsNormal(RecentGlobalXmin))
		RecentGlobalXmin = FirstNormalTransactionId;

	/* Check whether there's a replication slot requiring an older xmin. */
	if (TransactionIdIsValid(replication_slot_xmin) &&
		NormalTransactionIdPrecedes(replication_slot_xmin, RecentGlobalXmin))
		RecentGlobalXmin = replication_slot_xmin;

	/* Non-catalog tables can be vacuumed if older than this xid */
	RecentGlobalDataXmin = RecentGlobalXmin;

#ifdef ENABLE_DISTRIBUTED_TRANSACTION
	if (enable_global_cutoffts)
	{
		SpinLockAcquire(&ShmemVariableCache->ts_lock);
		GlobalCutoffTs = ShmemVariableCache->globalCutoffTs;
		SpinLockRelease(&ShmemVariableCache->ts_lock);
	}
	else
		GlobalCutoffTs = cutoffTs;
	if (enable_timestamp_debug_print)
		elog(LOG, "GetRecentGlobalXmin: Caculate global cutoff ts " UINT64_FORMAT, GlobalCutoffTs);
#endif

	/*
	 * Check whether there's a replication slot requiring an older catalog
	 * xmin.
	 */
	if (TransactionIdIsNormal(replication_slot_catalog_xmin) &&
		NormalTransactionIdPrecedes(replication_slot_catalog_xmin, RecentGlobalXmin))
		RecentGlobalXmin = replication_slot_catalog_xmin;

	return RecentGlobalXmin;
}
#ifdef ENABLE_DISTRIBUTED_TRANSACTION
CommitSeqNo
GetRecentGlobalTmin(void)
{
	return GlobalCutoffTs;
}
#endif
TransactionId
GetRecentGlobalDataXmin(void)
{
	if (TransactionIdIsValid(RecentGlobalDataXmin))
		return RecentGlobalDataXmin;

	(void) GetRecentGlobalXmin();
	Assert(TransactionIdIsValid(RecentGlobalDataXmin));

	return RecentGlobalDataXmin;
}

/*
 * GetOldestXmin -- returns oldest transaction that was running
 *					when any current transaction was started.
 *
 * If rel is NULL or a shared relation, all backends are considered, otherwise
 * only backends running in this database are considered.
 *
 * The flags are used to ignore the backends in calculation when any of the
 * corresponding flags is set. Typically, if you want to ignore ones with
 * PROC_IN_VACUUM flag, you can use PROCARRAY_FLAGS_VACUUM.
 *
 * PROCARRAY_SLOTS_XMIN causes GetOldestXmin to ignore the xmin and
 * catalog_xmin of any replication slots that exist in the system when
 * calculating the oldest xmin.
 *
 * This is used by VACUUM to decide which deleted tuples must be preserved in
 * the passed in table. For shared relations backends in all databases must be
 * considered, but for non-shared relations that's not required, since only
 * backends in my own database could ever see the tuples in them. Also, we can
 * ignore concurrently running lazy VACUUMs because (a) they must be working
 * on other tables, and (b) they don't need to do snapshot-based lookups.
 *
 * This is also used to determine where to truncate pg_csnlog. For that
 * backends in all databases have to be considered, so rel = NULL has to be
 * passed in.
 *
 * Note: we include all currently running xids in the set of considered xids.
 * This ensures that if a just-started xact has not yet set its snapshot,
 * when it does set the snapshot it cannot set xmin less than what we compute.
 * See notes in src/backend/access/transam/README.
 *
 * Note: despite the above, it's possible for the calculated value to move
 * backwards on repeated calls. The calculated value is conservative, so that
 * anything older is definitely not considered as running by anyone anymore,
 * but the exact value calculated depends on a number of things. For example,
 * if rel = NULL and there are no transactions running in the current
 * database, GetOldestXmin() returns latestCompletedXid. If a transaction
 * begins after that, its xmin will include in-progress transactions in other
 * databases that started earlier, so another call will return a lower value.
 * Nonetheless it is safe to vacuum a table in the current database with the
 * first result.  There are also replication-related effects: a walsender
 * process can set its xmin based on transactions that are no longer running
 * in the master but are still being replayed on the standby, thus possibly
 * making the GetOldestXmin reading go backwards.  In this case there is a
 * possibility that we lose data that the standby would like to have, but
 * unless the standby uses a replication slot to make its xmin persistent
 * there is little we can do about that --- data is only protected if the
 * walsender runs continuously while queries are executed on the standby.
 * (The Hot Standby code deals with such cases by failing standby queries
 * that needed to access already-removed data, so there's no integrity bug.)
 * The return value is also adjusted with vacuum_defer_cleanup_age, so
 * increasing that setting on the fly is another easy way to make
 * GetOldestXmin() move backwards, with no consequences for data integrity.
 *
 *
 * XXX: We track GlobalXmin in shared memory now. Would it makes sense to
 * have GetOldestXmin() just return that? At least for the rel == NULL case.
 *
 *
 * --------------------------------------------------------------------------------------
 * We design a timestamp based MVCC garbage collection algorithm to
 * make the gc not to vacuum the tuple versions that are visible to
 * concurrent and pending transactions.
 *
 * The algorithm consists of two parts.
 * The first part is the admission of transaction execution. We reject
 * the transaction with global snapshot that may access the tuple versions
 * garbage collected.
 *
 * Specifically, we reject the snapshot with start ts < maxCommitTs - gc_interval
 * in order to resolve the conflict with vacuum and hot-chain cleanup.
 *
 * The parameter gc_interval represents the allowed maximum delay from
 * the snapshot generated on the coordinator to its arrival on data node.
 *
 * The second part is the determining of the oldest committs before which the tuple
 * versions can be pruned. See CommittsSatisfiesVacuum().
 * The oldest committs computation is implemented in GetRecentGlobalXmin() and GetOldestXmin().
 * --------------------------------------------------------------------------------------
 * Written by Junbin Kang, 2020.01.18
 */

TransactionId
GetOldestXmin(Relation rel, int flags)
{
	ProcArrayStruct *arrayP = procArray;
	TransactionId result;
	int			index;
	bool		allDbs;
#ifdef ENABLE_DISTRIBUTED_TRANSACTION
	CommitSeqNo cutoffTs;
#endif

	volatile TransactionId replication_slot_xmin = InvalidTransactionId;
	volatile TransactionId replication_slot_catalog_xmin = InvalidTransactionId;

	/*
	 * If we're not computing a relation specific limit, or if a shared
	 * relation has been passed in, backends in all databases have to be
	 * considered.
	 */
	allDbs = rel == NULL || rel->rd_rel->relisshared;

	/* Cannot look for individual databases during recovery */
	Assert(allDbs || !RecoveryInProgress());

#ifdef ENABLE_DISTRIBUTED_TRANSACTION
	SpinLockAcquire(&ShmemVariableCache->ts_lock);
	cutoffTs = ShmemVariableCache->maxCommitTs;
	SpinLockRelease(&ShmemVariableCache->ts_lock);
	if (cutoffTs < SHIFT_GC_INTERVAL(gc_interval))
		elog(ERROR, "maxCommitTs " UINT64_FORMAT " is smaller than gc_interval " UINT64_FORMAT,
			 cutoffTs, SHIFT_GC_INTERVAL(gc_interval));
	cutoffTs = cutoffTs - SHIFT_GC_INTERVAL(gc_interval);
	pg_memory_barrier();
	if (enable_timestamp_debug_print)
		elog(LOG, "GetRecentGlobalXmin: Caculate cutoff ts " UINT64_FORMAT, cutoffTs);
#endif
	LWLockAcquire(ProcArrayLock, LW_SHARED);

	/*
	 * We initialize the MIN() calculation with latestCompletedXid + 1. This
	 * is a lower bound for the XIDs that might appear in the ProcArray later,
	 * and so protects us against overestimating the result due to future
	 * additions.
	 */
	result = pg_atomic_read_u32(&ShmemVariableCache->latestCompletedXid);
	Assert(TransactionIdIsNormal(result));
	TransactionIdAdvance(result);

	for (index = 0; index < arrayP->numProcs; index++)
	{
		int			pgprocno = arrayP->pgprocnos[index];
		volatile PGPROC *proc = &allProcs[pgprocno];
		volatile	PGXACT *pgxact = &allPgXact[pgprocno];

		if (pgxact->vacuumFlags & (flags & PROCARRAY_PROC_FLAGS_MASK))
			continue;

		if (allDbs ||
			proc->databaseId == MyDatabaseId ||
			proc->databaseId == 0)	/* always include WalSender */
		{
			/* Fetch xid just once - see GetNewTransactionId */
			TransactionId xid = pgxact->xid;
#ifdef ENABLE_DISTRIBUTED_TRANSACTION
			CommitSeqNo tmin;
#endif

			/* First consider the transaction's own Xid, if any */
			if (TransactionIdIsNormal(xid) &&
				TransactionIdPrecedes(xid, result))
				result = xid;

			/*
			 * Also consider the transaction's Xmin, if set.
			 *
			 * We must check both Xid and Xmin because a transaction might
			 * have an Xmin but not (yet) an Xid; conversely, if it has an
			 * Xid, that could determine some not-yet-set Xmin.
			 */
			xid = pgxact->xmin; /* Fetch just once */
			if (TransactionIdIsNormal(xid) &&
				TransactionIdPrecedes(xid, result))
				result = xid;
#ifdef ENABLE_DISTRIBUTED_TRANSACTION
			tmin = pg_atomic_read_u64(&pgxact->tmin);
			if (COMMITSEQNO_IS_NORMAL(tmin) && tmin < cutoffTs)
			{
				if (enable_timestamp_debug_print)
					elog(LOG, "GetRecentGlobalXmin: update cutoff ts " UINT64_FORMAT, tmin);
				cutoffTs = tmin;
			}
#endif
		}
	}

#ifdef ENABLE_DISTRIBUTED_TRANSACTION
	if (enable_global_cutoffts)
	{
		SpinLockAcquire(&ShmemVariableCache->ts_lock);
		GlobalCutoffTs = ShmemVariableCache->globalCutoffTs;
		SpinLockRelease(&ShmemVariableCache->ts_lock);
	}
	else
		GlobalCutoffTs = cutoffTs;

	if (enable_timestamp_debug_print)
		elog(LOG, "GetOldestXmin: Caculate global cutoff ts " UINT64_FORMAT, GlobalCutoffTs);

#endif
	/* fetch into volatile var while ProcArrayLock is held */
	replication_slot_xmin = procArray->replication_slot_xmin;
	replication_slot_catalog_xmin = procArray->replication_slot_catalog_xmin;

	LWLockRelease(ProcArrayLock);

	if (!RecoveryInProgress())
	{
		/*
		 * Compute the cutoff XID by subtracting vacuum_defer_cleanup_age,
		 * being careful not to generate a "permanent" XID.
		 *
		 * vacuum_defer_cleanup_age provides some additional "slop" for the
		 * benefit of hot standby queries on standby servers.  This is quick
		 * and dirty, and perhaps not all that useful unless the master has a
		 * predictable transaction rate, but it offers some protection when
		 * there's no walsender connection.  Note that we are assuming
		 * vacuum_defer_cleanup_age isn't large enough to cause wraparound ---
		 * so guc.c should limit it to no more than the xidStopLimit threshold
		 * in varsup.c.  Also note that we intentionally don't apply
		 * vacuum_defer_cleanup_age on standby servers.
		 */
		result -= vacuum_defer_cleanup_age;
		if (!TransactionIdIsNormal(result))
			result = FirstNormalTransactionId;
	}

	/*
	 * Check whether there are replication slots requiring an older xmin.
	 */
	if (!(flags & PROCARRAY_SLOTS_XMIN) &&
		TransactionIdIsValid(replication_slot_xmin) &&
		NormalTransactionIdPrecedes(replication_slot_xmin, result))
		result = replication_slot_xmin;

	/*
	 * After locks have been released and defer_cleanup_age has been applied,
	 * check whether we need to back up further to make logical decoding
	 * possible. We need to do so if we're computing the global limit (rel =
	 * NULL) or if the passed relation is a catalog relation of some kind.
	 */
	if (!(flags & PROCARRAY_SLOTS_XMIN) &&
		(rel == NULL ||
		 RelationIsAccessibleInLogicalDecoding(rel)) &&
		TransactionIdIsValid(replication_slot_catalog_xmin) &&
		NormalTransactionIdPrecedes(replication_slot_catalog_xmin, result))
		result = replication_slot_catalog_xmin;

	return result;
}

#ifdef ENABLE_DISTRIBUTED_TRANSACTION
static CommitSeqNo
GetGlobalSnapshot(void)
{
	CommitSeqNo snapshotcsn;
	/*
	 * Todo: we should assign logical timestamp max_ts for
	 * both distributed and local transactions.
	 */
	SpinLockAcquire(&ShmemVariableCache->ts_lock);
	snapshotcsn = ShmemVariableCache->maxCommitTs;
	SpinLockRelease(&ShmemVariableCache->ts_lock);
	if (snapshotcsn < COMMITSEQNO_FIRST_NORMAL)
		elog(ERROR, "csn abnormal "UINT64_FORMAT, snapshotcsn);
	return snapshotcsn;

}
/*
 * Support cluster-wide global vacuum.
 * oldest Tmin = min{max_ts, min{per-Proc's Tmin}}
 * max_ts is the local hybrid logical timestamp.
 *
 * As ProcArrayLock is held by both GetOldestTmin and GetSnapshot,
 * we can guarantee that no concurrent transactions would be assigned
 * start timestamps smaller than the returen oldest tmin.
 *
 * Furthermore, per-proc lock may be used to reduce the conection, but
 * it seems unnecessary now as ProcArrayLock is acquired in shared mode
 * in most critical path.
 *
 * Written by Junbin Kang
 */
CommitSeqNo
GetOldestTmin(void)
{
	ProcArrayStruct *arrayP = procArray;
	int			index;
	CommitSeqNo globalTmin = InvalidCommitSeqNo;
	CommitSeqNo tmin;

	LWLockAcquire(ProcArrayLock, LW_EXCLUSIVE);

	globalTmin = LogicalClockNow();

	for (index = 0; index < arrayP->numProcs; index++)
	{
		int			pgprocno = arrayP->pgprocnos[index];
		volatile	PGXACT *pgxact = &allPgXact[pgprocno];
		volatile PGPROC *pgproc = &allProcs[pgprocno];

		/*
		 * Backend is doing logical decoding which manages xmin separately,
		 * check below.
		 */
		if (pgxact->vacuumFlags & PROC_IN_LOGICAL_DECODING)
			continue;

		if (pgxact->vacuumFlags & PROC_IN_VACUUM)
			continue;

		tmin = pg_atomic_read_u64(&pgxact->tmin);

		if (COMMITSEQNO_IS_NORMAL(tmin))
		{
			if (enable_timestamp_debug_print)
				elog(LOG, "Proc %d: "
					 " tmin=" LOGICALTIME_FORMAT
					 " xid=%d"
					 " pid=%d"
					 " csn=" LOGICALTIME_FORMAT,
					 pgprocno,
					 LOGICALTIME_STRING(tmin),
					 pgxact->xid,
					 pgproc->pid,
					 LOGICALTIME_STRING(pgxact->snapshotcsn));

			if (tmin < globalTmin)
				globalTmin = tmin;
		}
	}

	LWLockRelease(ProcArrayLock);

	if (enable_timestamp_debug_print)
		elog(LOG, "Get global oldest tmin " UINT64_FORMAT, globalTmin);

	return globalTmin;
}

void
SetGlobalCutoffTs(CommitSeqNo cutoffTs)
{
	SpinLockAcquire(&ShmemVariableCache->ts_lock);
	ShmemVariableCache->globalCutoffTs = cutoffTs;
	SpinLockRelease(&ShmemVariableCache->ts_lock);

	if (enable_timestamp_debug_print)
		elog(LOG, "Set global cutoff " UINT64_FORMAT, cutoffTs);
}

PG_FUNCTION_INFO_V1(pg_get_oldest_tmin);
/*
 * function api to get csn for given xid
 */
Datum
pg_get_oldest_tmin(PG_FUNCTION_ARGS)
{
	CommitSeqNo ts;

	ts = GetOldestTmin();

	PG_RETURN_UINT64(ts);
}

PG_FUNCTION_INFO_V1(pg_set_global_cutoffts);
/*
 * function api to get csn for given xid
 */
Datum
pg_set_global_cutoffts(PG_FUNCTION_ARGS)
{
	CommitSeqNo ts = PG_GETARG_INT64(0);

	SetGlobalCutoffTs(ts);
	PG_RETURN_BOOL(true);
}

static bool
GlobalSnapshotIsAdmitted(Snapshot snapshot, CommitSeqNo * cutoffTs)
{
	CommitSeqNo maxCommitTs;

	if (enable_global_cutoffts)
		return true;

	if (!txnUseGlobalSnapshot)
	{
		SpinLockAcquire(&ShmemVariableCache->ts_lock);
		maxCommitTs = ShmemVariableCache->maxCommitTs;
		SpinLockRelease(&ShmemVariableCache->ts_lock);
	}
	else
	{
		maxCommitTs = RecentGlobalTs;
		if (!COMMITSEQNO_IS_NORMAL(maxCommitTs))
			elog(ERROR, "Recent global ts is invalid " UINT64_FORMAT, maxCommitTs);
	}

	/*
	 * We design a timestamp based MVCC garbage collection algorithm to make
	 * the gc not to vacuum the tuple versions that are visible to concurrent
	 * and pending transactions.
	 *
	 * The algorithm consists of two parts. The first part is the admission of
	 * transaction execution. We reject the transaction with global snapshot
	 * that may access the tuple versions garbage collected.
	 *
	 * Specifically, we reject the snapshot with start ts < maxCommitTs -
	 * gc_interval in order to resolve the conflict with vacuum and hot-chain
	 * cleanup.
	 *
	 * The parameter gc_interval represents the allowed maximum delay from the
	 * snapshot generated on the coordinator to its arrival on data node.
	 *
	 * The second part is the determining of the oldest committs before which
	 * the tuple versions can be pruned. See CommittsSatisfiesVacuum().
	 *
	 * The second part is the determining of the oldest committs before which
	 * the tuple versions can be pruned. See CommittsSatisfiesVacuum(). The
	 * oldest committs computation is implemented in GetRecentGlobalXmin() and
	 * GetOldestXmin().
	 *
	 * Written by Junbin Kang, 2020.01.18
	 */

	if (maxCommitTs < SHIFT_GC_INTERVAL(gc_interval))
		elog(ERROR, "maxCommitTs " UINT64_FORMAT " is smaller than vacuum delta " UINT64_FORMAT,
			 maxCommitTs, SHIFT_GC_INTERVAL(gc_interval));
	*cutoffTs = maxCommitTs - SHIFT_GC_INTERVAL(gc_interval);
	if (snapshot->snapshotcsn < *cutoffTs)
		return false;
	else
		return true;
}
#endif

/*

oldestActiveXid
	oldest XID that's currently in-progress

GlobalXmin
	oldest XID that's *seen* by any active snapshot as still in-progress

latestCompletedXid
	latest XID that has committed.

CSN
	current CSN



Get snapshot:

1. LWLockAcquire(ProcArrayLock, LW_SHARED)
2. Read oldestActiveXid. Store it in MyProc->xmin
3. Read CSN
4. LWLockRelease(ProcArrayLock)

End-of-xact:

1. LWLockAcquire(ProcArrayLock, LW_SHARED)
2. Reset MyProc->xmin, xid and CSN
3. Was my XID == oldestActiveXid? If so, advance oldestActiveXid.
4. Was my xmin == oldestXmin? If so, advance oldestXmin.
5. LWLockRelease(ProcArrayLock)

AdvanceGlobalXmin:

1. LWLockAcquire(ProcArrayLock, LW_SHARED)
2. Read current oldestActiveXid. That's the upper bound. If a transaction
   begins now, that's the xmin it would get.
3. Scan ProcArray, for the smallest xmin.
4. Set that as the new GlobalXmin.
5. LWLockRelease(ProcArrayLock)

AdvanceOldestActiveXid:

Two alternatives: scan the csnlog or scan the procarray. Scanning the
procarray is tricky: it's possible that a backend has just read nextXid,
but not set it in MyProc->xid yet.


*/



/*
 * GetSnapshotData -- returns an MVCC snapshot.
 *
 * The crux of the returned snapshot is the current Commit-Sequence-Number.
 * All transactions that committed before the CSN is considered
 * as visible to the snapshot, and all transactions that committed at or
 * later are considered as still-in-progress.
 *
 * The returned snapshot also includes xmin (lowest still-running xact ID),
 * and xmax (highest completed xact ID + 1). They can be used to avoid
 * the more expensive check against the CSN:
 *		All xact IDs < xmin are known to be finished.
 *		All xact IDs >= xmax are known to be still running.
 *		For an xact ID xmin <= xid < xmax, consult the CSNLOG to see
 *		whether its CSN is before or after the snapshot's CSN.
 *
 * This ensures that the set of transactions seen as "running" by the
 * current xact will not change after it takes the snapshot.
 *
 * We also update the following backend-global variables:
 *		TransactionXmin: the oldest xmin of any snapshot in use in the
 *			current transaction.
 *		RecentGlobalXmin: the global xmin (oldest TransactionXmin across all
 *			running transactions, except those running LAZY VACUUM). This
 *			can be used to opportunistically remove old dead tuples.
 *		RecentGlobalDataXmin: the global xmin for non-catalog tables
 *			>= RecentGlobalXmin
 *
 * Support CTS-based snapshots for distributed transaction isolation.
 *
 */
Snapshot
GetSnapshotDataExtend(Snapshot snapshot, bool latest)
{
	TransactionId xmin;
	TransactionId xmax;
	CommitSeqNo snapshotcsn;
	LogicalTime startTs;
	bool		takenDuringRecovery;
#ifdef ENABLE_DISTRIBUTED_TRANSACTION
	CommitSeqNo cutoffTs;
#endif

	Assert(snapshot != NULL);

	/*
	 * The ProcArrayLock is not needed here. We only set our xmin if it's not
	 * already set. There are only a few functions that check the xmin under
	 * exclusive ProcArrayLock: 1) ProcArrayInstallRestored/ImportedXmin --
	 * can only care about our xmin long after it has been first set. 2)
	 * ProcArrayEndTransaction is not called concurrently with
	 * GetSnapshotData.
	 */

	takenDuringRecovery = RecoveryInProgress();

	/* Anything older than oldestActiveXid is surely finished by now. */
	xmin = pg_atomic_read_u32(&ShmemVariableCache->oldestActiveXid);

	/* Announce my xmin, to hold back GlobalXmin. */
	if (!TransactionIdIsValid(MyPgXact->xmin))
	{
		TransactionId oldestActiveXid;

		MyPgXact->xmin = xmin;

		/*
		 * Recheck, if oldestActiveXid advanced after we read it.
		 *
		 * This protects against a race condition with AdvanceGlobalXmin(). If
		 * a transaction ends runs AdvanceGlobalXmin(), just after we fetch
		 * oldestActiveXid, but before we set MyPgXact->xmin, it's possible
		 * that AdvanceGlobalXmin() computed a new GlobalXmin that doesn't
		 * cover the xmin that we got. To fix that, check oldestActiveXid
		 * again, after setting xmin. Redoing it once is enough, we don't need
		 * to loop, because the (stale) xmin that we set prevents the same
		 * race condition from advancing oldestXid again.
		 *
		 * For a brief moment, we can have the situation that our xmin is
		 * lower than GlobalXmin, but it's OK because we don't use that xmin
		 * until we've re-checked and corrected it if necessary.
		 */

		/*
		 * memory barrier to make sure that setting the xmin in our PGPROC
		 * entry is made visible to others, before the read below.
		 */
		pg_memory_barrier();

		oldestActiveXid = pg_atomic_read_u32(&ShmemVariableCache->oldestActiveXid);
		if (oldestActiveXid != xmin)
		{
			xmin = oldestActiveXid;

			MyPgXact->xmin = xmin;
		}

		TransactionXmin = xmin;
	}


	/*
	 * Get the current snapshot CSN, and copy that to my PGPROC entry. This
	 * serializes us with any concurrent commits.
	 */
#ifdef ENABLE_DISTRIBUTED_TRANSACTION
	if (enable_global_cutoffts)
		LWLockAcquire(ProcArrayLock, LW_SHARED);

	startTs = TxnGetOrGenerateStartTs(latest);
	snapshotcsn = startTs;
	if (!COMMITSEQNO_IS_NORMAL(snapshotcsn))
		elog(ERROR, "invalid snapshot ts " UINT64_FORMAT, snapshotcsn);

	Assert(snapshotcsn >= COMMITSEQNO_FIRST_NORMAL);

	/*
	 * Be carefull of the order between setting MyPgXact->tmin and acquiring
	 * the ts_lock to fetch maxCommitTs in GlobalSnapshotIsAdmitted() which is
	 * critical to the correctness of garbage collection algorithm. Written by
	 * Junbin Kang, 2020.01.20
	 */
	if (!txnUseGlobalSnapshot && !latest)
	{
		pg_atomic_write_u64(&MyPgXact->tmin, snapshotcsn);
		pg_memory_barrier();
		if (enable_timestamp_debug_print)
			elog(LOG, "set local tmin " UINT64_FORMAT "procno %d", snapshotcsn, MyProc->pgprocno);
	}

	if (enable_global_cutoffts)
		LWLockRelease(ProcArrayLock);

#else
	snapshotcsn = pg_atomic_read_u64(&ShmemVariableCache->nextCommitSeqNo);
#endif
	if (MyPgXact->snapshotcsn == InvalidCommitSeqNo)
		MyPgXact->snapshotcsn = snapshotcsn;

	/*
	 * Also get xmax. It is always latestCompletedXid + 1. Make sure to read
	 * it after CSN (see TransactionIdAsyncCommitTree())
	 */
	pg_read_barrier();
	xmax = pg_atomic_read_u32(&ShmemVariableCache->latestCompletedXid);
	Assert(TransactionIdIsNormal(xmax));
	TransactionIdAdvance(xmax);

	snapshot->xmin = xmin;
	snapshot->xmax = xmax;
	snapshot->snapshotcsn = snapshotcsn;
	snapshot->curcid = GetCurrentCommandId(false);
	snapshot->takenDuringRecovery = takenDuringRecovery;

	/*
	 * This is a new snapshot, so set both refcounts are zero, and mark it as
	 * not copied in persistent memory.
	 */
	snapshot->active_count = 0;
	snapshot->regd_count = 0;
	snapshot->copied = false;

#ifdef ENABLE_DISTRIBUTED_TRANSACTION
	if (!latest && !GlobalSnapshotIsAdmitted(snapshot, &cutoffTs))
	{
		ereport(ERROR, (errcode(ERRCODE_SNAPSHOT_TOO_OLD),
						errmsg("stale snapshot: start_ts " LOGICALTIME_FORMAT
							   " < cutoff_ts " LOGICALTIME_FORMAT,
							   LOGICALTIME_STRING(snapshotcsn),
							   LOGICALTIME_STRING(cutoffTs))));
	}
#ifdef ENABLE_DISTR_DEBUG
	if (txnUseGlobalSnapshot && !latest)
	{
		if (snapshot_delay)
			pg_usleep(snapshot_delay * 1000);
	}
#endif
#endif
	if (old_snapshot_threshold < 0)
	{
		/*
		 * If not using "snapshot too old" feature, fill related fields with
		 * dummy values that don't require any locking.
		 */
		snapshot->lsn = InvalidXLogRecPtr;
		snapshot->whenTaken = 0;
	}
	else
	{
		/*
		 * Capture the current time and WAL stream location in case this
		 * snapshot becomes old enough to need to fall back on the special
		 * "old snapshot" logic.
		 */
		snapshot->lsn = GetXLogInsertRecPtr();
		snapshot->whenTaken = GetSnapshotCurrentTimestamp();
		MaintainOldSnapshotTimeMapping(snapshot->whenTaken, xmin);
	}

	return snapshot;
}

/*
 * ProcArrayInstallImportedXmin -- install imported xmin into MyPgXact->xmin
 *
 * This is called when installing a snapshot imported from another
 * transaction.  To ensure that OldestXmin doesn't go backwards, we must
 * check that the source transaction is still running, and we'd better do
 * that atomically with installing the new xmin.
 *
 * Returns true if successful, false if source xact is no longer running.
 */
bool
ProcArrayInstallImportedXmin(TransactionId xmin,
							 VirtualTransactionId *sourcevxid)
{
	bool		result = false;
	ProcArrayStruct *arrayP = procArray;
	int			index;

	Assert(TransactionIdIsNormal(xmin));
	if (!sourcevxid)
		return false;

	/*
	 * Get exclusive lock so source xact can't end while we're doing this.
	 */
	LWLockAcquire(ProcArrayLock, LW_EXCLUSIVE);

	for (index = 0; index < arrayP->numProcs; index++)
	{
		int			pgprocno = arrayP->pgprocnos[index];
		volatile PGPROC *proc = &allProcs[pgprocno];
		volatile	PGXACT *pgxact = &allPgXact[pgprocno];
		TransactionId xid;

		/* Ignore procs running LAZY VACUUM */
		if (pgxact->vacuumFlags & PROC_IN_VACUUM)
			continue;

		/* We are only interested in the specific virtual transaction. */
		if (proc->backendId != sourcevxid->backendId)
			continue;
		if (proc->lxid != sourcevxid->localTransactionId)
			continue;

		/*
		 * We check the transaction's database ID for paranoia's sake: if it's
		 * in another DB then its xmin does not cover us.  Caller should have
		 * detected this already, so we just treat any funny cases as
		 * "transaction not found".
		 */
		if (proc->databaseId != MyDatabaseId)
			continue;

		/*
		 * Likewise, let's just make real sure its xmin does cover us.
		 */
		xid = pgxact->xmin;		/* fetch just once */
		if (!TransactionIdIsNormal(xid) ||
			!TransactionIdPrecedesOrEquals(xid, xmin))
			continue;

		/*
		 * We're good.  Install the new xmin.  As in GetSnapshotData, set
		 * TransactionXmin too.  (Note that because snapmgr.c called
		 * GetSnapshotData first, we'll be overwriting a valid xmin here, so
		 * we don't check that.)
		 */
		MyPgXact->xmin = TransactionXmin = xmin;

		result = true;
		break;
	}

	LWLockRelease(ProcArrayLock);

	return result;
}

/*
 * ProcArrayInstallRestoredXmin -- install restored xmin into MyPgXact->xmin
 *
 * This is like ProcArrayInstallImportedXmin, but we have a pointer to the
 * PGPROC of the transaction from which we imported the snapshot, rather than
 * an XID.
 *
 * Returns true if successful, false if source xact is no longer running.
 */
bool
ProcArrayInstallRestoredXmin(TransactionId xmin, PGPROC *proc)
{
	bool		result = false;
	TransactionId xid;
	volatile	PGXACT *pgxact;

	Assert(TransactionIdIsNormal(xmin));
	Assert(proc != NULL);

	/*
	 * Get exclusive lock so source xact can't end while we're doing this.
	 */
	LWLockAcquire(ProcArrayLock, LW_EXCLUSIVE);

	pgxact = &allPgXact[proc->pgprocno];

	/*
	 * Be certain that the referenced PGPROC has an advertised xmin which is
	 * no later than the one we're installing, so that the system-wide xmin
	 * can't go backwards.  Also, make sure it's running in the same database,
	 * so that the per-database xmin cannot go backwards.
	 */
	xid = pgxact->xmin;			/* fetch just once */
	if (proc->databaseId == MyDatabaseId &&
		TransactionIdIsNormal(xid) &&
		TransactionIdPrecedesOrEquals(xid, xmin))
	{
		MyPgXact->xmin = TransactionXmin = xmin;
		result = true;
	}

	LWLockRelease(ProcArrayLock);

	return result;
}

/*
 * GetRunningTransactionData -- returns information about running transactions.
 *
 * Returns the oldest running TransactionId among all backends, even VACUUM
 * processes.
 *
 * We acquire XidGenlock, but the caller is responsible for releasing it.
 * Acquiring XidGenLock ensures that no new XID can be assigned until
 * the caller has WAL-logged this snapshot, and releases the lock.
 * FIXME: this also used to hold ProcArrayLock, to prevent any transactions
 * from committing until the caller has WAL-logged. I don't think we need
 * that anymore, but verify.
 *
 * Returns the current xmin and xmax, like GetSnapshotData does.
 *
 * The returned data structure is statically allocated; caller should not
 * modify it, and must not assume it is valid past the next call.
 *
 * We don't worry about updating other counters, we want to keep this as
 * simple as possible and leave GetSnapshotData() as the primary code for
 * that bookkeeping.
 */
RunningTransactions
GetRunningTransactionData(void)
{
	/* result workspace */
	static RunningTransactionsData CurrentRunningXactsData;

	ProcArrayStruct *arrayP = procArray;
	RunningTransactions CurrentRunningXacts = &CurrentRunningXactsData;
	TransactionId oldestRunningXid;
	int			index;

	Assert(!RecoveryInProgress());

	/*
	 * Ensure that no xids enter or leave the procarray while we obtain
	 * snapshot.
	 */
	LWLockAcquire(ProcArrayLock, LW_EXCLUSIVE);
	LWLockAcquire(XidGenLock, LW_SHARED);

	oldestRunningXid = ShmemVariableCache->nextXid;

	/*
	 * Spin over procArray collecting all xids
	 */
	for (index = 0; index < arrayP->numProcs; index++)
	{
		int			pgprocno = arrayP->pgprocnos[index];
		volatile	PGXACT *pgxact = &allPgXact[pgprocno];
		TransactionId xid;

		/* Fetch xid just once - see GetNewTransactionId */
		xid = pgxact->xid;

		/*
		 * We don't need to store transactions that don't have a TransactionId
		 * yet because they will not show as running on a standby server.
		 */
		if (!TransactionIdIsValid(xid))
			continue;

		if (TransactionIdPrecedes(xid, oldestRunningXid))
			oldestRunningXid = xid;
	}

	/*
	 * It's important *not* to include the limits set by slots here because
	 * snapbuild.c uses oldestRunningXid to manage its xmin horizon. If those
	 * were to be included here the initial value could never increase because
	 * of a circular dependency where slots only increase their limits when
	 * running xacts increases oldestRunningXid and running xacts only
	 * increases if slots do.
	 */

	CurrentRunningXacts->nextXid = ShmemVariableCache->nextXid;
	CurrentRunningXacts->oldestRunningXid = oldestRunningXid;

	Assert(TransactionIdIsValid(CurrentRunningXacts->nextXid));
	Assert(TransactionIdIsValid(CurrentRunningXacts->oldestRunningXid));

	/* We don't release XidGenLock here, the caller is responsible for that */

	return CurrentRunningXacts;
}

/*
 * GetOldestActiveTransactionId()
 *
 * Returns the oldest XID that's still running. We include
 * all PGXACTs with an assigned TransactionId, even VACUUM processes.
 * We look at all databases, though there is no need to include WALSender
 * since this has no effect on hot standby conflicts.
 *
 * We don't worry about updating other counters, we want to keep this as
 * simple as possible and leave GetSnapshotData() as the primary code for
 * that bookkeeping.
 *
 * XXX: We could just use return ShmemVariableCache->oldestActiveXid. this
 * uses a different method of computing the value though, so maybe this is
 * useful as a cross-check?
 */
TransactionId
GetOldestActiveTransactionId(void)
{
	ProcArrayStruct *arrayP = procArray;
	TransactionId oldestRunningXid;
	int			index;

	Assert(!RecoveryInProgress());

	/*
	 * Read nextXid, as the upper bound of what's still active.
	 *
	 * Reading a TransactionId is atomic, but we must grab the lock to make
	 * sure that all XIDs < nextXid are already present in the proc array (or
	 * have already completed), when we spin over it.
	 */
	LWLockAcquire(XidGenLock, LW_SHARED);
	oldestRunningXid = ShmemVariableCache->nextXid;
	LWLockRelease(XidGenLock);

	/*
	 * Spin over procArray collecting all xids and subxids.
	 */
	LWLockAcquire(ProcArrayLock, LW_SHARED);
	for (index = 0; index < arrayP->numProcs; index++)
	{
		int			pgprocno = arrayP->pgprocnos[index];
		volatile	PGXACT *pgxact = &allPgXact[pgprocno];
		TransactionId xid;

		/* Fetch xid just once - see GetNewTransactionId */
		xid = pgxact->xid;

		if (!TransactionIdIsNormal(xid))
			continue;

		if (TransactionIdPrecedes(xid, oldestRunningXid))
			oldestRunningXid = xid;

		/*
		 * Top-level XID of a transaction is always less than any of its
		 * subxids, so we don't need to check if any of the subxids are
		 * smaller than oldestRunningXid
		 */
	}
	LWLockRelease(ProcArrayLock);

	return oldestRunningXid;
}

/*
 * GetOldestSafeDecodingTransactionId -- lowest xid not affected by vacuum
 *
 * Returns the oldest xid that we can guarantee not to have been affected by
 * vacuum, i.e. no rows >= that xid have been vacuumed away unless the
 * transaction aborted. Note that the value can (and most of the time will) be
 * much more conservative than what really has been affected by vacuum, but we
 * currently don't have better data available.
 *
 * This is useful to initialize the cutoff xid after which a new changeset
 * extraction replication slot can start decoding changes.
 *
 * Must be called with ProcArrayLock held either shared or exclusively,
 * although most callers will want to use exclusive mode since it is expected
 * that the caller will immediately use the xid to peg the xmin horizon.
 */
TransactionId
GetOldestSafeDecodingTransactionId(bool catalogOnly)
{
	ProcArrayStruct *arrayP = procArray;
	TransactionId oldestSafeXid;
	int			index;
	bool		recovery_in_progress = RecoveryInProgress();

	Assert(LWLockHeldByMe(ProcArrayLock));

	/*
	 * Acquire XidGenLock, so no transactions can acquire an xid while we're
	 * running. If no transaction with xid were running concurrently a new xid
	 * could influence the RecentXmin et al.
	 *
	 * We initialize the computation to nextXid since that's guaranteed to be
	 * a safe, albeit pessimal, value.
	 */
	LWLockAcquire(XidGenLock, LW_SHARED);
	oldestSafeXid = ShmemVariableCache->nextXid;

	/*
	 * If there's already a slot pegging the xmin horizon, we can start with
	 * that value, it's guaranteed to be safe since it's computed by this
	 * routine initially and has been enforced since.  We can always use the
	 * slot's general xmin horizon, but the catalog horizon is only usable
	 * when only catalog data is going to be looked at.
	 */
	if (TransactionIdIsValid(procArray->replication_slot_xmin) &&
		TransactionIdPrecedes(procArray->replication_slot_xmin,
							  oldestSafeXid))
		oldestSafeXid = procArray->replication_slot_xmin;

	if (catalogOnly &&
		TransactionIdIsValid(procArray->replication_slot_catalog_xmin) &&
		TransactionIdPrecedes(procArray->replication_slot_catalog_xmin,
							  oldestSafeXid))
		oldestSafeXid = procArray->replication_slot_catalog_xmin;

	/*
	 * If we're not in recovery, we walk over the procarray and collect the
	 * lowest xid. Since we're called with ProcArrayLock held and have
	 * acquired XidGenLock, no entries can vanish concurrently, since
	 * PGXACT->xid is only set with XidGenLock held and only cleared with
	 * ProcArrayLock held.
	 *
	 * In recovery we can't lower the safe value besides what we've computed
	 * above, so we'll have to wait a bit longer there. We unfortunately can
	 * *not* use KnownAssignedXidsGetOldestXmin() since the KnownAssignedXids
	 * machinery can miss values and return an older value than is safe.
	 */
	if (!recovery_in_progress)
	{
		/*
		 * Spin over procArray collecting all min(PGXACT->xid)
		 */
		for (index = 0; index < arrayP->numProcs; index++)
		{
			int			pgprocno = arrayP->pgprocnos[index];
			volatile	PGXACT *pgxact = &allPgXact[pgprocno];
			TransactionId xid;

			/* Fetch xid just once - see GetNewTransactionId */
			xid = pgxact->xid;

			if (!TransactionIdIsNormal(xid))
				continue;

			if (TransactionIdPrecedes(xid, oldestSafeXid))
				oldestSafeXid = xid;
		}
	}

	LWLockRelease(XidGenLock);

	return oldestSafeXid;
}

/*
 * GetVirtualXIDsDelayingChkpt -- Get the VXIDs of transactions that are
 * delaying checkpoint because they have critical actions in progress.
 *
 * Constructs an array of VXIDs of transactions that are currently in commit
 * critical sections, as shown by having delayChkpt set in their PGXACT.
 *
 * Returns a palloc'd array that should be freed by the caller.
 * *nvxids is the number of valid entries.
 *
 * Note that because backends set or clear delayChkpt without holding any lock,
 * the result is somewhat indeterminate, but we don't really care.  Even in
 * a multiprocessor with delayed writes to shared memory, it should be certain
 * that setting of delayChkpt will propagate to shared memory when the backend
 * takes a lock, so we cannot fail to see a virtual xact as delayChkpt if
 * it's already inserted its commit record.  Whether it takes a little while
 * for clearing of delayChkpt to propagate is unimportant for correctness.
 */
VirtualTransactionId *
GetVirtualXIDsDelayingChkpt(int *nvxids)
{
	VirtualTransactionId *vxids;
	ProcArrayStruct *arrayP = procArray;
	int			count = 0;
	int			index;

	/* allocate what's certainly enough result space */
	vxids = (VirtualTransactionId *)
		palloc(sizeof(VirtualTransactionId) * arrayP->maxProcs);

	LWLockAcquire(ProcArrayLock, LW_SHARED);

	for (index = 0; index < arrayP->numProcs; index++)
	{
		int			pgprocno = arrayP->pgprocnos[index];
		volatile PGPROC *proc = &allProcs[pgprocno];
		volatile	PGXACT *pgxact = &allPgXact[pgprocno];

		if (pgxact->delayChkpt)
		{
			VirtualTransactionId vxid;

			GET_VXID_FROM_PGPROC(vxid, *proc);
			if (VirtualTransactionIdIsValid(vxid))
				vxids[count++] = vxid;
		}
	}

	LWLockRelease(ProcArrayLock);

	*nvxids = count;
	return vxids;
}

/*
 * HaveVirtualXIDsDelayingChkpt -- Are any of the specified VXIDs delaying?
 *
 * This is used with the results of GetVirtualXIDsDelayingChkpt to see if any
 * of the specified VXIDs are still in critical sections of code.
 *
 * Note: this is O(N^2) in the number of vxacts that are/were delaying, but
 * those numbers should be small enough for it not to be a problem.
 */
bool
HaveVirtualXIDsDelayingChkpt(VirtualTransactionId *vxids, int nvxids)
{
	bool		result = false;
	ProcArrayStruct *arrayP = procArray;
	int			index;

	LWLockAcquire(ProcArrayLock, LW_SHARED);

	for (index = 0; index < arrayP->numProcs; index++)
	{
		int			pgprocno = arrayP->pgprocnos[index];
		volatile PGPROC *proc = &allProcs[pgprocno];
		volatile	PGXACT *pgxact = &allPgXact[pgprocno];
		VirtualTransactionId vxid;

		GET_VXID_FROM_PGPROC(vxid, *proc);

		if (pgxact->delayChkpt && VirtualTransactionIdIsValid(vxid))
		{
			int			i;

			for (i = 0; i < nvxids; i++)
			{
				if (VirtualTransactionIdEquals(vxid, vxids[i]))
				{
					result = true;
					break;
				}
			}
			if (result)
				break;
		}
	}

	LWLockRelease(ProcArrayLock);

	return result;
}

/*
 * BackendPidGetProc -- get a backend's PGPROC given its PID
 *
 * Returns NULL if not found.  Note that it is up to the caller to be
 * sure that the question remains meaningful for long enough for the
 * answer to be used ...
 */
PGPROC *
BackendPidGetProc(int pid)
{
	PGPROC	   *result;

	if (pid == 0)				/* never match dummy PGPROCs */
		return NULL;

	LWLockAcquire(ProcArrayLock, LW_SHARED);

	result = BackendPidGetProcWithLock(pid);

	LWLockRelease(ProcArrayLock);

	return result;
}

/*
 * BackendPidGetProcWithLock -- get a backend's PGPROC given its PID
 *
 * Same as above, except caller must be holding ProcArrayLock.  The found
 * entry, if any, can be assumed to be valid as long as the lock remains held.
 */
PGPROC *
BackendPidGetProcWithLock(int pid)
{
	PGPROC	   *result = NULL;
	ProcArrayStruct *arrayP = procArray;
	int			index;

	if (pid == 0)				/* never match dummy PGPROCs */
		return NULL;

	for (index = 0; index < arrayP->numProcs; index++)
	{
		PGPROC	   *proc = &allProcs[arrayP->pgprocnos[index]];

		if (proc->pid == pid)
		{
			result = proc;
			break;
		}
	}

	return result;
}

/*
 * BackendXidGetPid -- get a backend's pid given its XID
 *
 * Returns 0 if not found or it's a prepared transaction.  Note that
 * it is up to the caller to be sure that the question remains
 * meaningful for long enough for the answer to be used ...
 *
 * Only main transaction Ids are considered.  This function is mainly
 * useful for determining what backend owns a lock.
 *
 * Beware that not every xact has an XID assigned.  However, as long as you
 * only call this using an XID found on disk, you're safe.
 */
int
BackendXidGetPid(TransactionId xid)
{
	int			result = 0;
	ProcArrayStruct *arrayP = procArray;
	int			index;

	if (xid == InvalidTransactionId)	/* never match invalid xid */
		return 0;

	LWLockAcquire(ProcArrayLock, LW_SHARED);

	for (index = 0; index < arrayP->numProcs; index++)
	{
		int			pgprocno = arrayP->pgprocnos[index];
		volatile PGPROC *proc = &allProcs[pgprocno];
		volatile	PGXACT *pgxact = &allPgXact[pgprocno];

		if (pgxact->xid == xid)
		{
			result = proc->pid;
			break;
		}
	}

	LWLockRelease(ProcArrayLock);

	return result;
}

/*
 * IsBackendPid -- is a given pid a running backend
 *
 * This is not called by the backend, but is called by external modules.
 */
bool
IsBackendPid(int pid)
{
	return (BackendPidGetProc(pid) != NULL);
}


/*
 * GetCurrentVirtualXIDs -- returns an array of currently active VXIDs.
 *
 * The array is palloc'd. The number of valid entries is returned into *nvxids.
 *
 * The arguments allow filtering the set of VXIDs returned.  Our own process
 * is always skipped.  In addition:
 *	If limitXmin is not InvalidTransactionId, skip processes with
 *		xmin > limitXmin.
 *	If excludeXmin0 is true, skip processes with xmin = 0.
 *	If allDbs is false, skip processes attached to other databases.
 *	If excludeVacuum isn't zero, skip processes for which
 *		(vacuumFlags & excludeVacuum) is not zero.
 *
 * Note: the purpose of the limitXmin and excludeXmin0 parameters is to
 * allow skipping backends whose oldest live snapshot is no older than
 * some snapshot we have.  Since we examine the procarray with only shared
 * lock, there are race conditions: a backend could set its xmin just after
 * we look.  Indeed, on multiprocessors with weak memory ordering, the
 * other backend could have set its xmin *before* we look.  We know however
 * that such a backend must have held shared ProcArrayLock overlapping our
 * own hold of ProcArrayLock, else we would see its xmin update.  Therefore,
 * any snapshot the other backend is taking concurrently with our scan cannot
 * consider any transactions as still running that we think are committed
 * (since backends must hold ProcArrayLock exclusive to commit).
 */
VirtualTransactionId *
GetCurrentVirtualXIDs(TransactionId limitXmin, bool excludeXmin0,
					  bool allDbs, int excludeVacuum,
					  int *nvxids)
{
	VirtualTransactionId *vxids;
	ProcArrayStruct *arrayP = procArray;
	int			count = 0;
	int			index;

	/* allocate what's certainly enough result space */
	vxids = (VirtualTransactionId *)
		palloc(sizeof(VirtualTransactionId) * arrayP->maxProcs);

	LWLockAcquire(ProcArrayLock, LW_SHARED);

	for (index = 0; index < arrayP->numProcs; index++)
	{
		int			pgprocno = arrayP->pgprocnos[index];
		volatile PGPROC *proc = &allProcs[pgprocno];
		volatile	PGXACT *pgxact = &allPgXact[pgprocno];

		if (proc == MyProc)
			continue;

		if (excludeVacuum & pgxact->vacuumFlags)
			continue;

		if (allDbs || proc->databaseId == MyDatabaseId)
		{
			/* Fetch xmin just once - might change on us */
			TransactionId pxmin = pgxact->xmin;

			if (excludeXmin0 && !TransactionIdIsValid(pxmin))
				continue;

			/*
			 * InvalidTransactionId precedes all other XIDs, so a proc that
			 * hasn't set xmin yet will not be rejected by this test.
			 */
			if (!TransactionIdIsValid(limitXmin) ||
				TransactionIdPrecedesOrEquals(pxmin, limitXmin))
			{
				VirtualTransactionId vxid;

				GET_VXID_FROM_PGPROC(vxid, *proc);
				if (VirtualTransactionIdIsValid(vxid))
					vxids[count++] = vxid;
			}
		}
	}

	LWLockRelease(ProcArrayLock);

	*nvxids = count;
	return vxids;
}

/*
 * GetConflictingVirtualXIDs -- returns an array of currently active VXIDs.
 *
 * Usage is limited to conflict resolution during recovery on standby servers.
 * limitXmin is supplied as either latestRemovedXid, or InvalidTransactionId
 * in cases where we cannot accurately determine a value for latestRemovedXid.
 *
 * If limitXmin is InvalidTransactionId then we want to kill everybody,
 * so we're not worried if they have a snapshot or not, nor does it really
 * matter what type of lock we hold.
 *
 * All callers that are checking xmins always now supply a valid and useful
 * value for limitXmin. The limitXmin is always lower than the lowest
 * numbered KnownAssignedXid (XXX) that is not already a FATAL error. This is
 * because we only care about cleanup records that are cleaning up tuple
 * versions from committed transactions. In that case they will only occur
 * at the point where the record is less than the lowest running xid. That
 * allows us to say that if any backend takes a snapshot concurrently with
 * us then the conflict assessment made here would never include the snapshot
 * that is being derived. So we take LW_SHARED on the ProcArray and allow
 * concurrent snapshots when limitXmin is valid. We might think about adding
 *	 Assert(limitXmin < lowest(KnownAssignedXids))
 * but that would not be true in the case of FATAL errors lagging in array,
 * but we already know those are bogus anyway, so we skip that test.
 *
 * If dbOid is valid we skip backends attached to other databases.
 *
 * Be careful to *not* pfree the result from this function. We reuse
 * this array sufficiently often that we use malloc for the result.
 */
VirtualTransactionId *
GetConflictingVirtualXIDs(TransactionId limitXmin, Oid dbOid)
{
	static VirtualTransactionId *vxids;
	ProcArrayStruct *arrayP = procArray;
	int			count = 0;
	int			index;

	/*
	 * If first time through, get workspace to remember main XIDs in. We
	 * malloc it permanently to avoid repeated palloc/pfree overhead. Allow
	 * result space, remembering room for a terminator.
	 */
	if (vxids == NULL)
	{
		vxids = (VirtualTransactionId *)
			malloc(sizeof(VirtualTransactionId) * (arrayP->maxProcs + 1));
		if (vxids == NULL)
			ereport(ERROR,
					(errcode(ERRCODE_OUT_OF_MEMORY),
					 errmsg("out of memory")));
	}

	LWLockAcquire(ProcArrayLock, LW_SHARED);

	for (index = 0; index < arrayP->numProcs; index++)
	{
		int			pgprocno = arrayP->pgprocnos[index];
		volatile PGPROC *proc = &allProcs[pgprocno];
		volatile	PGXACT *pgxact = &allPgXact[pgprocno];

		/* Exclude prepared transactions */
		if (proc->pid == 0)
			continue;

		if (!OidIsValid(dbOid) ||
			proc->databaseId == dbOid)
		{
			/* Fetch xmin just once - can't change on us, but good coding */
			TransactionId pxmin = pgxact->xmin;

			/*
			 * We ignore an invalid pxmin because this means that backend has
			 * no snapshot currently. We hold a Share lock to avoid contention
			 * with users taking snapshots.  That is not a problem because the
			 * current xmin is always at least one higher than the latest
			 * removed xid, so any new snapshot would never conflict with the
			 * test here.
			 */
			if (!TransactionIdIsValid(limitXmin) ||
				(TransactionIdIsValid(pxmin) && !TransactionIdFollows(pxmin, limitXmin)))
			{
				VirtualTransactionId vxid;

				GET_VXID_FROM_PGPROC(vxid, *proc);
				if (VirtualTransactionIdIsValid(vxid))
					vxids[count++] = vxid;
			}
		}
	}

	LWLockRelease(ProcArrayLock);

	/* add the terminator */
	vxids[count].backendId = InvalidBackendId;
	vxids[count].localTransactionId = InvalidLocalTransactionId;

	return vxids;
}

/*
 * CancelVirtualTransaction - used in recovery conflict processing
 *
 * Returns pid of the process signaled, or 0 if not found.
 */
pid_t
CancelVirtualTransaction(VirtualTransactionId vxid, ProcSignalReason sigmode)
{
	ProcArrayStruct *arrayP = procArray;
	int			index;
	pid_t		pid = 0;

	LWLockAcquire(ProcArrayLock, LW_SHARED);

	for (index = 0; index < arrayP->numProcs; index++)
	{
		int			pgprocno = arrayP->pgprocnos[index];
		volatile PGPROC *proc = &allProcs[pgprocno];
		VirtualTransactionId procvxid;

		GET_VXID_FROM_PGPROC(procvxid, *proc);

		if (procvxid.backendId == vxid.backendId &&
			procvxid.localTransactionId == vxid.localTransactionId)
		{
			proc->recoveryConflictPending = true;
			pid = proc->pid;
			if (pid != 0)
			{
				/*
				 * Kill the pid if it's still here. If not, that's what we
				 * wanted so ignore any errors.
				 */
				(void) SendProcSignal(pid, sigmode, vxid.backendId);
			}
			break;
		}
	}

	LWLockRelease(ProcArrayLock);

	return pid;
}

/*
 * MinimumActiveBackends --- count backends (other than myself) that are
 *		in active transactions.  Return true if the count exceeds the
 *		minimum threshold passed.  This is used as a heuristic to decide if
 *		a pre-XLOG-flush delay is worthwhile during commit.
 *
 * Do not count backends that are blocked waiting for locks, since they are
 * not going to get to run until someone else commits.
 */
bool
MinimumActiveBackends(int min)
{
	ProcArrayStruct *arrayP = procArray;
	int			count = 0;
	int			index;

	/* Quick short-circuit if no minimum is specified */
	if (min == 0)
		return true;

	/*
	 * Note: for speed, we don't acquire ProcArrayLock.  This is a little bit
	 * bogus, but since we are only testing fields for zero or nonzero, it
	 * should be OK.  The result is only used for heuristic purposes anyway...
	 */
	for (index = 0; index < arrayP->numProcs; index++)
	{
		int			pgprocno = arrayP->pgprocnos[index];
		volatile PGPROC *proc = &allProcs[pgprocno];
		volatile	PGXACT *pgxact = &allPgXact[pgprocno];

		/*
		 * Since we're not holding a lock, need to be prepared to deal with
		 * garbage, as someone could have incremented numProcs but not yet
		 * filled the structure.
		 *
		 * If someone just decremented numProcs, 'proc' could also point to a
		 * PGPROC entry that's no longer in the array. It still points to a
		 * PGPROC struct, though, because freed PGPROC entries just go to the
		 * free list and are recycled. Its contents are nonsense in that case,
		 * but that's acceptable for this function.
		 */
		if (pgprocno == -1)
			continue;			/* do not count deleted entries */
		if (proc == MyProc)
			continue;			/* do not count myself */
		if (pgxact->xid == InvalidTransactionId)
			continue;			/* do not count if no XID assigned */
		if (proc->pid == 0)
			continue;			/* do not count prepared xacts */
		if (proc->waitLock != NULL)
			continue;			/* do not count if blocked on a lock */
		count++;
		if (count >= min)
			break;
	}

	return count >= min;
}

/*
 * CountDBBackends --- count backends that are using specified database
 */
int
CountDBBackends(Oid databaseid)
{
	ProcArrayStruct *arrayP = procArray;
	int			count = 0;
	int			index;

	LWLockAcquire(ProcArrayLock, LW_SHARED);

	for (index = 0; index < arrayP->numProcs; index++)
	{
		int			pgprocno = arrayP->pgprocnos[index];
		volatile PGPROC *proc = &allProcs[pgprocno];

		if (proc->pid == 0)
			continue;			/* do not count prepared xacts */
		if (!OidIsValid(databaseid) ||
			proc->databaseId == databaseid)
			count++;
	}

	LWLockRelease(ProcArrayLock);

	return count;
}

/*
 * CountDBConnections --- counts database backends ignoring any background
 *		worker processes
 */
int
CountDBConnections(Oid databaseid)
{
	ProcArrayStruct *arrayP = procArray;
	int			count = 0;
	int			index;

	LWLockAcquire(ProcArrayLock, LW_SHARED);

	for (index = 0; index < arrayP->numProcs; index++)
	{
		int			pgprocno = arrayP->pgprocnos[index];
		volatile PGPROC *proc = &allProcs[pgprocno];

		if (proc->pid == 0)
			continue;			/* do not count prepared xacts */
		if (proc->isBackgroundWorker)
			continue;			/* do not count background workers */
		if (!OidIsValid(databaseid) ||
			proc->databaseId == databaseid)
			count++;
	}

	LWLockRelease(ProcArrayLock);

	return count;
}

/*
 * CancelDBBackends --- cancel backends that are using specified database
 */
void
CancelDBBackends(Oid databaseid, ProcSignalReason sigmode, bool conflictPending)
{
	ProcArrayStruct *arrayP = procArray;
	int			index;
	pid_t		pid = 0;

	/* tell all backends to die */
	LWLockAcquire(ProcArrayLock, LW_EXCLUSIVE);

	for (index = 0; index < arrayP->numProcs; index++)
	{
		int			pgprocno = arrayP->pgprocnos[index];
		volatile PGPROC *proc = &allProcs[pgprocno];

		if (databaseid == InvalidOid || proc->databaseId == databaseid)
		{
			VirtualTransactionId procvxid;

			GET_VXID_FROM_PGPROC(procvxid, *proc);

			proc->recoveryConflictPending = conflictPending;
			pid = proc->pid;
			if (pid != 0)
			{
				/*
				 * Kill the pid if it's still here. If not, that's what we
				 * wanted so ignore any errors.
				 */
				(void) SendProcSignal(pid, sigmode, procvxid.backendId);
			}
		}
	}

	LWLockRelease(ProcArrayLock);
}

/*
 * CountUserBackends --- count backends that are used by specified user
 */
int
CountUserBackends(Oid roleid)
{
	ProcArrayStruct *arrayP = procArray;
	int			count = 0;
	int			index;

	LWLockAcquire(ProcArrayLock, LW_SHARED);

	for (index = 0; index < arrayP->numProcs; index++)
	{
		int			pgprocno = arrayP->pgprocnos[index];
		volatile PGPROC *proc = &allProcs[pgprocno];

		if (proc->pid == 0)
			continue;			/* do not count prepared xacts */
		if (proc->isBackgroundWorker)
			continue;			/* do not count background workers */
		if (proc->roleId == roleid)
			count++;
	}

	LWLockRelease(ProcArrayLock);

	return count;
}

/*
 * CountOtherDBBackends -- check for other backends running in the given DB
 *
 * If there are other backends in the DB, we will wait a maximum of 5 seconds
 * for them to exit.  Autovacuum backends are encouraged to exit early by
 * sending them SIGTERM, but normal user backends are just waited for.
 *
 * The current backend is always ignored; it is caller's responsibility to
 * check whether the current backend uses the given DB, if it's important.
 *
 * Returns true if there are (still) other backends in the DB, false if not.
 * Also, *nbackends and *nprepared are set to the number of other backends
 * and prepared transactions in the DB, respectively.
 *
 * This function is used to interlock DROP DATABASE and related commands
 * against there being any active backends in the target DB --- dropping the
 * DB while active backends remain would be a Bad Thing.  Note that we cannot
 * detect here the possibility of a newly-started backend that is trying to
 * connect to the doomed database, so additional interlocking is needed during
 * backend startup.  The caller should normally hold an exclusive lock on the
 * target DB before calling this, which is one reason we mustn't wait
 * indefinitely.
 */
bool
CountOtherDBBackends(Oid databaseId, int *nbackends, int *nprepared)
{
	ProcArrayStruct *arrayP = procArray;

#define MAXAUTOVACPIDS	10		/* max autovacs to SIGTERM per iteration */
	int			autovac_pids[MAXAUTOVACPIDS];
	int			tries;

	/* 50 tries with 100ms sleep between tries makes 5 sec total wait */
	for (tries = 0; tries < 50; tries++)
	{
		int			nautovacs = 0;
		bool		found = false;
		int			index;

		CHECK_FOR_INTERRUPTS();

		*nbackends = *nprepared = 0;

		LWLockAcquire(ProcArrayLock, LW_SHARED);

		for (index = 0; index < arrayP->numProcs; index++)
		{
			int			pgprocno = arrayP->pgprocnos[index];
			volatile PGPROC *proc = &allProcs[pgprocno];
			volatile	PGXACT *pgxact = &allPgXact[pgprocno];

			if (proc->databaseId != databaseId)
				continue;
			if (proc == MyProc)
				continue;

			found = true;

			if (proc->pid == 0)
				(*nprepared)++;
			else
			{
				(*nbackends)++;
				if ((pgxact->vacuumFlags & PROC_IS_AUTOVACUUM) &&
					nautovacs < MAXAUTOVACPIDS)
					autovac_pids[nautovacs++] = proc->pid;
			}
		}

		LWLockRelease(ProcArrayLock);

		if (!found)
			return false;		/* no conflicting backends, so done */

		/*
		 * Send SIGTERM to any conflicting autovacuums before sleeping. We
		 * postpone this step until after the loop because we don't want to
		 * hold ProcArrayLock while issuing kill(). We have no idea what might
		 * block kill() inside the kernel...
		 */
		for (index = 0; index < nautovacs; index++)
			(void) kill(autovac_pids[index], SIGTERM);	/* ignore any error */

		/* sleep, then try again */
		pg_usleep(100 * 1000L); /* 100ms */
	}

	return true;				/* timed out, still conflicts */
}

#ifdef POLARDB_X
/*
 * ReloadConnInfoOnBackends -- reload/refresh connection information
 * for all the backends
 *
 * "refresh" is less destructive than "reload"
 */
void
ReloadConnInfoOnBackends(bool refresh_only)
{
    ProcArrayStruct *arrayP = procArray;
    int            index;
    pid_t        pid = 0;

    /* tell all backends to reload except this one who already reloaded */
    LWLockAcquire(ProcArrayLock, LW_EXCLUSIVE);

    for (index = 0; index < arrayP->numProcs; index++)
    {
        int            pgprocno = arrayP->pgprocnos[index];
        volatile PGPROC *proc = &allProcs[pgprocno];
        volatile PGXACT *pgxact = &allPgXact[pgprocno];
        VirtualTransactionId vxid;
        GET_VXID_FROM_PGPROC(vxid, *proc);

        if (proc == MyProc)
            continue;            /* do not do that on myself */
        if (proc->pid == 0)
            continue;            /* useless on prepared xacts */
        if (!OidIsValid(proc->databaseId))
            continue;            /* ignore backends not connected to a database */
        if (pgxact->vacuumFlags & PROC_IN_VACUUM)
            continue;            /* ignore vacuum processes */

        pid = proc->pid;
        /*
         * Send the reload signal if backend still exists
         */
        (void) SendProcSignal(pid, refresh_only?
                      PROCSIG_PGXCPOOL_REFRESH:PROCSIG_PGXCPOOL_RELOAD,
                      vxid.backendId);
    }

    LWLockRelease(ProcArrayLock);
}
#endif
/*
 * ProcArraySetReplicationSlotXmin
 *
 * Install limits to future computations of the xmin horizon to prevent vacuum
 * and HOT pruning from removing affected rows still needed by clients with
 * replication slots.
 */
void
ProcArraySetReplicationSlotXmin(TransactionId xmin, TransactionId catalog_xmin,
								bool already_locked)
{
	Assert(!already_locked || LWLockHeldByMe(ProcArrayLock));

	if (!already_locked)
		LWLockAcquire(ProcArrayLock, LW_EXCLUSIVE);

	procArray->replication_slot_xmin = xmin;
	procArray->replication_slot_catalog_xmin = catalog_xmin;

	if (!already_locked)
		LWLockRelease(ProcArrayLock);
}

/*
 * ProcArrayGetReplicationSlotXmin
 *
 * Return the current slot xmin limits. That's useful to be able to remove
 * data that's older than those limits.
 */
void
ProcArrayGetReplicationSlotXmin(TransactionId *xmin,
								TransactionId *catalog_xmin)
{
	LWLockAcquire(ProcArrayLock, LW_SHARED);

	if (xmin != NULL)
		*xmin = procArray->replication_slot_xmin;

	if (catalog_xmin != NULL)
		*catalog_xmin = procArray->replication_slot_catalog_xmin;

	LWLockRelease(ProcArrayLock);
}

/*
 * RecordKnownAssignedTransactionIds
 *		Record the given XID in KnownAssignedXids (FIXME: update comment, KnownAssignedXid is no more), as well as any preceding
 *		unobserved XIDs.
 *
 * RecordKnownAssignedTransactionIds() should be run for *every* WAL record
 * associated with a transaction. Must be called for each record after we
 * have executed StartupCLOG() et al, since we must ExtendCLOG() etc..
 *
 * Called during recovery in analogy with and in place of GetNewTransactionId()
 */
void
RecordKnownAssignedTransactionIds(TransactionId xid)
{
	Assert(standbyState >= STANDBY_INITIALIZED);
	Assert(TransactionIdIsValid(xid));
	Assert(TransactionIdIsValid(latestObservedXid));

	elog(trace_recovery(DEBUG4), "record known xact %u latestObservedXid %u",
		 xid, latestObservedXid);

	/*
	 * When a newly observed xid arrives, it is frequently the case that it is
	 * *not* the next xid in sequence. When this occurs, we must treat the
	 * intervening xids as running also.
	 */
	if (TransactionIdFollows(xid, latestObservedXid))
	{
		TransactionId next_expected_xid;

		/*
		 * Extend csnlog like we do in GetNewTransactionId() during normal
		 * operation using individual extend steps. Note that we do not need
		 * to extend clog since its extensions are WAL logged.
		 *
		 * This part has to be done regardless of standbyState since we
		 * immediately start assigning subtransactions to their toplevel
		 * transactions.
		 */
		next_expected_xid = latestObservedXid;
		while (TransactionIdPrecedes(next_expected_xid, xid))
		{
			TransactionIdAdvance(next_expected_xid);
#ifdef ENABLE_DISTRIBUTED_TRANSACTION

			/*
			 * Each time the transaction/subtransaction id is allocated, we
			 * have written CTS extend WAL entry. As a result, no extra CTS
			 * extend is needed here to avoid nested WAL which would report
			 * error. Written by Junbin Kang, 2020.06.16
			 */
#else
			ExtendCSNLOG(next_expected_xid);
#endif
		}
		Assert(next_expected_xid == xid);

		/*
		 * Now we can advance latestObservedXid
		 */
		latestObservedXid = xid;

		/* ShmemVariableCache->nextXid must be beyond any observed xid */
		next_expected_xid = latestObservedXid;
		TransactionIdAdvance(next_expected_xid);
		LWLockAcquire(XidGenLock, LW_EXCLUSIVE);
		ShmemVariableCache->nextXid = next_expected_xid;
		LWLockRelease(XidGenLock);
	}
}
