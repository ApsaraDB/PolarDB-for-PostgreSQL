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
 * transactions.  The xid and subxids fields of these are valid, as are the
 * myProcLocks lists.  They can be distinguished from regular backend PGPROCs
 * at need by checking for pid == 0.
 *
 * During hot standby, we also keep a list of XIDs representing transactions
 * that are known to be running in the master (or more precisely, were running
 * as of the current point in the WAL stream).  This list is kept in the
 * KnownAssignedXids array, and is updated by watching the sequence of
 * arriving XIDs.  This is necessary because if we leave those XIDs out of
 * snapshots taken for standby queries, then they will appear to be already
 * complete, leading to MVCC failures.  Note that in hot standby, the PGPROC
 * array represents standby processes, which by definition are not running
 * transactions that have XIDs.
 *
 * It is perhaps possible for a backend on the master to terminate without
 * writing an abort record for its transaction.  While that shouldn't really
 * happen, it would tie up KnownAssignedXids indefinitely, so we protect
 * ourselves by pruning the array when a valid list of running XIDs arrives.
 *
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
#include "access/subtrans.h"
#include "access/transam.h"
#include "access/twophase.h"
#include "access/xact.h"
#include "access/xlog.h"
#include "catalog/catalog.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "storage/proc.h"
#include "storage/procarray.h"
#include "storage/spin.h"
#include "utils/builtins.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"

/* POLAR */
#include "access/polar_logindex_redo.h"
#include "access/xlogdefs.h"
#include "utils/guc.h"
/* POLAR end */

/* POLAR csn */
#include "access/polar_csnlog.h"
#include "access/polar_csn_mvcc_vars.h"
/* POLAR end */

/*
 * polar replica multi version snapshot related structure
 */

/*
 * BasedOn:	SnapshotData and ProcArrayStruct
 * in replica mode, we can define database snapshot as below
 * for conveniently processing multi version snapshots 
 */
typedef struct polar_replica_multi_version_snapshot_t
{
	/* 
	 * Fields below are same meaning as in SnapshotData 
	 */

	TransactionId 	*xip;
	TransactionId 	xmin;
	TransactionId 	xmax;
	uint32			xcnt;	
	bool			overflowed;	

	/* Fields below are same meaning as in ProcArrayStruct */

	TransactionId 	replication_slot_xmin;
	TransactionId 	replication_slot_catalog_xmin;
} polar_replica_multi_version_snapshot_t;

/*
 *	we use slots to store multi version snapshots
 *	allocated in share memory
 */
typedef struct polar_replica_multi_version_snapshot_store_t
{
	/* 
	 * snapshot slot count
	 * set from guc var polar_replica_multi_version_snapshot_slot_num,
	 * range from 32 to 128, default is 32
	 */
	int            							slot_num;	

	/*
	 * snapshot set/get retry times
	 * trylock access this number of slots before switch to old get snapshot logic
	 * set from guc var polar_replica_multi_version_snapshot_retry_times,
	 * range from 0 to INT_MAX, default is 3 
	 */
	int										retry_times;

	/* 
	 * lwlock per slot
	 * both read and write need try lock in case of slot reuse 
	 * start from 0
	 * we want each lock fill entire cache line to avoid false sharing
	 */	
	LWLockPadded  							*slot_locks;	

	/* 
	 * slot array for multi version snapshot
	 * start from 0
	 */ 	
	polar_replica_multi_version_snapshot_t	*slot_snapshots; 

	/* 
	 * current slot number for snapshot read
	 * get by snapshot read backend; set by replica startup backend
	 * -1(PG_UINT32_MAX) for invalid value 
	 * initial value is -1
	 */
	pg_atomic_uint32    					curr_slot_idx;

	/* 
	 * next slot number for snapshot write
	 * only set by replica startup backend
	 * if slot not available, need retry
	 * initial value is 0
	 */
	int            							next_slot_idx;

	/* metric related fields */

	/* 
	 * write process can not lock acquire slot immediately,
	 * maybe lots of readers preemptively run cpu or slot num should enlarge.
	 * this metric indicate cpu time waste
	 */
	uint64_t								write_retried_times;

	/* 
	 * write process can not exclusive lock acquire slot immediately and has switched back
	 * to single version snapshot get
	 * this metric indicate ProcArrayLock wait cost
	 */
	uint64_t								write_switched_times;

	/*
	 * read process can not share lock acquire slot immediately or get invalid slot num,
	 * maybe write process can not get cpu or get ProcArrayLock to make valid snapshot
	 * this metric indicate cpu time waste
	 * 
	 * NB: need concurrent update
	 */
	pg_atomic_uint64								read_retried_times;

	/*
	 * read process can not share lock acquire slot immediately or get invalid slot num,
	 * and has switched back to single version snapshot get
	 * this metric indicate ProcArrayLock wait cost
	 * 
	 * NB: need concurrent update
	 */
	pg_atomic_uint64								read_switched_times;
} polar_replica_multi_version_snapshot_store_t;

/* multi version snapshots store must be global var */
static polar_replica_multi_version_snapshot_store_t *polar_replica_multi_version_snapshot_store;

/* Our shared memory area */
typedef struct ProcArrayStruct
{
	int			numProcs;		/* number of valid procs entries */
	int			maxProcs;		/* allocated size of procs array */

	/*
	 * Known assigned XIDs handling
	 */
	int			maxKnownAssignedXids;	/* allocated size of array */
	int			numKnownAssignedXids;	/* current # of valid entries */
	int			tailKnownAssignedXids;	/* index of oldest valid element */
	int			headKnownAssignedXids;	/* index of newest element, + 1 */
	slock_t		known_assigned_xids_lck;	/* protects head/tail pointers */

	/*
	 * Highest subxid that has been removed from KnownAssignedXids array to
	 * prevent overflow; or InvalidTransactionId if none.  We track this for
	 * similar reasons to tracking overflowing cached subxids in PGXACT
	 * entries.  Must hold exclusive ProcArrayLock to change this, and shared
	 * lock to read it.
	 */
	TransactionId lastOverflowedXid;

	/* oldest xmin of any replication slot */
	TransactionId replication_slot_xmin;
	/* oldest catalog xmin of any replication slot */
	TransactionId replication_slot_catalog_xmin;

	/* indexes into allPgXact[], has PROCARRAY_MAXPROCS entries */
	int			pgprocnos[FLEXIBLE_ARRAY_MEMBER];
} ProcArrayStruct;

static ProcArrayStruct *procArray;

static PGPROC *allProcs;
static PGXACT *allPgXact;

/*
 * Bookkeeping for tracking emulated transactions in recovery
 */
static TransactionId *KnownAssignedXids;
static bool *KnownAssignedXidsValid;
static TransactionId latestObservedXid = InvalidTransactionId;

/*
 * If we're in STANDBY_SNAPSHOT_PENDING state, standbySnapshotPendingXmin is
 * the highest xid that might still be running that we don't have in
 * KnownAssignedXids.
 */
static TransactionId standbySnapshotPendingXmin;

#ifdef XIDCACHE_DEBUG

/* counters for XidCache measurement */
static long xc_by_recent_xmin = 0;
static long xc_by_known_xact = 0;
static long xc_by_my_xact = 0;
static long xc_by_latest_xid = 0;
static long xc_by_main_xid = 0;
static long xc_by_child_xid = 0;
static long xc_by_known_assigned = 0;
static long xc_no_overflow = 0;
static long xc_slow_answer = 0;

#define xc_by_recent_xmin_inc()		(xc_by_recent_xmin++)
#define xc_by_known_xact_inc()		(xc_by_known_xact++)
#define xc_by_my_xact_inc()			(xc_by_my_xact++)
#define xc_by_latest_xid_inc()		(xc_by_latest_xid++)
#define xc_by_main_xid_inc()		(xc_by_main_xid++)
#define xc_by_child_xid_inc()		(xc_by_child_xid++)
#define xc_by_known_assigned_inc()	(xc_by_known_assigned++)
#define xc_no_overflow_inc()		(xc_no_overflow++)
#define xc_slow_answer_inc()		(xc_slow_answer++)

static void DisplayXidCache(void);
#else							/* !XIDCACHE_DEBUG */

#define xc_by_recent_xmin_inc()		((void) 0)
#define xc_by_known_xact_inc()		((void) 0)
#define xc_by_my_xact_inc()			((void) 0)
#define xc_by_latest_xid_inc()		((void) 0)
#define xc_by_main_xid_inc()		((void) 0)
#define xc_by_child_xid_inc()		((void) 0)
#define xc_by_known_assigned_inc()	((void) 0)
#define xc_no_overflow_inc()		((void) 0)
#define xc_slow_answer_inc()		((void) 0)
#endif							/* XIDCACHE_DEBUG */

/* Primitives for KnownAssignedXids array handling for standby */
static void KnownAssignedXidsCompress(bool force);
static void KnownAssignedXidsAdd(TransactionId from_xid, TransactionId to_xid,
					 bool exclusive_lock);
static bool KnownAssignedXidsSearch(TransactionId xid, bool remove);
static bool KnownAssignedXidExists(TransactionId xid);
static void KnownAssignedXidsRemove(TransactionId xid);
static void KnownAssignedXidsRemoveTree(TransactionId xid, int nsubxids,
							TransactionId *subxids);
static void KnownAssignedXidsRemovePreceding(TransactionId xid);
static int	KnownAssignedXidsGet(TransactionId *xarray, TransactionId xmax);
static int KnownAssignedXidsGetAndSetXmin(TransactionId *xarray,
							   TransactionId *xmin,
							   TransactionId xmax);
static TransactionId KnownAssignedXidsGetOldestXmin(void);
static void KnownAssignedXidsDisplay(int trace_level);
static void KnownAssignedXidsReset(void);
static inline void ProcArrayEndTransactionInternal(PGPROC *proc,
								PGXACT *pgxact, TransactionId latestXid);
static void ProcArrayGroupClearXid(PGPROC *proc, TransactionId latestXid);

/* POLAR begin */
static void polar_replica_multi_version_snapshot_store_shmem_init(void);
static void polar_replica_multi_version_snapshot_set_snapshot(void);
static bool polar_replica_multi_version_snapshot_get_snapshot(TransactionId *xip, int *count, bool *overflowed,
				   								  	TransactionId *xmin, TransactionId *xmax, 
												  	TransactionId *replication_slot_xmin, 
												  	TransactionId *replication_slot_catalog_xmin);
/* POLAR end */

/* POLAR csn */
static void AdvanceOldestActiveXidCSN(TransactionId myXid);
static void resetGlobalXminCacheCSN(void);
static Snapshot GetSnapshotDataCSN(Snapshot snapshot);
static void ProcArrayEndTransactionCSN(PGPROC *proc);
static void ProcArrayRemoveCSN(PGPROC *proc);
/* POLAR end */

static void resetGlobalXminCacheCSN(void)
{
	RecentGlobalXmin = InvalidTransactionId;
	RecentGlobalDataXmin = InvalidTransactionId;
}

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

	/*
	 * During Hot Standby processing we have a data structure called
	 * KnownAssignedXids, created in shared memory. Local data structures are
	 * also created in various backends during GetSnapshotData(),
	 * TransactionIdIsInProgress() and GetRunningTransactionData(). All of the
	 * main structures created in those functions must be identically sized,
	 * since we may at times copy the whole of the data structures around. We
	 * refer to this size as TOTAL_MAX_CACHED_SUBXIDS.
	 *
	 * Ideally we'd only create this structure if we were actually doing hot
	 * standby in the current run, but we don't know that yet at the time
	 * shared memory is being set up.
	 */
#define TOTAL_MAX_CACHED_SUBXIDS \
	((PGPROC_MAX_CACHED_SUBXIDS + 1) * PROCARRAY_MAXPROCS)

	if (EnableHotStandby)
	{
		size = add_size(size,
						mul_size(sizeof(TransactionId),
								 TOTAL_MAX_CACHED_SUBXIDS));
		size = add_size(size,
						mul_size(sizeof(bool), TOTAL_MAX_CACHED_SUBXIDS));
	}

	/* 
	 * We should allocate multi version snapshot memory whether or not we are in replica mode
	 * because master may switch to replica.  
	 * Max slot number is 128
	 * size of polar_replica_multi_version_snapshot_t is 32+TOTAL_MAX_CACHED_SUBXIDS*4 bytes,
	 * if MaxBackends is 1K and max_prepared_xacts is MaxBackends, the values is 32+130K*4 = 552KB
	 * then max total shared memory size we may be waste is about 128*552KB = 70MB
	 */
	if (polar_replica_multi_version_snapshot_enable)
		size = add_size(size, polar_replica_multi_version_snapshot_store_shmem_size());
    
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
		procArray->maxKnownAssignedXids = TOTAL_MAX_CACHED_SUBXIDS;
		procArray->numKnownAssignedXids = 0;
		procArray->tailKnownAssignedXids = 0;
		procArray->headKnownAssignedXids = 0;
		SpinLockInit(&procArray->known_assigned_xids_lck);
		procArray->lastOverflowedXid = InvalidTransactionId;
		procArray->replication_slot_xmin = InvalidTransactionId;
		procArray->replication_slot_catalog_xmin = InvalidTransactionId;
	}

	allProcs = ProcGlobal->allProcs;
	allPgXact = ProcGlobal->allPgXact;

	/* Create or attach to the KnownAssignedXids arrays too, if needed */
	if (EnableHotStandby)
	{
		KnownAssignedXids = (TransactionId *)
			ShmemInitStruct("KnownAssignedXids",
							mul_size(sizeof(TransactionId),
									 TOTAL_MAX_CACHED_SUBXIDS),
							&found);
		KnownAssignedXidsValid = (bool *)
			ShmemInitStruct("KnownAssignedXidsValid",
							mul_size(sizeof(bool), TOTAL_MAX_CACHED_SUBXIDS),
							&found);
	}

	/* Register and initialize fields of ProcLWLockTranche */
	LWLockRegisterTranche(LWTRANCHE_PROC, "proc");

	/*
	 * Create or attach to the polar_replica_multi_version_snapshot_store_t structure.
	 */
	if (polar_replica_multi_version_snapshot_enable)
	{
		polar_replica_multi_version_snapshot_store_shmem_init();
	}
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
 *
 * When latestXid is a valid XID, we are removing a live 2PC gxact from the
 * array, and thus causing it to appear as "not running" anymore.  In this
 * case we must advance latestCompletedXid.  (This is essentially the same
 * as ProcArrayEndTransaction followed by removal of the PGPROC, but we take
 * the ProcArrayLock only once, and don't damage the content of the PGPROC;
 * twophase.c depends on the latter.)
 */
void
ProcArrayRemove(PGPROC *proc, TransactionId latestXid)
{
	ProcArrayStruct *arrayP = procArray;
	int			index;

#ifdef XIDCACHE_DEBUG
	/* dump stats at backend shutdown, but not prepared-xact end */
	if (proc->pid != 0)
		DisplayXidCache();
#endif

	if (polar_csn_enable)
		return ProcArrayRemoveCSN(proc);

	LWLockAcquire(ProcArrayLock, LW_EXCLUSIVE);
	
	if (TransactionIdIsValid(latestXid))
	{
		Assert(TransactionIdIsValid(allPgXact[proc->pgprocno].xid));

		/* Advance global latestCompletedXid while holding the lock */
		if (TransactionIdPrecedes(ShmemVariableCache->latestCompletedXid,
								  latestXid))
			ShmemVariableCache->latestCompletedXid = latestXid;
	}
	else
	{
		/* Shouldn't be trying to remove a live transaction here */
		Assert(!TransactionIdIsValid(allPgXact[proc->pgprocno].xid));
	}

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

/*
 * POLAR csn
 * Like ProcArrayRemove, but we should also advance polar_oldest_active_xid
 * and reset xmin like ProcArrayEndTransactionCSN
 */
static void
ProcArrayRemoveCSN(PGPROC *proc)
{
	ProcArrayStruct *arrayP = procArray;
	int			index;
	PGXACT     *pgxact = &allPgXact[proc->pgprocno];
	TransactionId myXid = pgxact->xid;

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

			/* 
			 * If we were the oldest active XID, advance oldestXid. 
			 * 2pc need this.
			 */
			if (TransactionIdIsValid(myXid))
				AdvanceOldestActiveXidCSN(myXid);

			/* Reset cached variables */
			resetGlobalXminCacheCSN();

			return;
		}
	}

	/*no cover begin*/
	/* Oops */
	LWLockRelease(ProcArrayLock);

	elog(LOG, "failed to find proc %p in ProcArray", proc);
	/*no cover end*/
}

/*
 * ProcArrayEndTransaction -- mark a transaction as no longer running
 *
 * This is used interchangeably for commit and abort cases.  The transaction
 * commit/abort must already be reported to WAL and pg_xact.
 *
 * proc is currently always MyProc, but we pass it explicitly for flexibility.
 * latestXid is the latest Xid among the transaction's main XID and
 * subtransactions, or InvalidTransactionId if it has no XID.  (We must ask
 * the caller to pass latestXid, instead of computing it from the PGPROC's
 * contents, because the subxid information in the PGPROC might be
 * incomplete.)
 */
void
ProcArrayEndTransaction(PGPROC *proc, TransactionId latestXid)
{
	PGXACT	   *pgxact = &allPgXact[proc->pgprocno];

	if (polar_csn_enable)
		return ProcArrayEndTransactionCSN(proc);

	if (TransactionIdIsValid(latestXid))
	{
		/*
		 * We must lock ProcArrayLock while clearing our advertised XID, so
		 * that we do not exit the set of "running" transactions while someone
		 * else is taking a snapshot.  See discussion in
		 * src/backend/access/transam/README.
		 */
		Assert(TransactionIdIsValid(allPgXact[proc->pgprocno].xid));

		/*
		 * If we can immediately acquire ProcArrayLock, we clear our own XID
		 * and release the lock.  If not, use group XID clearing to improve
		 * efficiency.
		 */
		if (LWLockConditionalAcquire(ProcArrayLock, LW_EXCLUSIVE))
		{
			ProcArrayEndTransactionInternal(proc, pgxact, latestXid);
			LWLockRelease(ProcArrayLock);
		}
		else
			ProcArrayGroupClearXid(proc, latestXid);
	}
	else
	{
		/*
		 * If we have no XID, we don't need to lock, since we won't affect
		 * anyone else's calculation of a snapshot.  We might change their
		 * estimate of global xmin, but that's OK.
		 */
		Assert(!TransactionIdIsValid(allPgXact[proc->pgprocno].xid));

		proc->lxid = InvalidLocalTransactionId;
		pgxact->xmin = InvalidTransactionId;
		/* must be cleared with xid/xmin: */
		pgxact->vacuumFlags &= ~PROC_VACUUM_STATE_MASK;
		pgxact->delayChkpt = false; /* be sure this is cleared in abort */
		proc->recoveryConflictPending = false;

		Assert(pgxact->nxids == 0);
		Assert(pgxact->overflowed == false);
	}
}

/*
 * Mark a write transaction as no longer running.
 *
 * We don't do any locking here; caller must handle that.
 */
static inline void
ProcArrayEndTransactionInternal(PGPROC *proc, PGXACT *pgxact,
								TransactionId latestXid)
{
	pgxact->xid = InvalidTransactionId;
	proc->lxid = InvalidLocalTransactionId;
	pgxact->xmin = InvalidTransactionId;
	/* must be cleared with xid/xmin: */
	pgxact->vacuumFlags &= ~PROC_VACUUM_STATE_MASK;
	pgxact->delayChkpt = false; /* be sure this is cleared in abort */
	proc->recoveryConflictPending = false;

	/* Clear the subtransaction-XID cache too while holding the lock */
	pgxact->nxids = 0;
	pgxact->overflowed = false;

	/* Also advance global latestCompletedXid while holding the lock */
	if (TransactionIdPrecedes(ShmemVariableCache->latestCompletedXid,
							  latestXid))
		ShmemVariableCache->latestCompletedXid = latestXid;
}

static void
ProcArrayEndTransactionCSN(PGPROC *proc)
{
	PGXACT	   *pgxact = &allPgXact[proc->pgprocno];
	TransactionId myXid;

	myXid = pgxact->xid;

	/* A shared lock is enough to modify our own fields */
	LWLockAcquire(ProcArrayLock, LW_SHARED);

	pgxact->xid = InvalidTransactionId;
	pgxact->polar_csn = InvalidCommitSeqNo;
	proc->lxid = InvalidLocalTransactionId;
	pgxact->xmin = InvalidTransactionId;
	/* must be cleared with xid/xmin/polar_csn: */
	pgxact->vacuumFlags &= ~PROC_VACUUM_STATE_MASK;
	pgxact->delayChkpt = false; /* be sure this is cleared in abort */
	proc->recoveryConflictPending = false;

	/* Clear the subtransaction-XID cache too while holding the lock */
	pgxact->nxids = 0;
	pgxact->overflowed = false;
	
	LWLockRelease(ProcArrayLock);

	/* If we were the oldest active XID, advance oldestXid */
	if (TransactionIdIsValid(myXid))
		AdvanceOldestActiveXidCSN(myXid);

	/* Reset cached variables */
	resetGlobalXminCacheCSN();
}

void
ProcArrayResetXminCSN(PGPROC *proc, TransactionId new_xmin)
{
	PGXACT	   *pgxact = &allPgXact[proc->pgprocno];

	/*
	 * Note we can do this without locking because we assume that storing an Xid
	 * is atomic.
	 */
	pgxact->xmin = new_xmin;

	/* Reset cached variables */
	resetGlobalXminCacheCSN();
}

/*
 * ProcArrayGroupClearXid -- group XID clearing
 *
 * When we cannot immediately acquire ProcArrayLock in exclusive mode at
 * commit time, add ourselves to a list of processes that need their XIDs
 * cleared.  The first process to add itself to the list will acquire
 * ProcArrayLock in exclusive mode and perform ProcArrayEndTransactionInternal
 * on behalf of all group members.  This avoids a great deal of contention
 * around ProcArrayLock when many processes are trying to commit at once,
 * since the lock need not be repeatedly handed off from one committing
 * process to the next.
 */
static void
ProcArrayGroupClearXid(PGPROC *proc, TransactionId latestXid)
{
	volatile PROC_HDR *procglobal = ProcGlobal;
	uint32		nextidx;
	uint32		wakeidx;

	/* We should definitely have an XID to clear. */
	Assert(TransactionIdIsValid(allPgXact[proc->pgprocno].xid));

	/* Add ourselves to the list of processes needing a group XID clear. */
	proc->procArrayGroupMember = true;
	proc->procArrayGroupMemberXid = latestXid;
	while (true)
	{
		nextidx = pg_atomic_read_u32(&procglobal->procArrayGroupFirst);
		pg_atomic_write_u32(&proc->procArrayGroupNext, nextidx);

		if (pg_atomic_compare_exchange_u32(&procglobal->procArrayGroupFirst,
										   &nextidx,
										   (uint32) proc->pgprocno))
			break;
	}

	/*
	 * If the list was not empty, the leader will clear our XID.  It is
	 * impossible to have followers without a leader because the first process
	 * that has added itself to the list will always have nextidx as
	 * INVALID_PGPROCNO.
	 */
	if (nextidx != INVALID_PGPROCNO)
	{
		int			extraWaits = 0;

		/* Sleep until the leader clears our XID. */
		pgstat_report_wait_start(WAIT_EVENT_PROCARRAY_GROUP_UPDATE);
		for (;;)
		{
			/* acts as a read barrier */
			PGSemaphoreLock(proc->sem);
			if (!proc->procArrayGroupMember)
				break;
			extraWaits++;
		}
		pgstat_report_wait_end();

		Assert(pg_atomic_read_u32(&proc->procArrayGroupNext) == INVALID_PGPROCNO);

		/* Fix semaphore count for any absorbed wakeups */
		while (extraWaits-- > 0)
			PGSemaphoreUnlock(proc->sem);
		return;
	}

	/* We are the leader.  Acquire the lock on behalf of everyone. */
	LWLockAcquire(ProcArrayLock, LW_EXCLUSIVE);

	/*
	 * Now that we've got the lock, clear the list of processes waiting for
	 * group XID clearing, saving a pointer to the head of the list.  Trying
	 * to pop elements one at a time could lead to an ABA problem.
	 */
	while (true)
	{
		nextidx = pg_atomic_read_u32(&procglobal->procArrayGroupFirst);
		if (pg_atomic_compare_exchange_u32(&procglobal->procArrayGroupFirst,
										   &nextidx,
										   INVALID_PGPROCNO))
			break;
	}

	/* Remember head of list so we can perform wakeups after dropping lock. */
	wakeidx = nextidx;

	/* Walk the list and clear all XIDs. */
	while (nextidx != INVALID_PGPROCNO)
	{
		PGPROC	   *proc = &allProcs[nextidx];
		PGXACT	   *pgxact = &allPgXact[nextidx];

		ProcArrayEndTransactionInternal(proc, pgxact, proc->procArrayGroupMemberXid);

		/* Move to next proc in list. */
		nextidx = pg_atomic_read_u32(&proc->procArrayGroupNext);
	}

	/* We're done with the lock now. */
	LWLockRelease(ProcArrayLock);

	/*
	 * Now that we've released the lock, go back and wake everybody up.  We
	 * don't do this under the lock so as to keep lock hold times to a
	 * minimum.  The system calls we need to perform to wake other processes
	 * up are probably much slower than the simple memory writes we did while
	 * holding the lock.
	 */
	while (wakeidx != INVALID_PGPROCNO)
	{
		PGPROC	   *proc = &allProcs[wakeidx];

		wakeidx = pg_atomic_read_u32(&proc->procArrayGroupNext);
		pg_atomic_write_u32(&proc->procArrayGroupNext, INVALID_PGPROCNO);

		/* ensure all previous writes are visible before follower continues. */
		pg_write_barrier();

		proc->procArrayGroupMember = false;

		if (proc != MyProc)
			PGSemaphoreUnlock(proc->sem);
	}
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
	pgxact->polar_csn = InvalidCommitSeqNo;
	proc->lxid = InvalidLocalTransactionId;
	pgxact->xmin = InvalidTransactionId;
	proc->recoveryConflictPending = false;

	/* redundant, but just in case */
	pgxact->vacuumFlags &= ~PROC_VACUUM_STATE_MASK;
	pgxact->delayChkpt = false;

	/* Clear the subtransaction-XID cache too */
	pgxact->nxids = 0;
	pgxact->overflowed = false;

	if (polar_csn_enable)
	{
		/*
		 * We don't need to update oldestActiveXid, because the gxact entry in
		 * the procarray is still running with the same XID.
		 */

		/* Reset cached variables */
		resetGlobalXminCacheCSN();
	}
}

/*
 * ProcArrayInitRecovery -- initialize recovery xid mgmt environment
 *
 * Remember up to where the startup process initialized the CLOG and subtrans
 * so we can ensure it's initialized gaplessly up to the point where necessary
 * while in recovery.
 */
void
ProcArrayInitRecovery(TransactionId initializedUptoXID, TransactionId polar_oldest_active_xid)
{
	Assert(standbyState == STANDBY_INITIALIZED);
	Assert(TransactionIdIsNormal(initializedUptoXID));

	/*
	 * we set latestObservedXid to the xid SUBTRANS has been initialized up
	 * to, so we can extend it from that point onwards in
	 * RecordKnownAssignedTransactionIds, and when we get consistent in
	 * ProcArrayApplyRecoveryInfo().
	 */
	latestObservedXid = initializedUptoXID;
	TransactionIdRetreat(latestObservedXid);

	if (polar_csn_enable)
	{
		/* also initialize oldestActiveXid */
		pg_atomic_write_u32(&polar_shmem_csn_mvcc_var_cache->polar_oldest_active_xid, polar_oldest_active_xid);
	}
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
	TransactionId *xids;
	int			nxids;
	TransactionId nextXid;
	int			i;

	Assert(standbyState >= STANDBY_INITIALIZED);
	Assert(TransactionIdIsValid(running->nextXid));
	Assert(TransactionIdIsValid(running->oldestRunningXid));
	Assert(TransactionIdIsNormal(running->latestCompletedXid));

	/*
	 * Remove stale transactions, if any.
	 */
	ExpireOldKnownAssignedTransactionIds(running->oldestRunningXid);

	/*
	 * Remove stale locks, if any.
	 */
	StandbyReleaseOldLocks(running->oldestRunningXid);

	/*
	 * If our snapshot is already valid, nothing else to do...
	 */
	if (standbyState == STANDBY_SNAPSHOT_READY)
		return;

	/*
	 * If our initial RunningTransactionsData had an overflowed snapshot then
	 * we knew we were missing some subxids from our snapshot. If we continue
	 * to see overflowed snapshots then we might never be able to start up, so
	 * we make another test to see if our snapshot is now valid. We know that
	 * the missing subxids are equal to or earlier than nextXid. After we
	 * initialise we continue to apply changes during recovery, so once the
	 * oldestRunningXid is later than the nextXid from the initial snapshot we
	 * know that we no longer have missing information and can mark the
	 * snapshot as valid.
	 */
	if (standbyState == STANDBY_SNAPSHOT_PENDING)
	{
		/*
		 * If the snapshot isn't overflowed or if its empty we can reset our
		 * pending state and use this snapshot instead.
		 */
		if (!running->subxid_overflow || running->xcnt == 0)
		{
			/*
			 * If we have already collected known assigned xids, we need to
			 * throw them away before we apply the recovery snapshot.
			 */
			KnownAssignedXidsReset();
			standbyState = STANDBY_INITIALIZED;
			polar_set_hot_standby_state(STANDBY_INITIALIZED);
		}
		else
		{
			if (TransactionIdPrecedes(standbySnapshotPendingXmin,
									  running->oldestRunningXid))
			{
				standbyState = STANDBY_SNAPSHOT_READY;
				polar_set_hot_standby_state(STANDBY_SNAPSHOT_READY);
				elog(trace_recovery(DEBUG1),
					 "recovery snapshots are now enabled");
			}
			else
				elog(trace_recovery(DEBUG1),
					 "recovery snapshot waiting for non-overflowed snapshot or "
					 "until oldest active xid on standby is at least %u (now %u)",
					 standbySnapshotPendingXmin,
					 running->oldestRunningXid);
			return;
		}
	}

	Assert(standbyState == STANDBY_INITIALIZED);

	/*
	 * OK, we need to initialise from the RunningTransactionsData record.
	 *
	 * NB: this can be reached at least twice, so make sure new code can deal
	 * with that.
	 */

	/*
	 * Nobody else is running yet, but take locks anyhow
	 */
	LWLockAcquire(ProcArrayLock, LW_EXCLUSIVE);

	/*
	 * KnownAssignedXids is sorted so we cannot just add the xids, we have to
	 * sort them first.
	 *
	 * Some of the new xids are top-level xids and some are subtransactions.
	 * We don't call SubtransSetParent because it doesn't matter yet. If we
	 * aren't overflowed then all xids will fit in snapshot and so we don't
	 * need subtrans. If we later overflow, an xid assignment record will add
	 * xids to subtrans. If RunningXacts is overflowed then we don't have
	 * enough information to correctly update subtrans anyway.
	 */

	/*
	 * Allocate a temporary array to avoid modifying the array passed as
	 * argument.
	 */
	xids = palloc(sizeof(TransactionId) * (running->xcnt + running->subxcnt));

	/*
	 * Add to the temp array any xids which have not already completed.
	 */
	nxids = 0;
	for (i = 0; i < running->xcnt + running->subxcnt; i++)
	{
		TransactionId xid = running->xids[i];

		/*
		 * The running-xacts snapshot can contain xids that were still visible
		 * in the procarray when the snapshot was taken, but were already
		 * WAL-logged as completed. They're not running anymore, so ignore
		 * them.
		 */
		if (TransactionIdDidCommit(xid) || TransactionIdDidAbort(xid))
			continue;

		xids[nxids++] = xid;
	}

	if (nxids > 0)
	{
		if (procArray->numKnownAssignedXids != 0)
		{
			LWLockRelease(ProcArrayLock);
			elog(ERROR, "KnownAssignedXids is not empty");
		}

		/*
		 * Sort the array so that we can add them safely into
		 * KnownAssignedXids.
		 */
		qsort(xids, nxids, sizeof(TransactionId), xidComparator);

		/*
		 * Add the sorted snapshot into KnownAssignedXids.  The running-xacts
		 * snapshot may include duplicated xids because of prepared
		 * transactions, so ignore them.
		 */
		for (i = 0; i < nxids; i++)
		{
			if (i > 0 && TransactionIdEquals(xids[i - 1], xids[i]))
			{
				elog(DEBUG1,
					 "found duplicated transaction %u for KnownAssignedXids insertion",
					 xids[i]);
				continue;
			}
			KnownAssignedXidsAdd(xids[i], xids[i], true);
		}

		KnownAssignedXidsDisplay(trace_recovery(DEBUG3));
	}

	pfree(xids);

	/*
	 * latestObservedXid is at least set to the point where SUBTRANS was
	 * started up to (cf. ProcArrayInitRecovery()) or to the biggest xid
	 * RecordKnownAssignedTransactionIds() was called for.  Initialize
	 * subtrans from thereon, up to nextXid - 1.
	 *
	 * We need to duplicate parts of RecordKnownAssignedTransactionId() here,
	 * because we've just added xids to the known assigned xids machinery that
	 * haven't gone through RecordKnownAssignedTransactionId().
	 */
	Assert(TransactionIdIsNormal(latestObservedXid));
	TransactionIdAdvance(latestObservedXid);
	while (TransactionIdPrecedes(latestObservedXid, running->nextXid))
	{
		/*no cover begin*/
		/* 
		 * In polar csn, we need not extend csnlog here,
		 * because csnlog write zero page wal log like clog
		 */
		if (!polar_csn_enable)
			ExtendSUBTRANS(latestObservedXid);
		/*no cover end*/
		TransactionIdAdvance(latestObservedXid);
	}
	TransactionIdRetreat(latestObservedXid);	/* = running->nextXid - 1 */

	/* ----------
	 * Now we've got the running xids we need to set the global values that
	 * are used to track snapshots as they evolve further.
	 *
	 * - latestCompletedXid which will be the xmax for snapshots
	 * - lastOverflowedXid which shows whether snapshots overflow
	 * - nextXid
	 *
	 * If the snapshot overflowed, then we still initialise with what we know,
	 * but the recovery snapshot isn't fully valid yet because we know there
	 * are some subxids missing. We don't know the specific subxids that are
	 * missing, so conservatively assume the last one is latestObservedXid.
	 * ----------
	 */
	if (running->subxid_overflow)
	{
		standbyState = STANDBY_SNAPSHOT_PENDING;
		polar_set_hot_standby_state(STANDBY_SNAPSHOT_PENDING);

		standbySnapshotPendingXmin = latestObservedXid;
		procArray->lastOverflowedXid = latestObservedXid;
	}
	else
	{
		standbyState = STANDBY_SNAPSHOT_READY;
		polar_set_hot_standby_state(STANDBY_SNAPSHOT_READY);

		standbySnapshotPendingXmin = InvalidTransactionId;
	}
	
	/*
		* If a transaction wrote a commit record in the gap between taking and
		* logging the snapshot then latestCompletedXid may already be higher than
		* the value from the snapshot, so check before we use the incoming value.
		*/
	if (TransactionIdPrecedes(ShmemVariableCache->latestCompletedXid,
							running->latestCompletedXid))
		ShmemVariableCache->latestCompletedXid = running->latestCompletedXid;

	Assert(TransactionIdIsNormal(ShmemVariableCache->latestCompletedXid));

	LWLockRelease(ProcArrayLock);

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

	KnownAssignedXidsDisplay(trace_recovery(DEBUG3));
	if (standbyState == STANDBY_SNAPSHOT_READY)
		elog(trace_recovery(DEBUG1), "recovery snapshots are now enabled");
	else
		elog(trace_recovery(DEBUG1),
			 "recovery snapshot waiting for non-overflowed snapshot or "
			 "until oldest active xid on standby is at least %u (now %u)",
			 standbySnapshotPendingXmin,
			 running->oldestRunningXid);
}

/*
 * ProcArrayApplyXidAssignment
 *		Process an XLOG_XACT_ASSIGNMENT WAL record
 */
void
ProcArrayApplyXidAssignment(TransactionId topxid,
							int nsubxids, TransactionId *subxids)
{
	TransactionId max_xid;
	int			i;

	Assert(standbyState >= STANDBY_INITIALIZED);

	max_xid = TransactionIdLatest(topxid, nsubxids, subxids);

	/*
	 * Mark all the subtransactions as observed.
	 *
	 * NOTE: This will fail if the subxid contains too many previously
	 * unobserved xids to fit into known-assigned-xids. That shouldn't happen
	 * as the code stands, because xid-assignment records should never contain
	 * more than PGPROC_MAX_CACHED_SUBXIDS entries.
	 */
	RecordKnownAssignedTransactionIds(max_xid);

	/*
	 * Notice that we update pg_subtrans with the top-level xid, rather than
	 * the parent xid. This is a difference between normal processing and
	 * recovery, yet is still correct in all cases. The reason is that
	 * subtransaction commit is not marked in clog until commit processing, so
	 * all aborted subtransactions have already been clearly marked in clog.
	 * As a result we are able to refer directly to the top-level
	 * transaction's state rather than skipping through all the intermediate
	 * states in the subtransaction tree. This should be the first time we
	 * have attempted to SubTransSetParent().
	 */
	for (i = 0; i < nsubxids; i++)
	{
		/*no cover begin*/
		if (polar_csn_enable)
			polar_csnlog_set_parent(subxids[i], topxid);
		else
			SubTransSetParent(subxids[i], topxid);
		/*no cover end*/
	}

	/* KnownAssignedXids isn't maintained yet, so we're done for now */
	if (standbyState == STANDBY_INITIALIZED)
		return;

	/*
	 * Uses same locking as transaction commit
	 */
	LWLockAcquire(ProcArrayLock, LW_EXCLUSIVE);

	/*
	 * Remove subxids from known-assigned-xacts.
	 */
	KnownAssignedXidsRemoveTree(InvalidTransactionId, nsubxids, subxids);

	/*
	 * Advance lastOverflowedXid to be at least the last of these subxids.
	 */
	if (TransactionIdPrecedes(procArray->lastOverflowedXid, max_xid))
		procArray->lastOverflowedXid = max_xid;

	LWLockRelease(ProcArrayLock);
}

/*
 * TransactionIdIsInProgress -- is given transaction running in some backend
 *
 * Aside from some shortcuts such as checking RecentXmin and our own Xid,
 * there are four possibilities for finding a running transaction:
 *
 * 1. The given Xid is a main transaction Id.  We will find this out cheaply
 * by looking at the PGXACT struct for each backend.
 *
 * 2. The given Xid is one of the cached subxact Xids in the PGPROC array.
 * We can find this out cheaply too.
 *
 * 3. In Hot Standby mode, we must search the KnownAssignedXids list to see
 * if the Xid is running on the master.
 *
 * 4. Search the SubTrans tree to find the Xid's topmost parent, and then see
 * if that is running according to PGXACT or KnownAssignedXids.  This is the
 * slowest way, but sadly it has to be done always if the others failed,
 * unless we see that the cached subxact sets are complete (none have
 * overflowed).
 *
 * ProcArrayLock has to be held while we do 1, 2, 3.  If we save the top Xids
 * while doing 1 and 3, we can release the ProcArrayLock while we do 4.
 * This buys back some concurrency (and we can't retrieve the main Xids from
 * PGXACT again anyway; see GetNewTransactionId).
 */
bool
TransactionIdIsInProgress(TransactionId xid)
{
	static TransactionId *xids = NULL;
	int			nxids = 0;
	ProcArrayStruct *arrayP = procArray;
	TransactionId topxid;
	int			i,
				j;
	/* POLAR csn */
	TransactionId latestCompletedXid;

	/*
	 * Don't bother checking a transaction older than RecentXmin; it could not
	 * possibly still be running.  (Note: in particular, this guarantees that
	 * we reject InvalidTransactionId, FrozenTransactionId, etc as not
	 * running.)
	 */
	if (TransactionIdPrecedes(xid, RecentXmin))
	{
		xc_by_recent_xmin_inc();
		return false;
	}

	/*
	 * We may have just checked the status of this transaction, so if it is
	 * already known to be completed, we can fall out without any access to
	 * shared memory.
	 */
	if (TransactionIdIsKnownCompleted(xid))
	{
		xc_by_known_xact_inc();
		return false;
	}

	/*
	 * Also, we can handle our own transaction (and subtransactions) without
	 * any access to shared memory.
	 */
	if (TransactionIdIsCurrentTransactionId(xid))
	{
		xc_by_my_xact_inc();
		return true;
	}

	/* POLAR csn */
	if (polar_csn_enable)
	{
		/*
		 * if xact is in progress, maybe the xact is inprogress before crash
		 * recovery, check procArray again
		 */
		if (XID_INPROGRESS != polar_xact_get_status(xid))
			return false;
	}
	/* POLAR end */

	/*
	 * If first time through, get workspace to remember main XIDs in. We
	 * malloc it permanently to avoid repeated palloc/pfree overhead.
	 */
	if (xids == NULL)
	{
		/*
		 * In hot standby mode, reserve enough space to hold all xids in the
		 * known-assigned list. If we later finish recovery, we no longer need
		 * the bigger array, but we don't bother to shrink it.
		 */
		int			maxxids = RecoveryInProgress() ? TOTAL_MAX_CACHED_SUBXIDS : arrayP->maxProcs;

		xids = (TransactionId *) malloc(maxxids * sizeof(TransactionId));
		if (xids == NULL)
			ereport(ERROR,
					(errcode(ERRCODE_OUT_OF_MEMORY),
					 errmsg("out of memory")));
	}

	LWLockAcquire(ProcArrayLock, LW_SHARED);

	/*
	 * Now that we have the lock, we can check latestCompletedXid; if the
	 * target Xid is after that, it's surely still running.
	 */
	/*no cover begin*/
	if (polar_csn_enable)
		latestCompletedXid = pg_atomic_read_u32(&polar_shmem_csn_mvcc_var_cache->polar_latest_completed_xid);
	else
		latestCompletedXid = ShmemVariableCache->latestCompletedXid;
	/*no cover end*/

	if (TransactionIdPrecedes(latestCompletedXid, xid))
	{
		LWLockRelease(ProcArrayLock);
		xc_by_latest_xid_inc();
		return true;
	}

	/* No shortcuts, gotta grovel through the array */
	for (i = 0; i < arrayP->numProcs; i++)
	{
		int			pgprocno = arrayP->pgprocnos[i];
		volatile PGPROC *proc = &allProcs[pgprocno];
		volatile PGXACT *pgxact = &allPgXact[pgprocno];
		TransactionId pxid;

		/* Ignore my own proc --- dealt with it above */
		if (proc == MyProc)
			continue;

		/* Fetch xid just once - see GetNewTransactionId */
		pxid = pgxact->xid;

		if (!TransactionIdIsValid(pxid))
			continue;

		/*
		 * Step 1: check the main Xid
		 */
		if (TransactionIdEquals(pxid, xid))
		{
			LWLockRelease(ProcArrayLock);
			xc_by_main_xid_inc();
			return true;
		}

		/*
		 * We can ignore main Xids that are younger than the target Xid, since
		 * the target could not possibly be their child.
		 */
		if (TransactionIdPrecedes(xid, pxid))
			continue;

		/*
		 * Step 2: check the cached child-Xids arrays
		 */
		for (j = pgxact->nxids - 1; j >= 0; j--)
		{
			/* Fetch xid just once - see GetNewTransactionId */
			TransactionId cxid = proc->subxids.xids[j];

			if (TransactionIdEquals(cxid, xid))
			{
				LWLockRelease(ProcArrayLock);
				xc_by_child_xid_inc();
				return true;
			}
		}

		/*
		 * Save the main Xid for step 4.  We only need to remember main Xids
		 * that have uncached children.  (Note: there is no race condition
		 * here because the overflowed flag cannot be cleared, only set, while
		 * we hold ProcArrayLock.  So we can't miss an Xid that we need to
		 * worry about.)
		 */
		if (pgxact->overflowed)
			xids[nxids++] = pxid;
	}

	/*
	 * Step 3: in hot standby mode, check the known-assigned-xids list.  XIDs
	 * in the list must be treated as running.
	 */
	if (RecoveryInProgress())
	{
		/* none of the PGXACT entries should have XIDs in hot standby mode */
		Assert(nxids == 0);

		if (KnownAssignedXidExists(xid))
		{
			LWLockRelease(ProcArrayLock);
			xc_by_known_assigned_inc();
			return true;
		}

		/*
		 * If the KnownAssignedXids overflowed, we have to check pg_subtrans
		 * too.  Fetch all xids from KnownAssignedXids that are lower than
		 * xid, since if xid is a subtransaction its parent will always have a
		 * lower value.  Note we will collect both main and subXIDs here, but
		 * there's no help for it.
		 */
		if (TransactionIdPrecedesOrEquals(xid, procArray->lastOverflowedXid))
			nxids = KnownAssignedXidsGet(xids, xid);
	}

	LWLockRelease(ProcArrayLock);

	/*
	 * If none of the relevant caches overflowed, we know the Xid is not
	 * running without even looking at pg_subtrans.
	 */
	if (nxids == 0)
	{
		xc_no_overflow_inc();
		return false;
	}

	/*
	 * Step 4: have to check pg_subtrans.
	 *
	 * At this point, we know it's either a subtransaction of one of the Xids
	 * in xids[], or it's not running.  If it's an already-failed
	 * subtransaction, we want to say "not running" even though its parent may
	 * still be running.  So first, check pg_xact to see if it's been aborted.
	 */
	xc_slow_answer_inc();

	if (TransactionIdDidAbort(xid))
		return false;

	/*
	 * It isn't aborted, so check whether the transaction tree it belongs to
	 * is still running (or, more precisely, whether it was running when we
	 * held ProcArrayLock).
	 */
	/*no cover begin*/
	if (polar_csn_enable)
		topxid = polar_csnlog_get_top(xid);
	else
		topxid = SubTransGetTopmostTransaction(xid);
	/*no cover end*/
	Assert(TransactionIdIsValid(topxid));
	if (!TransactionIdEquals(topxid, xid))
	{
		for (i = 0; i < nxids; i++)
		{
			if (TransactionIdEquals(xids[i], topxid))
				return true;
		}
	}

	return false;
}

/*
 * TransactionIdIsActive -- is xid the top-level XID of an active backend?
 *
 * This differs from TransactionIdIsInProgress in that it ignores prepared
 * transactions, as well as transactions running on the master if we're in
 * hot standby.  Also, we ignore subtransactions since that's not needed
 * for current uses.
 *
 * POLAR csn
 * Not used in csn
 */
bool
TransactionIdIsActive(TransactionId xid)
{
	bool		result = false;
	ProcArrayStruct *arrayP = procArray;
	int			i;

	/*
	 * Don't bother checking a transaction older than RecentXmin; it could not
	 * possibly still be running.
	 */
	if (TransactionIdPrecedes(xid, RecentXmin))
		return false;

	LWLockAcquire(ProcArrayLock, LW_SHARED);

	for (i = 0; i < arrayP->numProcs; i++)
	{
		int			pgprocno = arrayP->pgprocnos[i];
		volatile PGPROC *proc = &allProcs[pgprocno];
		volatile PGXACT *pgxact = &allPgXact[pgprocno];
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
 * Advance oldestActiveXid. 'myXid' is the current value, and it's known to be
 * finished now.
 */
static void
AdvanceOldestActiveXidCSN(TransactionId myXid)
{
	TransactionId nextXid;
	TransactionId xid;
	TransactionId oldValue;

	oldValue = pg_atomic_read_u32(&polar_shmem_csn_mvcc_var_cache->polar_oldest_active_xid);

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
		xid = polar_csnlog_get_next_active_xid(xid, nextXid);

		if (xid == oldValue)
		{
			/* nothing more to do */
			break;
		}

		/*
		 * Update oldestActiveXid with that value.
		 */

		/*no cover begin*/
		if (!pg_atomic_compare_exchange_u32(&polar_shmem_csn_mvcc_var_cache->polar_oldest_active_xid,
											&oldValue,
											xid))
		{
			/*
			 * Someone beat us to it. This can happen if we hit the race
			 * condition described below. That's OK. We're no longer the oldest active
			 * XID in that case, so we're done.
			 */
			Assert(TransactionIdFollows(oldValue, myXid));
			break;
		}
		/*no cover end*/

		/*
		 * We're not necessarily done yet. It's possible that the XID that we saw
		 * as still running committed just before we updated oldestActiveXid.
		 * She didn't see herself as the oldest transaction, so she wouldn't
		 * update oldestActiveXid. Loop back to check the XID that we saw as
		 * the oldest in-progress one is still in-progress, and if not, update
		 * oldestActiveXid again, on behalf of that transaction.
		 */
		oldValue = xid;
	}
}

/* Only used for test */
void
AdvanceOldestActiveXidCSNWrapper(TransactionId myXid)
{
	AdvanceOldestActiveXidCSN(myXid);
}

/*
 * This is like GetOldestXmin(NULL, true), but can return slightly stale, cached value.
 */
TransactionId
GetRecentGlobalXminCSN(void)
{
	TransactionId globalXmin;
	ProcArrayStruct *arrayP = procArray;
	int			index;
	volatile TransactionId replication_slot_xmin = InvalidTransactionId;
	volatile TransactionId replication_slot_catalog_xmin = InvalidTransactionId;

	if (TransactionIdIsValid(RecentGlobalXmin))
		return RecentGlobalXmin;

	Assert(!TransactionIdIsValid(RecentGlobalDataXmin));

	LWLockAcquire(ProcArrayLock, LW_SHARED);

	/*
	 * We initialize the MIN() calculation with oldestActiveXid. This
	 * is a lower bound for the XIDs that might appear in the ProcArray later,
	 * and so protects us against overestimating the result due to future
	 * additions.
	 */
	globalXmin = pg_atomic_read_u32(&polar_shmem_csn_mvcc_var_cache->polar_oldest_active_xid);
	Assert(TransactionIdIsNormal(globalXmin));

	for (index = 0; index < arrayP->numProcs; index++)
	{
		int			pgprocno = arrayP->pgprocnos[index];
		volatile PGXACT *pgxact = &allPgXact[pgprocno];
		TransactionId xmin = pgxact->xmin;

		/*
		 * Backend is doing logical decoding which manages xmin separately,
		 * check below.
		 */
		/*no cover begin*/
		if (pgxact->vacuumFlags & PROC_IN_LOGICAL_DECODING)
			continue;
		/*no cover end*/

		if (pgxact->vacuumFlags & PROC_IN_VACUUM)
			continue;

		/*
		 * Consider the transaction's Xmin, if set.
		 */
		if (TransactionIdIsNormal(xmin) &&
			NormalTransactionIdPrecedes(xmin, globalXmin))
			globalXmin = xmin;
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

	/*
	 * Check whether there's a replication slot requiring an older catalog
	 * xmin.
	 */
	if (TransactionIdIsNormal(replication_slot_catalog_xmin) &&
		NormalTransactionIdPrecedes(replication_slot_catalog_xmin, RecentGlobalXmin))
		RecentGlobalXmin = replication_slot_catalog_xmin;

	return RecentGlobalXmin;
}

TransactionId
GetRecentGlobalDataXminCSN(void)
{
	if (TransactionIdIsValid(RecentGlobalDataXmin))
		return RecentGlobalDataXmin;

	Assert(!TransactionIdIsValid(RecentGlobalXmin));
	(void) GetRecentGlobalXminCSN();
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
 * This is also used to determine where to truncate pg_subtrans.  For that
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
 */
TransactionId
GetOldestXmin(Relation rel, int flags)
{
	ProcArrayStruct *arrayP = procArray;
	TransactionId result;
	int			index;
	bool		allDbs;

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

	LWLockAcquire(ProcArrayLock, LW_SHARED);

	/*
	 * We initialize the MIN() calculation with latestCompletedXid + 1. This
	 * is a lower bound for the XIDs that might appear in the ProcArray later,
	 * and so protects us against overestimating the result due to future
	 * additions.
	 *
	 * POLAR csn
	 * We use polar_oldest_active_xid in csn mode
	 */
	if (polar_csn_enable)
		result = pg_atomic_read_u32(&polar_shmem_csn_mvcc_var_cache->polar_oldest_active_xid);
	else
	{
		result = ShmemVariableCache->latestCompletedXid;
		Assert(TransactionIdIsNormal(result));
		TransactionIdAdvance(result);
	}

	for (index = 0; index < arrayP->numProcs; index++)
	{
		int			pgprocno = arrayP->pgprocnos[index];
		volatile PGPROC *proc = &allProcs[pgprocno];
		volatile PGXACT *pgxact = &allPgXact[pgprocno];

		if (pgxact->vacuumFlags & (flags & PROCARRAY_PROC_FLAGS_MASK))
			continue;

		if (allDbs ||
			proc->databaseId == MyDatabaseId ||
			proc->databaseId == 0)	/* always include WalSender */
		{
			/* Fetch xid just once - see GetNewTransactionId */
			TransactionId xid = pgxact->xid;

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
		}
	}

	/* fetch into volatile var while ProcArrayLock is held */
	replication_slot_xmin = procArray->replication_slot_xmin;
	replication_slot_catalog_xmin = procArray->replication_slot_catalog_xmin;

	if (RecoveryInProgress())
	{
		/*
		 * Check to see whether KnownAssignedXids contains an xid value older
		 * than the main procarray.
		 */
		TransactionId kaxmin = InvalidTransactionId;
		
		kaxmin = KnownAssignedXidsGetOldestXmin();

		LWLockRelease(ProcArrayLock);

		if (TransactionIdIsNormal(kaxmin) &&
			TransactionIdPrecedes(kaxmin, result))
			result = kaxmin;
	}
	else
	{
		/*
		 * No other information needed, so release the lock immediately.
		 */
		LWLockRelease(ProcArrayLock);

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

/*
 * GetMaxSnapshotXidCount -- get max size for snapshot XID array
 *
 * We have to export this for use by snapmgr.c.
 */
int
GetMaxSnapshotXidCount(void)
{
	return procArray->maxProcs;
}

/*
 * GetMaxSnapshotSubxidCount -- get max size for snapshot sub-XID array
 *
 * We have to export this for use by snapmgr.c.
 */
int
GetMaxSnapshotSubxidCount(void)
{
	return TOTAL_MAX_CACHED_SUBXIDS;
}

/*
 * GetSnapshotData -- returns information about running transactions.
 *
 * The returned snapshot includes xmin (lowest still-running xact ID),
 * xmax (highest completed xact ID + 1), and a list of running xact IDs
 * in the range xmin <= xid < xmax.  It is used as follows:
 *		All xact IDs < xmin are considered finished.
 *		All xact IDs >= xmax are considered still running.
 *		For an xact ID xmin <= xid < xmax, consult list to see whether
 *		it is considered running or not.
 * This ensures that the set of transactions seen as "running" by the
 * current xact will not change after it takes the snapshot.
 *
 * All running top-level XIDs are included in the snapshot, except for lazy
 * VACUUM processes.  We also try to include running subtransaction XIDs,
 * but since PGPROC has only a limited cache area for subxact XIDs, full
 * information may not be available.  If we find any overflowed subxid arrays,
 * we have to mark the snapshot's subxid data as overflowed, and extra work
 * *may* need to be done to determine what's running (see XidInMVCCSnapshot()
 * in tqual.c).
 *
 * We also update the following backend-global variables:
 *		TransactionXmin: the oldest xmin of any snapshot in use in the
 *			current transaction (this is the same as MyPgXact->xmin).
 *		RecentXmin: the xmin computed for the most recent snapshot.  XIDs
 *			older than this are known not running any more.
 *		RecentGlobalXmin: the global xmin (oldest TransactionXmin across all
 *			running transactions, except those running LAZY VACUUM).  This is
 *			the same computation done by
 *			GetOldestXmin(NULL, PROCARRAY_FLAGS_VACUUM).
 *		RecentGlobalDataXmin: the global xmin for non-catalog tables
 *			>= RecentGlobalXmin
 *
 * Note: this function should probably not be called with an argument that's
 * not statically allocated (see xip allocation below).
 */
Snapshot
GetSnapshotData(Snapshot snapshot)
{
	ProcArrayStruct *arrayP = procArray;
	TransactionId xmin;
	TransactionId xmax;
	TransactionId globalxmin;
	int			index;
	int			count = 0;
	int			subcount = 0;
	bool		suboverflowed = false;
	volatile TransactionId replication_slot_xmin = InvalidTransactionId;
	volatile TransactionId replication_slot_catalog_xmin = InvalidTransactionId;

	Assert(snapshot != NULL);

	/*
	 * Allocating space for maxProcs xids is usually overkill; numProcs would
	 * be sufficient.  But it seems better to do the malloc while not holding
	 * the lock, so we can't look at numProcs.  Likewise, we allocate much
	 * more subxip storage than is probably needed.
	 *
	 * This does open a possibility for avoiding repeated malloc/free: since
	 * maxProcs does not change at runtime, we can simply reuse the previous
	 * xip arrays if any.  (This relies on the fact that all callers pass
	 * static SnapshotData structs.)
	 */
	if (snapshot->xip == NULL)
	{
		/*
		 * First call for this snapshot. Snapshot is same size whether or not
		 * we are in recovery, see later comments.
		 */
		snapshot->xip = (TransactionId *)
			malloc(GetMaxSnapshotXidCount() * sizeof(TransactionId));
		if (snapshot->xip == NULL)
			ereport(ERROR,
					(errcode(ERRCODE_OUT_OF_MEMORY),
					 errmsg("out of memory")));
		Assert(snapshot->subxip == NULL);
		snapshot->subxip = (TransactionId *)
			malloc(GetMaxSnapshotSubxidCount() * sizeof(TransactionId));
		if (snapshot->subxip == NULL)
			ereport(ERROR,
					(errcode(ERRCODE_OUT_OF_MEMORY),
					 errmsg("out of memory")));
	}

	/* POLAR csn */
	if (polar_csn_enable)
		return GetSnapshotDataCSN(snapshot);

	/*
	 *	Rigth now, we only do multi version snapshot in polar replica mode
	 */
	if (polar_in_replica_mode() && polar_replica_multi_version_snapshot_enable) 
	{ 	
		bool succ;

		succ = polar_replica_multi_version_snapshot_get_snapshot(snapshot->subxip, &subcount, &suboverflowed,
			   													 &xmin, &xmax, 
																 (TransactionId *)(&replication_slot_xmin), 
																 (TransactionId *)(&replication_slot_catalog_xmin));
		if (succ) 
		{
			/*
			 * We must keep processing logic between ProcArrayLock acquire 
			 * and release same as original standby code path
			 */

			/* 
			 * code logic before original standby snapshot get
			 */

			/* 
			 * We get xmin/xmax from snapshot store 
			 * use xmax which value is latestCompletedXid+1 in some time point, 
			 * not current latestCompletedXid, to init globalxmin 
			 */
			globalxmin = xmax;

			/* We are in replica mode */
			snapshot->takenDuringRecovery = true;

			/* 
			 * code logic after original standby snapshot get
			 */

			/* We can get/set MyPgXact->xmin atomically without ProcArrayLock */
			if (!TransactionIdIsValid(MyPgXact->xmin))
				MyPgXact->xmin = TransactionXmin = xmin;

			/*
			 * we already get replication_slot_xmin/replication_slot_catalog_xmin 
			 * from snapshot store
			 */

			/* We use goto not if/else block to try to make POLAR code least overlapped with PG */
			goto SNAPSHOT_GOTTEN;
		}
	}

	/*
	 * It is sufficient to get shared lock on ProcArrayLock, even if we are
	 * going to set MyPgXact->xmin.
	 */
	LWLockAcquire(ProcArrayLock, LW_SHARED);

	/* xmax is always latestCompletedXid + 1 */
	xmax = ShmemVariableCache->latestCompletedXid;
	Assert(TransactionIdIsNormal(xmax));
	TransactionIdAdvance(xmax);

	/* initialize xmin calculation with xmax */
	globalxmin = xmin = xmax;

	snapshot->takenDuringRecovery = RecoveryInProgress();

	if (!snapshot->takenDuringRecovery)
	{
		int		   *pgprocnos = arrayP->pgprocnos;
		int			numProcs;

		/*
		 * Spin over procArray checking xid, xmin, and subxids.  The goal is
		 * to gather all active xids, find the lowest xmin, and try to record
		 * subxids.
		 */
		numProcs = arrayP->numProcs;
		for (index = 0; index < numProcs; index++)
		{
			int			pgprocno = pgprocnos[index];
			volatile PGXACT *pgxact = &allPgXact[pgprocno];
			TransactionId xid;

			/*
			 * Backend is doing logical decoding which manages xmin
			 * separately, check below.
			 */
			if (pgxact->vacuumFlags & PROC_IN_LOGICAL_DECODING)
				continue;

			/* Ignore procs running LAZY VACUUM */
			if (pgxact->vacuumFlags & PROC_IN_VACUUM)
				continue;

			/* Update globalxmin to be the smallest valid xmin */
			xid = pgxact->xmin; /* fetch just once */
			if (TransactionIdIsNormal(xid) &&
				NormalTransactionIdPrecedes(xid, globalxmin))
				globalxmin = xid;

			/* Fetch xid just once - see GetNewTransactionId */
			xid = pgxact->xid;

			/*
			 * If the transaction has no XID assigned, we can skip it; it
			 * won't have sub-XIDs either.  If the XID is >= xmax, we can also
			 * skip it; such transactions will be treated as running anyway
			 * (and any sub-XIDs will also be >= xmax).
			 */
			if (!TransactionIdIsNormal(xid)
				|| !NormalTransactionIdPrecedes(xid, xmax))
				continue;

			/*
			 * We don't include our own XIDs (if any) in the snapshot, but we
			 * must include them in xmin.
			 */
			if (NormalTransactionIdPrecedes(xid, xmin))
				xmin = xid;
			if (pgxact == MyPgXact)
				continue;

			/* Add XID to snapshot. */
			snapshot->xip[count++] = xid;

			/*
			 * Save subtransaction XIDs if possible (if we've already
			 * overflowed, there's no point).  Note that the subxact XIDs must
			 * be later than their parent, so no need to check them against
			 * xmin.  We could filter against xmax, but it seems better not to
			 * do that much work while holding the ProcArrayLock.
			 *
			 * The other backend can add more subxids concurrently, but cannot
			 * remove any.  Hence it's important to fetch nxids just once.
			 * Should be safe to use memcpy, though.  (We needn't worry about
			 * missing any xids added concurrently, because they must postdate
			 * xmax.)
			 *
			 * Again, our own XIDs are not included in the snapshot.
			 */
			if (!suboverflowed)
			{
				if (pgxact->overflowed)
					suboverflowed = true;
				else
				{
					int			nxids = pgxact->nxids;

					if (nxids > 0)
					{
						volatile PGPROC *proc = &allProcs[pgprocno];

						memcpy(snapshot->subxip + subcount,
							   (void *) proc->subxids.xids,
							   nxids * sizeof(TransactionId));
						subcount += nxids;
					}
				}
			}
		}
	}
	else
	{
		/*
		 * We're in hot standby, so get XIDs from KnownAssignedXids.
		 *
		 * We store all xids directly into subxip[]. Here's why:
		 *
		 * In recovery we don't know which xids are top-level and which are
		 * subxacts, a design choice that greatly simplifies xid processing.
		 *
		 * It seems like we would want to try to put xids into xip[] only, but
		 * that is fairly small. We would either need to make that bigger or
		 * to increase the rate at which we WAL-log xid assignment; neither is
		 * an appealing choice.
		 *
		 * We could try to store xids into xip[] first and then into subxip[]
		 * if there are too many xids. That only works if the snapshot doesn't
		 * overflow because we do not search subxip[] in that case. A simpler
		 * way is to just store all xids in the subxact array because this is
		 * by far the bigger array. We just leave the xip array empty.
		 *
		 * Either way we need to change the way XidInMVCCSnapshot() works
		 * depending upon when the snapshot was taken, or change normal
		 * snapshot processing so it matches.
		 *
		 * Note: It is possible for recovery to end before we finish taking
		 * the snapshot, and for newly assigned transaction ids to be added to
		 * the ProcArray.  xmax cannot change while we hold ProcArrayLock, so
		 * those newly added transaction ids would be filtered away, so we
		 * need not be concerned about them.
		 */
		subcount = KnownAssignedXidsGetAndSetXmin(snapshot->subxip, &xmin,
												  xmax);

		if (TransactionIdPrecedesOrEquals(xmin, procArray->lastOverflowedXid))
			suboverflowed = true;
	}


	/* fetch into volatile var while ProcArrayLock is held */
	replication_slot_xmin = procArray->replication_slot_xmin;
	replication_slot_catalog_xmin = procArray->replication_slot_catalog_xmin;

	if (!TransactionIdIsValid(MyPgXact->xmin))
		MyPgXact->xmin = TransactionXmin = xmin;

	LWLockRelease(ProcArrayLock);

SNAPSHOT_GOTTEN:

	/*
	 * Update globalxmin to include actual process xids.  This is a slightly
	 * different way of computing it than GetOldestXmin uses, but should give
	 * the same result.
	 */
	if (TransactionIdPrecedes(xmin, globalxmin))
		globalxmin = xmin;

	/* Update global variables too */
	RecentGlobalXmin = globalxmin - vacuum_defer_cleanup_age;
	if (!TransactionIdIsNormal(RecentGlobalXmin))
		RecentGlobalXmin = FirstNormalTransactionId;

	/* Check whether there's a replication slot requiring an older xmin. */
	if (TransactionIdIsValid(replication_slot_xmin) &&
		NormalTransactionIdPrecedes(replication_slot_xmin, RecentGlobalXmin))
		RecentGlobalXmin = replication_slot_xmin;

	/* Non-catalog tables can be vacuumed if older than this xid */
	RecentGlobalDataXmin = RecentGlobalXmin;

	/*
	 * Check whether there's a replication slot requiring an older catalog
	 * xmin.
	 */
	if (TransactionIdIsNormal(replication_slot_catalog_xmin) &&
		NormalTransactionIdPrecedes(replication_slot_catalog_xmin, RecentGlobalXmin))
		RecentGlobalXmin = replication_slot_catalog_xmin;

	RecentXmin = xmin;

	snapshot->xmin = xmin;
	snapshot->xmax = xmax;
	snapshot->xcnt = count;
	snapshot->subxcnt = subcount;
	snapshot->suboverflowed = suboverflowed;

	snapshot->curcid = GetCurrentCommandId(false);

	/*
	 * This is a new snapshot, so set both refcounts are zero, and mark it as
	 * not copied in persistent memory.
	 */
	snapshot->active_count = 0;
	snapshot->regd_count = 0;
	snapshot->copied = false;

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

static Snapshot
GetSnapshotDataCSN(Snapshot snapshot)
{
	TransactionId xmin;
	TransactionId xmax;
	CommitSeqNo snapshotcsn;

	Assert(snapshot != NULL);

	/*
	 * The ProcArrayLock is not needed here. We only set our xmin if
	 * it's not already set. There are only a few functions that check
	 * the xmin under exclusive ProcArrayLock:
	 * 1) ProcArrayInstallRestored/ImportedXmin -- can only care about
	 * our xmin long after it has been first set.
	 * 2) ProcArrayEndTransaction is not called concurrently with
	 * GetSnapshotData.
	 */

	/* Anything older than oldestActiveXid is surely finished by now. */
	xmin = pg_atomic_read_u32(&polar_shmem_csn_mvcc_var_cache->polar_oldest_active_xid);
	/* If no performance issue, we try best to maintain RecentXmin for xid based snapshot */
	RecentXmin = xmin;

	/* Announce my xmin, to hold back GlobalXmin. */
	if (!TransactionIdIsValid(MyPgXact->xmin))
	{
		TransactionId oldest_active_xid;

		MyPgXact->xmin = xmin;
		TransactionXmin = xmin;

		/*
		 * Recheck, if oldestActiveXid advanced after we read it.
		 *
		 * This protects against a race condition with GetRecentGlobalXmin().
		 * If a transaction ends runs GetRecentGlobalXmin(), just after we fetch
		 * polar_oldest_active_xid, but before we set MyPgXact->xmin, it's possible
		 * that GetRecentGlobalXmin() computed a new GlobalXmin that doesn't
		 * cover the xmin that we got. To fix that, check polar_oldest_active_xid
		 * again, after setting xmin. Redoing it once is enough, we don't need
		 * to loop, because the (stale) xmin that we set prevents the same
		 * race condition from advancing RecentGlobalXmin again.
		 *
		 * For a brief moment, we can have the situation that our xmin is
		 * lower than RecentGlobalXmin, but it's OK because we don't use that xmin
		 * until we've re-checked and corrected it if necessary.
		 */

		/*
		 * memory barrier to make sure that setting the xmin in our PGPROC entry
		 * is made visible to others, before the read below.
		 */
		pg_memory_barrier();

		oldest_active_xid  = pg_atomic_read_u32(&polar_shmem_csn_mvcc_var_cache->polar_oldest_active_xid);
		if (oldest_active_xid != xmin)
		{
			/*no cover begin*/
			xmin = oldest_active_xid;

			RecentXmin = xmin;
			MyPgXact->xmin = xmin;
			TransactionXmin = xmin;
			/*no cover end*/
		}
	}

	/*
	 * Get the current snapshot CSN. This
	 * serializes us with any concurrent commits.
	 */
	snapshotcsn = pg_atomic_read_u64(&polar_shmem_csn_mvcc_var_cache->polar_next_csn);
	
	/*
	 * Also get xmax. It is always latestCompletedXid + 1.
	 * Make sure to read it after CSN (see TransactionIdAsyncCommitTree())
	 */
	pg_read_barrier();
	xmax = pg_atomic_read_u32(&polar_shmem_csn_mvcc_var_cache->polar_latest_completed_xid);
	Assert(TransactionIdIsNormal(xmax));
	TransactionIdAdvance(xmax);

	snapshot->xmin = xmin;
	snapshot->xmax = xmax;
	snapshot->polar_snapshot_csn = snapshotcsn;
	snapshot->polar_csn_xid_snapshot = false;
	snapshot->xcnt = 0;
	snapshot->subxcnt = 0;
	snapshot->suboverflowed = false;
	snapshot->curcid = GetCurrentCommandId(false);

	/*
	 * This is a new snapshot, so set both refcounts are zero, and mark it as
	 * not copied in persistent memory.
	 */
	snapshot->active_count = 0;
	snapshot->regd_count = 0;
	snapshot->copied = false;

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

	/* 
	 * We get RecentGlobalXmin/RecentGlobalDataXmin lazily in polar csn.
	 * In master mode, we reset it when end transaction;
	 * In hot standby mode, wal replayed by startup backend, we has to reset
	 * it when get snapshot,
	 * because RecentGlobalXmin/RecentGlobalDataXmin are backend variables.
	 */
	if (RecoveryInProgress())
		resetGlobalXminCacheCSN();

	/* 
	 * We need xid snapshot, should generate it from csn snapshot.
	 * The logic is:
	 * 1. Scan csnlog from xmin(inclusive) to xmax(exclusive)
	 * 2. Add xids whose status are in_progress or committing or 
	 *    committed csn >= snapshotcsn to xid array
	 * Like hot standby, we don't know which xids are top-level and which are
	 * subxacts. So we use subxip to store xids as more as possible. 
	 */
	if (polar_csn_xid_snapshot)
	{
		if (TransactionIdPrecedes(xmin, xmax))
			polar_csnlog_get_running_xids(xmin, xmax, snapshotcsn, GetMaxSnapshotSubxidCount(),
				&snapshot->subxcnt, snapshot->subxip, &snapshot->suboverflowed);

		snapshot->polar_csn_xid_snapshot = true;
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

	/* Get lock so source xact can't end while we're doing this */
	if (polar_csn_enable)
		LWLockAcquire(ProcArrayLock, LW_EXCLUSIVE);	/* In csn mode, we should use exclusive lock */
	else
		LWLockAcquire(ProcArrayLock, LW_SHARED);

	for (index = 0; index < arrayP->numProcs; index++)
	{
		int			pgprocno = arrayP->pgprocnos[index];
		volatile PGPROC *proc = &allProcs[pgprocno];
		volatile PGXACT *pgxact = &allPgXact[pgprocno];
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
	volatile PGXACT *pgxact;

	Assert(TransactionIdIsNormal(xmin));
	Assert(proc != NULL);

	/* Get lock so source xact can't end while we're doing this */
	if (polar_csn_enable)
		LWLockAcquire(ProcArrayLock, LW_EXCLUSIVE);	/* In csn mode, we should use exclusive lock */
	else
		LWLockAcquire(ProcArrayLock, LW_SHARED);

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
 * Similar to GetSnapshotData but returns more information. We include
 * all PGXACTs with an assigned TransactionId, even VACUUM processes and
 * prepared transactions.
 *
 * We acquire XidGenLock and ProcArrayLock, but the caller is responsible for
 * releasing them. Acquiring XidGenLock ensures that no new XIDs enter the proc
 * array until the caller has WAL-logged this snapshot, and releases the
 * lock. Acquiring ProcArrayLock ensures that no transactions commit until the
 * lock is released.
 *
 * The returned data structure is statically allocated; caller should not
 * modify it, and must not assume it is valid past the next call.
 *
 * This is never executed during recovery so there is no need to look at
 * KnownAssignedXids.
 *
 * Dummy PGXACTs from prepared transaction are included, meaning that this
 * may return entries with duplicated TransactionId values coming from
 * transaction finishing to prepare.  Nothing is done about duplicated
 * entries here to not hold on ProcArrayLock more than necessary.
 *
 * We don't worry about updating other counters, we want to keep this as
 * simple as possible and leave GetSnapshotData() as the primary code for
 * that bookkeeping.
 *
 * Note that if any transaction has overflowed its cached subtransactions
 * then there is no real need include any subtransactions.
 * 
 * POLAR csn
 * We also aquire CommitSeqNoLock but the caller is responsible for
 * releasing it. Acquiring CommitSeqNoLock ensures that no transactions commit 
 * until the lock is released.
 *
 * When iterate ProcArray to find running xacts, we not only need check whether 
 * PGXACT->xid is valid, but also need check whether PGXACT->polar_csn is valid.
 * If both field valid, we should not add it to running xids list
 */
RunningTransactions
PolarGetRunningTransactionData(bool ignore_subxid)
{
	/* result workspace */
	static RunningTransactionsData CurrentRunningXactsData;

	ProcArrayStruct *arrayP = procArray;
	RunningTransactions CurrentRunningXacts = &CurrentRunningXactsData;
	TransactionId latestCompletedXid;
	TransactionId oldestRunningXid;
	TransactionId *xids;
	int			index;
	int			count;
	int			subcount;
	bool		suboverflowed;

	/* POLAR csn */
	CommitSeqNo currentSystemCsn pg_attribute_unused() = InvalidCommitSeqNo;

	Assert(!RecoveryInProgress());

	/*
	 * Allocating space for maxProcs xids is usually overkill; numProcs would
	 * be sufficient.  But it seems better to do the malloc while not holding
	 * the lock, so we can't look at numProcs.  Likewise, we allocate much
	 * more subxip storage than is probably needed.
	 *
	 * Should only be allocated in bgwriter, since only ever executed during
	 * checkpoints.
	 */
	if (CurrentRunningXacts->xids == NULL)
	{
		/*
		 * First call
		 */
		CurrentRunningXacts->xids = (TransactionId *)
			malloc(TOTAL_MAX_CACHED_SUBXIDS * sizeof(TransactionId));
		if (CurrentRunningXacts->xids == NULL)
			ereport(ERROR,
					(errcode(ERRCODE_OUT_OF_MEMORY),
					 errmsg("out of memory")));
	}

	xids = CurrentRunningXacts->xids;

	count = subcount = 0;
	suboverflowed = false;

	if (polar_csn_enable)
	{
		/* 
		 * ProcArrayEndTransaction hold share lock 
		 * Ensure no xids leave ProcArray
		 */
		LWLockAcquire(ProcArrayLock, LW_EXCLUSIVE);

		/* 
		 * GetNewTransactionId hold exclusive lock 
		 * Ensure no xids enter ProcArray
		 */
		LWLockAcquire(XidGenLock, LW_SHARED);

		/* 
		 * CommitTransaction hold share lock 
		 * Ensure no xids commit csn and set csn in ProcArray
		 */
		LWLockAcquire(CommitSeqNoLock, LW_EXCLUSIVE);

		/* 
		 * When we get here, xacts in ProcArray should be in status
		 * as below:
		 * 1. active with valid xid
		 * 2. aborted but with valid xid 
		 * 3. committed but with valid xid and csn < current polar_next_csn
		 * xacts satisfy to 1 or 2 are running xids;
		 * xacts satisfy to 3 are not, because their csns are less than
		 * current polar_next_csn, their update should be visible to
		 * current xid snapshot
		 */
	}
	else 
	{
		/*
		* Ensure that no xids enter or leave the procarray while we obtain
		* snapshot.
		*/
		LWLockAcquire(ProcArrayLock, LW_SHARED);
		LWLockAcquire(XidGenLock, LW_SHARED);
	}

	if (polar_csn_enable)
	{
		/* 
		 * POLAR csn
		 * Order is not important here, because xacts can not commit now,
		 * we just want to keep the order consistent with GetSnapshotData.
		 *
		 * oldestRunningXid can be computed by two ways:
		 * 1. smallest xid in running xacts
		 * 2. polar_oldest_active_xid
		 * polar_oldest_active_xid may be less than smallest xid,
		 * but for consistency, we use polar_oldest_active_xid
		 */

		oldestRunningXid = 	pg_atomic_read_u32(&polar_shmem_csn_mvcc_var_cache->polar_oldest_active_xid);
		currentSystemCsn = pg_atomic_read_u64(&polar_shmem_csn_mvcc_var_cache->polar_next_csn);
		latestCompletedXid = pg_atomic_read_u32(&polar_shmem_csn_mvcc_var_cache->polar_latest_completed_xid);
	}
	else
	{
		latestCompletedXid = ShmemVariableCache->latestCompletedXid;
		oldestRunningXid = ShmemVariableCache->nextXid;
	}

	/*
	 * Spin over procArray collecting all xids
	 */
	for (index = 0; index < arrayP->numProcs; index++)
	{
		int			pgprocno = arrayP->pgprocnos[index];
		volatile PGXACT *pgxact = &allPgXact[pgprocno];
		TransactionId xid;

		/* Fetch xid just once - see GetNewTransactionId */
		xid = pgxact->xid;

		/*
		 * We don't need to store transactions that don't have a TransactionId
		 * yet because they will not show as running on a standby server.
		 */
		if (!TransactionIdIsValid(xid))
			continue;

		/* POLAR csn */
		if (polar_csn_enable)
		{
			Assert(pgxact->polar_csn == InvalidCommitSeqNo || pgxact->polar_csn < currentSystemCsn);
			if (pgxact->polar_csn != InvalidCommitSeqNo)
			{
				/* Committed xact can not be added to running xid list */
				continue;
			}
		}

		/*
		 * Be careful not to exclude any xids before calculating the values of
		 * oldestRunningXid and suboverflowed, since these are used to clean
		 * up transaction information held on standbys.
		 */
		if (TransactionIdPrecedes(xid, oldestRunningXid))
			oldestRunningXid = xid;

		if (pgxact->overflowed)
			suboverflowed = true;

		/*
		 * If we wished to exclude xids this would be the right place for it.
		 * Procs with the PROC_IN_VACUUM flag set don't usually assign xids,
		 * but they do during truncation at the end when they get the lock and
		 * truncate, so it is not much of a problem to include them if they
		 * are seen and it is cleaner to include them.
		 */

		xids[count++] = xid;
	}

	/*
	 * Spin over procArray collecting all subxids, but only if there hasn't
	 * been a suboverflow.
	 */
	if (!suboverflowed && !ignore_subxid)
	{
		for (index = 0; index < arrayP->numProcs; index++)
		{
			int			pgprocno = arrayP->pgprocnos[index];
			volatile PGPROC *proc = &allProcs[pgprocno];
			volatile PGXACT *pgxact = &allPgXact[pgprocno];
			int			nxids;

			/* POLAR csn */
			if (polar_csn_enable)
			{
				Assert(pgxact->polar_csn == InvalidCommitSeqNo || pgxact->polar_csn < currentSystemCsn);
				if (pgxact->polar_csn != InvalidCommitSeqNo)
				{
					/* Subxids belong to committed xact can not be added to running xid list either */
					continue;
				}
			}

			/*
			 * Save subtransaction XIDs. Other backends can't add or remove
			 * entries while we're holding XidGenLock.
			 */
			nxids = pgxact->nxids;
			if (nxids > 0)
			{
				memcpy(&xids[count], (void *) proc->subxids.xids,
					   nxids * sizeof(TransactionId));
				count += nxids;
				subcount += nxids;

				/*
				 * Top-level XID of a transaction is always less than any of
				 * its subxids, so we don't need to check if any of the
				 * subxids are smaller than oldestRunningXid
				 */
			}
		}
	}

	/*
	 * It's important *not* to include the limits set by slots here because
	 * snapbuild.c uses oldestRunningXid to manage its xmin horizon. If those
	 * were to be included here the initial value could never increase because
	 * of a circular dependency where slots only increase their limits when
	 * running xacts increases oldestRunningXid and running xacts only
	 * increases if slots do.
	 */

	CurrentRunningXacts->xcnt = count - subcount;
	CurrentRunningXacts->subxcnt = subcount;
	CurrentRunningXacts->subxid_overflow = suboverflowed;
	CurrentRunningXacts->nextXid = ShmemVariableCache->nextXid;
	CurrentRunningXacts->oldestRunningXid = oldestRunningXid;
	CurrentRunningXacts->latestCompletedXid = latestCompletedXid;

	Assert(TransactionIdIsValid(CurrentRunningXacts->nextXid));
	Assert(TransactionIdIsValid(CurrentRunningXacts->oldestRunningXid));
	Assert(TransactionIdIsNormal(CurrentRunningXacts->latestCompletedXid));

	/* We don't release the locks here, the caller is responsible for that */

	return CurrentRunningXacts;
}

/*
 * GetOldestActiveTransactionId()
 *
 * Similar to GetSnapshotData but returns just oldestActiveXid. We include
 * all PGXACTs with an assigned TransactionId, even VACUUM processes.
 * We look at all databases, though there is no need to include WALSender
 * since this has no effect on hot standby conflicts.
 *
 * This is never executed during recovery so there is no need to look at
 * KnownAssignedXids.
 *
 * We don't worry about updating other counters, we want to keep this as
 * simple as possible and leave GetSnapshotData() as the primary code for
 * that bookkeeping.
 * 
 * POLAR
 * We could just use return polar_shmem_csn_mvcc_var_cache->polar_oldest_active_xid. 
 */
TransactionId
GetOldestActiveTransactionId(void)
{
	ProcArrayStruct *arrayP = procArray;
	TransactionId oldestRunningXid;
	int			index;

	Assert(!RecoveryInProgress());

	if (polar_csn_enable)
		return pg_atomic_read_u32(&polar_shmem_csn_mvcc_var_cache->polar_oldest_active_xid);

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
		volatile PGXACT *pgxact = &allPgXact[pgprocno];
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

	if (polar_csn_enable)
	{
		/* 
		* CommitTransaction hold share lock 
		* Ensure no xids commit csn and set csn in ProcArray
		*/
		LWLockAcquire(CommitSeqNoLock, LW_EXCLUSIVE);
	}

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
			volatile PGXACT *pgxact = &allPgXact[pgprocno];
			TransactionId xid;

			/* Fetch xid just once - see GetNewTransactionId */
			xid = pgxact->xid;

			if (!TransactionIdIsNormal(xid))
				continue;

			/* POLAR csn */
			if (polar_csn_enable)
			{
				if (pgxact->polar_csn != InvalidCommitSeqNo)
				{
					/* Committed xact can not be active */
					continue;
				}
			}

			if (TransactionIdPrecedes(xid, oldestSafeXid))
				oldestSafeXid = xid;
		}
	}

	if (polar_csn_enable)
	{
		LWLockRelease(CommitSeqNoLock);
	}

	LWLockRelease(XidGenLock);

	if (polar_csn_enable)
	{
		TransactionId oldest_active_xid;
		oldest_active_xid = pg_atomic_read_u32(&polar_shmem_csn_mvcc_var_cache->polar_oldest_active_xid);
		if (TransactionIdIsValid(oldest_active_xid) &&
				TransactionIdPrecedes(oldest_active_xid, oldestSafeXid))
			oldestSafeXid = oldest_active_xid;
	}

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
		volatile PGXACT *pgxact = &allPgXact[pgprocno];

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
		volatile PGXACT *pgxact = &allPgXact[pgprocno];
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
		volatile PGXACT *pgxact = &allPgXact[pgprocno];

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
		volatile PGXACT *pgxact = &allPgXact[pgprocno];

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
 * numbered KnownAssignedXid that is not already a FATAL error. This is
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
		volatile PGXACT *pgxact = &allPgXact[pgprocno];

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
		volatile PGXACT *pgxact = &allPgXact[pgprocno];

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
		if (proc->isBackgroundWorker ||proc->isPolarDispatcher /* POLAR: Shared Server */)
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
		if (proc->isBackgroundWorker || proc->isPolarDispatcher)
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
	int			polar_shared_backend_pids[MAXAUTOVACPIDS];
	int			tries;

	/* 50 tries with 100ms sleep between tries makes 5 sec total wait */
	for (tries = 0; tries < 50; tries++)
	{
		int			nautovacs = 0;
		int			n_shared_backends = 0;
		bool		found = false;
		int			index;

		CHECK_FOR_INTERRUPTS();

		*nbackends = *nprepared = 0;

		LWLockAcquire(ProcArrayLock, LW_SHARED);

		for (index = 0; index < arrayP->numProcs; index++)
		{
			int			pgprocno = arrayP->pgprocnos[index];
			volatile PGPROC *proc = &allProcs[pgprocno];
			volatile PGXACT *pgxact = &allPgXact[pgprocno];

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
				if (POLAR_SHARED_SERVER_RUNNING() &&
					proc->polar_shared_session == NULL &&
					!proc->polar_is_backend_dedicated &&
					n_shared_backends < MAXAUTOVACPIDS)
					polar_shared_backend_pids[n_shared_backends++] = proc->pid;

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

		for (index = 0; index < n_shared_backends; index++)
			(void) kill(polar_shared_backend_pids[index], SIGTERM);	/* ignore any error */

		/* sleep, then try again */
		pg_usleep(100 * 1000L); /* 100ms */
	}

	return true;				/* timed out, still conflicts */
}

/* POLAR: Shared Server */
bool
CountOtherUserBackends(Oid roleId, int *nbackends)
{
	ProcArrayStruct *arrayP = procArray;

	int			polar_shared_backend_pids[MAXAUTOVACPIDS];
	int			tries;

	if (!POLAR_SHARED_SERVER_RUNNING())
		return false;

	/* 50 tries with 100ms sleep between tries makes 5 sec total wait */
	for (tries = 0; tries < 50; tries++)
	{
		int			n_shared_backends = 0;
		bool		found = false;
		int			index;

		CHECK_FOR_INTERRUPTS();

		*nbackends = 0;

		LWLockAcquire(ProcArrayLock, LW_SHARED);

		for (index = 0; index < arrayP->numProcs; index++)
		{
			volatile PGPROC *proc = &allProcs[arrayP->pgprocnos[index]];

			if (proc->roleId != roleId || proc->pid == 0)
				continue;
			if (proc == MyProc)
				continue;

			found = true;

			(*nbackends)++;
			if (proc->polar_shared_session == NULL &&
				!proc->polar_is_backend_dedicated &&
				n_shared_backends < MAXAUTOVACPIDS)
				polar_shared_backend_pids[n_shared_backends++] = proc->pid;
		}

		LWLockRelease(ProcArrayLock);

		if (!found)
			return false;		/* no conflicting backends, so done */

		for (index = 0; index < n_shared_backends; index++)
			(void) kill(polar_shared_backend_pids[index], SIGTERM);	/* ignore any error */

		/* sleep, then try again */
		pg_usleep(100 * 1000L); /* 100ms */
	}

	return true;				/* timed out, still conflicts */
}

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


#define XidCacheRemove(i) \
	do { \
		MyProc->subxids.xids[i] = MyProc->subxids.xids[MyPgXact->nxids - 1]; \
		MyPgXact->nxids--; \
	} while (0)

/*
 * XidCacheRemoveRunningXids
 *
 * Remove a bunch of TransactionIds from the list of known-running
 * subtransactions for my backend.  Both the specified xid and those in
 * the xids[] array (of length nxids) are removed from the subxids cache.
 * latestXid must be the latest XID among the group.
 */
void
XidCacheRemoveRunningXids(TransactionId xid,
						  int nxids, const TransactionId *xids,
						  TransactionId latestXid)
{
	int			i,
				j;

	Assert(TransactionIdIsValid(xid));

	/*
	 * We must hold ProcArrayLock exclusively in order to remove transactions
	 * from the PGPROC array.  (See src/backend/access/transam/README.)  It's
	 * possible this could be relaxed since we know this routine is only used
	 * to abort subtransactions, but pending closer analysis we'd best be
	 * conservative.
	 */
	LWLockAcquire(ProcArrayLock, LW_EXCLUSIVE);

	/*
	 * Under normal circumstances xid and xids[] will be in increasing order,
	 * as will be the entries in subxids.  Scan backwards to avoid O(N^2)
	 * behavior when removing a lot of xids.
	 */
	for (i = nxids - 1; i >= 0; i--)
	{
		TransactionId anxid = xids[i];

		for (j = MyPgXact->nxids - 1; j >= 0; j--)
		{
			if (TransactionIdEquals(MyProc->subxids.xids[j], anxid))
			{
				XidCacheRemove(j);
				break;
			}
		}

		/*
		 * Ordinarily we should have found it, unless the cache has
		 * overflowed. However it's also possible for this routine to be
		 * invoked multiple times for the same subtransaction, in case of an
		 * error during AbortSubTransaction.  So instead of Assert, emit a
		 * debug warning.
		 */
		if (j < 0 && !MyPgXact->overflowed)
			elog(WARNING, "did not find subXID %u in MyProc", anxid);
	}

	for (j = MyPgXact->nxids - 1; j >= 0; j--)
	{
		if (TransactionIdEquals(MyProc->subxids.xids[j], xid))
		{
			XidCacheRemove(j);
			break;
		}
	}
	/* Ordinarily we should have found it, unless the cache has overflowed */
	if (j < 0 && !MyPgXact->overflowed)
		elog(WARNING, "did not find subXID %u in MyProc", xid);

	/* Also advance global latestCompletedXid while holding the lock */
	if (TransactionIdPrecedes(ShmemVariableCache->latestCompletedXid,
							  latestXid))
		ShmemVariableCache->latestCompletedXid = latestXid;

	LWLockRelease(ProcArrayLock);
}

#ifdef XIDCACHE_DEBUG

/*
 * Print stats about effectiveness of XID cache
 */
static void
DisplayXidCache(void)
{
	fprintf(stderr,
			"XidCache: xmin: %ld, known: %ld, myxact: %ld, latest: %ld, mainxid: %ld, childxid: %ld, knownassigned: %ld, nooflo: %ld, slow: %ld\n",
			xc_by_recent_xmin,
			xc_by_known_xact,
			xc_by_my_xact,
			xc_by_latest_xid,
			xc_by_main_xid,
			xc_by_child_xid,
			xc_by_known_assigned,
			xc_no_overflow,
			xc_slow_answer);
}
#endif							/* XIDCACHE_DEBUG */


/* ----------------------------------------------
 *		KnownAssignedTransactions sub-module
 * ----------------------------------------------
 */

/*
 * In Hot Standby mode, we maintain a list of transactions that are (or were)
 * running in the master at the current point in WAL.  These XIDs must be
 * treated as running by standby transactions, even though they are not in
 * the standby server's PGXACT array.
 *
 * We record all XIDs that we know have been assigned.  That includes all the
 * XIDs seen in WAL records, plus all unobserved XIDs that we can deduce have
 * been assigned.  We can deduce the existence of unobserved XIDs because we
 * know XIDs are assigned in sequence, with no gaps.  The KnownAssignedXids
 * list expands as new XIDs are observed or inferred, and contracts when
 * transaction completion records arrive.
 *
 * During hot standby we do not fret too much about the distinction between
 * top-level XIDs and subtransaction XIDs. We store both together in the
 * KnownAssignedXids list.  In backends, this is copied into snapshots in
 * GetSnapshotData(), taking advantage of the fact that XidInMVCCSnapshot()
 * doesn't care about the distinction either.  Subtransaction XIDs are
 * effectively treated as top-level XIDs and in the typical case pg_subtrans
 * links are *not* maintained (which does not affect visibility).
 *
 * We have room in KnownAssignedXids and in snapshots to hold maxProcs *
 * (1 + PGPROC_MAX_CACHED_SUBXIDS) XIDs, so every master transaction must
 * report its subtransaction XIDs in a WAL XLOG_XACT_ASSIGNMENT record at
 * least every PGPROC_MAX_CACHED_SUBXIDS.  When we receive one of these
 * records, we mark the subXIDs as children of the top XID in pg_subtrans,
 * and then remove them from KnownAssignedXids.  This prevents overflow of
 * KnownAssignedXids and snapshots, at the cost that status checks for these
 * subXIDs will take a slower path through TransactionIdIsInProgress().
 * This means that KnownAssignedXids is not necessarily complete for subXIDs,
 * though it should be complete for top-level XIDs; this is the same situation
 * that holds with respect to the PGPROC entries in normal running.
 *
 * When we throw away subXIDs from KnownAssignedXids, we need to keep track of
 * that, similarly to tracking overflow of a PGPROC's subxids array.  We do
 * that by remembering the lastOverflowedXID, ie the last thrown-away subXID.
 * As long as that is within the range of interesting XIDs, we have to assume
 * that subXIDs are missing from snapshots.  (Note that subXID overflow occurs
 * on primary when 65th subXID arrives, whereas on standby it occurs when 64th
 * subXID arrives - that is not an error.)
 *
 * Should a backend on primary somehow disappear before it can write an abort
 * record, then we just leave those XIDs in KnownAssignedXids. They actually
 * aborted but we think they were running; the distinction is irrelevant
 * because either way any changes done by the transaction are not visible to
 * backends in the standby.  We prune KnownAssignedXids when
 * XLOG_RUNNING_XACTS arrives, to forestall possible overflow of the
 * array due to such dead XIDs.
 */

/*
 * RecordKnownAssignedTransactionIds
 *		Record the given XID in KnownAssignedXids, as well as any preceding
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
		 * Extend subtrans like we do in GetNewTransactionId() during normal
		 * operation using individual extend steps. Note that we do not need
		 * to extend clog since its extensions are WAL logged.
		 *
		 * This part has to be done regardless of standbyState since we
		 * immediately start assigning subtransactions to their toplevel
		 * transactions.
		 * 
		 * POLAR
		 * In polar csn mode, we should extend csnlog instead of subtrans
		 */
		next_expected_xid = latestObservedXid;
		while (TransactionIdPrecedes(next_expected_xid, xid))
		{
			TransactionIdAdvance(next_expected_xid);
			/* 
			 * POLAR 
			 * we maintain subtrans info in csnlog
			 */
			/*no cover begin*/
			/* 
		 	 * In polar csn, we need not extend csnlog here,
		 	 * because csnlog write zero page wal log like clog
		 	 */
			if (!polar_csn_enable)
				ExtendSUBTRANS(next_expected_xid);
			/*no cover end*/
		}
		Assert(next_expected_xid == xid);

		/*
		* If the KnownAssignedXids machinery isn't up yet, there's nothing
		* more to do since we don't track assigned xids yet.
		*/
		if (standbyState <= STANDBY_INITIALIZED)
		{
			latestObservedXid = xid;
			return;
		}

		/*
		* Add (latestObservedXid, xid] onto the KnownAssignedXids array.
		*/
		next_expected_xid = latestObservedXid;
		TransactionIdAdvance(next_expected_xid);
		KnownAssignedXidsAdd(next_expected_xid, xid, false);

		/*
		 * Now we can advance latestObservedXid
		 */
		latestObservedXid = xid;

		/* ShmemVariableCache->nextXid must be beyond any observed xid */
		next_expected_xid = latestObservedXid;
		TransactionIdAdvance(next_expected_xid);
		LWLockAcquire(XidGenLock, LW_EXCLUSIVE);
		if (TransactionIdFollows(next_expected_xid, ShmemVariableCache->nextXid))
			ShmemVariableCache->nextXid = next_expected_xid;
		LWLockRelease(XidGenLock);
	}
}

/*
 * ExpireTreeKnownAssignedTransactionIds
 *		Remove the given XIDs from KnownAssignedXids.
 *
 * Called during recovery in analogy with and in place of ProcArrayEndTransaction()
 */
void
ExpireTreeKnownAssignedTransactionIds(TransactionId xid, int nsubxids,
									  TransactionId *subxids, TransactionId max_xid)
{
	Assert(standbyState >= STANDBY_INITIALIZED);

	/*
	 * Uses same locking as transaction commit
	 */
	LWLockAcquire(ProcArrayLock, LW_EXCLUSIVE);

	KnownAssignedXidsRemoveTree(xid, nsubxids, subxids);

	/* As in ProcArrayEndTransaction, advance latestCompletedXid */
	if (TransactionIdPrecedes(ShmemVariableCache->latestCompletedXid,
							  max_xid))
		ShmemVariableCache->latestCompletedXid = max_xid;

	if (polar_in_replica_mode() && polar_replica_multi_version_snapshot_enable)
		polar_replica_multi_version_snapshot_set_snapshot();

	LWLockRelease(ProcArrayLock);

	if (polar_csn_enable)
	{
		/* If we were the oldest active XID, advance oldestXid */
		AdvanceOldestActiveXidCSN(xid);
	}
}

/*
 * ExpireAllKnownAssignedTransactionIds
 *		Remove all entries in KnownAssignedXids
 */
void
ExpireAllKnownAssignedTransactionIds(void)
{
	LWLockAcquire(ProcArrayLock, LW_EXCLUSIVE);
	KnownAssignedXidsRemovePreceding(InvalidTransactionId);
	LWLockRelease(ProcArrayLock);
}

/*
 * ExpireOldKnownAssignedTransactionIds
 *		Remove KnownAssignedXids entries preceding the given XID
 */
void
ExpireOldKnownAssignedTransactionIds(TransactionId xid)
{
	LWLockAcquire(ProcArrayLock, LW_EXCLUSIVE);
	KnownAssignedXidsRemovePreceding(xid);
	LWLockRelease(ProcArrayLock);

	/* advance oldestXid */
	if (polar_csn_enable && 
		TransactionIdFollows(xid, pg_atomic_read_u32(&polar_shmem_csn_mvcc_var_cache->polar_oldest_active_xid)))
	{
		pg_atomic_write_u32(&polar_shmem_csn_mvcc_var_cache->polar_oldest_active_xid, xid);
	}
}


/*
 * Private module functions to manipulate KnownAssignedXids
 *
 * There are 5 main uses of the KnownAssignedXids data structure:
 *
 *	* backends taking snapshots - all valid XIDs need to be copied out
 *	* backends seeking to determine presence of a specific XID
 *	* startup process adding new known-assigned XIDs
 *	* startup process removing specific XIDs as transactions end
 *	* startup process pruning array when special WAL records arrive
 *
 * This data structure is known to be a hot spot during Hot Standby, so we
 * go to some lengths to make these operations as efficient and as concurrent
 * as possible.
 *
 * The XIDs are stored in an array in sorted order --- TransactionIdPrecedes
 * order, to be exact --- to allow binary search for specific XIDs.  Note:
 * in general TransactionIdPrecedes would not provide a total order, but
 * we know that the entries present at any instant should not extend across
 * a large enough fraction of XID space to wrap around (the master would
 * shut down for fear of XID wrap long before that happens).  So it's OK to
 * use TransactionIdPrecedes as a binary-search comparator.
 *
 * It's cheap to maintain the sortedness during insertions, since new known
 * XIDs are always reported in XID order; we just append them at the right.
 *
 * To keep individual deletions cheap, we need to allow gaps in the array.
 * This is implemented by marking array elements as valid or invalid using
 * the parallel boolean array KnownAssignedXidsValid[].  A deletion is done
 * by setting KnownAssignedXidsValid[i] to false, *without* clearing the
 * XID entry itself.  This preserves the property that the XID entries are
 * sorted, so we can do binary searches easily.  Periodically we compress
 * out the unused entries; that's much cheaper than having to compress the
 * array immediately on every deletion.
 *
 * The actually valid items in KnownAssignedXids[] and KnownAssignedXidsValid[]
 * are those with indexes tail <= i < head; items outside this subscript range
 * have unspecified contents.  When head reaches the end of the array, we
 * force compression of unused entries rather than wrapping around, since
 * allowing wraparound would greatly complicate the search logic.  We maintain
 * an explicit tail pointer so that pruning of old XIDs can be done without
 * immediately moving the array contents.  In most cases only a small fraction
 * of the array contains valid entries at any instant.
 *
 * Although only the startup process can ever change the KnownAssignedXids
 * data structure, we still need interlocking so that standby backends will
 * not observe invalid intermediate states.  The convention is that backends
 * must hold shared ProcArrayLock to examine the array.  To remove XIDs from
 * the array, the startup process must hold ProcArrayLock exclusively, for
 * the usual transactional reasons (compare commit/abort of a transaction
 * during normal running).  Compressing unused entries out of the array
 * likewise requires exclusive lock.  To add XIDs to the array, we just insert
 * them into slots to the right of the head pointer and then advance the head
 * pointer.  This wouldn't require any lock at all, except that on machines
 * with weak memory ordering we need to be careful that other processors
 * see the array element changes before they see the head pointer change.
 * We handle this by using a spinlock to protect reads and writes of the
 * head/tail pointers.  (We could dispense with the spinlock if we were to
 * create suitable memory access barrier primitives and use those instead.)
 * The spinlock must be taken to read or write the head/tail pointers unless
 * the caller holds ProcArrayLock exclusively.
 *
 * Algorithmic analysis:
 *
 * If we have a maximum of M slots, with N XIDs currently spread across
 * S elements then we have N <= S <= M always.
 *
 *	* Adding a new XID is O(1) and needs little locking (unless compression
 *		must happen)
 *	* Compressing the array is O(S) and requires exclusive lock
 *	* Removing an XID is O(logS) and requires exclusive lock
 *	* Taking a snapshot is O(S) and requires shared lock
 *	* Checking for an XID is O(logS) and requires shared lock
 *
 * In comparison, using a hash table for KnownAssignedXids would mean that
 * taking snapshots would be O(M). If we can maintain S << M then the
 * sorted array technique will deliver significantly faster snapshots.
 * If we try to keep S too small then we will spend too much time compressing,
 * so there is an optimal point for any workload mix. We use a heuristic to
 * decide when to compress the array, though trimming also helps reduce
 * frequency of compressing. The heuristic requires us to track the number of
 * currently valid XIDs in the array.
 */


/*
 * Compress KnownAssignedXids by shifting valid data down to the start of the
 * array, removing any gaps.
 *
 * A compression step is forced if "force" is true, otherwise we do it
 * only if a heuristic indicates it's a good time to do it.
 *
 * Caller must hold ProcArrayLock in exclusive mode.
 */
static void
KnownAssignedXidsCompress(bool force)
{
	/* use volatile pointer to prevent code rearrangement */
	volatile ProcArrayStruct *pArray = procArray;
	int			head,
				tail;
	int			compress_index;
	int			i;

	/* no spinlock required since we hold ProcArrayLock exclusively */
	head = pArray->headKnownAssignedXids;
	tail = pArray->tailKnownAssignedXids;

	if (!force)
	{
		/*
		 * If we can choose how much to compress, use a heuristic to avoid
		 * compressing too often or not often enough.
		 *
		 * Heuristic is if we have a large enough current spread and less than
		 * 50% of the elements are currently in use, then compress. This
		 * should ensure we compress fairly infrequently. We could compress
		 * less often though the virtual array would spread out more and
		 * snapshots would become more expensive.
		 */
		int			nelements = head - tail;

		if (nelements < 4 * PROCARRAY_MAXPROCS ||
			nelements < 2 * pArray->numKnownAssignedXids)
			return;
	}

	/*
	 * We compress the array by reading the valid values from tail to head,
	 * re-aligning data to 0th element.
	 */
	compress_index = 0;
	for (i = tail; i < head; i++)
	{
		if (KnownAssignedXidsValid[i])
		{
			KnownAssignedXids[compress_index] = KnownAssignedXids[i];
			KnownAssignedXidsValid[compress_index] = true;
			compress_index++;
		}
	}

	pArray->tailKnownAssignedXids = 0;
	pArray->headKnownAssignedXids = compress_index;
}

/*
 * Add xids into KnownAssignedXids at the head of the array.
 *
 * xids from from_xid to to_xid, inclusive, are added to the array.
 *
 * If exclusive_lock is true then caller already holds ProcArrayLock in
 * exclusive mode, so we need no extra locking here.  Else caller holds no
 * lock, so we need to be sure we maintain sufficient interlocks against
 * concurrent readers.  (Only the startup process ever calls this, so no need
 * to worry about concurrent writers.)
 */
static void
KnownAssignedXidsAdd(TransactionId from_xid, TransactionId to_xid,
					 bool exclusive_lock)
{
	/* use volatile pointer to prevent code rearrangement */
	volatile ProcArrayStruct *pArray = procArray;
	TransactionId next_xid;
	int			head,
				tail;
	int			nxids;
	int			i;

	Assert(TransactionIdPrecedesOrEquals(from_xid, to_xid));

	/*
	 * Calculate how many array slots we'll need.  Normally this is cheap; in
	 * the unusual case where the XIDs cross the wrap point, we do it the hard
	 * way.
	 */
	if (to_xid >= from_xid)
		nxids = to_xid - from_xid + 1;
	else
	{
		nxids = 1;
		next_xid = from_xid;
		while (TransactionIdPrecedes(next_xid, to_xid))
		{
			nxids++;
			TransactionIdAdvance(next_xid);
		}
	}

	/*
	 * Since only the startup process modifies the head/tail pointers, we
	 * don't need a lock to read them here.
	 */
	head = pArray->headKnownAssignedXids;
	tail = pArray->tailKnownAssignedXids;

	Assert(head >= 0 && head <= pArray->maxKnownAssignedXids);
	Assert(tail >= 0 && tail < pArray->maxKnownAssignedXids);

	/*
	 * Verify that insertions occur in TransactionId sequence.  Note that even
	 * if the last existing element is marked invalid, it must still have a
	 * correctly sequenced XID value.
	 */
	if (head > tail &&
		TransactionIdFollowsOrEquals(KnownAssignedXids[head - 1], from_xid))
	{
		KnownAssignedXidsDisplay(LOG);
		elog(ERROR, "out-of-order XID insertion in KnownAssignedXids");
	}

	/*
	 * If our xids won't fit in the remaining space, compress out free space
	 */
	if (head + nxids > pArray->maxKnownAssignedXids)
	{
		/* must hold lock to compress */
		if (!exclusive_lock)
			LWLockAcquire(ProcArrayLock, LW_EXCLUSIVE);

		KnownAssignedXidsCompress(true);

		head = pArray->headKnownAssignedXids;
		/* note: we no longer care about the tail pointer */

		if (!exclusive_lock)
			LWLockRelease(ProcArrayLock);

		/*
		 * If it still won't fit then we're out of memory
		 */
		if (head + nxids > pArray->maxKnownAssignedXids)
			elog(ERROR, "too many KnownAssignedXids");
	}

	/* Now we can insert the xids into the space starting at head */
	next_xid = from_xid;
	for (i = 0; i < nxids; i++)
	{
		KnownAssignedXids[head] = next_xid;
		KnownAssignedXidsValid[head] = true;
		TransactionIdAdvance(next_xid);
		head++;
	}

	/* Adjust count of number of valid entries */
	pArray->numKnownAssignedXids += nxids;

	/*
	 * Now update the head pointer.  We use a spinlock to protect this
	 * pointer, not because the update is likely to be non-atomic, but to
	 * ensure that other processors see the above array updates before they
	 * see the head pointer change.
	 *
	 * If we're holding ProcArrayLock exclusively, there's no need to take the
	 * spinlock.
	 */
	if (exclusive_lock)
		pArray->headKnownAssignedXids = head;
	else
	{
		SpinLockAcquire(&pArray->known_assigned_xids_lck);
		pArray->headKnownAssignedXids = head;
		SpinLockRelease(&pArray->known_assigned_xids_lck);
	}
}

/*
 * KnownAssignedXidsSearch
 *
 * Searches KnownAssignedXids for a specific xid and optionally removes it.
 * Returns true if it was found, false if not.
 *
 * Caller must hold ProcArrayLock in shared or exclusive mode.
 * Exclusive lock must be held for remove = true.
 */
static bool
KnownAssignedXidsSearch(TransactionId xid, bool remove)
{
	/* use volatile pointer to prevent code rearrangement */
	volatile ProcArrayStruct *pArray = procArray;
	int			first,
				last;
	int			head;
	int			tail;
	int			result_index = -1;

	if (remove)
	{
		/* we hold ProcArrayLock exclusively, so no need for spinlock */
		tail = pArray->tailKnownAssignedXids;
		head = pArray->headKnownAssignedXids;
	}
	else
	{
		/* take spinlock to ensure we see up-to-date array contents */
		SpinLockAcquire(&pArray->known_assigned_xids_lck);
		tail = pArray->tailKnownAssignedXids;
		head = pArray->headKnownAssignedXids;
		SpinLockRelease(&pArray->known_assigned_xids_lck);
	}

	/*
	 * Standard binary search.  Note we can ignore the KnownAssignedXidsValid
	 * array here, since even invalid entries will contain sorted XIDs.
	 */
	first = tail;
	last = head - 1;
	while (first <= last)
	{
		int			mid_index;
		TransactionId mid_xid;

		mid_index = (first + last) / 2;
		mid_xid = KnownAssignedXids[mid_index];

		if (xid == mid_xid)
		{
			result_index = mid_index;
			break;
		}
		else if (TransactionIdPrecedes(xid, mid_xid))
			last = mid_index - 1;
		else
			first = mid_index + 1;
	}

	if (result_index < 0)
		return false;			/* not in array */

	if (!KnownAssignedXidsValid[result_index])
		return false;			/* in array, but invalid */

	if (remove)
	{
		KnownAssignedXidsValid[result_index] = false;

		pArray->numKnownAssignedXids--;
		Assert(pArray->numKnownAssignedXids >= 0);

		/*
		 * If we're removing the tail element then advance tail pointer over
		 * any invalid elements.  This will speed future searches.
		 */
		if (result_index == tail)
		{
			tail++;
			while (tail < head && !KnownAssignedXidsValid[tail])
				tail++;
			if (tail >= head)
			{
				/* Array is empty, so we can reset both pointers */
				pArray->headKnownAssignedXids = 0;
				pArray->tailKnownAssignedXids = 0;
			}
			else
			{
				pArray->tailKnownAssignedXids = tail;
			}
		}
	}

	return true;
}

/*
 * Is the specified XID present in KnownAssignedXids[]?
 *
 * Caller must hold ProcArrayLock in shared or exclusive mode.
 */
static bool
KnownAssignedXidExists(TransactionId xid)
{
	Assert(TransactionIdIsValid(xid));

	return KnownAssignedXidsSearch(xid, false);
}

/*
 * Remove the specified XID from KnownAssignedXids[].
 *
 * Caller must hold ProcArrayLock in exclusive mode.
 */
static void
KnownAssignedXidsRemove(TransactionId xid)
{
	Assert(TransactionIdIsValid(xid));

	elog(trace_recovery(DEBUG4), "remove KnownAssignedXid %u", xid);

	/*
	 * Note: we cannot consider it an error to remove an XID that's not
	 * present.  We intentionally remove subxact IDs while processing
	 * XLOG_XACT_ASSIGNMENT, to avoid array overflow.  Then those XIDs will be
	 * removed again when the top-level xact commits or aborts.
	 *
	 * It might be possible to track such XIDs to distinguish this case from
	 * actual errors, but it would be complicated and probably not worth it.
	 * So, just ignore the search result.
	 */
	(void) KnownAssignedXidsSearch(xid, true);
}

/*
 * KnownAssignedXidsRemoveTree
 *		Remove xid (if it's not InvalidTransactionId) and all the subxids.
 *
 * Caller must hold ProcArrayLock in exclusive mode.
 */
static void
KnownAssignedXidsRemoveTree(TransactionId xid, int nsubxids,
							TransactionId *subxids)
{
	int			i;

	if (TransactionIdIsValid(xid))
		KnownAssignedXidsRemove(xid);

	for (i = 0; i < nsubxids; i++)
		KnownAssignedXidsRemove(subxids[i]);

	/* Opportunistically compress the array */
	KnownAssignedXidsCompress(false);
}

/*
 * Prune KnownAssignedXids up to, but *not* including xid. If xid is invalid
 * then clear the whole table.
 *
 * Caller must hold ProcArrayLock in exclusive mode.
 */
static void
KnownAssignedXidsRemovePreceding(TransactionId removeXid)
{
	/* use volatile pointer to prevent code rearrangement */
	volatile ProcArrayStruct *pArray = procArray;
	int			count = 0;
	int			head,
				tail,
				i;

	if (!TransactionIdIsValid(removeXid))
	{
		elog(trace_recovery(DEBUG4), "removing all KnownAssignedXids");
		pArray->numKnownAssignedXids = 0;
		pArray->headKnownAssignedXids = pArray->tailKnownAssignedXids = 0;
		return;
	}

	elog(trace_recovery(DEBUG4), "prune KnownAssignedXids to %u", removeXid);

	/*
	 * Mark entries invalid starting at the tail.  Since array is sorted, we
	 * can stop as soon as we reach an entry >= removeXid.
	 */
	tail = pArray->tailKnownAssignedXids;
	head = pArray->headKnownAssignedXids;

	for (i = tail; i < head; i++)
	{
		if (KnownAssignedXidsValid[i])
		{
			TransactionId knownXid = KnownAssignedXids[i];

			if (TransactionIdFollowsOrEquals(knownXid, removeXid))
				break;

			if (!StandbyTransactionIdIsPrepared(knownXid))
			{
				KnownAssignedXidsValid[i] = false;
				count++;
			}
		}
	}

	pArray->numKnownAssignedXids -= count;
	Assert(pArray->numKnownAssignedXids >= 0);

	/*
	 * Advance the tail pointer if we've marked the tail item invalid.
	 */
	for (i = tail; i < head; i++)
	{
		if (KnownAssignedXidsValid[i])
			break;
	}
	if (i >= head)
	{
		/* Array is empty, so we can reset both pointers */
		pArray->headKnownAssignedXids = 0;
		pArray->tailKnownAssignedXids = 0;
	}
	else
	{
		pArray->tailKnownAssignedXids = i;
	}

	/* Opportunistically compress the array */
	KnownAssignedXidsCompress(false);
}

/*
 * KnownAssignedXidsGet - Get an array of xids by scanning KnownAssignedXids.
 * We filter out anything >= xmax.
 *
 * Returns the number of XIDs stored into xarray[].  Caller is responsible
 * that array is large enough.
 *
 * Caller must hold ProcArrayLock in (at least) shared mode.
 */
static int
KnownAssignedXidsGet(TransactionId *xarray, TransactionId xmax)
{
	TransactionId xtmp = InvalidTransactionId;

	return KnownAssignedXidsGetAndSetXmin(xarray, &xtmp, xmax);
}

/*
 * KnownAssignedXidsGetAndSetXmin - as KnownAssignedXidsGet, plus
 * we reduce *xmin to the lowest xid value seen if not already lower.
 *
 * Caller must hold ProcArrayLock in (at least) shared mode.
 */
static int
KnownAssignedXidsGetAndSetXmin(TransactionId *xarray, TransactionId *xmin,
							   TransactionId xmax)
{
	int			count = 0;
	int			head,
				tail;
	int			i;

	/*
	 * Fetch head just once, since it may change while we loop. We can stop
	 * once we reach the initially seen head, since we are certain that an xid
	 * cannot enter and then leave the array while we hold ProcArrayLock.  We
	 * might miss newly-added xids, but they should be >= xmax so irrelevant
	 * anyway.
	 *
	 * Must take spinlock to ensure we see up-to-date array contents.
	 */
	SpinLockAcquire(&procArray->known_assigned_xids_lck);
	tail = procArray->tailKnownAssignedXids;
	head = procArray->headKnownAssignedXids;
	SpinLockRelease(&procArray->known_assigned_xids_lck);

	for (i = tail; i < head; i++)
	{
		/* Skip any gaps in the array */
		if (KnownAssignedXidsValid[i])
		{
			TransactionId knownXid = KnownAssignedXids[i];

			/*
			 * Update xmin if required.  Only the first XID need be checked,
			 * since the array is sorted.
			 */
			if (count == 0 &&
				TransactionIdPrecedes(knownXid, *xmin))
				*xmin = knownXid;

			/*
			 * Filter out anything >= xmax, again relying on sorted property
			 * of array.
			 */
			if (TransactionIdIsValid(xmax) &&
				TransactionIdFollowsOrEquals(knownXid, xmax))
				break;

			/* Add knownXid into output array */
			xarray[count++] = knownXid;
		}
	}

	return count;
}

/*
 * Get oldest XID in the KnownAssignedXids array, or InvalidTransactionId
 * if nothing there.
 */
static TransactionId
KnownAssignedXidsGetOldestXmin(void)
{
	int			head,
				tail;
	int			i;

	/*
	 * Fetch head just once, since it may change while we loop.
	 */
	SpinLockAcquire(&procArray->known_assigned_xids_lck);
	tail = procArray->tailKnownAssignedXids;
	head = procArray->headKnownAssignedXids;
	SpinLockRelease(&procArray->known_assigned_xids_lck);

	for (i = tail; i < head; i++)
	{
		/* Skip any gaps in the array */
		if (KnownAssignedXidsValid[i])
			return KnownAssignedXids[i];
	}

	return InvalidTransactionId;
}

/*
 * Display KnownAssignedXids to provide debug trail
 *
 * Currently this is only called within startup process, so we need no
 * special locking.
 *
 * Note this is pretty expensive, and much of the expense will be incurred
 * even if the elog message will get discarded.  It's not currently called
 * in any performance-critical places, however, so no need to be tenser.
 */
static void
KnownAssignedXidsDisplay(int trace_level)
{
	/* use volatile pointer to prevent code rearrangement */
	volatile ProcArrayStruct *pArray = procArray;
	StringInfoData buf;
	int			head,
				tail,
				i;
	int			nxids = 0;

	tail = pArray->tailKnownAssignedXids;
	head = pArray->headKnownAssignedXids;

	initStringInfo(&buf);

	for (i = tail; i < head; i++)
	{
		if (KnownAssignedXidsValid[i])
		{
			nxids++;
			appendStringInfo(&buf, "[%d]=%u ", i, KnownAssignedXids[i]);
		}
	}

	elog(trace_level, "%d KnownAssignedXids (num=%d tail=%d head=%d) %s",
		 nxids,
		 pArray->numKnownAssignedXids,
		 pArray->tailKnownAssignedXids,
		 pArray->headKnownAssignedXids,
		 buf.data);

	pfree(buf.data);
}

/*
 * KnownAssignedXidsReset
 *		Resets KnownAssignedXids to be empty
 */
static void
KnownAssignedXidsReset(void)
{
	/* use volatile pointer to prevent code rearrangement */
	volatile ProcArrayStruct *pArray = procArray;

	LWLockAcquire(ProcArrayLock, LW_EXCLUSIVE);

	pArray->numKnownAssignedXids = 0;
	pArray->tailKnownAssignedXids = 0;
	pArray->headKnownAssignedXids = 0;

	LWLockRelease(ProcArrayLock);
}

/*
 * polar_get_nosuper_and_super_conn_count ---count nosuper and super backends
 * However,nosupercount and supercount maybe is incorrect, because we can alter
 * superuser to nosuperuser, but we do not modify proc->issuper, so supercount is
 * incorrect, but we do not alter role superuser to nosuperuser or alter nosuperuser
 * to superuser in rds environment
 */
void
polar_get_nosuper_and_super_conn_count(int *nosupercount, int *supercount)
{
	ProcArrayStruct *arrayP = procArray;
	int			index;

	Assert(nosupercount != NULL && supercount != NULL);
	*nosupercount = *supercount = 0;
	LWLockAcquire(ProcArrayLock, LW_SHARED);

	for (index = 0; index < arrayP->numProcs; index++)
	{
		int			pgprocno = arrayP->pgprocnos[index];
		volatile PGPROC *proc = &allProcs[pgprocno];

		/*
		 * Do not count non-connection procs and prepared trans. See
		 * InitProcGlobal() for meaning of offset in allProcs.arrayP->pgprocnos[]
		 * is sorted by arrayP->pgprocnos inc, so we can break if arrayP->pgprocnos[index]
		 * >= MaxConnections
		 */
		if (arrayP->pgprocnos[index] >= MaxConnections)
			break;
		if (proc->issuper)
			(*supercount)++;
		else
			(*nosupercount)++;
	}

	LWLockRelease(ProcArrayLock);
}

/*
 * POLAR: Return the minimum lsn that backends process are replaying
 */
XLogRecPtr
polar_get_backend_min_replay_lsn(void)
{
	int i;
	XLogRecPtr result = InvalidXLogRecPtr;
	ProcArrayStruct *arrayP = procArray;

	LWLockAcquire(ProcArrayLock, LW_SHARED);
	for (i = 0; i < arrayP->numProcs; i++)
	{
		int			pgprocno = arrayP->pgprocnos[i];
		volatile PGPROC *proc = &allProcs[pgprocno];
		XLogRecPtr		read_min_lsn = InvalidXLogRecPtr;

		if (proc->pid == 0)
			continue;			/* do not count prepared xacts */

		read_min_lsn = (XLogRecPtr) pg_atomic_read_u64(&(proc->polar_read_min_lsn));
		if (XLogRecPtrIsInvalid(result) || 
			(!XLogRecPtrIsInvalid(read_min_lsn) && read_min_lsn < result))
			result = read_min_lsn;
	}
	LWLockRelease(ProcArrayLock);

	return result;
}

/*
 * polar_get_min_read_lsn --- get minimum database backends polar_read_min_lsn
 */
XLogRecPtr
polar_get_read_min_lsn(XLogRecPtr primary_consist_ptr)
{
	XLogRecPtr		result = primary_consist_ptr;
	XLogRecPtr      redo_min_lsn = polar_get_backend_min_replay_lsn();
	XLogRecPtr  	last_replayed_lsn;

	if (!XLogRecPtrIsInvalid(redo_min_lsn) && redo_min_lsn < result)
		result = redo_min_lsn;

	/* This replay lsn can avoid clearing last pending parsing redo log */
	last_replayed_lsn = GetXLogReplayRecPtr(NULL);

	/* Note: this can happen when last some logs don't be involved buffers */
	if (!XLogRecPtrIsInvalid(result) &&
			!XLogRecPtrIsInvalid(last_replayed_lsn) &&
			last_replayed_lsn < result)
		result = last_replayed_lsn;

	redo_min_lsn = polar_logindex_redo_get_min_replay_from_lsn(polar_logindex_redo_instance, primary_consist_ptr);

	if (!XLogRecPtrIsInvalid(redo_min_lsn) && redo_min_lsn < result)
		result = redo_min_lsn;

	return result;
}

/*
 * polar replica multi version snapshot store shared memory size
 * sizeof(polar_replica_multi_version_snapshot_store_t)+lwlock_array_size+
 * snapshot_array_size
 */
Size
polar_replica_multi_version_snapshot_store_shmem_size(void)
{
	Size	size;
	Size 	lwlock_array_size;
	Size 	snapshot_size;
	Size 	snapshot_array_size;
	
	if (!polar_replica_multi_version_snapshot_enable)
		return 0;

	lwlock_array_size = mul_size(sizeof(LWLockPadded), polar_replica_multi_version_snapshot_slot_num);

	/* in replica mode, active transaction list's max size is TOTAL_MAX_CACHED_SUBXIDS */
	snapshot_size = add_size(sizeof(polar_replica_multi_version_snapshot_t), 
							 mul_size(sizeof(TransactionId), GetMaxSnapshotSubxidCount()));
	snapshot_array_size = mul_size(snapshot_size, polar_replica_multi_version_snapshot_slot_num);

	size = sizeof(polar_replica_multi_version_snapshot_store_t);
	size = add_size(size, lwlock_array_size);
	size = add_size(size, snapshot_array_size);

	return size;
}

/*
 * Create or attach to the polar_replica_multi_version_snapshot_store_t structure.
 */
static void
polar_replica_multi_version_snapshot_store_shmem_init(void)
{
	bool found = false;

	polar_replica_multi_version_snapshot_store = (polar_replica_multi_version_snapshot_store_t *)
		ShmemInitStruct("PolarReplicaMultiVersionSnapshotStore",
						sizeof(polar_replica_multi_version_snapshot_store_t), &found);

	if (!found)
	{
		polar_replica_multi_version_snapshot_store_t *store = polar_replica_multi_version_snapshot_store;
		int slot_idx;

		store->slot_locks = 
			ShmemAlloc(mul_size(sizeof(LWLockPadded), polar_replica_multi_version_snapshot_slot_num));
		LWLockRegisterTranche(LWTRANCHE_POLAR_REPLICA_MULTI_VERSION_SNAPSHOT_SLOT, "polar_replica_multi_version_snapshot_slot");
		for (slot_idx = 0; slot_idx < polar_replica_multi_version_snapshot_slot_num; slot_idx++)
		{
			LWLockInitialize(&store->slot_locks[slot_idx].lock, 
								LWTRANCHE_POLAR_REPLICA_MULTI_VERSION_SNAPSHOT_SLOT);
		}

		store->slot_snapshots = 
			ShmemAlloc(mul_size(sizeof(polar_replica_multi_version_snapshot_t),
								polar_replica_multi_version_snapshot_slot_num));
		for (slot_idx = 0; slot_idx < polar_replica_multi_version_snapshot_slot_num; slot_idx++)
		{
			/* in replica mode, active transaction list's max size is TOTAL_MAX_CACHED_SUBXIDS */
			store->slot_snapshots[slot_idx].xip =
				ShmemAlloc(mul_size(sizeof(TransactionId), GetMaxSnapshotSubxidCount()));
		}

		store->slot_num = polar_replica_multi_version_snapshot_slot_num;
		store->retry_times = polar_replica_multi_version_snapshot_retry_times;
		pg_atomic_init_u32(&store->curr_slot_idx, PG_UINT32_MAX);
		store->next_slot_idx = 0;
		store->write_retried_times = 0;
		store->write_switched_times = 0;
		pg_atomic_init_u64(&store->read_retried_times, 0);
		pg_atomic_init_u64(&store->read_switched_times, 0);
	}
}

/*
 * In replica mode, Called in GetSnapshotData
 * When can not get snapshot in limited times(default is 3), return false
 * and switch back to original GetSnapshotData logic
 * ==SnapshotData related info==
 * xip								IN
 * count							OUT
 * overflowed						OUT
 * xmin								OUT
 * xmax								OUT
 * ==ProcArray related info==
 * replication_slot_xmin			OUT
 * replication_slot_catalog_xmin	OUT
 */
static bool 
polar_replica_multi_version_snapshot_get_snapshot(TransactionId *xip, int *count, bool *overflowed,
				   								  TransactionId *xmin, TransactionId *xmax, 
												  TransactionId *replication_slot_xmin, 
												  TransactionId *replication_slot_catalog_xmin)
{
	polar_replica_multi_version_snapshot_store_t *store = polar_replica_multi_version_snapshot_store;
	int retry_times = store->retry_times;
	
	/* 
	 * Each retry, we should read new curr_slot_idx from snapshot store 
	 * try best to get new snapshot
	 */
	while (retry_times-- != 0)
	{
		uint32 curr_slot_idx = pg_atomic_read_u32(&store->curr_slot_idx);

		/*
		 * For now, curr_slot_idx equals PG_UINT32_MAX has two meanings:
		 * 1. There is no item in multi version snapshot store yet, just go old way
		 * 2. Startup backend has no slot to write, force reader swiching to old way
		 * 
		 * In case 1, reader have to pay for polar_replica_multi_version_snapshot_retry_times+1 
		 * times atomic add, a little less efficiently but acceptable
		 */
		if (curr_slot_idx != PG_UINT32_MAX)
		{
			bool 			lock_succ;
			LWLockPadded	*slot_lock;
			polar_replica_multi_version_snapshot_t *slot_snapshot;

			Assert(curr_slot_idx < store->slot_num);

			slot_lock = &store->slot_locks[curr_slot_idx];
			slot_snapshot = &store->slot_snapshots[curr_slot_idx];
			lock_succ = LWLockConditionalAcquire(&slot_lock->lock, LW_SHARED);
			if (lock_succ)
			{
				memcpy(xip, slot_snapshot->xip, sizeof(TransactionId)*slot_snapshot->xcnt);
				*count = slot_snapshot->xcnt;
				*overflowed = slot_snapshot->overflowed;
				*xmin = slot_snapshot->xmin;
				*xmax = slot_snapshot->xmax;
				*replication_slot_xmin = slot_snapshot->replication_slot_xmin;
				*replication_slot_catalog_xmin = slot_snapshot->replication_slot_catalog_xmin;
				
				LWLockRelease(&slot_lock->lock);

				return true;
			}
		}
			
		/* 
		 * Share lock failed because of write lock held, it is possible though not common,
		 * just update metric and retry 
		 */
		pg_atomic_fetch_add_u64(&store->read_retried_times, 1);
	}

	/*
	 * Retry times have reached, we have to fall back to single version mode.
	 * This is not a common case buf possible, update metric and log it.
	 * For performance issue, use DEBUG1.
	 */

	pg_atomic_fetch_add_u64(&store->read_switched_times, 1);

	elog(DEBUG1, "reader has switched to single version snapshot get");

	return false;
}

/*
 *	In replica mode, called by startup backend
 *	ProcArrayLock must be hold in exclusive mode 
 */
static void 
polar_replica_multi_version_snapshot_set_snapshot(void)
{
	polar_replica_multi_version_snapshot_store_t *store = polar_replica_multi_version_snapshot_store;
	int retry_times = store->retry_times;
	
	Assert(LWLockHeldByMeInMode(ProcArrayLock, LW_EXCLUSIVE));

	while (retry_times-- != 0)
	{
		bool lock_succ;
		int next_slot_idx = store->next_slot_idx;
		LWLockPadded	*slot_lock = NULL;
		polar_replica_multi_version_snapshot_t *slot_snapshot = NULL;
		
		Assert(next_slot_idx >= 0);
		Assert(next_slot_idx < store->slot_num);

		slot_lock = &store->slot_locks[next_slot_idx];
		slot_snapshot = &store->slot_snapshots[next_slot_idx];
		
		/* 
		 *	a simple next strategy
		 *	just advance it
		 */
		store->next_slot_idx = (store->next_slot_idx+1) % store->slot_num;

		lock_succ = LWLockConditionalAcquire(&slot_lock->lock, LW_EXCLUSIVE);
		if (lock_succ)
		{
			int count = 0;
			bool overflowed = false;
			TransactionId xmin = InvalidTransactionId;
			TransactionId xmax = InvalidTransactionId;
			TransactionId replication_slot_xmin = procArray->replication_slot_xmin;
			TransactionId replication_slot_catalog_xmin = procArray->replication_slot_catalog_xmin;
			
			xmax = ShmemVariableCache->latestCompletedXid;
			Assert(TransactionIdIsNormal(xmax));
			TransactionIdAdvance(xmax);
			xmin = xmax;

			count = KnownAssignedXidsGetAndSetXmin(slot_snapshot->xip, &xmin, xmax);

			if (TransactionIdPrecedesOrEquals(xmin, procArray->lastOverflowedXid))
				overflowed = true;

			slot_snapshot->xcnt = count;
			slot_snapshot->overflowed = overflowed;
			slot_snapshot->xmin = xmin;
			slot_snapshot->xmax = xmax;
			slot_snapshot->replication_slot_xmin = replication_slot_xmin;
			slot_snapshot->replication_slot_catalog_xmin = replication_slot_catalog_xmin;
				
			LWLockRelease(&slot_lock->lock);

			pg_atomic_write_u32(&store->curr_slot_idx, next_slot_idx);

			return;
		}

		/* 
		 * Write lock failed because of share lock held, it is possible though not common,
		 * just update metric and retry.
		 * Only startup backend will update this metric, need not use atomic ops.
		 */
		store->write_retried_times++;
	} 

    /*
	 * Retry times have reached, there is no available slot for write,
     * we should let read backend go through the old way.
	 * This is not a common case, update metric and log it.
	 * For performance issue, use DEBUG1.
	 */

	pg_atomic_write_u32(&store->curr_slot_idx, PG_UINT32_MAX);
	
	store->write_switched_times++;

	elog(DEBUG1, "No available slot to make multi version snapshot");
}

/* 
 * replica multi version snapshot store get functions for dynamic view and test
 */

int
polar_replica_multi_version_snapshot_get_slot_num(void)
{
	polar_replica_multi_version_snapshot_store_t *store = polar_replica_multi_version_snapshot_store;

	if (store)
		return store->slot_num;
	else
		return 0;
}

int
polar_replica_multi_version_snapshot_get_retry_times(void)
{
	polar_replica_multi_version_snapshot_store_t *store = polar_replica_multi_version_snapshot_store;

	if (store)
		return store->retry_times;
	else 
		return 0;
}

uint32
polar_replica_multi_version_snapshot_get_curr_slot_no(void)
{
	polar_replica_multi_version_snapshot_store_t *store = polar_replica_multi_version_snapshot_store;

	if (store)
		return pg_atomic_read_u32(&store->curr_slot_idx);
	else
		return 0;
}

int
polar_replica_multi_version_snapshot_get_next_slot_no(void)
{
	polar_replica_multi_version_snapshot_store_t *store = polar_replica_multi_version_snapshot_store;

	if (store)
		return store->next_slot_idx;
	else
		return 0;
}

uint64
polar_replica_multi_version_snapshot_get_read_retried_times(void)
{
	polar_replica_multi_version_snapshot_store_t *store = polar_replica_multi_version_snapshot_store;

	if (store)
		return pg_atomic_read_u64(&store->read_retried_times);
	else
		return 0;
}

uint64
polar_replica_multi_version_snapshot_get_read_switched_times(void)
{
	polar_replica_multi_version_snapshot_store_t *store = polar_replica_multi_version_snapshot_store;

	if (store)
		return pg_atomic_read_u64(&store->read_switched_times);
	else
		return 0;
}

uint64
polar_replica_multi_version_snapshot_get_write_retried_times(void)
{
	polar_replica_multi_version_snapshot_store_t *store = polar_replica_multi_version_snapshot_store;

	if (store)
		return store->write_retried_times;
	else
		return 0;
}

uint64
polar_replica_multi_version_snapshot_get_write_switched_times(void)
{
	polar_replica_multi_version_snapshot_store_t *store = polar_replica_multi_version_snapshot_store;

	if (store)
		return store->write_switched_times;
	else 
		return 0;
}

/* POLAR test begin */

/*
 *	static func/proc wrapper
 */

void 
polar_test_replica_multi_version_snapshot_set_snapshot(void)
{
	LWLockAcquire(ProcArrayLock, LW_EXCLUSIVE);
	polar_replica_multi_version_snapshot_set_snapshot();
	LWLockRelease(ProcArrayLock);
}

bool 
polar_test_replica_multi_version_snapshot_get_snapshot(TransactionId *xip, int *count, bool *overflowed,
				   								  	TransactionId *xmin, TransactionId *xmax, 
												  	TransactionId *replication_slot_xmin, 
												  	TransactionId *replication_slot_catalog_xmin)
{
	return polar_replica_multi_version_snapshot_get_snapshot(xip, count, overflowed, xmin,  xmax, 
												  	replication_slot_xmin, 
												  	replication_slot_catalog_xmin);
}

void
polar_test_KnownAssignedXidsAdd(TransactionId from_xid, TransactionId to_xid)
{
	KnownAssignedXidsAdd(from_xid, to_xid, false);
}

void 
polar_test_KnownAssignedXidsReset(void)
{
	KnownAssignedXidsReset();
}

void
polar_test_set_curr_slot_num(int slot_num)
{
	polar_replica_multi_version_snapshot_store_t *store = polar_replica_multi_version_snapshot_store;

	pg_atomic_write_u32(&store->curr_slot_idx, slot_num);
}

void
polar_test_set_next_slot_num(int slot_num)
{
	polar_replica_multi_version_snapshot_store_t *store = polar_replica_multi_version_snapshot_store;

	store->next_slot_idx = slot_num;
}

void
polar_test_acquire_slot_lock(int slot_num, LWLockMode mode)
{
	polar_replica_multi_version_snapshot_store_t *store = polar_replica_multi_version_snapshot_store;
	LWLockPadded *slot_lock = &store->slot_locks[slot_num];

	LWLockAcquire(&slot_lock->lock, mode);
}

void
polar_test_release_slot_lock(int slot_num)
{
	polar_replica_multi_version_snapshot_store_t *store = polar_replica_multi_version_snapshot_store;
	LWLockPadded *slot_lock = &store->slot_locks[slot_num];

	LWLockRelease(&slot_lock->lock);
}

void 
polar_set_latestObservedXid(TransactionId latest_observed_xid)
{
	latestObservedXid = latest_observed_xid;
}

PGPROC *
polar_search_proc(pid_t pid)
{
	int i;
	PGPROC *proc = NULL;

	LWLockAcquire(ProcArrayLock, LW_SHARED);

	for (i = 0; i < ProcGlobal->allProcCount; i++)
	{
		if (ProcGlobal->allProcs[i].pid == pid)
		{
			proc = &ProcGlobal->allProcs[i];
			break;
		}
	}

	LWLockRelease(ProcArrayLock);

	return proc;
}

/* Used for POLAR csn */
TransactionId 
polar_get_latestObservedXid(void)
{
	return latestObservedXid;
}

/*
 * POLAR: Just get the running top transactions (without sub transactions).
 *
 * Now it is used by flashback.
 */
RunningTransactions
polar_get_running_top_trans(void)
{
	RunningTransactions trans;

	/* Ignore the sub transactions */
	trans = PolarGetRunningTransactionData(true);

	if (polar_csn_enable)
	{
		LWLockRelease(ProcArrayLock);
		LWLockRelease(XidGenLock);
		LWLockRelease(CommitSeqNoLock);
	}
	else
	{
		LWLockRelease(ProcArrayLock);
		LWLockRelease(XidGenLock);
	}
	return trans;
}

/* POLAR test end */
