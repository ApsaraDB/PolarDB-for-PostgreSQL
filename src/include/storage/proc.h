/*-------------------------------------------------------------------------
 *
 * proc.h
 *	  per-process shared memory data structures
 *
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/storage/proc.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef _PROC_H_
#define _PROC_H_

#include "access/clog.h"
#include "access/xlogdefs.h"
#include "access/polar_csn_mvcc_vars.h"
#include "lib/ilist.h"
#include "storage/latch.h"
#include "storage/lock.h"
#include "storage/pg_sema.h"
#include "storage/proclist_types.h"
#include "portability/instr_time.h"
#include "polar_dma/polar_dma.h"

/* POLAR: Shared Server */
#include "storage/polar_session_context.h"

/*
 * Each backend advertises up to PGPROC_MAX_CACHED_SUBXIDS TransactionIds
 * for non-aborted subtransactions of its current top transaction.  These
 * have to be treated as running XIDs by other backends.
 *
 * We also keep track of whether the cache overflowed (ie, the transaction has
 * generated at least one subtransaction that didn't fit in the cache).
 * If none of the caches have overflowed, we can assume that an XID that's not
 * listed anywhere in the PGPROC array is not a running transaction.  Else we
 * have to look at pg_subtrans.
 */
#define PGPROC_MAX_CACHED_SUBXIDS 64	/* XXX guessed-at value */
#define PGPROC_WAIT_STACK_LEN	4		/* the stack length for wait object and wait time */
#define PGPROC_WAIT_PID 0				/* current proc wait for pid */
#define PGPROC_WAIT_FD 1				/* current proc wait for fd */
#define PGPROC_INVAILD_WAIT_OBJ -1		/* flag of current wait obj invalid */

struct XidCache
{
	TransactionId xids[PGPROC_MAX_CACHED_SUBXIDS];
};

/*
 * Flags for PGXACT->vacuumFlags
 *
 * Note: If you modify these flags, you need to modify PROCARRAY_XXX flags
 * in src/include/storage/procarray.h.
 *
 * PROC_RESERVED may later be assigned for use in vacuumFlags, but its value is
 * used for PROCARRAY_SLOTS_XMIN in procarray.h, so GetOldestXmin won't be able
 * to match and ignore processes with this flag set.
 */
#define		PROC_IS_AUTOVACUUM	0x01	/* is it an autovac worker? */
#define		PROC_IN_VACUUM		0x02	/* currently running lazy vacuum */
#define		PROC_IN_ANALYZE		0x04	/* currently running analyze */
#define		PROC_VACUUM_FOR_WRAPAROUND	0x08	/* set by autovac only */
#define		PROC_IN_LOGICAL_DECODING	0x10	/* currently doing logical
												 * decoding outside xact */
#define		PROC_RESERVED				0x20	/* reserved for procarray */

/* flags reset at EOXact */
#define		PROC_VACUUM_STATE_MASK \
	(PROC_IN_VACUUM | PROC_IN_ANALYZE | PROC_VACUUM_FOR_WRAPAROUND)

/*
 * We allow a small number of "weak" relation locks (AccesShareLock,
 * RowShareLock, RowExclusiveLock) to be recorded in the PGPROC structure
 * rather than the main lock table.  This eases contention on the lock
 * manager LWLocks.  See storage/lmgr/README for additional details.
 */
#define		FP_LOCK_SLOTS_PER_BACKEND 16

/*
 * An invalid pgprocno.  Must be larger than the maximum number of PGPROC
 * structures we could possibly have.  See comments for MAX_BACKENDS.
 */
#define INVALID_PGPROCNO		PG_INT32_MAX

/*
 * Each backend has a PGPROC struct in shared memory.  There is also a list of
 * currently-unused PGPROC structs that will be reallocated to new backends.
 *
 * links: list link for any list the PGPROC is in.  When waiting for a lock,
 * the PGPROC is linked into that lock's waitProcs queue.  A recycled PGPROC
 * is linked into ProcGlobal's freeProcs list.
 *
 * Note: twophase.c also sets up a dummy PGPROC struct for each currently
 * prepared transaction.  These PGPROCs appear in the ProcArray data structure
 * so that the prepared transactions appear to be still running and are
 * correctly shown as holding locks.  A prepared transaction PGPROC can be
 * distinguished from a real one at need by the fact that it has pid == 0.
 * The semaphore and lock-activity fields in a prepared-xact PGPROC are unused,
 * but its myProcLocks[] lists are valid.
 */
struct PGPROC
{
	/* proc->links MUST BE FIRST IN STRUCT (see ProcSleep,ProcWakeup,etc) */
	SHM_QUEUE	links;			/* list link if process is in a list */
	PGPROC	  **procgloballist; /* procglobal list that owns this PGPROC */

	PGSemaphore sem;			/* ONE semaphore to sleep on */
	int			waitStatus;		/* STATUS_WAITING, STATUS_OK or STATUS_ERROR */

	Latch		procLatch;		/* generic latch for process */

	LocalTransactionId lxid;	/* local id of top-level transaction currently
								 * being executed by this proc, if running;
								 * else InvalidLocalTransactionId */
	int			pid;			/* Backend's process ID; 0 if prepared xact */
	int			pgprocno;

	/* These fields are zero while a backend is still starting up: */
	BackendId	backendId;		/* This backend's backend ID (if assigned) */
	Oid			databaseId;		/* OID of database this backend is using */
	Oid			roleId;			/* OID of role using this backend */

	/* POLAR: role is superuser? */
	bool		issuper;

	Oid			tempNamespaceId;	/* OID of temp schema this backend is
									 * using */

	bool		isBackgroundWorker; /* true if background worker. */

	/*
	 * While in hot standby mode, shows that a conflict signal has been sent
	 * for the current transaction. Set/cleared while holding ProcArrayLock,
	 * though not required. Accessed without lock, if needed.
	 */
	bool		recoveryConflictPending;

	/* Info about LWLock the process is currently waiting for, if any. */
	bool		lwWaiting;		/* true if waiting for an LW lock */
	uint8		lwWaitMode;		/* lwlock mode being waited for */
	proclist_node lwWaitLink;	/* position in LW lock wait list */

	/* Support for condition variables. */
	proclist_node cvWaitLink;	/* position in CV wait list */

	/* Info about lock the process is currently waiting for, if any. */
	/* waitLock and waitProcLock are NULL if not currently waiting. */
	LOCK	   *waitLock;		/* Lock object we're sleeping on ... */
	PROCLOCK   *waitProcLock;	/* Per-holder info for awaited lock */
	LOCKMODE	waitLockMode;	/* type of lock we're waiting for */
	LOCKMASK	heldLocks;		/* bitmask for lock types already held on this
								 * lock object by this backend */

	/*
	 * Info to allow us to wait for synchronous replication, if needed.
	 * waitLSN is InvalidXLogRecPtr if not waiting; set only by user backend.
	 * syncRepState must not be touched except by owning process or WALSender.
	 * syncRepLinks used only while holding SyncRepLock.
	 */
	XLogRecPtr	waitLSN;		/* waiting for this LSN or higher */
	int			syncRepState;	/* wait state for sync rep */
	SHM_QUEUE	syncRepLinks;	/* list link if process is in syncrep queue */

	/*
	 * POLAR: Info used for waiting for consensus to synchronize our XLog Flush 
	 * point or consensus command.
	 */
	ConsensusProcInfo consensusInfo;


	/* POLAR: record min lsn when reading in case that replica clear xlog buffer */
	pg_atomic_uint64	polar_read_min_lsn;
	/* POLAR end */

	/*
	 * All PROCLOCK objects for locks held or awaited by this backend are
	 * linked into one of these lists, according to the partition number of
	 * their lock.
	 */
	SHM_QUEUE	myProcLocks[NUM_LOCK_PARTITIONS];

	struct XidCache subxids;	/* cache for subtransaction XIDs */

	/* Support for group XID clearing. */
	/* true, if member of ProcArray group waiting for XID clear */
	bool		procArrayGroupMember;
	/* next ProcArray group member waiting for XID clear */
	pg_atomic_uint32 procArrayGroupNext;

	/*
	 * latest transaction id among the transaction's main XID and
	 * subtransactions
	 */
	TransactionId procArrayGroupMemberXid;

	uint32		wait_event_info;	/* proc's wait information */
	int			wait_object[PGPROC_WAIT_STACK_LEN];  		/* proc's wait object stack */
	int8		wait_type[PGPROC_WAIT_STACK_LEN];			/* proc's wait type: 0 pid, 1 fd...*/
	instr_time	wait_time[PGPROC_WAIT_STACK_LEN];			/* proc's wait time stack*/
	int8		cur_wait_stack_index;						/* current index for wait stack */

	/* Support for group transaction status update. */
	bool		clogGroupMember;	/* true, if member of clog group */
	pg_atomic_uint32 clogGroupNext; /* next clog group member */
	TransactionId clogGroupMemberXid;	/* transaction id of clog group member */
	XidStatus	clogGroupMemberXidStatus;	/* transaction status of clog
											 * group member */
	int			clogGroupMemberPage;	/* clog page corresponding to
										 * transaction id of clog group member */
	XLogRecPtr	clogGroupMemberLsn; /* WAL location of commit record for clog
									 * group member */

	/* Per-backend LWLock.  Protects fields below (but not group fields). */
	LWLock		backendLock;

	/* Lock manager data, recording fast-path locks taken by this backend. */
	uint64		fpLockBits;		/* lock modes held for each fast-path slot */
	Oid			fpRelId[FP_LOCK_SLOTS_PER_BACKEND]; /* slots for rel oids */
	bool		fpVXIDLock;		/* are we holding a fast-path VXID lock? */
	LocalTransactionId fpLocalTransactionId;	/* lxid for fast-path VXID
												 * lock */

	/*
	 * Support for lock groups.  Use LockHashPartitionLockByProc on the group
	 * leader to get the LWLock protecting these fields.
	 */
	PGPROC	   *lockGroupLeader;	/* lock group leader, if I'm a member */
	dlist_head	lockGroupMembers;	/* list of members, if I'm a leader */
	dlist_node	lockGroupLink;	/* my member link, if I'm a member */

	/* POLAR px */
	pg_atomic_flag	polar_px_is_executing;		/* whether this process is executing PX? */
	/* POLAR end */

	/* POLAR: Shared Server */
	bool							isPolarDispatcher; /* true if polar dispatcher worker. */
	/* Backend is bound to 1 session and serves only that session. */
	volatile bool       			polar_is_backend_dedicated;
	PolarSessionContext * volatile	polar_shared_session;
	/* POLAR end */
};

/* NOTE: "typedef struct PGPROC PGPROC" appears in storage/lock.h. */

/* POLAR: We need to remember read min lsn before read from storage to prevent
 * bgwriter clean hashtable and logindex
 */
#define POLAR_SET_BACKEND_READ_MIN_LSN(lsn) \
	do { \
		if (MyProc) \
			pg_atomic_write_u64(&(MyProc->polar_read_min_lsn), (lsn)); \
	} while (0)

/* POLAR: Reset read min lsn */
#define POLAR_RESET_BACKEND_READ_MIN_LSN() \
	do { \
		if (MyProc) \
			pg_atomic_write_u64(&(MyProc->polar_read_min_lsn), InvalidXLogRecPtr); \
	} while(0)

extern PGDLLIMPORT PGPROC *MyProc;
extern PGDLLIMPORT struct PGXACT *MyPgXact;

/* POLAR px: Special for PX reader gangs */
extern PGDLLIMPORT PGPROC *lockHolderProcPtr;

/*
 * Prior to PostgreSQL 9.2, the fields below were stored as part of the
 * PGPROC.  However, benchmarking revealed that packing these particular
 * members into a separate array as tightly as possible sped up GetSnapshotData
 * considerably on systems with many CPU cores, by reducing the number of
 * cache lines needing to be fetched.  Thus, think very carefully before adding
 * anything else here.
 */
typedef struct PGXACT
{
	TransactionId xid;			/* id of top-level transaction currently being
								 * executed by this proc, if running and XID
								 * is assigned; else InvalidTransactionId */

	TransactionId xmin;			/* minimal running XID as it was when we were
								 * starting our xact, excluding LAZY VACUUM:
								 * vacuum must not remove tuples deleted by
								 * xid >= xmin ! */

	/* 
	 * POLAR csn 
	 * When transaction committing, record commit csn in PGXACT with CommitSeqNoLock hold.
	 * For now, only used in GetRunningTransactionData to iterate ProcArray and filter out
	 * committed xacts in active xacts list.
	 * If committed, value is csn, else InvalidTransactionId.
	 */
	CommitSeqNo polar_csn; 

	uint8		vacuumFlags;	/* vacuum-related flags, see above */
	bool		overflowed;
	bool		delayChkpt;		/* true if this proc delays checkpoint start;
								 * previously called InCommit */

	uint8		nxids;
} PGXACT;

/*
 * There is one ProcGlobal struct for the whole database cluster.
 */
typedef struct PROC_HDR
{
	/* Array of PGPROC structures (not including dummies for prepared txns) */
	PGPROC	   *allProcs;
	/* Array of PGXACT structures (not including dummies for prepared txns) */
	PGXACT	   *allPgXact;
	/* Length of allProcs array */
	uint32		allProcCount;
	/* Head of list of free PGPROC structures */
	PGPROC	   *freeProcs;
	/* Head of list of autovacuum's free PGPROC structures */
	PGPROC	   *autovacFreeProcs;
	/* Head of list of dispatcher worker free PGPROC structures */
	PGPROC	   *polarDispatcherFreeProcs;
	/* Head of list of bgworker free PGPROC structures */
	PGPROC	   *bgworkerFreeProcs;
	/* First pgproc waiting for group XID clear */
	pg_atomic_uint32 procArrayGroupFirst;
	/* First pgproc waiting for group transaction status update */
	pg_atomic_uint32 clogGroupFirst;
	/* WALWriter process's latch */
	Latch	   *walwriterLatch;
	/* Checkpointer process's latch */
	Latch	   *checkpointerLatch;
	/* Current shared estimate of appropriate spins_per_delay value */
	int			spins_per_delay;
	/* The proc of the Startup process, since not in ProcArray */
	PGPROC	   *startupProc;
	int			startupProcPid;
	/* Buffer id of the buffer that Startup process waits for pin on, or -1 */
	int			startupBufferPinWaitBufId;
	
	/* POLAR px */
    /* Counter for assigning serial numbers to processes */
    pg_atomic_uint32	pxSessionCounter;
	pg_atomic_uint32	pxWorkerCounter;
	pg_atomic_uint32	sqlRequestCounter;

	/* POLAR wal_pipeliner process's latch */
	Latch	   	* volatile polar_wal_pipeliner_latch;
} PROC_HDR;

extern PGDLLIMPORT PROC_HDR *ProcGlobal;

extern PGPROC *PreparedXactProcs;

/* Accessor for PGPROC given a pgprocno. */
#define GetPGProcByNumber(n) (&ProcGlobal->allProcs[(n)])

/*
 * We set aside some extra PGPROC structures for auxiliary processes,
 * ie things that aren't full-fledged backends but need shmem access.
 *
 * Background writer, checkpointer and WAL writer run during normal operation.
 * Startup process and WAL receiver also consume 2 slots, but WAL writer is
 * launched only after startup has exited, so we only need 4 slots.
 * 
 * POLAR wal pipeline
 * add a new wal pipeline process
 *
 * POLAR flashback log
 * add flashback log background inserter and writer.
 */
#define NUM_AUXILIARY_PROCS		8

/* configurable options */
extern PGDLLIMPORT int DeadlockTimeout;
extern PGDLLIMPORT int StatementTimeout;
extern PGDLLIMPORT int LockTimeout;
extern PGDLLIMPORT int IdleInTransactionSessionTimeout;
extern bool log_lock_waits;


/*
 * Function Prototypes
 */
extern int	ProcGlobalSemas(void);
extern Size ProcGlobalShmemSize(void);
extern void InitProcGlobal(void);
extern void InitProcess(void);
extern void InitProcessPhase2(void);
extern void InitAuxiliaryProcess(void);

extern void PublishStartupProcessInformation(void);
extern void SetStartupBufferPinWaitBufId(int bufid);
extern int	GetStartupBufferPinWaitBufId(void);

extern bool HaveNFreeProcs(int n);
extern void ProcReleaseLocks(bool isCommit);

extern void ProcQueueInit(PROC_QUEUE *queue);
extern int	ProcSleep(LOCALLOCK *locallock, LockMethod lockMethodTable);
extern PGPROC *ProcWakeup(PGPROC *proc, int waitStatus);
extern void ProcLockWakeup(LockMethod lockMethodTable, LOCK *lock);
extern void CheckDeadLockAlert(void);
extern bool IsWaitingForLock(void);
extern void LockErrorCleanup(void);

extern void ProcWaitForSignal(uint32 wait_event_info);
extern void ProcSendSignal(int pid);

extern PGPROC *AuxiliaryPidGetProc(int pid);

extern void BecomeLockGroupLeader(void);
extern bool BecomeLockGroupMember(PGPROC *leader, int pid);

#endif							/* PROC_H */
