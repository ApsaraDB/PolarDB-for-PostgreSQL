/*-------------------------------------------------------------------------
 *
 * ipci.c
 *	  POSTGRES inter-process communication initialization code.
 *
 * Portions Copyright (c) 2020, Alibaba Group Holding Limited
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/storage/ipc/ipci.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/clog.h"
#include "access/commit_ts.h"
#include "access/ctslog.h"
#include "access/heapam.h"
#include "access/multixact.h"
#include "access/nbtree.h"
#include "access/twophase.h"
#include "commands/async.h"
#include "distributed_txn/logical_clock.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "postmaster/autovacuum.h"
#include "postmaster/bgworker_internals.h"
#include "postmaster/bgwriter.h"
#include "postmaster/postmaster.h"
#include "replication/logicallauncher.h"
#include "replication/slot.h"
#include "replication/walreceiver.h"
#include "replication/walsender.h"
#include "replication/origin.h"
#include "storage/bufmgr.h"
#include "storage/dsm.h"
#include "storage/ipc.h"
#include "storage/pg_shmem.h"
#include "storage/pmsignal.h"
#include "storage/predicate.h"
#include "storage/proc.h"
#include "storage/procarray.h"
#include "storage/procsignal.h"
#include "storage/sinvaladt.h"
#include "storage/spin.h"
#include "utils/backend_random.h"
#include "utils/snapmgr.h"
#ifdef ENABLE_DISTRIBUTED_TRANSACTION
#include "access/ctslog.h"
#endif

/* POLAR */
#include "polar_datamax/polar_datamax.h"
#include "polar_dma/polar_dma.h"
/* POLAR end */

shmem_startup_hook_type shmem_startup_hook = NULL;

static Size total_addin_request = 0;
static bool addin_request_allowed = true;


/*
 * RequestAddinShmemSpace
 *		Request that extra shmem space be allocated for use by
 *		a loadable module.
 *
 * This is only useful if called from the _PG_init hook of a library that
 * is loaded into the postmaster via shared_preload_libraries.  Once
 * shared memory has been allocated, calls will be ignored.  (We could
 * raise an error, but it seems better to make it a no-op, so that
 * libraries containing such calls can be reloaded if needed.)
 */
void
RequestAddinShmemSpace(Size size)
{
	if (IsUnderPostmaster || !addin_request_allowed)
		return;					/* too late */
	total_addin_request = add_size(total_addin_request, size);
}


/*
 * CreateSharedMemoryAndSemaphores
 *		Creates and initializes shared memory and semaphores.
 *
 * This is called by the postmaster or by a standalone backend.
 * It is also called by a backend forked from the postmaster in the
 * EXEC_BACKEND case.  In the latter case, the shared memory segment
 * already exists and has been physically attached to, but we have to
 * initialize pointers in local memory that reference the shared structures,
 * because we didn't inherit the correct pointer values from the postmaster
 * as we do in the fork() scenario.  The easiest way to do that is to run
 * through the same code as before.  (Note that the called routines mostly
 * check IsUnderPostmaster, rather than EXEC_BACKEND, to detect this case.
 * This is a bit code-wasteful and could be cleaned up.)
 *
 * If "makePrivate" is true then we only need private memory, not shared
 * memory.  This is true for a standalone backend, false for a postmaster.
 */
void
CreateSharedMemoryAndSemaphores(bool makePrivate, int port)
{
	PGShmemHeader *shim = NULL;

	if (!IsUnderPostmaster)
	{
		PGShmemHeader *seghdr;
		Size		size;
		int			numSemas;

		/* Compute number of semaphores we'll need */
		numSemas = ProcGlobalSemas();
		numSemas += SpinlockSemas();

		/*
		 * Size of the Postgres shared-memory block is estimated via
		 * moderately-accurate estimates for the big hogs, plus 100K for the
		 * stuff that's too small to bother with estimating.
		 *
		 * We take some care during this phase to ensure that the total size
		 * request doesn't overflow size_t.  If this gets through, we don't
		 * need to be so careful during the actual allocation phase.
		 */
		size = 100000;
		size = add_size(size, PGSemaphoreShmemSize(numSemas));
		size = add_size(size, SpinlockSemaSize());
		size = add_size(size, hash_estimate_size(SHMEM_INDEX_SIZE,
												 sizeof(ShmemIndexEnt)));
		size = add_size(size, BufferShmemSize());
		size = add_size(size, LockShmemSize());
		size = add_size(size, PredicateLockShmemSize());
		size = add_size(size, ProcGlobalShmemSize());
		size = add_size(size, XLOGShmemSize());
#ifdef ENABLE_PARALLEL_RECOVERY
		size = add_size(size, ParallelRecoveryShmemSize());
#endif
#ifdef ENABLE_DISTR_DEBUG
		size = add_size(size, CLOGShmemSize());
#endif
#ifdef ENABLE_DISTRIBUTED_TRANSACTION
		size = add_size(size, CTSLOGShmemSize());
#endif
		size = add_size(size, CommitTsShmemSize());
		size = add_size(size, TwoPhaseShmemSize());
		size = add_size(size, BackgroundWorkerShmemSize());
		size = add_size(size, MultiXactShmemSize());
		size = add_size(size, LWLockShmemSize());
		size = add_size(size, ProcArrayShmemSize());
		size = add_size(size, BackendStatusShmemSize());
		size = add_size(size, SInvalShmemSize());
		size = add_size(size, PMSignalShmemSize());
		size = add_size(size, ProcSignalShmemSize());
		size = add_size(size, CheckpointerShmemSize());
		size = add_size(size, AutoVacuumShmemSize());
		size = add_size(size, ReplicationSlotsShmemSize());
		size = add_size(size, ReplicationOriginShmemSize());
		size = add_size(size, WalSndShmemSize());
		size = add_size(size, WalRcvShmemSize());
		size = add_size(size, ApplyLauncherShmemSize());
		size = add_size(size, SnapMgrShmemSize());
		size = add_size(size, BTreeShmemSize());
		size = add_size(size, SyncScanShmemSize());
		size = add_size(size, AsyncShmemSize());
		size = add_size(size, BackendRandomShmemSize());
		/* POLAR: consensus share memory size */
		if (polar_enable_dma)
			size = add_size(size, ConsensusShmemSize());

#ifdef EXEC_BACKEND
		size = add_size(size, ShmemBackendArraySize());
#endif

		/* POLAR: Datamax control strunct size */
		size = add_size(size, polar_datamax_shmem_size());
		/* POLAR end */

		/* freeze the addin request size and include it */
		addin_request_allowed = false;
		size = add_size(size, total_addin_request);

		/* might as well round it off to a multiple of a typical page size */
		size = add_size(size, 8192 - (size % 8192));

		elog(DEBUG3, "invoking IpcMemoryCreate(size=%zu)", size);

		/*
		 * Create the shmem segment
		 */
		seghdr = PGSharedMemoryCreate(size, makePrivate, port, &shim);

		InitShmemAccess(seghdr);

		/*
		 * Create semaphores
		 */
		PGReserveSemaphores(numSemas, port);

		/*
		 * If spinlocks are disabled, initialize emulation layer (which
		 * depends on semaphores, so the order is important here).
		 */
#ifndef HAVE_SPINLOCKS
		SpinlockSemaInit();
#endif
	}
	else
	{
		/*
		 * We are reattaching to an existing shared memory segment. This
		 * should only be reached in the EXEC_BACKEND case, and even then only
		 * with makePrivate == false.
		 */
#ifdef EXEC_BACKEND
		Assert(!makePrivate);
#else
		elog(PANIC, "should be attached to shared memory already");
#endif
	}

	/*
	 * Set up shared memory allocation mechanism
	 */
	if (!IsUnderPostmaster)
		InitShmemAllocation();

	/*
	 * Now initialize LWLocks, which do shared memory allocation and are
	 * needed for InitShmemIndex.
	 */
	CreateLWLocks();

	/*
	 * Set up shmem.c index hashtable
	 */
	InitShmemIndex();

	/*
	 * Set up xlog, clog, and buffers
	 */
	XLOGShmemInit();
#ifdef ENABLE_PARALLEL_RECOVERY
	ParallelRecoveryInit();
#endif
#ifdef ENABLE_DISTR_DEBUG
	CLOGShmemInit();
#endif
#ifdef ENABLE_DISTRIBUTED_TRANSACTION
	CTSLOGShmemInit();
#endif
	CommitTsShmemInit();
	MultiXactShmemInit();
	InitBufferPool();

	/* POLAR: init DataMax control struct */
	polar_datamax_shmem_init();
	/* POLAR end */

	if (polar_enable_dma)
		ConsensusShmemInit();

	/*
	 * Set up lock manager
	 */
	InitLocks();

	/*
	 * Set up predicate lock manager
	 */
	InitPredicateLocks();

	/*
	 * Set up process table
	 */
	if (!IsUnderPostmaster)
		InitProcGlobal();
	CreateSharedProcArray();
	CreateSharedBackendStatus();
	TwoPhaseShmemInit();
	BackgroundWorkerShmemInit();

	/*
	 * Set up shared-inval messaging
	 */
	CreateSharedInvalidationState();

	/*
	 * Set up interprocess signaling mechanisms
	 */
	PMSignalShmemInit();
	ProcSignalShmemInit();
	CheckpointerShmemInit();
	AutoVacuumShmemInit();
	ReplicationSlotsShmemInit();
	ReplicationOriginShmemInit();
	WalSndShmemInit();
	WalRcvShmemInit();
	ApplyLauncherShmemInit();

	/*
	 * Set up other modules that need some shared memory space
	 */
	SnapMgrInit();
	BTreeShmemInit();
	SyncScanShmemInit();
	AsyncShmemInit();
	BackendRandomShmemInit();
	LogicalClockShmemInit();

#ifdef EXEC_BACKEND

	/*
	 * Alloc the win32 shared backend array
	 */
	if (!IsUnderPostmaster)
		ShmemBackendArrayAllocation();
#endif

	/* Initialize dynamic shared memory facilities. */
	if (!IsUnderPostmaster)
		dsm_postmaster_startup(shim);

	/*
	 * Now give loadable modules a chance to set up their shmem allocations
	 */
	if (shmem_startup_hook)
		shmem_startup_hook();
}
