/*-------------------------------------------------------------------------
 *
 * ipci.c
 *	  POSTGRES inter-process communication initialization code.
 *
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
#include "access/heapam.h"
#include "access/multixact.h"
#include "access/nbtree.h"
#include "access/slru.h"
#include "access/subtrans.h"
#include "access/twophase.h"
#include "commands/async.h"
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

/* POLAR */
#include <unistd.h>

#include "access/polar_logindex_redo.h"
#include "access/polar_async_ddl_lock_replay.h"
#include "common/file_perm.h"
#include "polar_datamax/polar_datamax.h"
#include "polar_flashback/polar_flashback_log.h"
#include "postmaster/polar_parallel_bgwriter.h"
#include "storage/polar_shmem.h"
#include "storage/polar_xlogbuf.h"
#include "access/polar_csnlog.h"
#include "polar_dma/polar_dma.h"
#include "utils/faultinjector.h"
#include "executor/nodeShareInputScan.h"
/* POLAR end */

shmem_startup_hook_type shmem_startup_hook = NULL;

/* POLAR: used for polar_monitor hook */
polar_monitor_hook_type polar_monitor_hook = NULL;

polar_heap_profile_hook_type polar_heap_profile_hook = NULL;

static Size total_addin_request = 0;
static bool addin_request_allowed = true;

/* POLAR */
static char *polar_shmem_stat_file = "polar_shmem_stat_file";
static void polar_output_shmem_stat(Size size);
/* POLAR end */

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
 */
void
CreateSharedMemoryAndSemaphores(int port)
{
	PGShmemHeader *shim = NULL;

	if (!IsUnderPostmaster)
	{
		PGShmemHeader *seghdr;
		Size		size;
		int			numSemas;

		/* POLAR */
		Size 		polar_size = 0;

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
		size = 300000;
		size = add_size(size, PGSemaphoreShmemSize(numSemas));
		size = add_size(size, SpinlockSemaSize());
		size = add_size(size, hash_estimate_size(SHMEM_INDEX_SIZE,
												 sizeof(ShmemIndexEnt)));
		size = add_size(size, smgr_shmem_size());
		size = add_size(size, BufferShmemSize());
		size = add_size(size, LockShmemSize());
		size = add_size(size, PredicateLockShmemSize());
		size = add_size(size, ProcGlobalShmemSize());
		size = add_size(size, XLOGShmemSize());
		size = add_size(size, CLOGShmemSize());
		size = add_size(size, CommitTsShmemSize());
		/* POLAR csn */
		size = add_size(size, polar_csnlog_shmem_size());
		/* POLAR end */
		size = add_size(size, SUBTRANSShmemSize());
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
		/* POLAR : Add log index share memory size */
		size = add_size(size, polar_logindex_redo_shmem_size());
		/* POLAR end */

		/* POLAR: consensus share memory size */
		if (POLAR_ENABLE_DMA())
			size = add_size(size, ConsensusShmemSize());

#ifdef FAULT_INJECTOR
		size = add_size(size, FaultInjector_ShmemSize());
#endif

#ifdef EXEC_BACKEND
		size = add_size(size, ShmemBackendArraySize());
#endif

		/* POLAR: add parallel background writer shared memory size */
		size = add_size(size, polar_parallel_bgwriter_shmem_size());

		/* POLAR: add polar xlog buffer share memory size */
		if (polar_enable_xlog_buffer)
			size = add_size(size, polar_xlog_buffer_shmem_size());
		/* POLAR end */

		/* POLAR: Datamax control strunct size */
		size = add_size(size, polar_datamax_shmem_size());
		/* POLAR end */
		
		/* POLAR: Add addtional shared memory size for unit test, the default value is 0 */
		size = add_size(size, polar_unit_test_mem_size * 1024L * 1024L);
		/* POLAR end */

		/* POLAR: add async ddl lock replay related share memory size */
		size = add_size(size, polar_async_ddl_lock_replay_shmem_size());
		/* POLAR end */

		/* POLAR: add shared memory size for flashback log */
		size = add_size(size, polar_flog_shmem_size());

		/* POLAR end */

		/* freeze the addin request size and include it */
		addin_request_allowed = false;
		size = add_size(size, total_addin_request);

		/* POLAR px */
		size = add_size(size, ShareInputShmemSize());

		/* might as well round it off to a multiple of a typical page size */
		size = add_size(size, 8192 - (size % 8192));

		elog(DEBUG3, "invoking IpcMemoryCreate(size=%zu)", size);

		/*
		 * Create the shmem segment
		 */
		seghdr = PGSharedMemoryCreate(size, port, &shim, POLAR_SHMEM_NORMAL);

		InitShmemAccess(seghdr);

		/*
		 * POLAR: Create the polar separate shmem segment
		 */
		if (polar_persisted_buffer_pool_enabled(NULL))
		{
			PGShmemHeader *polar_seghdr;
			/*
			 * Like size above, we also plus 100k for the stuff that's too
			 * small to bother with estimating.
			 */
			polar_size = 100000;
			polar_size = add_size(polar_size, polar_persisted_buffer_pool_size());
			polar_seghdr = PGSharedMemoryCreate(polar_size, port, NULL, POLAR_SHMEM_PERSISTED);
			polar_init_shmem_access(polar_seghdr);
		}
		/* POLAR end */

		/* POLAR: output shared memory size to a world readable file */
		polar_output_shmem_stat(add_size(size, polar_size));

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
		 * should only be reached in the EXEC_BACKEND case.
		 */
#ifndef EXEC_BACKEND
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
	 * Now give loadable modules a chance to set up their shmem allocations
	 */
	/*
	 * POLAR: For read ctl file in shared storage
	 * move hook to here.
	 */
	if (shmem_startup_hook)
		shmem_startup_hook();

	/* POLAR: init slru */
	polar_slru_init();

	smgr_shmem_init();

	/*
	 * Set up xlog, clog, and buffers
	 */
	XLOGShmemInit();
	CLOGShmemInit();
	CommitTsShmemInit();
	/* POLAR csn */
	polar_csnlog_shmem_init();
	/* POLAR end */
	SUBTRANSShmemInit();
	MultiXactShmemInit();
	InitBufferPool();

	/* POLAR: init parallel background writer */
	polar_init_parallel_bgwriter();

	/*
	 * POLAR: Setup logindex
	 */
	polar_logindex_redo_shmem_init();
	/* POLAR end */

	/* POLAR: init xlog buffer share memory */
	if (polar_enable_xlog_buffer)
		polar_init_xlog_buffer();
	/* POLAR end */

	/* POLAR: init DataMax control struct */
	polar_datamax_shmem_init();
	/* POLAR end */
	
	/* POLAR: init async ddl lock replay share memory struct */
	polar_init_async_ddl_lock_replay();
	/* POLAR end */

	/* POLAR: init shared memory for flashback log */
	polar_flog_shmem_init();
	/* POLAR end */

	if (POLAR_ENABLE_DMA())
		ConsensusShmemInit();

#ifdef FAULT_INJECTOR
	FaultInjector_ShmemInit();
#endif

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
	/* POLAR px */
	ShareInputShmemInit();

	BackendRandomShmemInit();


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

}

/*
 * POLAR: output shared memory size to file
 */
static void
polar_output_shmem_stat(Size size)
{
	FILE *fshmemfile = fopen(polar_shmem_stat_file, "w");
	if (fshmemfile)
	{
		fprintf(fshmemfile, "%zu", size);
		fclose(fshmemfile);

		/* Make file world readable with mode as same as other files. */
		if (chmod(polar_shmem_stat_file, pg_file_create_mode) != 0)
			elog(ERROR, "could not change permissions of shmem_total_size file: %s", strerror(errno));
	}
	else
		elog(ERROR, "could not write shmem_total_size file: %s", strerror(errno));
}

/*
 * POLAR: on_proc_exit callback to delete shared memory stat file
 */
void
polar_unlink_shmem_stat_file(int status, Datum arg)
{
	unlink(polar_shmem_stat_file);
}
