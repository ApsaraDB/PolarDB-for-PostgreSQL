/*-------------------------------------------------------------------------
 *
 * procarray.h
 *	  POSTGRES process array definitions.
 *
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/storage/procarray.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PROCARRAY_H
#define PROCARRAY_H

#include "storage/lock.h"
#include "storage/standby.h"
#include "utils/relcache.h"
#include "utils/snapshot.h"


/*
 * These are to implement PROCARRAY_FLAGS_XXX
 *
 * Note: These flags are cloned from PROC_XXX flags in src/include/storage/proc.h
 * to avoid forcing to include proc.h when including procarray.h. So if you modify
 * PROC_XXX flags, you need to modify these flags.
 */
#define		PROCARRAY_VACUUM_FLAG			0x02	/* currently running lazy
													 * vacuum */
#define		PROCARRAY_ANALYZE_FLAG			0x04	/* currently running
													 * analyze */
#define		PROCARRAY_LOGICAL_DECODING_FLAG 0x10	/* currently doing logical
													 * decoding outside xact */

#define		PROCARRAY_SLOTS_XMIN			0x20	/* replication slot xmin,
													 * catalog_xmin */
/*
 * Only flags in PROCARRAY_PROC_FLAGS_MASK are considered when matching
 * PGXACT->vacuumFlags. Other flags are used for different purposes and
 * have no corresponding PROC flag equivalent.
 */
#define		PROCARRAY_PROC_FLAGS_MASK	(PROCARRAY_VACUUM_FLAG | \
										 PROCARRAY_ANALYZE_FLAG | \
										 PROCARRAY_LOGICAL_DECODING_FLAG)

/* Use the following flags as an input "flags" to GetOldestXmin function */
/* Consider all backends except for logical decoding ones which manage xmin separately */
#define		PROCARRAY_FLAGS_DEFAULT			PROCARRAY_LOGICAL_DECODING_FLAG
/* Ignore vacuum backends */
#define		PROCARRAY_FLAGS_VACUUM			PROCARRAY_FLAGS_DEFAULT | PROCARRAY_VACUUM_FLAG
/* Ignore analyze backends */
#define		PROCARRAY_FLAGS_ANALYZE			PROCARRAY_FLAGS_DEFAULT | PROCARRAY_ANALYZE_FLAG
/* Ignore both vacuum and analyze backends */
#define		PROCARRAY_FLAGS_VACUUM_ANALYZE	PROCARRAY_FLAGS_DEFAULT | PROCARRAY_VACUUM_FLAG | PROCARRAY_ANALYZE_FLAG

/* POLAR csn */
/*  
 * In csn mode, RecentGlobalXmin/RecentGlobalDataXmin maybe very old, 
 * we should call func to get newest value.
 */
#define 	GetRecentGlobalXmin() (polar_csn_enable ? GetRecentGlobalXminCSN() : RecentGlobalXmin)
#define 	GetRecentGlobalDataXmin() (polar_csn_enable ? GetRecentGlobalDataXminCSN() : RecentGlobalDataXmin)
/* POLAR end */

extern Size ProcArrayShmemSize(void);
extern void CreateSharedProcArray(void);
extern void ProcArrayAdd(PGPROC *proc);
extern void ProcArrayRemove(PGPROC *proc, TransactionId latestXid);

/* POLAR csn */
extern void ProcArrayEndTransaction(PGPROC *proc, TransactionId latestXid);
extern void ProcArrayClearTransaction(PGPROC *proc);

extern void ProcArrayInitRecovery(TransactionId initializedUptoXID, TransactionId polar_oldest_active_xid);
extern void ProcArrayApplyRecoveryInfo(RunningTransactions running);
extern void ProcArrayApplyXidAssignment(TransactionId topxid,
							int nsubxids, TransactionId *subxids);

extern void RecordKnownAssignedTransactionIds(TransactionId xid);
extern void ExpireTreeKnownAssignedTransactionIds(TransactionId xid,
									  int nsubxids, TransactionId *subxids,
									  TransactionId max_xid);
extern void ExpireAllKnownAssignedTransactionIds(void);
extern void ExpireOldKnownAssignedTransactionIds(TransactionId xid);

extern int	GetMaxSnapshotXidCount(void);
extern int	GetMaxSnapshotSubxidCount(void);

extern Snapshot GetSnapshotData(Snapshot snapshot);

extern bool ProcArrayInstallImportedXmin(TransactionId xmin,
							 VirtualTransactionId *sourcevxid);
extern bool ProcArrayInstallRestoredXmin(TransactionId xmin, PGPROC *proc);

extern RunningTransactions PolarGetRunningTransactionData(bool ignore_subxid);

extern bool TransactionIdIsInProgress(TransactionId xid);
extern bool TransactionIdIsActive(TransactionId xid);
extern TransactionId GetOldestXmin(Relation rel, int flags);
extern TransactionId GetOldestActiveTransactionId(void);
extern TransactionId GetOldestSafeDecodingTransactionId(bool catalogOnly);

extern VirtualTransactionId *GetVirtualXIDsDelayingChkpt(int *nvxids);
extern bool HaveVirtualXIDsDelayingChkpt(VirtualTransactionId *vxids, int nvxids);

extern PGPROC *BackendPidGetProc(int pid);
extern PGPROC *BackendPidGetProcWithLock(int pid);
extern int	BackendXidGetPid(TransactionId xid);
extern bool IsBackendPid(int pid);

extern VirtualTransactionId *GetCurrentVirtualXIDs(TransactionId limitXmin,
					  bool excludeXmin0, bool allDbs, int excludeVacuum,
					  int *nvxids);
extern VirtualTransactionId *GetConflictingVirtualXIDs(TransactionId limitXmin, Oid dbOid);
extern pid_t CancelVirtualTransaction(VirtualTransactionId vxid, ProcSignalReason sigmode);

extern bool MinimumActiveBackends(int min);
extern int	CountDBBackends(Oid databaseid);
extern int	CountDBConnections(Oid databaseid);
extern void CancelDBBackends(Oid databaseid, ProcSignalReason sigmode, bool conflictPending);
extern int	CountUserBackends(Oid roleid);
extern bool CountOtherDBBackends(Oid databaseId,
					 int *nbackends, int *nprepared);

/* POLAR: Shared Server */
extern bool CountOtherUserBackends(Oid roleId, int *nbackends);

extern void XidCacheRemoveRunningXids(TransactionId xid,
						  int nxids, const TransactionId *xids,
						  TransactionId latestXid);

extern void ProcArraySetReplicationSlotXmin(TransactionId xmin,
								TransactionId catalog_xmin, bool already_locked);

extern void ProcArrayGetReplicationSlotXmin(TransactionId *xmin,
								TransactionId *catalog_xmin);

/* POLAR */
extern void polar_get_nosuper_and_super_conn_count(int *nosupercount, int *supercount);
extern XLogRecPtr polar_get_read_min_lsn(XLogRecPtr primary_consist_ptr);
extern XLogRecPtr polar_get_backend_min_replay_lsn(void);
extern PGPROC* polar_search_proc(pid_t pid);

/* replica multi version snapshot related get functions */
extern Size polar_replica_multi_version_snapshot_store_shmem_size(void);
extern int polar_replica_multi_version_snapshot_get_slot_num(void);
extern int polar_replica_multi_version_snapshot_get_retry_times(void);
extern uint32 polar_replica_multi_version_snapshot_get_curr_slot_no(void);
extern int polar_replica_multi_version_snapshot_get_next_slot_no(void);
extern uint64 polar_replica_multi_version_snapshot_get_read_retried_times(void);
extern uint64 polar_replica_multi_version_snapshot_get_read_switched_times(void);
extern uint64 polar_replica_multi_version_snapshot_get_write_retried_times(void);
extern uint64 polar_replica_multi_version_snapshot_get_write_switched_times(void);
/* replica multi version snapshot related test functions */
extern void polar_test_replica_multi_version_snapshot_set_snapshot(void);
extern bool polar_test_replica_multi_version_snapshot_get_snapshot(TransactionId *xip, int *count, bool *overflowed,
		TransactionId *xmin, TransactionId *xmax,
		TransactionId *replication_slot_xmin,
		TransactionId *replication_slot_catalog_xmin);
extern void polar_test_KnownAssignedXidsAdd(TransactionId from_xid, TransactionId to_xid);
extern void polar_test_KnownAssignedXidsReset(void);
extern void polar_test_set_curr_slot_num(int slot_num);
extern void polar_test_set_next_slot_num(int slot_num);
extern void polar_test_acquire_slot_lock(int slot_num, LWLockMode mode);
extern void polar_test_release_slot_lock(int slot_num);
/* POLAR end */

/* POLAR csn */
extern TransactionId GetRecentGlobalXminCSN(void);
extern TransactionId GetRecentGlobalDataXminCSN(void);
extern void ProcArrayResetXminCSN(PGPROC *proc, TransactionId new_xmin);
extern void AdvanceOldestActiveXidCSNWrapper(TransactionId myXid);
extern void polar_set_latestObservedXid(TransactionId latest_observed_xid);
extern TransactionId polar_get_latestObservedXid(void);
/* POLAR end */

#define GetRunningTransactionData() (PolarGetRunningTransactionData(false))
extern RunningTransactions polar_get_running_top_trans(void);

#endif							/* PROCARRAY_H */
