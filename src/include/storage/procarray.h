/*-------------------------------------------------------------------------
 *
 * procarray.h
 *	  POSTGRES process array definitions.
 *
 * Portions Copyright (c) 2020, Alibaba Group Holding Limited
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
#include "postgres.h"

#ifdef ENABLE_DISTRIBUTED_TRANSACTION
/* User-settable GUC parameters */
extern int	gc_interval;
extern int	snapshot_delay;
#endif

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

extern Size ProcArrayShmemSize(void);
extern void CreateSharedProcArray(void);
extern void ProcArrayAdd(PGPROC *proc);
extern void ProcArrayRemove(PGPROC *proc);

extern void ProcArrayEndTransaction(PGPROC *proc);
extern void ProcArrayClearTransaction(PGPROC *proc);
extern void ProcArrayResetXmin(PGPROC *proc);
extern void AdvanceOldestActiveXid(TransactionId myXid);
extern void ProcArrayInitRecovery(TransactionId oldestActiveXID, TransactionId initializedUptoXID);
extern void ProcArrayApplyRecoveryInfo(RunningTransactions running);
extern void ProcArrayApplyXidAssignment(TransactionId topxid,
							int nsubxids, TransactionId *subxids);

extern void RecordKnownAssignedTransactionIds(TransactionId xid);

extern Snapshot GetSnapshotDataExtend(Snapshot snapshot, bool latest);
#define GetSnapshotData(snapshot) GetSnapshotDataExtend(snapshot, true)

extern bool ProcArrayInstallImportedXmin(TransactionId xmin,
							 VirtualTransactionId *sourcevxid);
extern bool ProcArrayInstallRestoredXmin(TransactionId xmin, PGPROC *proc);

extern RunningTransactions GetRunningTransactionData(void);

extern bool TransactionIdIsActive(TransactionId xid);
extern TransactionId GetRecentGlobalXmin(void);
extern TransactionId GetRecentGlobalDataXmin(void);
extern TransactionId GetOldestXmin(Relation rel, int flags);
extern TransactionId GetOldestActiveTransactionId(void);
extern TransactionId GetOldestSafeDecodingTransactionId(bool catalogOnly);
#ifdef ENABLE_DISTRIBUTED_TRANSACTION
extern CommitSeqNo GetRecentGlobalTmin(void);
extern CommitSeqNo GetOldestTmin(void);
extern void SetGlobalCutoffTs(CommitSeqNo cutoffTs);
#endif
extern VirtualTransactionId *GetVirtualXIDsDelayingChkpt(int *nvxids);
extern bool HaveVirtualXIDsDelayingChkpt(VirtualTransactionId *vxids, int nvxids);

extern PGPROC *BackendPidGetProc(int pid);
extern PGPROC *BackendPidGetProcWithLock(int pid);
extern int	BackendXidGetPid(TransactionId xid);
extern bool IsBackendPid(int pid);

extern VirtualTransactionId *GetCurrentVirtualXIDs(TransactionId limitXmin, bool excludeXmin0,
					  bool allDbs, int excludeVacuum, int *nvxids);
extern VirtualTransactionId *GetConflictingVirtualXIDs(TransactionId limitXmin, Oid dbOid);
extern pid_t CancelVirtualTransaction(VirtualTransactionId vxid, ProcSignalReason sigmode);

extern bool MinimumActiveBackends(int min);
extern int	CountDBBackends(Oid databaseid);
extern int	CountDBConnections(Oid databaseid);
extern void CancelDBBackends(Oid databaseid, ProcSignalReason sigmode, bool conflictPending);
extern int	CountUserBackends(Oid roleid);
extern bool CountOtherDBBackends(Oid databaseId,
					 int *nbackends, int *nprepared);

extern void ProcArraySetReplicationSlotXmin(TransactionId xmin,
								TransactionId catalog_xmin, bool already_locked);

extern void ProcArrayGetReplicationSlotXmin(TransactionId *xmin,
								TransactionId *catalog_xmin);

#ifdef ENABLE_DISTRIBUTED_TRANSACTION
extern bool CommittsSatisfiesVacuum(CommitSeqNo committs);
extern CommitSeqNo RecentGlobalTs;
extern bool txnUseGlobalSnapshot;
#endif
#endif							/* PROCARRAY_H */
