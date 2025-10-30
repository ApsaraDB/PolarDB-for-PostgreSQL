/*-------------------------------------------------------------------------
 *
 * predicate.h
 *	  POSTGRES public predicate locking definitions.
 *
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/storage/predicate.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PREDICATE_H
#define PREDICATE_H

#include "storage/itemptr.h"
#include "storage/lock.h"
#include "utils/relcache.h"
#include "utils/snapshot.h"


/*
 * GUC variables
 */
extern PGDLLIMPORT int max_predicate_locks_per_xact;
extern PGDLLIMPORT int max_predicate_locks_per_relation;
extern PGDLLIMPORT int max_predicate_locks_per_page;

/*
 * A handle used for sharing SERIALIZABLEXACT objects between the participants
 * in a parallel query.
 */
typedef void *SerializableXactHandle;

/*
 * function prototypes
 */

/* housekeeping for shared memory predicate lock structures */
extern void PredicateLockShmemInit(void);
extern Size PredicateLockShmemSize(void);

extern void CheckPointPredicate(void);

/* predicate lock reporting */
extern bool PageIsPredicateLocked(Relation relation, BlockNumber blkno);

/* predicate lock maintenance */
extern Snapshot GetSerializableTransactionSnapshot(Snapshot snapshot);
extern void SetSerializableTransactionSnapshot(Snapshot snapshot,
											   VirtualTransactionId *sourcevxid,
											   int sourcepid);
extern void RegisterPredicateLockingXid(TransactionId xid);
extern void PredicateLockRelation(Relation relation, Snapshot snapshot);
extern void PredicateLockPage(Relation relation, BlockNumber blkno, Snapshot snapshot);
extern void PredicateLockTID(Relation relation, const ItemPointerData *tid, Snapshot snapshot,
							 TransactionId tuple_xid);
extern void PredicateLockPageSplit(Relation relation, BlockNumber oldblkno, BlockNumber newblkno);
extern void PredicateLockPageCombine(Relation relation, BlockNumber oldblkno, BlockNumber newblkno);
extern void TransferPredicateLocksToHeapRelation(Relation relation);
extern void ReleasePredicateLocks(bool isCommit, bool isReadOnlySafe);

/* conflict detection (may also trigger rollback) */
extern bool CheckForSerializableConflictOutNeeded(Relation relation, Snapshot snapshot);
extern void CheckForSerializableConflictOut(Relation relation, TransactionId xid, Snapshot snapshot);
extern void CheckForSerializableConflictIn(Relation relation, const ItemPointerData *tid, BlockNumber blkno);
extern void CheckTableForSerializableConflictIn(Relation relation);

/* final rollback checking */
extern void PreCommit_CheckForSerializationFailure(void);

/* two-phase commit support */
extern void AtPrepare_PredicateLocks(void);
extern void PostPrepare_PredicateLocks(FullTransactionId fxid);
extern void PredicateLockTwoPhaseFinish(FullTransactionId xid, bool isCommit);
extern void predicatelock_twophase_recover(FullTransactionId fxid, uint16 info,
										   void *recdata, uint32 len);

/* parallel query support */
extern SerializableXactHandle ShareSerializableXact(void);
extern void AttachSerializableXact(SerializableXactHandle handle);

#endif							/* PREDICATE_H */
