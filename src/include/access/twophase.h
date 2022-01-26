/*-------------------------------------------------------------------------
 *
 * twophase.h
 *	  Two-phase-commit related declarations.
 *
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/access/twophase.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef TWOPHASE_H
#define TWOPHASE_H

#include "access/xlogdefs.h"
#include "access/xact.h"
#include "datatype/timestamp.h"
#include "storage/lock.h"


#ifdef POLARDB_X
typedef struct
{
	char gid[GIDSIZE];
} GidLookupTag;

typedef struct
{
	GidLookupTag tag;
	TransactionId localxid;
} GidLookupEnt;
#endif

/*
 * GlobalTransactionData is defined in twophase.c; other places have no
 * business knowing the internal definition.
 */
typedef struct GlobalTransactionData *GlobalTransaction;

/* GUC variable */
extern PGDLLIMPORT int max_prepared_xacts;

extern Size TwoPhaseShmemSize(void);
extern void TwoPhaseShmemInit(void);

extern void AtAbort_Twophase(void);
extern void PostPrepare_Twophase(void);

extern PGPROC *TwoPhaseGetDummyProc(TransactionId xid);
extern BackendId TwoPhaseGetDummyBackendId(TransactionId xid);

extern GlobalTransaction MarkAsPreparing(TransactionId xid, const char *gid,
				TimestampTz prepared_at,
				Oid owner, Oid databaseid);

extern void StartPrepare(GlobalTransaction gxact);
extern void EndPrepare(GlobalTransaction gxact);
extern bool StandbyTransactionIdIsPrepared(TransactionId xid);

#ifdef ENABLE_DISTRIBUTED_TRANSACTION
extern void EndGlobalPrepare(GlobalTransaction gxact);
#endif

extern TransactionId PrescanPreparedTransactions(TransactionId **xids_p,
							int *nxids_p);
extern void ParsePrepareRecord(uint8 info, char *xlrec,
				   xl_xact_parsed_prepare *parsed);
extern void StandbyRecoverPreparedTransactions(void);
extern void RecoverPreparedTransactions(void);

extern void CheckPointTwoPhase(XLogRecPtr redo_horizon);

extern void FinishPreparedTransaction(const char *gid, bool isCommit);

extern void PrepareRedoAdd(char *buf, XLogRecPtr start_lsn,
			   XLogRecPtr end_lsn, RepOriginId origin_id);
#ifdef POLARDB_X
extern void PrepareRedoRemove(TransactionId xid, bool giveWarning, bool shouldClear);
#else
extern void PrepareRedoRemove(TransactionId xid, bool giveWarning);
#endif
extern void restoreTwoPhaseData(void);
#ifdef POLARDB_X
extern void SetTwoPhaseXactCommitTimestamp(char* gid, GlobalTimestamp timestamp);
extern void RecordTwoPhaseXactCommitTimestamp(char* gid, GlobalTimestamp timestamp);
extern void UpdateTwoPhaseFileCommitTimestamp(char* gid, GlobalTimestamp timestamp);
extern Size TwoPhaseGidHashTableShmemSize(void);
extern void GidHashTabShmemInit(void);
extern bool CleanUpTwoPhaseFile(char* gid);
extern void RefreshGidHashMap(bool need_lock);
extern TransactionId GetTwoPhaseXactLocalxid(char* gid);
extern bool RemoveFromGidHashTab(char* gid, TransactionId xid);
extern bool AddToGidHashTab(char* gid, TransactionId xid);
extern GlobalTransaction GetXactByXid(TransactionId xid, bool need_lock);
extern bool CompareGXact(char* gid, TransactionId xid, bool need_lock);
#endif
#endif							/* TWOPHASE_H */
