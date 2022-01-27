/*-------------------------------------------------------------------------
 *
 * execRemoteTrans.h
 *
 *          Distributed transaction support
 *
 *
 * Portions Copyright (c) 2012-2014, TransLattice, Inc.
 * Portions Copyright (c) 1996-2011, PostgreSQL Global Development Group
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 * src/include/pgxc/transam/txn_coordinator.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef POLARDBX_TXNSUP_H
#define POLARDBX_TXNSUP_H

#include "postgres.h"

#include "datatype/timestamp.h"
#include "nodes/nodes.h"
#include "pg_config_manual.h"
#include "pgxc/pgxc.h"
#include "pgxc/pgxcnode.h"
#include "utils/memutils.h"
#include "utils/pg_rusage.h"
#include "utils/resowner.h"

#define implicit2PC_head "_$XC$"

extern int  delay_before_acquire_committs;
extern int  delay_after_acquire_committs;
extern bool is_distri_report;
extern bool enable_distri_debug;
extern bool enable_statistic;
extern bool xc_maintenance_mode;

#define LocalCommitTimestamp    ((TimestampTz) 1)
#define FrozenGlobalTimestamp   ((TimestampTz) 2)
#define GlobalTimestampIsValid(timestamp) (((TimestampTz) (timestamp)) != InvalidGlobalTimestamp)
#define CommitTimestampIsLocal(timestamp) (((TimestampTz) (timestamp))  == LocalCommitTimestamp)
#define GlobalTimestampIsFrozen(timestamp) (((TimestampTz) (timestamp)) == FrozenGlobalTimestamp)

extern void SetGTMxactStartTimestamp(TimestampTz);
extern TimestampTz GetCurrentGTMStartTimestamp(void);
extern void		   SetCurrentGTMDeltaTimestamp(TimestampTz timestamp);

extern void SetGlobalCommitTimestamp(GlobalTimestamp timestamp);
extern GlobalTimestamp GetGlobalCommitTimestamp(void);
extern void SetGlobalPrepareTimestamp(GlobalTimestamp timestamp);
extern GlobalTimestamp GetGlobalPrepareTimestamp(void);

extern void SetLocalCommitTimestamp(GlobalTimestamp timestamp);
extern GlobalTimestamp GetLocalCommitTimestamp(void);
extern void SetLocalPrepareTimestamp(GlobalTimestamp timestamp);
extern GlobalTimestamp GetLocalPrepareTimestamp(void);
extern void AtEOXact_Global(void);

extern void AssignGlobalXid(void);
extern char *GetGlobalXid(void);
extern char *GetGlobalXidNoCheck(void);
extern void SetGlobalXid(const char *globalXidString);
extern uint64 GetGlobalXidVersion(void);

extern bool InSubTransaction(void);
extern bool InPlpgsqlFunc(void);
extern bool NeedBeginTxn(void);
extern bool NeedBeginSubTxn(void);
extern void SetNodeBeginTxn(Oid nodeoid);
extern void SetNodeBeginSubTxn(Oid nodeoid);
extern bool NodeHasBeginTxn(Oid nodeoid);
extern bool NodeHasBeginSubTxn(Oid nodeoid);
extern void SetTopXactNeedBeginTxn(void);
extern void SetEnterPlpgsqlFunc(void);
extern void SetExitPlpgsqlFunc(void);
extern bool SavepointDefined(void);
extern bool ExecDDLWithoutAcquireXid(Node* parsetree);
extern bool IsTransactionIdle(void);

extern MemoryContext GetCurrentTransactionContext(void);
extern ResourceOwner GetCurrentTransactionResourceOwner(void);

extern const char * GetPrepareGID(void);
extern void ClearPrepareGID(void);
extern char * GetImplicit2PCGID(const char *head, bool localWrite);

extern bool IsXidImplicit(const char *xid);
extern void SaveReceivedCommandId(CommandId cid);
extern void SetReceivedCommandId(CommandId cid);
extern CommandId GetReceivedCommandId(void);
extern void ReportCommandIdChange(CommandId cid);
extern bool IsSendCommandId(void);
extern void SetSendCommandId(bool status);

void ResetUsageCommon(struct rusage *save_r, struct timeval *save_t);
void ResetUsage(void);
void ShowUsageCommon(const char *title, struct rusage *save_r, struct timeval *save_t);
void ShowUsage(const char *title);

extern bool IsTransactionState(void);
extern bool IsAbortedTransactionBlockState(void);
extern TransactionId GetTopTransactionId(void);
extern TransactionId GetTopTransactionIdIfAny(void);
extern TransactionId GetCurrentTransactionId(void);


#endif /* POLARDBX_TXNSUP_H */
