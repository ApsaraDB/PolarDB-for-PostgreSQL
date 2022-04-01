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
#include "utils/memutils.h"
#include "utils/pg_rusage.h"
#include "utils/resowner.h"

#define implicit2PC_head "_$XC$"

extern bool xc_maintenance_mode;

#define LocalCommitTimestamp    ((TimestampTz) 1)
#define FrozenGlobalTimestamp   ((TimestampTz) 2)
#define GlobalTimestampIsValid(timestamp) (((TimestampTz) (timestamp)) != InvalidGlobalTimestamp)

extern void SetGlobalCommitTimestamp(GlobalTimestamp timestamp);
extern void SetGlobalPrepareTimestamp(GlobalTimestamp timestamp);
extern GlobalTimestamp GetGlobalPrepareTimestamp(void);

extern void AtEOXact_Global(void);

extern bool SavepointDefined(void);

extern void SaveReceivedCommandId(CommandId cid);
extern void SetReceivedCommandId(CommandId cid);
extern CommandId GetReceivedCommandId(void);
extern void ReportCommandIdChange(CommandId cid);
extern bool IsSendCommandId(void);
extern void SetSendCommandId(bool status);

void ResetUsage(void);
void ShowUsage(const char *title);

extern bool IsTransactionState(void);
extern bool IsAbortedTransactionBlockState(void);
extern TransactionId GetTopTransactionId(void);
extern TransactionId GetTopTransactionIdIfAny(void);
extern TransactionId GetCurrentTransactionId(void);


#endif /* POLARDBX_TXNSUP_H */
