/*-------------------------------------------------------------------------
 *
 * logical_clock.h
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/distributed_txn/logical_clock.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef LOGICAL_CLOCK_H
#define LOGICAL_CLOCK_H

#include "c.h"

typedef uint64 LogicalTime;

extern bool DebugLogicalClock;

LogicalTime PhysicalClockNow(void);
LogicalTime LogicalClockNow(void);
LogicalTime LogicalClockTick(void);
LogicalTime LogicalClockUpdate(LogicalTime);

uint64 LogicalTimeGetMillis(LogicalTime);
uint64 LogicalTimeGetCounter(LogicalTime);

extern void LogicalClockShmemInit(void);

#define LogicalClockMask   UINT64CONST(0xFFFF) 
#define ClockLock (&ShmemVariableCache->ts_lock)
#define ClockState (ShmemVariableCache->maxCommitTs)
#define ToLogicalClock(ts) ((ts) & LogicalClockMask)
#define LOGICALTIME_FORMAT "("UINT64_FORMAT","UINT64_FORMAT")"
#define LOGICALTIME_STRING(ts) LogicalTimeGetMillis(ts), LogicalTimeGetCounter(ts)

/*
 * Datum logical_clock_now();
 * Datum logical_clock_update(newtime);
 * Datum logical_clock_tick();
*/

#endif /* LOGICAL_CLOCK_H */
