/*-------------------------------------------------------------------------
 *
 * logical_clock.c
 *
 * The implementation of hybrid logical clocks.
 * 
 * Copyright (c) 2020, Alibaba Group Holding Limited
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *   http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * 
 *
 * src/backend/distributed_txn/logical_clock.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "distributed_txn/logical_clock.h"

#include "access/mvccvars.h"
#include "access/transam.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "storage/spin.h"
#include "storage/shmem.h"
#include "portability/instr_time.h"


bool DebugLogicalClock = false;


/* Init shared memory structures */
void
LogicalClockShmemInit(void)
{
}

/*
 * Physical time: | 48 bits milliseconds | 16 bits counter |
 */
LogicalTime 
PhysicalClockNow(void)
{
	LogicalTime milli;
	instr_time wall;
	/*
	 * In debug mode, the physical clock will be fixed
	 */
	if (DebugLogicalClock)
	{
		return 0;
	}
	

	/* Use monotonic clock to avoid clock jump back */
	clock_gettime(CLOCK_REALTIME, &wall);
	milli = (LogicalTime)INSTR_TIME_GET_MILLISEC(wall);
	milli <<= 16;
	
	return milli;
}

uint64 LogicalTimeGetMillis(LogicalTime ts)
{
	return ts >> 16;
}

uint64 LogicalTimeGetCounter(LogicalTime ts)
{
	return ts & LogicalClockMask;
}

LogicalTime LogicalClockNow(void)
{
	LogicalTime res;
	LogicalTime newTime = PhysicalClockNow();
	
	SpinLockAcquire(ClockLock);
	ClockState = Max(ClockState, newTime);
	res = ClockState;
	SpinLockRelease(ClockLock);

	Assert(!COMMITSEQNO_IS_SUBTRANS(res));
	return res;
}

LogicalTime LogicalClockTick(void)
{
	LogicalTime res;
	LogicalTime physicalNow = PhysicalClockNow();
	
	SpinLockAcquire(ClockLock);
	ClockState = Max(ClockState, physicalNow);
	/* TODO handle overflow */
	ClockState++;
	res = ClockState;
	SpinLockRelease(ClockLock);
	
	Assert(!COMMITSEQNO_IS_SUBTRANS(res));
	return res;
}

LogicalTime LogicalClockUpdate(LogicalTime newTime)
{
	LogicalTime res;
	
	SpinLockAcquire(ClockLock);
	ClockState = Max(ClockState, newTime);
	res = ClockState;
	SpinLockRelease(ClockLock);
	
	return res;
}

PG_FUNCTION_INFO_V1(logical_clock_now);
PG_FUNCTION_INFO_V1(logical_clock_update);
PG_FUNCTION_INFO_V1(logical_clock_tick);
PG_FUNCTION_INFO_V1(logical_clock_debug_set);

Datum
logical_clock_now(PG_FUNCTION_ARGS)
{
	PG_RETURN_UINT64(LogicalClockNow());
}

Datum
logical_clock_update(PG_FUNCTION_ARGS)
{
	LogicalTime newTime = PG_GETARG_INT64(0);
	LogicalTime res = LogicalClockUpdate(newTime);
	PG_RETURN_UINT64(res);
}

Datum
logical_clock_tick(PG_FUNCTION_ARGS)
{
	LogicalTime res = LogicalClockTick();
	PG_RETURN_UINT64(res);
}

Datum
logical_clock_debug_set(PG_FUNCTION_ARGS)
{
	LogicalTime newTime = PG_GETARG_INT64(0);
	SpinLockAcquire(ClockLock);
	ClockState = newTime;
	SpinLockRelease(ClockLock);
	
	PG_RETURN_NULL();
}
