/*-------------------------------------------------------------------------
 *
 * instrument.c
 *	 functions for instrumentation of plan execution
 *
 *
 * Copyright (c) 2001-2018, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/executor/instrument.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <unistd.h>

#include "executor/instrument.h"

BufferUsage pgBufferUsage;
static BufferUsage save_pgBufferUsage;

static void BufferUsageAdd(BufferUsage *dst, const BufferUsage *add);
static void BufferUsageAccumDiff(BufferUsage *dst,
					 const BufferUsage *add, const BufferUsage *sub);


/* Allocate new instrumentation structure(s) */
Instrumentation *
InstrAlloc(int n, int instrument_options)
{
	Instrumentation *instr;

	/* initialize all fields to zeroes, then modify as needed */
	instr = palloc0(n * sizeof(Instrumentation));
	if (instrument_options & (INSTRUMENT_BUFFERS | INSTRUMENT_TIMER 
								| INSTRUMENT_OPERATION | INSTRUMENT_PX /* POLAR px */))
	{
		int			i;
		bool		need_buffers = (instrument_options & INSTRUMENT_BUFFERS) != 0;
		bool		need_timer = (instrument_options & INSTRUMENT_TIMER) != 0;		

		/* POLAR px */
		bool need_operation = (instrument_options & INSTRUMENT_OPERATION) != 0;
		bool		need_px = (instrument_options & INSTRUMENT_PX) != 0;

		for (i = 0; i < n; i++)
		{
			instr[i].need_bufusage = need_buffers;
			instr[i].need_timer = need_timer;

			/* POLAR px */
			instr[i].need_operation = need_operation;
			instr[i].need_px = need_px;
		}

		/* POLAR px */
		if (need_operation) 
		{
			instr_time	curr_time;
			double double_curr_time;
			INSTR_TIME_SET_CURRENT(curr_time);
			double_curr_time = INSTR_TIME_GET_DOUBLE(curr_time);
			for (i = 0; i < n; i++)
				instr[i].open_time = double_curr_time;
		}
		/* POLAR end*/
	}

	return instr;
}

/* Initialize a pre-allocated instrumentation structure. */
void
InstrInit(Instrumentation *instr, int instrument_options)
{
	memset(instr, 0, sizeof(Instrumentation));
	instr->need_bufusage = (instrument_options & INSTRUMENT_BUFFERS) != 0;
	instr->need_timer = (instrument_options & INSTRUMENT_TIMER) != 0;

	/* POLAR px */
	instr->need_operation = (instrument_options & INSTRUMENT_OPERATION) != 0;

	if (instr->need_operation) 
	{
		instr_time	curr_time;
		INSTR_TIME_SET_CURRENT(curr_time);
		instr->open_time = INSTR_TIME_GET_DOUBLE(curr_time);
	}
	/* POLAR end */
}

/* Entry to a plan node */
void
InstrStartNode(Instrumentation *instr)
{
	if (instr->need_timer)
	{
		if (INSTR_TIME_IS_ZERO(instr->starttime))
			INSTR_TIME_SET_CURRENT(instr->starttime);
		else
			elog(ERROR, "InstrStartNode called twice in a row");
	}

	/* save buffer usage totals at node entry, if needed */
	if (instr->need_bufusage)
		instr->bufusage_start = pgBufferUsage;
}

/* Exit from a plan node */
void
InstrStopNode(Instrumentation *instr, double nTuples)
{
	instr_time	endtime;
	/* POLAR px */
	instr_time	starttime;
	starttime = instr->starttime;
	/* POLAR end */

	/* count the returned tuples */
	instr->tuplecount += nTuples;

	/* let's update the time only if the timer was requested */
	if (instr->need_timer)
	{
		if (INSTR_TIME_IS_ZERO(instr->starttime))
			elog(ERROR, "InstrStopNode called without start");

		INSTR_TIME_SET_CURRENT(endtime);
		INSTR_TIME_ACCUM_DIFF(instr->counter, endtime, instr->starttime);

		INSTR_TIME_SET_ZERO(instr->starttime);
	}

	/* POLAR px */
	if (instr->need_operation 
		&& nTuples > 0
		&& (NULL== instr->sampling_period 
			|| 0 == (int64)instr->tuplecount % *instr->sampling_period)) 
	{
		if (!instr->need_timer) 
			INSTR_TIME_SET_CURRENT(endtime);
		if (instr->running) 
		{
			instr->last_change_time = INSTR_TIME_GET_DOUBLE(endtime);
		}
		else 
		{
			instr->rescan_calls += (instr->last_change_time != 0);
			instr->first_change_time = INSTR_TIME_GET_DOUBLE(endtime);
			instr->last_change_time = instr->first_change_time;
		}
	}
	/* POLAR end */

	/* Add delta of buffer usage since entry to node's totals */
	if (instr->need_bufusage)
		BufferUsageAccumDiff(&instr->bufusage,
							 &pgBufferUsage, &instr->bufusage_start);

	/* Is this the first tuple of this cycle? */
	if (!instr->running)
	{
		instr->running = true;
		instr->firsttuple = INSTR_TIME_GET_DOUBLE(instr->counter);
		/* POLAR px : save this start time as the first start */
		instr->firststart = starttime;
		/* POLAR end */
	}
}

/* POLAR px: Finish a run cycle for a plan node */
void
InstrEndNode(Instrumentation *instr)
{
	if (instr->need_operation) 
	{
		instr_time	curr_time;
		INSTR_TIME_SET_CURRENT(curr_time);
		instr->close_time = INSTR_TIME_GET_DOUBLE(curr_time);
	}
}
/* POLAR end */

/* Finish a run cycle for a plan node */
void
InstrEndLoop(Instrumentation *instr)
{
	double		totaltime;

	/* Skip if nothing has happened, or already shut down */
	if (!instr->running)
		return;

	if (!INSTR_TIME_IS_ZERO(instr->starttime))
		elog(ERROR, "InstrEndLoop called on running node");

	/* Accumulate per-cycle statistics into totals */
	totaltime = INSTR_TIME_GET_DOUBLE(instr->counter);

	instr->startup += instr->firsttuple;
	instr->total += totaltime;
	instr->ntuples += instr->tuplecount;
	instr->nloops += 1;

	/* Reset for next cycle (if any) */
	instr->running = false;
	INSTR_TIME_SET_ZERO(instr->starttime);
	INSTR_TIME_SET_ZERO(instr->counter);
	instr->firsttuple = 0;
	instr->tuplecount = 0;
}

/* aggregate instrumentation information */
void
InstrAggNode(Instrumentation *dst, Instrumentation *add)
{
	if (!dst->running && add->running)
	{
		dst->running = true;
		dst->firsttuple = add->firsttuple;
	}
	else if (dst->running && add->running && dst->firsttuple > add->firsttuple)
		dst->firsttuple = add->firsttuple;

	INSTR_TIME_ADD(dst->counter, add->counter);

	dst->tuplecount += add->tuplecount;
	dst->startup += add->startup;
	dst->total += add->total;
	dst->ntuples += add->ntuples;
	dst->ntuples2 += add->ntuples2;
	dst->nloops += add->nloops;
	dst->nfiltered1 += add->nfiltered1;
	dst->nfiltered2 += add->nfiltered2;

	/* Add delta of buffer usage since entry to node's totals */
	if (dst->need_bufusage)
		BufferUsageAdd(&dst->bufusage, &add->bufusage);
}

/* note current values during parallel executor startup */
void
InstrStartParallelQuery(void)
{
	save_pgBufferUsage = pgBufferUsage;
}

/* report usage after parallel executor shutdown */
void
InstrEndParallelQuery(BufferUsage *result)
{
	memset(result, 0, sizeof(BufferUsage));
	BufferUsageAccumDiff(result, &pgBufferUsage, &save_pgBufferUsage);
}

/* accumulate work done by workers in leader's stats */
void
InstrAccumParallelQuery(BufferUsage *result)
{
	BufferUsageAdd(&pgBufferUsage, result);
}

/* dst += add */
static void
BufferUsageAdd(BufferUsage *dst, const BufferUsage *add)
{
	dst->shared_blks_hit += add->shared_blks_hit;
	dst->shared_blks_read += add->shared_blks_read;
	dst->shared_blks_dirtied += add->shared_blks_dirtied;
	dst->shared_blks_written += add->shared_blks_written;
	dst->local_blks_hit += add->local_blks_hit;
	dst->local_blks_read += add->local_blks_read;
	dst->local_blks_dirtied += add->local_blks_dirtied;
	dst->local_blks_written += add->local_blks_written;
	dst->temp_blks_read += add->temp_blks_read;
	dst->temp_blks_written += add->temp_blks_written;
	INSTR_TIME_ADD(dst->blk_read_time, add->blk_read_time);
	INSTR_TIME_ADD(dst->blk_write_time, add->blk_write_time);
}

/* dst += add - sub */
static void
BufferUsageAccumDiff(BufferUsage *dst,
					 const BufferUsage *add,
					 const BufferUsage *sub)
{
	dst->shared_blks_hit += add->shared_blks_hit - sub->shared_blks_hit;
	dst->shared_blks_read += add->shared_blks_read - sub->shared_blks_read;
	dst->shared_blks_dirtied += add->shared_blks_dirtied - sub->shared_blks_dirtied;
	dst->shared_blks_written += add->shared_blks_written - sub->shared_blks_written;
	dst->local_blks_hit += add->local_blks_hit - sub->local_blks_hit;
	dst->local_blks_read += add->local_blks_read - sub->local_blks_read;
	dst->local_blks_dirtied += add->local_blks_dirtied - sub->local_blks_dirtied;
	dst->local_blks_written += add->local_blks_written - sub->local_blks_written;
	dst->temp_blks_read += add->temp_blks_read - sub->temp_blks_read;
	dst->temp_blks_written += add->temp_blks_written - sub->temp_blks_written;
	INSTR_TIME_ACCUM_DIFF(dst->blk_read_time,
						  add->blk_read_time, sub->blk_read_time);
	INSTR_TIME_ACCUM_DIFF(dst->blk_write_time,
						  add->blk_write_time, sub->blk_write_time);
}