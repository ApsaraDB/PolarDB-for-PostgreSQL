/*-------------------------------------------------------------------------
 *
 * instrument.h
 *	  definitions for run-time statistics collection
 *
 *
 * Copyright (c) 2001-2018, PostgreSQL Global Development Group
 *
 * src/include/executor/instrument.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef INSTRUMENT_H
#define INSTRUMENT_H

#include "portability/instr_time.h"


typedef struct BufferUsage
{
	long		shared_blks_hit;	/* # of shared buffer hits */
	long		shared_blks_read;	/* # of shared disk blocks read */
	long		shared_blks_dirtied;	/* # of shared blocks dirtied */
	long		shared_blks_written;	/* # of shared disk blocks written */
	long		local_blks_hit; /* # of local buffer hits */
	long		local_blks_read;	/* # of local disk blocks read */
	long		local_blks_dirtied; /* # of shared blocks dirtied */
	long		local_blks_written; /* # of local disk blocks written */
	long		temp_blks_read; /* # of temp blocks read */
	long		temp_blks_written;	/* # of temp blocks written */
	instr_time	blk_read_time;	/* time spent reading */
	instr_time	blk_write_time; /* time spent writing */
} BufferUsage;

/* Flag bits included in InstrAlloc's instrument_options bitmask */
typedef enum InstrumentOption
{
	INSTRUMENT_NONE = 0,
	INSTRUMENT_TIMER = 1 << 0,	/* needs timer (and row counts) */
	INSTRUMENT_BUFFERS = 1 << 1,	/* needs buffer usage */
	INSTRUMENT_ROWS = 1 << 2,	/* needs row count */
	/* POLAR px */
	INSTRUMENT_MEMORY_DETAIL = 1 << 28,	/* needs detailed memory accounting */
	INSTRUMENT_OPERATION = 1 << 29,	/* needs operation statistics */
	INSTRUMENT_PX = 1 << 30,	/* needs px statistics */
	/* 
	 * POLAR: needs row count for polardb, to avoid conflict with the 
	 * community version, add the flag from the high bits. 
	 */
	INSTRUMENT_POLAR_PLAN = 1 << 31,
	/* POLAR End */
	INSTRUMENT_ALL = PG_INT32_MAX
} InstrumentOption;

typedef struct Instrumentation
{
	/* Parameters set at node creation: */
	bool		need_timer;		/* true if we need timer data */
	bool		need_bufusage;	/* true if we need buffer usage data */
	bool		need_operation;		/* true if we need timer data */
	/* Info about current plan cycle: */
	bool		running;		/* true if we've completed first tuple */
	instr_time	starttime;		/* Start time of current iteration of node */
	instr_time	counter;		/* Accumulated runtime for this node */
	double		firsttuple;		/* Time for first tuple of this cycle */
	double		tuplecount;		/* Tuples emitted so far this cycle */
	BufferUsage bufusage_start; /* Buffer usage at start */

	/* POLAR px: Info about current node: */
	int 		*sampling_period;	/* sampling period */
	int			rescan_calls;		/* rescan calls of this node */
	double		open_time;  		/* open time of this node */
	double		first_change_time;  /* first row output time of this node */
	double		last_change_time;  	/* last row output time of this node */
	double		close_time;  		/* open time of this node */
	/* POLAR end */

	/* Accumulated statistics across all completed cycles: */
	double		startup;		/* Total startup time (in seconds) */
	double		total;			/* Total total time (in seconds) */
	double		ntuples;		/* Total tuples produced */
	double		ntuples2;		/* Secondary node-specific tuple counter */
	double		nloops;			/* # of run cycles for this node */
	double		nfiltered1;		/* # tuples removed by scanqual or joinqual */
	double		nfiltered2;		/* # tuples removed by "other" quals */
	BufferUsage bufusage;		/* Total buffer usage */

	/* POLAR px */
	bool 		need_px;        /* PX: true if we need px statistics */
	double		execmemused;	/* PX: executor memory used (bytes) */
	double		workmemused;	/* PX: work_mem actually used (bytes) */
	double		workmemwanted;	/* PX: work_mem to avoid scratch i/o (bytes) */
	instr_time	firststart;		/* PX: Start time of first iteration of node */
	bool		workfileCreated;	/* TRUE if workfiles are created in this
									 * node */
	int			numPartScanned; /* Number of part tables scanned */
	const char *sortMethod;		/* PX: Type of sort */
	const char *sortSpaceType;	/* PX: Sort space type (Memory / Disk) */
	long		sortSpaceUsed;	/* PX: Memory / Disk used by sort(KBytes) */
	struct PxExplain_NodeSummary *pxNodeSummary;	/* stats from all qExecs */
	/* POLAR end */
} Instrumentation;

typedef struct WorkerInstrumentation
{
	int			num_workers;	/* # of structures that follow */
	Instrumentation instrument[FLEXIBLE_ARRAY_MEMBER];
} WorkerInstrumentation;

extern PGDLLIMPORT BufferUsage pgBufferUsage;

extern Instrumentation *InstrAlloc(int n, int instrument_options);
extern void InstrInit(Instrumentation *instr, int instrument_options);
extern void InstrStartNode(Instrumentation *instr);
extern void InstrStopNode(Instrumentation *instr, double nTuples);
extern void InstrEndLoop(Instrumentation *instr);
extern void InstrEndNode(Instrumentation *instr);
extern void InstrAggNode(Instrumentation *dst, Instrumentation *add);
extern void InstrStartParallelQuery(void);
extern void InstrEndParallelQuery(BufferUsage *result);
extern void InstrAccumParallelQuery(BufferUsage *result);

#endif							/* INSTRUMENT_H */
