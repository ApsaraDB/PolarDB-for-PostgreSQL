/*-------------------------------------------------------------------------
 *
 * datarowstore.c
 *	  Generalized routines for temporary polarx datarow storage.
 *
 * Copyright (c) 2021, Alibaba Group Holding Limited
 * Licensed under the Apache License, Version 2.0 (the "License");
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  contrib/polarx/utils/datarowstore.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <limits.h>

#include "access/htup_details.h"
#include "commands/tablespace.h"
#include "executor/executor.h"
#include "miscadmin.h"
#include "storage/buffile.h"
#include "utils/memutils.h"
#include "utils/resowner.h"
#include "utils/datarowstore.h"


/*
 * Possible states of a Datarowstore object.  These denote the states that
 * persist between calls of Datarowstore routines.
 */
typedef enum
{
	DRSS_INMEM,					/* Tuples still fit in memory */
	DRSS_WRITEFILE,				/* Writing to temp file */
	DRSS_READFILE				/* Reading from temp file */
} DatarowStoreStatus;


typedef struct
{
    int32     msglen;
    char   *msg;
} msg_data;

/*
 * State for a single read pointer.  If we are in state INMEM then all the
 * read pointers' "current" fields denote the read positions.  In state
 * WRITEFILE, the file/offset fields denote the read positions.  In state
 * READFILE, inactive read pointers have valid file/offset, but the active
 * read pointer implicitly has position equal to the temp file's seek position.
 *
 * Special case: if eof_reached is true, then the pointer's read position is
 * implicitly equal to the write position, and current/file/offset aren't
 * maintained.  This way we need not update all the read pointers each time
 * we write.
 */
typedef struct
{
	int			eflags;			/* capability flags */
	bool		eof_reached;	/* read has reached EOF */
	int			current;		/* next array index to read */
	int			file;			/* temp file# */
	off_t		offset;			/* byte offset in file */
} DRSReadPointer;

/*
 * Private state of a Datarowstore operation.
 */
struct Datarowstorestate
{
	DatarowStoreStatus status;		/* enumerated value as shown above */
	int			eflags;			/* capability flags (OR of pointers' flags) */
	bool		backward;		/* store extra length words in file? */
	bool		interXact;		/* keep open through transactions? */
	bool		truncated;		/* datarowstore_trim has removed tuples? */
	int64		availMem;		/* remaining memory available, in bytes */
	int64		allowedMem;		/* total memory allowed, in bytes */
	int64		tuples;			/* number of tuples added */
	BufFile    *myfile;			/* underlying file, or NULL if none */
	MemoryContext context;		/* memory context for holding tuples */
	ResourceOwner resowner;		/* resowner for holding temp files */

	/*
	 * These function pointers decouple the routines that must know what kind
	 * of tuple we are handling from the routines that don't need to know it.
	 * They are set up by the datarowstore_begin_xxx routines.
	 *
	 * (Although datarowstore.c currently only supports heap tuples, I've copied
	 * this part of tuplesort.c so that extension to other kinds of objects
	 * will be easy if it's ever needed.)
	 *
	 * Function to copy a supplied input tuple into palloc'd space. (NB: we
	 * assume that a single pfree() is enough to release the tuple later, so
	 * the representation must be "flat" in one palloc chunk.) state->availMem
	 * must be decreased by the amount of space used.
	 */
	void	   *(*copytup) (Datarowstorestate *state, void *tup);

	/*
	 * Function to write a stored tuple onto tape.  The representation of the
	 * tuple on tape need not be the same as it is in memory; requirements on
	 * the tape representation are given below.  After writing the tuple,
	 * pfree() it, and increase state->availMem by the amount of memory space
	 * thereby released.
	 */
	void		(*writetup) (Datarowstorestate *state, void *tup);

	/*
	 * Function to read a stored tuple from tape back into memory. 'len' is
	 * the already-read length of the stored tuple.  Create and return a
	 * palloc'd copy, and decrease state->availMem by the amount of memory
	 * space consumed.
	 */
	void	   *(*readtup) (Datarowstorestate *state, unsigned int len);

	/*
	 * This array holds pointers to tuples in memory if we are in state INMEM.
	 * In states WRITEFILE and READFILE it's not used.
	 *
	 * When memtupdeleted > 0, the first memtupdeleted pointers are already
	 * released due to a datarowstore_trim() operation, but we haven't expended
	 * the effort to slide the remaining pointers down.  These unused pointers
	 * are set to NULL to catch any invalid accesses.  Note that memtupcount
	 * includes the deleted pointers.
	 */
	void	  **memtuples;		/* array of pointers to palloc'd tuples */
	int			memtupdeleted;	/* the first N slots are currently unused */
	int			memtupcount;	/* number of tuples currently present */
	int			memtupsize;		/* allocated length of memtuples array */
	bool		growmemtuples;	/* memtuples' growth still underway? */

	/*
	 * These variables are used to keep track of the current positions.
	 *
	 * In state WRITEFILE, the current file seek position is the write point;
	 * in state READFILE, the write position is remembered in writepos_xxx.
	 * (The write position is the same as EOF, but since BufFileSeek doesn't
	 * currently implement SEEK_END, we have to remember it explicitly.)
	 */
	DRSReadPointer *readptrs;	/* array of read pointers */
	int			activeptr;		/* index of the active read pointer */
	int			readptrcount;	/* number of pointers currently valid */
	int			readptrsize;	/* allocated length of readptrs array */

	int			writepos_file;	/* file# (valid if READFILE state) */
	off_t		writepos_offset;	/* offset (valid if READFILE state) */
};

#define COPYTUP(state,tup)	((*(state)->copytup) (state, tup))
#define WRITETUP(state,tup) ((*(state)->writetup) (state, tup))
#define READTUP(state,len)	((*(state)->readtup) (state, len))
#define LACKMEM(state)		((state)->availMem < 0)
#define USEMEM(state,amt)	((state)->availMem -= (amt))
#define FREEMEM(state,amt)	((state)->availMem += (amt))

/*--------------------
 *
 * NOTES about on-tape representation of tuples:
 *
 * We require the first "unsigned int" of a stored tuple to be the total size
 * on-tape of the tuple, including itself (so it is never zero).
 * The remainder of the stored tuple
 * may or may not match the in-memory representation of the tuple ---
 * any conversion needed is the job of the writetup and readtup routines.
 *
 * If state->backward is true, then the stored representation of
 * the tuple must be followed by another "unsigned int" that is a copy of the
 * length --- so the total tape space used is actually sizeof(unsigned int)
 * more than the stored length value.  This allows read-backwards.  When
 * state->backward is not set, the write/read routines may omit the extra
 * length word.
 *
 * writetup is expected to write both length words as well as the tuple
 * data.  When readtup is called, the tape is positioned just after the
 * front length word; readtup must read the tuple data and advance past
 * the back length word (if present).
 *
 * The write/read routines can make use of the tuple description data
 * stored in the Datarowstorestate record, if needed. They are also expected
 * to adjust state->availMem by the amount of memory space (not tape space!)
 * released or consumed.  There is no error return from either writetup
 * or readtup; they should ereport() on failure.
 *
 *
 * NOTES about memory consumption calculations:
 *
 * We count space allocated for tuples against the maxKBytes limit,
 * plus the space used by the variable-size array memtuples.
 * Fixed-size space (primarily the BufFile I/O buffer) is not counted.
 * We don't worry about the size of the read pointer array, either.
 *
 * Note that we count actual space used (as shown by GetMemoryChunkSpace)
 * rather than the originally-requested size.  This is important since
 * palloc can add substantial overhead.  It's not a complete answer since
 * we won't count any wasted space in palloc allocation blocks, but it's
 * a lot better than what we were doing before 7.3.
 *
 *--------------------
 */


static Datarowstorestate *datarowstore_begin_common(int eflags,
						bool interXact,
						int maxKBytes);
static void datarowstore_puttuple_common(Datarowstorestate *state, void *tuple);
static void dumptuples(Datarowstorestate *state);
static unsigned int getlen(Datarowstorestate *state, bool eofOK);
static void *copytup_datarow(Datarowstorestate *state, void *tup);
static void writetup_datarow(Datarowstorestate *state, void *tup);
static void *readtup_datarow(Datarowstorestate *state, unsigned int len);

/*
 *		datarowstore_begin_xxx
 *
 * Initialize for a tuple store operation.
 */
static Datarowstorestate *
datarowstore_begin_common(int eflags, bool interXact, int maxKBytes)
{
	Datarowstorestate *state;

	state = (Datarowstorestate *) palloc0(sizeof(Datarowstorestate));

	state->status = DRSS_INMEM;
	state->eflags = eflags;
	state->interXact = interXact;
	state->truncated = false;
	state->allowedMem = maxKBytes * 1024L;
	state->availMem = state->allowedMem;
	state->myfile = NULL;
	state->context = CurrentMemoryContext;
	state->resowner = CurrentResourceOwner;

	state->memtupdeleted = 0;
	state->memtupcount = 0;
	state->tuples = 0;

	/*
	 * Initial size of array must be more than ALLOCSET_SEPARATE_THRESHOLD;
	 * see comments in grow_memtuples().
	 */
	state->memtupsize = Max(16384 / sizeof(void *),
							ALLOCSET_SEPARATE_THRESHOLD / sizeof(void *) + 1);

	state->growmemtuples = true;
	state->memtuples = (void **) palloc(state->memtupsize * sizeof(void *));

	USEMEM(state, GetMemoryChunkSpace(state->memtuples));

	state->activeptr = 0;
	state->readptrcount = 1;
	state->readptrsize = 8;		/* arbitrary */
	state->readptrs = (DRSReadPointer *)
		palloc(state->readptrsize * sizeof(DRSReadPointer));

	state->readptrs[0].eflags = eflags;
	state->readptrs[0].eof_reached = false;
	state->readptrs[0].current = 0;

	return state;
}


/*
 * datarowstore_end
 *
 *	Release resources and clean up.
 */
void
datarowstore_end(Datarowstorestate *state)
{
	int			i;

	if (state->myfile)
		BufFileClose(state->myfile);
	if (state->memtuples)
	{
		for (i = state->memtupdeleted; i < state->memtupcount; i++)
			pfree(state->memtuples[i]);
		pfree(state->memtuples);
	}
	pfree(state->readptrs);
	pfree(state);
}


/*
 * Grow the memtuples[] array, if possible within our memory constraint.  We
 * must not exceed INT_MAX tuples in memory or the caller-provided memory
 * limit.  Return true if we were able to enlarge the array, false if not.
 *
 * Normally, at each increment we double the size of the array.  When doing
 * that would exceed a limit, we attempt one last, smaller increase (and then
 * clear the growmemtuples flag so we don't try any more).  That allows us to
 * use memory as fully as permitted; sticking to the pure doubling rule could
 * result in almost half going unused.  Because availMem moves around with
 * tuple addition/removal, we need some rule to prevent making repeated small
 * increases in memtupsize, which would just be useless thrashing.  The
 * growmemtuples flag accomplishes that and also prevents useless
 * recalculations in this function.
 */
static bool
grow_memtuples(Datarowstorestate *state)
{
	int			newmemtupsize;
	int			memtupsize = state->memtupsize;
	int64		memNowUsed = state->allowedMem - state->availMem;

	/* Forget it if we've already maxed out memtuples, per comment above */
	if (!state->growmemtuples)
		return false;

	/* Select new value of memtupsize */
	if (memNowUsed <= state->availMem)
	{
		/*
		 * We've used no more than half of allowedMem; double our usage,
		 * clamping at INT_MAX tuples.
		 */
		if (memtupsize < INT_MAX / 2)
			newmemtupsize = memtupsize * 2;
		else
		{
			newmemtupsize = INT_MAX;
			state->growmemtuples = false;
		}
	}
	else
	{
		/*
		 * This will be the last increment of memtupsize.  Abandon doubling
		 * strategy and instead increase as much as we safely can.
		 *
		 * To stay within allowedMem, we can't increase memtupsize by more
		 * than availMem / sizeof(void *) elements. In practice, we want to
		 * increase it by considerably less, because we need to leave some
		 * space for the tuples to which the new array slots will refer.  We
		 * assume the new tuples will be about the same size as the tuples
		 * we've already seen, and thus we can extrapolate from the space
		 * consumption so far to estimate an appropriate new size for the
		 * memtuples array.  The optimal value might be higher or lower than
		 * this estimate, but it's hard to know that in advance.  We again
		 * clamp at INT_MAX tuples.
		 *
		 * This calculation is safe against enlarging the array so much that
		 * LACKMEM becomes true, because the memory currently used includes
		 * the present array; thus, there would be enough allowedMem for the
		 * new array elements even if no other memory were currently used.
		 *
		 * We do the arithmetic in float8, because otherwise the product of
		 * memtupsize and allowedMem could overflow.  Any inaccuracy in the
		 * result should be insignificant; but even if we computed a
		 * completely insane result, the checks below will prevent anything
		 * really bad from happening.
		 */
		double		grow_ratio;

		grow_ratio = (double) state->allowedMem / (double) memNowUsed;
		if (memtupsize * grow_ratio < INT_MAX)
			newmemtupsize = (int) (memtupsize * grow_ratio);
		else
			newmemtupsize = INT_MAX;

		/* We won't make any further enlargement attempts */
		state->growmemtuples = false;
	}

	/* Must enlarge array by at least one element, else report failure */
	if (newmemtupsize <= memtupsize)
		goto noalloc;

	/*
	 * On a 32-bit machine, allowedMem could exceed MaxAllocHugeSize.  Clamp
	 * to ensure our request won't be rejected.  Note that we can easily
	 * exhaust address space before facing this outcome.  (This is presently
	 * impossible due to guc.c's MAX_KILOBYTES limitation on work_mem, but
	 * don't rely on that at this distance.)
	 */
	if ((Size) newmemtupsize >= MaxAllocHugeSize / sizeof(void *))
	{
		newmemtupsize = (int) (MaxAllocHugeSize / sizeof(void *));
		state->growmemtuples = false;	/* can't grow any more */
	}

	/*
	 * We need to be sure that we do not cause LACKMEM to become true, else
	 * the space management algorithm will go nuts.  The code above should
	 * never generate a dangerous request, but to be safe, check explicitly
	 * that the array growth fits within availMem.  (We could still cause
	 * LACKMEM if the memory chunk overhead associated with the memtuples
	 * array were to increase.  That shouldn't happen because we chose the
	 * initial array size large enough to ensure that palloc will be treating
	 * both old and new arrays as separate chunks.  But we'll check LACKMEM
	 * explicitly below just in case.)
	 */
	if (state->availMem < (int64) ((newmemtupsize - memtupsize) * sizeof(void *)))
		goto noalloc;

	/* OK, do it */
	FREEMEM(state, GetMemoryChunkSpace(state->memtuples));
	state->memtupsize = newmemtupsize;
	state->memtuples = (void **)
		repalloc_huge(state->memtuples,
					  state->memtupsize * sizeof(void *));
	USEMEM(state, GetMemoryChunkSpace(state->memtuples));
	if (LACKMEM(state))
		elog(ERROR, "unexpected out-of-memory situation in datarowstore");
	return true;

noalloc:
	/* If for any reason we didn't realloc, shut off future attempts */
	state->growmemtuples = false;
	return false;
}

/*
 * Accept one tuple and append it to the datarowstore.
 *
 * Note that the input tuple is always copied; the caller need not save it.
 *
 * If the active read pointer is currently "at EOF", it remains so (the read
 * pointer implicitly advances along with the write pointer); otherwise the
 * read pointer is unchanged.  Non-active read pointers do not move, which
 * means they are certain to not be "at EOF" immediately after puttuple.
 * This curious-seeming behavior is for the convenience of nodeMaterial.c and
 * nodeCtescan.c, which would otherwise need to do extra pointer repositioning
 * steps.
 *
 * datarowstore_puttupleslot() is a convenience routine to collect data from
 * a TupleTableSlot without an extra copy operation.
 */
void
datarowstore_putdatarow(Datarowstorestate *state,
						RemoteDataRow datarow)
{
    MemoryContext oldcxt = MemoryContextSwitchTo(state->context);
    RemoteDataRow tuple = NULL;
    int len = datarow->msglen;

    /* if we already have datarow make a copy */
    tuple = (RemoteDataRow) palloc(sizeof(RemoteDataRowData) + len);
    tuple->msgnode = datarow->msgnode;
    tuple->msglen = len;
    memcpy(tuple->msg, datarow->msg, len);

    USEMEM(state, GetMemoryChunkSpace(tuple));

    datarowstore_puttuple_common(state, (void *) tuple);

    MemoryContextSwitchTo(oldcxt);
}


static void
datarowstore_puttuple_common(Datarowstorestate *state, void *tuple)
{
	DRSReadPointer *readptr;
	int			i;
	ResourceOwner oldowner;

	state->tuples++;

	switch (state->status)
	{
		case DRSS_INMEM:

			/*
			 * Update read pointers as needed; see API spec above.
			 */
			readptr = state->readptrs;
			for (i = 0; i < state->readptrcount; readptr++, i++)
			{
				if (readptr->eof_reached && i != state->activeptr)
				{
					readptr->eof_reached = false;
					readptr->current = state->memtupcount;
				}
			}

			/*
			 * Grow the array as needed.  Note that we try to grow the array
			 * when there is still one free slot remaining --- if we fail,
			 * there'll still be room to store the incoming tuple, and then
			 * we'll switch to tape-based operation.
			 */
			if (state->memtupcount >= state->memtupsize - 1)
			{
				(void) grow_memtuples(state);
				Assert(state->memtupcount < state->memtupsize);
			}

			/* Stash the tuple in the in-memory array */
			state->memtuples[state->memtupcount++] = tuple;

			/*
			 * Done if we still fit in available memory and have array slots.
			 */
			if (state->memtupcount < state->memtupsize && !LACKMEM(state))
				return;

			/*
			 * Nope; time to switch to tape-based operation.  Make sure that
			 * the temp file(s) are created in suitable temp tablespaces.
			 */
			PrepareTempTablespaces();

			/* associate the file with the store's resource owner */
			oldowner = CurrentResourceOwner;
			CurrentResourceOwner = state->resowner;

			state->myfile = BufFileCreateTemp(state->interXact);

			CurrentResourceOwner = oldowner;

			/*
			 * Freeze the decision about whether trailing length words will be
			 * used.  We can't change this choice once data is on tape, even
			 * though callers might drop the requirement.
			 */
			state->backward = (state->eflags & EXEC_FLAG_BACKWARD) != 0;
			state->status = DRSS_WRITEFILE;
			dumptuples(state);
			break;
		case DRSS_WRITEFILE:

			/*
			 * Update read pointers as needed; see API spec above. Note:
			 * BufFileTell is quite cheap, so not worth trying to avoid
			 * multiple calls.
			 */
			readptr = state->readptrs;
			for (i = 0; i < state->readptrcount; readptr++, i++)
			{
				if (readptr->eof_reached && i != state->activeptr)
				{
					readptr->eof_reached = false;
					BufFileTell(state->myfile,
								&readptr->file,
								&readptr->offset);
				}
			}

			WRITETUP(state, tuple);
			break;
		case DRSS_READFILE:

			/*
			 * Switch from reading to writing.
			 */
			if (!state->readptrs[state->activeptr].eof_reached)
				BufFileTell(state->myfile,
							&state->readptrs[state->activeptr].file,
							&state->readptrs[state->activeptr].offset);
			if (BufFileSeek(state->myfile,
							state->writepos_file, state->writepos_offset,
							SEEK_SET) != 0)
				ereport(ERROR,
						(errcode_for_file_access(),
						 errmsg("could not seek in datarowstore temporary file: %m")));
			state->status = DRSS_WRITEFILE;

			/*
			 * Update read pointers as needed; see API spec above.
			 */
			readptr = state->readptrs;
			for (i = 0; i < state->readptrcount; readptr++, i++)
			{
				if (readptr->eof_reached && i != state->activeptr)
				{
					readptr->eof_reached = false;
					readptr->file = state->writepos_file;
					readptr->offset = state->writepos_offset;
				}
			}

			WRITETUP(state, tuple);
			break;
		default:
			elog(ERROR, "invalid datarowstore state");
			break;
	}
}

/*
 * Fetch the next tuple in either forward or back direction.
 * Returns NULL if no more tuples.  If should_free is set, the
 * caller must pfree the returned tuple when done with it.
 *
 * Backward scan is only allowed if randomAccess was set true or
 * EXEC_FLAG_BACKWARD was specified to datarowstore_set_eflags().
 */
static void *
datarowstore_gettuple(Datarowstorestate *state, bool forward,
					bool *should_free)
{
	DRSReadPointer *readptr = &state->readptrs[state->activeptr];
	unsigned int tuplen;
	void	   *tup;

	Assert(forward || (readptr->eflags & EXEC_FLAG_BACKWARD));

	switch (state->status)
	{
		case DRSS_INMEM:
			*should_free = false;
			if (forward)
			{
				if (readptr->eof_reached)
					return NULL;
				if (readptr->current < state->memtupcount)
				{
					/* We have another tuple, so return it */
					return state->memtuples[readptr->current++];
				}
				readptr->eof_reached = true;
				return NULL;
			}
			else
			{
				/*
				 * if all tuples are fetched already then we return last
				 * tuple, else tuple before last returned.
				 */
				if (readptr->eof_reached)
				{
					readptr->current = state->memtupcount;
					readptr->eof_reached = false;
				}
				else
				{
					if (readptr->current <= state->memtupdeleted)
					{
						Assert(!state->truncated);
						return NULL;
					}
					readptr->current--; /* last returned tuple */
				}
				if (readptr->current <= state->memtupdeleted)
				{
					Assert(!state->truncated);
					return NULL;
				}
				return state->memtuples[readptr->current - 1];
			}
			break;

		case DRSS_WRITEFILE:
			/* Skip state change if we'll just return NULL */
			if (readptr->eof_reached && forward)
				return NULL;

			/*
			 * Switch from writing to reading.
			 */
			BufFileTell(state->myfile,
						&state->writepos_file, &state->writepos_offset);
			if (!readptr->eof_reached)
				if (BufFileSeek(state->myfile,
								readptr->file, readptr->offset,
								SEEK_SET) != 0)
					ereport(ERROR,
							(errcode_for_file_access(),
							 errmsg("could not seek in datarowstore temporary file: %m")));
			state->status = DRSS_READFILE;
			/* FALLTHROUGH */

		case DRSS_READFILE:
			*should_free = true;
			if (forward)
			{
				if ((tuplen = getlen(state, true)) != 0)
				{
					tup = READTUP(state, tuplen);
					return tup;
				}
				else
				{
					readptr->eof_reached = true;
					return NULL;
				}
			}

			/*
			 * Backward.
			 *
			 * if all tuples are fetched already then we return last tuple,
			 * else tuple before last returned.
			 *
			 * Back up to fetch previously-returned tuple's ending length
			 * word. If seek fails, assume we are at start of file.
			 */
			if (BufFileSeek(state->myfile, 0, -(long) sizeof(unsigned int),
							SEEK_CUR) != 0)
			{
				/* even a failed backwards fetch gets you out of eof state */
				readptr->eof_reached = false;
				Assert(!state->truncated);
				return NULL;
			}
			tuplen = getlen(state, false);

			if (readptr->eof_reached)
			{
				readptr->eof_reached = false;
				/* We will return the tuple returned before returning NULL */
			}
			else
			{
				/*
				 * Back up to get ending length word of tuple before it.
				 */
				if (BufFileSeek(state->myfile, 0,
								-(long) (tuplen + 2 * sizeof(unsigned int)),
								SEEK_CUR) != 0)
				{
					/*
					 * If that fails, presumably the prev tuple is the first
					 * in the file.  Back up so that it becomes next to read
					 * in forward direction (not obviously right, but that is
					 * what in-memory case does).
					 */
					if (BufFileSeek(state->myfile, 0,
									-(long) (tuplen + sizeof(unsigned int)),
									SEEK_CUR) != 0)
						ereport(ERROR,
								(errcode_for_file_access(),
								 errmsg("could not seek in datarowstore temporary file: %m")));
					Assert(!state->truncated);
					return NULL;
				}
				tuplen = getlen(state, false);
			}

			/*
			 * Now we have the length of the prior tuple, back up and read it.
			 * Note: READTUP expects we are positioned after the initial
			 * length word of the tuple, so back up to that point.
			 */
			if (BufFileSeek(state->myfile, 0,
							-(long) tuplen,
							SEEK_CUR) != 0)
				ereport(ERROR,
						(errcode_for_file_access(),
						 errmsg("could not seek in datarowstore temporary file: %m")));
			tup = READTUP(state, tuplen);
			return tup;

		default:
			elog(ERROR, "invalid datarowstore state");
			return NULL;		/* keep compiler quiet */
	}
}

/*
 * datarowstore_gettupleslot - exported function to fetch a MinimalTuple
 *
 * If successful, put tuple in slot and return true; else, clear the slot
 * and return false.
 *
 * If copy is true, the slot receives a copied tuple (allocated in current
 * memory context) that will stay valid regardless of future manipulations of
 * the datarowstore's state.  If copy is false, the slot may just receive a
 * pointer to a tuple held within the datarowstore.  The latter is more
 * efficient but the slot contents may be corrupted if additional writes to
 * the datarowstore occur.  (If using datarowstore_trim, see comments therein.)
 */
bool
datarowstore_getdatarow(Datarowstorestate *state, bool forward,
						bool copy, RemoteDataRow *datarow)
{
    bool		should_free;
    RemoteDataRow datarow_store = (RemoteDataRow) datarowstore_gettuple(state, forward, &should_free);

	if (datarow_store)
    {
        if (copy && !should_free)
        {
            RemoteDataRow dup = (RemoteDataRow) palloc(sizeof(RemoteDataRowData) + datarow_store->msglen);
            dup->msgnode = datarow_store->msgnode;
            dup->msglen = datarow_store->msglen;
            memcpy(dup->msg, datarow_store->msg, datarow_store->msglen);
            if(datarow)
                *datarow = dup;
        }
        else
        {
            if(datarow)
                *datarow = datarow_store;
        }
        return true;
    }

    return false;
}


/*
 * dumptuples - remove tuples from memory and write to tape
 *
 * As a side effect, we must convert each read pointer's position from
 * "current" to file/offset format.  But eof_reached pointers don't
 * need to change state.
 */
static void
dumptuples(Datarowstorestate *state)
{
	int			i;

	for (i = state->memtupdeleted;; i++)
	{
		DRSReadPointer *readptr = state->readptrs;
		int			j;

		for (j = 0; j < state->readptrcount; readptr++, j++)
		{
			if (i == readptr->current && !readptr->eof_reached)
				BufFileTell(state->myfile,
							&readptr->file, &readptr->offset);
		}
		if (i >= state->memtupcount)
			break;
		WRITETUP(state, state->memtuples[i]);
	}
	state->memtupdeleted = 0;
	state->memtupcount = 0;
}


/*
 * Tape interface routines
 */

static unsigned int
getlen(Datarowstorestate *state, bool eofOK)
{
	unsigned int len;
	size_t		nbytes;

	nbytes = BufFileRead(state->myfile, (void *) &len, sizeof(len));
	if (nbytes == sizeof(len))
		return len;
	if (nbytes != 0 || !eofOK)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not read from datarowstore temporary file: %m")));
	return 0;
}


/*
 * Routines to support Datarow tuple format, used for exchange between nodes
 * as well as send data to client
 */
Datarowstorestate *
datarowstore_begin_datarow(bool interXact, int maxKBytes)
{
    Datarowstorestate *state;

    state = datarowstore_begin_common(0, interXact, maxKBytes);

    state->copytup = copytup_datarow;
    state->writetup = writetup_datarow;
    state->readtup = readtup_datarow;

    return state;
}


/*
 * Do we need this at all?
 */
static void *
copytup_datarow(Datarowstorestate *state, void *tup)
{
    Assert(false);
    return NULL;
}

static void
writetup_datarow(Datarowstorestate *state, void *tup)
{
    RemoteDataRow tuple = (RemoteDataRow) tup;

    /* the part of the MinimalTuple we'll write: */
    char       *tupbody = tuple->msg;
    unsigned int tupbodylen = tuple->msglen;

    /* total on-disk footprint: */
    unsigned int tuplen = tupbodylen + sizeof(int) + sizeof(tuple->msgnode);

    if (BufFileWrite(state->myfile, (void *) &tuplen,
                     sizeof(int)) != sizeof(int))
        elog(ERROR, "write failed");
    if (BufFileWrite(state->myfile, (void *) &tuple->msgnode,
                     sizeof(tuple->msgnode)) != sizeof(tuple->msgnode))
        elog(ERROR, "write failed");
    if (BufFileWrite(state->myfile, (void *) tupbody,
                     tupbodylen) != (size_t) tupbodylen)
        elog(ERROR, "write failed");
    if (state->backward)        /* need trailing length word? */
        if (BufFileWrite(state->myfile, (void *) &tuplen,
                         sizeof(tuplen)) != sizeof(tuplen))
            elog(ERROR, "write failed");

    FREEMEM(state, GetMemoryChunkSpace(tuple));
    pfree(tuple);
}

static void *
readtup_datarow(Datarowstorestate *state, unsigned int len)
{
    RemoteDataRow tuple = (RemoteDataRow) palloc(len);
    unsigned int tupbodylen = len - sizeof(int) - sizeof(tuple->msgnode);

    USEMEM(state, GetMemoryChunkSpace(tuple));
    /* read in the tuple proper */
    tuple->msglen = tupbodylen;
    if (BufFileRead(state->myfile, (void *) &tuple->msgnode,
                    sizeof(tuple->msgnode)) != sizeof(tuple->msgnode))
        elog(ERROR, "unexpected end of data");
    if (BufFileRead(state->myfile, (void *) tuple->msg,
                    tupbodylen) != (size_t) tupbodylen)
        elog(ERROR, "unexpected end of data");
    if (state->backward)        /* need trailing length word? */
        if (BufFileRead(state->myfile, (void *) &len,
                        sizeof(len)) != sizeof(len))
            elog(ERROR, "unexpected end of data");
    return (void *) tuple;
}
