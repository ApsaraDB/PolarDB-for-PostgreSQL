/*--------------------------------------------------------------------------
 *
 * px_copy.h
 *	 Definitions and API functions for pxcopy.c
 *	 These are functions that are used by the backend
 *	 COPY command in Greenplum Database.
 *
 * Portions Copyright (c) 2005-2008, Greenplum inc
 * Portions Copyright (c) 2012-Present Pivotal Software, Inc.
 *
 *
 * IDENTIFICATION
 *	    src/include/px/px_copy.h
 *
 *--------------------------------------------------------------------------
 */

#ifndef PXCOPY_H
#define PXCOPY_H

#include "lib/stringinfo.h"
#include "px/px_gang.h"

#define COPYOUT_CHUNK_SIZE 16 * 1024

struct PxDispatcherState;
struct CopyStateData;

typedef struct PxCopy
{
	int			total_segs;		/* total number of segments in px */
	bool		copy_in;		/* direction: true for COPY FROM false for COPY TO */

	StringInfoData	copy_out_buf;/* holds a chunk of data from the database */

	List		*seglist;    	/* segs that currently take part in copy.
								 * for copy out, once a segment gave away all it's
								 * data rows, it is taken out of the list */
	struct PxDispatcherState *dispatcherState;
} PxCopy;



/* global function declarations */
extern PxCopy *makePxCopy(struct CopyStateData *cstate, bool copy_in);
extern void pxCopyStart(PxCopy *pxCopy, CopyStmt *stmt, int file_encoding);
extern void pxCopySendDataToAll(PxCopy *c, const char *buffer, int nbytes);
extern void pxCopySendData(PxCopy *c, int target_seg, const char *buffer, int nbytes);
extern bool pxCopyGetData(PxCopy *c, bool cancel, uint64 *rows_processed);
extern void pxCopyAbort(PxCopy *c);
extern void pxCopyEnd(PxCopy *c,
		   int64 *total_rows_completed_p,
		   int64 *total_rows_rejected_p);

#endif   /* PXCOPY_H */