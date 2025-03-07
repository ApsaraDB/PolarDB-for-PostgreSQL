/*-------------------------------------------------------------------------
 *
 * hio.h
 *	  POSTGRES heap access method input/output definitions.
 *
 *
 * Portions Copyright (c) 1996-2022, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/access/hio.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef HIO_H
#define HIO_H

#include "access/htup.h"
#include "storage/buf.h"
#include "utils/relcache.h"

/*
 * state for bulk inserts --- private to heapam.c and hio.c
 *
 * If current_buf isn't InvalidBuffer, then we are holding an extra pin
 * on that buffer.
 *
 * "typedef struct BulkInsertStateData *BulkInsertState" is in heapam.h
 */
typedef struct BulkInsertStateData
{
	BufferAccessStrategy strategy;	/* our BULKWRITE strategy object */
	Buffer		current_buf;	/* current insertion target page */
} BulkInsertStateData;

/* GUCs */
extern PGDLLIMPORT int polar_index_bulk_extend_size;
extern PGDLLIMPORT int polar_heap_bulk_extend_size;

extern void RelationPutHeapTuple(Relation relation, Buffer buffer,
								 HeapTuple tuple, bool token);
extern Buffer RelationGetBufferForTuple(Relation relation, Size len,
										Buffer otherBuffer, int options,
										BulkInsertStateData *bistate,
										Buffer *vmbuffer, Buffer *vmbuffer_other);

extern int	polar_get_bulk_extend_size(BlockNumber first_block, int bulk_extend_size);
extern Buffer polar_index_add_blocks(Relation relation);

#endif							/* HIO_H */
