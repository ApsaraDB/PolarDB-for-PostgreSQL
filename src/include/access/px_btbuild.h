/*-------------------------------------------------------------------------
 *
 * px_btbuild.h
 *	  Header file for PolarDB PX btree access method implementation.
 *
 * Copyright (c) 2020, Alibaba Group Holding Limited
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * IDENTIFICATION
 *	  src/include/access/px_btbuild.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PX_BTBUILD_H
#define PX_BTBUILD_H

#include "access/parallel.h"
#include "catalog/pg_am.h"
#include "catalog/pg_class.h"
#include "commands/tablecmds.h"
#include "executor/spi.h"
#include "nodes/execnodes.h"
#include "storage/shm_toc.h"
#include "utils/portal.h"

#define PX_KEY_BTREE_SHARED		UINT64CONST(0xA000000000000006)
#define PX_KEY_QUERY_TEXT		UINT64CONST(0xA000000000000007)
#define PX_KEY_BUFFER_USAGE		UINT64CONST(0xA000000000000008)

/* Make maxalign to 4kB for pxbuild */
#define PXBUILD_MAXIMUM_ALIGNOF	4096
#define PXBUILD_MAXALIGN(LEN)	TYPEALIGN(PXBUILD_MAXIMUM_ALIGNOF, (LEN))


#define PX_RELOPTION_BTBUILD_ON(relation) 									\
	((relation)->rd_options && 												\
	 ((relation)->rd_rel->relkind == RELKIND_INDEX ||						\
	 (relation)->rd_rel->relkind == RELKIND_PARTITIONED_INDEX) &&			\
	 (relation)->rd_rel->relam == BTREE_AM_OID &&							\
	 (((StdRdOptions *)(relation)->rd_options)->px_bt_build_offset != 0 ?	\
	 strcmp((char *)(relation)->rd_options + 								\
	 		((StdRdOptions *)(relation)->rd_options)->px_bt_build_offset,	\
	 		"on") == 0 : false))

#define PX_ENABLE_BTBUILD(index) \
	(px_enable_btbuild && PX_RELOPTION_BTBUILD_ON(index))

#define PX_ENABLE_BTBUILD_CIC_PHASE2(index) \
	(px_enable_btbuild_cic_phase2 && PX_ENABLE_BTBUILD(index))

#define PX_ENABLE_BULK_WRITE(index) \
	(polar_bt_write_page_buffer_size > 0 && PX_ENABLE_BTBUILD(index))

#define BufferConsumed(b) (b->idx == b->icount)
#define PxQueueNext(i) \
	((i + 1) % px_btbuild_queue_size)

#define POLAR_GET_PX_ITUPLE(PX_SHARED, INDEX) \
	((PxIndexTupleBuffer *)((char*)PX_SHARED->addr + \
		(sizeof(PxIndexTupleBuffer) + sizeof(Size) * px_btbuild_batch_size + \
		sizeof(char) * px_btbuild_mem_size * (INDEX_MAX_KEYS + 1)) * INDEX))

#define POLAR_GET_PX_ITUPLE_MEM(ITUPLE) \
	((char *)ITUPLE->addr + sizeof(Size) * px_btbuild_batch_size)

#define TvDiff(tv2, tv1) \
	((tv2.tv_sec - tv1.tv_sec) * 1000 + (tv2.tv_usec - tv1.tv_usec) / 1000)

typedef struct PxIndexTupleBuffer
{
	volatile int	icount;		/* the number of index tuples in this buffer */
	volatile int	idx;		/* the sequence of index tuples in this buffer to be used */
	volatile int	ioffset;	/* memory offset in this buffer */
	Size 			addr[0]; 	/* varlen offset: ituple + mem */
} PxIndexTupleBuffer;

typedef struct PxShared
{
	/*
	 * These fields are not modified during the sort.  They primarily exist
	 * for the benefit of worker processes that need to create BTSpool state
	 * corresponding to that used by the leader.
	 */
	Oid		heaprelid;
	Oid		indexrelid;
	bool	isunique;
	bool	isconcurrent;

	ConditionVariable 	cv;		/* ipc */
	slock_t				mutex;

	volatile bool 		done;	/* all the tuples have been managed */
	volatile int 		bfilled;	/* new PxIndexTupleBuffer to be dealed with */
	volatile int 		bc;		/* buffer consume */
	volatile int 		bp;		/* buffer produce */
	char 				addr[0]; /* varlen */
} PxShared;

typedef struct PxWorkerstate
{
	SPIPlanPtr 			plan;
	Portal 				portal;
	uint64 				processed;
	uint64 				processbuffers;
	MemoryContext 		fetchcontext;

	struct timeval tv1, tv2, tv3, tv4;

	StringInfo 			sql;

	Relation 			heap;
	Relation 			index;
	PxShared 			*shared;	/* in DSM */
	uint64 				waittimes;
	uint64 				last_produce;	/* location from the last produce buffer */
	bool 				fetch_end;	/* all the tuples have been dealed with */
} PxWorkerstate;

typedef struct PxLeaderstate
{
	/* parallel context itself */
	ParallelContext 	*pcxt;
	uint64 				processed;
	uint64 				processbuffers;
	Relation 			heap;
	Relation 			index;
	PxIndexTupleBuffer 	*buffer;
	int 				bufferidx;
	PxShared 			*shared;	/* in DSM */
	Snapshot 			snapshot;
	BufferUsage 		*bufferusage;
	struct timeval 		tv1, tv2, tv3, tv4;
	uint64 				waittimes;
} PxLeaderstate;

typedef struct PxIndexBufferItem
{
	Page page;
	BlockNumber blkno;
} PxIndexBufferItem;

typedef struct PxIndexPageBuffer
{
	int item_len;
	BlockNumber last_written_blkno;
	BlockNumber current_written_blkno;
	PxIndexBufferItem item[0];
} PxIndexPageBuffer;

#ifdef USE_PX

extern void polar_px_bt_build_main(dsm_segment *seg, shm_toc *toc);
extern void polar_px_enable_btbuild_precheck(bool isTopLevel, List *reloptions);

#else

/* Keep compilers quiet in case the build used --disable-gpopt */
static void polar_px_bt_build_main(dsm_segment *seg, shm_toc *toc);
static IndexBuildResult* pxbuild(Relation heap, Relation index, IndexInfo *indexInfo);
static IndexTuple _bt_consume_pxleader(PxLeaderstate *pxleader);
static void _bt_px_alloc_page_buffer(PxIndexPageBuffer **px_index_buffer);
static void _bt_px_blwritepage(void *wstate, Page page, BlockNumber blkno);
static void _bt_px_flush_blpage(void *wstate);

static void
polar_px_bt_build_main(dsm_segment *seg, shm_toc *toc)
{
	elog(ERROR, "no support PX build index in no-px mode");
	/* keep compiler quiet, should not get here */
	pxbuild(NULL, NULL, NULL);
	_bt_consume_pxleader(NULL);
}

static IndexBuildResult*
pxbuild(Relation heap, Relation index, IndexInfo *indexInfo)
{
	elog(ERROR, "no support PX build index in no-px mode");
	/* keep compiler quiet, should not get here */
	polar_px_bt_build_main(NULL, NULL);
	_bt_px_blwritepage(NULL, NULL, 0);
	_bt_px_flush_blpage(NULL);
	polar_px_btbuild_update_pg_class(NULL, NULL);
	return NULL;
}

static IndexTuple
_bt_consume_pxleader(PxLeaderstate *pxleader)
{
	elog(ERROR, "no support PX build index in no-px mode");
	return NULL;
}

static void
_bt_px_blwritepage(void *wstate, Page page, BlockNumber blkno)
{
	/* keep compiler quiet, should not get here */
	_bt_px_alloc_page_buffer(NULL);
	elog(ERROR, "no support PX build index in no-px mode");
}

static void
_bt_px_flush_blpage(void *wstate)
{
	elog(ERROR, "no support PX build index in no-px mode");
}

static void _bt_px_alloc_page_buffer(PxIndexPageBuffer **px_index_buffer)
{
	elog(ERROR, "no support PX build index in no-px mode");
}

static inline void polar_px_enable_btbuild_precheck(bool isTopLevel, List *reloptions)
{
}

#endif

#endif							/* PX_BTBUILD_H */
