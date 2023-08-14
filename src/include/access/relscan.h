/*-------------------------------------------------------------------------
 *
 * relscan.h
 *	  POSTGRES relation scan descriptor definitions.
 *
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/access/relscan.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef RELSCAN_H
#define RELSCAN_H

#include "access/genam.h"
#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/itup.h"
#include "access/tupdesc.h"
#include "storage/spin.h"

/* POLAR px */
#include "utils/guc.h"

/*
 * Shared state for parallel heap scan.
 *
 * Each backend participating in a parallel heap scan has its own
 * HeapScanDesc in backend-private memory, and those objects all contain
 * a pointer to this structure.  The information here must be sufficient
 * to properly initialize each new HeapScanDesc as workers join the scan,
 * and it must act as a font of block numbers for those workers.
 */
typedef struct ParallelHeapScanDescData
{
	Oid			phs_relid;		/* OID of relation to scan */
	bool		phs_syncscan;	/* report location to syncscan logic? */
	BlockNumber phs_nblocks;	/* # blocks in relation at start of scan */
	slock_t		phs_mutex;		/* mutual exclusion for setting startblock */
	BlockNumber phs_startblock; /* starting block number */
	pg_atomic_uint64 phs_nallocated;	/* number of blocks allocated to
										 * workers so far. */
	bool		phs_snapshot_any;	/* SnapshotAny, not phs_snapshot_data? */
	char		phs_snapshot_data[FLEXIBLE_ARRAY_MEMBER];
} ParallelHeapScanDescData;


/*  POLAR px
 *  share storage extent size:
 *  default is 4MB = 2^22 just like polarFS
 * 	BLCKSZ = 8KB = 2^13
 * 	PXSCAN_UNIT_BIT = 22 - 13 = 9
 * 	one share storage extent contains 2^9 blocks
 */
 
#define PXSCAN_UNIT_BIT (px_scan_unit_bit)
#define PXSCAN_UNIT_SIZE (px_scan_unit_size)
#define PXSCAN_BlockNum2UnitNum(blockno) ((blockno) >>  PXSCAN_UNIT_BIT)
#define PXSCAN_UnitNum2BlockNum(unitno) ((unitno) <<  PXSCAN_UNIT_BIT)
#define PXSCAN_FirstWorker(workerid) ((workerid) == 0)
typedef uint32 PXUnitNumber;

typedef struct PXScanDescData
{
	int 		pxs_plan_id;		/* id for multi seqscan task */
	int 		pxs_worker_id;		/* local segment index for current px workers */
	int 		pxs_total_workers;	/* local segment total count for current px workers */
	BlockNumber	pxs_fetch_end;		/* end page number of per fetch */
	BlockNumber pxs_unit_processed;	/* unit(4M) processed count of current local segment */
	BlockNumber pxs_unit_count;		/* total unit(4M) count need processed */
	int64_t 	pxs_scan_round;		/* multi times scan */
	bool		pxs_adaptive_scan;	/* adaptive scan */
	bool		pxs_prefetch_inner_scan; /* current is a prefetch inner scan */
} PXScanDescData;

typedef PXScanDescData* PXScanDesc;
/* POLAR end */

typedef struct HeapScanDescData
{
	/* scan parameters */
	Relation	rs_rd;			/* heap relation descriptor */
	Snapshot	rs_snapshot;	/* snapshot to see */
	int			rs_nkeys;		/* number of scan keys */
	ScanKey		rs_key;			/* array of scan key descriptors */
	bool		rs_bitmapscan;	/* true if this is really a bitmap scan */
	bool		rs_samplescan;	/* true if this is really a sample scan */
	bool		rs_pageatatime; /* verify visibility page-at-a-time? */
	bool		rs_allow_strat; /* allow or disallow use of access strategy */
	bool		rs_allow_sync;	/* allow or disallow use of syncscan */
	bool		rs_temp_snap;	/* unregister snapshot at scan end? */

	/* state set up at initscan time */
	BlockNumber rs_nblocks;		/* total number of blocks in rel */
	BlockNumber rs_startblock;	/* block # to start at */
	BlockNumber rs_numblocks;	/* max number of blocks to scan */
	/* rs_numblocks is usually InvalidBlockNumber, meaning "scan whole rel" */
	BufferAccessStrategy rs_strategy;	/* access strategy for reads */
	bool		rs_syncscan;	/* report location to syncscan logic? */

	/* scan current state */
	bool		rs_inited;		/* false = scan not init'd yet */
	HeapTupleData rs_ctup;		/* current tuple in scan, if any */
	BlockNumber rs_cblock;		/* current block # in scan, if any */
	Buffer		rs_cbuf;		/* current buffer in scan, if any */
	/* NB: if rs_cbuf is not InvalidBuffer, we hold a pin on that buffer */
	ParallelHeapScanDesc rs_parallel;	/* parallel scan information */

	/* these fields only used in page-at-a-time mode and for bitmap scans */
	int			rs_cindex;		/* current tuple's index in vistuples */
	int			rs_ntuples;		/* number of visible tuples on page */
	OffsetNumber rs_vistuples[MaxHeapTuplesPerPage];	/* their offsets */

	/* POLAR: scan direction value, for bulk read use */
	ScanDirection polar_scan_direction;
	
	/* POLAR: parallel execution scan information*/
	PXScanDesc px_scan;
	/* POLAR end */
}			HeapScanDescData;

/*
 * We use the same IndexScanDescData structure for both amgettuple-based
 * and amgetbitmap-based index scans.  Some fields are only relevant in
 * amgettuple-based scans.
 */
typedef struct IndexScanDescData
{
	/* scan parameters */
	Relation	heapRelation;	/* heap relation descriptor, or NULL */
	Relation	indexRelation;	/* index relation descriptor */
	Snapshot	xs_snapshot;	/* snapshot to see */
	int			numberOfKeys;	/* number of index qualifier conditions */
	int			numberOfOrderBys;	/* number of ordering operators */
	ScanKey		keyData;		/* array of index qualifier descriptors */
	ScanKey		orderByData;	/* array of ordering op descriptors */
	bool		xs_want_itup;	/* caller requests index tuples */
	bool		xs_temp_snap;	/* unregister snapshot at scan end? */

	/* signaling to index AM about killing index tuples */
	bool		kill_prior_tuple;	/* last-returned tuple is dead */
	bool		ignore_killed_tuples;	/* do not return killed entries */
	bool		xactStartedInRecovery;	/* prevents killing/seeing killed
										 * tuples */

	/* index access method's private state */
	void	   *opaque;			/* access-method-specific info */

	/*
	 * In an index-only scan, a successful amgettuple call must fill either
	 * xs_itup (and xs_itupdesc) or xs_hitup (and xs_hitupdesc) to provide the
	 * data returned by the scan.  It can fill both, in which case the heap
	 * format will be used.
	 */
	IndexTuple	xs_itup;		/* index tuple returned by AM */
	TupleDesc	xs_itupdesc;	/* rowtype descriptor of xs_itup */
	HeapTuple	xs_hitup;		/* index data returned by AM, as HeapTuple */
	TupleDesc	xs_hitupdesc;	/* rowtype descriptor of xs_hitup */

	/* xs_ctup/xs_cbuf/xs_recheck are valid after a successful index_getnext */
	HeapTupleData xs_ctup;		/* current heap tuple, if any */
	Buffer		xs_cbuf;		/* current heap buffer in scan, if any */
	/* NB: if xs_cbuf is not InvalidBuffer, we hold a pin on that buffer */
	bool		xs_recheck;		/* T means scan keys must be rechecked */

	/*
	 * When fetching with an ordering operator, the values of the ORDER BY
	 * expressions of the last returned tuple, according to the index.  If
	 * xs_recheckorderby is true, these need to be rechecked just like the
	 * scan keys, and the values returned here are a lower-bound on the actual
	 * values.
	 */
	Datum	   *xs_orderbyvals;
	bool	   *xs_orderbynulls;
	bool		xs_recheckorderby;

	/* state data for traversing HOT chains in index_getnext */
	bool		xs_continue_hot;	/* T if must keep walking HOT chain */

	/* parallel index scan information, in shared memory */
	ParallelIndexScanDesc parallel_scan;

	/* POLAR px: scan desc */
	PXScanDesc px_scan;
	/* POLAR end */
}			IndexScanDescData;

/* Generic structure for parallel scans */
typedef struct ParallelIndexScanDescData
{
	Oid			ps_relid;
	Oid			ps_indexid;
	Size		ps_offset;		/* Offset in bytes of am specific structure */
	char		ps_snapshot_data[FLEXIBLE_ARRAY_MEMBER];
}			ParallelIndexScanDescData;

/* Struct for heap-or-index scans of system tables */
typedef struct SysScanDescData
{
	Relation	heap_rel;		/* catalog being scanned */
	Relation	irel;			/* NULL if doing heap scan */
	HeapScanDesc scan;			/* only valid in heap-scan case */
	IndexScanDesc iscan;		/* only valid in index-scan case */
	Snapshot	snapshot;		/* snapshot to unregister at end of scan */
}			SysScanDescData;

#endif							/* RELSCAN_H */
