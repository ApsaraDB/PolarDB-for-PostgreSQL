/*-------------------------------------------------------------------------
 *
 * polar_flashback_table.h
 *
 *
 * Copyright (c) 2021, Alibaba Group Holding limited
 *
 * src/include/polar_flashback/polar_flashback_table.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef POLAR_FLASHBACK_TABLE_H
#define POLAR_FLASHBACK_TABLE_H

#include "catalog/pg_class.h"
#include "nodes/parsenodes.h"
#include "pgtime.h"
#include "polar_flashback/polar_flashback_log_internal.h"
#include "storage/relfilenode.h"
#include "storage/shm_toc.h"

extern int polar_workers_per_flashback_table;

typedef struct flashback_table_shared_state_t
{
	Oid    old_relid; /* old relation id */
	Oid    new_relid; /* new relation id */

	polar_flog_rec_ptr flog_start_ptr; /* flashback logindex iterator start with */

	XLogRecPtr wal_start_lsn; /* wal lsn start with */
	XLogRecPtr wal_end_lsn; /* wal lsn end with */

	TransactionId curr_xid; /* The current transaction id */
	TransactionId next_xid; /* The next transaction id */

	RelFileNode     rel_filenode; /* The relation file node in the target time */
	BlockNumber nblocks; /* The old blocks number */
	pg_atomic_uint32 next_blkno; /* The next block number to process */
	uint32 next_clog_subdir_no; /* Next sub clog directory */
} flashback_table_shared_state_t;

typedef flashback_table_shared_state_t *flshbak_tbl_shr_st_t;

typedef struct flashback_table_state_t
{
	flshbak_tbl_shr_st_t shared_state;
	Snapshot snapshot; /* The snapshot at the target time */
} flashback_table_state_t;

typedef flashback_table_state_t *flshbak_tbl_st_t;

extern bool polar_can_rel_flashback(Form_pg_class reltup, Oid relid, bool no_persistence_check);
extern void polar_log_cannot_flashback_cause(Form_pg_class reltup, Oid relid, bool no_persistence_check);
extern void polar_exec_flashback_table_stmt(PolarFlashbackTableStmt *stmt);
extern void polar_flashback_pages_woker_main(dsm_segment *seg, shm_toc *toc);

#endif
