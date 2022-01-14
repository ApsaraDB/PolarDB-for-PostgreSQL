/*---------------------------------------------------------------------------
 * rmgrlist.h
 *
 * The resource manager list is kept in its own source file for possible
 * use by automatic tools.  The exact representation of a rmgr is determined
 * by the PG_RMGR macro, which is not defined in this file; it can be
 * defined by the caller for special purposes.
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/access/rmgrlist.h
 *---------------------------------------------------------------------------
 */

/* there is deliberately not an #ifndef RMGRLIST_H here */

/*
 * List of resource manager entries.  Note that order of entries defines the
 * numerical values of each rmgr's ID, which is stored in WAL records.  New
 * entries should be added at the end, to avoid changing IDs of existing
 * entries.
 *
 * Changes to this list possibly need an XLOG_PAGE_MAGIC bump.
 */

/* symbol name, textual name, redo, polar_idx_save, polar_idx_parse, polar_idx_redo, desc, identify, startup, cleanup */
PG_RMGR(RM_XLOG_ID, "XLOG", xlog_redo, polar_xlog_idx_save, polar_xlog_idx_parse, polar_xlog_idx_redo, xlog_desc, xlog_identify, NULL, NULL, NULL)
PG_RMGR(RM_XACT_ID, "Transaction", xact_redo, NULL,NULL, NULL, xact_desc, xact_identify, NULL, NULL, NULL)
PG_RMGR(RM_SMGR_ID, "Storage", smgr_redo, NULL, polar_storage_idx_parse, NULL, smgr_desc, smgr_identify, NULL, NULL, NULL)
PG_RMGR(RM_CLOG_ID, "CLOG", clog_redo, NULL, NULL, NULL, clog_desc, clog_identify, NULL, NULL, NULL)
PG_RMGR(RM_DBASE_ID, "Database", dbase_redo, NULL, NULL, NULL, dbase_desc, dbase_identify, NULL, NULL, NULL)
PG_RMGR(RM_TBLSPC_ID, "Tablespace", tblspc_redo, NULL, NULL, NULL, tblspc_desc, tblspc_identify, NULL, NULL, NULL)
PG_RMGR(RM_MULTIXACT_ID, "MultiXact", multixact_redo, NULL, NULL, NULL, multixact_desc, multixact_identify, NULL, NULL, NULL)
PG_RMGR(RM_RELMAP_ID, "RelMap", relmap_redo, NULL, NULL, NULL, relmap_desc, relmap_identify, NULL, NULL, NULL)
PG_RMGR(RM_STANDBY_ID, "Standby", standby_redo, NULL, NULL, NULL, standby_desc, standby_identify, NULL, NULL, NULL)
PG_RMGR(RM_HEAP2_ID, "Heap2", heap2_redo, polar_heap2_idx_save, polar_heap2_idx_parse, polar_heap2_idx_redo, heap2_desc, heap2_identify, NULL, NULL, heap_mask)
PG_RMGR(RM_HEAP_ID, "Heap", heap_redo, polar_heap_idx_save, polar_heap_idx_parse, polar_heap_idx_redo, heap_desc, heap_identify, NULL, NULL, heap_mask)
PG_RMGR(RM_BTREE_ID, "Btree", btree_redo, polar_btree_idx_save, polar_btree_idx_parse, polar_btree_idx_redo, btree_desc, btree_identify, NULL, NULL, btree_mask)
PG_RMGR(RM_HASH_ID, "Hash", hash_redo, polar_hash_idx_save, polar_hash_idx_parse, polar_hash_idx_redo, hash_desc, hash_identify, NULL, NULL, hash_mask)
PG_RMGR(RM_GIN_ID, "Gin", gin_redo, polar_gin_idx_save, polar_gin_idx_parse, polar_gin_idx_redo, gin_desc, gin_identify, gin_xlog_startup, gin_xlog_cleanup, gin_mask)
PG_RMGR(RM_GIST_ID, "Gist", gist_redo, polar_gist_idx_save, polar_gist_idx_parse, polar_gist_idx_redo, gist_desc, gist_identify, gist_xlog_startup, gist_xlog_cleanup, gist_mask)
PG_RMGR(RM_SEQ_ID, "Sequence", seq_redo, polar_seq_idx_save, polar_seq_idx_parse, polar_seq_idx_redo, seq_desc, seq_identify, NULL, NULL, seq_mask)
PG_RMGR(RM_SPGIST_ID, "SPGist", spg_redo, polar_spg_idx_save, polar_spg_idx_parse, polar_spg_idx_redo, spg_desc, spg_identify, spg_xlog_startup, spg_xlog_cleanup, spg_mask)
PG_RMGR(RM_BRIN_ID, "BRIN", brin_redo, polar_brin_idx_save, polar_brin_idx_parse, polar_brin_idx_redo, brin_desc, brin_identify, NULL, NULL, brin_mask)
PG_RMGR(RM_COMMIT_TS_ID, "CommitTs", commit_ts_redo, NULL, NULL, NULL, commit_ts_desc, commit_ts_identify, NULL, NULL, NULL)
PG_RMGR(RM_REPLORIGIN_ID, "ReplicationOrigin", replorigin_redo, NULL, NULL, NULL, replorigin_desc, replorigin_identify, NULL, NULL, NULL)
PG_RMGR(RM_GENERIC_ID, "Generic", generic_redo, polar_generic_idx_save, polar_generic_idx_parse, polar_generic_idx_redo, generic_desc, generic_identify, NULL, NULL, generic_mask)
PG_RMGR(RM_LOGICALMSG_ID, "LogicalMessage", logicalmsg_redo, NULL, NULL, NULL, logicalmsg_desc, logicalmsg_identify, NULL, NULL, NULL)
