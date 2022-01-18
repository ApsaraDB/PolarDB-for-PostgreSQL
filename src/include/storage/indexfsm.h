/*-------------------------------------------------------------------------
 *
 * indexfsm.h
 *	  POSTGRES free space map for quickly finding an unused page in index
 *
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/storage/indexfsm.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef INDEXFSM_H_
#define INDEXFSM_H_

#include "storage/block.h"
#include "utils/relcache.h"

/* POLAR */
#include "storage/buf.h"
#include "storage/smgr.h"

extern BlockNumber GetFreeIndexPage(Relation rel);
extern void RecordFreeIndexPage(Relation rel, BlockNumber page);
extern void RecordUsedIndexPage(Relation rel, BlockNumber page);

extern void IndexFreeSpaceMapVacuum(Relation rel);

/* POLAR */
extern Buffer polar_index_add_extra_blocks_and_return_last_buffer(Relation reln, BlockNumber blockNum);


#endif							/* INDEXFSM_H_ */
