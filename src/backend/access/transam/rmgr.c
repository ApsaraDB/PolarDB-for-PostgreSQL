/*
 * rmgr.c
 *
 * Resource managers definition
 *
 * src/backend/access/transam/rmgr.c
 */
#include "postgres.h"

#include "access/clog.h"
#include "access/commit_ts.h"
#include "access/ginxlog.h"
#include "access/gistxlog.h"
#include "access/generic_xlog.h"
#include "access/hash_xlog.h"
#include "access/heapam_xlog.h"
#include "access/brin_xlog.h"
#include "access/multixact.h"
#include "access/nbtxlog.h"
#include "access/spgxlog.h"
#include "access/xact.h"
#include "access/xlog_internal.h"
#include "catalog/storage_xlog.h"
#include "commands/dbcommands_xlog.h"
#include "commands/sequence.h"
#include "commands/tablespace.h"
#include "replication/message.h"
#include "replication/origin.h"
#include "storage/standby.h"
#include "utils/relmapper.h"

/* POLAR */
#include "access/polar_logindex_redo.h"

/* must be kept in sync with RmgrData definition in xlog_internal.h */
#define PG_RMGR(symname,name,redo,polar_idx_save, polar_idx_parse, polar_idx_redo, desc,identify,startup,cleanup,mask) \
	{ name, redo, desc, identify, startup, cleanup, mask },

const RmgrData RmgrTable[RM_MAX_ID + 1] = {
#include "access/rmgrlist.h"
};
