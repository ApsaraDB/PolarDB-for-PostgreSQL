/*-------------------------------------------------------------------------
 *
 * procstat.h
 *
 *	Copyright (c) 2020, Alibaba.inc
 *
 * IDENTIFICATION
 *		external/polar_monitor/polar_monitor.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef POLAR_MONIROT_H
#define POLAR_MONIROT_H

#include "postgres.h"

#include "access/htup_details.h"
#include "access/slru.h"
#include "access/xact.h"
#include "access/xlog.h"
#include "catalog/pg_authid.h"
#include "catalog/pg_type.h"
#include "funcapi.h"
#include "pgstat.h"
#include "libpq/libpq-be.h"
#include "miscadmin.h"
#include "postmaster/bgworker.h"
#include "procstat.h"
#include "replication/slot.h"
#include "replication/walreceiver.h"
#include "replication/walsender.h"
#include "storage/buf_internals.h"
#include "storage/bufmgr.h"
#include "storage/pg_shmem.h"
#include "storage/polar_io_stat.h"
#include "storage/procarray.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/pg_lsn.h"
#include "utils/timestamp.h"

extern Datum polar_stat_process(PG_FUNCTION_ARGS);
extern Datum polar_stat_io_info(PG_FUNCTION_ARGS);
extern Datum polar_io_latency_info(PG_FUNCTION_ARGS);

#endif	
