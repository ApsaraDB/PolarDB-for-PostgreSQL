/*-------------------------------------------------------------------------
 *
 * polar_monitor.h
 *
 * Copyright (c) 2021, Alibaba Group Holding Limited
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
 *		external/polar_monitor/polar_monitor.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef POLAR_MONIROT_H
#define POLAR_MONIROT_H

#include "postgres.h"

#include "access/htup_details.h"
#include "access/polar_logindex_redo.h"
#include "access/slru.h"
#include "access/xact.h"
#include "access/xlog.h"
#include "catalog/pg_authid.h"
#include "catalog/pg_type.h"
#include "funcapi.h"
#include "libpq/libpq-be.h"
#include "miscadmin.h"
#include "postmaster/bgworker.h"
#include "postmaster/polar_wal_pipeliner.h"
#include "postmaster/postmaster.h"
#include "procstat.h"
#include "replication/slot.h"
#include "replication/polar_cluster_info.h"
#include "replication/walreceiver.h"
#include "replication/walsender.h"
#include "storage/buf_internals.h"
#include "storage/bufmgr.h"
#include "storage/pg_shmem.h"
#include "storage/polar_io_stat.h"
#include "storage/procarray.h"
#include "storage/smgr.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/pg_lsn.h"
#include "utils/relfilenodemap.h"
#include "utils/timestamp.h"

extern Datum polar_stat_process(PG_FUNCTION_ARGS);
extern Datum polar_stat_io_info(PG_FUNCTION_ARGS);
extern Datum polar_io_latency_info(PG_FUNCTION_ARGS);
extern Datum polar_io_read_delta_info(PG_FUNCTION_ARGS);

#endif	
