/*-------------------------------------------------------------------------
 *
 * txn_util.c
 *
 *      Distributed transaction support
 *
 * Copyright (c) 2021, Alibaba Group Holding Limited
 * Licensed under the Apache License, Version 2.0 (the "License");
 *
 * IDENTIFICATION
 * src/backend/polardbx/transam/txn_util.c
 *
 *-------------------------------------------------------------------------
 */

#include "pgxc/transam/txn_util.h"

#include <sys/time.h>

#include "access/xact.h"
#include "commands/extension.h"
#include "miscadmin.h"
#include "pgxc/transam/txn_coordinator.h"
#include "postmaster/postmaster.h"
#include "storage/proc.h"
#include "utils/builtins.h"
#include "utils/elog.h"
#include "utils/memutils.h"
#include "utils/tqual.h"

static GlobalTimestamp XactGlobalCommitTimestamp = 0;
static GlobalTimestamp XactGlobalPrepareTimestamp = 0;
bool xc_maintenance_mode		   = false;


void
AtEOXact_Global(void)
{
    XactGlobalCommitTimestamp = InvalidGlobalTimestamp;
    XactGlobalPrepareTimestamp = InvalidGlobalTimestamp;
}


void SetGlobalCommitTimestamp(GlobalTimestamp timestamp)
{
    if (timestamp < XactGlobalPrepareTimestamp)
        elog(ERROR, "prepare timestamp should not lag behind commit timestamp: "
                    "prepare " UINT64_FORMAT "commit " UINT64_FORMAT,
             XactGlobalPrepareTimestamp, timestamp);

    XactGlobalCommitTimestamp = timestamp;
}


void SetGlobalPrepareTimestamp(GlobalTimestamp timestamp)
{

    XactGlobalPrepareTimestamp = timestamp;
}

GlobalTimestamp
GetGlobalPrepareTimestamp(void)
{
    return XactGlobalPrepareTimestamp;
}
