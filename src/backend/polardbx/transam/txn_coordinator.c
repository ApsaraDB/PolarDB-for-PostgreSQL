/*-------------------------------------------------------------------------
 *
 * execRemoteTrans.c
 *
 *      Distributed transaction coordination
 *
 *
 * Copyright (c) 2021, Alibaba Group Holding Limited
 * Licensed under the Apache License, Version 2.0 (the "License");
 * Portions Copyright (c) 2012-2014, TransLattice, Inc.
 * Portions Copyright (c) 1996-2011, PostgreSQL Global Development Group
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 * IDENTIFICATION
 * src/backend/polardbx/transam/txn_coordinator.c
 *
 *-------------------------------------------------------------------------
 */

#include "pgxc/transam/txn_coordinator.h"

#include <time.h>
#include <unistd.h>

#include "access/transam.h"
#include "access/twophase.h"
#include "access/xact.h"
#include "commands/tablecmds.h"
#include "miscadmin.h"
#include "nodes/parsenodes.h"
#include "distributed_txn/txn_timestamp.h"
#include "pgxc/transam/txn_util.h"
#include "storage/ipc.h"
#include "storage/lwlock.h"
#include "storage/proc.h"
#include "utils/builtins.h"
#include "utils/elog.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/pg_rusage.h"
#include "utils/tqual.h"

LocalTwoPhaseState g_twophase_state;


#ifdef POLARDB_X
bool enable_twophase_recover_debug_print = false;
#endif


void InitLocalTwoPhaseState(void)
{
    int participants_capacity;
    g_twophase_state.is_readonly = false;
    g_twophase_state.gid = (char *)MemoryContextAllocZero(TopMemoryContext, GIDSIZE);
    g_twophase_state.state = TWO_PHASE_INITIALTRANS;
    g_twophase_state.coord_index = g_twophase_state.datanode_index = 0;
    g_twophase_state.connections_num = 0;
    g_twophase_state.response_operation = OTHER_OPERATIONS;

    g_twophase_state.coord_state = (ConnTransState *)MemoryContextAllocZero(
            TopMemoryContext, POLARX_MAX_COORDINATOR_NUMBER * sizeof(ConnTransState));
    g_twophase_state.datanode_state = (ConnTransState *)MemoryContextAllocZero(
            TopMemoryContext, POLARX_MAX_DATANODE_NUMBER * sizeof(ConnTransState));
    /* since participates conclude nodename and  ","*/
    participants_capacity =
            (NAMEDATALEN + 1) * (POLARX_MAX_DATANODE_NUMBER + POLARX_MAX_COORDINATOR_NUMBER);
    g_twophase_state.participants =
            (char *)MemoryContextAllocZero(TopMemoryContext, participants_capacity);
    g_twophase_state.connections = (AllConnNodeInfo *)MemoryContextAllocZero(
            TopMemoryContext,
            (POLARX_MAX_DATANODE_NUMBER + POLARX_MAX_COORDINATOR_NUMBER) * sizeof(AllConnNodeInfo));
}
