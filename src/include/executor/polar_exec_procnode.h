
/*--------------------------------------------------------------------------
 * polar_exec_procnode.h
 *	  header file for polar execProcnode
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
 *      src/include/executor/polar_exec_procnode.h
 *--------------------------------------------------------------------------
 */
#ifndef POLAR_EXEC_PROCNODE_H
#define POLAR_EXEC_PROCNODE_H

#ifdef HAVE_SYS_RESOURCE_H
#include <sys/time.h>
#include <sys/resource.h>
#endif

#ifndef HAVE_GETRUSAGE
#include "rusagestub.h"
#endif

#define RUSAGE_BLOCK_SIZE	512			/* Size of a block for getrusage() */

#include "portability/instr_time.h"

#include "nodes/nodes.h"
#include "nodes/execnodes.h"
#include "storage/polar_lock_stats.h"
#include "storage/polar_io_stat.h"
typedef struct PolarStatSqlCollector
{
    struct rusage rusage_start;
    polar_lwlock_stat lwlock_stat_start;  /* lwlock_stat snapshot in ExecutorStart */   
    polar_regular_lock_stat lock_stats_table_start[LOCKTAG_LAST_TYPE + 1]; /* lock_stat snapshot in ExecutorStart */
    PolarCollectIoStat start_io_stat;
    instr_time 	polar_execute_start;
}PolarStatSqlCollector;

extern int polar_check_instrument_option(Plan *plan, EState *estate);
#endif							/* POLAR_EXEC_PROCNODE_H */