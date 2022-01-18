/*-------------------------------------------------------------------------
 *
 * polar_parallel_bgwriter.h
 *		Parallel background writer process manager.
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
 *      src/include/postmaster/polar_parallel_bgwriter.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef PALAR_PARALLEL_BGWRITER_H
#define PALAR_PARALLEL_BGWRITER_H

#include "utils/guc.h"

#define polar_parallel_bgwriter_enabled() \
	(polar_enable_shared_storage_mode && polar_enable_flushlist && polar_enable_parallel_bgwriter)

#define CURRENT_PARALLEL_WORKERS \
	(pg_atomic_read_u32(&polar_parallel_bgwriter_info->current_workers))

#define DOUBLE_GUC_BGWRITER_WORKERS \
	(2 * polar_parallel_bgwriter_workers)

/* Server can have 2 * polar_parallel_bgwriter_workers workers at most */
#define POLAR_MAX_BGWRITER_WORKERS \
	(DOUBLE_GUC_BGWRITER_WORKERS < MAX_NUM_OF_PARALLEL_BGWRITER ? DOUBLE_GUC_BGWRITER_WORKERS : MAX_NUM_OF_PARALLEL_BGWRITER)

/*
 * Only the normal bgwriter access this struct, so we do not need a lock to
 * protect it.
 */
typedef struct ParallelBgwriterHandle
{
	int			slot;
	uint64		generation;
} ParallelBgwriterHandle;

typedef struct ParallelBgwriterInfo
{
	ParallelBgwriterHandle handles[MAX_NUM_OF_PARALLEL_BGWRITER];
	pg_atomic_uint32 current_workers;
} ParallelBgwriterInfo;

extern ParallelBgwriterInfo *polar_parallel_bgwriter_info;

extern void polar_init_parallel_bgwriter(void);
extern Size polar_parallel_bgwriter_shmem_size(void);
extern void	polar_parallel_bgwriter_worker_main(Datum main_arg);
extern void polar_launch_parallel_bgwriter_workers(void);
extern void polar_register_parallel_bgwriter_workers(int workers, bool update_workers);
extern void polar_shutdown_parallel_bgwriter_workers(int workers);
extern void polar_check_parallel_bgwriter_worker(void);

#endif //PALAR_PARALLEL_BGWRITER_H
