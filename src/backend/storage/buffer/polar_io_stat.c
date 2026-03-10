/*-------------------------------------------------------------------------
 *
 * polar_io_stat.c
 *	  Collect IO stat
 *
 * Copyright (c) 2025, Alibaba Group Holding Limited
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
 *	  src/backend/storage/buffer/polar_io_stat.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "storage/bufmgr.h"
#include "storage/polar_io_stat.h"
#include "storage/shmem.h"

PolarWriteCombineStat *polar_write_combine_stat;

Size
polar_write_combine_stat_shmem_size(void)
{
	return sizeof(PolarWriteCombineStat);
}

void
polar_write_combine_stat_shmem_init(void)
{
	bool		found = false;

	polar_write_combine_stat = ShmemInitStruct("Polar Write Combine Stat",
											   sizeof(PolarWriteCombineStat), &found);

	if (!found)
		memset(polar_write_combine_stat, 0, sizeof(PolarWriteCombineStat));
}

void
polar_write_combine_stat_count_time(int nblocks, instr_time io_start)
{
	instr_time	io_time;

	Assert(nblocks > 0 && nblocks <= MAX_BUFFERS_TO_WRITE_BY);

	polar_write_combine_stat->counts[nblocks - 1]++;

	INSTR_TIME_SET_CURRENT(io_time);
	INSTR_TIME_SUBTRACT(io_time, io_start);
	polar_write_combine_stat->times[nblocks - 1] += INSTR_TIME_GET_MICROSEC(io_time);
}
