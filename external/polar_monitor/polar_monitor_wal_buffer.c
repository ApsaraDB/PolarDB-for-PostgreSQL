/*-------------------------------------------------------------------------
 *
 * polar_monitor_wal_buffer.c
 *	  Getting statistics of WAL buffer in shared memory.
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
 *	  external/polar_monitor/polar_monitor_wal_buffer.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "funcapi.h"
#include "access/xlog.h"

/*
 * polar_monitor_stat_wal_buffer_counters
 *
 * Monitoring the counters at critical path of WAL buffer.
 */
PG_FUNCTION_INFO_V1(polar_monitor_stat_wal_buffer_counters);
Datum
polar_monitor_stat_wal_buffer_counters(PG_FUNCTION_ARGS)
{
	return polar_get_wal_buffer_stat(fcinfo);
}

PG_FUNCTION_INFO_V1(polar_monitor_stat_wal_io_counters);
Datum
polar_monitor_stat_wal_io_counters(PG_FUNCTION_ARGS)
{
	return polar_get_wal_io_stat(fcinfo);
}

/*
 * polar_monitor_stat_wal_buffer_counters_reset
 *
 * Reset all the WAL buffer stat counters.
 */
PG_FUNCTION_INFO_V1(polar_monitor_stat_wal_buffer_counters_reset);
Datum
polar_monitor_stat_wal_buffer_counters_reset(PG_FUNCTION_ARGS)
{
	polar_reset_wal_buffer_stat();

	PG_RETURN_VOID();
}
