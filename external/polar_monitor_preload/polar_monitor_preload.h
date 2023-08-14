/*-------------------------------------------------------------------------
 *
 * polar_monitor_preload.h
 *    display some information of polardb
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
 *    external/polar_monitor/polar_monitor_preload.h
 *-------------------------------------------------------------------------
 */

#ifndef __POLAR_MONITOR_H__
#define __POLAR_MONITOR_H__

#define BMSSIZE		(MAXALIGN(offsetof(BackendMemoryStat, stats)) + \
					 MAXALIGN(N_MC_STAT * sizeof(MemoryContextStat)))

/* POLAR: the number of memory contexts for one backend */
#define N_MC_STAT 2048

/*
 * Record the backend memory context
 */
typedef struct MemoryContextStat {
	NameData				name;
	int32					level;
	int32					type;
	bool					is_shared;
	NameData				ident;
	MemoryContextCounters	stat;
} MemoryContextStat;

typedef struct BackendMemoryStat {
	LWLock				*lock;
	int32				pid;
	int32				nContext;
	bool				is_session_pid;
	pg_atomic_uint32	signal_ready;
	pg_atomic_uint32	data_ready;
	MemoryContextStat	stats[FLEXIBLE_ARRAY_MEMBER];
} BackendMemoryStat;

typedef struct MemoryContextIteratorState {
	MemoryContext			context;
	int						level;
} MemoryContextIteratorState;

typedef struct InstanceState {
	int32				iContext;
	BackendMemoryStat	*stat;
} InstanceState;

extern bool	polar_mcxt_view;
extern int	polar_mcxt_timeout; /* default 3 seconds */

extern BackendMemoryStat *memstats;

extern Size getMemstatSize(void);
extern void allocShmem(void);
extern void polar_handle_monitor_hook(PolarHookActionType action, void *args);
extern void polar_set_signal_mctx(void);
extern void polar_check_signal_mctx(void);
extern void polar_ss_check_signal_mctx(void *session);

#endif