/*-------------------------------------------------------------------------
 *
 * polar_monitor_preload.c
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
 *    external/polar_monitor/polar_monitor_preload.c
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "funcapi.h"
#include "miscadmin.h"
#include "storage/buf_internals.h"
#include "utils/guc.h"
#include "storage/ipc.h"
#include "polar_monitor_backend.h"
#include "polar_monitor_lock.h"
#include "polar_monitor_network.h"
#include "polar_monitor_preload.h"
#include "polar_monitor_plan.h"
#include "pgstat.h"

static shmem_startup_hook_type prev_shmem_startup_hook = NULL;
static polar_postmaster_child_init_register prev_polar_stat_hook = NULL;
void polar_monitor_exit_work(void);

void		_PG_init(void);
void		_PG_fini(void);

PG_MODULE_MAGIC;

void
polar_handle_monitor_hook(PolarHookActionType action, void *args)
{
	switch (action)
	{
		case POLAR_SET_SIGNAL_MCTX:
			polar_set_signal_mctx();
			break;

		case POLAR_CHECK_SIGNAL_MCTX:
			polar_check_signal_mctx();
			break;

		case POLAR_SET_LOGGING_PLAN_OF_RUNNING_QUERY:
			HandleLogCurrentPlanInterrupt();
			break;

		case POLAR_CHECK_LOGGING_PLAN_OF_RUNNING_QUERY:
			ProcessLogCurrentPlanInterrupt();
			break;

		/* POLAR: Shared Server */
		case POLAR_SS_CHECK_SIGNAL_MCTX:
			polar_ss_check_signal_mctx(args);
			break;

		default:
			break;
	}
}

void 
polar_monitor_exit_work(void)
{
	if (prev_polar_stat_hook)
		prev_polar_stat_hook();

	on_shmem_exit(polar_backend_stat_shmem_shutdown, (Datum) 0);
}

void
allocShmem(void)
{
	bool found;

	if (prev_shmem_startup_hook)
		prev_shmem_startup_hook();

	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);

	memstats = ShmemInitStruct("external/polar_memstat",
							   getMemstatSize(),
							   &found);

	if (!found)
	{
		memstats->pid = -1;
		memstats->lock = &(GetNamedLWLockTranche("polar_memstat"))->lock;
		pg_atomic_init_u32(&memstats->data_ready, 0);
		pg_atomic_init_u32(&memstats->signal_ready, 0);
	}

	LWLockRelease(AddinShmemInitLock);

	polar_lock_stat_shmem_startup();
	polar_network_stat_shmem_startup();
	polar_backend_stat_shmem_startup();

}

void
_PG_init(void)
{
	if (!process_shared_preload_libraries_in_progress)
		return;

	DefineCustomBoolVariable("polar_monitor.mcxt_view",
								"on/off the polar_mcxt_view_hook",
								NULL,
								&polar_mcxt_view,
								true,
								PGC_SIGHUP,
								0,
								NULL,
								NULL,
								NULL);

	DefineCustomIntVariable("polar_monitor.mcxt_timeout",
								"configure for the polar_mcxt_view_hook waitting time",
								NULL,
								&polar_mcxt_timeout,
								1000,
								10,
								100000,
								PGC_USERSET,
								0,
								NULL,
								NULL,
								NULL);

	/*
	 * Define (or redefine) custom GUC variables.
	 */
	DefineCustomIntVariable("polar_monitor.backend_type_max",
							"Sets the maximum number of backend type.",
							NULL,
							&polar_hash_entry_max,
							1024,
							10,
							INT_MAX,
							PGC_POSTMASTER,
							0,
							NULL,
							NULL,
							NULL);

	RequestAddinShmemSpace(getMemstatSize());
	RequestNamedLWLockTranche("polar_memstat", 1);
	RequestNamedLWLockTranche("polar_stat_backend", 1);
	RequestAddinShmemSpace(lwlock_stat_shmem_size());
	RequestAddinShmemSpace(lock_stat_shmem_size());
	RequestAddinShmemSpace(network_stat_shmem_size());
	RequestAddinShmemSpace(backend_stat_memsize());

	prev_shmem_startup_hook = shmem_startup_hook;
	shmem_startup_hook = allocShmem;

	polar_monitor_hook = polar_handle_monitor_hook;

	prev_polar_stat_hook = polar_stat_hook;
	polar_stat_hook = polar_monitor_exit_work;
}

void
_PG_fini(void)
{
	shmem_startup_hook = prev_shmem_startup_hook;
	polar_stat_hook = prev_polar_stat_hook;
}