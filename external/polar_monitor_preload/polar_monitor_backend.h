/*-------------------------------------------------------------------------
 *
 * polar_monitor_backend.h
 * 	  show cpu stats
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
 *    external/polar_monitor_preload/polar_monitor_backend.h
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "storage/ipc.h"
#include "storage/spin.h"

#ifndef POLAR_MONITOR_BACKEND_H
#define POLAR_MONITOR_BACKEND_H

extern int	polar_hash_entry_max;

extern void polar_backend_stat_shmem_startup(void);
extern Size backend_stat_memsize(void);
extern void polar_backend_stat_shmem_shutdown(int code, Datum arg);

#endif							/* POLAR_MONITOR_BACKEND_H */