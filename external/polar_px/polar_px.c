/*-------------------------------------------------------------------------
 *
 * polar_px.c
 *	  POLAR_PX install polar_px_workerid function into pg_proc
 *	  for px subselect.
 *
 * Copyright (c) 2020, Alibaba Group Holding Limited
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
 *	  external/polar_px/polar_px.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <unistd.h>

#include "access/hash.h"
#include "catalog/pg_type.h"
#include "executor/executor.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "postmaster/autovacuum.h"
#include "storage/fd.h"
#include "storage/ipc.h"
#include "storage/spin.h"
#include "utils/guc.h"
#include "utils/builtins.h"
#include "postmaster/postmaster.h"

PG_MODULE_MAGIC;

/*--- Functions --- */

void	_PG_init(void);
void	_PG_fini(void);

void
_PG_init(void)
{
	if (!process_shared_preload_libraries_in_progress)
	{
		elog(ERROR, "This module can only be loaded via shared_preload_libraries");
		return;
	}
}

void
_PG_fini(void)
{
}

PG_FUNCTION_INFO_V1(polar_px_workerid);

Datum
polar_px_workerid(PG_FUNCTION_ARGS)
{
	int workerid;
	
	workerid = get_px_workerid();

	PG_RETURN_INT32(workerid);
}

PG_FUNCTION_INFO_V1(polar_px_workerid_funcid);

Datum
polar_px_workerid_funcid(PG_FUNCTION_ARGS)
{
	uint32 funcid;
	
	funcid = get_px_workerid_funcid();

	PG_RETURN_INT32(funcid);
}
