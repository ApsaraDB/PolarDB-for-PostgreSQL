/*-------------------------------------------------------------------------
 *
 * invalid_cache.c
 *
 * Copyright (c) 2023, Alibaba Group Holding Limited
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
 *	  external/pg_cron_preload/invalid_cache.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "catalog/namespace.h"
#include "commands/trigger.h"
#include "fmgr.h"
#include "utils/inval.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(cron_job_cache_invalidate);

static void InvalidateJobCache(void);
static Oid	CronJobRelationId(void);

#define CRON_SCHEMA_NAME "cron"
#define JOBS_TABLE_NAME "job"

static Oid	CachedCronJobRelationId = InvalidOid;

/*
 * cron_job_cache_invalidate invalidates the job cache in response to
 * a trigger.
 */
Datum
cron_job_cache_invalidate(PG_FUNCTION_ARGS)
{
	if (!CALLED_AS_TRIGGER(fcinfo))
	{
		ereport(ERROR, (errcode(ERRCODE_E_R_I_E_TRIGGER_PROTOCOL_VIOLATED),
						errmsg("must be called as trigger")));
	}

	InvalidateJobCache();

	PG_RETURN_DATUM(PointerGetDatum(NULL));
}

/*
 * Invalidate job cache ensures the job cache is reloaded on the next
 * iteration of pg_cron.
 */
static void
InvalidateJobCache(void)
{
	HeapTuple	classTuple = NULL;

	classTuple = SearchSysCache1(RELOID, ObjectIdGetDatum(CronJobRelationId()));
	if (HeapTupleIsValid(classTuple))
	{
		CacheInvalidateRelcacheByTuple(classTuple);
		ReleaseSysCache(classTuple);
	}
}

/*
 * CachedCronJobRelationId returns a cached oid of the cron.job relation.
 */
static Oid
CronJobRelationId(void)
{
	if (CachedCronJobRelationId == InvalidOid)
	{
		Oid			cronSchemaId = get_namespace_oid(CRON_SCHEMA_NAME, false);

		CachedCronJobRelationId = get_relname_relid(JOBS_TABLE_NAME, cronSchemaId);
	}

	return CachedCronJobRelationId;
}
