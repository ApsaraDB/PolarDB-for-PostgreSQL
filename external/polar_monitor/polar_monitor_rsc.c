/*-------------------------------------------------------------------------
 *
 * polar_monitor_rsc.c
 *	  Show infomation of PolarDB-PG relation size cache (RSC).
 *
 * Copyright (c) 2024, Alibaba Group Holding Limited
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
 *	  external/polar_monitor/polar_monitor_rsc.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/relation.h"
#include "funcapi.h"
#include "storage/lmgr.h"
#include "storage/polar_rsc.h"
#include "utils/builtins.h"
#include "utils/rel.h"

static Datum
rsc_stat_counters_internal(PG_FUNCTION_ARGS,
						   polar_rsc_stat_version_t version)
{
#define RSC_STAT_COUNTER_COLS 8
	Datum		values[RSC_STAT_COUNTER_COLS];
	bool		isnull[RSC_STAT_COUNTER_COLS];
	TupleDesc	tupdesc;

	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	Assert(tupdesc->natts <= RSC_STAT_COUNTER_COLS);

	tupdesc = BlessTupleDesc(tupdesc);

	MemSet(isnull, 0, sizeof(isnull));

	polar_rsc_global_stat_fetch(values, tupdesc->natts, version);

	PG_RETURN_DATUM(HeapTupleGetDatum(heap_form_tuple(tupdesc, values, isnull)));
#undef RSC_STAT_COUNTER_COLS
}

/*
 * polar_monitor_stat_rsc_counters
 *
 * Monitoring the counters at critical path of RSC.
 */
PG_FUNCTION_INFO_V1(polar_monitor_stat_rsc_counters);
Datum
polar_monitor_stat_rsc_counters(PG_FUNCTION_ARGS)
{
	return rsc_stat_counters_internal(fcinfo, POLAR_RSC_STAT_V1);
}

PG_FUNCTION_INFO_V1(polar_monitor_stat_rsc_counters_v2);
Datum
polar_monitor_stat_rsc_counters_v2(PG_FUNCTION_ARGS)
{
	return rsc_stat_counters_internal(fcinfo, POLAR_RSC_STAT_V2);
}

/*
 * polar_monitor_stat_rsc_counters_reset
 *
 * Reset all the RSC stat counters.
 */
PG_FUNCTION_INFO_V1(polar_monitor_stat_rsc_counters_reset);
Datum
polar_monitor_stat_rsc_counters_reset(PG_FUNCTION_ARGS)
{
	polar_rsc_global_stat_reset();

	PG_RETURN_VOID();
}

/*
 * polar_monitor_stat_rsc_check_consistency
 *
 * Get the nblocks value from both RSC and through file system call.
 * Check if they are the same.
 */
PG_FUNCTION_INFO_V1(polar_monitor_stat_rsc_check_consistency);
Datum
polar_monitor_stat_rsc_check_consistency(PG_FUNCTION_ARGS)
{
	Oid			relid = PG_GETARG_OID(0);
	text	   *forkName = PG_GETARG_TEXT_PP(1);
	Relation	rel;
	bool		need_lock;
	bool		consistent;

	rel = try_relation_open(relid, AccessShareLock);
	if (rel == NULL)
	{
		elog(WARNING, "relation with OID %u does not exist", relid);
		PG_RETURN_BOOL(false);
	}

	/*
	 * On primary, the extension lock should be held before checking
	 * consistency of RSC, to avoid getting into the interval where the
	 * relation is extended but the RSC is not yet updated.
	 */
	if (polar_is_primary())
	{
		ForkNumber	forknum = forkname_to_number(text_to_cstring(forkName));

		need_lock = !RELATION_IS_LOCAL(rel);
		if (need_lock)
			LockRelationForkForExtension(rel->rd_locator, forknum, ExclusiveLock);

		consistent = polar_rsc_check_consistent(rel, forknum);

		if (need_lock)
			UnlockRelationForkForExtension(rel->rd_locator, forknum, ExclusiveLock);
	}
	else
		elog(ERROR, "RSC consistency check on replica/standby is not yet supported");

	relation_close(rel, AccessShareLock);

	if (consistent == false)
		elog(ERROR, "RSC consistency check failure");

	PG_RETURN_BOOL(consistent);
}

/*
 * polar_monitor_stat_rsc_entries
 *
 * RSC shared memory pool monitoring, by returning the detailed information
 * of each entry currently in the shared pool, with or without locking.
 */
PG_FUNCTION_INFO_V1(polar_monitor_stat_rsc_entries);
Datum
polar_monitor_stat_rsc_entries(PG_FUNCTION_ARGS)
{
	bool		withlock = PG_GETARG_BOOL(0);

	InitMaterializedSRF(fcinfo, MAT_SRF_USE_EXPECTED_DESC);

	if (!POLAR_RSC_ENABLED())
		ereport(ERROR, errmsg("RSC is not enabled currently"));

	polar_rsc_stat_pool_entries((ReturnSetInfo *) fcinfo->resultinfo, withlock);

	return (Datum) 0;
}

/*
 * polar_monitor_stat_rsc_clear_current_db
 *
 * Clear all RSC entries which belongs to current database.
 */
PG_FUNCTION_INFO_V1(polar_monitor_stat_rsc_clear_current_db);
Datum
polar_monitor_stat_rsc_clear_current_db(PG_FUNCTION_ARGS)
{
	polar_rsc_drop_entries(MyDatabaseId, InvalidOid);

	PG_RETURN_VOID();
}
