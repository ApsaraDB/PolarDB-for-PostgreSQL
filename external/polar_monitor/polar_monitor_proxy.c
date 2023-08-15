/*-------------------------------------------------------------------------
 *
 * polar_monitor_proxy.c
 *    views of polardb proxy
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
 *    external/polar_monitor/polar_monitor_proxy.c
 *-------------------------------------------------------------------------
 */
#include "polar_monitor.h"

/*
 * POLAR: return the xact split info
 */
PG_FUNCTION_INFO_V1(polar_stat_xact_split_info);
Datum
polar_stat_xact_split_info(PG_FUNCTION_ARGS)
{
#define NUM_XACT_SPLIT_INFO_ELEM 3
	TupleDesc	tupdesc;
	Datum		values[NUM_XACT_SPLIT_INFO_ELEM];
	bool		nulls[NUM_XACT_SPLIT_INFO_ELEM];
	HeapTuple	tuple;
	Datum		result;
	char	   *xids = NULL;
	bool		splittable = false;
	int			i = 0;

	if (RecoveryInProgress())
		elog(ERROR, "recovery is in progress, this function is only available in RW");

	MemSet(nulls, 0, sizeof(nulls));
	MemSet(values, 0, sizeof(values));

	tupdesc = CreateTemplateTupleDesc(NUM_XACT_SPLIT_INFO_ELEM, false);
	TupleDescInitEntry(tupdesc, (AttrNumber) 1, "xids", TEXTOID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 2, "splittable", BOOLOID, -1, 0);
	/* Use int8 rather than LSN, for convenience of test. Might be overflow, but still fine for our test */
	TupleDescInitEntry(tupdesc, (AttrNumber) 3, "lsn", INT8OID, -1, 0);

	xids = polar_xact_split_xact_info();
	splittable = polar_xact_split_splittable();

	if(xids != NULL)
	{
		values[i++] = CStringGetTextDatum(xids);
		pfree(xids);
	}
	else
		nulls[i++] = true;
	values[i++] = BoolGetDatum(splittable);
	values[i++] = Int64GetDatum(GetXLogInsertRecPtr());

	tuple = heap_form_tuple(BlessTupleDesc(tupdesc), values, nulls);
	result = HeapTupleGetDatum(tuple);

	PG_RETURN_DATUM(result);
}

static char *polar_stat_proxy_infos[] = {
	"total",
	"splittable",
	"disablesplit",
	"unsplittable",
	"error",
	"lock",
	"combocid",
	"autoxact",
	"readimplicit",
	"readbeforewrite",
	"readafterwrite",
};

/*
 * POLAR: return the proxy stat info
 */
PG_FUNCTION_INFO_V1(polar_stat_proxy_info);
Datum
polar_stat_proxy_info(PG_FUNCTION_ARGS)
{
#define POLARPROXYSTATSIZE 2
	int				i = 0;
	ReturnSetInfo  *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	TupleDesc		tupdesc;
	MemoryContext	per_query_ctx;
	MemoryContext	oldcontext;
	Tuplestorestate *tupstore;
	Datum		values[POLARPROXYSTATSIZE];
	bool		nulls[POLARPROXYSTATSIZE];

	StaticAssertStmt((sizeof(polar_stat_proxy_infos) / sizeof(char *)) ==
		(sizeof(PolarStat_Proxy) / sizeof(uint64)), "polar_stat_proxy_infos should match PolarStat_Proxy");

	MemSet(nulls, 0, sizeof(nulls));

	tupdesc = CreateTemplateTupleDesc(POLARPROXYSTATSIZE, false);
	TupleDescInitEntry(tupdesc, (AttrNumber) 1, "reason", TEXTOID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 2, "count", INT8OID, -1, 0);

	per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
	oldcontext = MemoryContextSwitchTo(per_query_ctx);

	tupstore = tuplestore_begin_heap(true, false, work_mem);
	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = tupstore;
	rsinfo->setDesc = tupdesc;

	MemoryContextSwitchTo(oldcontext);

	for (i = 0; i < sizeof(polar_stat_proxy_infos) / sizeof(char *); ++i)
	{
		values[0] = CStringGetTextDatum(polar_stat_proxy_infos[i]);
		values[1] = UInt64GetDatum((&polar_stat_proxy->proxy_total)[i]);
		tuplestore_putvalues(tupstore, tupdesc, values, nulls);
	}

	/* clean up and return the tuplestore */
	tuplestore_donestoring(tupstore);

	return (Datum) 0;
}

/*
 * POLAR: reset the proxy stat info
 */
PG_FUNCTION_INFO_V1(polar_stat_reset_proxy_info);
Datum
polar_stat_reset_proxy_info(PG_FUNCTION_ARGS)
{
	MemSet(polar_stat_proxy, 0, sizeof(PolarStat_Proxy));
	PG_RETURN_NULL();
}

/*
 * POLAR: get real pid and virtual pid
 */
PG_FUNCTION_INFO_V1(polar_stat_get_real_pid);
Datum
polar_stat_get_real_pid(PG_FUNCTION_ARGS)
{
	int	pid = PG_ARGISNULL(0) ? MyProcPid : PG_GETARG_INT32(0);
	if (!PG_ARGISNULL(0) && !POLAR_IS_VIRTUAL_PID(pid))
		elog(ERROR, "POLAR: Invalid virtual pid: %d%s",
			pid, POLAR_SHARED_SERVER_RUNNING() ? "" : ", should between (10^7, 2*10^7)");
	PG_RETURN_INT32(polar_pgstat_get_real_pid(pid, 0, false, true));
}

PG_FUNCTION_INFO_V1(polar_stat_get_virtual_pid);
Datum
polar_stat_get_virtual_pid(PG_FUNCTION_ARGS)
{
	int	pid = PG_ARGISNULL(0) ? MyProcPid : PG_GETARG_INT32(0);
	if (!PG_ARGISNULL(0) && !POLAR_IS_REAL_PID(pid))
		elog(ERROR, "POLAR: Invalid real pid: %d%s",
			pid, POLAR_SHARED_SERVER_RUNNING() ? "" : ", should between (0, 10^7)");
	PG_RETURN_INT32(polar_pgstat_get_virtual_pid(pid, true));
}
