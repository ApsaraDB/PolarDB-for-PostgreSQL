/*-------------------------------------------------------------------------
 *
 * polar_monitor_shmem.c
 *    views of polardb share memroy
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
 *    external/polar_monitor/polar_monitor_shmem.c
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/htup_details.h"
#include "catalog/pg_type.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "storage/lwlock.h"
#include "storage/pg_shmem.h"
#include "storage/polar_shmem.h"
#include "storage/shmem.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/pg_lsn.h"

#define SHMEM_TYPE_NORMAL		"normal"
#define SHMEM_TYPE_PERSISTED	"persisted"

/*
 * record context for stat shared memory.
 */
typedef struct
{
	char name[NAMEDATALEN];
	char type[NAMEDATALEN];
	uint64 size;
} SharedMemoryRec;

/*
 * Function context for stat shared memory.
 */
typedef struct
{
	TupleDesc   tupdesc;
	SharedMemoryRec *record;
} SharedMemoryContext;

static long normal_shmem_num_entries(void);
static long persisted_shmem_num_entries(void);
static void build_normal_shmem_record(SharedMemoryContext *shmem_ctx, int *index);
static void build_persisted_shmem_record(SharedMemoryContext *shmem_ctx, int *index);

PG_FUNCTION_INFO_V1(polar_show_shared_memory);

Datum
polar_show_shared_memory(PG_FUNCTION_ARGS)
{
	Datum		result;
	HeapTuple	tuple;
	FuncCallContext	*func_ctx;
	SharedMemoryContext	*shmem_ctx;

 	if (SRF_IS_FIRSTCALL())
	{
		int 			index = 0;
		MemoryContext	oldcontext;
		TupleDesc		tupdesc;
		long 			normal_num_elem = 0;
		long 			num_elem = 0;

		/* init first */
		func_ctx = SRF_FIRSTCALL_INIT();

		/* Calculate the number of shmem entries. */
		normal_num_elem = normal_shmem_num_entries();
		num_elem = normal_num_elem;
		num_elem += persisted_shmem_num_entries();

		/* switch context when allocating stuff to be used in later calls */
		oldcontext = MemoryContextSwitchTo(func_ctx->multi_call_memory_ctx);
		shmem_ctx = (SharedMemoryContext *) palloc(sizeof(SharedMemoryContext));

		/* Build a tuple descriptor for our result type */
		if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
			/*no cover line*/
			elog(ERROR, "return type must be a row type");

		/* Allocate SharedMemoryContext */
		shmem_ctx->record = (SharedMemoryRec *) MemoryContextAllocHuge(
			CurrentMemoryContext,
			sizeof(SharedMemoryRec) * num_elem);

		/* Set max calls and remember the user function context. */
		func_ctx->max_calls = num_elem;
		func_ctx->user_fctx = shmem_ctx;
		shmem_ctx->tupdesc = BlessTupleDesc(tupdesc);

		/* return to original context when allocating transient memory */
		MemoryContextSwitchTo(oldcontext);

		build_normal_shmem_record(shmem_ctx, &index);
		Assert(index >= normal_num_elem);

		build_persisted_shmem_record(shmem_ctx, &index);
		Assert(index >= num_elem);
	}

	func_ctx = SRF_PERCALL_SETUP();

	/* Get the saved state */
	shmem_ctx = func_ctx->user_fctx;

	if (func_ctx->call_cntr < func_ctx->max_calls)
	{
		uint32      i = func_ctx->call_cntr;
		Datum       values[func_ctx->max_calls];
		bool        nulls[func_ctx->max_calls];

		values[0] = CStringGetTextDatum(shmem_ctx->record[i].name);
		values[1] = UInt64GetDatum(shmem_ctx->record[i].size);
		values[2] = CStringGetTextDatum(shmem_ctx->record[i].type);

		MemSet(nulls, 0, sizeof(nulls));

		tuple = heap_form_tuple(shmem_ctx->tupdesc, values, nulls);
		result = HeapTupleGetDatum(tuple);

		SRF_RETURN_NEXT(func_ctx, result);
	}
	else
		SRF_RETURN_DONE(func_ctx);
}

PG_FUNCTION_INFO_V1(polar_shmem_total_size);
Datum
polar_shmem_total_size(PG_FUNCTION_ARGS)
{
#define SHMEM_TOTAL_SIZE_COLS	2

	ReturnSetInfo 	*rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	PGShmemHeader	*ShmemSegHdr;
	Tuplestorestate *tupstore;
	MemoryContext 	per_query_ctx;
	MemoryContext 	oldcontext;
	TupleDesc	tupdesc;
	Datum		values[SHMEM_TOTAL_SIZE_COLS];
	bool		nulls[SHMEM_TOTAL_SIZE_COLS];

	/* check to see if caller supports us returning a tuplestore */
	if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo))
		/*no cover line*/
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					errmsg("set-valued function called in context that cannot accept a set")));
	if (!(rsinfo->allowedModes & SFRM_Materialize))
		/*no cover line*/
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					errmsg("materialize mode required, but it is not " \
						"allowed in this context")));

	/* Build a tuple descriptor for our result type */
	tupdesc = CreateTemplateTupleDesc(SHMEM_TOTAL_SIZE_COLS, false);
	TupleDescInitEntry(tupdesc, (AttrNumber) 1, "shmemsize",
					   INT8OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 2, "shmemtype",
					   TEXTOID, -1, 0);

	per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
	oldcontext = MemoryContextSwitchTo(per_query_ctx);

	tupstore = tuplestore_begin_heap(true, false, work_mem);
	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = tupstore;
	rsinfo->setDesc = tupdesc;
	MemoryContextSwitchTo(oldcontext);

	MemSet(nulls, 0, sizeof(nulls));

	/* Normal shmem total size. */
	ShmemSegHdr = polar_get_shmem_seg_hdr();
	values[0] = UInt64GetDatum(ShmemSegHdr->totalsize);
	values[1] = CStringGetTextDatum(SHMEM_TYPE_NORMAL);
	tuplestore_putvalues(tupstore, tupdesc, values, nulls);

	/* Persisted shmem total size. */
	if (polar_persisted_buffer_pool_enabled(NULL))
	{
		ShmemSegHdr = polar_get_persisted_shmem_seg_hdr();
		values[0] = UInt64GetDatum(ShmemSegHdr->totalsize);
		values[1] = CStringGetTextDatum(SHMEM_TYPE_PERSISTED);
		tuplestore_putvalues(tupstore, tupdesc, values, nulls);
	}

	/* clean up and return the tuplestore */
	tuplestore_donestoring(tupstore);

	return (Datum) 0;
}

static long
normal_shmem_num_entries(void)
{
	HTAB			*ShmemIndex;
	long 			num_elem = 0;

	/* init hash_seq and get the number of shared memory element */
	LWLockAcquire(ShmemIndexLock, LW_SHARED);
	ShmemIndex = polar_get_shmem_index();

	/* Plus 1 for Anonymous Shared Memory */
	num_elem = hash_get_num_entries(ShmemIndex) + 1;
	LWLockRelease(ShmemIndexLock);

	return num_elem;
}

static long
persisted_shmem_num_entries(void)
{
	if (!polar_persisted_buffer_pool_enabled(NULL))
		return 0l;

	/* Plus 1 for Anonymous Shared Memory */
	return POLAR_PERSISTED_SHMEM_NUM + 1;
}

static void
build_normal_shmem_record(SharedMemoryContext *shmem_ctx, int *index)
{
	HTAB			*ShmemIndex;
	HASH_SEQ_STATUS	hstat;
	ShmemIndexEnt	*ent;
	PGShmemHeader	*ShmemSegHdr;
	Size 			allocated = 0;

	LWLockAcquire(ShmemIndexLock, LW_SHARED);

	ShmemIndex = polar_get_shmem_index();
	ShmemSegHdr = polar_get_shmem_seg_hdr();
	hash_seq_init(&hstat, ShmemIndex);

	while ((ent = (ShmemIndexEnt *) hash_seq_search(&hstat)) != NULL)
	{
		snprintf(shmem_ctx->record[*index].name, NAMEDATALEN, "%s", ent->key);
		snprintf(shmem_ctx->record[*index].type, NAMEDATALEN, "%s", SHMEM_TYPE_NORMAL);
		shmem_ctx->record[(*index)++].size = ent->size;
		allocated += ent->size;
	}

	snprintf(shmem_ctx->record[*index].name, NAMEDATALEN, "%s", "Anonymous Shared Memory");
	snprintf(shmem_ctx->record[*index].type, NAMEDATALEN, "%s", SHMEM_TYPE_NORMAL);
	shmem_ctx->record[(*index)++].size = ShmemSegHdr->freeoffset - allocated;

	LWLockRelease(ShmemIndexLock);
}

static void
build_persisted_shmem_record(SharedMemoryContext *shmem_ctx, int *index)
{
	int	i;
	PolarShmemInfo	*infos;
	PGShmemHeader	*polar_shmem_hdr;
	Size 			allocated = 0;

	if (!polar_persisted_buffer_pool_enabled(NULL))
		return;

	infos = polar_get_persisted_shmem_info();

	/* Minus Anonymous Shared Memory */
	for(i = 0; i < persisted_shmem_num_entries() - 1; i++)
	{
		PolarShmemInfo	*info = &infos[i];
		Assert(info);
		snprintf(shmem_ctx->record[*index].name, NAMEDATALEN, "%s", info->name);
		snprintf(shmem_ctx->record[*index].type, NAMEDATALEN, "%s", SHMEM_TYPE_PERSISTED);
		shmem_ctx->record[(*index)++].size = info->size;
		allocated += info->size;
	}

	polar_shmem_hdr = polar_get_persisted_shmem_seg_hdr();
	snprintf(shmem_ctx->record[*index].name, NAMEDATALEN, "%s", "Anonymous Shared Memory");
	snprintf(shmem_ctx->record[*index].type, NAMEDATALEN, "%s", SHMEM_TYPE_NORMAL);
	shmem_ctx->record[(*index)++].size = polar_shmem_hdr->freeoffset - allocated;
}