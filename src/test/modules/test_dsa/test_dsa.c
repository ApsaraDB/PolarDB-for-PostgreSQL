/* -------------------------------------------------------------------------
 *
 * test_dsa.c
 *		Simple exercises for dsa.c.
 *
 * Copyright (C) 2016, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		src/test/modules/test_dsa/test_dsa.c
 *
 * -------------------------------------------------------------------------
 */

#include "postgres.h"

#include "fmgr.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "postmaster/bgworker.h"
#include "storage/latch.h"
#include "storage/proc.h"
#include "utils/builtins.h"
#include "utils/dsa.h"
#include "utils/resowner.h"
#include "utils/timestamp.h"

#include <stdlib.h>
#include <unistd.h>

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(test_dsa_random);
PG_FUNCTION_INFO_V1(test_dsa_random_parallel);
PG_FUNCTION_INFO_V1(test_dsa_oom);

/* Which order to free objects in, within each loop. */
typedef enum
{
	/* Free in random order. */
	MODE_RANDOM,
	/* Free in the same order we allocated (FIFO). */
	MODE_FORWARDS,
	/* Free in reverse order of allocation (LIFO). */
	MODE_BACKWARDS
} test_mode;

/* Per-worker results. */
typedef struct
{
	pid_t pid;
	int64 count;
	int64 elapsed_time_us;
} test_result;

/* Parameters for a test run, passed to workers. */
typedef struct
{
	int loops;
	int num_allocs;
	int min_alloc;
	int max_alloc;
	test_mode mode;
	test_result results[1]; /* indexed by worker number */
} test_parameters;

/* The startup message given to each worker. */
typedef struct
{
	/* How to connect to the shmem area. */
	dsa_handle area_handle;
	/* Where to find the parameters. */
	dsa_pointer parameters;
	/* What index this worker should write results to. */
	Size output_index;
} test_hello;

static test_mode
parse_test_mode(text *mode)
{
	test_mode result = MODE_RANDOM;
	char *cstr = text_to_cstring(mode);

	if (strcmp(cstr, "random") == 0)
		result = MODE_RANDOM;
	else if (strcmp(cstr, "forwards") == 0)
		result = MODE_FORWARDS;
	else if (strcmp(cstr, "backwards") == 0)
		result = MODE_BACKWARDS;
	else
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("unknown mode")));
	return result;
}

static void
check_parameters(const test_parameters *parameters)
{
	if (parameters->min_alloc < 1 || parameters->min_alloc > parameters->max_alloc)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("min_alloc must be >= 1, and min_alloc must be <= max_alloc")));
}

static int
my_tranche_id(void)
{
	static int tranche_id = 0;

	if (tranche_id == 0)
		tranche_id = LWLockNewTrancheId();

	return tranche_id;
}

static int64
timestamp_diff(TimestampTz start, TimestampTz end)
{
	long		secs = 0;
	int			microsecs = 0;

	TimestampDifference(end, start, &secs, &microsecs);
	return secs * 1000 + microsecs;
}

static void
do_random_test(dsa_area *area, Size output_index, test_parameters *parameters)
{
	dsa_pointer *objects;
	int min_alloc;
	int extra_alloc;
	int32 i;
	int32 loop;
	TimestampTz start_time = GetCurrentTimestamp();
	int64 total_allocations = 0;

	/*
	 * Make tests reproducible (on the same computer at least) by using the
	 * same random sequence every time.
	 */
	srand(42);

	min_alloc = parameters->min_alloc;
	extra_alloc = parameters->max_alloc - parameters->min_alloc;

	objects = palloc(sizeof(dsa_pointer) * parameters->num_allocs);
	Assert(objects != NULL);
	for (loop = 0; loop < parameters->loops; ++loop)
	{
		int num_actually_allocated = 0;

		for (i = 0; i < parameters->num_allocs; ++i)
		{
			Size size;
			void *memory;

			/* Adjust size randomly if needed. */
			size = min_alloc;
			if (extra_alloc > 0)
				size += rand() % extra_alloc;

			/* Allocate! */
			objects[i] = dsa_allocate_extended(area, size, DSA_ALLOC_NO_OOM);
			if (!DsaPointerIsValid(objects[i]))
			{
				elog(LOG, "dsa: loop %d: out of memory after allocating %d objects", loop, i + 1);
				break;
			}
			++num_actually_allocated;
			/* Pay the cost of accessing that memory */
			memory = dsa_get_address(area, objects[i]);
			memset(memory, 42, size);
		}
		if (parameters->mode == MODE_RANDOM)
		{
			for (i = 0; i < num_actually_allocated; ++i)
			{
				Size x = rand() % num_actually_allocated;
				Size y = rand() % num_actually_allocated;
				dsa_pointer temp = objects[x];

				objects[x] = objects[y];
				objects[y] = temp;
			}
		}
		if (parameters->mode == MODE_BACKWARDS)
		{
			for (i = num_actually_allocated - 1; i >= 0; --i)
				dsa_free(area, objects[i]);
		}
		else
		{
			for (i = 0; i < num_actually_allocated; ++i)
				dsa_free(area, objects[i]);
		}
		total_allocations += num_actually_allocated;
	}
	pfree(objects);

	parameters->results[output_index].elapsed_time_us =
	 	timestamp_diff(start_time, GetCurrentTimestamp());
	parameters->results[output_index].pid = getpid();
	parameters->results[output_index].count = total_allocations;
}

/* Non-parallel version: just do it. */
Datum
test_dsa_random(PG_FUNCTION_ARGS)
{
	test_parameters parameters;
	dsa_area *area;

	parameters.loops = PG_GETARG_INT32(0);
	parameters.num_allocs = PG_GETARG_INT32(1);
	parameters.min_alloc = PG_GETARG_INT32(2);
	parameters.max_alloc = PG_GETARG_INT32(3);
	parameters.mode = parse_test_mode(PG_GETARG_TEXT_PP(4));
	check_parameters(&parameters);

	area = dsa_create(my_tranche_id());
	do_random_test(area, 0, &parameters);
	dsa_dump(area);
	dsa_detach(area);

	PG_RETURN_NULL();
}

Datum test_dsa_random_worker_main(Datum arg);

Datum
test_dsa_random_worker_main(Datum arg)
{
	test_hello hello;
	dsa_area *area;
	test_parameters *parameters;

	CurrentResourceOwner = ResourceOwnerCreate(NULL, "test_dsa toplevel");

	/* Receive hello message and attach to shmem area. */
	memcpy(&hello, MyBgworkerEntry->bgw_extra, sizeof(hello));
	area = dsa_attach(hello.area_handle);
	Assert(area != NULL);
	parameters = dsa_get_address(area, hello.parameters);
	Assert(parameters != NULL);

	do_random_test(area, hello.output_index, parameters);

	dsa_detach(area);

	return (Datum) 0;
}

/* Parallel version: fork a bunch of background workers to do it. */
Datum
test_dsa_random_parallel(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	TupleDesc	tupdesc;
	Tuplestorestate *tupstore;
	MemoryContext per_query_ctx;
	MemoryContext oldcontext;

	test_hello hello;
	test_parameters *parameters;
	dsa_area *area;
	int workers;
	int i;
	BackgroundWorkerHandle **handles;

	/* tuplestore boilerplate stuff... */
	if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("set-valued function called in context that cannot accept a set")));
	if (!(rsinfo->allowedModes & SFRM_Materialize))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("materialize mode required, but it is not " \
						"allowed in this context")));
	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");
	per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
	oldcontext = MemoryContextSwitchTo(per_query_ctx);
	tupstore = tuplestore_begin_heap(true, false, work_mem);
	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = tupstore;
	rsinfo->setDesc = tupdesc;
	MemoryContextSwitchTo(oldcontext);

	/* Prepare to work! */
	workers = PG_GETARG_INT32(5);
	handles = palloc(sizeof(BackgroundWorkerHandle *) * workers);

	/* Set up the shared memory area. */
	area = dsa_create(my_tranche_id());

	/* The workers then will attach to it. */
	hello.area_handle = dsa_get_handle(area);

	/* Allocate space for the parameters object. */
	hello.parameters = dsa_allocate(area, sizeof(test_parameters) +
									sizeof(test_result) * workers);
	Assert(DsaPointerIsValid(hello.parameters));

	/* Set up the parameters object. */
	parameters = dsa_get_address(area, hello.parameters);
	parameters->loops = PG_GETARG_INT32(0);
	parameters->num_allocs = PG_GETARG_INT32(1);
	parameters->min_alloc = PG_GETARG_INT32(2);
	parameters->max_alloc = PG_GETARG_INT32(3);
	parameters->mode = parse_test_mode(PG_GETARG_TEXT_PP(4));
	check_parameters(parameters);

	/* Start the workers. */
	for (i = 0; i < workers; ++i)
	{
		BackgroundWorker bgw;

		snprintf(bgw.bgw_name, sizeof(bgw.bgw_name), "worker%d", i);
		bgw.bgw_flags = BGWORKER_SHMEM_ACCESS;
		bgw.bgw_start_time = BgWorkerStart_PostmasterStart;
		bgw.bgw_restart_time = BGW_NEVER_RESTART;
		snprintf(bgw.bgw_library_name, sizeof(bgw.bgw_library_name),
				 "test_dsa");
		snprintf(bgw.bgw_function_name, sizeof(bgw.bgw_function_name),
				 "test_dsa_random_worker_main");
		Assert(sizeof(parameters) <= BGW_EXTRALEN);
		/* Each worker will write its output to a different slot. */
		hello.output_index = i;
		memcpy(bgw.bgw_extra, &hello, sizeof(hello));
		bgw.bgw_notify_pid = MyProcPid;

		if (!RegisterDynamicBackgroundWorker(&bgw, &handles[i]))
			elog(ERROR, "Can't start worker");
	}

	/* Wait for the workers to complete. */
	for (i = 0; i < workers; ++i)
		/* erm, should really check for BGWH_STOPPED */
		WaitForBackgroundWorkerShutdown(handles[i]);

	/* Generate result tuples. */
	for (i = 0; i < workers; ++i)
	{
		Datum values[3];
		bool nulls[] = { false, false, false };
		Interval *interval = palloc(sizeof(Interval));

		interval->month = 0;
		interval->day = 0;
		interval->time = parameters->results[i].elapsed_time_us
#ifndef HAVE_INT64_TIMESTAMP
			/ 1000000.0
#endif
			;

		values[0] = Int32GetDatum(parameters->results[i].pid);
		values[1] = Int64GetDatum(parameters->results[i].count);
		values[2] = PointerGetDatum(interval);
		tuplestore_putvalues(tupstore, tupdesc, values, nulls);
	}
	tuplestore_donestoring(tupstore);

	pfree(handles);
	dsa_detach(area);

	return (Datum) 0;
}

/* OOM test. Allocate memory until oom, than free and try allocate again. */
Datum
test_dsa_oom(PG_FUNCTION_ARGS)
{
	test_parameters parameters;
	dsa_area *area;

	parameters.loops = 1;
	parameters.num_allocs = 256;
	parameters.min_alloc = 4096;
	parameters.max_alloc = 4096;
	parameters.mode = MODE_RANDOM;
	check_parameters(&parameters);

	area = dsa_create(my_tranche_id());
	dsa_set_size_limit(area, 1*1024*1024);
	dsa_dump(area);

	do_random_test(area, 0, &parameters);
	dsa_dump(area);
	elog(LOG, "Allocated %ld", parameters.results[0].count);

	/* try again */
	do_random_test(area, 0, &parameters);
	dsa_dump(area);
	elog(LOG, "Allocated %ld", parameters.results[0].count);

	parameters.num_allocs = 4000;
	parameters.min_alloc = 256;
	parameters.max_alloc = 256*1024;
	parameters.mode = MODE_RANDOM;
	check_parameters(&parameters);

	do_random_test(area, 0, &parameters);
	dsa_dump(area);
	elog(LOG, "Allocated %ld", parameters.results[0].count);

	do_random_test(area, 0, &parameters);
	dsa_dump(area);
	elog(LOG, "Allocated %ld", parameters.results[0].count);

	dsa_detach(area);

	PG_RETURN_NULL();
}