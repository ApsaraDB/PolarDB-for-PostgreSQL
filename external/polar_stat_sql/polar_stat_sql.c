/*-------------------------------------------------------------------------
 * polar_stat_sql.c
 *    There are two parts: one is the collection of information about the kernel,
 *    and the other is the statistics about the execution plan nodes.
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
 *    external/polar_stat_sql/polar_stat_sql.c
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <unistd.h>

#ifdef HAVE_SYS_RESOURCE_H
#include <sys/time.h>
#include <sys/resource.h>
#endif

#ifndef HAVE_GETRUSAGE
#include "rusagestub.h"
#endif

#include "access/hash.h"
#include "access/parallel.h"
#include "catalog/pg_type.h"
#include "executor/executor.h"
#include "executor/hashjoin.h"
#include "executor/instrument.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "parser/analyze.h"
#include "pgstat.h"
#include "postmaster/autovacuum.h"
#include "storage/fd.h"
#include "storage/ipc.h"
#include "storage/polar_lock_stats.h"
#include "storage/polar_io_stat.h"
#include "storage/spin.h"
#include "tcop/utility.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/polar_sql_time_stat.h"

/* POLAR */
#include "utils/polar_sql_time_stat.h"

PG_MODULE_MAGIC;

#define PGSS_DUMP_FILE		"pg_stat/polar_stat_sql.stat"

typedef uint64 pgss_queryid;

#define USAGE_INCREASE			(1.0)
#define USAGE_DECREASE_FACTOR	(0.99)	/* decreased every pgss_entry_dealloc */
#define USAGE_DEALLOC_PERCENT	5		/* free this % of entries at once */
#define USAGE_INIT				(1.0)	/* including initial planning */

/* 
 * ru_inblock block size is 512 bytes with Linux
 * see http://lkml.indiana.edu/hypermail/linux/kernel/0703.2/0937.html
 */
#define RUSAGE_BLOCK_SIZE	512			/* Size of a block for getrusage() */

#define TIMEVAL_DIFF(start, end) (((double) end.tv_sec + (double) end.tv_usec / 1000000.0) \
	- ((double) start.tv_sec + (double) start.tv_usec / 1000000.0))

#define TIMESPEC_DIFF_IN_MS(t2, t1) ((t2 - t1)/1000.0)

#define LOCK_WAIT_TIMING(tag) \
	 TIMESPEC_DIFF_IN_MS(polar_lock_stat_local_summary[tag].wait_time,  \
						queryDesc->polar_sql_info->lock_stats_table_start[tag].wait_time);
						
/*
 * Extension version number, for supporting older extension versions' objects
 */
typedef enum pgssVersion
{
	PGSS_V1_0 = 0,
	PGSS_V1_1,
} pgssVersion;

typedef enum POLAR_SS_COL_DEF {
	COL_QUERY_ID = 0,
   	COL_USER_ID,
   	COL_DB_ID,
   	COL_READS,
   	COL_WRITES,
   	COL_USER_TIME,
   	COL_SYSTEM_TIME,
   	COL_MINFLTS,
   	COL_MAJFLTS,
   	COL_NSWAPS,
   	COL_MSGSNDS,
	COL_MSGRCVS,
	COL_NSIGNALS,
	COL_NVCSWS,
	COL_NIVCSWS,
	COL_SCAN_ROWS,
	COL_SCAN_TIME,
	COL_SCAN_COUNT,
	COL_JOIN_ROWS,
	COL_JOIN_TIME,
	COL_JOIN_COUNT,
	COL_SORT_ROWS,
	COL_SORT_TIME,
	COL_SORT_COUNT,
	COL_GROUP_ROWS,
	COL_GROUP_TIME,
	COl_GROUP_COUNT,
	COL_HASH_ROWS,
	COL_HASH_MEMORY,
	COL_HASH_COUNT,
	COL_PARSE_TIME,
	COL_ANALYZE_TIME,
	COL_REWRITE_TIME,
	COL_PLAN_TIME,
	COL_EXECUTE_TIME,
	COL_LWLOCK_WAIT,
	COL_REL_LOCK_WAIT,
	COL_XACT_LOCK_WAIT,
	COL_PAGE_LOCK_WAIT,
	COL_TUPLE_LOCK_WAIT,
	COL_SHARED_READ_PS,
	COL_SHARED_WRITE_PS,
	COL_SHARED_READ_THROUGHPUT,
	COL_SHARED_WRITE_THROUGHPUT,
	COL_SHARED_READ_LATENCY,
	COL_SHARED_WRITE_LATENCY,
	COL_IO_OPEN_NUM,
	COL_IO_SEEK_COUNT,
	COL_IO_OPEN_TIME,
	COL_IO_SEEK_TIME,
   	COL_POLAR_STAT_SQL_MAX
} POLAR_SS_COL_DEF;

static const uint32 PGSS_FILE_HEADER = 0x0d756e10;

/*
 * Current getrusage counters.
 *
 * For platform without getrusage support, we rely on postgres implementation
 * defined in rusagestub.h, which only supports user and system time.
 *
 * Note that the following counters are not maintained on GNU/Linux:
 *   - ru_nswap
 *   - ru_msgsnd
 *   - ru_msgrcv
 *   - ru_nsignals
*/
typedef struct pgssCounters
{
	double			usage;		/* usage factor */
	float8			utime;		/* CPU user time */
	float8			stime;		/* CPU system time */
	int64			minflts;	/* page reclaims (soft page faults) */
	int64			majflts;	/* page faults (hard page faults) */
	int64			nswaps;		/* page faults (hard page faults) */
	int64			reads;		/* Physical block reads */
	int64			writes;		/* Physical block writes */
	int64			msgsnds;	/* IPC messages sent */
	int64			msgrcvs;	/* IPC messages received */
	int64			nsignals;	/* signals received */
	int64			nvcsws;		/* voluntary context witches */
	int64			nivcsws;	/* unvoluntary context witches */

	double		    scan_rows;			/* scan rows */
	double		    scan_time;			/* scan time */
	int64		    scan_count;			/* scan count */
	double		    join_rows;		    /* join rows */
	double		    join_time;		    /* join time */
	int64		    join_count;   	    /* join count */
	double		    sort_rows;			/* sort rows */
	double		    sort_time;			/* sort time */
	int64		    sort_count;   	    /* sort count */
	double		    group_rows;			/* group rows */
	double		    group_time;			/* group time */
	int64		    group_count;   	    /* group count */
	double		    hash_rows;			/* hash rows */
	int64		    hash_memory;		/* hash memory, unit kB */
	int64		    hash_count;   	    /* hash count */
	double		    parse_time;			/* total time for sql parse */
	double		    analyze_time;		/* total time for sql analyze */
	double		    rewrite_time;		/* total time for sql analyze */
	double		    plan_time;			/* total time for sql plan  */
	double		    execute_time;		/* total time for sql execute */
	double		    lwlock_wait;		/* lwlock wait time */
	double		    rel_lock_wait;		/* rel lock wait time */
	double		    xact_lock_wait;		/* xact lock wait time */
	double		    page_lock_wait;		/* page lock wait time */
	double		    tuple_lock_wait;	/* tuple lock wait time */
	int64	shared_read_ps;
	int64	shared_write_ps;
	int64	shared_read_throughput;
	int64	shared_write_throughput;
	double	shared_read_latency;
	double 	shared_write_latency;
	int64	io_open_num;
	int64	io_seek_count;
	double	io_open_time;
	double	io_seek_time;
} pgssCounters;

static int	pgss_max = 0;	/* max #queries to store. pg_stat_statements.max is used */

/*
 * Hashtable key that defines the identity of a hashtable entry.  We use the
 * same hash as pg_stat_statements
 */
typedef struct pgssHashKey
{
	Oid			userid;			/* user OID */
	Oid			dbid;			/* database OID */
	pgss_queryid		queryid;		/* query identifier */
} pgssHashKey;

/*
 * Statistics per database
 */
typedef struct pgssEntry
{
	pgssHashKey		key;		/* hash key of entry - MUST BE FIRST */
	pgssCounters	counters;	/* statistics for this query */
	slock_t			mutex;		/* protects the counters only */
} pgssEntry;

/*
 * Global shared state
 */
typedef struct pgssSharedState
{
	LWLock	   *lock;					/* protects hashtable search/modification */
} pgssSharedState;

/*
 * struct about plan info 
*/
typedef struct pgssPlanInfo
{
	double		scan_rows;			/* scan rows */
	double		scan_time;			/* scan time */
	int64		scan_count;			/* scan count */
	double		join_rows;			/* join rows */
	double		join_time;			/* join time */
	int64		join_count;   		/* join count */
	double		sort_rows;			/* sort rows */
	double		sort_time;			/* sort time */
	int64		sort_count;   	    /* sort count */
	double		group_rows;			/* group rows */
	double		group_time;			/* group time */
	int64		group_count;   	    /* group count */
	double		hash_rows;			/* hash rows */
	int64		hash_memory;		/* hash memory, unit kB */
	int64		hash_count;   	    /* hash count */
}pgssPlanInfo;

/* QPS */
typedef struct QueryTypeStat
{
	LWLock	   *lock;
	/* DQL */
	pg_atomic_uint64 select_count;
	/* DML */
	pg_atomic_uint64 update_count;
	pg_atomic_uint64 insert_count;
	pg_atomic_uint64 delete_count;
	pg_atomic_uint64 explain_count;
	pg_atomic_uint64 lock_count;
	/* DDL */
	pg_atomic_uint64 create_count;
	pg_atomic_uint64 drop_count;
	pg_atomic_uint64 rename_count;
	pg_atomic_uint64 truncate_count;
	pg_atomic_uint64 comment_count;
	pg_atomic_uint64 alter_count;
	/* DCL */
	pg_atomic_uint64 grant_count;
} QueryTypeStat;

static QueryTypeStat* query_type_stat = NULL;

static void add_various_tuple(Tuplestorestate *state, TupleDesc tdesc, char* sqltype, char* cmdtype,
					 const int arraylen, int64 count, Datum *values, 
					 bool *nulls, int array_size);
static void record_query_type(Query *query);
static void pgsk_post_parse_analyze(ParseState *pstate, Query *query);
/* QPS end */

/* saved hook address in case of unload */
static shmem_startup_hook_type prev_shmem_startup_hook = NULL;
static ExecutorStart_hook_type prev_ExecutorStart = NULL;
static ExecutorRun_hook_type prev_ExecutorRun = NULL;
static ExecutorFinish_hook_type prev_ExecutorFinish = NULL;
static ExecutorEnd_hook_type prev_ExecutorEnd = NULL;
static post_parse_analyze_hook_type prev_post_parse_analyze_hook = NULL;
static ProcessUtility_hook_type prev_ProcessUtility = NULL;

/* Links to shared memory state */
static pgssSharedState *pgss = NULL;
static HTAB *polar_stat_sql_hash = NULL;

/*--- Functions --- */

void	_PG_init(void);
void	_PG_fini(void);

extern PGDLLEXPORT Datum	polar_stat_sql_reset(PG_FUNCTION_ARGS);
extern PGDLLEXPORT Datum	polar_stat_sql(PG_FUNCTION_ARGS);
extern PGDLLEXPORT Datum	polar_stat_sql_1_0(PG_FUNCTION_ARGS);
extern PGDLLEXPORT Datum	polar_stat_sql_1_1(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(polar_stat_sql_reset);
PG_FUNCTION_INFO_V1(polar_stat_sql);
PG_FUNCTION_INFO_V1(polar_stat_sql_1_0);
PG_FUNCTION_INFO_V1(polar_stat_sql_1_1);

static void polar_stat_sql_internal(FunctionCallInfo fcinfo, pgssVersion
		api_version);

static void pgss_setmax(void);
static Size pgss_memsize(void);

static void polar_stat_shmem_startup(void);
static void pgss_shmem_shutdown(int code, Datum arg);
static void polar_stat_ExecutorStart(QueryDesc *queryDesc, int eflags);
static void polar_stat_ExecutorEnd(QueryDesc *queryDesc);
static void polar_stat_ExecutorRun(QueryDesc *queryDesc, ScanDirection direction, uint64 count,
				 bool execute_once);
static void
polar_stat_ExecutorFinish(QueryDesc *queryDesc);
static void
polar_stat_ProcessUtility(PlannedStmt *pstmt, const char *queryString,
					ProcessUtilityContext context,
					ParamListInfo params, QueryEnvironment *queryEnv,
					DestReceiver *dest, char *completionTag);

static pgssEntry *pgss_entry_alloc(pgssHashKey *key);
static void pgss_entry_dealloc(void);
static void pgss_entry_reset(void);
static void pgss_entry_store(pgss_queryid queryId, pgssCounters counters, pgssPlanInfo *plan_info, PolarCollectIoStat *io_stat_info, QueryDesc *queryDesc);
static uint32 polar_stat_sql_hash_fn(const void *key, Size keysize);
static int	pgss_match_fn(const void *key1, const void *key2, Size keysize);

static void pgss_stat_plan_node(PlanState *planstate, pgssPlanInfo *planInfo);
static void pgss_stat_subplan_node(List *plans, pgssPlanInfo  *planInfo);

static bool pgss_assign_linux_hz_check_hook(int *newval, void **extra, GucSource source);

static Size pgss_queryids_array_size(void);

static int	pgss_linux_hz;
static bool polar_ss_enable_stat;      	/* whether to enable the extension */
static bool polar_ss_getuage;      		/* whether to enable getuage */
static bool polar_ss_gather_plan_info; 	/* whether to gather plan node information */
static bool polar_ss_plan_need_time;		/* whether to gather plan node information need time*/
static bool polar_ss_save;				/* whether to save stats across shutdown */
static bool polar_pgsk_qps_monitor;		/* whether to monitor qps */
static double polar_ss_sample_rate;   	/* sample rate of sql */

static bool is_sql_sampled;      			/* sql level sampled */
static bool is_stat_enable;      			/* stat enable switch */

/* Current nesting depth of ExecutorRun+ProcessUtility calls */
static int	nested_level = 0;

void
_PG_init(void)
{
	if (!process_shared_preload_libraries_in_progress)
	{
		elog(ERROR, "This module can only be loaded via shared_preload_libraries");
		return;
	}

	DefineCustomIntVariable("polar_stat_sql.linux_hz",
				"Inform polar_stat_sql of the linux CONFIG_HZ config option",
				"This is used by polar_stat_sql to compensate for sampling errors "
				"in getrusage due to the kernel adhering to its ticks. The default value, -1, "
				"tries to guess it at startup. ",
							&pgss_linux_hz,
							-1,
							-1,
							INT_MAX,
							PGC_USERSET,
							0,
							pgss_assign_linux_hz_check_hook,
							NULL,
							NULL);


	/*
	 * POLAR: whether to enable the extension
	 */
	DefineCustomBoolVariable("polar_stat_sql.enable_stat",
							 "Whether to enable the extension",
							 NULL,
							 &polar_ss_enable_stat,
							 false,
							 PGC_SUSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomRealVariable("polar_stat_sql.sample_rate",
							 "Sampling rate. 1 means every query, 0.2 means 1 in five queries",
							 NULL,
							 &polar_ss_sample_rate,
							 0.5,
							 0.0,
							 1.0,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	/*
	 * POLAR: whether to enable getrusage
	 */
	DefineCustomBoolVariable("polar_stat_sql.enable_getrusage",
							 "Whether to enable getuage",
							 NULL,
							 &polar_ss_getuage,
							 false,
							 PGC_SUSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	/*
	 * POLAR: whether to gather plan nodes info switch
	 */
	DefineCustomBoolVariable("polar_stat_sql.enable_gather_plan_info",
							 "Whether to gather plan nodes info",
							 NULL,
							 &polar_ss_gather_plan_info,
							 false,
							 PGC_SUSET,
							 0,
							 NULL,
							 NULL,
							 NULL);
	/*
	 * POLAR: whether to gather plan nodes need time
	 */
	DefineCustomBoolVariable("polar_stat_sql.enable_plan_need_time",
							 "Whether to gather plan nodes need time information",
							 NULL,
							 &polar_ss_plan_need_time,
							 false,
							 PGC_SUSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	/*
	 * POLAR: Set the default value to be 'false' here.
	 */
	DefineCustomBoolVariable("polar_stat_sql.save",
							 "Save polar statistics for across server shutdowns.",
							 NULL,
							 &polar_ss_save,
							 false,
							 PGC_SIGHUP,
							 0,
							 NULL,
							 NULL,
							 NULL);

	/*
	 * POLAR: Set the default value to be 'false' here, 
	 */
	DefineCustomBoolVariable("polar_stat_sql.enable_qps_monitor",
							 "Selects whether to monitor qps ",
							 NULL,
							 &polar_pgsk_qps_monitor,
							 false,
							 PGC_SUSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	EmitWarningsOnPlaceholders("polar_stat_sql");

	/* set pgss_max if needed */
	pgss_setmax();
	RequestAddinShmemSpace(pgss_memsize());

	RequestNamedLWLockTranche("polar_stat_sql", 1);

	/* Install hook */
	prev_shmem_startup_hook = shmem_startup_hook;
	shmem_startup_hook = polar_stat_shmem_startup;
	prev_post_parse_analyze_hook = post_parse_analyze_hook;
	post_parse_analyze_hook = pgsk_post_parse_analyze;
	prev_ExecutorStart = ExecutorStart_hook;
	ExecutorStart_hook = polar_stat_ExecutorStart;
	prev_ExecutorRun = ExecutorRun_hook;
	ExecutorRun_hook = polar_stat_ExecutorRun;
	prev_ExecutorFinish = ExecutorFinish_hook;
	ExecutorFinish_hook = polar_stat_ExecutorFinish;
	prev_ExecutorEnd = ExecutorEnd_hook;
	ExecutorEnd_hook = polar_stat_ExecutorEnd;
	prev_ProcessUtility = ProcessUtility_hook;
	ProcessUtility_hook = polar_stat_ProcessUtility;
}

void
_PG_fini(void)
{
	/* uninstall hook */
	ExecutorStart_hook = prev_ExecutorStart;
	ExecutorRun_hook = prev_ExecutorRun;
	ExecutorFinish_hook = prev_ExecutorFinish;
	ExecutorEnd_hook = prev_ExecutorEnd;
	shmem_startup_hook = prev_shmem_startup_hook;
	post_parse_analyze_hook = prev_post_parse_analyze_hook;
	ProcessUtility_hook = prev_ProcessUtility;
}

static bool
pgss_assign_linux_hz_check_hook(int *newval, void **extra, GucSource source)
{
	int			val = *newval;
	struct rusage myrusage;
	struct timeval previous_value;

	/* In that case we try to guess it */
	if (val == -1)
	{
		elog(LOG, "Auto detecting polar_stat_sql.linux_hz parameter...");
		getrusage(RUSAGE_SELF, &myrusage);
		previous_value = myrusage.ru_utime;
		while (myrusage.ru_utime.tv_usec == previous_value.tv_usec &&
			   myrusage.ru_utime.tv_sec == previous_value.tv_sec)
		{
			getrusage(RUSAGE_SELF, &myrusage);
		}
		*newval = (int) (1 / ((myrusage.ru_utime.tv_sec - previous_value.tv_sec) +
		   (myrusage.ru_utime.tv_usec - previous_value.tv_usec) / 1000000.));
		elog(LOG, "polar_stat_sql.linux_hz is set to %d", *newval);
	}
	return true;
}

static void
polar_stat_shmem_startup(void)
{
	bool		found;
	HASHCTL		info;
	FILE		*file;
	int			i;
	uint32		header;
	int32		num;
	pgssEntry	*buffer = NULL;

	if (prev_shmem_startup_hook)
		prev_shmem_startup_hook();

	/* reset in case this is a restart within the postmaster */
	pgss = NULL;
	query_type_stat = NULL;

	/* Create or attach to the shared memory state */
	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);

	query_type_stat = ShmemInitStruct("query_type_stat",
						   sizeof(QueryTypeStat),
						   &found);

	/* global access lock */
	pgss = ShmemInitStruct("polar_stat_sql",
					sizeof(pgssSharedState),
					&found);

	if (!found)
	{
		/* First time through ... */
		LWLockPadded *locks = GetNamedLWLockTranche("polar_stat_sql");
		pgss->lock = &(locks[0]).lock;
	}

	/* set pgss_max if needed */
	pgss_setmax();

	memset(&info, 0, sizeof(info));
	info.keysize = sizeof(pgssHashKey);
	info.entrysize = sizeof(pgssEntry);
	info.hash = polar_stat_sql_hash_fn;
	info.match = pgss_match_fn;

	/* allocate stats shared memory hash */
	polar_stat_sql_hash = ShmemInitHash("polar_stat_sql hash",
							  pgss_max, pgss_max,
							  &info,
							  HASH_ELEM | HASH_FUNCTION | HASH_COMPARE);

	LWLockRelease(AddinShmemInitLock);

	if (!IsUnderPostmaster)
		on_shmem_exit(pgss_shmem_shutdown, (Datum) 0);

	/*
	 * Done if some other process already completed our initialization.
	 */
	if (found)
		return;

	/*
	 * If we were told not to load old statistics, we're done.  (Note we do
	 * not try to unlink any old dump file in this case.  This seems a bit
	 * questionable but it's the historical behavior.)
	 */
	if (!polar_ss_save)
	{
		return;
	}

	/* Load stat file, don't care about locking */
	file = AllocateFile(PGSS_DUMP_FILE, PG_BINARY_R);
	if (file == NULL)
	{
		if (errno == ENOENT)
			return;			/* ignore not-found error */
		goto error;
	}

	/* check if header is valid */
	if (fread(&header, sizeof(uint32), 1, file) != 1 ||
		header != PGSS_FILE_HEADER)
		goto error;

	/* get number of entries */
	if (fread(&num, sizeof(int32), 1, file) != 1)
		goto error;

	for (i = 0; i < num ; i++)
	{
		pgssEntry	temp;
		pgssEntry  *entry;

		if (fread(&temp, sizeof(pgssEntry), 1, file) != 1)
			goto error;

		entry = pgss_entry_alloc(&temp.key);

		/* copy in the actual stats */
		entry->counters = temp.counters;
		/* don't initialize spinlock, already done */
	}

	FreeFile(file);

	/*
	 * Remove the file so it's not included in backups/replication slaves,
	 * etc. A new file will be written on next shutdown.
	 */
	unlink(PGSS_DUMP_FILE);

	return;

error:
	ereport(LOG,
			(errcode_for_file_access(),
			 errmsg("could not read polar_stat_sql file \"%s\": %m",
					PGSS_DUMP_FILE)));
	if (buffer)
		pfree(buffer);
	if (file)
		FreeFile(file);
	/* delete bogus file, don't care of errors in this case */
	unlink(PGSS_DUMP_FILE);
}

/*
 * shmem_shutdown hook: dump statistics into file.
 *
 */
static void
pgss_shmem_shutdown(int code, Datum arg)
{
	FILE	*file;
	HASH_SEQ_STATUS hash_seq;
	int32	num_entries;
	pgssEntry	*entry;

	/* Don't try to dump during a crash. */
	if (code)
		return;

	if (!pgss)
		return;


	/* Don't dump if told not to. */
	if (!polar_ss_save)
		return;

	file = AllocateFile(PGSS_DUMP_FILE ".tmp", PG_BINARY_W);
	if (file == NULL)
		goto error;

	if (fwrite(&PGSS_FILE_HEADER, sizeof(uint32), 1, file) != 1)
		goto error;

	num_entries = hash_get_num_entries(polar_stat_sql_hash);

	if (fwrite(&num_entries, sizeof(int32), 1, file) != 1)
		goto error;

	hash_seq_init(&hash_seq, polar_stat_sql_hash);
	while ((entry = hash_seq_search(&hash_seq)) != NULL)
	{
		if (fwrite(entry, sizeof(pgssEntry), 1, file) != 1)
		{
			/* note: we assume hash_seq_term won't change errno */
			hash_seq_term(&hash_seq);
			goto error;
		}
	}

	if (FreeFile(file))
	{
		file = NULL;
		goto error;
	}

	/*
	 * Rename file inplace
	 */
	if (rename(PGSS_DUMP_FILE ".tmp", PGSS_DUMP_FILE) != 0)
		ereport(LOG,
				(errcode_for_file_access(),
				 errmsg("could not rename polar_stat_sql file \"%s\": %m",
						PGSS_DUMP_FILE ".tmp")));

	return;

error:
	ereport(LOG,
			(errcode_for_file_access(),
			 errmsg("could not read polar_stat_sql file \"%s\": %m",
					PGSS_DUMP_FILE)));

	if (file)
		FreeFile(file);
	unlink(PGSS_DUMP_FILE);
}

/*
 * Retrieve pg_stat_statement.max GUC value and store it into pgss_max, since
 * we want to store the same number of entries as pg_stat_statements. Don't do
 * anything if pgss_max is already set.
 */
static void pgss_setmax(void)
{
	const char *pgss_max_str;
	const char *name = "pg_stat_statements.max";

	if (pgss_max != 0)
		return;

	pgss_max_str = GetConfigOption(name, true, false);

	/*
	 * Retrieving pg_stat_statements.max can fail if pgss is loaded after pgss
	 * in shared_preload_libraries.  Hint user in case this happens.
	 */
	if (!pgss_max_str)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("unrecognized configuration parameter \"%s\"",
						name),
				 errhint("make sure pg_stat_statements is loaded,\n"
					 "and make sure polar_stat_sql is present after pg_stat_statements"
					 " in the shared_preload_libraries setting")));

	pgss_max = atoi(pgss_max_str);
}

static Size pgss_memsize(void)
{
	Size	size;

	Assert(pgss_max != 0);

	size = MAXALIGN(sizeof(pgssSharedState));
	size = add_size(size, hash_estimate_size(pgss_max, sizeof(pgssEntry)));
	size = add_size(size, MAXALIGN(pgss_queryids_array_size()));

	return size;
}

static Size
pgss_queryids_array_size(void)
{
	/*
	 * queryid isn't pushed to parallel workers.  We store them in shared mem
	 * for each query, identified by their BackendId.  If need room for all
	 * possible backends, plus autovacuum launcher and workers, plus bg workers
	 * and an extra one since BackendId numerotation starts at 1.
	 */
	return (sizeof(bool) * (MaxConnections + autovacuum_max_workers + 1
							+ MaxPolarDispatcher/* POLAR: Shared Server */
							+ max_worker_processes + 1));
}

/*
 * support functions
 */

static void
pgss_entry_store(pgss_queryid queryId, pgssCounters counters, pgssPlanInfo *plan_info, 
	PolarCollectIoStat *io_stat_info, QueryDesc *queryDesc)
{
	volatile pgssEntry *e;

	pgssHashKey key;
	pgssEntry  *entry;

	/* Safety check... */
	if (!pgss || !polar_stat_sql_hash)
		return;

	/* Set up key for hashtable search */
	key.userid = GetUserId();
	key.dbid = MyDatabaseId;
	key.queryid = queryId;

	/* Lookup the hash table entry with shared lock. */
	LWLockAcquire(pgss->lock, LW_SHARED);

	entry = (pgssEntry *) hash_search(polar_stat_sql_hash, &key, HASH_FIND, NULL);

	/* Create new entry, if not present */
	if (!entry)
	{
		/* Need exclusive lock to make a new hashtable entry - promote */
		LWLockRelease(pgss->lock);
		LWLockAcquire(pgss->lock, LW_EXCLUSIVE);

		/* OK to create a new hashtable entry */
		entry = pgss_entry_alloc(&key);
	}

	/*
	 * Grab the spinlock while updating the counters (see comment about
	 * locking rules at the head of the file)
	 */
	e = (volatile pgssEntry *) entry;

	SpinLockAcquire(&e->mutex);

	e->counters.usage += USAGE_INCREASE;

	e->counters.utime += counters.utime;
	e->counters.stime += counters.stime;

	e->counters.minflts += counters.minflts;
	e->counters.majflts += counters.majflts;
	e->counters.nswaps += counters.nswaps;
	e->counters.reads += counters.reads;
	e->counters.writes += counters.writes;
	e->counters.msgsnds += counters.msgsnds;
	e->counters.msgrcvs += counters.msgrcvs;
	e->counters.nsignals += counters.nsignals;
	e->counters.nvcsws += counters.nvcsws;
	e->counters.nivcsws += counters.nivcsws;

	e->counters.scan_rows += plan_info->scan_rows;
	e->counters.scan_time += plan_info->scan_time;
	e->counters.scan_count += plan_info->scan_count;
	e->counters.join_rows += plan_info->join_rows;
	e->counters.join_time += plan_info->join_time;
	e->counters.join_count += plan_info->join_count;
	e->counters.sort_rows += plan_info->sort_rows;
	e->counters.sort_time += plan_info->sort_time;
	e->counters.sort_count += plan_info->sort_count;
	e->counters.group_rows += plan_info->group_rows;
	e->counters.group_time += plan_info->group_time;
	e->counters.group_count += plan_info->group_count;
	e->counters.hash_rows += plan_info->hash_rows;
	e->counters.hash_memory += plan_info->hash_memory;
	e->counters.hash_count += plan_info->hash_count;
	
	/* sql stat time */
	if (polar_enable_track_sql_time_stat)
	{
		e->counters.execute_time += polar_sql_time_stat_local_summary.execute_time / 1000.0;
		/*
		 * Only the top sql need to record the parse/analyze/rewrite/plan time.
		 */
		if (nested_level == 0)
		{
			e->counters.parse_time += polar_sql_time_stat_local_summary.parse_time / 1000.0;
			e->counters.analyze_time += polar_sql_time_stat_local_summary.analyze_time / 1000.0;
			e->counters.rewrite_time += polar_sql_time_stat_local_summary.rewrite_time / 1000.0;
			e->counters.plan_time += polar_sql_time_stat_local_summary.plan_time / 1000.0;
			/*
			* When all the sql execute finish, need to init the global record.
			*/
			polar_init_sql_time_local_stats();
		} 
	}

	/* lwlock timing */
	e->counters.lwlock_wait += TIMESPEC_DIFF_IN_MS(
											polar_lwlock_stat_local_summary.wait_time, 
											queryDesc->polar_sql_info->lwlock_stat_start.wait_time);
	/* lock timing */
	e->counters.rel_lock_wait += LOCK_WAIT_TIMING(LOCKTAG_RELATION);
	e->counters.rel_lock_wait += LOCK_WAIT_TIMING(LOCKTAG_RELATION_EXTEND);
	e->counters.xact_lock_wait += LOCK_WAIT_TIMING(LOCKTAG_TRANSACTION);
	e->counters.xact_lock_wait += LOCK_WAIT_TIMING(LOCKTAG_VIRTUALTRANSACTION);
	e->counters.xact_lock_wait += LOCK_WAIT_TIMING(LOCKTAG_SPECULATIVE_TOKEN);
	e->counters.page_lock_wait += LOCK_WAIT_TIMING(LOCKTAG_PAGE);
	e->counters.tuple_lock_wait += LOCK_WAIT_TIMING(LOCKTAG_TUPLE);
	
	e->counters.shared_read_ps += io_stat_info->shared_read_ps;
	e->counters.shared_write_ps += io_stat_info->shared_write_ps;

	e->counters.shared_read_throughput += io_stat_info->shared_read_throughput;
	e->counters.shared_write_throughput += io_stat_info->shared_write_throughput;

	e->counters.shared_read_latency += io_stat_info->shared_read_latency;
	e->counters.shared_write_latency += io_stat_info->shared_write_latency;

	e->counters.io_open_num += io_stat_info->io_open_num;
	e->counters.io_seek_count += io_stat_info->io_seek_count;

	e->counters.io_open_time += io_stat_info->io_open_time;
	e->counters.io_seek_time += io_stat_info->io_seek_time;

	SpinLockRelease(&e->mutex);

	LWLockRelease(pgss->lock);
}

/*
 * Allocate a new hashtable entry.
 * caller must hold an exclusive lock on pgss->lock
 */
static pgssEntry *pgss_entry_alloc(pgssHashKey *key)
{
	pgssEntry  *entry;
	bool		found;

	/* Make space if needed */
	while (hash_get_num_entries(polar_stat_sql_hash) >= pgss_max)
		pgss_entry_dealloc();

	/* Find or create an entry with desired hash code */
	entry = (pgssEntry *) hash_search(polar_stat_sql_hash, key, HASH_ENTER, &found);

	if (!found)
	{
		/* New entry, initialize it */

		/* reset the statistics */
		memset(&entry->counters, 0, sizeof(pgssCounters));
		/* set the appropriate initial usage count */
		entry->counters.usage = USAGE_INIT;
		/* re-initialize the mutex each time ... we assume no one using it */
		SpinLockInit(&entry->mutex);
	}

	return entry;
}

/*
 * qsort comparator for sorting into increasing usage order
 */
static int
entry_cmp(const void *lhs, const void *rhs)
{
	double		l_usage = (*(pgssEntry *const *) lhs)->counters.usage;
	double		r_usage = (*(pgssEntry *const *) rhs)->counters.usage;

	if (l_usage < r_usage)
		return -1;
	else if (l_usage > r_usage)
		return +1;
	else
		return 0;
}

/*
 * Deallocate least used entries.
 * Caller must hold an exclusive lock on pgss->lock.
 */
static void
pgss_entry_dealloc(void)
{
	HASH_SEQ_STATUS hash_seq;
	pgssEntry **entries;
	pgssEntry  *entry;
	int			nvictims;
	int			i;

	/*
	 * Sort entries by usage and deallocate USAGE_DEALLOC_PERCENT of them.
	 * While we're scanning the table, apply the decay factor to the usage
	 * values.
	 */
	entries = palloc(hash_get_num_entries(polar_stat_sql_hash) * sizeof(pgssEntry *));

	i = 0;
	hash_seq_init(&hash_seq, polar_stat_sql_hash);
	while ((entry = hash_seq_search(&hash_seq)) != NULL)
	{
		entries[i++] = entry;
		entry->counters.usage *= USAGE_DECREASE_FACTOR;
	}

	qsort(entries, i, sizeof(pgssEntry *), entry_cmp);

	nvictims = Max(10, i * USAGE_DEALLOC_PERCENT / 100);
	nvictims = Min(nvictims, i);

	for (i = 0; i < nvictims; i++)
	{
		hash_search(polar_stat_sql_hash, &entries[i]->key, HASH_REMOVE, NULL);
	}

	pfree(entries);
}

static void pgss_entry_reset(void)
{
	HASH_SEQ_STATUS hash_seq;
	pgssEntry  *entry;

	LWLockAcquire(pgss->lock, LW_EXCLUSIVE);

	hash_seq_init(&hash_seq, polar_stat_sql_hash);
	while ((entry = hash_seq_search(&hash_seq)) != NULL)
	{
		hash_search(polar_stat_sql_hash, &entry->key, HASH_REMOVE, NULL);
	}

	LWLockRelease(pgss->lock);
}

/*
 * Hooks
 */

static void
polar_stat_ExecutorStart (QueryDesc *queryDesc, int eflags)
{

	is_sql_sampled = (random() <= (MAX_RANDOM_VALUE * polar_ss_sample_rate));
	is_stat_enable = polar_ss_enable_stat;

	if (is_stat_enable && queryDesc->polar_sql_info == NULL) 
	{
		queryDesc->polar_sql_info = palloc0(sizeof(PolarStatSqlCollector));
	}

	if (is_stat_enable && is_sql_sampled) {
 
		/* capture kernel usage stats in rusage_start */
		if (polar_ss_getuage)
			getrusage(RUSAGE_SELF, &(queryDesc->polar_sql_info->rusage_start));
	

		if (polar_enable_track_sql_time_stat)
		{
			INSTR_TIME_SET_CURRENT(queryDesc->polar_sql_info->polar_execute_start);
			
		}
		
		/*
		* Enable per-node instrumentation if plan execute info is required.
		* We need set the instrument_options before the function standard_ExecutorStart
		*/
		if (queryDesc->plannedstmt->queryId != UINT64CONST(0) 
			&& polar_ss_gather_plan_info)
		{
			queryDesc->instrument_options |= INSTRUMENT_POLAR_PLAN;
			if (polar_ss_plan_need_time)
				queryDesc->instrument_options |= INSTRUMENT_TIMER;
				
		}

		queryDesc->polar_sql_info->lwlock_stat_start = polar_lwlock_stat_local_summary;
		memcpy(queryDesc->polar_sql_info->lock_stats_table_start, polar_lock_stat_local_summary,
				sizeof(queryDesc->polar_sql_info->lock_stats_table_start));

		polar_collect_io_stat(&queryDesc->polar_sql_info->start_io_stat, MyBackendId);
	}

	/* give control back to PostgreSQL */
	if (prev_ExecutorStart)
		prev_ExecutorStart(queryDesc, eflags);
	else
		standard_ExecutorStart(queryDesc, eflags);
}

/*
 * ExecutorRun hook: all we need do is track nesting depth
 */
static void
polar_stat_ExecutorRun(QueryDesc *queryDesc, ScanDirection direction, uint64 count,
				 bool execute_once)
{
	nested_level++;
	PG_TRY();
	{
		if (prev_ExecutorRun)
			prev_ExecutorRun(queryDesc, direction, count, execute_once);
		else
			standard_ExecutorRun(queryDesc, direction, count, execute_once);
		nested_level--;
	}
	PG_CATCH();
	{
		nested_level--;
		PG_RE_THROW();
	}
	PG_END_TRY();
}

/*
 * ExecutorFinish hook: all we need do is track nesting depth
 */
static void
polar_stat_ExecutorFinish(QueryDesc *queryDesc)
{
	nested_level++;
	PG_TRY();
	{
		if (prev_ExecutorFinish)
			prev_ExecutorFinish(queryDesc);
		else
			standard_ExecutorFinish(queryDesc);
		nested_level--;
	}
	PG_CATCH();
	{
		nested_level--;
		PG_RE_THROW();
	}
	PG_END_TRY();
}


/*
 * ProcessUtility hook
 */
static void
polar_stat_ProcessUtility(PlannedStmt *pstmt, const char *queryString,
					ProcessUtilityContext context,
					ParamListInfo params, QueryEnvironment *queryEnv,
					DestReceiver *dest, char *completionTag)
{
		nested_level++;
		PG_TRY();
		{
			if (prev_ProcessUtility)
				prev_ProcessUtility(pstmt, queryString,
									context, params, queryEnv,
									dest, completionTag);
			else
				standard_ProcessUtility(pstmt, queryString,
										context, params, queryEnv,
										dest, completionTag);
			nested_level--;
		}
		PG_CATCH();
		{
			nested_level--;
			PG_RE_THROW();
		}
		PG_END_TRY();
}

static void
polar_stat_ExecutorEnd (QueryDesc *queryDesc)
{
	pgss_queryid queryId;
	struct rusage rusage_start;
	struct rusage rusage_end;
	pgssCounters counters;
	pgssPlanInfo planInfo = {0};
	PolarCollectIoStat start_io_stat = {0};
	PolarCollectIoStat end_io_stat = {0};
	PolarCollectIoStat interval_io_stat = {0};
	

	if (is_stat_enable && is_sql_sampled) {
		
		/* capture kernel usage stats in rusage_end */
		if (polar_ss_getuage)
			getrusage(RUSAGE_SELF, &rusage_end);

		if (polar_enable_track_sql_time_stat 
			&& ! (INSTR_TIME_IS_ZERO(queryDesc->polar_sql_info->polar_execute_start)))
		{
			instr_time end;
			uint64 diff;
			INSTR_TIME_SET_CURRENT(end);
			diff = INSTR_TIME_GET_MICROSEC(end) - INSTR_TIME_GET_MICROSEC(queryDesc->polar_sql_info->polar_execute_start);
			polar_sql_time_stat_local_summary.execute_time = diff;
		}

		/* gather plan nodes information */
		if (polar_ss_gather_plan_info)
			pgss_stat_plan_node(queryDesc->planstate, &planInfo);

		queryId = queryDesc->plannedstmt->queryId;

		rusage_start = queryDesc->polar_sql_info->rusage_start;
		/* Compute CPU time delta */
		counters.utime = 1000.0 * TIMEVAL_DIFF(rusage_start.ru_utime, rusage_end.ru_utime);
		counters.stime = 1000.0 * TIMEVAL_DIFF(rusage_start.ru_stime, rusage_end.ru_stime);

		if (queryDesc->totaltime)
		{
			/* Make sure stats accumulation is done */
			InstrEndLoop(queryDesc->totaltime);

			/*
			* We only consider values greater than 3 * linux tick, otherwise the
			* bias is too big
			*/
			if (queryDesc->totaltime->total < (3. / pgss_linux_hz))
			{
				counters.stime = 0;
				counters.utime = 1000.0 * queryDesc->totaltime->total; 
			}
		}

		/* Compute the rest of the counters */
		counters.minflts = rusage_end.ru_minflt - rusage_start.ru_minflt;
		counters.majflts = rusage_end.ru_majflt - rusage_start.ru_majflt;
		counters.nswaps = rusage_end.ru_nswap - rusage_start.ru_nswap;
		counters.reads = rusage_end.ru_inblock - rusage_start.ru_inblock;
		counters.writes = rusage_end.ru_oublock - rusage_start.ru_oublock;
		counters.msgsnds = rusage_end.ru_msgsnd - rusage_start.ru_msgsnd;
		counters.msgrcvs = rusage_end.ru_msgrcv - rusage_start.ru_msgrcv;
		counters.nsignals = rusage_end.ru_nsignals - rusage_start.ru_nsignals;
		counters.nvcsws = rusage_end.ru_nvcsw - rusage_start.ru_nvcsw;
		counters.nivcsws = rusage_end.ru_nivcsw - rusage_start.ru_nivcsw;

		

		/* If disable getuage, memset the counters */
		if (!polar_ss_getuage)
			memset(&counters, 0, sizeof(counters));

		polar_collect_io_stat(&end_io_stat, MyBackendId);
		start_io_stat = queryDesc->polar_sql_info->start_io_stat;

		interval_io_stat.shared_read_ps = end_io_stat.shared_read_ps - start_io_stat.shared_read_ps;
		interval_io_stat.shared_write_ps = end_io_stat.shared_write_ps - start_io_stat.shared_write_ps;
		interval_io_stat.shared_read_throughput = end_io_stat.shared_read_throughput - start_io_stat.shared_read_throughput;
		interval_io_stat.shared_write_throughput = end_io_stat.shared_write_throughput - start_io_stat.shared_write_throughput;
		interval_io_stat.shared_read_latency = end_io_stat.shared_read_latency - start_io_stat.shared_read_latency;
		interval_io_stat.shared_write_latency = end_io_stat.shared_write_latency - start_io_stat.shared_write_latency;

		interval_io_stat.io_open_num = end_io_stat.io_open_num - start_io_stat.io_open_num;
		interval_io_stat.io_seek_count = end_io_stat.io_seek_count - start_io_stat.io_seek_count;
		interval_io_stat.io_open_time = end_io_stat.io_open_time - start_io_stat.io_open_time;
		interval_io_stat.io_seek_time = end_io_stat.io_seek_time - start_io_stat.io_seek_time;

		/* store current number of block reads and writes */
		pgss_entry_store(queryId, counters, &planInfo, &interval_io_stat, queryDesc);

		pfree(queryDesc->polar_sql_info);
		queryDesc->polar_sql_info = NULL;
	}

	/* give control back to PostgreSQL */
	if (prev_ExecutorEnd)
		prev_ExecutorEnd(queryDesc);
	else
		standard_ExecutorEnd(queryDesc);

}

/*
 * get hash nodes memory information.
 */
static int64
get_hash_memory_info(PlanState  *planstate)
{
	HashState *hashstate = (HashState *)planstate;
	int64 spacePeakKb = 0;
	HashInstrumentation hinstrument = {0};
	/*
	 * In a parallel query, the leader process may or may not have run the
	 * hash join, and even if it did it may not have built a hash table due to
	 * timing (if it started late it might have seen no tuples in the outer
	 * relation and skipped building the hash table).  Therefore we have to be
	 * prepared to get instrumentation data from all participants.
	 */
	if (hashstate->hashtable)
	{
		hinstrument.nbatch = hashstate->hashtable->nbatch;
		hinstrument.space_peak = hashstate->hashtable->spacePeak;
	}
	/*
	 * Merge results from workers.  In the parallel-oblivious case, the
	 * results from all participants should be identical, except where
	 * participants didn't run the join at all so have no data.  In the
	 * parallel-aware case, we need to consider all the results.  Each worker
	 * may have seen a different subset of batches and we want to find the
	 * highest memory usage for any one batch across all batches.
	 */
	if (hashstate->shared_info)
	{
		SharedHashInfo *shared_info = hashstate->shared_info;
		int			i;
		for (i = 0; i < shared_info->num_workers; ++i)
		{
			HashInstrumentation *worker_hi = &shared_info->hinstrument[i];
			if (!worker_hi)
			{
				continue;
			}
			if (worker_hi->nbatch > 0)
			{
				/*
				 * Normally every participant should agree on the number of
				 * batches too, but it's possible for a backend that started
				 * late and missed the whole join not to have the final nbatch
				 * number.  So we'll take the largest number.
				 */
				hinstrument.nbatch = Max(hinstrument.nbatch, worker_hi->nbatch);
				/*
				 * In a parallel-aware hash join, for now we report the
				 * maximum peak memory reported by any worker.
				 */
				hinstrument.space_peak =
					Max(hinstrument.space_peak, worker_hi->space_peak);
			}
		}
	}
	if (hinstrument.nbatch > 0)
	{
		spacePeakKb = (hinstrument.space_peak + 1023) / 1024;
	}
	return spacePeakKb;
}

/*
 * Statistical execution plan node information
 */
static void
pgss_stat_plan_node(PlanState  *planstate, pgssPlanInfo  *planInfo)
{
	Plan *plan;

	/*
	 * if planstate or instrument is null, we should return.  
	 */
	if (!planstate || !planstate->instrument)
		return;

	plan = planstate->plan;
	/*
	* Make sure stats accumulation is done.  
	*/
	InstrEndLoop(planstate->instrument);
	/*
	* In version 1.9, we only gather information about scan, join, sort agg/group_by hash nodes 
	*/
	switch (nodeTag(plan))
	{
		case T_Scan:
		case T_SeqScan:
		case T_SampleScan:
		case T_IndexScan:
		case T_IndexOnlyScan:
		case T_BitmapIndexScan:
		case T_BitmapHeapScan:
		case T_TidScan:
		case T_SubqueryScan:
		case T_FunctionScan:
		case T_ValuesScan:
		case T_TableFuncScan:
		case T_CteScan:
		case T_NamedTuplestoreScan:
		case T_WorkTableScan:
		case T_ForeignScan:
		case T_CustomScan:
			planInfo->scan_rows += planstate->instrument->ntuples;
			planInfo->scan_time += 1000.0
				* planstate->instrument->total;   /* Total scan_time (in milliseconds) */
			planInfo->scan_count += 1;
			break;
		case T_NestLoop:
		case T_MergeJoin:
		case T_HashJoin: planInfo->join_rows += planstate->instrument->ntuples;
			planInfo->join_time += 1000.0
				* planstate->instrument->total;   /* Total join_time (in milliseconds) */
			planInfo->join_count += 1;
			break;
		case T_Sort: planInfo->sort_rows += planstate->instrument->ntuples;
			planInfo->sort_time += 1000.0
				* planstate->instrument->total;   /* Total sort_time (in milliseconds) */
			planInfo->sort_count += 1;
			break;
		case T_Group:
		case T_Agg: planInfo->group_rows += planstate->instrument->ntuples;
			planInfo->group_time += 1000.0
				* planstate->instrument->total;   /* Total total time (in milliseconds) */
			planInfo->group_count += 1;
			break;
		case T_Hash: planInfo->hash_rows += planstate->instrument->ntuples;
			planInfo->hash_memory += get_hash_memory_info(planstate);   /* KB */
			planInfo->hash_count += 1;
			break;
		default: break;
	}

	/* initPlan-s */
	if (planstate->initPlan)
		pgss_stat_subplan_node(planstate->initPlan, planInfo);

	/* lefttree */
	if (outerPlanState(planstate))
		pgss_stat_plan_node(outerPlanState(planstate), planInfo);
	/* righttree */
	if (innerPlanState(planstate))
		pgss_stat_plan_node(innerPlanState(planstate), planInfo);
	/* subPlan-s */
	if (planstate->subPlan)
		pgss_stat_subplan_node(planstate->subPlan, planInfo);
}

/*
 * Statistical execution sub plan node information
 */
static void
pgss_stat_subplan_node(List *plans, pgssPlanInfo  *planInfo)
{
	ListCell   *lst;
	
	foreach(lst, plans)
	{
		SubPlanState *sps = (SubPlanState *) lfirst(lst);
		pgss_stat_plan_node(sps->planstate, planInfo);
	}
}

/*
 * Calculate hash value for a key
 */
static uint32
polar_stat_sql_hash_fn(const void *key, Size keysize)
{
	const pgssHashKey *k = (const pgssHashKey *) key;

	return hash_uint32((uint32) k->userid) ^
		hash_uint32((uint32) k->dbid) ^
		hash_uint32((uint32) k->queryid);
}

/*
 * Compare two keys - zero means match
 */
static int
pgss_match_fn(const void *key1, const void *key2, Size keysize)
{
	const pgssHashKey *k1 = (const pgssHashKey *) key1;
	const pgssHashKey *k2 = (const pgssHashKey *) key2;

	if (k1->userid == k2->userid &&
		k1->dbid == k2->dbid &&
		k1->queryid == k2->queryid)
		return 0;
	else
		return 1;
}

/*
 * Reset statistics.
 */
PGDLLEXPORT Datum
polar_stat_sql_reset(PG_FUNCTION_ARGS)
{
	if (!pgss)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("polar_stat_sql must be loaded via shared_preload_libraries")));

	pgss_entry_reset();
	PG_RETURN_VOID();
}

PGDLLEXPORT Datum
polar_stat_sql(PG_FUNCTION_ARGS)
{
	polar_stat_sql_internal(fcinfo, PGSS_V1_0);

	return (Datum) 0;
}

PGDLLEXPORT Datum
polar_stat_sql_1_0(PG_FUNCTION_ARGS)
{
	polar_stat_sql_internal(fcinfo, PGSS_V1_0);

	return (Datum) 0;
}

PGDLLEXPORT Datum
polar_stat_sql_1_1(PG_FUNCTION_ARGS)
{
	polar_stat_sql_internal(fcinfo, PGSS_V1_1);

	return (Datum) 0;
}

static void
polar_stat_sql_internal(FunctionCallInfo fcinfo, pgssVersion api_version)
{
	ReturnSetInfo	*rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	MemoryContext	per_query_ctx;
	MemoryContext	oldcontext;
	TupleDesc		tupdesc;
	Tuplestorestate	*tupstore;
	HASH_SEQ_STATUS hash_seq;
	pgssEntry		*entry;


	if (!pgss)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("polar_stat_sql must be loaded via shared_preload_libraries")));
	/* check to see if caller supports us returning a tuplestore */
	if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("set-valued function called in context that cannot accept a set")));
	if (!(rsinfo->allowedModes & SFRM_Materialize))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("materialize mode required, but it is not " \
							"allowed in this context")));

	/* Switch into long-lived context to construct returned data structures */
	per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
	oldcontext = MemoryContextSwitchTo(per_query_ctx);

	/* Build a tuple descriptor for our result type */
	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	tupstore = tuplestore_begin_heap(true, false, work_mem);
	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = tupstore;
	rsinfo->setDesc = tupdesc;

	MemoryContextSwitchTo(oldcontext);

	LWLockAcquire(pgss->lock, LW_SHARED);

	hash_seq_init(&hash_seq, polar_stat_sql_hash);
	while ((entry = hash_seq_search(&hash_seq)) != NULL)
	{
		Datum			values[COL_POLAR_STAT_SQL_MAX];
		bool			nulls[COL_POLAR_STAT_SQL_MAX];
		pgssCounters	tmp;

		int64			reads, writes;

		memset(values, 0, sizeof(values));
		memset(nulls, 0, sizeof(nulls));

		/* copy counters to a local variable to keep locking time short */
		{
			volatile pgssEntry *e = (volatile pgssEntry *) entry;

			SpinLockAcquire(&e->mutex);
			tmp = e->counters;
			SpinLockRelease(&e->mutex);
		}

		values[COL_QUERY_ID] = Int64GetDatum(entry->key.queryid);
		values[COL_USER_ID] = ObjectIdGetDatum(entry->key.userid);
		values[COL_DB_ID] = ObjectIdGetDatum(entry->key.dbid);

		reads = tmp.reads * RUSAGE_BLOCK_SIZE;
		writes = tmp.writes * RUSAGE_BLOCK_SIZE;
		values[COL_READS] = Int64GetDatumFast(reads);
		values[COL_WRITES] = Int64GetDatumFast(writes);

		values[COL_USER_TIME] = Float8GetDatumFast(tmp.utime);
		values[COL_SYSTEM_TIME] = Float8GetDatumFast(tmp.stime);

		values[COL_MINFLTS] = Int64GetDatumFast(tmp.minflts);
		values[COL_MAJFLTS] = Int64GetDatumFast(tmp.majflts);
		values[COL_NSWAPS] = Int64GetDatumFast(tmp.nswaps);
		values[COL_MSGSNDS] = Int64GetDatumFast(tmp.msgsnds);
		values[COL_MSGRCVS] = Int64GetDatumFast(tmp.msgrcvs);
		values[COL_NSIGNALS] = Int64GetDatumFast(tmp.nsignals);
		values[COL_NVCSWS] = Int64GetDatumFast(tmp.nvcsws);
		values[COL_NIVCSWS] = Int64GetDatumFast(tmp.nivcsws);
		
		values[COL_SCAN_ROWS] = Float8GetDatumFast(tmp.scan_rows);
		values[COL_SCAN_TIME] = Float8GetDatumFast(tmp.scan_time);
		values[COL_SCAN_COUNT] = Int64GetDatumFast(tmp.scan_count);

		values[COL_JOIN_ROWS] = Float8GetDatumFast(tmp.join_rows);
		values[COL_JOIN_TIME] = Float8GetDatumFast(tmp.join_time);
		values[COL_JOIN_COUNT] = Int64GetDatumFast(tmp.join_count);

		values[COL_SORT_ROWS] = Float8GetDatumFast(tmp.sort_rows);
		values[COL_SORT_TIME] = Float8GetDatumFast(tmp.sort_time);
		values[COL_SORT_COUNT] = Int64GetDatumFast(tmp.sort_count);

		values[COL_GROUP_ROWS] = Float8GetDatumFast(tmp.group_rows);
		values[COL_GROUP_TIME] = Float8GetDatumFast(tmp.group_time);
		values[COl_GROUP_COUNT] = Int64GetDatumFast(tmp.group_count);

		values[COL_HASH_ROWS] = Float8GetDatumFast(tmp.hash_rows);
		values[COL_HASH_MEMORY] = Int64GetDatumFast(tmp.hash_memory);
		values[COL_HASH_COUNT] = Int64GetDatumFast(tmp.hash_count);

		if (api_version >= PGSS_V1_1)
		{
			values[COL_PARSE_TIME] = Float8GetDatumFast(tmp.parse_time);
			values[COL_ANALYZE_TIME] = Float8GetDatumFast(tmp.analyze_time);
			values[COL_REWRITE_TIME] = Float8GetDatumFast(tmp.rewrite_time);
			values[COL_PLAN_TIME] = Float8GetDatumFast(tmp.plan_time);
			values[COL_EXECUTE_TIME] = Float8GetDatumFast(tmp.execute_time);

			values[COL_LWLOCK_WAIT] = Float8GetDatumFast(tmp.lwlock_wait);
			values[COL_REL_LOCK_WAIT] = Float8GetDatumFast(tmp.rel_lock_wait);
			values[COL_XACT_LOCK_WAIT] = Float8GetDatumFast(tmp.xact_lock_wait);
			values[COL_PAGE_LOCK_WAIT] = Float8GetDatumFast(tmp.page_lock_wait);
			values[COL_TUPLE_LOCK_WAIT] = Float8GetDatumFast(tmp.tuple_lock_wait);

			values[COL_SHARED_READ_PS] = Int64GetDatumFast(tmp.shared_read_ps);
			values[COL_SHARED_WRITE_PS] = Int64GetDatumFast(tmp.shared_write_ps);
			values[COL_SHARED_READ_THROUGHPUT] = Int64GetDatumFast(tmp.shared_read_throughput);
			values[COL_SHARED_WRITE_THROUGHPUT] = Int64GetDatumFast(tmp.shared_write_throughput);
			values[COL_SHARED_READ_LATENCY] = Float8GetDatumFast(tmp.shared_read_latency);
			values[COL_SHARED_WRITE_LATENCY] = Float8GetDatumFast(tmp.shared_write_latency);
			values[COL_IO_OPEN_NUM] = Int64GetDatumFast(tmp.io_open_num);
			values[COL_IO_SEEK_COUNT] = Int64GetDatumFast(tmp.io_seek_count);
			values[COL_IO_OPEN_TIME] = Float8GetDatumFast(tmp.io_open_time);
			values[COL_IO_SEEK_TIME] = Float8GetDatumFast(tmp.io_seek_time);

		}

		tuplestore_putvalues(tupstore, tupdesc, values, nulls);
	}

	LWLockRelease(pgss->lock);

	/* clean up and return the tuplestore */
	tuplestore_donestoring(tupstore);
}

static void
pgsk_post_parse_analyze(ParseState *pstate, Query *query)
{
	if (prev_post_parse_analyze_hook)
		prev_post_parse_analyze_hook(pstate, query);

	/*
	 * Record call times of each query type
	 */
	record_query_type(query);
}

/*
 * Add various query type to tuple.
 */
static void
add_various_tuple(Tuplestorestate *state, TupleDesc tdesc, char* sqltype, char* cmdtype,
					 const int arraylen, int64 count, Datum *values, 
					 bool *nulls, int array_size)
{
	int col = 0;

	MemSet(values, 0, sizeof(Datum) * array_size);
	MemSet(nulls, 0, sizeof(bool) * array_size);
	values[col++] = CStringGetTextDatum(sqltype);
	values[col++] = CStringGetTextDatum(cmdtype);
	values[col++] = Int64GetDatumFast(count);
	Assert(col <= arraylen);
	tuplestore_putvalues(state, tdesc, values, nulls);
}

/*
 * Record call times of each query type
 */
static void
record_query_type(Query *query)
{
	if(polar_pgsk_qps_monitor)
	{
		switch(query->commandType){
			case CMD_SELECT:
				pg_atomic_add_fetch_u64(&(query_type_stat->select_count),1);
				break;
			case CMD_UPDATE:
				pg_atomic_add_fetch_u64(&(query_type_stat->update_count),1);
				break;
			case CMD_INSERT:
				pg_atomic_add_fetch_u64(&(query_type_stat->insert_count),1);
				break;
			case CMD_DELETE:
				pg_atomic_add_fetch_u64(&(query_type_stat->delete_count),1);
				break;
			case CMD_UTILITY:
				Assert(query->utilityStmt != NULL);
				switch (query->utilityStmt->type)
				{
					case T_ExplainStmt:
						pg_atomic_add_fetch_u64(&(query_type_stat->explain_count),1);
						break;
					case T_CreateStmt:
						pg_atomic_add_fetch_u64(&(query_type_stat->create_count),1);
						break;
					case T_DropStmt:
						pg_atomic_add_fetch_u64(&(query_type_stat->drop_count),1);
						break;
					case T_RenameStmt:
						pg_atomic_add_fetch_u64(&(query_type_stat->rename_count),1);
						break;
					case T_TruncateStmt:
						pg_atomic_add_fetch_u64(&(query_type_stat->truncate_count),1);
						break;
					case T_CommentStmt:
						pg_atomic_add_fetch_u64(&(query_type_stat->comment_count),1);
						break;
					case T_GrantStmt:
						pg_atomic_add_fetch_u64(&(query_type_stat->grant_count),1);
						break;
					case T_LockStmt:
						pg_atomic_add_fetch_u64(&(query_type_stat->lock_count),1);
						break;
					case T_AlterTableStmt:
					case T_AlterTableCmd:
					case T_AlterDomainStmt:
					case T_AlterDefaultPrivilegesStmt:
					case T_AlterFunctionStmt:
					case T_AlterSeqStmt:
					case T_AlterRoleStmt:
					case T_AlterDatabaseStmt:
					case T_AlterDatabaseSetStmt:
					case T_AlterRoleSetStmt:
					case T_AlterOpFamilyStmt:
					case T_AlterObjectDependsStmt:
					case T_AlterObjectSchemaStmt:
					case T_AlterOwnerStmt:
					case T_AlterOperatorStmt:
					case T_AlterEnumStmt:
					case T_AlterTSDictionaryStmt:
					case T_AlterTSConfigurationStmt:
					case T_AlterFdwStmt:
					case T_AlterForeignServerStmt:
					case T_AlterUserMappingStmt:
					case T_AlterTableSpaceOptionsStmt:
					case T_AlterTableMoveAllStmt:
					case T_AlterExtensionStmt:
					case T_AlterExtensionContentsStmt:
					case T_AlterEventTrigStmt:
					case T_AlterSystemStmt:
					case T_AlterPolicyStmt:
					case T_AlterPublicationStmt:
					case T_AlterSubscriptionStmt:
					case T_AlterCollationStmt:
						pg_atomic_add_fetch_u64(&(query_type_stat->alter_count),1);
						break;
					default:
						break;
				}
				break;
			default:
				break;
		}	
	}
}

PG_FUNCTION_INFO_V1(polar_stat_query_count);
Datum
polar_stat_query_count(PG_FUNCTION_ARGS)
{
	const int POLAR_QUERYTYPE_STAT_COLS = 3;
	int  			index = 1;
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	TupleDesc	tupdesc;
	Tuplestorestate *tupstore;
	MemoryContext per_query_ctx;
	MemoryContext oldcontext;

	Datum		values[POLAR_QUERYTYPE_STAT_COLS];
	bool		nulls[POLAR_QUERYTYPE_STAT_COLS];

	/* check to see if caller supports us returning a tuplestore */
	if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("set-valued function called in context that cannot accept a set")));
	if (!(rsinfo->allowedModes & SFRM_Materialize))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("materialize mode required, but it is not " \
						"allowed in this context")));

	tupdesc = CreateTemplateTupleDesc(POLAR_QUERYTYPE_STAT_COLS, false);
	TupleDescInitEntry(tupdesc, (AttrNumber) index++, "sql_type",
						TEXTOID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) index++, "cmp_type",
						TEXTOID, -1, 0);					
	TupleDescInitEntry(tupdesc, (AttrNumber) index++, "count",
						INT8OID, -1, 0);
	Assert(index - 1 <= POLAR_QUERYTYPE_STAT_COLS);
	per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
	oldcontext = MemoryContextSwitchTo(per_query_ctx);

	tupstore = tuplestore_begin_heap(true, false, work_mem);
	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = tupstore;
	rsinfo->setDesc = tupdesc;

	MemoryContextSwitchTo(oldcontext);

	/* DQL */
	add_various_tuple(tupstore, tupdesc, "DQL", "SELECT", POLAR_QUERYTYPE_STAT_COLS,
					 pg_atomic_read_u64(&(query_type_stat->select_count)), 
					 values, nulls, POLAR_QUERYTYPE_STAT_COLS);
	/* DML */
	add_various_tuple(tupstore, tupdesc, "DML", "INSERT", POLAR_QUERYTYPE_STAT_COLS,
					 pg_atomic_read_u64(&(query_type_stat->insert_count)), 
					 values, nulls, POLAR_QUERYTYPE_STAT_COLS);
	add_various_tuple(tupstore, tupdesc, "DML", "UPDATE", POLAR_QUERYTYPE_STAT_COLS,
					 pg_atomic_read_u64(&(query_type_stat->update_count)), 
					 values, nulls, POLAR_QUERYTYPE_STAT_COLS);
	add_various_tuple(tupstore, tupdesc, "DML", "DELETE", POLAR_QUERYTYPE_STAT_COLS,
					 pg_atomic_read_u64(&(query_type_stat->delete_count)), 
					 values, nulls, POLAR_QUERYTYPE_STAT_COLS);
	add_various_tuple(tupstore, tupdesc, "DML", "EXPLAIN", POLAR_QUERYTYPE_STAT_COLS,
					 pg_atomic_read_u64(&(query_type_stat->explain_count)), 
					 values, nulls, POLAR_QUERYTYPE_STAT_COLS);
	add_various_tuple(tupstore, tupdesc, "DML", "LOCKTABLE", POLAR_QUERYTYPE_STAT_COLS,
					 pg_atomic_read_u64(&(query_type_stat->lock_count)), 
					 values, nulls, POLAR_QUERYTYPE_STAT_COLS);
	/* DDL */
	add_various_tuple(tupstore, tupdesc, "DDL", "CREATE", POLAR_QUERYTYPE_STAT_COLS,
					 pg_atomic_read_u64(&(query_type_stat->create_count)), 
					 values, nulls, POLAR_QUERYTYPE_STAT_COLS);
	add_various_tuple(tupstore, tupdesc, "DDL", "DROP", POLAR_QUERYTYPE_STAT_COLS,
					 pg_atomic_read_u64(&(query_type_stat->drop_count)), 
					 values, nulls, POLAR_QUERYTYPE_STAT_COLS);
	add_various_tuple(tupstore, tupdesc, "DDL", "RENAME", POLAR_QUERYTYPE_STAT_COLS,
					 pg_atomic_read_u64(&(query_type_stat->rename_count)), 
					 values, nulls, POLAR_QUERYTYPE_STAT_COLS);
	add_various_tuple(tupstore, tupdesc, "DDL", "TRUNCATE", POLAR_QUERYTYPE_STAT_COLS,
					 pg_atomic_read_u64(&(query_type_stat->truncate_count)), 
					 values, nulls, POLAR_QUERYTYPE_STAT_COLS);
	add_various_tuple(tupstore, tupdesc, "DDL", "COMMENT", POLAR_QUERYTYPE_STAT_COLS,
					 pg_atomic_read_u64(&(query_type_stat->comment_count)), 
					 values, nulls, POLAR_QUERYTYPE_STAT_COLS);
	add_various_tuple(tupstore, tupdesc, "DDL", "ALTER", POLAR_QUERYTYPE_STAT_COLS,
					 pg_atomic_read_u64(&(query_type_stat->alter_count)), 
					 values, nulls, POLAR_QUERYTYPE_STAT_COLS);
	/* DCL */
	add_various_tuple(tupstore, tupdesc, "DCL", "GRANT", POLAR_QUERYTYPE_STAT_COLS,
					 pg_atomic_read_u64(&(query_type_stat->grant_count)), 
					 values, nulls, POLAR_QUERYTYPE_STAT_COLS);

	/* clean up and return the tuplestore */
	tuplestore_donestoring(tupstore);
	
	return (Datum) 0;
}
