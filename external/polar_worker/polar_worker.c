/*-------------------------------------------------------------------------
 *
 * polar_worker.c
 *		Do some backgroud things for polardb periodically. Such as:
 *		(1) auto prealloc wal files, (2) auto clean core dump files,
 * 		(3) auto clean xlog temp files.
 *
 *	Copyright (c) 2019, Alibaba.inc
 *
 *	IDENTIFICATION
 *		external/polar_worker/polar_worker.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <unistd.h>

#include "miscadmin.h"
#include "pgstat.h"

#include "access/polar_fullpage.h"
#include "access/polar_logindex.h"
#include "access/polar_logindex_internal.h"
#include "access/xlog.h"
#include "access/xlog_internal.h"
#include "common/file_perm.h"
#include "postmaster/bgworker.h"
#include "postmaster/startup.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/lwlock.h"
#include "storage/polar_fd.h"
#include "storage/proc.h"
#include "storage/procsignal.h"
#include "utils/guc.h"
#include "utils/polar_coredump.h"
#include "utils/timestamp.h"

PG_MODULE_MAGIC;

#define POLAR_MAX_BUFF_SIZE 1024
#define POLAR_NUM_FILE_INTERVAL 32
#define POLAR_XLOG_TEMP_FILE_SUFFIX "xlogtemp"

#define POLAR_DATA_DIR() (POLAR_FILE_IN_SHARED_STORAGE() ? polar_datadir : DataDir)
#define POLAR_FILE_PATH(path, orign) \
	snprintf((path), MAXPGPATH, "%s/%s", POLAR_DATA_DIR(), (orign));

typedef struct
{
	time_t mtime;
	char file_path[MAXPGPATH];
} polar_file_descriptor;

void		_PG_init(void);
void		polar_worker_handler_main(Datum main_arg);

static void start_polar_worker(void);
static void prealloc_wal_files(void);
static void polar_worker_sighup_handler(SIGNAL_ARGS);
static void polar_worker_sigterm_handler(SIGNAL_ARGS);
static void polar_xlog_temp_file_clean(void);

/* Flags set by signal handlers */
static volatile sig_atomic_t got_sigterm = false;
static volatile sig_atomic_t got_sighup = false;

/* GUC variables. */
static bool enable_polar_worker = true; /* Start prealloc wal file worker? */
static int	polar_worker_check_interval;	/* Check interval */
static int	prealloc_wal_file_num;	/* How many files needed to prealloc */
static char *polar_core_file_path;
static char *core_name_suffix;
static int	core_file_outdate_time;
static int	num_corefile_reserved_old;
static int	num_corefile_reserved_new;
static int  xlog_temp_outdate_time;

/*
 * Module load callback.
 */
void
_PG_init(void)
{
	DefineCustomIntVariable("polar_worker.polar_worker_check_interval",
							"Sets the interval between polar worker check",
							"If set to zero, polar worker is disabled.",
							&polar_worker_check_interval,
							5,
							0, INT_MAX / 1000,
							PGC_SIGHUP,
							GUC_UNIT_S,
							NULL,
							NULL,
							NULL);

	DefineCustomIntVariable("polar_worker.prealloc_wal_file_num",
							"Sets the num of how many prealloc wal file",
							NULL,
							&prealloc_wal_file_num,
							2,
							1, INT_MAX / 1000,
							PGC_SIGHUP,
							0,
							NULL,
							NULL,
							NULL);

	/* can't define PGC_POSTMASTER variable after startup */
	DefineCustomBoolVariable("polar_worker.enable_polar_worker",
							 "Starts worker or not.",
							 NULL,
							 &enable_polar_worker,
							 true,
							 PGC_POSTMASTER,
							 0,
							 NULL,
							 NULL,
							 NULL);

  DefineCustomStringVariable(
							"polar_worker.core_file_path",
							"path of core file",
							"path of core file",
							&polar_core_file_path,
							".",
							PGC_SIGHUP,
							0,
							NULL,
							NULL,
							NULL
							);

	DefineCustomStringVariable(
							"polar_worker.core_name_suffix",
							"To assign corefile name.",
							"To assign corefile name.",
							&core_name_suffix,
							"core",
							PGC_SIGHUP,
							0,
							NULL,
							NULL,
							NULL
							);
	DefineCustomIntVariable(
							"polar_worker.core_file_outdate_time",
							"outdate time of core file.",
							NULL,
							&core_file_outdate_time,
							-1 ,
							-1, INT_MAX / 1000,
							PGC_SIGHUP,
							0,
							NULL,
							NULL,
							NULL
							);
	DefineCustomIntVariable(
							"polar_worker.num_corefile_reserved_old",
							"num of oldest reserverd corefile.",
							NULL,
							&num_corefile_reserved_old,
							32,
							0, INT_MAX / 1000,
							PGC_SIGHUP,
							0,
							NULL,
							NULL,
							NULL
							);
	DefineCustomIntVariable(
							"polar_worker.num_corefile_reserved_new",
							"num of newest reserverd corefile.",
							NULL,
							&num_corefile_reserved_new,
							32,
							0, INT_MAX / 1000,
							PGC_SIGHUP,
							0,
							NULL,
							NULL,
							NULL
							);
	DefineCustomIntVariable(
							"polar_worker.xlog_temp_outdate_time",
							"outdate time of xlog temp.",
							NULL,
							&xlog_temp_outdate_time,
							-1,
							-1, INT_MAX / 1000,
							PGC_SIGHUP,
							0,
							NULL,
							NULL,
							NULL
							);
	EmitWarningsOnPlaceholders("polar_worker");

	/* Register polar worker. */
	start_polar_worker();
}

/*
 * Start autoprewarm master worker process.
 */
static void
start_polar_worker(void)
{
	BackgroundWorker worker;
	BackgroundWorkerHandle *handle;
	BgwHandleStatus status;
	pid_t		pid;

	MemSet(&worker, 0, sizeof(BackgroundWorker));
	worker.bgw_flags = BGWORKER_SHMEM_ACCESS;
	worker.bgw_start_time = BgWorkerStart_PostmasterStart;
	worker.bgw_restart_time = 30;
	strcpy(worker.bgw_library_name, "polar_worker");
	strcpy(worker.bgw_function_name, "polar_worker_handler_main");
	strcpy(worker.bgw_name, "polar worker process");
	strcpy(worker.bgw_type, "polar worker process");

	if (process_shared_preload_libraries_in_progress)
	{
		RegisterBackgroundWorker(&worker);
		return;
	}

	/* must set notify PID to wait for startup */
	worker.bgw_notify_pid = MyProcPid;

	if (!RegisterDynamicBackgroundWorker(&worker, &handle))
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_RESOURCES),
				 errmsg("could not register background process"),
				 errhint("You may need to increase max_worker_processes.")));

	status = WaitForBackgroundWorkerStartup(handle, &pid);
	if (status != BGWH_STARTED)
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_RESOURCES),
				 errmsg("could not start background process"),
				 errhint("More details may be available in the server log.")));
}

/*
 * Preallocate wal files beyond last redolog endpoint.
 */
static void
prealloc_wal_files(void)
{
	XLogRecPtr	insert_ptr = GetXLogInsertRecPtr();
	XLogSegNo	_log_seg_no;
	int			lf;
	bool		use_existent;
	int		count = 0;

	XLByteToPrevSeg(insert_ptr, _log_seg_no, wal_segment_size);
	while (count < prealloc_wal_file_num)
	{
		_log_seg_no++;
		count++;
		use_existent = true;
		lf = XLogFileInit(_log_seg_no, &use_existent, true);
		polar_close(lf);
		if (!use_existent)
			CheckpointStats.ckpt_segs_added++;
	}
}

/*
 * Main entry point for polar worker process.
 */
void
polar_worker_handler_main(Datum main_arg)
{
	TimestampTz last_time = GetCurrentTimestamp();

	/* Establish signal handlers; once that's done, unblock signals. */
	pqsignal(SIGTERM, polar_worker_sigterm_handler);
	pqsignal(SIGHUP, polar_worker_sighup_handler);
	pqsignal(SIGUSR1, procsignal_sigusr1_handler);
	BackgroundWorkerUnblockSignals();

	/* Periodically prealloc wal file until terminated. */
	while (!got_sigterm)
	{
		int		rc = 0;

		/* In case of a SIGHUP, just reload the configuration. */
		if (got_sighup)
		{
			got_sighup = false;
			ProcessConfigFile(PGC_SIGHUP);
		}

		if (!enable_polar_worker)
		{
			/* We don't want to prealloc wal file now, so just wait forever. */
			rc = WaitLatch(&MyProc->procLatch,
						   WL_LATCH_SET | WL_POSTMASTER_DEATH,
						   -1L,
						   PG_WAIT_EXTENSION);
		}
		/* POLAR: in replica mode, polar worker only do fullpage snapshot work */
		else if (POLAR_ENABLE_FULLPAGE_SNAPSHOT() && polar_in_replica_mode())
		{
			polar_bgworker_fullpage_snapshot_replay_main();
		}
		/*POLAR: coredump/wal prefetch only in master/standby take affect */
		else
		{
			long		delay_in_ms = 0;
			TimestampTz next_time = 0;
			long		secs = 0;
			int			usecs = 0;

			/* Compute the next prealloc wal file time. */
			next_time =
				TimestampTzPlusMilliseconds(last_time,
											polar_worker_check_interval * 1000);
			TimestampDifference(GetCurrentTimestamp(), next_time,
								&secs, &usecs);
			delay_in_ms = secs + (usecs / 1000);

			/* If we are in Recovery mode, sleep util consistant */
			if (RecoveryInProgress())
				delay_in_ms = polar_worker_check_interval;
			else if (delay_in_ms <= 0)
			{
				last_time = GetCurrentTimestamp();
				/* Perform a prealloc wal and fullpage file operation if it's time. */
				if (!RecoveryInProgress())
				{
					prealloc_wal_files();
					polar_prealloc_fullpage_files();
				}

				/* Perform xlog temp file clean */
				if (xlog_temp_outdate_time >= 0)
					polar_xlog_temp_file_clean();

				continue;
			}

			/* Sleep until the next prealloc wal file time. */
			rc = WaitLatch(&MyProc->procLatch,
						   WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
						   delay_in_ms,
						   PG_WAIT_EXTENSION);
		}

		/* Reset the latch, bail out if postmaster died, otherwise loop. */
		ResetLatch(&MyProc->procLatch);
		if (rc & WL_POSTMASTER_DEATH)
			proc_exit(1);
	}

	/*
	 * Don't allow to exit in signal handler,maybe process
	 * is IO pending now, file cannot close normally, so
	 * exit here for safety
	 */
	if (got_sigterm)
		proc_exit(1);
}

/*
 * Signal handler for SIGTERM
 */
static void
polar_worker_sigterm_handler(SIGNAL_ARGS)
{
	int			save_errno = errno;

	got_sigterm = true;

	if (MyProc)
		SetLatch(&MyProc->procLatch);

	polar_set_shutdown_requested_flag();

	errno = save_errno;
}

/*
 * Signal handler for SIGHUP
 */
static void
polar_worker_sighup_handler(SIGNAL_ARGS)
{
	int			save_errno = errno;

	got_sighup = true;
	ConfigReloadPending = true;

	if (MyProc)
		SetLatch(&MyProc->procLatch);

	errno = save_errno;
}

/*
 * POLAR: remove xlog temp file if it is outdate.
 */
static void
polar_xlog_temp_file_clean(void)
{
	DIR *waldir;
	struct dirent *walde;
	struct stat st;
	char current_xlog_temp_file_path[MAXPGPATH] = {0};
	char current_dir[MAXPGPATH] = {0};
	time_t timestamp;
	static time_t last_clean_timestamp;

	time(&timestamp);

	/* to scan disk periodically */
	if (last_clean_timestamp && timestamp - last_clean_timestamp < xlog_temp_outdate_time)
		return ;
	last_clean_timestamp = timestamp;

	/* search xlog info in the PG WAL DIR */
	POLAR_FILE_PATH(current_dir, "pg_wal");
	waldir = polar_allocate_dir(current_dir);
	while ((walde = polar_readdir(waldir)) != NULL)
	{
		snprintf(current_xlog_temp_file_path, MAXPGPATH, "%s/%s", current_dir, walde->d_name);

		/* Skip special stuff */
		if (strcmp(walde->d_name, ".") == 0 || strcmp(walde->d_name, "..") == 0)
			continue;

		/* skip file could not open valid */
		if (stat(current_xlog_temp_file_path, &st))
			continue;

		/* skip file not belong to itself */
		if (st.st_uid != geteuid())
			continue;

		/* skip dir */
		if (S_ISDIR(st.st_mode))
			continue;

		/* skip file name invalid */
		if (strncmp(walde->d_name, POLAR_XLOG_TEMP_FILE_SUFFIX, strlen(POLAR_XLOG_TEMP_FILE_SUFFIX)))
			continue;

		/* delete file if it is outdate */
		if (timestamp - st.st_mtime >= xlog_temp_outdate_time)
		{
			polar_unlink(current_xlog_temp_file_path);
		}
	}
	FreeDir(waldir);
}

/*
 * POLAR: Get the core-file path from the system core-pattern file
 */
bool
polar_read_core_pattern(const char *core_pattern_path, char *core_file_path)
{
	int fd = 0;
	char* ret = NULL;
	struct stat st;
	char cwd[MAXPGPATH] = {0};
	char buf[MAXPGPATH] = {0};

	/* open system file and read core pattern path */
	fd = open(core_pattern_path, O_RDONLY | PG_BINARY);
	if (fd < 0)
	{
		ereport(LOG, (errcode_for_file_access(), 
			errmsg("could not open file \"%s\".", core_pattern_path)));
		return false;
	}
	if (read(fd, (char *) buf, sizeof(buf)) < 0)
	{
		ereport(LOG, (errcode_for_file_access(), 
			errmsg("could not read file \"%s\".", core_pattern_path)));
		return false;
	}
	close(fd);

	/* get the current working path */
	if (!getcwd(cwd, MAXPGPATH))
	{
		elog(LOG, "could not determine current directory");
		return false;
	}

	/* get the rightmost / if possible */
	ret = strrchr(buf, '/');

	/* first case: / did not exsit in path (e.g. core) the practical path is the current working path */
	if (ret == NULL)
	{
		snprintf(core_file_path, MAXPGPATH, "%s", cwd);
		return true;
	}

	/* get the core directory from the core pattern */ 
	if (ret - buf + 1 > MAXPGPATH)
	{
		elog(LOG, "core pattern length exceeds MAXPGPATH");
		return false;
	}
	buf[ret - buf + 1] = '\0';

	/* second case: absolute path (e.g. /tmp/corefile/core) */
	if (buf[0] == '/')
	{
		snprintf(core_file_path, MAXPGPATH, "%s", buf);
	}
	else
	{
		/* third case: relative path (e.g. corefile/core) */
		snprintf(core_file_path, MAXPGPATH, "%s/%s", cwd, buf);
	}

	/* judge whether core_file_path could be opened valid */
	if (stat(core_file_path, &st))
		return false;

	return true;
}
