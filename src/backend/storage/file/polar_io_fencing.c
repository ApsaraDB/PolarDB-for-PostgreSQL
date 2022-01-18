/*-------------------------------------------------------------------------
 *
 * polar_io_fencing.c
 *	  Polardb shared storage I/O fencing code.
 *
 * Copyright (c) 2020, Alibaba Group Holding Limited
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
 *	  src/backend/storage/file/polar_io_fencing.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <sys/file.h>
#include <sys/param.h>
#include <sys/stat.h>
#ifndef WIN32
#include <sys/mman.h>
#endif
#include <limits.h>
#include <unistd.h>
#include <fcntl.h>
#ifdef HAVE_SYS_RESOURCE_H
#include <sys/resource.h>		/* for getrlimit */
#endif

#include "miscadmin.h"
#include "access/xact.h"
#include "access/xlog_internal.h"
#include "access/xlog.h"
#include "catalog/pg_tablespace.h"
#include "common/file_perm.h"
#include "pgstat.h"
#include "portability/mem.h"
#include "storage/fd.h"
#include "storage/ipc.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/resowner_private.h"
#include "storage/polar_fd.h"
#include "storage/polar_io_fencing.h"

struct RWID polar_rwid;

/*
 * POLAR: check whether the shared storage is availabled or not
 * via checking DEATH flag.
 */
bool
polar_shared_storage_is_available()
{
	char death_path[MAXPGPATH];
	struct stat fst;
	bool available = true;

	polar_make_file_path_level2(death_path, POLAR_SHARED_STORAGE_UNAVAILABLE);
	if (polar_stat(death_path, &fst) < 0)
	{
		if (ENOENT == errno)
			available = true;
		else
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not stat file \"%s\": %m", death_path)));
	}
	else if (S_ISDIR(fst.st_mode))
		available = true;
	else
		available = false;

	return available;
}

/*
 * POLAR: generate and write one unique RWID into shared storage.
 * There are two conditions:
 * (1) after RW mount.
 * (2) fater RO remount while force_hold is true.
 * force_hold is available only in RO node.
 */
void
polar_hold_shared_storage(bool force_hold)
{
	int fd;
	uint32_t seed;
	char rwid_path[MAXPGPATH];
	int i;
	static bool first_build = true;

	/* step 1: generate a random RWID whose header is polar_hostid only once. */
	polar_rwid.hostid = polar_hostid;
	for (i = 0; first_build && i < POLAR_RWID_LENGTH; i += sizeof(uint32_t))
	{
		seed = random();
		memcpy(polar_rwid.random_id + i, &seed, sizeof(uint32_t));
	}
	first_build = false;

	/* check: RO should not hold shared storage unless it is promoting. */
	if (polar_in_replica_mode())
	{
		if (force_hold)
			elog(LOG, "RO can hold the shared storage during online promote.");
		else
		{
			elog(LOG, "RO can not hold the shared storage.");
			return;
		}
	}

	/* step 2: write RWID into file on shared storage */
	polar_make_file_path_level2(rwid_path, POLAR_RWID);
	fd = polar_open_transient_file(rwid_path, O_RDWR | O_CREAT | PG_BINARY);
	if (fd < 0)
	{
		if (EEXIST == errno)
			fd = polar_open_transient_file(rwid_path, O_RDWR | PG_BINARY);

		if (fd < 0)
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not open file \"%s\": %m", rwid_path)));
	}
	
	if ((int) polar_write(fd, &polar_rwid, sizeof(polar_rwid)) != sizeof(polar_rwid))
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not write to file \"%s\": %m", rwid_path)));
	if (polar_fsync(fd) != 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not fsync file \"%s\": %m", rwid_path)));
	CloseTransientFile(fd);
	elog(LOG, "polardb hold the shared storage %s.", polar_datadir);
}

/*
 * POLAR: create file DEATH to mark shared storage unavailable.
 */
void
polar_mark_shared_storage_unavailable()
{
	int fd;
	char death_path[MAXPGPATH];

	polar_make_file_path_level2(death_path, POLAR_SHARED_STORAGE_UNAVAILABLE);
	fd = polar_creat(death_path, pg_file_create_mode);
	if (fd < 0)
		ereport(FATAL,
				(errcode_for_file_access(),
				 errmsg("could not create file \"%s\" to make shared storage unavailable: %m",
				 		death_path)));
	if (polar_fsync(fd) != 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not fsync file \"%s\": %m", death_path)));
	if (polar_close(fd))
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not close file \"%s\": %m", death_path)));
}

/*
 * POLAR:
 * (1) report PANIC after shared storage is unavailable.
 * (2) mark shared storage unavailable and report PANIC after
 * RWID on shared storage is different with RWID in memory.
 */
void
polar_check_double_write()
{
	int fd;
	char rwid_path[MAXPGPATH];
	struct RWID cur_rwid;

	/* Only for shared storage mode. */
	if (!polar_enable_io_fencing || !POLAR_FILE_IN_SHARED_STORAGE())
		return;

	if (!polar_shared_storage_is_available())
		ereport(PANIC,
				(errcode_for_file_access(),
				 errmsg("polardb shared storage is unavailable.")));

	/* RO shouldn't do double write check with RWID because it can't write RWID into shared storage. */
	if (polar_in_replica_mode())
		return;
	
	polar_make_file_path_level2(rwid_path, POLAR_RWID);
	fd = polar_open_transient_file(rwid_path, O_RDONLY);
	if (fd < 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not open file \"%s\": %m", rwid_path)));
	
	MemSet(&cur_rwid, 0x0, sizeof(cur_rwid));
	/* It is normal to read less than sizeof(struct RWID) bytes from RWID file. */
	if ((int) polar_read(fd, &cur_rwid, sizeof(cur_rwid)) < 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not read from file \"%s\": %m", rwid_path)));
	CloseTransientFile(fd);
	if (memcmp(&polar_rwid, &cur_rwid, sizeof(polar_rwid)) != 0)
	{
		polar_mark_shared_storage_unavailable();
		ereport(PANIC,
				(errcode_for_file_access(),
				 errmsg("polardb shared storage is unavailable, shared storage's owner is %d, current hostid is %d.",
					 	cur_rwid.hostid, polar_rwid.hostid)));
	}
}

/*
 * POLAR: It is the enhanced version of pg_usleep which would not
 * be affected by signal.
 */
void
polar_usleep(TimestampTz start, TimestampTz microsec)
{
	struct timeval delay;
	while (microsec > 0)
	{
		int ret;
		delay.tv_sec = microsec / 1000000L;
		delay.tv_usec = microsec % 1000000L;
		ret = select(0, NULL, NULL, NULL, &delay);
		if (ret == 0)
		{
			/* POLAR: timeout */
			break;
		}
		else if (errno == EINTR)
		{
			/* POLAR: signal comes */
			CHECK_FOR_INTERRUPTS();
			microsec -= GetCurrentTimestamp() - start;
		}
		else
		{
			/* POLAR: anything else is an error. */
			ereport(FATAL, (errmsg("select() failed while waiting for target: %m")));
		}
	}
}
