/*-------------------------------------------------------------------------
 *
 * polar_fd.c
 *
 *	  PolarDB Virtual file descriptor code.
 *
 *	  Copyright (c) 2020, Alibaba Group Holding Limited
 *	  Licensed under the Apache License, Version 2.0 (the "License");
 *	  you may not use this file except in compliance with the License.
 *	  You may obtain a copy of the License at
 *	  http://www.apache.org/licenses/LICENSE-2.0
 *	  Unless required by applicable law or agreed to in writing, software
 *	  distributed under the License is distributed on an "AS IS" BASIS,
 *	  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *	  See the License for the specific language governing permissions and
 *	  limitations under the License.
 *
 * IDENTIFICATION
 *	  src/backend/storage/file/polar_fd.c
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
#include "utils/guc.h"
#include "utils/resowner_private.h"
#include "storage/polar_fd.h"

bool	polar_mount_pfs_readonly_mode = true;
bool	polar_openfile_with_readonly_in_replica = false;
int		polar_vfs_switch = POLAR_VFS_SWITCH_LOCAL;

static inline const vfs_mgr* polar_get_local_vfs_mgr(const char *path);

/*
 * POLAR: The virtual file interface
 * 1 The standard file interface function is used by default.
 * polar_vfs_switch = POLAR_VFS_SWITCH_LOCAL
 * 2 When a storage plugin is accessed, all io call implemented in the plugin.
 * polar_vfs_switch = POLAR_VFS_SWITCH_PLUGIN
 * 3 polar_vfs means use function in plugin to read write data which include local file or 
 * stared storage file.
 */
vfs_mgr polar_vfs[] =
{
	{
		.vfs_mount = NULL,
		.vfs_umount = NULL,
		.vfs_remount = NULL,
		.vfs_open = (vfs_open_type)open,
		.vfs_creat = creat,
		.vfs_close = close,
		.vfs_read = read,
		.vfs_write = write,
		.vfs_pread = pread,
		.vfs_pwrite = pwrite,
		.vfs_stat = stat,
		.vfs_fstat = fstat,
		.vfs_lstat = lstat,
		.vfs_lseek = lseek,
		.vfs_access = access,
		.vfs_fsync = pg_fsync,
		.vfs_unlink = unlink,
		.vfs_rename = rename,
		.vfs_fallocate = posix_fallocate,
		.vfs_ftruncate = ftruncate,
		.vfs_opendir = opendir,
		.vfs_readdir = readdir,
		.vfs_closedir = closedir,
		.vfs_mkdir = mkdir,
		.vfs_rmdir = rmdir,
		.vfs_mgr_func = polar_get_local_vfs_mgr
	},
	{
		.vfs_mount = NULL,
		.vfs_umount = NULL,
		.vfs_remount = NULL,
		.vfs_open = NULL,
		.vfs_creat = NULL,
		.vfs_close = NULL,
		.vfs_read = NULL,
		.vfs_write = NULL,
		.vfs_pread = NULL,
		.vfs_pwrite = NULL,
		.vfs_stat = NULL,
		.vfs_fstat = NULL,
		.vfs_lstat = NULL,
		.vfs_lseek = NULL,
		.vfs_access = NULL,
		.vfs_fsync = NULL,
		.vfs_unlink = NULL,
		.vfs_rename = NULL,
		.vfs_fallocate = NULL,
		.vfs_ftruncate = NULL,
		.vfs_opendir = NULL,
		.vfs_readdir = NULL,
		.vfs_closedir = NULL,
		.vfs_mkdir = NULL,
		.vfs_rmdir = NULL,
		.vfs_mgr_func = NULL
	}
};

inline int
polar_mount(void)
{
	if (polar_vfs[polar_vfs_switch].vfs_mount)
		return polar_vfs[polar_vfs_switch].vfs_mount();

	return 0;
}

inline void
polar_umount(void)
{
	if (polar_vfs[polar_vfs_switch].vfs_umount)
		return polar_vfs[polar_vfs_switch].vfs_umount();

	return;
}

inline int
polar_remount(void)
{
	int ret = 0;
	if (polar_vfs[polar_vfs_switch].vfs_remount)
		ret = polar_vfs[polar_vfs_switch].vfs_remount();
#if 0
	if (polar_enable_io_fencing && ret == 0)
	{
		/* POLAR: FATAL when shared storage is unavailable, or force to write RWID. */
		if (polar_shared_storage_is_available())
		{
			polar_hold_shared_storage(true);
			POLAR_IO_FENCING_SET_STATE(polar_io_fencing_get_instance(), POLAR_IO_FENCING_WAIT);
		}
		else
			elog(FATAL, "polardb shared storage %s is unavailable.", polar_datadir);
	}
#endif
	return ret;
}

inline int
polar_open(const char *path, int flags, mode_t mode)
{
	return polar_vfs[polar_vfs_switch].vfs_open(path, flags, mode);
}

inline int
polar_creat(const char *path, mode_t mode)
{
	return polar_vfs[polar_vfs_switch].vfs_creat(path, mode);
}

inline int
polar_close(int fd)
{
	return polar_vfs[polar_vfs_switch].vfs_close(fd);
}

inline ssize_t
polar_read(int fd, void *buf, size_t len)
{
	return polar_vfs[polar_vfs_switch].vfs_read(fd, buf, len);
}

inline ssize_t
polar_write(int fd, const void *buf, size_t len)
{
	return polar_vfs[polar_vfs_switch].vfs_write(fd, buf, len);
}

inline ssize_t
polar_pread(int fd, void *buf, size_t len, off_t offset)
{
	return polar_vfs[polar_vfs_switch].vfs_pread(fd, buf, len, offset);
}

inline ssize_t
polar_pwrite(int fd, const void *buf, size_t len, off_t offset)
{
	return polar_vfs[polar_vfs_switch].vfs_pwrite(fd, buf, len, offset);
}

/* POLAR: Replacing stat function, check all unlink when every merge code */
inline int
polar_stat(const char *path, struct stat *buf)
{
	return polar_vfs[polar_vfs_switch].vfs_stat(path, buf);
}

inline int
polar_fstat(int fd, struct stat *buf)
{
	return polar_vfs[polar_vfs_switch].vfs_fstat(fd, buf);
}

inline int
polar_lstat(const char *path, struct stat *buf)
{
	return polar_vfs[polar_vfs_switch].vfs_lstat(path, buf);
}

inline off_t
polar_lseek(int fd, off_t offset, int whence)
{
	return polar_vfs[polar_vfs_switch].vfs_lseek(fd, offset, whence);
}

inline int
polar_access(const char *path, int mode)
{
	return polar_vfs[polar_vfs_switch].vfs_access(path, mode);
}

inline int
polar_fsync(int fd)
{
	return polar_vfs[polar_vfs_switch].vfs_fsync(fd);
}

/* POLAR: Replacing unlink function, check all unlink when every merge code */
inline int
polar_unlink(const char *fname)
{
	return polar_vfs[polar_vfs_switch].vfs_unlink(fname);
}

inline int
polar_rename(const char *oldfile, const char *newfile)
{
	return polar_vfs[polar_vfs_switch].vfs_rename(oldfile, newfile);
}

inline int
polar_fallocate(int fd, off_t offset, off_t len)
{
	return polar_vfs[polar_vfs_switch].vfs_fallocate(fd, offset, len);
}

inline int
polar_ftruncate(int fd, off_t len)
{
	return polar_vfs[polar_vfs_switch].vfs_ftruncate(fd, len);
}

inline DIR	*
polar_opendir(const char *path)
{
	return polar_vfs[polar_vfs_switch].vfs_opendir(path);
}

inline struct dirent *
polar_readdir(DIR *dir)
{
	return polar_vfs[polar_vfs_switch].vfs_readdir(dir);
}

inline int
polar_closedir(DIR *dir)
{
	return polar_vfs[polar_vfs_switch].vfs_closedir(dir);
}

inline int
polar_mkdir(const char *path, mode_t mode)
{
	return polar_vfs[polar_vfs_switch].vfs_mkdir(path, mode);
}

/* POLAR: Replacing rmdir function, check all unlink when every merge code */
inline int
polar_rmdir(const char *path)
{
	return polar_vfs[polar_vfs_switch].vfs_rmdir(path);
}

static inline const vfs_mgr*
polar_get_local_vfs_mgr(const char *path)
{
	return &polar_vfs[POLAR_VFS_SWITCH_LOCAL];
}

const vfs_mgr*
polar_vfs_mgr(const char *path)
{
	if (polar_vfs[polar_vfs_switch].vfs_mgr_func)
		return (polar_vfs[polar_vfs_switch].vfs_mgr_func)(path);

	return NULL;
}

int
polar_make_pg_directory(const char *directoryName)
{
	return polar_mkdir(directoryName, pg_dir_create_mode);
}

void
polar_copydir(char *fromdir, char *todir, bool recurse)
{
	DIR		   *xldir;
	struct dirent *xlde;
	char		fromfile[MAXPGPATH * 2];
	char		tofile[MAXPGPATH * 2];

	if (polar_mkdir(todir, S_IRWXU) != 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not create directory \"%s\": %m", todir)));

	xldir = polar_allocate_dir(fromdir);
	if (xldir == NULL)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not open directory \"%s\": %m", fromdir)));

	while ((xlde = ReadDir(xldir, fromdir)) != NULL)
	{
		struct stat fst;

		CHECK_FOR_INTERRUPTS();

		if (strcmp(xlde->d_name, ".") == 0 ||
			strcmp(xlde->d_name, "..") == 0)
			continue;

		snprintf(fromfile, sizeof(fromfile), "%s/%s", fromdir, xlde->d_name);
		snprintf(tofile, sizeof(tofile), "%s/%s", todir, xlde->d_name);

		if (polar_stat(fromfile, &fst) < 0)
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not stat file \"%s\": %m", fromfile)));

		if (S_ISDIR(fst.st_mode))
		{
			if (recurse)
			{
				polar_copydir(fromfile, tofile, true);
			}
		}
		else if (S_ISREG(fst.st_mode))
			polar_copy_file(fromfile, tofile);
	}
	FreeDir(xldir);

	return;
}

void
polar_copy_file(char *fromfile, char *tofile)
{
	char	   *buffer;
	int			srcfd;
	int			dstfd;
	int			nbytes;
	off_t		offset;

#define COPY_BUF_SIZE (8 * BLCKSZ)

	buffer = palloc(COPY_BUF_SIZE);

	srcfd = polar_open_transient_file(fromfile, O_RDONLY | PG_BINARY);
	if (srcfd < 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not open file \"%s\": %m", fromfile)));

	dstfd = polar_open_transient_file(tofile, O_RDWR | O_CREAT | O_EXCL | PG_BINARY);
	if (dstfd < 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not create file \"%s\": %m", tofile)));

	for (offset = 0;; offset += nbytes)
	{
		CHECK_FOR_INTERRUPTS();

		pgstat_report_wait_start(WAIT_EVENT_COPY_FILE_READ);
		nbytes = polar_read(srcfd, buffer, COPY_BUF_SIZE);
		pgstat_report_wait_end();
		if (nbytes < 0)
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not read file \"%s\": %m", fromfile)));
		if (nbytes == 0)
			break;
		errno = 0;
		pgstat_report_wait_start(WAIT_EVENT_COPY_FILE_WRITE);
		if ((int) polar_write(dstfd, buffer, nbytes) != nbytes)
		{
			pgstat_report_wait_end();
			if (errno == 0)
				errno = ENOSPC;
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not write to file \"%s\": %m", tofile)));
		}
		pgstat_report_wait_end();

		polar_fsync(dstfd);
	}

	if (CloseTransientFile(dstfd))
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not close file \"%s\": %m", tofile)));

	CloseTransientFile(srcfd);

	pfree(buffer);
}

void
polar_make_file_path_level3(char *path, char *base, char *file_path)
{
	if (POLAR_FILE_IN_SHARED_STORAGE())
		snprintf(path, MAXPGPATH, "%s/%s/%s", polar_datadir, base, file_path);
	else
		snprintf(path, MAXPGPATH, "%s/%s", base, file_path);

	return;
}

void
polar_make_file_path_level2(char *path, char *file_path)
{
	if (POLAR_FILE_IN_SHARED_STORAGE())
		snprintf(path, MAXPGPATH, "%s/%s", polar_datadir, file_path);
	else
		snprintf(path, MAXPGPATH, "%s", file_path);

	return;
}

/*
 * POLAR: Extend from XLogFilePath
 */
void
polar_xLog_file_path(char *path, TimeLineID tli, XLogSegNo logSegNo, int wal_segsz_bytes)
{
	if (POLAR_FILE_IN_SHARED_STORAGE())
	{
		snprintf(path, MAXPGPATH, "%s/" XLOGDIR "/%08X%08X%08X", polar_datadir, tli,	
			 (uint32) ((logSegNo) / XLogSegmentsPerXLogId(wal_segsz_bytes)), 
			 (uint32) ((logSegNo) % XLogSegmentsPerXLogId(wal_segsz_bytes)));
	}
	else
	{
		snprintf(path, MAXPGPATH, XLOGDIR "/%08X%08X%08X", tli,	
			 (uint32) ((logSegNo) / XLogSegmentsPerXLogId(wal_segsz_bytes)), 
			 (uint32) ((logSegNo) % XLogSegmentsPerXLogId(wal_segsz_bytes)));
	}

	return;
}

/*
 * POLAR: Extend from TLHistoryFilePath
 */
void
polar_tl_history_file_path(char *path, TimeLineID tli)
{
	if (POLAR_FILE_IN_SHARED_STORAGE())
		snprintf(path, MAXPGPATH, "%s/" XLOGDIR "/%08X.history", polar_datadir, tli);
	else
		snprintf(path, MAXPGPATH, XLOGDIR "/%08X.history", tli);

	return;
}

/*
 * POLAR: StatusFilePath
 */
void
polar_status_file_path(char *path, const char *xlog, char *suffix)
{
	if (POLAR_FILE_IN_SHARED_STORAGE())
		snprintf(path, MAXPGPATH, "%s/" XLOGDIR "/archive_status/%s%s", polar_datadir, xlog, suffix);
	else
		snprintf(path, MAXPGPATH, XLOGDIR "/archive_status/%s%s", xlog, suffix);

	return;
}

/*
 * POLAR: Extend from BackupHistoryFilePath
 */
void
polar_backup_history_file_path(char *path, TimeLineID tli, XLogSegNo logSegNo, XLogRecPtr startpoint, int wal_segsz_bytes)
{
	if (POLAR_FILE_IN_SHARED_STORAGE())
	{
		snprintf(path, MAXPGPATH, "%s/" XLOGDIR "/%08X%08X%08X.%08X.backup", polar_datadir, tli,
			 (uint32) ((logSegNo) / XLogSegmentsPerXLogId(wal_segsz_bytes)),
			 (uint32) ((logSegNo) % XLogSegmentsPerXLogId(wal_segsz_bytes)),
			 (uint32) (XLogSegmentOffset((startpoint), wal_segsz_bytes)));
	}
	else
	{
		snprintf(path, MAXPGPATH, XLOGDIR "/%08X%08X%08X.%08X.backup", tli,
			 (uint32) ((logSegNo) / XLogSegmentsPerXLogId(wal_segsz_bytes)),
			 (uint32) ((logSegNo) % XLogSegmentsPerXLogId(wal_segsz_bytes)),
			 (uint32) (XLogSegmentOffset((startpoint), wal_segsz_bytes)));
	}

	return;
}

void
polar_reset_vfs_switch(void)
{
	polar_vfs_switch = POLAR_VFS_SWITCH_LOCAL;
}

