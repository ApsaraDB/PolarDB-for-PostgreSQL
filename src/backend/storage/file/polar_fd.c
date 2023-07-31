/*-------------------------------------------------------------------------
 *
 * polar_fd.c
 *	  Polardb Virtual file descriptor code.
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
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/resowner_private.h"
#include "storage/polar_fd.h"

/* POLAR */
#include "polar_datamax/polar_datamax.h"
#include "storage/polar_io_fencing.h"
/* POLAR end */

/* set mode by parsing recovery.conf */
#define		POLAR_REPLICA_MODE 	0x01
#define		POLAR_STANDBY_MODE	0x02
#define		POLAR_DATAMAX_MODE	0x04

bool	polar_mount_pfs_readonly_mode = true;
bool	polar_openfile_with_readonly_in_replica = false;
int		polar_vfs_switch = POLAR_VFS_SWITCH_LOCAL;

/*
 * Record the initial node type, it is initialized by postmaster, inherited by
 * other backends. We record this value in shared memory later.
 */
PolarNodeType	polar_local_node_type = POLAR_UNKNOWN;
static bool polar_tls_callback_registered = false;

static void polar_vfs_exit_cleanup(int code, Datum arg);

/* Polar Conseusus */
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
		.vfs_env_init = NULL,
		.vfs_env_destroy = NULL,
		.vfs_mount = NULL,
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
		.vfs_lseek_cache = lseek,
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
		.vfs_env_init = NULL,
		.vfs_env_destroy = NULL,
		.vfs_mount = NULL,
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
		.vfs_lseek_cache = NULL,
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
polar_env_init(void)
{
	if (polar_vfs[polar_vfs_switch].vfs_env_init)	
		return polar_vfs[polar_vfs_switch].vfs_env_init();

	return 0;
}

inline int
polar_env_destroy(void)
{
	if (polar_vfs[polar_vfs_switch].vfs_env_destroy)
		return polar_vfs[polar_vfs_switch].vfs_env_destroy();

	return 0;
}

inline int
polar_mount(void)
{
	int ret = 0;
	if (polar_vfs[polar_vfs_switch].vfs_mount)
		ret = polar_vfs[polar_vfs_switch].vfs_mount();
	if (polar_enable_io_fencing && ret == 0)
	{
		/* POLAR: FATAL when shared storage is unavailable, or force to write RWID. */
		if (polar_shared_storage_is_available())
		{
			polar_hold_shared_storage(false);
			POLAR_IO_FENCING_SET_STATE(polar_io_fencing_get_instance(), POLAR_IO_FENCING_WAIT);
		}
		else
			elog(FATAL, "polardb shared storage %s is unavailable.", polar_datadir);
	}
	return ret;
}

inline int
polar_remount(void)
{
	int ret = 0;
	if (polar_vfs[polar_vfs_switch].vfs_remount)
		ret = polar_vfs[polar_vfs_switch].vfs_remount();
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

inline off_t
polar_lseek_cache(int fd, off_t offset, int whence)
{
	return polar_vfs[polar_vfs_switch].vfs_lseek_cache(fd, offset, whence);
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
polar_copydir(char *fromdir, char *todir, bool recurse, bool clean, bool skip_file_err, bool skip_open_dir_err)
{
	DIR		   *xldir;
	struct dirent *xlde;
	char		fromfile[MAXPGPATH * 2];
	char		tofile[MAXPGPATH * 2];
	int			read_dir_err;

	if (polar_mkdir(todir, S_IRWXU) != 0)
	{
		if (EEXIST == errno && clean)
			polar_remove_file_in_dir(todir);
		else
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not create directory \"%s\": %m", todir)));
	}

	/*
	 * For polar store, the readdir will cache a dir entry, if the dir is deleted
	 * when readdir, it will fail. So we should retry.
	 *
	 * For create/drop database, we will have a lock during the copying time,
	 * no directories will be deleted, so do not care this failure.
	 */
read_dir_failed:

	read_dir_err = 0;
	xldir = polar_allocate_dir(fromdir);
	if (xldir == NULL)
	{
		if (ENOENT == errno && skip_open_dir_err)
		{
			ereport(LOG,
					(errcode_for_file_access(),
					errmsg("could not open directory \"%s\": %m", fromdir)));

			return;
		}
		else
			ereport(ERROR,
					(errcode_for_file_access(),
					errmsg("could not open directory \"%s\": %m", fromdir)));
	}

	while ((xlde = polar_read_dir_ext(xldir, fromdir, WARNING, &read_dir_err)) != NULL)
	{
		struct stat fst;

		CHECK_FOR_INTERRUPTS();

		if (strcmp(xlde->d_name, ".") == 0 ||
			strcmp(xlde->d_name, "..") == 0)
			continue;

		snprintf(fromfile, sizeof(fromfile), "%s/%s", fromdir, xlde->d_name);
		snprintf(tofile, sizeof(tofile), "%s/%s", todir, xlde->d_name);

		if (polar_stat(fromfile, &fst) < 0)
		{
			/* File may be deleted after ReadDir */
			if (ENOENT == errno && skip_file_err)
			{
				ereport(LOG,
						(errcode_for_file_access(),
						 errmsg("file \"%s\" not exist: %m", fromfile)));

				/* Skip it, continue to deal with other directories */
				continue;
			}
			else
			{
				ereport(ERROR,
						(errcode_for_file_access(),
						 errmsg("could not stat file \"%s\": %m", fromfile)));
			}
		}

		if (S_ISDIR(fst.st_mode))
		{
			if (recurse)
				polar_copydir(fromfile, tofile, true, clean, skip_file_err, false);
		}
		else if (S_ISREG(fst.st_mode))
			polar_copy_file(fromfile, tofile, skip_file_err);
	}
	FreeDir(xldir);

	if (read_dir_err == ENOENT)
	{
		ereport(LOG,
				(errcode_for_file_access(),
					errmsg("When readdir, some entries were deleted, retry.")));
		goto read_dir_failed;
	}

	return;
}

void
polar_copy_file(char *fromfile, char *tofile, bool skiperr)
{
	char  *buffer;
	int   srcfd;
	int   dstfd;
	int   nbytes;
	off_t offset;

#define COPY_BUF_SIZE (8 * BLCKSZ)

	buffer = palloc(COPY_BUF_SIZE);

	srcfd = polar_open_transient_file(fromfile, O_RDONLY | PG_BINARY);
	if (srcfd < 0)
	{
		/* File may be deleted, skip it and free buffer */
		if (ENOENT == errno && skiperr)
		{
			ereport(LOG,
					(errcode_for_file_access(),
					 errmsg("file \"%s\" not exist: %m", fromfile)));
			pfree(buffer);
			return;
		}
		else
		{
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not open file \"%s\": %m", fromfile)));
		}
	}

	dstfd = polar_open_transient_file(tofile, O_RDWR | O_CREAT | O_EXCL | PG_BINARY);
	if (dstfd < 0)
	{
		if (EEXIST == errno)
			dstfd = polar_open_transient_file(tofile, O_RDWR | O_EXCL | PG_BINARY);

		if (dstfd < 0)
			ereport(ERROR,
					(errcode_for_file_access(),
					errmsg("could not create file \"%s\": %m", tofile)));
	}

	for (offset = 0;; offset += nbytes)
	{
		CHECK_FOR_INTERRUPTS();

		pgstat_report_wait_start(WAIT_EVENT_COPY_FILE_READ);
		if ((nbytes = polar_read(srcfd, buffer, COPY_BUF_SIZE)) < 0)
		{
			/* File was deleted during we read it. */
			if (errno == ENOENT)
				break;
			else
				ereport(ERROR,
						(errcode_for_file_access(),
							errmsg("could not read file \"%s\": %m", fromfile)));
		}
		pgstat_report_wait_end();
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
polar_make_file_path_level3(char *path, const char *base, const char *file_path)
{
	if (POLAR_FILE_IN_SHARED_STORAGE())
		snprintf(path, MAXPGPATH, "%s/%s/%s", polar_datadir, base, file_path);
	else
		snprintf(path, MAXPGPATH, "%s/%s", base, file_path);

	return;
}

void
polar_make_file_path_level2(char *path, const char *file_path)
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
	/* POLAR: handle datamax branch */
	if (polar_is_datamax_mode)
	{
		polar_datamax_wal_file_path(path, tli, logSegNo, wal_segsz_bytes);
		return;
	}
	/* POLAR end */

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
	/* POLAR: handle datamax branch */
	if (polar_is_datamax_mode)
	{
		polar_datamax_tl_history_file_path(path, tli);
		return;
	}
	/* POLAR end */

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
	/* POLAR: handle datamax branch */
	if (polar_is_datamax_mode)
	{
		polar_datamax_status_file_path(path, xlog, suffix);
		return;
	}
	/* POLAR end */

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

void
polar_set_vfs_function_ready(void)
{
	polar_vfs_switch = POLAR_VFS_SWITCH_PLUGIN;
}

struct dirent *
polar_read_dir_ext(DIR *dir, const char *dirname, int elevel, int *err)
{
	struct dirent *res;

	res = ReadDirExtended(dir, dirname, elevel);
	*err = errno;
	return res;
}

/* POLAR: register tls cleanup function */
void
polar_register_tls_cleanup(void)
{
	if (POLAR_FILE_IN_SHARED_STORAGE() == false)
		return;

	if (polar_vfs_switch == POLAR_VFS_SWITCH_LOCAL)
		return;

	if (polar_tls_callback_registered == false)
	{
		on_proc_exit(polar_vfs_exit_cleanup, 0);
		polar_tls_callback_registered = true;
	}
}

static void
polar_vfs_exit_cleanup(int code, Datum arg)
{
	polar_env_destroy();
	return;
}

void
polar_validate_dir(char *path)
{
	struct stat st;

	if (polar_stat(path, &st) == 0)
	{
		
		if (!S_ISDIR(st.st_mode))
			ereport(FATAL,
					(errmsg("required directory \"%s\" exists, but is not dir",
							path)));
	}
	else
	{
		ereport(LOG,
			(errmsg("creating missing directory \"%s\"",
					path)));

		if (polar_make_pg_directory(path) < 0)
			ereport(FATAL,
					(errmsg("could not create missing directory \"%s\": %m",
							path)));
	}
}

ssize_t
polar_read_line(int fd, void *buffer, size_t n)
{
	ssize_t num_read;                    /* # of bytes fetched by last read() */
	size_t tot_read;                     /* Total bytes read so far */
	char *buf;
	char ch;
	if (n <= 0 || buffer == NULL) {
		errno = EINVAL;
		return -1;
	}

	buf = buffer;                       /* No pointer arithmetic on "void *" */

	tot_read = 0;
	for (;;) {
		num_read = polar_read(fd, &ch, 1);

		if (num_read == -1) {
			if (errno == EINTR)         /* Interrupted --> restart read() */
				continue;
			else
				return -1;              /* Some other error */

		} else if (num_read == 0) {      /* EOF */
			if (tot_read == 0)           /* No bytes read; return 0 */
				return 0;
			else                        /* Some bytes read; add '\0' */
				break;

		} else {                        /* 'numRead' must be 1 if we get here */
			if (tot_read < n - 1) {      /* Discard > (n - 1) bytes */
				tot_read++;
				*buf++ = ch;
			}

			if (ch == '\n')
				break;
		}
	}

	*buf = '\0';
	return tot_read;
}

void
polar_init_node_type(void)
{
	polar_local_node_type = polar_node_type_by_file();
}

PolarNodeType
polar_node_type_by_file(void)
{
	FILE	   *fd;
	ConfigVariable *item,
	               *head = NULL,
	               *tail = NULL;
	/* use flag to show whether replica/standby/datamax is requested */
	uint8_t 	flag = 0;
	PolarNodeType polar_node_type = POLAR_UNKNOWN;

#define RECOVERY_COMMAND_FILE	"recovery.conf"

	fd = AllocateFile(RECOVERY_COMMAND_FILE, "r");
	if (fd == NULL)
	{
		if (errno == ENOENT)
		{
			polar_node_type = POLAR_MASTER;
			elog(LOG, "recovery.conf not exist, polardb in readwrite mode");
			return polar_node_type;
		}
		ereport(FATAL,
		        (errcode_for_file_access(),
			        errmsg("could not open recovery command file \"%s\": %m",
			               RECOVERY_COMMAND_FILE)));
	}

	/*
	 * Since we're asking ParseConfigFp() to report errors as FATAL, there's
	 * no need to check the return value.
	 */
	(void) ParseConfigFp(fd, RECOVERY_COMMAND_FILE, 0, FATAL, &head, &tail);

	FreeFile(fd);

	for (item = head; item; item = item->next)
	{
		/* set true when replica or standby mode is set */
		bool polar_is_set = false;
		if (!POLAR_ENABLE_DMA() && strcmp(item->name, "polar_replica") == 0)
		{
			if (!parse_bool(item->value, &polar_is_set))
				ereport(ERROR,
				        (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					        errmsg("parameter \"%s\" requires a Boolean value",
					               "polar_replica")));
			ereport(LOG,
			        (errmsg_internal("polar_replica = '%s'", item->value)));
			if (polar_is_set)
				flag |= POLAR_REPLICA_MODE;
		}
		else if (!POLAR_ENABLE_DMA() && strcmp(item->name, "standby_mode") == 0)
		{
			if (!parse_bool(item->value, &polar_is_set))
				ereport(ERROR,
				        (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					        errmsg("parameter \"%s\" requires a Boolean value",
					               "standby_mode")));
			ereport(LOG,
			        (errmsg_internal("standby_mode = '%s'", item->value)));
			if (polar_is_set)
				flag |= POLAR_STANDBY_MODE;
		}
		else if (strcmp(item->name, "polar_datamax_mode") == 0)
		{
			if (strcmp(item->value, "standalone") == 0)
			{
				flag |= POLAR_DATAMAX_MODE;
			}
			else
				ereport(FATAL, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					errmsg("parameter \"%s\" has unknown value: %s, set polar_datamax_mode = standalone",
					       "polar_datamax_mode", item->value)));
		}
	}

	if (flag & POLAR_REPLICA_MODE)
	{
		polar_node_type = POLAR_REPLICA;
		elog(LOG,
		     "read polar_replica = on, polardb in replica mode, use ro mode mount pfs");
	}

	if (flag & POLAR_STANDBY_MODE)
	{
		polar_node_type = POLAR_STANDBY;
		elog(LOG,
		     "read standby_mode = on, polardb in standby mode, use readwrite mode mount pfs");
	}

	if (flag & POLAR_DATAMAX_MODE)
	{
		polar_node_type = POLAR_STANDALONE_DATAMAX;
		elog(LOG,
			 "read polar_datamax_mode=standalone config, polardb in datamax mode with independent storage.");
	}

	if ((flag & (flag - 1)) != 0)
	{
		ereport(FATAL, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
			errmsg("replica, standby and datamax mode is mutually exclusive.")));
	}

	/*
	 * Exists one recovery.conf, but it is not replica or standby, it may be
	 * a PITR recovery.conf.
	 */
	if (!flag)
	{
		elog(LOG, "Exists recovery.conf, but it's not replica or standby, may be in archive recovery(PITR).");
		polar_node_type = POLAR_MASTER;
	}

	FreeConfigVariables(head);

	return polar_node_type;
}

/*
 * POLAR: We provide this macro in order to remove protocol
 * from polar_datadir. The polar_datadir must conform to the format:
 * [protocol]://[path]
 *
 * Notice: The polar_datadir's protocol must be same as the polar_vfs_klind
 * inside polar_vfs.c. This macro should ONLY be used when you can't use polar_vfs
 * interface.
 */
const char *
polar_path_remove_protocol(const char *path)
{
	const char *vfs_path = strstr(path, POLAR_VFS_PROTOCOL_TAG);
	if (vfs_path)
		return vfs_path + strlen(POLAR_VFS_PROTOCOL_TAG);
	else
		return path;
}

/*
 * POLAR: DirectIO optimization will be turned on when
 * polar_datadir use local DirectIO protocol.
 */
void
assign_polar_datadir(const char *newval, void *extra)
{
	polar_enable_buffer_alignment = false;
	if (strncmp(POLAR_VFS_PROTOCAL_LOCAL_DIO, newval, strlen(POLAR_VFS_PROTOCAL_LOCAL_DIO)) == 0)
		polar_enable_buffer_alignment = true;
}

inline bool
polar_file_exists(const char *path)
{
	struct stat st;

	return (polar_stat(path, &st) == 0) && S_ISREG(st.st_mode);
}
