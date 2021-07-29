/*-------------------------------------------------------------------------
 *
 * polar_vfs.c
 *
 *	  PolarDB Virtual file system code.
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
 *	  external/polar_vfs/polar_vfs.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/htup_details.h"
#include "access/xact.h"
#include "access/xlog.h"
#include "catalog/pg_type.h"
#include "funcapi.h"
#include "libpq/pqsignal.h"
#include "miscadmin.h"
#include "storage/backendid.h"
#include "storage/ipc.h"
#include "storage/polar_fd.h"
#include "storage/shmem.h"
#include "storage/lwlock.h"
#include "tcop/utility.h"
#include "portability/instr_time.h"
#include "postmaster/postmaster.h"
#include "postmaster/bgworker.h"
#include "pgstat.h"
#include "port.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/timeout.h"
#include "utils/guc.h"

#include <unistd.h>
#include <sys/time.h>
#include <semaphore.h>
#include <pthread.h>
#include "pfsd_sdk.h"

PG_MODULE_MAGIC;

typedef enum polardb_node_state
{
	unknown = -1,
	readwrite = 0,
	readonly,
	standby
}polardb_node_state;

/* POLAR: Status at startup, determined from the configuration file recovery.conf */
polardb_node_state	polardb_start_state = unknown;

#define MIN_VFS_FD_DIR_SIZE		10

#define VFD_CLOSED (-1)

#define		VFS_UNKNOWN_FILE	-1
#define		VFS_LOCAL_FILE		0
#define		VFS_PFS_FILE		1

typedef struct vfs_vfd
{
	int				fd;
	int				kind;
	int				next_free;
	off_t			file_size;
	char			*file_name;
} vfs_vfd;

static vfs_vfd	*vfs_vfd_cache = NULL;
static size_t	size_vfd_cache = 0;
static int		num_open_file = 0;

typedef struct
{
	int				kind;
	DIR				*dir;
} vfs_dir_desc;

static int	num_vfs_dir_descs = 0;
static int	max_vfs_dir_descs = 0;
static vfs_dir_desc *vfs_dir_descs = NULL;

static	bool	inited = false;
static shmem_startup_hook_type prev_shmem_startup_hook = NULL;

#define PFSD_MIN_MAX_IOSIZE			(4 * 1024)
#define PFSD_DEAFAULT_MAX_IOSIZE	(4 * 1024 * 1024)
#define PFSD_MAX_MAX_IOSIZE			(128 * 1024 * 1024)

static	int max_pfsd_io_size = PFSD_DEAFAULT_MAX_IOSIZE;
static bool	localfs_mode = false;
static bool	pfs_force_mount = true;

typedef void (*vfs_umount_type)(void);

Datum polar_vfs_disk_expansion(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(polar_vfs_disk_expansion);

Datum polar_libpfs_version(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(polar_libpfs_version);

void		_PG_init(void);
void		_PG_fini(void);

static void vfs_startup(void);
static bool is_db_in_replica_mode(bool *force_mount);
static bool file_exists(const char *name);

static void init_vfs_global(void);
static bool init_vfs_function(void);

static int vfs_mount(void);
static void vfs_umount(int code, Datum arg);
static int vfs_remount(void);

static int vfs_creat(const char *path, mode_t mode);
static int vfs_open(const char *path, int flags, mode_t mode);
static int vfs_close(int file);

static ssize_t vfs_read(int file, void *buf, size_t len);
static ssize_t vfs_write(int file, const void *buf, size_t len);
static ssize_t vfs_pread(int file, void *buf, size_t len, off_t offset);
static ssize_t vfs_pwrite(int file, const void *buf, size_t len, off_t offset);

static int vfs_stat(const char *path, struct stat *buf);
static int vfs_fstat(int file, struct stat *buf);
static int vfs_lstat(const char *path, struct stat *buf);
static off_t vfs_lseek(int file, off_t offset, int whence);
static int vfs_access(const char *path, int mode);

static int vfs_fsync(int file);
static int vfs_unlink(const char *fname);
static int vfs_rename(const char *oldfile, const char *newfile);
static int vfs_fallocate(int file, off_t offset, off_t len);
static int vfs_ftruncate(int file, off_t len);
static DIR *vfs_opendir(const char *dirname);
static struct dirent *vfs_readdir(DIR *dir);
static int vfs_closedir(DIR *dir);
static int vfs_mkdir(const char *path, mode_t mode);
static int vfs_rmdir(const char *path);

static inline void vfs_free_vfd(int file);
static inline File vfs_allocate_vfd(void);
static inline bool vfs_allocated_dir(void);
static inline vfs_vfd *vfs_find_file(int file);
static inline int vfs_file_type(const char *path);
static const vfs_mgr *vfs_get_mgr(const char *path);

static const vfs_mgr vfs[] = 
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
		.vfs_mgr_func = NULL
	},
	{
		.vfs_mount = NULL,
		.vfs_umount = NULL,
		.vfs_remount = NULL,
		.vfs_open = pfsd_open,
		.vfs_creat = pfsd_creat,
		.vfs_close = pfsd_close,
		.vfs_read = pfsd_read,
		.vfs_write = pfsd_write,
		.vfs_pread = pfsd_pread,
		.vfs_pwrite = pfsd_pwrite,
		.vfs_stat = pfsd_stat,
		.vfs_fstat = pfsd_fstat,
		.vfs_lstat = pfsd_stat,
		.vfs_lseek = pfsd_lseek,
		.vfs_access = pfsd_access,
		.vfs_fsync = pfsd_fsync,
		.vfs_unlink = pfsd_unlink,
		.vfs_rename = pfsd_rename,
		.vfs_fallocate = pfsd_posix_fallocate,
		.vfs_ftruncate = pfsd_ftruncate,
		.vfs_opendir = pfsd_opendir,
		.vfs_readdir = pfsd_readdir,
		.vfs_closedir = pfsd_closedir,
		.vfs_mkdir = pfsd_mkdir,
		.vfs_rmdir = pfsd_rmdir,
		.vfs_mgr_func = NULL
	}
};

static const vfs_mgr vfs_interface =
{
	.vfs_mount = vfs_mount,
	.vfs_umount = (vfs_umount_type)vfs_umount,
	.vfs_remount = vfs_remount,
	.vfs_open = vfs_open,
	.vfs_creat = vfs_creat,
	.vfs_close = vfs_close,
	.vfs_read = vfs_read,
	.vfs_write = vfs_write,
	.vfs_pread = vfs_pread,
	.vfs_pwrite = vfs_pwrite,
	.vfs_stat = vfs_stat,
	.vfs_fstat = vfs_fstat,
	.vfs_lstat = vfs_lstat,
	.vfs_lseek = vfs_lseek,
	.vfs_access = vfs_access,
	.vfs_fsync = vfs_fsync,
	.vfs_unlink = vfs_unlink,
	.vfs_rename = vfs_rename,
	.vfs_fallocate = vfs_fallocate,
	.vfs_ftruncate = vfs_ftruncate,
	.vfs_opendir = vfs_opendir,
	.vfs_readdir = vfs_readdir,
	.vfs_closedir = vfs_closedir,
	.vfs_mkdir = vfs_mkdir,
	.vfs_rmdir = vfs_rmdir,
	.vfs_mgr_func = vfs_get_mgr
};

void
_PG_init(void)
{
	if (!process_shared_preload_libraries_in_progress)
	{
		elog(WARNING, "polar_vfs init in subbackend %d", (int)getpid());
		return;
	}

	DefineCustomBoolVariable("polar_vfs.pfs_force_mount",
								"pfs force mount mode when ro switch rw",
								NULL,
								&pfs_force_mount,
								true,
								PGC_POSTMASTER,
								0,
								NULL,
								NULL,
								NULL);

	DefineCustomBoolVariable("polar_vfs.localfs_mode",
							"localfs test mode",
							 NULL,
							 &localfs_mode,
							 false,
							 PGC_POSTMASTER,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomIntVariable("polar_vfs.max_pfsd_io_size",
							"max pfsd io size",
							NULL,
							&max_pfsd_io_size,
							PFSD_DEAFAULT_MAX_IOSIZE,
							PFSD_MIN_MAX_IOSIZE,
							PFSD_MAX_MAX_IOSIZE,
							PGC_POSTMASTER,
							0,
							NULL,
							NULL,
							NULL);

	elog(LOG, "polar_vfs loaded in postmaster %d", (int)getpid());

	init_vfs_global();
	init_vfs_function();

	EmitWarningsOnPlaceholders("polar_vfs");

	prev_shmem_startup_hook = shmem_startup_hook;
	shmem_startup_hook = vfs_startup;

	return;
}

void
_PG_fini(void)
{
	shmem_startup_hook = prev_shmem_startup_hook;
}

static void
vfs_startup(void)
{
	if (prev_shmem_startup_hook)
		prev_shmem_startup_hook();

	vfs_mount();

	if (!IsUnderPostmaster)
		on_shmem_exit(vfs_umount, (Datum) 0);

	elog(LOG, "polar_vfs init done");

	return;
}

static void
vfs_umount(int code, Datum arg)
{
	if (code != STATUS_OK)
		elog(WARNING, "subprocess exit abnormally");

	if (localfs_mode)
	{
		elog(LOG, "vfs shutdown success");
		inited = false;
		return;
	}

	if (inited)
	{
		elog(LOG, "umount pfs %s", polar_disk_name);
		if (pfsd_umount_force(polar_disk_name) < 0)
			elog(ERROR, "can't umount PBD %s, id %d", polar_disk_name, polar_hostid);
		else
			elog(LOG, "umount PBD %s, id %d success", polar_disk_name, polar_hostid);
		inited = false;
	}

	return;
}

Datum
polar_vfs_disk_expansion(PG_FUNCTION_ARGS)
{
	char			*expansion_disk_name = text_to_cstring(PG_GETARG_TEXT_PP(0));

	if (strcmp(expansion_disk_name, polar_disk_name) != 0)
		elog(ERROR, "expansion_disk_name %s is not equal with polar_disk_name %s, id %d", 
			expansion_disk_name, polar_disk_name, polar_hostid);

	if (pfsd_mount_growfs(expansion_disk_name) < 0)
		elog(ERROR, "can't growfs PBD %s, id %d", expansion_disk_name, polar_hostid);

	PG_RETURN_BOOL(true);
}

/*
 * Init polar file system
 * create shared memory and mount file system
 */
static int
vfs_mount(void)
{
	bool	do_force_mount = false;
	char	*mode;
	int		flag;

	if (!polar_enable_shared_storage_mode)
		return 1;

	if (is_db_in_replica_mode(&do_force_mount))
	{
		polar_mount_pfs_readonly_mode = true;
		mode = "readonly";
		flag = PFS_RD;
	}
	else
	{
		polar_mount_pfs_readonly_mode = false;
		mode = "readwrite";
		flag = PFS_RDWR;
	}

	if (localfs_mode)
	{
		elog(LOG, "pfs in localfs test mode");
		elog(LOG, "mount pfs %s %s mode success", polar_disk_name, mode);
		polar_vfs_switch = POLAR_VFS_SWITCH_PLUGIN;
		inited = true;
		return 0;
	}

	if (do_force_mount)
	{
		flag |= MNTFLG_PAXOS_BYFORCE;
		mode = "readwrite (force)";
	}

	if (polar_disk_name == NULL ||
		polar_hostid <= 0 ||
		pg_strcasecmp(polar_disk_name, "") == 0 ||
		strlen(polar_disk_name) < 1)
	{
		elog(ERROR, "invalid polar_disk_name or polar_hostid");
	}

	Assert(inited == false);

	if (polar_storage_cluster_name)
		elog(LOG, "init pg cluster %s", polar_storage_cluster_name);

	elog(LOG, "begin mount pfs name %s id %d pid %d backendid %d",
				polar_disk_name, polar_hostid, MyProcPid, MyBackendId);
	if (pfsd_mount(polar_storage_cluster_name, polar_disk_name,
				  polar_hostid, flag) < 0)
		elog(ERROR, "can't mount PBD %s, id %d", polar_disk_name, polar_hostid);

	polar_vfs_switch = POLAR_VFS_SWITCH_PLUGIN;
	inited = true;
	elog(LOG, "mount pfs %s %s mode success", polar_disk_name, mode);

	return 0;
}

/* When read polar_replica = on, we mount pfs use read only mode */
static bool
is_db_in_replica_mode(bool *force_mount)
{
#define RECOVERY_COMMAND_DONE	"recovery.done"

	/* POLAR: ro switch to rw found recovery.done, means we need do force mount */
	if ((polar_local_node_type == POLAR_MASTER) && pfs_force_mount &&
		force_mount && file_exists(RECOVERY_COMMAND_DONE))
		*force_mount = true;

	return polar_local_node_type == POLAR_REPLICA;
}

/* Init local variable for file handle or dir pointer */
static void
init_vfs_global(void)
{
	if (vfs_vfd_cache == NULL)
	{
		vfs_vfd_cache = (vfs_vfd *) malloc(sizeof(vfs_vfd));
		if (vfs_vfd_cache == NULL)
			ereport(FATAL,
					(errcode(ERRCODE_OUT_OF_MEMORY),
					 errmsg("out of memory vfs_vfd_cache")));
		MemSet((char *) &(vfs_vfd_cache[0]), 0, sizeof(vfs_vfd));
		vfs_vfd_cache->fd = VFD_CLOSED;
		size_vfd_cache = 1;
	}
	else
		elog(WARNING, "vfs_vfd_cache already initialized size %zu", size_vfd_cache);

	if (vfs_dir_descs == NULL)
	{
		vfs_dir_desc	*new_descs;
		int				new_max;

		new_max = MIN_VFS_FD_DIR_SIZE;
		new_descs = (vfs_dir_desc *) malloc(new_max * sizeof(vfs_dir_desc));
		if (new_descs == NULL)
			ereport(ERROR,
					(errcode(ERRCODE_OUT_OF_MEMORY),
					 errmsg("out of memory vfs_dir_desc")));
		vfs_dir_descs = new_descs;
		max_vfs_dir_descs = new_max;
		num_vfs_dir_descs = 0;
	}
	else
		elog(WARNING, "vfs_dir_descs already initialized size %d", num_vfs_dir_descs);

	return;
}

static bool
init_vfs_function(void)
{
	if (polar_enable_shared_storage_mode == false)
		return false;

	polar_vfs[POLAR_VFS_SWITCH_PLUGIN] = vfs_interface;

	return true;
}

static int
vfs_remount(void)
{
	int		flag = 0;

	if (localfs_mode)
	{
		elog(LOG, "pfs in localfs mode");
		return 0;
	}

	polar_mount_pfs_readonly_mode = false;
	flag = PFS_RDWR;

	if (file_exists(RECOVERY_COMMAND_DONE))
		flag |=PFS_PAXOS_BYFORCE;

	elog(LOG, "begin remount pfs name %s id %d pid %d backendid %d flag=%x",
				polar_disk_name, polar_hostid, MyProcPid, MyBackendId, flag);

	if (pfsd_remount(polar_storage_cluster_name, polar_disk_name,
				  polar_hostid, flag) < 0)
		elog(ERROR, "can't mount PBD %s, id %d", polar_disk_name, polar_hostid);
	/* after remount, clean recovey.done */
	unlink(RECOVERY_COMMAND_DONE);
	elog(LOG, "removed file \"%s\"", RECOVERY_COMMAND_DONE);
	elog(LOG, "remount pfs %s readwrite mode success", polar_disk_name);
	return 0;
}

static int
vfs_creat(const char *path, mode_t mode)
{
	int		file = vfs_allocate_vfd();
	vfs_vfd		*vfdP = &vfs_vfd_cache[file];

	elog(LOG, "vfs creat file %s, fd %d file %d num open file %d", vfdP->file_name, vfdP->fd, file, num_open_file);
	vfdP->kind = vfs_file_type(path);
	vfdP->fd = vfs[vfdP->kind].vfs_creat(path, mode);
	if (vfdP->fd < 0)
	{
		int save_errno = errno;

		vfs_free_vfd(file);
		errno = save_errno;
		return -1;
	}

	Assert(vfdP->file_name == NULL);
	vfdP->file_name = strdup(path);	
	vfdP->file_size = 0;
	num_open_file++;

	return file;
}

static int
vfs_open(const char *path, int flags, mode_t mode)
{
	int			file = -1;
	vfs_vfd		*vfdP = NULL;

	if (path == NULL)
		return -1;

	file = vfs_allocate_vfd();
	vfdP = &vfs_vfd_cache[file];
	elog(DEBUG1, "vfs open file %s num open file %d", path, num_open_file);
	vfdP->kind = vfs_file_type(path);
	pgstat_report_wait_start(WAIT_EVENT_DATA_VFS_FILE_OPEN);
	vfdP->fd = vfs[vfdP->kind].vfs_open(path, flags, mode);
	if (vfdP->fd < 0)
	{
		int	save_errno = errno;

		pgstat_report_wait_end();
		vfs_free_vfd(file);
		errno = save_errno;
		return -1;
	}

	Assert(vfdP->file_name == NULL);
	vfdP->file_name = strdup(path);	
	vfdP->file_size = 0;
	num_open_file++;

	return file;
}

static int
vfs_close(int file)
{
	vfs_vfd		*vfdP = NULL;

	vfdP = vfs_find_file(file);
	elog(DEBUG1, "vfs close file %s, fd %d file %d num open file %d", vfdP->file_name, vfdP->fd, file, num_open_file);
	if (vfdP->fd != VFD_CLOSED)
	{
		if (vfs[vfdP->kind].vfs_close(vfdP->fd))
			elog(WARNING, "vfs could not close file \"%s\": %m", vfdP->file_name);

		--num_open_file;
		vfdP->fd = VFD_CLOSED;
	}
	else
		elog(WARNING, "vfs file %s file %d not open", vfdP->file_name, file);

	vfs_free_vfd(file);

	return 0;
}

static ssize_t
vfs_write(int file, const void *buf, size_t len)
{
	vfs_vfd		*vfdP = NULL;
	ssize_t		res = -1;
	char		*from;
	ssize_t		nleft;
	ssize_t		writesize;
	ssize_t		count = 0;

	vfdP = vfs_find_file(file);
	nleft = len;
	from = (char *)buf;

	while(nleft > 0)
	{
		writesize = Min(nleft, max_pfsd_io_size);
		errno = 0;
		res = vfs[vfdP->kind].vfs_write(vfdP->fd, from, writesize);
		if (res <= 0)
		{
			if (count == 0)
				count = res;

			break;
		}

		count += res;
		nleft -= res;
		from += res;
	}

	return count;
}

static ssize_t
vfs_read(int file, void *buf, size_t len)
{
	vfs_vfd		*vfdP = NULL;
	ssize_t		res = -1;
	ssize_t		iolen = 0;
	ssize_t		nleft = len;
	char		*from = (char *)buf;
	ssize_t		count = 0;

	vfdP = vfs_find_file(file);

	while (nleft > 0)
	{
		iolen = Min(nleft, max_pfsd_io_size);
		errno = 0;
		res = vfs[vfdP->kind].vfs_read(vfdP->fd, from, iolen);
		if (res <= 0)
		{
			if (count == 0)
				count = res;

			break;
		}

		count += res;
		from += res;
		nleft -= res;
	}

	return count;
}

static ssize_t
vfs_pread(int file, void *buf, size_t len, off_t offset)
{
	vfs_vfd		*vfdP = NULL;
	ssize_t		res = -1;
	ssize_t		iolen = 0;
	off_t		off = offset;
	ssize_t		nleft = len;
	char		*from = (char *)buf;
	ssize_t		count = 0;

	vfdP = vfs_find_file(file);

	while (nleft > 0)
	{
		iolen = Min(nleft, max_pfsd_io_size);
		errno = 0;
		res = vfs[vfdP->kind].vfs_pread(vfdP->fd, from, iolen, off);
		if (res <= 0)
		{
			if (count == 0)
				count = res;

			break;
		}

		count += res;
		from += res;
		off += res;
		nleft -= res;
	}

	return count;
}

static ssize_t
vfs_pwrite(int file, const void *buf, size_t len, off_t offset)
{
	vfs_vfd		*vfdP = NULL;
	char		*from;
	ssize_t		nleft;
	off_t		startoffset;
	ssize_t		writesize;
	ssize_t 	res = -1;
	ssize_t		count = 0;

	vfdP = vfs_find_file(file);

	nleft = len;
	from = (char *)buf;
	startoffset = offset;

	while(nleft > 0)
	{
		writesize = Min(nleft, max_pfsd_io_size);
		errno = 0;
		res = vfs[vfdP->kind].vfs_pwrite(vfdP->fd, from, writesize, startoffset);
		if (res <= 0)
		{
			if (count == 0)
				count = res;

			break;
		}

		count += res;
		nleft -= res;
		from += res;
		startoffset += res;
	}

	return count;
}

static int
vfs_stat(const char *path, struct stat *buf)
{
	int			rc = -1;
	int			kind = -1;

	if (path == NULL)
		return -1;

	kind = vfs_file_type(path);
	rc = vfs[kind].vfs_stat(path, buf);

	return rc;
}

static int
vfs_fstat(int file, struct stat *buf)
{
	vfs_vfd		*vfdP = NULL;
	int			rc = 0;

	vfdP = vfs_find_file(file);
	rc = vfs[vfdP->kind].vfs_fstat(vfdP->fd, buf);

	return rc;
}

static int
vfs_lstat(const char *path, struct stat *buf)
{
	int			rc = -1;
	int			kind = -1;

	if (path == NULL)
		return -1;

	kind = vfs_file_type(path);
	rc = vfs[kind].vfs_lstat(path, buf);

	return rc;
}

static off_t
vfs_lseek(int file, off_t offset, int whence)
{
	vfs_vfd		*vfdP = NULL;
	off_t		rc = 0;

	vfdP = vfs_find_file(file);
	pgstat_report_wait_start(WAIT_EVENT_DATA_VFS_FILE_LSEEK);
	rc = vfs[vfdP->kind].vfs_lseek(vfdP->fd, offset, whence);
	pgstat_report_wait_end();

	return rc;
}

int
vfs_access(const char *path, int mode)
{
	int		rc = -1;
	int		kind = -1;

	if (path == NULL)
		return -1;

	kind = vfs_file_type(path);
	rc = vfs[kind].vfs_access(path, mode);

	return rc;
}

static int
vfs_fsync(int file)
{
	vfs_vfd		*vfdP = NULL;
	int			rc = 0;

	vfdP = vfs_find_file(file);
	rc = vfs[vfdP->kind].vfs_fsync(vfdP->fd);

	return rc;
}

static int
vfs_unlink(const char *fname)
{
	int		rc = -1;
	int		kind = -1;

	if (fname == NULL)
		return -1;

	elog(LOG, "vfs_unlink %s", fname);
	kind = vfs_file_type(fname);
	rc = vfs[kind].vfs_unlink(fname);

	return rc;
}

static int
vfs_rename(const char *oldfile, const char *newfile)
{
	int			rc = -1;
	int			kindold = -1;
	int			kindnew = -1;

	if (oldfile == NULL || newfile == NULL)
		return -1;

	kindold = vfs_file_type(oldfile);
	kindnew = vfs_file_type(newfile);
	elog(LOG, "vfs_rename from %s to %s", oldfile, newfile);
	if (kindold == kindnew)
		rc = vfs[kindold].vfs_rename(oldfile, newfile);
	else
		elog(ERROR, "vfs unsupported rename operation");

	return rc;
}

static int
vfs_fallocate(int file, off_t offset, off_t len)
{
	vfs_vfd		*vfdP = NULL;
	int			rc = 0;

	vfdP = vfs_find_file(file);
	elog(LOG, "vfs_fallocate from %s", vfdP->file_name);
	rc = vfs[vfdP->kind].vfs_fallocate(vfdP->fd, offset, len);

	return rc;
}

static int
vfs_ftruncate(int file, off_t len)
{
	vfs_vfd		*vfdP = NULL;
	int			rc = 0;

	vfdP = vfs_find_file(file);
	elog(LOG, "vfs_ftruncate from %s", vfdP->file_name);
	rc = vfs[vfdP->kind].vfs_ftruncate(vfdP->fd, len);

	return rc;
}

static DIR *
vfs_opendir(const char *dirname)
{
	DIR		   *dir = NULL;
	int			kind = -1;

	if (dirname == NULL)
		return NULL;

	if (!vfs_allocated_dir())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_RESOURCES),
				 errmsg("exceeded max_vfs_dir_descs (%d) while trying to open directory \"%s\"",
						max_vfs_dir_descs, dirname)));

	kind = vfs_file_type(dirname);
	dir = vfs[kind].vfs_opendir(dirname);
	if (dir != NULL)
	{
		vfs_dir_desc *desc = &vfs_dir_descs[num_vfs_dir_descs];
		desc->kind = kind;
		desc->dir = dir;
		num_vfs_dir_descs++;
		elog(LOG, "vfs open dir %s, num open dir %d", dirname, num_vfs_dir_descs);
		return desc->dir;
	}

	return NULL;
}

static struct dirent *
vfs_readdir(DIR *dir)
{
	int		i;

	if (dir == NULL)
		return NULL;

	for (i = num_vfs_dir_descs; --i >= 0;)
	{
		vfs_dir_desc *desc = &vfs_dir_descs[i];

		if (desc->dir == dir)
			return vfs[desc->kind].vfs_readdir(dir);
	}

	elog(WARNING, "vfs could not find vfs_desc");

	return NULL;
}

static int
vfs_closedir(DIR *dir)
{
	int		i;
	int		result = -1;

	for (i = num_vfs_dir_descs; --i >= 0;)
	{
		vfs_dir_desc *desc = &vfs_dir_descs[i];

		if (desc->dir == dir)
		{
			result = vfs[desc->kind].vfs_closedir(dir);
			num_vfs_dir_descs--;

			/* POLAR: this is end of array */
			if (i == num_vfs_dir_descs)
			{
				desc->dir = NULL;
				desc->kind = VFS_UNKNOWN_FILE;
			}
			else
			{
				/* POLAR: Compact storage in the allocatedDescs array */
				*desc = vfs_dir_descs[num_vfs_dir_descs];
			}
			return result;
		}
	}

	elog(WARNING, "vfs does not find vfs_dir from vfs_dir_descs");

	return result;
}

static int
vfs_mkdir(const char *path, mode_t mode)
{
	int			rc = -1;
	int			kind = -1;

	if (path == NULL)
		return -1;

	kind = vfs_file_type(path);
	rc = vfs[kind].vfs_mkdir(path, mode);

	return rc;
}

static int
vfs_rmdir(const char *path)
{
	int			rc = -1;
	int			kind = -1;

	if (path == NULL)
		return -1;

	kind = vfs_file_type(path);
	rc = vfs[kind].vfs_rmdir(path);

	return rc;
}

/* Determine if the file is on a shared storage device */
static inline int
vfs_file_type(const char *path)
{
	static int	polar_disk_strsize = 0;
	int		strpathlen = 0;

	if (localfs_mode)
		return VFS_LOCAL_FILE;

	if (path == NULL)
		return VFS_LOCAL_FILE;

	if (polar_disk_strsize == 0)
		polar_disk_strsize = strlen(polar_disk_name);

	strpathlen = strlen(path);
	if (strpathlen <= 1)
		return VFS_LOCAL_FILE;

	if (strpathlen < polar_disk_strsize + 1)
		return VFS_LOCAL_FILE;

	if (path[0] != '/')
		return VFS_LOCAL_FILE;

	if (strncmp(polar_disk_name, path + 1, polar_disk_strsize) == 0)
	{
		return VFS_PFS_FILE;
	}

	return VFS_LOCAL_FILE;
}

/* local file fd cache */
static inline int
vfs_allocate_vfd(void)
{
	Index		i;
	File		file;

	elog(DEBUG1, "vfs_allocate_vfd. cache size %zu", size_vfd_cache);

	Assert(size_vfd_cache > 0);

	if (vfs_vfd_cache[0].next_free == 0)
	{
		Size		new_cache_size = size_vfd_cache * 2;
		vfs_vfd		*new_vfd_cache;

		if (new_cache_size < 32)
			new_cache_size = 32;

		new_vfd_cache = (vfs_vfd *) realloc(vfs_vfd_cache, sizeof(vfs_vfd) * new_cache_size);
		if (new_vfd_cache == NULL)
			ereport(ERROR,
					(errcode(ERRCODE_OUT_OF_MEMORY),
					 errmsg("out of memory vfs_allocate_vfd")));

		vfs_vfd_cache = new_vfd_cache;
		for (i = size_vfd_cache; i < new_cache_size; i++)
		{
			MemSet((char *) &(vfs_vfd_cache[i]), 0, sizeof(vfs_vfd));
			vfs_vfd_cache[i].next_free = i + 1;
			vfs_vfd_cache[i].fd = VFD_CLOSED;
		}
		vfs_vfd_cache[new_cache_size - 1].next_free = 0;
		vfs_vfd_cache[0].next_free = size_vfd_cache;
		size_vfd_cache = new_cache_size;
	}

	file = vfs_vfd_cache[0].next_free;
	vfs_vfd_cache[0].next_free = vfs_vfd_cache[file].next_free;

	return file;
}

static inline void
vfs_free_vfd(int file)
{
	vfs_vfd		   *vfdP = &vfs_vfd_cache[file];

	elog(DEBUG1, "vfs_free_vfd: %d (%s)",
			   file, vfdP->file_name ? vfdP->file_name : "");

	if (vfdP->file_name != NULL)
	{
		free(vfdP->file_name);
		vfdP->file_name = NULL;
	}

	vfdP->kind = VFS_UNKNOWN_FILE;
	vfdP->file_size = 0;
	vfdP->next_free = vfs_vfd_cache[0].next_free;
	vfs_vfd_cache[0].next_free = file;
}

static inline bool
vfs_allocated_dir(void)
{
	vfs_dir_desc	*new_descs;
	int				new_max;

	if (num_vfs_dir_descs < max_vfs_dir_descs)
		return true;

	Assert(vfs_dir_descs);

	new_max = max_safe_fds / 2;
	if (new_max > max_vfs_dir_descs)
	{
		new_descs = (vfs_dir_desc *) realloc(vfs_dir_descs,
											new_max * sizeof(vfs_dir_desc));
		if (new_descs == NULL)
			ereport(ERROR,
					(errcode(ERRCODE_OUT_OF_MEMORY),
					 errmsg("out of memory vfs_allocated_dir")));
		vfs_dir_descs = new_descs;
		max_vfs_dir_descs = new_max;
		return true;
	}

	return false;
}

static inline vfs_vfd *
vfs_find_file(int file)
{
	if (file >= size_vfd_cache)
		elog(ERROR, "vfs file fd %d out of cache", file);

	return &vfs_vfd_cache[file];
}

Datum
polar_libpfs_version(PG_FUNCTION_ARGS)
{
	int64 version_num = pfsd_meta_version_get();
	const char	*version_str = pfsd_build_version_get();
	char	libpfs_version[MAXPGPATH] = {0};

	snprintf(libpfs_version, MAXPGPATH-1, "%s version number "INT64_FORMAT"", version_str, version_num);
        PG_RETURN_TEXT_P(cstring_to_text(libpfs_version));
}

static bool
file_exists(const char *name)
{
	struct stat st;

	AssertArg(name != NULL);

	if (stat(name, &st) == 0)
		return S_ISDIR(st.st_mode) ? false : true;
	else if (!(errno == ENOENT || errno == ENOTDIR || errno == EACCES))
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not access file \"%s\": %m", name)));

	return false;
}

static const vfs_mgr*
vfs_get_mgr(const char *path)
{
	int kind = vfs_file_type(path);

	return &vfs[kind];
}

