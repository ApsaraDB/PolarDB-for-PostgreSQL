/*-------------------------------------------------------------------------
 *
 * polar_fd.h
 *
 *	  PolarDB Virtual file descriptor definitions.
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
 * src/include/storage/polar_fd.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef POLARFD_H
#define POLARFD_H

#include <dirent.h>
#include <sys/stat.h>
#include "utils/resowner.h"


/*
 * polar file system interface
 */
#define POLAR_FILE_IN_SHARED_STORAGE()				(polar_enable_shared_storage_mode)

#define POLAR_ENABLE_PWRITE()			(polar_enable_pwrite)
#define POLAR_ENABLE_PREAD()			(polar_enable_pread)

#define	AmPolarBackgroundWriterProcess()		(polar_enable_shared_storage_mode && AmBackgroundWriterProcess())

#define	POLAR_VFS_SWITCH_LOCAL		0
#define	POLAR_VFS_SWITCH_PLUGIN		1
#define POLAR_VFS_PROTOCOL_TAG	"://"

typedef enum PolarNodeType
{
	POLAR_UNKNOWN = 0,
	POLAR_MASTER = 1,
	POLAR_REPLICA,
	POLAR_STANDBY
} PolarNodeType;

extern PolarNodeType polar_local_node_type;
extern bool polar_mount_pfs_readonly_mode;
extern int	polar_vfs_switch;
extern bool polar_openfile_with_readonly_in_replica;

typedef int (*vfs_open_type) (const char *path, int flags, mode_t mode);

typedef struct vfs_mgr
{
	int			(*vfs_mount) (void);
	void		(*vfs_umount) (void);
	int			(*vfs_remount) (void);
	int			(*vfs_open) (const char *path, int flags, mode_t mode);
	int			(*vfs_creat) (const char *path, mode_t mode);
	int			(*vfs_close) (int fd);
	ssize_t		(*vfs_read) (int fd, void *buf, size_t len);
	ssize_t		(*vfs_write) (int fd, const void *buf, size_t len);
	ssize_t		(*vfs_pread) (int fd, void *buf, size_t len, off_t offset);
	ssize_t		(*vfs_pwrite) (int fd, const void *buf, size_t len, off_t offset);
	int			(*vfs_stat) (const char *path, struct stat *buf);
	int			(*vfs_fstat) (int fd, struct stat *buf);
	int			(*vfs_lstat) (const char *path, struct stat *buf);
	off_t		(*vfs_lseek) (int fd, off_t offset, int whence);
	int			(*vfs_access) (const char *path, int mode);
	int			(*vfs_fsync) (int fd);
	int			(*vfs_unlink) (const char *path);
	int			(*vfs_rename) (const char *oldpath, const char *newpath);
	int			(*vfs_fallocate) (int fd, off_t offset, off_t len);
	int			(*vfs_ftruncate) (int fd, off_t len);
	DIR		   *(*vfs_opendir) (const char *path);
	struct dirent *(*vfs_readdir) (DIR *dir);
	int			(*vfs_closedir) (DIR *dir);
	int			(*vfs_mkdir) (const char *path, mode_t mode);
	int			(*vfs_rmdir) (const char *path);
	const struct vfs_mgr *(*vfs_mgr_func) (const char *path);
} vfs_mgr;

extern vfs_mgr polar_vfs[];

extern int	polar_mount(void);
extern void polar_umount(void);
extern int	polar_remount(void);

extern const vfs_mgr *polar_vfs_mgr(const char *path);

extern int	polar_open(const char *path, int flags, mode_t mode);
extern int	polar_creat(const char *path, mode_t mode);
extern int	polar_close(int fd);

extern ssize_t polar_read(int fd, void *buf, size_t len);
extern ssize_t polar_write(int fd, const void *buf, size_t len);
extern ssize_t polar_pread(int fd, void *buf, size_t len, off_t offset);
extern ssize_t polar_pwrite(int fd, const void *buf, size_t len, off_t offset);

extern int	polar_stat(const char *path, struct stat *buf);
extern int	polar_fstat(int fd, struct stat *buf);
extern int	polar_lstat(const char *path, struct stat *buf);
extern off_t polar_lseek(int fd, off_t offset, int whence);
extern int	polar_fallocate(int fd, off_t offset, off_t len);
extern int	polar_ftruncate(int fd, off_t len);
extern int	polar_access(const char *path, int mode);
extern int	polar_fsync(int fd);
extern int	polar_unlink(const char *fname);
extern int	polar_rename(const char *oldfile, const char *newfile);

extern DIR *polar_opendir(const char *path);
extern struct dirent *polar_readdir(DIR *dir);
extern int	polar_closedir(DIR *dir);
extern int	polar_mkdir(const char *path, mode_t mode);
extern int	polar_rmdir(const char *path);
extern int	polar_make_pg_directory(const char *directoryName);

extern void polar_copy_file(char *fromfile, char *tofile, bool skiperr);
extern void polar_copydir(char *fromdir, char *todir, bool recurse, bool clean, bool skiperr);

extern void polar_make_file_path_level3(char *path, char *base, char *file_path);
extern void polar_make_file_path_level2(char *path, char *file_path);

extern void polar_reset_vfs_switch(void);
extern void polar_init_node_type(void);
extern PolarNodeType polar_node_type_by_file(void);
extern struct dirent *polar_read_dir_ext(DIR *dir, const char *dirname, int elevel, int *err);
const char *polar_path_remove_protocol(const char *path);
#endif
