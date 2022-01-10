/*-------------------------------------------------------------------------
 *
 * polar_fs_fe.h
 *	  Polardb  file storage definitions for fontend.
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
 * NB: 
 *      If you want to use pfsd in your code, 
 *      don't forget to add libpfsd after libpgcommon in Makefile.
 *
 * IDENTIFICATION
 *      src/include/common/polar_fs_fe.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef POLAR_FS_FE_H
#define POLAR_FS_FE_H

#include "postgres_fe.h"

#include <unistd.h>
#include <dirent.h>
#include <sys/stat.h>
#include <fcntl.h>

#include "common/file_perm.h"
#include "common/file_utils.h"

extern void polar_fs_init(bool is_pfs, char *polar_storage_cluster_name, char *polar_disk_name, int polar_hostid);
extern void polar_fs_destory(bool is_pfs, char *polar_disk_name, int polar_hostid);
extern int polar_chmod(const char *path, mode_t mode, bool is_pfs);
extern int polar_mkdir(const char *path, mode_t mode, bool is_pfs);
extern int polar_open(const char *path, int flags, mode_t mode, bool is_pfs);
extern int polar_close(int fd, bool is_pfs);
extern int polar_write(int fd, const void *buf, size_t len, bool is_pfs);
extern int polar_read(int fd, void *buf, size_t len, bool is_pfs);
extern int polar_fsync(int fd, bool is_pfs);
extern int polar_unlink(const char *path, bool is_pfs);
extern int polar_stat(const char *path, struct stat *buf, bool is_pfs);
extern int polar_lstat(const char *path, struct stat *buf, bool is_pfs);
extern int polar_rename(const char *oldpath, const char *newpath, bool is_pfs);
extern int polar_closedir(DIR *dir, bool is_pfs);
extern DIR *polar_opendir(const char *path, bool is_pfs);
extern struct dirent *polar_readdir(DIR *dir, bool is_pfs);
extern int polar_check_dir(const char *dir, bool is_pfs);
extern int polar_mkdir_p(char *path, int omode, bool is_pfs);
extern int polar_rmdir(const char *path, bool is_pfs);
extern off_t polar_lseek(int fd, off_t offset, int whence, bool is_pfs);
extern int polar_fsync_fname(const char *fname, bool isdir, const char *progname, bool is_pfs);
extern int polar_fsync_parent_path(const char *fname, const char *progname, bool is_pfs);
extern int polar_durable_rename(const char *oldfile, const char *newfile, const char *progname, bool is_pfs);
extern bool polar_rmtree(const char *path, bool rmtopdir, bool is_pfs);
extern char	**polarfnames(const char *path, bool is_pfs);
extern void polarfnames_cleanup(char **filenames);

#endif
