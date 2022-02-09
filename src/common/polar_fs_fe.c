/*-------------------------------------------------------------------------
 *
 * polar_fs_fe.c - 
 *
 * Author: 
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
 *		  src/common/polar_fs_fe.c
 *-------------------------------------------------------------------------
 */

#include "common/polar_fs_fe.h"
#include "common/file_utils.h"
#ifdef USE_PFSD
#include "pfsd_sdk.h"
#endif

#define LOCAL_IO_INDEX 0
#define PFS_IO_INDEX 1

typedef int	(*vfs_open_type)(const char *path, int flags, mode_t mode);
typedef struct vfs_mgr
{
	int (*vfs_open)(const char *path, int flags, mode_t mode);
	int (*vfs_creat)(const char *path, mode_t mode);
	int (*vfs_close)(int fd);
	ssize_t (*vfs_read)(int fd, void *buf, size_t len);
	ssize_t (*vfs_write)(int fd, const void *buf, size_t len);
	ssize_t (*vfs_pread)(int fd, void *buf, size_t len, off_t offset);
	ssize_t (*vfs_pwrite)(int fd, const void *buf, size_t len, off_t offset);
	int (*vfs_stat)(const char *path, struct stat *buf);
	int (*vfs_fstat)(int fd, struct stat *buf);
	int (*vfs_lstat)(const char *path, struct stat *buf);
	off_t (*vfs_lseek)(int fd, off_t offset, int whence);
	off_t (*vfs_lseek_cache)(int fd, off_t offset, int whence);
	int (*vfs_access)(const char *path, int mode);
	int (*vfs_fsync)(int fd);
	int (*vfs_unlink)(const char *path);
	int (*vfs_rename)(const char *oldpath, const char *newpath);
	int (*vfs_fallocate)(int fd, off_t offset, off_t len);
	int (*vfs_ftruncate)(int fd, off_t len);
	DIR *(*vfs_opendir)(const char *path);
	struct dirent *(*vfs_readdir)(DIR *dir);
	int (*vfs_closedir)(DIR *dir);
	int (*vfs_mkdir)(const char *path, mode_t mode);
	int (*vfs_rmdir)(const char *path);
	int (*vfs_chmod) (const char *file, mode_t mode);
} vfs_mgr;

typedef struct vfs_vfd
{
	int				fd;
	int				kind;
	char			*file_name;
} vfs_vfd;

static const vfs_mgr vfs[] = 
{
	{
		.vfs_open =(vfs_open_type)open,
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
		.vfs_fsync = fsync,
		.vfs_unlink = unlink,
		.vfs_rename = rename,
		.vfs_fallocate = posix_fallocate,
		.vfs_ftruncate = ftruncate,
		.vfs_opendir = opendir,
		.vfs_readdir = readdir,
		.vfs_closedir = closedir,
		.vfs_mkdir = mkdir,
		.vfs_rmdir = rmdir,
		.vfs_chmod = chmod
	},
#ifdef USE_PFSD
	{
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
		.vfs_lseek_cache = pfsd_lseek,
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
		.vfs_chmod = pfsd_chmod
	}
#endif
};

static int polar_vfs_kind(bool is_pfs);

#ifdef USE_PFSD
void polar_fs_init(bool is_pfs, char *polar_storage_cluster_name, char *polar_disk_name, int polar_hostid)
{
	if (!is_pfs)
	{
		return;
	} 

	if (polar_disk_name == NULL || polar_hostid <= 0 ||
		pg_strcasecmp(polar_disk_name, "") == 0 || strlen(polar_disk_name) < 1)
	{
		fprintf(stderr, _("invalid polar_disk_name or polar_hostid"));
	}
	if (pfsd_mount(polar_storage_cluster_name, polar_disk_name,
				  polar_hostid, PFS_RDWR) < 0)
		fprintf(stderr, _("can't mount cluster %s PBD %s, id %d"), 
		polar_storage_cluster_name, 
		polar_disk_name, polar_hostid);
}

void polar_fs_destory(bool is_pfs, char *polar_disk_name, int polar_hostid)
{
	if (!is_pfs)
		return;
	if (pfsd_umount_force(polar_disk_name) < 0)
		fprintf(stderr, _("can't mount PBD %s, id %d"), polar_disk_name, polar_hostid);
}
#else
void polar_fs_init(bool is_pfs, char *polar_storage_cluster_name, char *polar_disk_name, int polar_hostid) {}
void polar_fs_destory(bool is_pfs, char *polar_disk_name, int polar_hostid) {}
#endif

int polar_chmod(const char *path, mode_t mode, bool is_pfs)
{
	int io_kind = polar_vfs_kind(is_pfs);
	return vfs[io_kind].vfs_chmod(path, mode);
}

int polar_mkdir(const char *path, mode_t mode, bool is_pfs)
{
	int io_kind = polar_vfs_kind(is_pfs);
	return vfs[io_kind].vfs_mkdir(path, mode);
}

int polar_open(const char *path, int flags, mode_t mode, bool is_pfs)
{
	int io_kind = polar_vfs_kind(is_pfs);
	return vfs[io_kind].vfs_open(path, flags, mode);
}

int polar_close(int fd, bool is_pfs)
{
	int io_kind = polar_vfs_kind(is_pfs);
	return vfs[io_kind].vfs_close(fd);
}

int polar_write(int fd, const void *buf, size_t len, bool is_pfs)
{
	int io_kind = polar_vfs_kind(is_pfs);
	return vfs[io_kind].vfs_write(fd, buf, len);
}

int polar_read(int fd, void *buf, size_t len, bool is_pfs)
{
	int io_kind = polar_vfs_kind(is_pfs);
	return vfs[io_kind].vfs_read(fd, buf, len);
}

int polar_fsync(int fd, bool is_pfs)
{
	int io_kind = polar_vfs_kind(is_pfs);
	return vfs[io_kind].vfs_fsync(fd);
}

int polar_closedir(DIR *dir, bool is_pfs)
{
	int io_kind = polar_vfs_kind(is_pfs);
	return vfs[io_kind].vfs_closedir(dir);
}

DIR *polar_opendir(const char *path, bool is_pfs)
{
	int io_kind = polar_vfs_kind(is_pfs);
	return vfs[io_kind].vfs_opendir(path);
}

struct dirent *polar_readdir(DIR *dir, bool is_pfs)
{
	int io_kind = polar_vfs_kind(is_pfs);
	return vfs[io_kind].vfs_readdir(dir);
}

int polar_stat(const char *path, struct stat *buf, bool is_pfs)
{
	int io_kind = polar_vfs_kind(is_pfs);
	return vfs[io_kind].vfs_stat(path, buf);
}

int polar_lstat(const char *path, struct stat *buf, bool is_pfs)
{
	int io_kind = polar_vfs_kind(is_pfs);
	return vfs[io_kind].vfs_lstat(path, buf);
}

off_t polar_lseek(int fd, off_t offset, int whence, bool is_pfs)
{
	int io_kind = polar_vfs_kind(is_pfs);
	return vfs[io_kind].vfs_lseek(fd, offset, whence);
}

int polar_rename(const char *oldpath, const char *newpath, bool is_pfs)
{
	int io_kind = polar_vfs_kind(is_pfs);
	return vfs[io_kind].vfs_rename(oldpath, newpath);
}

int polar_unlink(const char *path, bool is_pfs)
{
	int io_kind = polar_vfs_kind(is_pfs);
	return vfs[io_kind].vfs_unlink(path);
}

int polar_rmdir(const char *path, bool is_pfs)
{
	int io_kind = polar_vfs_kind(is_pfs);
	return vfs[io_kind].vfs_rmdir(path);
}

int
polar_check_dir(const char *dir, bool is_pfs)
{
	int			result = 1;
	DIR		   *chkdir;
	struct dirent *file;
	bool		dot_found = false;
	bool		mount_found = false;
	int			readdir_errno;

	chkdir = polar_opendir(dir, is_pfs);
	if (chkdir == NULL)
		return (errno == ENOENT) ? 0 : -1;

	while (errno = 0, (file = polar_readdir(chkdir, is_pfs)) != NULL)
	{
		if (strcmp(".", file->d_name) == 0 ||
			strcmp("..", file->d_name) == 0)
		{
			/* skip this and parent directory */
			continue;
		}
#ifndef WIN32
		/* file starts with "." */
		else if (file->d_name[0] == '.')
		{
			dot_found = true;
		}
		/* lost+found directory */
		else if (strcmp("lost+found", file->d_name) == 0)
		{
			mount_found = true;
		}
#endif
		else
		{
			result = 4;			/* not empty */
			break;
		}
	}

	if (errno)
		result = -1;			/* some kind of I/O error? */

	/* Close chkdir and avoid overwriting the readdir errno on success */
	readdir_errno = errno;
	if (polar_closedir(chkdir, is_pfs))
		result = -1;			/* error executing closedir */
	else
		errno = readdir_errno;

	/* We report on mount point if we find a lost+found directory */
	if (result == 1 && mount_found)
		result = 3;

	/* We report on dot-files if we _only_ find dot files */
	if (result == 1 && dot_found)
		result = 2;

	return result;
}

int
polar_mkdir_p(char *path, int omode, bool is_pfs)
{
	struct stat sb;
	mode_t		numask,
				oumask;
	int			last,
				retval;
	char	   *p;
	bool	is_polar_disk_name = true;

	retval = 0;
	p = path;

#ifdef WIN32
	/* skip network and drive specifiers for win32 */
	if (strlen(p) >= 2)
	{
		if (p[0] == '/' && p[1] == '/')
		{
			/* network drive */
			p = strstr(p + 2, "/");
			if (p == NULL)
			{
				errno = EINVAL;
				return -1;
			}
		}
		else if (p[1] == ':' &&
				 ((p[0] >= 'a' && p[0] <= 'z') ||
				  (p[0] >= 'A' && p[0] <= 'Z')))
		{
			/* local drive */
			p += 2;
		}
	}
#endif

	/*
	 * POSIX 1003.2: For each dir operand that does not name an existing
	 * directory, effects equivalent to those caused by the following command
	 * shall occur:
	 *
	 * mkdir -p -m $(umask -S),u+wx $(dirname dir) && mkdir [-m mode] dir
	 *
	 * We change the user's umask and then restore it, instead of doing
	 * chmod's.  Note we assume umask() can't change errno.
	 */
	oumask = umask(0);
	numask = oumask & ~(S_IWUSR | S_IXUSR);
	(void) umask(numask);

	if (p[0] == '/')			/* Skip leading '/'. */
		++p;
	for (last = 0; !last; ++p)
	{
		if (p[0] == '\0')
			last = 1;
		else if (p[0] != '/')
			continue;
		
		if (is_pfs && is_polar_disk_name)
		{
			is_polar_disk_name = false;
			continue;
		}

		*p = '\0';
		if (!last && p[1] == '\0')
			last = 1;

		if (last)
			(void) umask(oumask);

		/* check for pre-existing directory */
		if (polar_stat(path, &sb, is_pfs) == 0)
		{
			if (!S_ISDIR(sb.st_mode))
			{
				if (last)
					errno = EEXIST;
				else
					errno = ENOTDIR;
				retval = -1;
				break;
			}
		}
		else if (polar_mkdir(path, last ? omode : S_IRWXU | S_IRWXG | S_IRWXO, is_pfs) < 0)
		{
			retval = -1;
			break;
		}
		if (!last)
			*p = '/';
	}

	/* ensure we restored umask */
	(void) umask(oumask);

	return retval;
}


/*
 * fsync_fname -- Try to fsync a file or directory
 *
 * Ignores errors trying to open unreadable files, or trying to fsync
 * directories on systems where that isn't allowed/required.  Reports
 * other errors non-fatally.
 */
int
polar_fsync_fname(const char *fname, bool isdir, const char *progname, bool is_pfs)
{
	int			fd;
	int			flags;
	int			returncode;

	/*
	 * Some OSs require directories to be opened read-only whereas other
	 * systems don't allow us to fsync files opened read-only; so we need both
	 * cases here.  Using O_RDWR will cause us to fail to fsync files that are
	 * not writable by our userid, but we assume that's OK.
	 */
	flags = PG_BINARY;
	if (!isdir)
		flags |= O_RDWR;
	else
		flags |= O_RDONLY;

	/*
	 * Open the file, silently ignoring errors about unreadable files (or
	 * unsupported operations, e.g. opening a directory under Windows), and
	 * logging others.
	 */
	fd = polar_open(fname, flags, 0, is_pfs);
	if (fd < 0)
	{
		if (errno == EACCES || (isdir && errno == EISDIR))
			return 0;
		fprintf(stderr, _("%s: could not open file \"%s\": %s\n"),
				progname, fname, strerror(errno));
		return -1;
	}

	returncode = polar_fsync(fd, is_pfs);

	/*
	 * Some OSes don't allow us to fsync directories at all, so we can ignore
	 * those errors. Anything else needs to be reported.
	 */
	if (returncode != 0 && !(isdir && errno == EBADF))
	{
		fprintf(stderr, _("%s: could not fsync file \"%s\": %s\n"),
				progname, fname, strerror(errno));
		(void) polar_close(fd, is_pfs);
		return -1;
	}

	(void) polar_close(fd, is_pfs);
	return 0;
}

/*
 * fsync_parent_path -- fsync the parent path of a file or directory
 *
 * This is aimed at making file operations persistent on disk in case of
 * an OS crash or power failure.
 */
int
polar_fsync_parent_path(const char *fname, const char *progname, bool is_pfs)
{
	char		parentpath[MAXPGPATH];

	strlcpy(parentpath, fname, MAXPGPATH);
	get_parent_directory(parentpath);

	/*
	 * get_parent_directory() returns an empty string if the input argument is
	 * just a file name (see comments in path.c), so handle that as being the
	 * current directory.
	 */
	if (strlen(parentpath) == 0)
		strlcpy(parentpath, ".", MAXPGPATH);

	if (polar_fsync_fname(parentpath, true, progname, is_pfs) != 0)
		return -1;

	return 0;
}


/*
 * durable_rename -- rename(2) wrapper, issuing fsyncs required for durability
 *
 * Wrapper around rename, similar to the backend version.
 */
int
polar_durable_rename(const char *oldfile, const char *newfile, const char *progname, bool is_pfs)
{
	int			fd;

	/*
	 * First fsync the old and target path (if it exists), to ensure that they
	 * are properly persistent on disk. Syncing the target file is not
	 * strictly necessary, but it makes it easier to reason about crashes;
	 * because it's then guaranteed that either source or target file exists
	 * after a crash.
	 */
	if (polar_fsync_fname(oldfile, false, progname, is_pfs) != 0)
		return -1;

	fd = polar_open(newfile, PG_BINARY | O_RDWR, 0, is_pfs);
	if (fd < 0)
	{
		if (errno != ENOENT)
		{
			fprintf(stderr, _("%s: could not open file \"%s\": %s\n"),
					progname, newfile, strerror(errno));
			return -1;
		}
	}
	else
	{
		if (polar_fsync(fd, is_pfs) != 0)
		{
			fprintf(stderr, _("%s: could not fsync file \"%s\": %s\n"),
					progname, newfile, strerror(errno));
			polar_close(fd, is_pfs);
			return -1;
		}
		polar_close(fd, is_pfs);
	}

	/* Time to do the real deal... */
	if (polar_rename(oldfile, newfile, is_pfs) != 0)
	{
		fprintf(stderr, _("%s: could not rename file \"%s\" to \"%s\": %s\n"),
				progname, oldfile, newfile, strerror(errno));
		return -1;
	}

	/*
	 * To guarantee renaming the file is persistent, fsync the file with its
	 * new name, and its containing directory.
	 */
	if (polar_fsync_fname(newfile, false, progname, is_pfs) != 0)
		return -1;

	if (polar_fsync_parent_path(newfile, progname, is_pfs) != 0)
		return -1;

	return 0;
}


/*
 *	polar_rmtree
 *
 *	Delete a directory tree recursively.
 *	Assumes path points to a valid directory.
 *	Deletes everything under path.
 *	If rmtopdir is true deletes the directory too.
 *	Returns true if successful, false if there was any problem.
 *	(The details of the problem are reported already, so caller
 *	doesn't really have to say anything more, but most do.)
 *
 *  POLAR: for BACKEND support local or shared storage
 */
bool
polar_rmtree(const char *path, bool rmtopdir, bool is_pfs)
{
	bool		result = true;
	char		pathbuf[MAXPGPATH];
	char	  **filenames;
	char	  **filename;
	struct stat statbuf;

	/*
	 * we copy all the names out of the directory before we start modifying
	 * it.
	 */
	filenames = polarfnames(path, is_pfs);

	if (filenames == NULL)
		return false;

	/* now we have the names we can start removing things */
	for (filename = filenames; *filename; filename++)
	{
		snprintf(pathbuf, MAXPGPATH, "%s/%s", path, *filename);

		/*
		 * It's ok if the file is not there anymore; we were just about to
		 * delete it anyway.
		 *
		 * This is not an academic possibility. One scenario where this
		 * happens is when bgwriter has a pending unlink request for a file in
		 * a database that's being dropped. In dropdb(), we call
		 * ForgetDatabaseFsyncRequests() to flush out any such pending unlink
		 * requests, but because that's asynchronous, it's not guaranteed that
		 * the bgwriter receives the message in time.
		 */
		if (polar_lstat(pathbuf, &statbuf, is_pfs) != 0)
		{
			if (errno != ENOENT)
			{
				fprintf(stderr, _("could not stat file or directory \"%s\": %s\n"),
						pathbuf, strerror(errno));
				result = false;
			}
			continue;
		}

		if (S_ISDIR(statbuf.st_mode))
		{
			/* call ourselves recursively for a directory */
			if (!polar_rmtree(pathbuf, true, is_pfs))
			{
				/* we already reported the error */
				result = false;
			}
		}
		else
		{
			if (polar_unlink(pathbuf, is_pfs) != 0)
			{
				if (errno != ENOENT)
				{
					fprintf(stderr, _("could not remove file or directory \"%s\": %s\n"),
							pathbuf, strerror(errno));
					result = false;
				}
			}
		}
	}

	if (rmtopdir)
	{
		if (polar_rmdir(path, is_pfs) != 0)
		{
			fprintf(stderr, _("could not remove file or directory \"%s\": %s\n"),
					path, strerror(errno));
			result = false;
		}
	}

	polarfnames_cleanup(filenames);

	return result;
}


/*
 * polarfnames
 *
 * return a list of the names of objects in the argument directory.  Caller
 * must call pgfnames_cleanup later to free the memory allocated by this
 * function.
 */
char	  **
polarfnames(const char *path, bool is_pfs)
{
	DIR		   *dir;
	struct dirent *file;
	char	  **filenames;
	int			numnames = 0;
	int			fnsize = 200;	/* enough for many small dbs */

	dir = polar_opendir(path, is_pfs);
	if (dir == NULL)
	{
		fprintf(stderr, _("could not open directory \"%s\": %s\n"),
				path, strerror(errno));
		return NULL;
	}

	filenames = (char **) palloc(fnsize * sizeof(char *));

	while (errno = 0, (file = polar_readdir(dir, is_pfs)) != NULL)
	{
		if (strcmp(file->d_name, ".") != 0 && strcmp(file->d_name, "..") != 0)
		{
			if (numnames + 1 >= fnsize)
			{
				fnsize *= 2;
				filenames = (char **) repalloc(filenames,
											   fnsize * sizeof(char *));
			}
			filenames[numnames++] = pstrdup(file->d_name);
		}
	}

	if (errno)
	{
		fprintf(stderr, _("could not read directory \"%s\": %s\n"),
				path, strerror(errno));
	}

	filenames[numnames] = NULL;

	if (polar_closedir(dir, is_pfs))
	{
		fprintf(stderr, _("could not close directory \"%s\": %s\n"),
				path, strerror(errno));
	}

	return filenames;
}


/*
 *	pgfnames_cleanup
 *
 *	deallocate memory used for filenames
 */
void
polarfnames_cleanup(char **filenames)
{
	char	  **fn;

	for (fn = filenames; *fn; fn++)
		pfree(*fn);

	pfree(filenames);
}

static int polar_vfs_kind(bool is_pfs)
{
	return  is_pfs ? PFS_IO_INDEX: LOCAL_IO_INDEX;;
}
