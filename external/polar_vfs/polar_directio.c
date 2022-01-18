/*-------------------------------------------------------------------------
 *
 * polar_directio.c
 *
 * Copyright (c) 2020, Alibaba Group Holding Limited
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
 *	  external/polar_vfs/polar_directio.c
 *
 *-------------------------------------------------------------------------
 */
#include <unistd.h>
#include <sys/time.h>
#include "postgres.h"
#include "port.h"
#include "funcapi.h"
#include "storage/backendid.h"
#include "storage/ipc.h"
#include "storage/pg_shmem.h"
#include "storage/shmem.h"
#include "storage/lwlock.h"
#include "utils/guc.h"

/* POLAR */
#include "storage/polar_fd.h"
#include "storage/polar_directio.h"
/* POLAR END */

int polar_max_direct_io_size = POLAR_DIRECTIO_DEFAULT_IOSIZE;
char *polar_directio_buffer = NULL;

/*
 * POLAR: open file with PG_O_DIRECT flag.
 * O_WRONLY is conflict to PG_O_DIRECT, so it needs to be
 * replaced to O_RDWR. Besides, directory can not be open
 * with PG_O_DIRECT.
 */
int
polar_directio_open(const char *path, int flags, mode_t mode)
{
	struct stat st;
	int ret = 0;

	Assert(path != NULL);
	/* POLAR: Check for path not exists or points to a regular file. */
	ret = stat(path, &st);
	if ((ret == 0 && S_ISREG(st.st_mode)) ||
			(ret != 0 && errno == ENOENT))
	{
		if ((flags & POLAR_ACCESS_MODE_MASK) == O_WRONLY)
			flags = (flags & (~POLAR_ACCESS_MODE_MASK)) | O_RDWR;
		flags |= PG_O_DIRECT;
		elog(DEBUG1, "polar_directio_open file with PG_O_DIRECT: %s", path);
	}
	else if (ret != 0)
		return ret;

	return open(path, flags, mode);
}

ssize_t
polar_directio_write(int fd, const void *buf, size_t len)
{
	ssize_t res = -1;
	off_t   offset = lseek(fd, (off_t)0, SEEK_CUR);

	if (offset < 0)
		return res;

	if (POLAR_DIECRTIO_IS_ALIGNED(buf) &&
			POLAR_DIECRTIO_IS_ALIGNED(len) &&
			POLAR_DIECRTIO_IS_ALIGNED(offset))
		return write(fd, buf, len);

	res = polar_directio_pwrite(fd, buf, len, offset);

	if (res > 0 &&
			lseek(fd, (off_t)(offset + res), SEEK_SET) < 0)
		res = -1;

	return res;
}

ssize_t
polar_directio_read(int fd, void *buf, size_t len)
{
	ssize_t res = -1;
	off_t   offset = lseek(fd, (off_t)0, SEEK_CUR);

	if (offset < 0)
		return res;

	if (POLAR_DIECRTIO_IS_ALIGNED(buf) &&
			POLAR_DIECRTIO_IS_ALIGNED(len) &&
			POLAR_DIECRTIO_IS_ALIGNED(offset))
		return read(fd, buf, len);

	res = polar_directio_pread(fd, buf, len, offset);

	if (res > 0 &&
			lseek(fd, (off_t)(offset + res), SEEK_SET) < 0)
		res = -1;

	return res;
}

/*
 * POLAR: It do pread work with PG_O_DIRECT flag.
 * First, malloc one new buffer which be aligned address.
 * Second, pread content of file for aligned length from aligned offset.
 * Third, copy content of file from new buffer to old buffer.
 *
 * We split the [offset, offset + len] into three sections: the first section,
 * the middle sections and the last section. The boundary of content to be read
 * will be processing during in the first and last section.
 */
ssize_t
polar_directio_pread(int fd, void *buffer, size_t len, off_t offset)
{
	char    *buf = polar_directio_buffer;
	ssize_t res = -1;
	char    *from;
	off_t   head_start;
	off_t   head_end;
	off_t   tail_start;
	off_t   tail_end;
	ssize_t count;
	off_t   off;
	off_t   nleft;
	ssize_t cplen;

	if (POLAR_DIECRTIO_IS_ALIGNED(buffer) &&
			POLAR_DIECRTIO_IS_ALIGNED(len) &&
			POLAR_DIECRTIO_IS_ALIGNED(offset))
		return pread(fd, buffer, len, offset);

	from = (char *) buffer;
	head_start = POLAR_DIRECTIO_ALIGN_DOWN(offset);
	head_end = POLAR_DIRECTIO_ALIGN(offset);
	tail_start = POLAR_DIRECTIO_ALIGN_DOWN(offset + len);
	tail_end = POLAR_DIRECTIO_ALIGN(offset + len);
	count = 0;
	nleft = len;

	if (buf == NULL)
		return res;

	/* read from the first section */
	if (head_start < head_end &&
			nleft > 0)
	{
		off = head_start;
		res = pread(fd, buf, POLAR_DIRECTIO_ALIGN_LEN, off);

		if (res < 0)
			return res;
		else if (res <= (offset & (POLAR_DIRECTIO_ALIGN_LEN - 1)))
			return count;
		else
		{
			cplen = Min(res - (offset & (POLAR_DIRECTIO_ALIGN_LEN - 1)), len);
			cplen = Min(nleft, cplen);
		}

		memcpy(from, buf + (offset & (POLAR_DIRECTIO_ALIGN_LEN - 1)), cplen);
		from += cplen;
		count += cplen;
		nleft -= cplen;
	}

	/* read from the middle sections */
	if (head_end < tail_start &&
			nleft > 0)
	{
		off = head_end;

		while (off < tail_start)
		{
			cplen = Min(tail_start - off, polar_max_direct_io_size);
			res = pread(fd, buf, cplen, off);

			if (res < 0)
				return res;

			res = Min(nleft, res);
			memcpy(from, buf, res);
			from += res;
			count += res;
			nleft -= res;

			if (res < cplen)
				break;

			off += res;
		}
	}

	/* read from the last section */
	if (head_end <= tail_start &&
			tail_start < tail_end &&
			nleft > 0)
	{
		off = tail_start;
		res = pread(fd, buf, POLAR_DIRECTIO_ALIGN_LEN, off);

		if (res < 0)
			return res;
		else
		{
			cplen = Min(res, ((offset + len) & (POLAR_DIRECTIO_ALIGN_LEN - 1)));
			cplen = Min(nleft, cplen);
		}

		memcpy(from, buf, cplen);
		from += cplen;
		count += cplen;
		nleft -= cplen;
	}

	return count;
}

/*
 * POLAR: It do pwrite work with PG_O_DIRECT flag.
 * First, malloc one new buffer which be aligned address.
 * Second, pread missing content for aligned length from aligned offset.
 * Third, copy content of old buffer into new buffer and pwrite new
 * buffer into file.
 *
 * We split the [offset, offset + len] into three sections: the first section,
 * the middle sections and the last section. The boundary of content to be write
 * will be processing during in the first and last section.
 *
 * The pwrite with PG_O_DIRECT will extend file size to mutiples of 4096 which
 * means the final file's size could be bigger than expected! But we don't
 * want that! Because some functions will use file's size in other way, such as
 * twophase transaction's function ReadTwoPhaseFile. So, we truncate file to
 * expected size in this case.
 */
ssize_t
polar_directio_pwrite(int fd, const void *buffer, size_t len, off_t offset)
{
#define POLAR_DIRECTIO_PWRITE_SECTION(start, len)               \
	do                                                          \
	{                                                           \
		MemSet(buf, 0x0, POLAR_DIRECTIO_ALIGN_LEN);             \
		res = pread(fd, buf, POLAR_DIRECTIO_ALIGN_LEN, off);    \
		if (res < 0)                                            \
			return res;                                         \
		memcpy(buf + start, from, len);                         \
		res = pwrite(fd, buf, POLAR_DIRECTIO_ALIGN_LEN, off);   \
		if (res < 0)                                            \
			return res;                                         \
		Assert(res == POLAR_DIRECTIO_ALIGN_LEN);                \
		from += len;                                            \
		count += len;                                           \
		nleft -= len;                                           \
	} while(0)

	char    *buf = polar_directio_buffer;
	ssize_t res = -1;
	char    *from;
	off_t   head_start;
	off_t   head_end;
	off_t   tail_start;
	off_t   tail_end;
	ssize_t count;
	off_t   off;
	off_t   nleft;
	ssize_t cplen;
	bool	need_truncate = false;
	struct	stat stat_buf;

	if (POLAR_DIECRTIO_IS_ALIGNED(buffer) &&
			POLAR_DIECRTIO_IS_ALIGNED(len) &&
			POLAR_DIECRTIO_IS_ALIGNED(offset))
		return pwrite(fd, buffer, len, offset);

	from = (char *) buffer;
	head_start = POLAR_DIRECTIO_ALIGN_DOWN(offset);
	head_end = POLAR_DIRECTIO_ALIGN(offset);
	tail_start = POLAR_DIRECTIO_ALIGN_DOWN(offset + len);
	tail_end = POLAR_DIRECTIO_ALIGN(offset + len);
	count = 0;
	nleft = len;

	if (buf == NULL)
		return res;

	/*
	 * POLAR: Whether we should truncate file to expected size or not.
	 * stat_buf constains the original file's states including size.
	 */
	if (!POLAR_DIECRTIO_IS_ALIGNED(offset + len))
	{
		res = fstat(fd, &stat_buf);
		if (res < 0)
			return res;
		if (stat_buf.st_size < tail_end)
			need_truncate = true;
	}

	/* write the first section */
	if (head_start < head_end &&
			nleft > 0)
	{
		off = head_start;
		cplen = Min(nleft, POLAR_DIRECTIO_ALIGN_LEN - (offset & (POLAR_DIRECTIO_ALIGN_LEN - 1)));
		POLAR_DIRECTIO_PWRITE_SECTION((offset & (POLAR_DIRECTIO_ALIGN_LEN - 1)), cplen);
	}

	/* write the middle sections */
	if (head_end < tail_start &&
			nleft > 0)
	{
		off = head_end;

		while (off < tail_start)
		{
			cplen = Min(tail_start - off, polar_max_direct_io_size);
			memcpy(buf, from, cplen);
			res = pwrite(fd, buf, cplen, off);

			if (res < 0)
				return res;

			Assert(res == cplen);
			from += res;
			count += res;
			nleft -= res;
			off += res;
		}
	}

	/* write the last section */
	if (head_end <= tail_start &&
			tail_start < tail_end &&
			nleft > 0)
	{
		off = tail_start;
		cplen = Min(nleft, (offset + len) & (POLAR_DIRECTIO_ALIGN_LEN - 1));
		POLAR_DIRECTIO_PWRITE_SECTION(0, cplen);
	}

	Assert(nleft == 0);

	/* If we should truncate file to true size */
	if (need_truncate)
	{
		res = ftruncate(fd, Max(offset + len, stat_buf.st_size));
		if (res < 0)
			return res;
	}
	return count;
}
