/*-------------------------------------------------------------------------
 *
 * polar_pfsd.c
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
 *	  external/polar_vfs/polar_pfsd.c
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
#ifdef USE_PFSD
#include "pfsd_sdk.h"
#endif

/* POLAR */
#include "storage/polar_pfsd.h"
/* POLAR END */

int max_pfsd_io_size = PFSD_DEFAULT_MAX_IOSIZE;

ssize_t
polar_pfsd_write(int fd, const void *buf, size_t len)
{
	ssize_t     res = -1;
	char        *from;
	ssize_t     nleft;
	ssize_t     writesize;
	ssize_t     count = 0;

	nleft = len;
	from = (char *)buf;

	while (nleft > 0)
	{
		writesize = Min(nleft, max_pfsd_io_size);
		res = pfsd_write(fd, from, writesize);

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

ssize_t
polar_pfsd_read(int fd, void *buf, size_t len)
{
	ssize_t     res = -1;
	ssize_t     iolen = 0;
	ssize_t     nleft = len;
	char        *from = (char *)buf;
	ssize_t     count = 0;

	while (nleft > 0)
	{
		iolen = Min(nleft, max_pfsd_io_size);
		res = pfsd_read(fd, from, iolen);

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

ssize_t
polar_pfsd_pread(int fd, void *buf, size_t len, off_t offset)
{
	ssize_t     res = -1;
	ssize_t     iolen = 0;
	off_t       off = offset;
	ssize_t     nleft = len;
	char        *from = (char *)buf;
	ssize_t     count = 0;

	while (nleft > 0)
	{
		iolen = Min(nleft, max_pfsd_io_size);
		res = pfsd_pread(fd, from, iolen, off);

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

ssize_t
polar_pfsd_pwrite(int fd, const void *buf, size_t len, off_t offset)
{
	char        *from;
	ssize_t     nleft;
	off_t       startoffset;
	ssize_t     writesize;
	ssize_t     res = -1;
	ssize_t     count = 0;

	nleft = len;
	from = (char *)buf;
	startoffset = offset;

	while (nleft > 0)
	{
		writesize = Min(nleft, max_pfsd_io_size);
		res = pfsd_pwrite(fd, from, writesize, startoffset);

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
