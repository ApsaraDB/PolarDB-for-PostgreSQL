/*-------------------------------------------------------------------------
 *
 * polar_shmem.c
 *	  create polar persisted shared memory and initialize shared memory data structures.
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
 *
 * IDENTIFICATION
 *	  src/backend/storage/ipc/polar_shmem.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/transam.h"
#include "miscadmin.h"
#include "storage/polar_shmem.h"

extern bool polar_enable_persisted_buffer_pool;
extern bool polar_enable_shared_storage_mode;

#define polar_shmem_is_allocated(idx) (polar_shmem_allocated & (1 << idx))
#define polar_shmem_set_allocated(idx) (polar_shmem_allocated |= (1 << idx))
#define polar_shmem_addr(idx)	(polar_shmem_infos[idx].location)
#define polar_shmem_addr_is_valid(addr) \
	((addr >= polar_shmem_base) && (addr < polar_shmem_end))

/* shared memory global variables */
static PGShmemHeader	*polar_shmem_hdr;
static void		*polar_shmem_base;
static void		*polar_shmem_end;
static int32	polar_shmem_allocated;
static PolarShmemInfo	polar_shmem_infos[POLAR_PERSISTED_SHMEM_NUM];

static const char *polar_persisted_shmem_names[POLAR_PERSISTED_SHMEM_NUM] =
					  {"POLAR Buffer Pool Ctl", "Buffer Descriptors",
					   "Buffer Blocks"};
static int persisted_shmem_idx(const char *name);
static void *polar_shmem_alloc(PolarPersistedShmem idx, Size size);

/*
 *	polar_init_shmem_access --- set up basic pointers to shared memory.
 *
 * Note: the argument should be declared "PGShmemHeader *seghdr",
 * but we use void to avoid having to include ipc.h in polar_shmem.h.
 */
void
polar_init_shmem_access(void *seghdr)
{
	PGShmemHeader *shmhdr = (PGShmemHeader *) seghdr;

	polar_shmem_hdr = shmhdr;
	polar_shmem_base = (void *) shmhdr;
	polar_shmem_end = (char *) polar_shmem_base + shmhdr->totalsize;
	polar_shmem_allocated = 0;
	memset(polar_shmem_infos, 0, sizeof(PolarShmemInfo) * POLAR_PERSISTED_SHMEM_NUM);
}

/*
 * polar_shmem_alloc -- allocate max-aligned chunk from shared memory
 *
 * Throws error if request cannot be satisfied.
 */
static void *
polar_shmem_alloc(PolarPersistedShmem idx, Size size)
{
	Size		newStart;
	Size		newFree;
	void	   *newSpace;

	size = CACHELINEALIGN(size);

	Assert(polar_shmem_hdr != NULL);

	newStart = polar_shmem_hdr->freeoffset;
	newFree = newStart + size;
	if (newFree > polar_shmem_hdr->totalsize)
	{
		ereport(ERROR,
				(errcode(ERRCODE_OUT_OF_MEMORY),
					errmsg("out of shared memory (%zu bytes requested)", size)));
	}

	newSpace = (void *) ((char *) polar_shmem_base + newStart);
	polar_shmem_hdr->freeoffset = newFree;
	polar_shmem_set_allocated(idx);

	Assert(polar_shmem_addr_is_valid(newSpace));
	Assert(newSpace == (void *) CACHELINEALIGN(newSpace));

	return newSpace;
}

/*
 * polar_shmem_init_struct -- Create/attach to a structure in shared memory.
 *
 *		This is called during initialization to find or allocate
 *		a data structure in shared memory.  If no other process
 *		has created the structure, this routine allocates space
 *		for it.  If it exists already, a pointer to the existing
 *		structure is returned.
 *
 *	Returns: pointer to the object.  *foundPtr is set true if the object was
 *		already initialized.
 */
void *
polar_shmem_init_struct(const char *name, Size size, bool *foundPtr)
{
	void	*struct_ptr;
	PolarShmemInfo	*shmem_info;
	PolarPersistedShmem shmem;

	shmem = persisted_shmem_idx(name);
	Assert(shmem >= 0);
	if (polar_shmem_is_allocated(shmem))
	{
		*foundPtr = true;
		return polar_shmem_addr(shmem);
	}

	struct_ptr = polar_shmem_alloc(shmem, size);

	/* Store shmem info. */
	shmem_info = &polar_shmem_infos[shmem];
	shmem_info->name = polar_persisted_shmem_names[shmem];
	shmem_info->size = size;
	shmem_info->location = struct_ptr;

	*foundPtr = false;

	return struct_ptr;
}

/*
 * Check if persisted buffer pool is enable. If name is not null, we should
 * check if this shmem named *name* is allowed to use persisted shared memory.
 */
bool
polar_persisted_buffer_pool_enabled(const char *name)
{
	if (!polar_enable_persisted_buffer_pool || IsBootstrapProcessingMode() ||
		!polar_enable_shared_storage_mode)
		return false;

	if (name == NULL)
		return true;

	if (persisted_shmem_idx(name) >= 0)
		return true;

	return false;
}

static int
persisted_shmem_idx(const char *name)
{
	const char	*cur_dir = NULL;
	int			i;

	for (i = 0; i < POLAR_PERSISTED_SHMEM_NUM; i++)
	{
		cur_dir = polar_persisted_shmem_names[i];
		if (strcmp(name, cur_dir) == 0)
			break;
	}

	if (i == POLAR_PERSISTED_SHMEM_NUM)
		return -1;

	return i;
}

PolarShmemInfo *
polar_get_persisted_shmem_info(void)
{
	return polar_shmem_infos;
}

PGShmemHeader *
polar_get_persisted_shmem_seg_hdr(void)
{
	return polar_shmem_hdr;
}
