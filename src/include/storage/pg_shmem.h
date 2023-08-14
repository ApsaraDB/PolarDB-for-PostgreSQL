/*-------------------------------------------------------------------------
 *
 * pg_shmem.h
 *	  Platform-independent API for shared memory support.
 *
 * Every port is expected to support shared memory with approximately
 * SysV-ish semantics; in particular, a memory block is not anonymous
 * but has an ID, and we must be able to tell whether there are any
 * remaining processes attached to a block of a specified ID.
 *
 * To simplify life for the SysV implementation, the ID is assumed to
 * consist of two unsigned long values (these are key and ID in SysV
 * terms).  Other platforms may ignore the second value if they need
 * only one ID number.
 *
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/storage/pg_shmem.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_SHMEM_H
#define PG_SHMEM_H

#include "storage/dsm_impl.h"

/* POLAR */
#include "utils/hsearch.h"

#define POLAR_SHMEM_PERSISTED_INIT_SIZE 100000

typedef enum PolarShmemType
{
	POLAR_SHMEM_NORMAL = 0,
	POLAR_SHMEM_PERSISTED,
	/* The number of shmem type, it is not a valid type. */
	POLAR_SHMEM_NUM
} PolarShmemType;

#define polar_shmem_is_persisted(shmem_type) (shmem_type == POLAR_SHMEM_PERSISTED)
#define polar_shmem_is_normal(shmem_type) (shmem_type == POLAR_SHMEM_NORMAL)

typedef struct PGShmemHeader	/* standard header for all Postgres shmem */
{
	int32		magic;			/* magic # to identify Postgres segments */
#define PGShmemMagic  679834894
	pid_t		creatorPID;		/* PID of creating process (set but unread) */
	Size		totalsize;		/* total size of segment */
	Size		freeoffset;		/* offset to first free space */
	dsm_handle	dsm_control;	/* ID of dynamic shared memory control seg */
	void	   *index;			/* pointer to ShmemIndex table */
#ifndef WIN32					/* Windows doesn't have useful inode#s */
	dev_t		device;			/* device data directory is on */
	ino_t		inode;			/* inode number of data directory */
#endif
	PolarShmemType	type;
} PGShmemHeader;

/* GUC variable */
extern int	huge_pages;

/* Possible values for huge_pages */
typedef enum
{
	HUGE_PAGES_OFF,
	HUGE_PAGES_ON,
	HUGE_PAGES_TRY
}			HugePagesType;

#ifndef WIN32
extern unsigned long UsedShmemSegID;

/* POLAR */
extern unsigned long polar_used_shmem_seg_id;
#else
extern HANDLE UsedShmemSegID;
extern void *ShmemProtectiveRegion;
#endif
extern void *UsedShmemSegAddr;

/* POLAR */
extern void	*polar_used_shmem_seg_addr;

#ifdef EXEC_BACKEND
extern void PGSharedMemoryReAttach(void);
extern void PGSharedMemoryNoReAttach(void);
#endif

extern PGShmemHeader *PGSharedMemoryCreate(Size size, int port,
					 PGShmemHeader **shim, PolarShmemType polar_shmem_type);
extern bool PGSharedMemoryIsInUse(unsigned long id1, unsigned long id2);
extern void PGSharedMemoryDetach(void);

/* POLAR */
extern HTAB* polar_get_shmem_index(void);
extern PGShmemHeader* polar_get_shmem_seg_hdr(void);
extern bool polar_delete_shmem(int shmId);
#endif							/* PG_SHMEM_H */
