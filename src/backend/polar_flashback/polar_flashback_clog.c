/*-------------------------------------------------------------------------
 *
 * polar_flashback_clog.c
 *
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2021, Alibaba Group Holding limited
 *
 * Move these clog files to flashback log dir instead of truncate.
 * These clog files will be segmented into some sub directory named by
 * create time.
 *
 * IDENTIFICATION
 *    src/backend/polar_flashback/polar_flashback_clog.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/clog.h"
#include "miscadmin.h"
#include "polar_flashback/polar_fast_recovery_area.h"
#include "polar_flashback/polar_flashback_clog.h"
#include "polar_flashback/polar_flashback_log_internal.h"
#include "storage/fd.h"
#include "storage/polar_fd.h"
#include "storage/shmem.h"

#define GET_CLOG_SUBDIR_FULL_PATH(dir_path, subdir_no, path) \
	snprintf(path, MAXPGPATH, "%s/%08X", dir_path, subdir_no)

static void
get_clog_file_full_path(const char *clog_dir_path, uint32 clog_subdir_no, const char *fname, char *path)
{
	char clog_subdir_path[MAXPGPATH];

	GET_CLOG_SUBDIR_FULL_PATH(clog_dir_path, clog_subdir_no, clog_subdir_path);
	polar_make_file_path_level3(path, clog_subdir_path, fname);
}

/* POLAR: Validate the flashback clog subdir */
static void
validate_flashback_clog_subdir(const char *clog_dir_path, uint32 clog_subdir_no)
{
	char path[MAXPGPATH];

	GET_CLOG_SUBDIR_FULL_PATH(clog_dir_path, clog_subdir_no, path);
	polar_validate_dir(path);
}

static bool
clog_file_in_subdir(const char *clog_subdir_path, const char *fname)
{
	struct stat st;
	char path[MAXPGPATH];

	snprintf(path, MAXPGPATH, "%s/%s", clog_subdir_path, fname);

	if ((polar_stat(path, &st) == 0) && S_ISREG(st.st_mode))
		return true;
	else
		return false;
}

static bool
is_clog_subdir_right(flashback_clog_ctl_t clog_ctl, const char *fname, uint32 clog_subdir_no,
					 const char *clog_dir_path)
{
	int seg_no;
	char clog_subdir_path[MAXPGPATH];

	seg_no = (int) strtol(fname, NULL, 16);
	GET_CLOG_SUBDIR_FULL_PATH(clog_dir_path, clog_subdir_no, clog_subdir_path);

	/* The segment file is in subdir already, so we need to create a new dir */
	if (clog_file_in_subdir(clog_subdir_path, fname))
		return false;
	else if (clog_subdir_no == FLASHBACK_CLOG_MIN_SUBDIR && seg_no < clog_ctl->min_clog_seg_no)
		return false;

	return true;
}

static uint32
get_right_clog_subdir_by_fname(flashback_clog_ctl_t clog_ctl, const char *clog_dir_path,
							   const char *fname, bool *need_update_ctl)
{
	bool need_new_subdir = false;
	uint32 next_clog_subdir_no;
	uint32 clog_subdir_no = FLASHBACK_CLOG_MIN_SUBDIR;

	next_clog_subdir_no = pg_atomic_read_u32(&clog_ctl->next_clog_subdir_no);

	/* There are no clog subdir. */
	if (next_clog_subdir_no == FLASHBACK_CLOG_MIN_NEXT_SUBDIR)
		need_new_subdir = !is_clog_subdir_right(clog_ctl, fname, clog_subdir_no, clog_dir_path);
	else if (FLASHBACK_CLOG_IS_EMPTY(next_clog_subdir_no))
		need_new_subdir = true;
	else
	{
		Assert(!FLASHBACK_CLOG_IS_EMPTY(next_clog_subdir_no));
		clog_subdir_no = next_clog_subdir_no - 2;

		/* First check prev sub directory, next to check lastest sub directory */
		if (!is_clog_subdir_right(clog_ctl, fname, clog_subdir_no, clog_dir_path))
		{
			clog_subdir_no++;
			need_new_subdir = !is_clog_subdir_right(clog_ctl, fname, clog_subdir_no, clog_dir_path);
		}
	}

	if (need_new_subdir)
	{
		if (!FLASHBACK_CLOG_IS_EMPTY(next_clog_subdir_no))
			clog_subdir_no++;

		validate_flashback_clog_subdir(clog_dir_path, clog_subdir_no);

		if (pg_atomic_compare_exchange_u32(&clog_ctl->next_clog_subdir_no, &next_clog_subdir_no,
										   next_clog_subdir_no + 1))
			*need_update_ctl = true;
	}

	return clog_subdir_no;
}

void
polar_flashback_clog_shmem_init_data(flashback_clog_ctl_t ctl)
{
	MemSet(ctl, 0, sizeof(flashback_clog_ctl_data_t));
	pg_atomic_init_u32(&ctl->next_clog_subdir_no, FLASHBACK_CLOG_MIN_SUBDIR);
	ctl->min_clog_seg_no = FLASHBACK_CLOG_INVALID_SEG;
}

flashback_clog_ctl_t
polar_flashback_clog_shmem_init(const char *name)
{
	flashback_clog_ctl_t clog_ctl;
	bool found;
	char ctl_name[FL_OBJ_MAX_NAME_LEN];

	FLOG_GET_OBJ_NAME(ctl_name, name, FLASHBACK_CLOG_CTL_NAME_SUFFIX);

	clog_ctl = (flashback_clog_ctl_t) ShmemInitStruct(ctl_name, FLASHBACK_CLOG_SHMEM_SIZE, &found);

	if (!IsUnderPostmaster)
	{
		Assert(!found);
		polar_flashback_clog_shmem_init_data(clog_ctl);
	}
	else
		Assert(found);

	return clog_ctl;
}

void
polar_startup_flashback_clog(flashback_clog_ctl_t clog_ctl, int min_clog_seg_no,
							 uint32 next_clog_subdir_no)
{
	/* Update the minimal clog segment no after polar_enable_fast_recovery_area is on. */
	if (min_clog_seg_no == FLASHBACK_CLOG_INVALID_SEG)
		clog_ctl->min_clog_seg_no = polar_get_clog_min_seg_no();
	else
		clog_ctl->min_clog_seg_no = min_clog_seg_no;

	pg_atomic_write_u32(&clog_ctl->next_clog_subdir_no, next_clog_subdir_no);
}

void
polar_mv_clog_seg_to_fra(flashback_clog_ctl_t clog_ctl, const char *fra_dir,
						 const char *fname, const char *old_path, bool *need_update_ctl)
{
	char new_path[MAXPGPATH];
	char clog_dir_path[MAXPGPATH];
	uint32 clog_subdir_no;

	/* Find right clog sub directory */
	FRA_GET_SUBDIR_PATH(fra_dir, FLASHBACK_CLOG_DIR, clog_dir_path);
	clog_subdir_no = get_right_clog_subdir_by_fname(clog_ctl, clog_dir_path, fname, need_update_ctl);
	/* Move the clog file to fast recovery area */
	get_clog_file_full_path(clog_dir_path, clog_subdir_no, fname, new_path);
	polar_durable_rename(old_path, new_path, ERROR);

	elog(LOG, "The clog file %s is renamed to %s", old_path, new_path);
}

void
polar_truncate_flashback_clog_subdir(const char *fra_dir, uint32 next_clog_subdir_no)
{
	DIR        *cldir;
	struct dirent *clde;
	char clog_dir_path[MAXPGPATH];

	/* Empty or just one sub directory, return */
	if (next_clog_subdir_no < FLASHBACK_CLOG_MIN_NEXT_SUBDIR)
		return;

	next_clog_subdir_no--;
	FRA_GET_SUBDIR_PATH(fra_dir, FLASHBACK_CLOG_DIR, clog_dir_path);
	cldir = polar_allocate_dir(clog_dir_path);

	while ((clde = ReadDir(cldir, clog_dir_path)) != NULL)
	{
		size_t      len;
		uint32      subdir_no;

		len = strlen(clde->d_name);

		if ((len == FLASHBACK_CLOG_SUBDIR_NAME_LEN) &&
				strspn(clde->d_name, "0123456789ABCDEF") == len)
		{
			subdir_no = (uint32) strtoul(clde->d_name, NULL, 16);

			if (subdir_no < next_clog_subdir_no)
			{
				char clog_subdir_path[MAXPGPATH];

				GET_CLOG_SUBDIR_FULL_PATH(clog_dir_path, subdir_no, clog_subdir_path);
				rmtree(clog_subdir_path, true);
			}
		}
	}

	FreeDir(cldir);
}

/*
 * POLAR: Get the status of the xid.
 *
 * If it is in fast recovery area, find the right clog sub directory and read the page from disk.
 * Otherwise, get the status form slru shared buffer.
 */
XidStatus
polar_flashback_get_xid_status(TransactionId xid, TransactionId max_xid, uint32 next_clog_subdir_no,
							   const char *fra_dir)
{
	bool must_found = false;
	XLogRecPtr lsn;

	if (!FLASHBACK_CLOG_IS_EMPTY(next_clog_subdir_no))
	{
		char clog_dir_path[MAXPGPATH];
		char clog_subdir_path[MAXPGPATH];

		/* Get the real clog subdir no */
		next_clog_subdir_no--;

		/* The transaction in the last clog sub directory */
		if (xid >= max_xid)
		{
			if (next_clog_subdir_no == FLASHBACK_CLOG_MIN_SUBDIR)
				elog(ERROR, "There is no clog subdir but xid %u is larger than or equal to max_xid %u", xid, max_xid);

			/* Must in the last clog sub directory */
			next_clog_subdir_no--;
			must_found = true;
		}

		/* Find clog sub directory */
		FRA_GET_SUBDIR_PATH(fra_dir, FLASHBACK_CLOG_DIR, clog_dir_path);
		GET_CLOG_SUBDIR_FULL_PATH(clog_dir_path, next_clog_subdir_no, clog_subdir_path);

		if (polar_xid_in_clog_dir(xid, clog_subdir_path))
			return polar_get_xid_status(xid, clog_subdir_path);
		/*no cover begin*/
		else if (unlikely(must_found))
			elog(ERROR, "We can't find the xid %u in %s", xid, clog_subdir_path);
		/*no cover end*/
	}

	/* It is not in fast recovery area */
	return TransactionIdGetStatus(xid, &lsn);
}
