/*-------------------------------------------------------------------------
 *
 * test_flashback_table.c
 *
 *
 * Copyright (c) 2020-2120, Alibaba-inc PolarDB Group
 *
 * IDENTIFICATION
 *    src/test/modules/test_flashback_table/test_flashback_table.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "miscadmin.h"
#include "polar_flashback/polar_fast_recovery_area.h"
#include "polar_flashback/polar_flashback_snapshot.h"
#include "polar_flashback/polar_flashback_snapshot.h"
#include "utils/guc.h"

PG_MODULE_MAGIC;

#define TEST_FRA_NAME "testfra"
#define TEST_CLOG_SEGNO (4095)
#define XID_PER_CLOG_SEG (8*1024*32*4)
#define get_clog_segno_fname(segno, fname) (snprintf(fname, 5, "%04X", segno))
#define MIN_RECORD_TIME (2)
#define LUCKY_RECORD_NO (17) /* Must be (2, FBPOINT_REC_PER_SEG * 3 + 2] */

/* Saved hook values in case of unload */
static shmem_startup_hook_type prev_shmem_startup_hook = NULL;

static fra_ctl_t test_fra = NULL;
static fpoint_ctl_t test_point_ctl = NULL;
static flashback_clog_ctl_t test_clog_ctl = NULL;

/*---- Function declarations ----*/
void        _PG_init(void);
void        _PG_fini(void);

static bool
check_dir_validate(char *path)
{
	struct stat st;

	if ((polar_stat(path, &st) == 0) && S_ISDIR(st.st_mode))
		return true;
	else
		return false;
}

static bool
check_fra_subdir_validate(const char *sub_path)
{
	char path[MAXPGPATH];

	FRA_GET_SUBDIR_PATH(test_fra->dir, sub_path, path);
	return check_dir_validate(path);
}

static void
check_fra_dir_validate(bool is_rm)
{
	bool clog_exist = false;
	bool fbpoint_exist = false;

	if (is_rm)
	{
		Assert(!check_dir_validate(test_fra->dir));
		return;
	}

	Assert(check_dir_validate(test_fra->dir));
	clog_exist = check_fra_subdir_validate(FLASHBACK_CLOG_DIR);
	fbpoint_exist = check_fra_subdir_validate(FBPOINT_DIR);
	Assert(clog_exist && fbpoint_exist);
}

static void
init_fake_clog(char *path, XidStatus xid_status)
{
	int fd;
	static char data[SLRU_PAGES_PER_SEGMENT * BLCKSZ];
	int rc = 0;

	fd = BasicOpenFile(path, O_RDWR | O_CREAT | O_EXCL | PG_BINARY, true);
	Assert(fd >= 0);

	MemSet(data, 0, sizeof(data));
	data[0] = xid_status;
	rc = (int)polar_pwrite(fd, data, SLRU_PAGES_PER_SEGMENT * BLCKSZ, 0);

	Assert(rc == SLRU_PAGES_PER_SEGMENT * BLCKSZ);
	polar_fsync(fd);
	polar_close(fd);
}

static void
get_clog_path(int segno, char *path)
{
	if (polar_enable_shared_storage_mode)
		snprintf(path, MAXPGPATH, "%s/%s/%04X", polar_datadir, "pg_xact", segno);
	else
		snprintf(path, MAXPGPATH, "%s/%04X", "pg_xact", segno);
}

static bool
is_file_exists(char *path)
{
	struct stat st;

	if ((polar_stat(path, &st) == 0) && S_ISREG(st.st_mode))
		return true;
	else
		return false;
}

static void
get_fra_clog_path(char *clog_dir_path, uint32 clog_subdir_no, char *fname, char *path)
{
	snprintf(path, MAXPGPATH, "%s/%08X/%s", clog_dir_path, clog_subdir_no, fname);
}

static bool
is_file_in_fra(char *fname_relative_path)
{
	char file_path[MAXPGPATH];

	FRA_GET_SUBDIR_PATH(test_fra->dir, fname_relative_path, file_path);
	return is_file_exists(file_path);
}

static void
check_clog_in_fra(char *fname)
{
	char relative_path[MAXPGPATH];
	uint32 next_clog_subdir_no;
	bool is_exist = false;

	next_clog_subdir_no = pg_atomic_read_u32(&test_clog_ctl->next_clog_subdir_no);
	Assert(next_clog_subdir_no);
	next_clog_subdir_no--;
	get_fra_clog_path(FLASHBACK_CLOG_DIR, next_clog_subdir_no, fname, relative_path);
	is_exist = is_file_in_fra(relative_path);
	Assert(is_exist);
}

static void
test_fra_init(void)
{
	test_fra = fra_shmem_init_internal(TEST_FRA_NAME);
	test_point_ctl = test_fra->point_ctl;
	test_clog_ctl = test_fra->clog_ctl;
	Assert(strncmp(test_fra->dir, TEST_FRA_NAME, FL_INS_MAX_NAME_LEN) == 0);
}

static void
test_fra_startup(int min_clog_seg_no, uint32 next_clog_subdir_no,
				 uint64 next_fbpoint_rec_no, XLogRecPtr min_keep_lsn, fbpoint_pos_t snapshot_end_pos)
{
	polar_startup_fra(test_fra);

	Assert(test_fra->next_fbpoint_rec_no == next_fbpoint_rec_no);
	Assert(test_fra->min_keep_lsn == min_keep_lsn);
	Assert(FBPOINT_POS_EQUAL(test_fra->snapshot_end_pos, snapshot_end_pos));
	Assert(test_point_ctl->next_fbpoint_rec_no == next_fbpoint_rec_no);
	Assert(test_clog_ctl->min_clog_seg_no == min_clog_seg_no);
	Assert(pg_atomic_read_u32(&test_clog_ctl->next_clog_subdir_no) == next_clog_subdir_no);
	check_fra_dir_validate(false);
}

static void
test_fra_shmem_startup(void)
{
	if (prev_shmem_startup_hook)
		prev_shmem_startup_hook();

	/*
	 * Create or attach to the shared memory state, including hash table
	 */
	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);
	test_fra_init();
	LWLockRelease(AddinShmemInitLock);
}

static void
test_clog_to_fra(int segno, uint32 clog_subdir_expected, XidStatus xid_status)
{
	SlruCtlData clog_ctl;
	char fname[5];
	char path[MAXPGPATH];

	StrNCpy(clog_ctl.Dir, "pg_xact", 64);
	Assert(polar_slru_seg_need_mv(test_fra, &clog_ctl));

	/* Create a fake clog */
	get_clog_path(segno, path);
	init_fake_clog(path, xid_status);

	/* Move clog to FRA */
	get_clog_segno_fname(segno, fname);
	polar_mv_slru_seg_to_fra(test_fra, fname, path);

	/* Check */
	check_clog_in_fra(fname);
	Assert(pg_atomic_read_u32(&test_clog_ctl->next_clog_subdir_no) == clog_subdir_expected + 1);
}

static void
get_fbpoint_rec_expected(fbpoint_rec_data_t *rec_data, fbpoint_wal_info_data_t *wal_info,
						 polar_flog_rec_ptr flog_ptr)
{
	uint32  next_clog_subdir_no;

	next_clog_subdir_no = pg_atomic_read_u32(&test_clog_ctl->next_clog_subdir_no);
	rec_data->flog_ptr = flog_ptr;
	rec_data->redo_lsn = wal_info->fbpoint_lsn;
	rec_data->time = wal_info->fbpoint_time;
	rec_data->next_clog_subdir_no = next_clog_subdir_no;
}

static void
check_fra_ctl_file(uint64 next_fbpoint_rec_no, XLogRecPtr min_keep_lsn,
				   int min_clog_seg_no, uint32 next_clog_subdir_no)
{
	char ctl_file_path[MAXPGPATH];
	int fd;
	int read_len;
	pg_crc32c   crc;
	int rc;
	fra_ctl_file_data_t *ctl_file_data;

	ctl_file_data = palloc0(sizeof(fra_ctl_file_data_t));
	polar_make_file_path_level3(ctl_file_path, test_fra->dir, FRA_CTL_FILE_NAME);
	fd = BasicOpenFile(ctl_file_path, O_RDWR | PG_BINARY, true);
	Assert(fd >= 0);
	read_len = polar_read(fd, ctl_file_data, sizeof(fra_ctl_file_data_t));
	Assert(read_len == sizeof(fra_ctl_file_data_t));
	rc = polar_close(fd);
	Assert(!rc);

	/* Verify CRC */
	INIT_CRC32C(crc);
	COMP_CRC32C(crc, (char *) ctl_file_data, offsetof(fra_ctl_file_data_t, crc));
	FIN_CRC32C(crc);
	Assert(EQ_CRC32C(crc, ctl_file_data->crc));

	Assert(ctl_file_data->version_no == FRA_CTL_FILE_VERSION);
	Assert(ctl_file_data->next_fbpoint_rec_no == next_fbpoint_rec_no);
	Assert(ctl_file_data->min_keep_lsn == min_keep_lsn);
	Assert(ctl_file_data->min_clog_seg_no == min_clog_seg_no);
	Assert(ctl_file_data->next_clog_subdir_no == next_clog_subdir_no);

	pfree(ctl_file_data);
}

static void
get_test_fbpoint_file_path(uint32 seg_no, char *path)
{
	char relative_path[MAXPGPATH];

	snprintf(relative_path, MAXPGPATH, "%s/%s/%08X", test_fra->dir, FBPOINT_DIR, seg_no);

	polar_make_file_path_level2(path, relative_path);
}

static void
check_fbpoint_page_crc(fbpoint_page_header_t *header)
{
	pg_crc32c   crc;

	INIT_CRC32C(crc);
	COMP_CRC32C(crc, (char *) header + FBPOINT_PAGE_HEADER_SIZE, FBPOINT_PAGE_SIZE - FBPOINT_PAGE_HEADER_SIZE);
	COMP_CRC32C(crc, (char *) header, offsetof(fbpoint_page_header_t, crc));
	FIN_CRC32C(crc);

	Assert(EQ_CRC32C(crc, header->crc));
}

static inline bool
check_fbpoint_record_data(fbpoint_rec_data_t *r1, polar_flog_rec_ptr flog_ptr,
		XLogRecPtr redo_lsn, pg_time_t time, uint32 next_clog_subdir_no)
{
	return r1->flog_ptr == flog_ptr && r1->redo_lsn == redo_lsn && r1->time == time &&
			r1->next_clog_subdir_no == next_clog_subdir_no;
}

static inline bool
fbpoint_record_expected(fbpoint_rec_data_t *r1, fbpoint_rec_data_t *r2)
{
	return memcmp(r1, r2, FBPOINT_RECORD_DATA_SIZE) == 0;
}

static void
check_fbpoint_rec_in_disk(fbpoint_rec_data_t *rec_data)
{
	char    path[MAXPGPATH];
	uint32  seg_no;
	uint64  next_fbpoint_rec_no = test_point_ctl->next_fbpoint_rec_no;
	char    buf[FBPOINT_REC_END_POS];
	int fd;
	int read_len;
	int rc;
	uint32 page_no = 0;
	fbpoint_page_header_t *header;
	uint32 max_page_no;
	fbpoint_rec_data_t *rec;

	seg_no = FBPOINT_GET_SEG_NO_BY_REC_NO(next_fbpoint_rec_no - 1);
	max_page_no = FBPOINT_GET_PAGE_NO_BY_REC_NO(next_fbpoint_rec_no - 1);

	get_test_fbpoint_file_path(seg_no, path);
	fd = BasicOpenFile(path, O_RDWR | PG_BINARY, true);

	read_len = polar_pread(fd, buf, FBPOINT_REC_END_POS, 0);
	Assert(read_len == FBPOINT_REC_END_POS);
	rc = polar_close(fd);
	Assert(!rc);

	header = (fbpoint_page_header_t *) buf;

	/* Check the CRC */
	for (; page_no < max_page_no; page_no++)
	{
		check_fbpoint_page_crc(header);
		header = (fbpoint_page_header_t *)(buf + FBPOINT_PAGE_SIZE);
	}

	rec = (fbpoint_rec_data_t *) FBPOINT_GET_OFFSET_BY_REC_NO(buf, next_fbpoint_rec_no - 1);
	Assert(fbpoint_record_expected(rec, rec_data));
}

static void
check_fbpoint_rec_in_mem(uint64 rec_no, fbpoint_rec_data_t *rec_data)
{
	uint64 page_no;
	char    *start_ptr;
	bool    is_expected;

	Assert(test_point_ctl->next_fbpoint_rec_no == rec_no + 1);
	page_no = FBPOINT_GET_PAGE_NO_BY_REC_NO(rec_no);

	start_ptr = test_point_ctl->fbpoint_rec_buf + page_no * FBPOINT_PAGE_SIZE;
	Assert(((fbpoint_page_header_t *) start_ptr)->version == FBPOINT_REC_VERSION);
	check_fbpoint_page_crc((fbpoint_page_header_t *) start_ptr);

	start_ptr = FBPOINT_GET_OFFSET_BY_REC_NO(test_point_ctl->fbpoint_rec_buf, rec_no);
	is_expected = fbpoint_record_expected((fbpoint_rec_data_t *)start_ptr, rec_data);
	Assert(is_expected);
}

static int
test_xid_comparator(const void *arg1, const void *arg2)
{
	TransactionId xid1 = *(const TransactionId *) arg1;
	TransactionId xid2 = *(const TransactionId *) arg2;

	if (xid1 == xid2)
		return 0;

	if (TransactionIdPrecedes(xid1, xid2))
		return -1;
	else
		return 1;
}

static bool
is_snapshot_eqaul(flashback_snapshot_t s1, Snapshot s2_sorted)
{
	TransactionId *xip;
	TransactionId *xip_sorted;
	uint32 xcnt;

	if (s1->xmin != s2_sorted->xmin || s1->xmax != s2_sorted->xmax || s1->xcnt != s2_sorted->xcnt ||
			s1->lsn != s2_sorted->lsn)
		return false;

	xip = (TransactionId *) POLAR_GET_FLSHBAK_SNAPSHOT_XIP(s1);
	xcnt = s1->xcnt;
	xip_sorted = s2_sorted->xip;

	qsort(xip, xcnt, sizeof(TransactionId), test_xid_comparator);

	return memcmp(xip, xip_sorted, xcnt * sizeof(TransactionId)) == 0;
}

static inline void
free_test_snapshot(Snapshot snapshot)
{
	/* Free it */
	if (snapshot->xip)
		pfree(snapshot->xip);

	if (snapshot->subxip)
		pfree(snapshot->subxip);

	pfree(snapshot);
}

static void
check_flashback_snapshot(fbpoint_pos_t snapshot_pos, flashback_snapshot_t snapshot_expected)
{
	Snapshot snapshot = NULL;
	bool is_snapshot_right;
	uint32 next_clog_subdir_no;
	uint32 next_xid;

	snapshot = polar_get_flashback_snapshot(test_fra->dir, snapshot_pos, &next_clog_subdir_no, &next_xid);
	is_snapshot_right = is_snapshot_eqaul(snapshot_expected, snapshot);
	Assert(is_snapshot_right);
	Assert(snapshot_expected->next_clog_subdir_no == next_clog_subdir_no);
	Assert(snapshot_expected->next_xid == next_xid);

	free_test_snapshot(snapshot);
}

/* Create a fake flashback snapshot */
static flashback_snapshot_header_t
create_a_fake_snapshot(uint32 size, XLogRecPtr lsn, uint32 next_clog_subdir_no, TransactionId next_xid)
{
	uint32 xcnt;
	flashback_snapshot_header_t header;
	flashback_snapshot_t snapshot;

	xcnt = size / sizeof(TransactionId) + 1;
	header = (flashback_snapshot_header_t) palloc(FLSHBAK_GET_SNAPSHOT_TOTAL_SIZE(xcnt));
	header->data_size = FLSHBAK_GET_SNAPSHOT_DATA_SIZE(xcnt);
	SET_FLSHBAK_SNAPSHOT_VERSION(header->info);

	snapshot = FLSHBAK_GET_SNAPSHOT_DATA(header);
	snapshot->lsn = lsn;
	snapshot->xcnt = xcnt;
	snapshot->next_clog_subdir_no = next_clog_subdir_no;
	snapshot->next_xid = next_xid;

	/* Xip is fake */
	return header;
}

static fbpoint_pos_t
test_back_snapshot_to_fra(fbpoint_pos_t *snapshot_end_pos, uint32 size, uint32 next_clog_subdir_no, TransactionId next_xid)
{
	XLogRecPtr lsn;
	fbpoint_pos_t snapshot_pos;
	flashback_snapshot_header_t header;
	flashback_snapshot_t snapshot;

	lsn = GetXLogInsertRecPtr();
	header = create_a_fake_snapshot(size, lsn, next_clog_subdir_no, next_xid);
	snapshot_pos = polar_backup_snapshot_to_fra(header, snapshot_end_pos, test_fra->dir);

	/* Check the snapshot info */
	snapshot = FLSHBAK_GET_SNAPSHOT_DATA(header);
	check_flashback_snapshot(snapshot_pos, snapshot);

	pfree(header);
	return snapshot_pos;
}

static void
test_flush_fbpoint_rec(void)
{
	int i;
	uint32 next_clog_subdir_no;
	uint64 rec_no_expected;
	fbpoint_rec_data_t rec_data;
	fbpoint_rec_data_t rec_data_expected;
	uint64 fake_info;
	fbpoint_pos_t snapshot_pos_expected;
	uint32 snapshot_size;
	bool snapshot_over_seg = false;
	fbpoint_pos_t snapshot_end_pos = test_fra->snapshot_end_pos;

	/* I want there is one snapshot will be splitted to two segments. */
	snapshot_size = (FBPOINT_SEG_SIZE - FBPOINT_REC_END_POS) / FBPOINT_REC_PER_SEG;
	next_clog_subdir_no = pg_atomic_read_u32(&test_clog_ctl->next_clog_subdir_no);

	/* Flush three fbpoint record files */
	for (i = 0; i < FBPOINT_REC_PER_SEG; i++)
	{
		rec_no_expected = test_point_ctl->next_fbpoint_rec_no;
		fake_info = rec_no_expected + MIN_RECORD_TIME;

		/* Test back snapshot to fra */
		snapshot_pos_expected = test_back_snapshot_to_fra(&snapshot_end_pos, snapshot_size, next_clog_subdir_no, (TransactionId) fake_info);

		if (snapshot_end_pos.seg_no != rec_no_expected / FBPOINT_REC_PER_SEG)
			snapshot_over_seg = true;

		/* It is a lucky no, skip it */
		if (fake_info == LUCKY_RECORD_NO)
			fake_info++;

		rec_data.flog_ptr = fake_info;
		rec_data.redo_lsn = fake_info;
		rec_data.time = fake_info;
		rec_data.next_clog_subdir_no = next_clog_subdir_no;
		rec_data.snapshot_pos = snapshot_pos_expected;
		rec_data_expected = rec_data;
		polar_flush_fbpoint_rec(test_point_ctl, test_fra->dir, &rec_data);
		Assert(test_point_ctl->next_fbpoint_rec_no == rec_no_expected + 1);
		check_fbpoint_rec_in_mem(rec_no_expected, &rec_data_expected);
		/* Check the flashback point record file in disk */
		check_fbpoint_rec_in_disk(&rec_data_expected);
	}

	/* There must be a snapshot not in the same segment file */
	Assert(snapshot_over_seg);

	/* Update the test_fra for rightness */
	test_fra->next_fbpoint_rec_no = test_point_ctl->next_fbpoint_rec_no;
	test_fra->snapshot_end_pos = snapshot_end_pos;
}

static void
test_get_right_fbpoint(uint64 info, uint64 expected_info, uint32 next_clog_subdir_no)
{
	fbpoint_rec_data_t record;
	uint32  seg_no_expected = 0;
	uint32  seg_no = 0;
	bool    is_expected;

	Assert(expected_info >= MIN_RECORD_TIME && expected_info != LUCKY_RECORD_NO);
	polar_get_right_fbpoint(test_point_ctl, test_fra->dir, info, &record, &seg_no);
	is_expected = check_fbpoint_record_data(&record, expected_info, expected_info, expected_info,
			next_clog_subdir_no);
	Assert(is_expected);

	if (expected_info < LUCKY_RECORD_NO)
		seg_no_expected = FBPOINT_GET_SEG_NO_BY_REC_NO(expected_info - MIN_RECORD_TIME);
	else
		seg_no_expected = FBPOINT_GET_SEG_NO_BY_REC_NO(expected_info - MIN_RECORD_TIME - 1);

	Assert(seg_no == seg_no_expected);
}

static void
test_get_right_fbpoints(void)
{
	uint64  next_rec_no;

	next_rec_no = test_point_ctl->next_fbpoint_rec_no;

	/* Now the times of the records are [MIN_RECORD_TIME, test_point_ctl->next_fbpoint_rec_no + MIN_RECORD_TIME] */

	/* the time of every record is newer than the keep time but return minimal */
	test_get_right_fbpoint(1, MIN_RECORD_TIME, 1);

	/* the time of every record is older than the keep time but return maximal */
	test_get_right_fbpoint(next_rec_no + MIN_RECORD_TIME, next_rec_no + MIN_RECORD_TIME - 1, 3);

	/* a record is the right one whose time is older than the keep time */
	test_get_right_fbpoint(LUCKY_RECORD_NO, LUCKY_RECORD_NO - 1, FBPOINT_GET_SEG_NO_BY_REC_NO(LUCKY_RECORD_NO - MIN_RECORD_TIME - 2) + 1);

	/* a record is the right one whose time is equals to the keep time */
	test_get_right_fbpoint(next_rec_no, next_rec_no,  FBPOINT_GET_SEG_NO_BY_REC_NO(next_rec_no - MIN_RECORD_TIME - 1) + 1);
}

static void
test_get_xid_status_from_fra(TransactionId xid, TransactionId max_xid, uint32 next_clog_subdir_no, XidStatus status_expected)
{
	XidStatus status;

	status = polar_flashback_get_xid_status(xid, max_xid, next_clog_subdir_no, test_fra->dir);
	Assert(status == status_expected);
}

static void
check_dir_empty(char *dir_path)
{
	DIR        *dir;
	struct dirent *de;
	bool result = true;

	dir = polar_allocate_dir(dir_path);

	while ((de = ReadDir(dir, dir_path)) != NULL)
	{
		if (!strcmp(de->d_name, ".") || !(strcmp(de->d_name, "..")))
			continue;
		else
		{
			result = false;
			break;
		}
	}

	FreeDir(dir);

	Assert(result);
}

static void
check_files_removed_in_dir(char *dir_path, uint64 value)
{
	DIR        *dir;
	struct dirent *de;
	bool result = true;
	bool found_keep = false;

	dir = polar_allocate_dir(dir_path);

	while ((de = ReadDir(dir, dir_path)) != NULL)
	{
		uint64 fname;
		int nfields;
		int len;

		len = strlen(de->d_name);

		if (strspn(de->d_name, "0123456789ABCDEF") == len)
		{
			nfields = sscanf(de->d_name, "%lu", &fname);

			Assert(nfields == 1);

			if (fname < value)
			{
				result = false;
				break;
			}
			else if (fname == value)
				found_keep = true;
		}
	}

	FreeDir(dir);

	Assert(result && found_keep);
}

static void
check_fra_file_removed(uint32 fbpoint_keep_seg_no, pg_time_t keep_time, uint32 keep_clog_subdir_no)
{
	char fbpoint_dir_path[MAXPGPATH];
	char clog_dir_path[MAXPGPATH];

	FRA_GET_SUBDIR_PATH(test_fra->dir, FBPOINT_DIR, fbpoint_dir_path);
	check_files_removed_in_dir(fbpoint_dir_path, fbpoint_keep_seg_no);

	FRA_GET_SUBDIR_PATH(test_fra->dir, FLASHBACK_CLOG_DIR, clog_dir_path);

	if (keep_clog_subdir_no == 0)
		check_dir_empty(clog_dir_path);
	else
		check_files_removed_in_dir(clog_dir_path, keep_clog_subdir_no);
}

static fbpoint_pos_t
compute_snapshot_end_pos(flashback_snapshot_t snapshot, fbpoint_pos_t *pos)
{
	uint32 seg_no;
	uint32 offset;
	Size data_size;
	Size write_size;
	fbpoint_pos_t end_pos;

	data_size = FLSHBAK_GET_SNAPSHOT_TOTAL_SIZE(snapshot->xcnt);
	seg_no = pos->seg_no;
	offset = pos->offset;

	if (offset - FBPOINT_REC_END_POS < FLSHBAK_SNAPSHOT_HEADER_SIZE)
	{
		seg_no++;
		offset = FBPOINT_SEG_SIZE;
	}

	if (data_size < offset - FBPOINT_REC_END_POS)
		SET_FBPOINT_POS(*pos, seg_no, offset - data_size);
	else
		SET_FBPOINT_POS(*pos, seg_no, FBPOINT_REC_END_POS);

	do
	{
		write_size = Min(data_size, offset - FBPOINT_REC_END_POS);
		end_pos.seg_no = seg_no;
		end_pos.offset = offset - write_size;
		data_size -= write_size;
		offset = FBPOINT_SEG_SIZE;
		seg_no++;
	}
	while (data_size > 0);

	Assert(data_size == 0);

	return end_pos;
}

static void
test_fra_fbpoint(XLogRecPtr fbpoint_lsn, pg_time_t fbpoint_time, XLogRecPtr prior_fbpoint_lsn)
{
	fbpoint_wal_info_data_t fake_wal_info;
	flashback_snapshot_header_t header;
	flashback_snapshot_t snapshot;
	XLogRecPtr lsn;
	polar_flog_rec_ptr flog_ptr;
	polar_flog_rec_ptr start_ptr;
	fbpoint_rec_data_t rec_data_expected;
	uint64 rec_no_expected;
	int min_clog_seg_no;
	uint32 next_clog_subdir_no;
	fbpoint_pos_t snapshot_pos;
	fbpoint_pos_t snapshot_end_pos;

	rec_no_expected = test_point_ctl->next_fbpoint_rec_no;
	min_clog_seg_no = test_clog_ctl->min_clog_seg_no;
	next_clog_subdir_no = pg_atomic_read_u32(&test_clog_ctl->next_clog_subdir_no);
	snapshot_pos = snapshot_end_pos = test_fra->snapshot_end_pos;

	fake_wal_info.fbpoint_lsn = fbpoint_lsn;
	fake_wal_info.fbpoint_time = fbpoint_time;
	fake_wal_info.prior_fbpoint_lsn = prior_fbpoint_lsn;

	start_ptr = flog_ptr = polar_get_flog_write_result(flog_instance->buf_ctl);

	get_fbpoint_rec_expected(&rec_data_expected, &fake_wal_info, flog_ptr);

	lsn = GetXLogInsertRecPtr();
	header = polar_get_flashback_snapshot_data(test_fra, lsn);
	snapshot = FLSHBAK_GET_SNAPSHOT_DATA(header);
	Assert(snapshot->lsn == lsn);

	polar_fra_do_fbpoint(test_fra, &fake_wal_info, &flog_ptr, header);
	Assert(flog_ptr == Min(start_ptr, rec_no_expected + MIN_RECORD_TIME - 1));
	Assert(test_fra->next_fbpoint_rec_no == rec_no_expected + 1);
	Assert(test_point_ctl->next_fbpoint_rec_no == rec_no_expected + 1);
	Assert(test_fra->min_keep_lsn == rec_no_expected + MIN_RECORD_TIME - 1);

	snapshot_end_pos = compute_snapshot_end_pos(snapshot, &snapshot_pos);
	SET_FBPOINT_POS(rec_data_expected.snapshot_pos, snapshot_pos.seg_no, snapshot_pos.offset);
	check_fbpoint_rec_in_mem(rec_no_expected, &rec_data_expected);
	Assert(FBPOINT_POS_EQUAL(test_fra->snapshot_end_pos, snapshot_end_pos));

	/* Check the new fbpoint record */
	check_fbpoint_rec_in_disk(&rec_data_expected);

	/* Check the flashback snapshot */
	check_flashback_snapshot(snapshot_pos, snapshot);

	check_fra_ctl_file(rec_no_expected + 1, rec_no_expected + MIN_RECORD_TIME - 1, min_clog_seg_no, next_clog_subdir_no);
	/*
	 * The time is large than we flushed in test_flush_fbpoint_rec, so
	 * the keep flashback point record is:
	 * rec_data.flog_ptr = rec_no_expected + MIN_RECORD_TIME - 1;
	 * rec_data.redo_lsn = rec_no_expected + MIN_RECORD_TIME - 1;
	 * rec_data.time = rec_no_expected + MIN_RECORD_TIME - 1;
	 * rec_data.next_clog_subdir_no = 3;
	 * rec_data.next_xid = rec_no_expected + MIN_RECORD_TIME - 1;
	 * It is in the fbpoint segment 2, so we remove segment 0 and 1.
	 */
	check_fra_file_removed(2, rec_no_expected + MIN_RECORD_TIME - 1, 2);
}

static void
test_flashback_snapshot_insert_xid(flshbak_rel_snpsht_t rsnapshot, TransactionId xid)
{
	uint32 xcnt;
	uint32 xip_size = rsnapshot->xip_size;
	TransactionId xmax = rsnapshot->snapshot->xmax;
	TransactionId xmin = rsnapshot->snapshot->xmin;
	uint32 removed_size = rsnapshot->removed_size;
	uint32 insert_xcnt;
	TransactionId *xip;
	int i;

	if (rsnapshot->snapshot->takenDuringRecovery)
		xcnt = rsnapshot->snapshot->subxcnt;
	else
		xcnt = rsnapshot->snapshot->xcnt;

	Assert(!TransactionIdPrecedes(xid, rsnapshot->snapshot->xmax));
	polar_update_flashback_snapshot(rsnapshot, xid);

	insert_xcnt = (int32)(xid - xmax);

	if (xid < xmax)
		insert_xcnt -= FirstNormalTransactionId;

	if (insert_xcnt == 0)
		return;

	if (rsnapshot->snapshot->takenDuringRecovery)
	{
		Assert(rsnapshot->snapshot->subxcnt == (xcnt + insert_xcnt));
		xip = rsnapshot->snapshot->subxip;
	}
	else
	{
		Assert(rsnapshot->snapshot->xcnt == (xcnt + insert_xcnt));
		xip = rsnapshot->snapshot->xip;
	}

	for (i = 0; i < insert_xcnt; i++)
	{
		Assert(xip[xcnt + i] == xmax);
		TransactionIdAdvance(xmax);
	}

	TransactionIdAdvance(xid);
	Assert(rsnapshot->snapshot->xmax == xid);
	Assert(rsnapshot->snapshot->xmin == xmin);
	Assert(rsnapshot->removed_size == removed_size);
	/* Enlarge or not */
	Assert(rsnapshot->xip_size == xip_size || rsnapshot->xip_size == (xcnt + insert_xcnt + rsnapshot->removed_size + ENLARGE_XIP_SIZE_ONCE));
}

static void
test_flashback_snapshot_remove_xid(flshbak_rel_snpsht_t rsnapshot, TransactionId xid, bool is_desc)
{
	uint32 xcnt;
	uint32 xip_size = rsnapshot->xip_size;
	TransactionId xmax = rsnapshot->snapshot->xmax;
	TransactionId xmin = rsnapshot->snapshot->xmin;
	uint32 removed_size = rsnapshot->removed_size;

	if (rsnapshot->snapshot->takenDuringRecovery)
		xcnt = rsnapshot->snapshot->subxcnt;
	else
		xcnt = rsnapshot->snapshot->xcnt;

	Assert(TransactionIdPrecedes(xid, rsnapshot->snapshot->xmax));
	polar_update_flashback_snapshot(rsnapshot, xid);

	if (rsnapshot->snapshot->takenDuringRecovery)
		Assert(rsnapshot->snapshot->subxcnt == (xcnt - 1));
	else
		Assert(rsnapshot->snapshot->xcnt == (xcnt - 1));

	Assert(rsnapshot->snapshot->xmax == xmax);

	if (is_desc)
	{
		if (rsnapshot->removed_size == 1)
			Assert(rsnapshot->removed_xid_pos[0] == xcnt - 1);
		else
		{
			Assert(rsnapshot->removed_size == removed_size + 1);
			Assert(rsnapshot->removed_xid_pos[0] == xcnt - 1);
		}

		Assert(rsnapshot->snapshot->xmin == xmin);
	}

	Assert(rsnapshot->xip_size == xip_size);
}

static void
test_flashback_snapshot(bool is_standby)
{
#define TEST_XID 300
	XLogRecord record;
	SnapshotData snapshot;
	uint32 removed_xid_pos[256];
	flashback_rel_snapshot_t rsnapshot;
	XLogReaderState xlogreader;
	TimestampTz time = 0;
	int i;
	uint32 xcnt;
	TransactionId *xip = NULL;
	TransactionId next_xid;

	/* Construct a fake record which will update the next_xid */
	record.xl_rmid = RM_XLOG_ID;
	record.xl_xid = TEST_XID;
	rsnapshot.next_clog_subdir_no = 0;
	rsnapshot.next_xid = TEST_XID;
	Assert(!polar_flashback_xact_redo(&record, &rsnapshot, time, &xlogreader));
	Assert(rsnapshot.next_clog_subdir_no == 0 && rsnapshot.next_xid == TEST_XID + 1);

	/* Construct a fake record which will update the clog_subdir_no and next_xid */
	rsnapshot.next_xid = FirstNormalTransactionId;
	record.xl_rmid = RM_XLOG_ID;
	record.xl_xid = FirstNormalTransactionId;
	Assert(!polar_flashback_xact_redo(&record, &rsnapshot, time, &xlogreader));
	Assert(rsnapshot.next_clog_subdir_no == 1 && rsnapshot.next_xid == FirstNormalTransactionId + 1);

	/* Test insert a xid to xip */
	snapshot.xcnt = 0;
	snapshot.subxcnt = 0;
	snapshot.xmax = snapshot.xmin = MaxTransactionId;
	snapshot.xip = NULL;
	snapshot.subxip = NULL;
	rsnapshot.removed_size = 0;
	rsnapshot.removed_xid_pos = removed_xid_pos;
	rsnapshot.snapshot = &snapshot;
	rsnapshot.xip_size = 0;

	if (is_standby)
		snapshot.takenDuringRecovery = true;
	else
		snapshot.takenDuringRecovery = false;

	/* Insert the xid and make xip full */
	next_xid = TEST_XID;
	test_flashback_snapshot_insert_xid(&rsnapshot, next_xid);
	/* Now xip is {MaxTransactionId, 3, 4, 5, 6, 7...299} */

	/* Test remove a xid from xip */
	for (i = 100; i < TEST_XID; i++)
	{
		next_xid--;
		test_flashback_snapshot_remove_xid(&rsnapshot, next_xid, true);
	}

	/* Now xip is {MaxTransactionId, 3, 4, 5, 6, 7...99} */

	/* Update the xmin */
	next_xid = MaxTransactionId;
	test_flashback_snapshot_remove_xid(&rsnapshot, next_xid, false);
	Assert(snapshot.xmin == FirstNormalTransactionId);

	/* Test compact the xip */
	if (is_standby)
	{
		xcnt = snapshot.subxcnt;
		xip = snapshot.subxip;
	}
	else
	{
		xcnt = snapshot.xcnt;
		xip = snapshot.xip;
	}

	polar_compact_xip(&rsnapshot);
	Assert(rsnapshot.removed_size == 0);
	next_xid = FirstNormalTransactionId;

	for (i = 0; i < xcnt; i++)
	{
		Assert(xip[i] == next_xid);
		TransactionIdAdvance(next_xid);
	}

	pfree(xip);
}

static void
test_startup_from_ctl_file(void)
{
	int min_clog_seg_no;
	uint32 next_clog_subdir_no;
	uint64 next_fbpoint_rec_no;
	XLogRecPtr min_keep_lsn;
	fbpoint_pos_t snapshot_end_pos;

	min_clog_seg_no = test_clog_ctl->min_clog_seg_no;
	next_clog_subdir_no = pg_atomic_read_u32(&test_clog_ctl->next_clog_subdir_no);
	next_fbpoint_rec_no = test_point_ctl->next_fbpoint_rec_no;
	min_keep_lsn = test_fra->min_keep_lsn;
	snapshot_end_pos = test_fra->snapshot_end_pos;

	MemSet(test_clog_ctl, 0, sizeof(flashback_clog_ctl_data_t));
	test_fra->min_keep_lsn = 0;
	test_fra->next_fbpoint_rec_no = 0;
	SET_FBPOINT_POS(test_fra->snapshot_end_pos, 0, 0);
	test_point_ctl->next_fbpoint_rec_no = 0;

	test_fra_init();
	test_fra_startup(min_clog_seg_no, next_clog_subdir_no, next_fbpoint_rec_no, min_keep_lsn, snapshot_end_pos);
}

void
_PG_init(void)
{
	if (!process_shared_preload_libraries_in_progress)
		return;

	RequestAddinShmemSpace(polar_fra_shmem_size());
	prev_shmem_startup_hook = shmem_startup_hook;
	shmem_startup_hook = test_fra_shmem_startup;
}

void
_PG_fini(void)
{
	shmem_startup_hook = prev_shmem_startup_hook;
}

PG_FUNCTION_INFO_V1(test_fast_recovery_area);
/*
 * SQL-callable entry point to perform all tests.
 */
Datum
test_fast_recovery_area(PG_FUNCTION_ARGS)
{
	fbpoint_pos_t pos;

	if (!polar_enable_fra(fra_instance))
		PG_RETURN_VOID();

	/* Test fra init */
	test_fra_init();

	/* Test fra startup */
	SET_FBPOINT_POS(pos, 0, FBPOINT_SEG_SIZE);
	test_fra_startup(0, 0, 0, GetRedoRecPtr(), pos);

	/* Test back clog to fra */
	test_clog_to_fra(TEST_CLOG_SEGNO, 0, TRANSACTION_STATUS_COMMITTED);

	/* Test flush fbpoint record in segment 0 with next_clog_subdir_no 1 */
	test_flush_fbpoint_rec();

	/* Update the min clog segment no and test move clog to fra */
	test_fra->clog_ctl->min_clog_seg_no = TEST_CLOG_SEGNO;
	test_clog_to_fra(TEST_CLOG_SEGNO - 1, 1, TRANSACTION_STATUS_ABORTED);

	/* Test flush fbpoint record in segment 1 with next_clog_subdir_no 2 */
	test_flush_fbpoint_rec();

	/* Test back clog to fra with a new subdir */
	test_clog_to_fra(TEST_CLOG_SEGNO - 1, 2, TRANSACTION_STATUS_ABORTED);

	/* Test get xid status from fra clog */
	test_get_xid_status_from_fra((uint32) XID_PER_CLOG_SEG * TEST_CLOG_SEGNO,
								 ((uint32) XID_PER_CLOG_SEG * TEST_CLOG_SEGNO + 1), 1, TRANSACTION_STATUS_COMMITTED);
	test_get_xid_status_from_fra((uint32) XID_PER_CLOG_SEG * (TEST_CLOG_SEGNO - 1),
								 (uint32) XID_PER_CLOG_SEG * (TEST_CLOG_SEGNO - 1) - 1, 3, TRANSACTION_STATUS_ABORTED);

	/* Test flush fbpoint record in segment 2 with next_clog_subdir_no 3 */
	test_flush_fbpoint_rec();

	/* Test get right fbpoint cases */
	test_get_right_fbpoints();

	/* Test fra flashback point */
	test_fra_fbpoint(GetRedoRecPtr(), (pg_time_t) time(NULL), GetRedoRecPtr() - 1);

	/* Test flashback snapshot */
	test_flashback_snapshot(false);

	/* Test fra startup again */
	test_startup_from_ctl_file();

	PG_RETURN_VOID();
}
