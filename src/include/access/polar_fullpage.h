#ifndef POLAR_FULLPAGE_H
#define POLAR_FULLPAGE_H

#include "access/polar_log.h"
#include "access/xlogdefs.h"
#include "storage/buf_internals.h"
#include "storage/lwlock.h"
#include "storage/relfilenode.h"
#include "utils/guc.h"


typedef struct log_index_snapshot_t		*logindex_snapshot_t;
typedef struct polar_fullpage_ctl_t
{
	pg_atomic_uint64            max_fullpage_no;
	/* New file init lock*/
	LWLockMinimallyPadded		file_lock;
	/* write fullpage wal lock, only one process can write fullpage wal */
	LWLockMinimallyPadded		write_lock;
} polar_fullpage_ctl_t;

#define POLAR_FULLPAGE_DIR              "polar_fullpage"

#define FULLPAGE_SEGMENT_SIZE (256 * 1024 * 1024) /* 256MB */

#define FULLPAGE_NUM_PER_FILE (FULLPAGE_SEGMENT_SIZE / BLCKSZ)
#define FULLPAGE_FILE_SEG_NO(fullpage_no) (fullpage_no / FULLPAGE_NUM_PER_FILE)
#define FULLPAGE_FILE_OFFSET(fullpage_no) ((fullpage_no * BLCKSZ) % FULLPAGE_SEGMENT_SIZE)

#define FULLPAGE_FILE_NAME(path, fullpage_no) \
	snprintf((path), MAXPGPATH, "%s/%s/%08lX.fp", POLAR_DATA_DIR(), logindex_snapshot->dir, FULLPAGE_FILE_SEG_NO(fullpage_no));
#define FULLPAGE_SEG_FILE_NAME(path, seg_no) \
	snprintf((path), MAXPGPATH, "%s/%s/%08lX.fp", POLAR_DATA_DIR(), logindex_snapshot->dir, seg_no);

#define LOG_INDEX_FULLPAGE_FILE_LOCK                     \
		(&(polar_fullpage_ctl->file_lock.lock))

#define LOG_INDEX_FULLPAGE_WRITE_LOCK                     \
		(&(polar_fullpage_ctl->write_lock.lock))

extern Size polar_fullpage_shmem_size(void);
extern void polar_fullpage_shmem_init(void);
extern uint64 polar_log_fullpage_begin(void);
extern void polar_log_fullpage_end(void);
extern void polar_write_fullpage(Page page, uint64 fullpage_no);
extern void polar_read_fullpage(Page page, uint64 fullpage_no);
extern int polar_fullpage_file_init(logindex_snapshot_t logindex_snapshot, uint64 fullpage_no);
extern void polar_update_max_fullpage_no(uint64 fullpage_no);
extern void polar_prealloc_fullpage_files(void);
extern void polar_remove_old_fullpage_files(void);
extern void polar_remove_old_fullpage_file(logindex_snapshot_t logindex_snapshot, const char *segname, uint64 min_fullpage_seg_no);
extern void polar_logindex_calc_max_fullpage_no(logindex_snapshot_t logindex_snapshot);
extern XLogRecPtr polar_log_fullpage_snapshot_image(Buffer buffer, XLogRecPtr oldest_apply_lsn);
#endif	/* POLAR_FULLPAGE_H */
