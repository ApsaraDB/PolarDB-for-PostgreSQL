#include "postgres.h"

#include <sys/stat.h>
#include <unistd.h>

#include "access/hash.h"
#include "access/polar_fullpage.h"
#include "access/polar_logindex.h"
#include "access/polar_logindex_internal.h"
#include "access/slru.h"
#include "access/transam.h"
#include "access/xlog.h"
#include "access/xlogdefs.h"
#include "catalog/pg_control.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "port/atomics.h"
#include "port/pg_crc32c.h"
#include "postmaster/startup.h"
#include "replication/walsender.h"
#include "replication/walreceiver.h"
#include "storage/block.h"
#include "storage/fd.h"
#include "storage/ipc.h"
#include "storage/relfilenode.h"
#include "utils/guc.h"
#include "utils/hashutils.h"
#include "utils/memutils.h"


#define LOG_INDEX_IO_LOG_LEVEL LOG
#define MEM_TBL_SIZE(type) ( (int) polar_logindex_mem_tbl_size * logindex_snapshot_mem_tbl_size_ratios[type])
/* POLAR: only walwriter can flush active to avoid another process update active memtable */
#define POLAR_ENABLE_FLUSH_ACTIVE_TABLE(logindex_snapshot) \
	(AmWalWriterProcess() && polar_enable_flush_active_logindex_memtable && \
	 logindex_snapshot->type == LOGINDEX_FULLPAGE_SNAPSHOT)
#define POLAR_ENABLE_READ_ACTIVE_TABLE(logindex_snapshot) \
	(polar_enable_flush_active_logindex_memtable && \
	 logindex_snapshot->type == LOGINDEX_FULLPAGE_SNAPSHOT)
/* POLAR: for DB upgrade, old version maybe don't has fullpage meta file */
#define POLAR_SKIP_META_FILE_ERROR(logindex_snapshot) \
	(polar_in_replica_mode() && logindex_snapshot->type == LOGINDEX_FULLPAGE_SNAPSHOT)

extern int                      polar_logindex_mem_size;
extern int                      polar_logindex_bloom_blocks;
static log_index_io_err_t       logindex_io_err = 0;
static int                      logindex_errno = 0;
struct log_index_snapshot_t    *polar_logindex_snapshot[LOGINDEX_SNAPSHOT_NUM];
int                             polar_logindex_mem_tbl_size = 0;
log_table_cache_t               logindex_table_cache[LOGINDEX_SNAPSHOT_NUM] = {{0}, {0}};

const char *const logindex_snapshot_shmem_names[] =
{
	"Log Index Snapshot", /* LOGINDEX_WAL_SNAPSHOT */
	"Log Index Fullpage Snapshot", /* LOGINDEX_FULLPAGE_SNAPSHOT */
	NULL
};
const char *const logindex_snapshot_locks_names[] =
{
	"Log Index Lock", /* LOGINDEX_WAL_SNAPSHOT */
	"Log Index Fullpage Lock", /* LOGINDEX_FULLPAGE_SNAPSHOT */
	NULL
};
const char *const logindex_snapshot_bloom_names[] =
{
	"logindex_bloom_lru", /* LOGINDEX_WAL_SNAPSHOT */
	"fullpage_bloom_lru", /* LOGINDEX_FULLPAGE_SNAPSHOT */
	NULL
};

const char *const logindex_snapshot_dirs[] =
{
	LOG_INDEX_DIR, /* LOGINDEX_WAL_SNAPSHOT */
	POLAR_FULLPAGE_DIR, /* LOGINDEX_FULLPAGE_SNAPSHOT */
	NULL
};
const char *const logindex_snapshot_meta_files[] =
{
	LOG_INDEX_DIR"/log_index_meta", /* LOGINDEX_WAL_SNAPSHOT */
	POLAR_FULLPAGE_DIR"/log_index_meta", /* LOGINDEX_FULLPAGE_SNAPSHOT */
	NULL
};

const double logindex_snapshot_mem_tbl_size_ratios[] =
{
	0.85, /* LOGINDEX_WAL_SNAPSHOT */
	0.15, /* LOGINDEX_FULLPAGE_SNAPSHOT */
	0
};


static void log_index_meta_file_auto_close(int code, Datum arg);
static void log_index_write_meta(log_index_snapshot_t *logindex_snapshot, log_index_meta_t *meta, bool update);
static void polar_logindex_snapshot_shmem_init(int type);
static void polar_logindex_snapshot_remove(int type);
static void log_index_insert_new_item(log_index_lsn_t *lsn_info, log_mem_table_t *table, uint32 key, log_seg_id_t new_item_id);
static void log_index_insert_new_seg(log_mem_table_t *table, log_seg_id_t head, log_seg_id_t seg_id, log_index_lsn_t *lsn_info);

bool
polar_log_index_check_state(log_index_snapshot_t *logindex_snapshot, uint32 state)
{
	return pg_atomic_read_u32(&logindex_snapshot->state) & state;
}

XLogRecPtr
log_index_item_max_lsn(log_idx_table_data_t *table, log_item_head_t *item)
{
	log_item_seg_t *seg;

	if (item->head_seg == item->tail_seg)
		return LOG_INDEX_SEG_MAX_LSN(table, item);

	seg = LOG_INDEX_ITEM_SEG(table, item->tail_seg);
	Assert(seg != NULL);

	return LOG_INDEX_SEG_MAX_LSN(table, seg);
}

void
log_index_validate_dir(void)
{
	int i = 0;
	struct stat stat_buf;
	char path[MAXPGPATH];

	/*
	 * If it's shared storage and mount pfs as readonly mode then
	 * we don't need to check pg_logindex directory.
	 */
	if (polar_enable_shared_storage_mode
			&& polar_mount_pfs_readonly_mode)
		return ;

	/* Creating pg_logindex and polar_fullpage directory */
	for (i = 0; i < LOGINDEX_SNAPSHOT_NUM; i++)
	{
		POLAR_FILE_PATH(path, polar_logindex_snapshot[i]->dir);

		if (polar_stat(path, &stat_buf) == 0)
		{
			if (!S_ISDIR(stat_buf.st_mode))
				ereport(FATAL,
						(errmsg("required log index directory \"%s\" does not exist",
								path)));
		}
		else
		{
			ereport(LOG,
					(errmsg("creating missing log index directory \"%s\"",
							path)));

			if (polar_make_pg_directory(path) < 0)
				ereport(FATAL,
						(errmsg("could not create missing directory \"%s\": %m",
								path)));
		}
	}
}

static Size
log_index_mem_tbl_shmem_size(int type)
{
	Size size = offsetof(log_index_snapshot_t, mem_table);
	size = add_size(size, mul_size(sizeof(log_mem_table_t), MEM_TBL_SIZE(type)));

	ereport(LOG, (errmsg("The total log index memory table size is %ld", size)));

	return size;
}

static Size
log_index_bloom_shmem_size(int type)
{
	return SimpleLruShmemSize(polar_logindex_bloom_blocks, 0);
}

static Size
log_index_lwlock_shmem_size(int type)
{
	Size size = mul_size(sizeof(LWLockMinimallyPadded), LOG_INDEX_LWLOCK_NUM(MEM_TBL_SIZE(type)));
	size = add_size(size, PG_CACHE_LINE_SIZE);

	return size;
}

Size
polar_log_index_shmem_size(void)
{
	Size size = 0;
	int i = 0;

	if (!polar_enable_redo_logindex)
		return size;

	polar_logindex_mem_tbl_size = (polar_logindex_mem_size * 1024L * 1024L) / (sizeof(log_mem_table_t) + sizeof(LWLockMinimallyPadded));

	for (i = 0; i < LOGINDEX_SNAPSHOT_NUM; i++)
		size = add_size(size, log_index_mem_tbl_shmem_size(i));

	for (i = 0; i < LOGINDEX_SNAPSHOT_NUM; i++)
		size = add_size(size, log_index_bloom_shmem_size(i));

	for (i = 0; i < LOGINDEX_SNAPSHOT_NUM; i++)
		size = add_size(size, log_index_lwlock_shmem_size(i));

	return size;
}

static void
log_index_flush_table(log_index_snapshot_t *logindex_snapshot, XLogRecPtr checkpoint_lsn)
{
	log_mem_table_t *table;
	LWLock *lock;
	log_index_meta_t *meta = &logindex_snapshot->meta;
	uint32 mid;
	bool need_flush;
	bool succeed = true;
	int  flushed = 0;
	static XLogRecPtr last_flush_max_lsn = InvalidXLogRecPtr;

	SpinLockAcquire(LOG_INDEX_SNAPSHOT_LOCK);
	mid = meta->max_idx_table_id % logindex_snapshot->mem_tbl_size;
	SpinLockRelease(LOG_INDEX_SNAPSHOT_LOCK);

	do
	{
		table = LOG_INDEX_MEM_TBL(mid);

		if (LOG_INDEX_MEM_TBL_STATE(table) == LOG_INDEX_MEM_TBL_STATE_INACTIVE ||
				(POLAR_ENABLE_FLUSH_ACTIVE_TABLE(logindex_snapshot) &&
				 LOG_INDEX_MEM_TBL_STATE(table) == LOG_INDEX_MEM_TBL_STATE_ACTIVE &&
				 !LOG_INDEX_MEM_TBL_IS_NEW(table)))
		{
			lock = LOG_INDEX_MEM_TBL_LOCK(table);
			LWLockAcquire(lock, LW_EXCLUSIVE);

			/* Nothing to change since last flush table */
			if (POLAR_ENABLE_FLUSH_ACTIVE_TABLE(logindex_snapshot) &&
					LOG_INDEX_MEM_TBL_STATE(table) == LOG_INDEX_MEM_TBL_STATE_ACTIVE &&
					last_flush_max_lsn == table->data.max_lsn &&
					!LOG_INDEX_MEM_TBL_IS_NEW(table))
			{
				LWLockRelease(lock);
				break;
			}

			/*
			 * Check state again, it may be force flushed by other process.
			 * During crash recovery GetFlushRecPtr return invalid value, so we compare
			 * with GetXLogReplayRecPtr().
			 */
			if ((LOG_INDEX_MEM_TBL_STATE(table) == LOG_INDEX_MEM_TBL_STATE_INACTIVE ||
					LOG_INDEX_MEM_TBL_STATE(table) == LOG_INDEX_MEM_TBL_STATE_ACTIVE)
					&& ((POLAR_LOGINDEX_FLUSHABLE_LSN() > table->data.max_lsn) ||
						(polar_logindex_unit_test == POLAR_LOGINDEX_MASTER_WRITE_TEST)))
			{
				succeed = polar_log_index_write_table(logindex_snapshot, table);

				if (succeed)
				{
					/* Only inactive table can set FLUSHED */
					if (LOG_INDEX_MEM_TBL_STATE(table) == LOG_INDEX_MEM_TBL_STATE_INACTIVE)
						LOG_INDEX_MEM_TBL_SET_STATE(table, LOG_INDEX_MEM_TBL_STATE_FLUSHED);

					/* save last flush fullpage logindex max_lsn for active table */
					if (POLAR_ENABLE_FLUSH_ACTIVE_TABLE(logindex_snapshot))
						last_flush_max_lsn = table->data.max_lsn;

					flushed++;
				}
			}

			LWLockRelease(lock);
		}

		/*
		 * If checkpoint lsn is valid we will flush all table which state is INACTIVE
		 * and table's lsn is smaller than checkpoint lsn.
		 * If consistent lsn is larger than max saved logindex lsn, we will try to flush table until saved logindex lsn
		 * is larger than consistent lsn.
		 * Otherwise we will try to flush all INACTIVE table, but don't flush table number more
		 * than polar_logindex_table_batch_size.
		 */
		SpinLockAcquire(LOG_INDEX_SNAPSHOT_LOCK);
		table = LOG_INDEX_MEM_TBL_ACTIVE();

		need_flush = LOG_INDEX_MEM_TBL_TID(table) > (meta->max_idx_table_id + 1);

		if (need_flush)
		{
			if (XLogRecPtrIsInvalid(checkpoint_lsn))
			{
				need_flush = (flushed < polar_logindex_table_batch_size ||
							  polar_get_consistent_lsn() > meta->max_lsn);
			}
			else
			{
				need_flush = (checkpoint_lsn > meta->max_lsn
							  && !(checkpoint_lsn >= table->data.min_lsn && checkpoint_lsn <= table->data.max_lsn));
			}
		}

		mid = meta->max_idx_table_id;
		SpinLockRelease(LOG_INDEX_SNAPSHOT_LOCK);

		mid %= logindex_snapshot->mem_tbl_size;
	}
	while (need_flush);
}

void
log_index_master_bg_write(log_index_snapshot_t *logindex_snapshot)
{
	log_index_flush_table(logindex_snapshot, InvalidXLogRecPtr);
}

void
polar_log_index_flush_table(log_index_snapshot_t *logindex_snapshot, XLogRecPtr checkpoint_lsn)
{
	log_index_flush_table(logindex_snapshot, checkpoint_lsn);
}

void
polar_log_index_invalid_bloom_cache(log_index_snapshot_t *logindex_snapshot, log_idx_table_id_t tid)
{
	int pageno = LOG_INDEX_TBL_BLOOM_PAGE_NO(tid);

	polar_slru_invalid_page(&logindex_snapshot->bloom_ctl, pageno);
}

static void
log_index_replica_bg_write(log_index_snapshot_t *logindex_snapshot)
{
	log_mem_table_t *table;
	LWLock *lock;
	uint32 mid;
	log_index_meta_t *meta = &logindex_snapshot->meta;
	bool need_flush = false;
	log_idx_table_id_t max_tid, max_saved_tid = LOG_INDEX_TABLE_INVALID_ID;
	static log_idx_table_id_t last_max_table_id = 0;

	SpinLockAcquire(LOG_INDEX_SNAPSHOT_LOCK);
	max_tid = logindex_snapshot->max_idx_table_id;
	SpinLockRelease(LOG_INDEX_SNAPSHOT_LOCK);

	/* If no new table is added then no table state need to update */
	if (max_tid == last_max_table_id)
		return;

	last_max_table_id = max_tid;

	LWLockAcquire(LOG_INDEX_IO_LOCK, LW_EXCLUSIVE);

	if (log_index_get_meta(logindex_snapshot, meta))
		max_saved_tid = meta->max_idx_table_id;
	else
		elog(WARNING, "Failed to get logindex meta from storage");

	LWLockRelease(LOG_INDEX_IO_LOCK);

	mid = (max_saved_tid - 1) % logindex_snapshot->mem_tbl_size;

	/* Update table state from maximum saved table to maximum table in memory */
	do
	{
		table = LOG_INDEX_MEM_TBL(mid);
		lock = LOG_INDEX_MEM_TBL_LOCK(table);

		need_flush = LOG_INDEX_MEM_TBL_STATE(table) == LOG_INDEX_MEM_TBL_STATE_INACTIVE;

		if (need_flush)
		{
			LWLockAcquire(lock, LW_EXCLUSIVE);

			/* Check state again, it may be force flushed by other process */
			if (LOG_INDEX_MEM_TBL_STATE(table) == LOG_INDEX_MEM_TBL_STATE_INACTIVE
					&& LOG_INDEX_MEM_TBL_TID(table) <= max_saved_tid)
			{
				polar_log_index_invalid_bloom_cache(logindex_snapshot, LOG_INDEX_MEM_TBL_TID(table));
				LOG_INDEX_MEM_TBL_SET_STATE(table, LOG_INDEX_MEM_TBL_STATE_FLUSHED);
			}

			LWLockRelease(lock);
		}

		mid = LOG_INDEX_MEM_TBL_PREV_ID(mid);
	}
	while (need_flush);
}

void
polar_log_index_bg_write(void)
{
	int i = 0;

	for (i = 0; i < LOGINDEX_SNAPSHOT_NUM; i++)
	{
		if (polar_in_replica_mode())
			log_index_replica_bg_write(polar_logindex_snapshot[i]);
		else
			log_index_master_bg_write(polar_logindex_snapshot[i]);
	}
}

static void
log_index_init_lwlock(log_index_snapshot_t *logindex_snapshot, int offset, int size, int tranche_id, const char *name)
{
	int i, j;

	LWLockRegisterTranche(tranche_id, name);

	for (i = offset, j = 0; j < size; i++, j++)
		LWLockInitialize(&(logindex_snapshot->lwlock_array[i].lock), tranche_id);
}

void
polar_log_index_shmem_init(void)
{
	int         i = 0;

	if (!polar_enable_redo_logindex)
		return ;

	for (i = 0; i < LOGINDEX_SNAPSHOT_NUM; i++)
		polar_logindex_snapshot_shmem_init(i);
}

static XLogRecPtr
polar_log_index_snapshot_base_init(log_index_snapshot_t *logindex_snapshot, XLogRecPtr checkpoint_lsn, bool replica_mode)
{
	log_index_meta_t *meta = &logindex_snapshot->meta;
	XLogRecPtr start_lsn = checkpoint_lsn;

	Assert(!polar_log_index_check_state(logindex_snapshot, POLAR_LOGINDEX_STATE_INITIALIZED));

	LWLockAcquire(LOG_INDEX_IO_LOCK, LW_EXCLUSIVE);

	/*
	 * Reset logindex when we can not get correct logindex meta from storage
	 */
	if (!polar_streaming_xlog_meta || !log_index_get_meta(logindex_snapshot, meta))
	{
		polar_logindex_snapshot_remove(logindex_snapshot->type);
		MemSet(meta, 0, sizeof(log_index_meta_t));
	}

	POLAR_LOG_LOGINDEX_META_INFO(meta);

	logindex_snapshot->max_idx_table_id = meta->max_idx_table_id;
	LOG_INDEX_MEM_TBL_ACTIVE_ID = meta->max_idx_table_id % logindex_snapshot->mem_tbl_size;

	logindex_snapshot->max_lsn = meta->max_lsn;
	MemSet(logindex_snapshot->mem_table, 0,
		   sizeof(log_mem_table_t) * logindex_snapshot->mem_tbl_size);

	if (polar_streaming_xlog_meta)
	{
		if (!replica_mode)
		{
			/* POLAR: Init log_index_meta file for master and standby. */
			if (XLogRecPtrIsInvalid(meta->start_lsn))
			{
				meta->start_lsn = checkpoint_lsn;
				log_index_write_meta(logindex_snapshot, meta, false);
			}
		}

		if (!POLAR_SKIP_META_FILE_ERROR(logindex_snapshot) &&
				meta->start_lsn == InvalidXLogRecPtr)
			elog(FATAL, "Failed to get start_lsn of logindex for replica mode");

		start_lsn = Min(meta->start_lsn, checkpoint_lsn);

		/*
		 * When we start to truncate lsn , latest_page_number may not be set up; insert a
		 * suitable value to bypass the sanity test in SimpleLruTruncate.
		 */
		logindex_snapshot->bloom_ctl.shared->latest_page_number = UINT32_MAX;

		LWLockRelease(LOG_INDEX_IO_LOCK);

		/*
		 * When initialize log index snapshot, we load max table id's data to memory.
		 * When first insert lsn to memory table, need to check whether it already exists
		 * in previous table
		 */
		if (!XLogRecPtrIsInvalid(checkpoint_lsn))
			polar_load_logindex_snapshot_from_storage(logindex_snapshot, checkpoint_lsn, true);
	}
	else
		LWLockRelease(LOG_INDEX_IO_LOCK);

	pg_atomic_fetch_or_u32(&logindex_snapshot->state, POLAR_LOGINDEX_STATE_INITIALIZED);

	return start_lsn;
}


XLogRecPtr
polar_log_index_snapshot_init(XLogRecPtr checkpoint_lsn, bool replica_mode)
{
	int i = 0;
	XLogRecPtr start_lsn = InvalidXLogRecPtr;

	log_index_validate_dir();

	for (i = 0; i < LOGINDEX_SNAPSHOT_NUM; i++)
	{
		XLogRecPtr res_lsn = InvalidXLogRecPtr;
		res_lsn = polar_log_index_snapshot_base_init(polar_logindex_snapshot[i], checkpoint_lsn, replica_mode);

		/*
		 * We only care normal logindex snapshot start_lsn,
		 * maybe fullpage snapshot is too old!
		 */
		if (polar_logindex_snapshot[i]->type == LOGINDEX_WAL_SNAPSHOT)
			start_lsn = res_lsn;
	}

	ereport(LOG, (errmsg("Init log index snapshot succeed")));

	return start_lsn;
}


static void
log_index_init_lwlock_array(log_index_snapshot_t *logindex_snapshot)
{
	Assert(logindex_snapshot->mem_tbl_size > 0);
	log_index_init_lwlock(logindex_snapshot, MINI_TRANSACTION_LOCK_OFFSET, 1,
						  LWTRANCHE_LOGINDEX_MINI_TRANSACTION, "logindex_mini_transaction");
	log_index_init_lwlock(logindex_snapshot, MINI_TRANSACTION_TABLE_LOCK_OFFSET, MINI_TRANSACTION_TABLE_SIZE,
						  LWTRANCHE_LOGINDEX_MINI_TRANSACTION_TBL, "logindex_mini_transaction_tbl");

	log_index_init_lwlock(logindex_snapshot, LOG_INDEX_MEMTBL_LOCK_OFFSET, logindex_snapshot->mem_tbl_size,
						  LWTRANCHE_LOGINDEX_MEM_TBL, "logindex_mem_tbl");

	log_index_init_lwlock(logindex_snapshot, LOG_INDEX_BLOOM_LRU_LOCK_OFFSET, 1,
						  LWTRANCHE_LOGINDEX_BLOOM_LRU, "logindex_bloom_lru");

	log_index_init_lwlock(logindex_snapshot, LOG_INDEX_HASH_LOCK_OFFSET, LOG_INDEX_MEM_TBL_HASH_LOCK_NUM,
						  LWTRANCHE_LOGINDEX_HASH_LOCK, "logindex_hash_lock");
	log_index_init_lwlock(logindex_snapshot, LOG_INDEX_IO_LOCK_OFFSET, 1,
						  LWTRANCHE_LOGINDEX_IO, "logindex_io");
}

static bool
log_index_page_precedes(int page1, int page2)
{
	return page1 < page2;
}

static void
polar_logindex_snapshot_shmem_init(int type)
{
	log_index_snapshot_t *logindex_snapshot = NULL;
	bool        found_snapshot;
	bool        found_locks;
	Size        size;

	if (!polar_enable_redo_logindex)
		return ;

	size = log_index_mem_tbl_shmem_size(type);

	StaticAssertStmt(sizeof(log_item_head_t) == LOG_INDEX_TBL_SEG_SIZE,
					 "log_item_head_t size is not same as LOG_INDEX_MEM_TBL_SEG_SIZE");
	StaticAssertStmt(sizeof(log_item_seg_t) == LOG_INDEX_TBL_SEG_SIZE,
					 "log_item_seg_t size is not same as LOG_INDEX_MEM_TBL_SEG_SIZE");

	StaticAssertStmt(LOG_INDEX_FILE_TBL_BLOOM_SIZE > sizeof(log_file_table_bloom_t),
					 "LOG_INDEX_FILE_TBL_BLOOM_SIZE is not enough for log_file_table_bloom_t");

	StaticAssertStmt(MINI_TRANSACTION_TABLE_SIZE <= sizeof(uint64_t) * CHAR_BIT,
					 "MINI_TRANSACTION_TABLE_SIZE is larger than 64bit");

	polar_logindex_snapshot[type] = (log_index_snapshot_t *)
									ShmemInitStruct(logindex_snapshot_shmem_names[type],
													size, &found_snapshot);
	logindex_snapshot = polar_logindex_snapshot[type];

	Assert(logindex_snapshot != NULL);

	/* Align lwlocks to cacheline boundary */
	logindex_snapshot->lwlock_array = (LWLockMinimallyPadded *)
									  ShmemInitStruct(logindex_snapshot_locks_names[type], log_index_lwlock_shmem_size(type),
													  &found_locks);

	if (!IsUnderPostmaster)
	{
		Assert(!found_snapshot && !found_locks);
		logindex_snapshot->type = type;
		logindex_snapshot->mem_tbl_size = MEM_TBL_SIZE(type);

		logindex_snapshot->mem_cxt = AllocSetContextCreate(TopMemoryContext,
														   "logindex snapshot mem context",
														   ALLOCSET_DEFAULT_SIZES);
		pg_atomic_init_u32(&logindex_snapshot->state, 0);

		log_index_init_lwlock_array(logindex_snapshot);

		MemSet(&logindex_snapshot->mini_transaction, 0, sizeof(mini_trans_t));
		logindex_snapshot->mini_transaction.lsn = InvalidXLogRecPtr;

		logindex_snapshot->max_allocated_seg_no = 0;

		SpinLockInit(LOG_INDEX_SNAPSHOT_LOCK);

		StrNCpy(logindex_snapshot->dir, logindex_snapshot_dirs[type], NAMEDATALEN);
		StrNCpy(logindex_snapshot->meta_file_name, logindex_snapshot_meta_files[type], MAXPGPATH);
	}
	else
		Assert(found_snapshot && found_locks);

	logindex_snapshot->bloom_ctl.PagePrecedes = log_index_page_precedes;
	SimpleLruInit(&logindex_snapshot->bloom_ctl, logindex_snapshot_bloom_names[type],
				  polar_logindex_bloom_blocks, 0,
				  LOG_INDEX_BLOOM_LRU_LOCK, logindex_snapshot_dirs[type],
				  LWTRANCHE_LOGINDEX_BLOOM_LRU, true);
}

static bool
log_index_handle_update_v1_to_v2(log_index_meta_t *meta)
{
	if (polar_is_standby())
		return false;

	return true;
}

static bool
log_index_data_compatible(log_index_meta_t *meta)
{
	switch (meta->version)
	{
		case 1:
			return log_index_handle_update_v1_to_v2(meta);

		case LOG_INDEX_VERSION:
			return true;

		default:
			return false;
	}
}

static void
log_index_meta_file_auto_close(int code, Datum arg)
{
	File fd = (File) arg;
	Assert(fd >= 0);
	FileClose(fd);
}

bool
log_index_get_meta(log_index_snapshot_t *logindex_snapshot, log_index_meta_t *meta)
{
	/* fd is static variable, we want to open file only once */
	static File fd[LOGINDEX_SNAPSHOT_NUM] = {-1, -1};
	int         r;
	char        meta_path[MAXPGPATH];
	pg_crc32    crc;
	int         type = logindex_snapshot->type;

	MemSet(meta, 0, sizeof(log_index_meta_t));

	snprintf(meta_path, MAXPGPATH, "%s/%s", POLAR_DATA_DIR(), LOG_INDEX_META_FILE);

	if (fd[type] < 0)
	{
		if ((fd[type] = PathNameOpenFile(meta_path, O_RDONLY | PG_BINARY, true)) < 0)
			return false;

		Assert(fd[type] >= 0);
		/* Register function to close meta file, we only open meta file once */
		before_shmem_exit(log_index_meta_file_auto_close, (Datum) fd[type]);
	}

	r = polar_file_pread(fd[type], (char *)meta, sizeof(log_index_meta_t), 0, WAIT_EVENT_LOGINDEX_META_READ);
	logindex_errno = errno;

	if (r != sizeof(log_index_meta_t))
	{
		ereport(WARNING,
				(errmsg("could not read file \"%s\": read %d of %d and errno=%d",
						meta_path, r, (int) sizeof(log_index_meta_t), logindex_errno)));

		return false;
	}

	crc = meta->crc;

	if (meta->magic != LOG_INDEX_MAGIC)
	{
		POLAR_LOG_LOGINDEX_META_INFO(meta);
		ereport(WARNING,
				(errmsg("The magic number of meta file is incorrect, got %d, expect %d",
						meta->magic, LOG_INDEX_MAGIC)));

		return false;
	}

	meta->crc = 0;
	meta->crc = log_index_calc_crc((unsigned char *)meta, sizeof(log_index_meta_t));

	if (crc != meta->crc)
	{
		POLAR_LOG_LOGINDEX_META_INFO(meta);
		ereport(WARNING,
				(errmsg("The crc of file %s is incorrect, got %d but expect %d", meta_path,
						crc, meta->crc)));

		return false;
	}

	if (meta->version != LOG_INDEX_VERSION
			&& !log_index_data_compatible(meta))
	{
		POLAR_LOG_LOGINDEX_META_INFO(meta);
		ereport(WARNING,
				(errmsg("The version is incorrect and incompatible, got %d, expect %d",
						meta->version, LOG_INDEX_VERSION)));

		return false;
	}

	return true;
}

XLogRecPtr
polar_log_index_start_lsn(void)
{
	log_index_snapshot_t *logindex_snapshot = POLAR_LOGINDEX_WAL_SNAPSHOT;
	XLogRecPtr start_lsn = InvalidXLogRecPtr;

	if (logindex_snapshot != NULL)
	{
		LWLockAcquire(LOG_INDEX_IO_LOCK, LW_SHARED);
		start_lsn = logindex_snapshot->meta.start_lsn;
		LWLockRelease(LOG_INDEX_IO_LOCK);
	}

	return start_lsn;
}

static void
log_index_write_meta(log_index_snapshot_t *logindex_snapshot, log_index_meta_t *meta, bool update)
{
	File         fd;
	char         meta_path[MAXPGPATH];
	int          flag = O_RDWR | PG_BINARY;

	log_index_validate_dir();

	snprintf(meta_path, MAXPGPATH, "%s/%s", POLAR_DATA_DIR(), LOG_INDEX_META_FILE);

	if (meta->max_lsn != InvalidXLogRecPtr)
		meta->start_lsn = meta->max_lsn;

	meta->magic = LOG_INDEX_MAGIC;
	meta->version = LOG_INDEX_VERSION;
	meta->crc = 0;
	meta->crc = log_index_calc_crc((unsigned char *)meta, sizeof(log_index_meta_t));

	if (!update)
		flag |= O_CREAT;

	if ((fd = PathNameOpenFile(meta_path, flag, true)) == -1)
	{
		POLAR_LOG_LOGINDEX_META_INFO(meta);
		ereport(PANIC,
				(errcode_for_file_access(),
				 errmsg("could not open file \"%s\": %m", meta_path)));
	}

	if (FileWrite(fd, (char *)meta, sizeof(log_index_meta_t), WAIT_EVENT_LOGINDEX_META_WRITE)
			!= sizeof(log_index_meta_t))
	{
		/* if write didn't set errno, assume problem is no disk space */
		if (errno == 0)
			errno = ENOSPC;

		POLAR_LOG_LOGINDEX_META_INFO(meta);
		ereport(PANIC,
				(errcode_for_file_access(),
				 errmsg("could not write file \"%s\": %m", meta_path)));
	}

	if (FileSync(fd, WAIT_EVENT_LOGINDEX_META_FLUSH) != 0)
	{
		POLAR_LOG_LOGINDEX_META_INFO(meta);
		ereport(PANIC,
				(errcode_for_file_access(),
				 errmsg("could not flush file \"%s\": %m", meta_path)));
	}

	FileClose(fd);
}

/*
 * Save table if it's running in master node's recovery process.
 * During master recovery, bgwriter is not started and we have to
 * synchronized save table to get active table.
 */
void
log_index_force_save_table(log_index_snapshot_t *logindex_snapshot, log_mem_table_t *table)
{
	static log_idx_table_id_t force_table = LOG_INDEX_TABLE_INVALID_ID;
	log_index_meta_t *meta = &logindex_snapshot->meta;
	log_idx_table_id_t tid = LOG_INDEX_MEM_TBL_TID(table);

	if (force_table != tid)
	{
		elog(LOG, "force save table %ld", tid);
		force_table = tid;
	}

	if (polar_enable_shared_storage_mode && polar_mount_pfs_readonly_mode)
	{
		log_idx_table_id_t max_tid;

		LWLockAcquire(LOG_INDEX_IO_LOCK, LW_EXCLUSIVE);

		if (log_index_get_meta(logindex_snapshot, meta))
			max_tid = meta->max_idx_table_id;
		else
			elog(FATAL, "Failed to get logindex meta from storage");

		LWLockRelease(LOG_INDEX_IO_LOCK);

		if (max_tid >= LOG_INDEX_MEM_TBL_TID(table))
		{
			polar_log_index_invalid_bloom_cache(logindex_snapshot, LOG_INDEX_MEM_TBL_TID(table));
			LOG_INDEX_MEM_TBL_SET_STATE(table, LOG_INDEX_MEM_TBL_STATE_FLUSHED);
		}
	}
	else
	{
		if (POLAR_LOGINDEX_FLUSHABLE_LSN() > table->data.max_lsn)
		{
			if (polar_log_index_write_table(logindex_snapshot, table))
				LOG_INDEX_MEM_TBL_SET_STATE(table, LOG_INDEX_MEM_TBL_STATE_FLUSHED);
			else
			{
				POLAR_LOG_LOGINDEX_META_INFO(meta);
				elog(FATAL, "Failed to save logindex table, table id=%ld", LOG_INDEX_MEM_TBL_TID(table));
			}
		}
	}
}

static void
log_index_wait_active(log_index_snapshot_t *logindex_snapshot, log_mem_table_t *table, XLogRecPtr lsn)
{
	LWLock     *lock;
	bool        end = false;

	Assert(table != NULL);
	lock = LOG_INDEX_MEM_TBL_LOCK(table);

	for (;;)
	{
		/*
		 * Wait table state changed from INACTIVE to ACTIVE.
		 */
		LWLockAcquire(lock, LW_EXCLUSIVE);

		/*
		 * We only save table to storage when polar_streaming_xlog_meta is true.
		 * If the table we are waiting is inactive then force to save it in this process.
		 */
		if (LOG_INDEX_MEM_TBL_STATE(table) == LOG_INDEX_MEM_TBL_STATE_INACTIVE
				&& polar_streaming_xlog_meta)
			log_index_force_save_table(logindex_snapshot, table);

		if (LOG_INDEX_MEM_TBL_STATE(table) == LOG_INDEX_MEM_TBL_STATE_FLUSHED)
			MemSet(table, 0, sizeof(log_mem_table_t));

		if (LOG_INDEX_MEM_TBL_STATE(table) == LOG_INDEX_MEM_TBL_STATE_FREE)
			LOG_INDEX_MEM_TBL_NEW_ACTIVE(table, lsn);

		if (LOG_INDEX_MEM_TBL_STATE(table) == LOG_INDEX_MEM_TBL_STATE_ACTIVE
				&& !LOG_INDEX_MEM_TBL_FULL(table))
			end = true;

		LWLockRelease(lock);

		if (end)
		{
			pg_atomic_fetch_and_u32(&logindex_snapshot->state, ~POLAR_LOGINDEX_STATE_WAITING);
			break;
		}

		pg_atomic_fetch_or_u32(&logindex_snapshot->state, POLAR_LOGINDEX_STATE_WAITING);
		pg_usleep(10);

		if (InRecovery)
			HandleStartupProcInterrupts();
		else
			CHECK_FOR_INTERRUPTS();
	}
}

static log_seg_id_t
log_index_mem_tbl_exists_page(BufferTag *tag,
							  log_idx_table_data_t *table, uint32 key)
{
	log_seg_id_t    exists = LOG_INDEX_TBL_SLOT_VALUE(table, key);
	log_item_head_t *item;

	item = LOG_INDEX_ITEM_HEAD(table, exists);

	while (item != NULL &&
			!BUFFERTAGS_EQUAL(item->tag, *tag))
	{
		exists = item->next_item;
		item = LOG_INDEX_ITEM_HEAD(table, exists);
	}

	return exists;
}

static bool
log_index_mem_seg_full(log_mem_table_t *table, log_seg_id_t head)
{
	log_item_head_t *item;
	log_item_seg_t *seg;

	Assert(head != LOG_INDEX_TBL_INVALID_SEG);

	item = LOG_INDEX_ITEM_HEAD(&table->data, head);

	if (item->tail_seg == head)
	{
		if (item->number == LOG_INDEX_ITEM_HEAD_LSN_NUM)
			return true;
	}
	else
	{
		seg = LOG_INDEX_ITEM_SEG(&table->data, item->tail_seg);
		Assert(seg != NULL);

		if (seg->number == LOG_INDEX_ITEM_SEG_LSN_NUM)
			return true;
	}

	return false;
}

static void
log_index_insert_new_item(log_index_lsn_t *lsn_info,
						  log_mem_table_t *table, uint32 key,
						  log_seg_id_t new_item_id)
{
	log_item_head_t *new_item = LOG_INDEX_ITEM_HEAD(&table->data, new_item_id);
	log_seg_id_t   *slot = LOG_INDEX_TBL_SLOT(&table->data, key);

	new_item->head_seg = new_item_id;
	new_item->next_item = LOG_INDEX_TBL_INVALID_SEG;
	new_item->next_seg = LOG_INDEX_TBL_INVALID_SEG;
	new_item->tail_seg = new_item_id;
	memcpy(&(new_item->tag), lsn_info->tag, sizeof(BufferTag));
	new_item->number = 1;
	new_item->prev_page_lsn = lsn_info->prev_lsn;
	LOG_INDEX_INSERT_LSN_INFO(new_item, 0, lsn_info);

	if (*slot == LOG_INDEX_TBL_INVALID_SEG)
		*slot = new_item_id;
	else
	{
		new_item->next_item = *slot;
		*slot = new_item_id;
	}
}

static void
log_index_insert_new_seg(log_mem_table_t *table, log_seg_id_t head,
						 log_seg_id_t seg_id, log_index_lsn_t *lsn_info)
{
	log_item_head_t *item = LOG_INDEX_ITEM_HEAD(&table->data, head);
	log_item_seg_t *seg = LOG_INDEX_ITEM_SEG(&table->data, seg_id);

	seg->head_seg = head;

	if (item->tail_seg == head)
		item->next_seg = seg_id;
	else
	{
		log_item_seg_t *pre_seg = LOG_INDEX_ITEM_SEG(&table->data, item->tail_seg);

		if (pre_seg == NULL)
		{
			POLAR_LOG_LOGINDEX_MEM_TABLE_INFO(table);
			ereport(PANIC, (errmsg("The log index table is corrupted, the segment %d is NULL;head=%d, seg_id=%d",
								   item->tail_seg, head, seg_id)));
		}

		pre_seg->next_seg = seg_id;
	}

	seg->prev_seg = item->tail_seg;
	item->tail_seg = seg_id;

	seg->next_seg = LOG_INDEX_TBL_INVALID_SEG;
	seg->number = 1;
	LOG_INDEX_INSERT_LSN_INFO(seg, 0, lsn_info);
}

static uint8
log_index_append_lsn(log_mem_table_t *table, log_seg_id_t head, log_index_lsn_t *lsn_info)
{
	log_item_head_t *item;
	log_item_seg_t  *seg;
	uint8           idx;

	Assert(head != LOG_INDEX_TBL_INVALID_SEG);

	item = LOG_INDEX_ITEM_HEAD(&table->data, head);

	if (item->tail_seg == head)
	{
		Assert(item->number < LOG_INDEX_ITEM_HEAD_LSN_NUM);
		idx = item->number;
		LOG_INDEX_INSERT_LSN_INFO(item, idx, lsn_info);
		item->number++;
	}
	else
	{
		seg = LOG_INDEX_ITEM_SEG(&table->data, item->tail_seg);
		Assert(seg != NULL);
		Assert(seg->number < LOG_INDEX_ITEM_SEG_LSN_NUM);
		idx = seg->number;
		LOG_INDEX_INSERT_LSN_INFO(seg, idx, lsn_info);
		seg->number++;
	}

	return idx;
}

static log_seg_id_t
log_index_next_free_seg(log_index_snapshot_t *logindex_snapshot, XLogRecPtr lsn, log_mem_table_t **active_table)
{
	log_seg_id_t    dst = LOG_INDEX_TBL_INVALID_SEG;
	log_mem_table_t *active;
	int next_mem_id = -1;

	for (;;)
	{
		active = LOG_INDEX_MEM_TBL_ACTIVE();

		if (LOG_INDEX_MEM_TBL_STATE(active) == LOG_INDEX_MEM_TBL_STATE_ACTIVE)
		{
			/*
			 * 1. However when we get a new active table, we don't know its data.prefix_lsn,
			 * we assign InvalidXLogRecPtr lsn to data.prefix_lsn, so we should
			 * distinguish which table is new without prefix_lsn, and reassign it
			 * 2. If active table is full or
			 * new lsn prefix is different than this table's lsn prefix
			 * we will allocate new active memory table.
			 */
			if (LOG_INDEX_MEM_TBL_IS_NEW(active))
			{
				LOG_INDEX_MEM_TBL_SET_PREFIX_LSN(active, lsn);
				dst = LOG_INDEX_MEM_TBL_UPDATE_FREE_HEAD(active);
			}

			if (LOG_INDEX_MEM_TBL_FULL(active) ||
					!LOG_INDEX_SAME_TABLE_LSN_PREFIX(&active->data, lsn))
			{
				LOG_INDEX_MEM_TBL_SET_STATE(active, LOG_INDEX_MEM_TBL_STATE_INACTIVE);
				next_mem_id = LOG_INDEX_MEM_TBL_NEXT_ID(LOG_INDEX_MEM_TBL_ACTIVE_ID);
			}
			else
				dst = LOG_INDEX_MEM_TBL_UPDATE_FREE_HEAD(active);
		}

		if (dst != LOG_INDEX_TBL_INVALID_SEG)
			return dst;

		if (next_mem_id != -1)
		{
			active = LOG_INDEX_MEM_TBL(next_mem_id);
			*active_table = active;

			polar_try_to_wake_bgwriter();
		}

		pgstat_report_wait_start(WAIT_EVENT_LOGINDEX_WAIT_ACTIVE);
		log_index_wait_active(logindex_snapshot, active, lsn);
		pgstat_report_wait_end();

		if (next_mem_id != -1)
		{
			SpinLockAcquire(LOG_INDEX_SNAPSHOT_LOCK);
			LOG_INDEX_MEM_TBL_ACTIVE_ID = next_mem_id;
			SpinLockRelease(LOG_INDEX_SNAPSHOT_LOCK);
		}
	}

	/* never reach here */
	return LOG_INDEX_TBL_INVALID_SEG;
}

static log_file_table_bloom_t *
log_index_get_bloom_lru(log_index_snapshot_t *logindex_snapshot, log_idx_table_id_t tid, int *slot)
{
	int pageno = LOG_INDEX_TBL_BLOOM_PAGE_NO(tid);
	int offset = LOG_INDEX_TBL_BLOOM_PAGE_OFFSET(tid);
	SlruShared  shared = logindex_snapshot->bloom_ctl.shared;

	if (offset == 0)
		*slot = SimpleLruZeroPage(&logindex_snapshot->bloom_ctl, pageno);
	else
	{
		*slot = SimpleLruReadPage(&logindex_snapshot->bloom_ctl, pageno,
								  false, InvalidTransactionId);
	}

	return (log_file_table_bloom_t *)(shared->page_buffer[*slot] + offset);
}


log_file_table_bloom_t *
log_index_get_tbl_bloom(log_index_snapshot_t *logindex_snapshot, log_idx_table_id_t tid)
{
	int pageno = LOG_INDEX_TBL_BLOOM_PAGE_NO(tid);
	int offset = LOG_INDEX_TBL_BLOOM_PAGE_OFFSET(tid);
	SlruShared  shared = logindex_snapshot->bloom_ctl.shared;

	int slot;

	slot = SimpleLruReadPage(&logindex_snapshot->bloom_ctl, pageno, false,
							 InvalidTransactionId);

	return (log_file_table_bloom_t *)(shared->page_buffer[slot] + offset);
}


static void
log_index_calc_bloom(log_mem_table_t *table, log_file_table_bloom_t *bloom)
{
	bloom_filter *filter;
	int i;

	filter = bloom_init_struct(bloom->bloom_bytes, bloom->buf_size,
							   LOG_INDEX_BLOOM_ELEMS_NUM, 0);

	for (i = 0; i < LOG_INDEX_MEM_TBL_HASH_NUM; i++)
	{
		log_seg_id_t id = LOG_INDEX_TBL_SLOT_VALUE(&table->data, i);

		if (id != LOG_INDEX_TBL_INVALID_SEG)
		{
			log_item_head_t *item = LOG_INDEX_ITEM_HEAD(&table->data, id);

			while (item != NULL)
			{
				bloom->max_lsn = Max(bloom->max_lsn,
									 log_index_item_max_lsn(&table->data, item));
				bloom->min_lsn =
					Min(bloom->min_lsn, LOG_INDEX_SEG_MIN_LSN(&table->data, item));
				bloom_add_element(filter, (unsigned char *) & (item->tag),
								  sizeof(BufferTag));
				item = LOG_INDEX_ITEM_HEAD(&table->data, item->next_item);
			}
		}
	}
}

static bool
log_index_save_table(log_index_snapshot_t *logindex_snapshot, log_idx_table_data_t *table, File fd, log_file_table_bloom_t *bloom)
{
	int ret = -1;
	uint64 segno = LOG_INDEX_FILE_TABLE_SEGMENT_NO(table->idx_table_id);
	int offset = LOG_INDEX_FILE_TABLE_SEGMENT_OFFSET(table->idx_table_id);
	char        path[MAXPGPATH];

	table->min_lsn = bloom->min_lsn;
	table->max_lsn = bloom->max_lsn;

	table->crc = 0;
	table->crc = log_index_calc_crc((unsigned char *)table, sizeof(log_idx_table_data_t));

	ret = FileWrite(fd, (char *)table, sizeof(*table), WAIT_EVENT_LOGINDEX_TBL_WRITE);

	if (ret != sizeof(*table))
	{
		logindex_errno = errno;
		logindex_io_err = LOG_INDEX_WRITE_FAILED;

		LOG_INDEX_FILE_TABLE_NAME(path, segno);
		ereport(LOG_INDEX_IO_LOG_LEVEL,
				(errcode_for_file_access(),
				 errmsg("Could not write whole table to file \"%s\" at offset %u, write size %d, errno %d",
						path, offset, ret, logindex_errno)));
		return false;
	}

	ret = FileSync(fd, WAIT_EVENT_LOGINDEX_TBL_FLUSH);

	if (ret != 0)
	{
		LOG_INDEX_FILE_TABLE_NAME(path, segno);
		ereport(LOG,
				(errcode_for_file_access(),
				 errmsg("Could not fsync file \"%s\", return code %d", path, ret)));
	}

	return true;
}

static void
log_index_get_bloom_data(log_mem_table_t *table, log_idx_table_id_t tid, log_file_table_bloom_t *bloom)
{

	bloom->idx_table_id = tid;
	bloom->max_lsn = InvalidXLogRecPtr;
	bloom->min_lsn = UINT64_MAX;
	bloom->buf_size = LOG_INDEX_FILE_TBL_BLOOM_SIZE -
					  offsetof(log_file_table_bloom_t, bloom_bytes);

	log_index_calc_bloom(table, bloom);

	bloom->crc = 0;
	bloom->crc = log_index_calc_crc((unsigned char *)bloom, LOG_INDEX_FILE_TBL_BLOOM_SIZE);
}

static void
log_index_save_bloom(log_index_snapshot_t *logindex_snapshot, log_idx_table_id_t tid, log_file_table_bloom_t *bloom)
{
	int slot;
	log_file_table_bloom_t *lru_bloom;
	SlruShared  shared = logindex_snapshot->bloom_ctl.shared;

	LWLockAcquire(LOG_INDEX_BLOOM_LRU_LOCK, LW_EXCLUSIVE);
	lru_bloom = log_index_get_bloom_lru(logindex_snapshot, tid, &slot);
	memcpy(lru_bloom, bloom, LOG_INDEX_FILE_TBL_BLOOM_SIZE);
	shared->page_dirty[slot] = true;
	/*
	 * If this tid write from offset=0, then this segment file does not exits,
	 * O_CREAT flag will be set to open this file.Otherwise we will open this segment file
	 * without O_CREAT flag to append bloom data.
	 */
	polar_slru_append_page(&logindex_snapshot->bloom_ctl, slot, LOG_INDEX_FILE_TABLE_SEGMENT_OFFSET(tid) != 0);
	LWLockRelease(LOG_INDEX_BLOOM_LRU_LOCK);
}

static int
log_index_open_table_file(log_index_snapshot_t *logindex_snapshot, log_idx_table_id_t tid, bool readonly, int elevel)
{
	uint64 segno = LOG_INDEX_FILE_TABLE_SEGMENT_NO(tid);
	int offset = LOG_INDEX_FILE_TABLE_SEGMENT_OFFSET(tid);
	char        path[MAXPGPATH];
	File fd;
	int flag;

	LOG_INDEX_FILE_TABLE_NAME(path, segno);

	if (readonly)
		flag = O_RDONLY | PG_BINARY;
	else
	{
		flag = O_RDWR | PG_BINARY;

		if (offset == 0)
			flag |= O_CREAT;
	}

	fd = PathNameOpenFile(path, flag, true);

	if (fd < 0)
	{
		logindex_io_err = LOG_INDEX_OPEN_FAILED;
		logindex_errno = errno;

		ereport(elevel,
				(errcode_for_file_access(),
				 errmsg("Could not open file \"%s\", errno %d", path, logindex_errno)));
		return -1;
	}

	if ((offset > 0) && (FileSeek(fd, (off_t) offset, SEEK_SET) < 0))
	{
		logindex_io_err = LOG_INDEX_SEEK_FAILED;
		logindex_errno = errno;
		FileClose(fd);

		ereport(elevel,
				(errcode_for_file_access(),
				 errmsg("Could not seek in file \"%s\" to offset %u, errno %d",
						path, offset, logindex_errno)));
		return -1;
	}

	return fd;
}

bool
log_index_read_table_data(log_index_snapshot_t *logindex_snapshot, log_idx_table_data_t *table, log_idx_table_id_t tid, int elevel)
{
	File fd;
	pg_crc32 crc;
	int bytes;
	uint64 segno = LOG_INDEX_FILE_TABLE_SEGMENT_NO(table->idx_table_id);
	int offset = LOG_INDEX_FILE_TABLE_SEGMENT_OFFSET(table->idx_table_id);
	static char        path[MAXPGPATH];

	fd = log_index_open_table_file(logindex_snapshot, tid, true, elevel);

	if (fd < 0)
		return false;

	bytes = FileRead(fd, (char *)table, sizeof(log_idx_table_data_t), WAIT_EVENT_LOGINDEX_TBL_READ);

	if (bytes != sizeof(log_idx_table_data_t))
	{
		logindex_io_err = LOG_INDEX_READ_FAILED;
		logindex_errno = errno;
		FileClose(fd);

		LOG_INDEX_FILE_TABLE_NAME(path, segno);
		ereport(elevel,
				(errcode_for_file_access(),
				 errmsg("Could not read whole table from file \"%s\" at offset %u, read size %d, errno %d",
						path, offset, bytes, logindex_errno)));
		return false;
	}

	FileClose(fd);
	crc = table->crc;
	table->crc = 0;
	table->crc = log_index_calc_crc((unsigned char *)table, sizeof(log_idx_table_data_t));

	if (crc != table->crc)
	{
		logindex_io_err = LOG_INDEX_CRC_FAILED;

		LOG_INDEX_FILE_TABLE_NAME(path, segno);
		ereport(elevel,
				(errmsg("The crc32 check failed in file \"%s\" at offset %u, result crc %u, expected crc %u",
						path, offset, crc, table->crc)));
		return false;
	}

	return true;
}

static bool
log_index_write_table_data(log_index_snapshot_t *logindex_snapshot, log_mem_table_t *table)
{
	bool ret = false;
	File fd;
	log_file_table_bloom_t *bloom;
	log_idx_table_id_t tid = table->data.idx_table_id;

	fd = log_index_open_table_file(logindex_snapshot, tid, false, LOG_INDEX_IO_LOG_LEVEL);

	if (fd < 0)
		return ret;

	bloom = (log_file_table_bloom_t *)palloc0(LOG_INDEX_FILE_TBL_BLOOM_SIZE);

	log_index_get_bloom_data(table, tid, bloom);

	/*
	 * POLAR: we save bloom data before table data, in case that replica
	 * read bloom data with zero page
	 */
	log_index_save_bloom(logindex_snapshot, tid, bloom);
	ret = log_index_save_table(logindex_snapshot, &table->data, fd, bloom);
	pfree(bloom);

	FileClose(fd);

	return ret;
}

static void
log_index_report_io_error(log_index_snapshot_t *logindex_snapshot, log_idx_table_id_t tid)
{
	uint64 segno = LOG_INDEX_FILE_TABLE_SEGMENT_NO(tid);
	int offset = LOG_INDEX_FILE_TABLE_SEGMENT_OFFSET(tid);
	char        path[MAXPGPATH];

	LOG_INDEX_FILE_TABLE_NAME(path, segno);
	errno = logindex_errno;

	switch (logindex_io_err)
	{
		case LOG_INDEX_OPEN_FAILED:
			ereport(FATAL,
					(errcode_for_file_access(),
					 errmsg("Could not open file \"%s\"", path)));
			break;

		case LOG_INDEX_SEEK_FAILED:
			ereport(FATAL,
					(errcode_for_file_access(),
					 errmsg("Could not seek in file \"%s\" to offset %u",
							path, offset)));
			break;

		case LOG_INDEX_READ_FAILED:
			ereport(FATAL,
					(errcode_for_file_access(),
					 errmsg("Could not read from file \"%s\" at offset %u",
							path, offset)));
			break;

		case LOG_INDEX_WRITE_FAILED:
			ereport(FATAL,
					(errcode_for_file_access(),
					 errmsg("Could not write to file \"%s\" at offset %u",
							path, offset)));
			break;

		case LOG_INDEX_FSYNC_FAILED:
			ereport(FATAL,
					(errcode_for_file_access(),
					 errmsg("Could not fsync file \"%s\"", path)));
			break;

		case LOG_INDEX_CLOSE_FAILED:
			ereport(FATAL,
					(errcode_for_file_access(),
					 errmsg("Could not close file \"%s\"", path)));
			break;

		case LOG_INDEX_CRC_FAILED:
			ereport(FATAL,
					(errmsg("The crc32 check failed in file \"%s\" at offset %u",
							path, offset)));
			break;

		default:
			/* can't get here, we trust */
			elog(PANIC, "unrecognized LogIndex error cause: %d",
				 (int) logindex_io_err);
			break;
	}
}

static bool
log_index_read_seg_file(log_index_snapshot_t *logindex_snapshot, log_table_cache_t *cache, uint64 segno)
{
	char   path[MAXPGPATH];
	File fd;
	int bytes;
	int i;
	log_index_meta_t meta;
	log_idx_table_data_t *table_data;

	LOG_INDEX_COPY_META(&meta);
	cache->min_idx_table_id = LOG_INDEX_TABLE_INVALID_ID;
	cache->max_idx_table_id = LOG_INDEX_TABLE_INVALID_ID;

	LOG_INDEX_FILE_TABLE_NAME(path, segno);
	fd = PathNameOpenFile(path, O_RDONLY | PG_BINARY, true);

	if (fd < 0)
	{
		/*no cover begin*/
		logindex_io_err = LOG_INDEX_OPEN_FAILED;
		logindex_errno = errno;

		ereport(LOG_INDEX_IO_LOG_LEVEL,
				(errcode_for_file_access(),
					errmsg("Could not open file \"%s\", errno %d", path, logindex_errno)));
		return false;
		/*no cover end*/
	}

	bytes = FileRead(fd, cache->data, LOG_INDEX_TABLE_CACHE_SIZE, WAIT_EVENT_LOGINDEX_TBL_READ);

	if (bytes < sizeof(log_idx_table_data_t))
	{
		/*no cover begin*/
		logindex_io_err = LOG_INDEX_READ_FAILED;
		logindex_errno = errno;
		FileClose(fd);

		ereport(LOG_INDEX_IO_LOG_LEVEL,
				(errcode_for_file_access(),
					errmsg("Could not read whole table from file \"%s\" at offset 0, read size %d, errno %d",
						path, bytes, logindex_errno)));
		return false;
		/*no cover end*/
	}

	FileClose(fd);

	/* segno start from 0, while log_index_table_id_t start from 1 */
	cache->min_idx_table_id = segno * LOG_INDEX_TABLE_NUM_PER_FILE + 1;
	table_data = (log_idx_table_data_t *)cache->data;

	if (table_data->idx_table_id != cache->min_idx_table_id || cache->min_idx_table_id > meta.max_idx_table_id)
	{
		cache->min_idx_table_id = LOG_INDEX_TABLE_INVALID_ID;
		elog(WARNING, "Read unexpected logindex segment=%ld file", segno);
		return false;
	}

	i = (bytes / sizeof(log_idx_table_data_t)) - 1;

	/*
	 * If this segment file is renamed from previous segment file, maybe there're old tables
	 * in the end of the file. So we have to compare to get max table id, and compare with meta to
	 * check it's already full flushed to the storage.
	 */
	while (i >= 0)
	{
		table_data = (log_idx_table_data_t *)(cache->data + sizeof(log_idx_table_data_t) * i);

		if (table_data->idx_table_id >= cache->min_idx_table_id && table_data->idx_table_id <= meta.max_idx_table_id)
		{
			cache->max_idx_table_id = table_data->idx_table_id;
			break;
		}

		i--;
	}

	return true;
}

log_idx_table_data_t *
log_index_read_table(log_index_snapshot_t *logindex_snapshot, log_idx_table_id_t tid)
{
	if (tid < logindex_table_cache[logindex_snapshot->type].min_idx_table_id || tid > logindex_table_cache[logindex_snapshot->type].max_idx_table_id)
	{
		if (!log_index_read_seg_file(logindex_snapshot, &logindex_table_cache[logindex_snapshot->type], LOG_INDEX_FILE_TABLE_SEGMENT_NO(tid)))
		{
			log_index_report_io_error(logindex_snapshot, tid);
			return NULL;
		}
	}

	if (tid < logindex_table_cache[logindex_snapshot->type].min_idx_table_id || tid > logindex_table_cache[logindex_snapshot->type].max_idx_table_id)
		elog(PANIC, "Failed to read tid = %ld, while min_tid = %ld and max_tid = %ld",
			 tid, logindex_table_cache[logindex_snapshot->type].min_idx_table_id, logindex_table_cache[logindex_snapshot->type].max_idx_table_id);

	return LOG_INDEX_GET_CACHE_TABLE(&logindex_table_cache[logindex_snapshot->type], tid);
}

bool
log_index_write_table(log_index_snapshot_t *logindex_snapshot, log_mem_table_t *table)
{
	bool succeed = false;
	uint64 segment_no;
	log_idx_table_id_t tid;
	log_index_meta_t *meta = &logindex_snapshot->meta;
	log_index_file_segment_t *min_seg = &meta->min_segment_info;

	tid = LOG_INDEX_MEM_TBL_TID(table);

	LWLockAcquire(LOG_INDEX_IO_LOCK, LW_EXCLUSIVE);

	/*
	 * Save logindex table base on the table id order
	 */
	if (tid == meta->max_idx_table_id + 1)
	{
		if (!log_index_write_table_data(logindex_snapshot, table))
			log_index_report_io_error(logindex_snapshot, LOG_INDEX_MEM_TBL_TID(table));
		/* We don't update meta when flush active memtable */
		else if (polar_logindex_unit_test == 0 &&
				 LOG_INDEX_MEM_TBL_STATE(table) == LOG_INDEX_MEM_TBL_STATE_ACTIVE)
		{
			/* Flush active memtable, nothing to do */
			succeed = true;
		}
		else
		{
			SpinLockAcquire(LOG_INDEX_SNAPSHOT_LOCK);
			meta->max_idx_table_id = Max(meta->max_idx_table_id, tid);
			meta->max_lsn = Max(meta->max_lsn, table->data.max_lsn);
			SpinLockRelease(LOG_INDEX_SNAPSHOT_LOCK);

			segment_no = LOG_INDEX_FILE_TABLE_SEGMENT_NO(tid);

			min_seg->segment_no = Min(min_seg->segment_no, segment_no);

			if (min_seg->segment_no == segment_no)
			{
				min_seg->max_lsn = Max(table->data.max_lsn, min_seg->max_lsn);

				min_seg->max_idx_table_id = Max(tid, min_seg->max_idx_table_id);
				min_seg->min_idx_table_id = min_seg->segment_no * LOG_INDEX_TABLE_NUM_PER_FILE + 1;
			}

			log_index_write_meta(logindex_snapshot, meta, true);

			succeed = true;
		}
	}

	LWLockRelease(LOG_INDEX_IO_LOCK);
	return succeed;
}

bool
polar_log_index_truncate_mem_table(XLogRecPtr lsn)
{
	int k = 0;

	for (k = 0; k < LOGINDEX_SNAPSHOT_NUM; k++)
	{
		log_index_snapshot_t *logindex_snapshot = polar_logindex_snapshot[k];
		int i;
		log_mem_table_t *table;
		LWLock *lock;

		for (i = 0; i < logindex_snapshot->mem_tbl_size; i++)
		{
			table = LOG_INDEX_MEM_TBL(i);
			lock = LOG_INDEX_MEM_TBL_LOCK(table);

			LWLockAcquire(lock, LW_EXCLUSIVE);

			if (LOG_INDEX_MEM_TBL_STATE(table) == LOG_INDEX_MEM_TBL_STATE_INACTIVE)
			{
				if (lsn > table->data.max_lsn)
					LOG_INDEX_MEM_TBL_SET_STATE(table, LOG_INDEX_MEM_TBL_STATE_FLUSHED);
			}

			LWLockRelease(lock);
		}
	}

	return true;
}

static void
log_index_update_min_segment(log_index_snapshot_t *logindex_snapshot)
{
	log_index_meta_t            *meta = &logindex_snapshot->meta;
	log_index_file_segment_t    *min_seg = &meta->min_segment_info;

	min_seg->segment_no++;
	min_seg->min_idx_table_id = min_seg->segment_no * LOG_INDEX_TABLE_NUM_PER_FILE + 1;

	if (LOG_INDEX_FILE_TABLE_SEGMENT_NO(meta->max_idx_table_id) == min_seg->segment_no)
	{
		min_seg->max_idx_table_id = meta->max_idx_table_id;
		min_seg->max_lsn = meta->max_lsn;
	}
	else
	{
		log_idx_table_data_t *table = palloc(sizeof(log_idx_table_data_t));
		min_seg->max_idx_table_id = (min_seg->segment_no + 1) * LOG_INDEX_TABLE_NUM_PER_FILE;

		if (log_index_read_table_data(logindex_snapshot, table, min_seg->max_idx_table_id, LOG_INDEX_IO_LOG_LEVEL) == false)
		{
			POLAR_LOG_LOGINDEX_META_INFO(meta);
			ereport(PANIC,
					(errmsg("Failed to read log index which tid=%ld when truncate logindex",
							min_seg->max_idx_table_id)));
		}
		else
			min_seg->max_lsn = table->max_lsn;

		pfree(table);
	}


	log_index_write_meta(logindex_snapshot, &logindex_snapshot->meta, true);
}

static bool
log_index_rename_segment_file(log_index_snapshot_t *logindex_snapshot, char *old_file)
{
	char new_file[MAXPGPATH];
	uint64 new_seg_no;
	uint64 max_seg_no;
	log_index_meta_t *meta = &logindex_snapshot->meta;
	int ret;

	LWLockAcquire(LOG_INDEX_IO_LOCK, LW_SHARED);
	max_seg_no = LOG_INDEX_FILE_TABLE_SEGMENT_NO(meta->max_idx_table_id);
	max_seg_no = Max(max_seg_no, logindex_snapshot->max_allocated_seg_no);
	new_seg_no = max_seg_no + 1;
	LOG_INDEX_FILE_TABLE_NAME(new_file, new_seg_no);

	ret = polar_durable_rename(old_file, new_file, LOG);

	if (ret == 0)
		logindex_snapshot->max_allocated_seg_no = new_seg_no;

	LWLockRelease(LOG_INDEX_IO_LOCK);

	if (ret == 0)
		elog(LOG, "logindex rename %s to %s", old_file, new_file);

	return ret == 0;
}

static bool
log_index_truncate(log_index_snapshot_t *logindex_snapshot, XLogRecPtr lsn)
{
	log_index_meta_t            *meta = &logindex_snapshot->meta;
	log_index_file_segment_t    *min_seg = &meta->min_segment_info;
	uint64                      bloom_page;
	char                        path[MAXPGPATH];
	uint64                      min_segment_no;
	uint64                      max_segment_no;
	log_idx_table_id_t          max_unused_tid;

	LWLockAcquire(LOG_INDEX_IO_LOCK, LW_EXCLUSIVE);

	if (meta->crc == 0 || XLogRecPtrIsInvalid(meta->max_lsn)
			|| XLogRecPtrIsInvalid(min_seg->max_lsn)
			|| min_seg->max_lsn >= lsn)
	{
		LWLockRelease(LOG_INDEX_IO_LOCK);
		return false;
	}

	Assert(LOG_INDEX_FILE_TABLE_SEGMENT_NO(meta->max_idx_table_id) >= min_seg->segment_no);
	Assert(meta->max_idx_table_id >= min_seg->max_idx_table_id);

	/* Keep last saved segment file */
	if (LOG_INDEX_FILE_TABLE_SEGMENT_NO(meta->max_idx_table_id) == min_seg->segment_no)
	{
		LWLockRelease(LOG_INDEX_IO_LOCK);
		return false;
	}

	/*
	 * Update meta first. If meta update succeed but fail to remove files, we will not read these files.
	 * Otherwise if we remove files succeed but fail to update meta, we will fail to read file base on meta data.
	 */
	max_segment_no = LOG_INDEX_FILE_TABLE_SEGMENT_NO(meta->max_idx_table_id);
	min_segment_no = min_seg->segment_no;
	max_unused_tid = min_seg->max_idx_table_id;
	log_index_update_min_segment(logindex_snapshot);
	max_segment_no = Max(logindex_snapshot->max_allocated_seg_no, max_segment_no);
	LWLockRelease(LOG_INDEX_IO_LOCK);

	LOG_INDEX_FILE_TABLE_NAME(path, min_segment_no);
	bloom_page = LOG_INDEX_TBL_BLOOM_PAGE_NO(max_unused_tid);
	elog(LOG, "logindex truncate bloom id=%ld page=%ld", max_unused_tid, bloom_page);

	SimpleLruTruncate(&logindex_snapshot->bloom_ctl, bloom_page);

	if (max_segment_no - min_segment_no < polar_max_logindex_files)
	{
		/* Rename unused file for next segment */
		if (log_index_rename_segment_file(logindex_snapshot, path))
			return true;
	}

	durable_unlink(path, LOG);

	return true;
}

void
polar_log_index_truncate(log_index_snapshot_t *logindex_snapshot, XLogRecPtr lsn)
{
	MemoryContext oldcontext = MemoryContextSwitchTo(logindex_snapshot->mem_cxt);

	while (log_index_truncate(logindex_snapshot, lsn))
		;

	MemoryContextSwitchTo(oldcontext);
}

bool
polar_log_index_write_table(log_index_snapshot_t *logindex_snapshot, struct log_mem_table_t *table)
{
	MemoryContext  oldcontext = MemoryContextSwitchTo(logindex_snapshot->mem_cxt) ;
	bool succeed = log_index_write_table(logindex_snapshot, table);
	MemoryContextSwitchTo(oldcontext);

	return succeed;
}

static bool
log_index_exists_in_saved_table(log_index_snapshot_t *logindex_snapshot, log_index_lsn_t *lsn_info)
{
	uint32 mid;
	log_mem_table_t *table;
	uint32 i;
	BufferTag      tag;
	log_index_lsn_t saved_lsn;

	SpinLockAcquire(LOG_INDEX_SNAPSHOT_LOCK);
	mid = LOG_INDEX_MEM_TBL_PREV_ID(LOG_INDEX_MEM_TBL_ACTIVE_ID);
	SpinLockRelease(LOG_INDEX_SNAPSHOT_LOCK);

	table = LOG_INDEX_MEM_TBL(mid);

	if (LOG_INDEX_MEM_TBL_STATE(table) != LOG_INDEX_MEM_TBL_STATE_FLUSHED)
		return false;

	saved_lsn.tag = &tag;

	for (i = table->data.last_order; i > 0; i--)
	{
		if (log_index_get_order_lsn(&table->data, i - 1, &saved_lsn) != lsn_info->lsn)
			return false;

		if (BUFFERTAGS_EQUAL(*(lsn_info->tag), *(saved_lsn.tag)))
			return true;
	}

	return false;
}

void
log_index_insert_lsn(log_index_snapshot_t *logindex_snapshot, log_index_lsn_t *lsn_info,  uint32 key)
{
	log_mem_table_t     *active = NULL;
	bool                new_item;
	log_seg_id_t        head = LOG_INDEX_TBL_INVALID_SEG;;

	Assert(lsn_info->prev_lsn < lsn_info->lsn);

	active = LOG_INDEX_MEM_TBL_ACTIVE();

	/*
	 * 1. Logindex table state is atomic uint32, it's safe to change state without table lock
	 * 2. Only one process insert lsn, so it's safe to check exists page without hash lock
	 */
	if (LOG_INDEX_MEM_TBL_STATE(active) == LOG_INDEX_MEM_TBL_STATE_ACTIVE &&
			LOG_INDEX_SAME_TABLE_LSN_PREFIX(&active->data, lsn_info->lsn))
		head = log_index_mem_tbl_exists_page(lsn_info->tag, &active->data, key);

	new_item = (head == LOG_INDEX_TBL_INVALID_SEG);

	if (!new_item && !log_index_mem_seg_full(active, head))
	{
		uint8 idx;
		log_item_head_t *item;

		LWLockAcquire(LOG_INDEX_HASH_LOCK(key), LW_EXCLUSIVE);
		idx = log_index_append_lsn(active, head, lsn_info);
		item = LOG_INDEX_ITEM_HEAD(&active->data, head);
		LOG_INDEX_MEM_TBL_ADD_ORDER(&active->data, item->tail_seg, idx);
		LWLockRelease(LOG_INDEX_HASH_LOCK(key));
	}
	else
	{
		log_seg_id_t    dst;
		log_mem_table_t *old_active = active;

		dst = log_index_next_free_seg(logindex_snapshot, lsn_info->lsn, &active);

		Assert(dst != LOG_INDEX_TBL_INVALID_SEG);

		LWLockAcquire(LOG_INDEX_HASH_LOCK(key), LW_EXCLUSIVE);

		if (new_item || active != old_active)
			log_index_insert_new_item(lsn_info, active, key, dst);
		else
			log_index_insert_new_seg(active, head, dst, lsn_info);

		LOG_INDEX_MEM_TBL_ADD_ORDER(&active->data, dst, 0);
		LWLockRelease(LOG_INDEX_HASH_LOCK(key));
	}

	SpinLockAcquire(LOG_INDEX_SNAPSHOT_LOCK);
	active->data.max_lsn = Max(lsn_info->lsn, active->data.max_lsn);
	active->data.min_lsn = Min(lsn_info->lsn, active->data.min_lsn);
	logindex_snapshot->max_lsn = active->data.max_lsn;
	SpinLockRelease(LOG_INDEX_SNAPSHOT_LOCK);
}

void
polar_log_index_add_lsn(log_index_snapshot_t *logindex_snapshot, BufferTag *tag, XLogRecPtr prev, XLogRecPtr lsn)
{
	uint32      key = LOG_INDEX_MEM_TBL_HASH_PAGE(tag);
	log_index_meta_t *meta = NULL;
	log_index_lsn_t lsn_info;

	Assert(tag != NULL);
	Assert(lsn > prev);

	meta = &logindex_snapshot->meta;

	lsn_info.tag = tag;
	lsn_info.lsn = lsn;
	lsn_info.prev_lsn = prev;

	if (polar_enable_debug)
	{
		elog(LOG, "%s LOGINDEX TYPE:%d %X/%X, ([%u, %u, %u]), %u, %u", __func__,
			 logindex_snapshot->type,
			 (uint32)(lsn >> 32), (uint32)lsn,
			 tag->rnode.spcNode,
			 tag->rnode.dbNode,
			 tag->rnode.relNode,
			 tag->forkNum,
			 tag->blockNum);
	}


	if (!polar_log_index_check_state(logindex_snapshot, POLAR_LOGINDEX_STATE_ADDING))
	{
		/*
		 * If log index initialization is not finished
		 * we don't save lsn if it's less than max saved lsn,
		 * which means it's already in saved table
		 */
		if (lsn < meta->max_lsn)
			return;

		/*
		 * If lsn is equal to max saved lsn then
		 * we check whether the tag is in saved table
		 */
		if (meta->max_lsn == lsn
				&& log_index_exists_in_saved_table(logindex_snapshot, &lsn_info))
			return;

		/*
		 * If we come here which means complete to check lsn overlap
		 * then we can save lsn to logindex memory table
		 */
		pg_atomic_fetch_or_u32(&logindex_snapshot->state, POLAR_LOGINDEX_STATE_ADDING);
		elog(LOG, "log index is insert from %lx", lsn);
	}

	log_index_insert_lsn(logindex_snapshot, &lsn_info, key);
}

log_item_head_t *
log_index_tbl_find(BufferTag *tag,
				   log_idx_table_data_t *table, uint32 key)
{
	log_seg_id_t    item_id;

	Assert(table != NULL);

	item_id = log_index_mem_tbl_exists_page(tag, table, key);
	return LOG_INDEX_ITEM_HEAD(table, item_id);
}

/*
 * POLAR: we cannot use logindex_snapshot to generate path, maybe in hashtable mode
 * can enter this function
 */
static void
polar_logindex_snapshot_remove(int type)
{
	char path[MAXPGPATH] = {0};

	/* replica cannot remove file from storage */
	if (polar_enable_shared_storage_mode && polar_mount_pfs_readonly_mode)
		return;

	POLAR_FILE_PATH(path, logindex_snapshot_dirs[type]);
	rmtree(path, false);
}

void
polar_log_index_remove_all(void)
{
	int i = 0;

	/* replica cannot remove file from storage */
	if (polar_enable_shared_storage_mode && polar_mount_pfs_readonly_mode)
		return;

	for (i = 0; i < LOGINDEX_SNAPSHOT_NUM; i++)
		polar_logindex_snapshot_remove(i);
}

void
polar_log_index_truncate_lsn(void)
{
	XLogRecPtr min_lsn = polar_calc_min_used_lsn();

	if (!XLogRecPtrIsInvalid(min_lsn))
	{
		int i = 0;
		elog(LOG, "%s min_lsn=%lX", __func__, min_lsn);

		for (i = 0; i < LOGINDEX_SNAPSHOT_NUM; i++)
			polar_log_index_truncate(polar_logindex_snapshot[i], min_lsn);
	}
}

void
polar_log_index_trigger_bgwriter(XLogRecPtr lsn)
{
	static XLogRecPtr last_trigger_lsn = InvalidXLogRecPtr;

	Assert(lsn > last_trigger_lsn);

	/*
	 * Assume worst logindex compression ratio is 1:2, if new lsn value minus last trigger lsn
	 * is larger than two memory table size then we wake up background process to
	 * truncate logindex table
	 */
	if (lsn - last_trigger_lsn > 2 * sizeof(log_mem_table_t))
	{
		polar_try_to_wake_bgwriter();
		last_trigger_lsn = lsn;
	}
}

XLogRecPtr
polar_get_logindex_snapshot_max_lsn(log_index_snapshot_t *logindex_snapshot)
{
	XLogRecPtr  max_lsn = InvalidXLogRecPtr;
	SpinLockAcquire(LOG_INDEX_SNAPSHOT_LOCK);
	max_lsn = logindex_snapshot->max_lsn;
	SpinLockRelease(LOG_INDEX_SNAPSHOT_LOCK);
	return max_lsn;
}

/*
 * POLAR: load flushed/active tables from storages
 */
void
polar_load_logindex_snapshot_from_storage(log_index_snapshot_t *logindex_snapshot, XLogRecPtr start_lsn, bool is_init)
{
	log_index_meta_t *meta = &logindex_snapshot->meta;
	log_idx_table_id_t new_max_idx_table_id = LOG_INDEX_TABLE_INVALID_ID;
	log_mem_table_t *active = LOG_INDEX_MEM_TBL_ACTIVE();
	log_mem_table_t *mem_tbl = NULL;
	static log_idx_table_data_t table;
	int mid = 0;
	log_idx_table_id_t tid = 0, min_tid = 0;

	/*
	 * If it's RW node, we only load tables whose min_lsn is less than start_lsn.
	 * But if we enable wal prefetch, we will create lsn iterator during
	 * recovery, so loading flushed table into memory can improve performance
	 * in some degree.
	 * If it's ro node, we will load all saved table from max_idx_table_id to memory table.
	 * It's optimization for the following page iterator, saving time to load table from storage.
	 */
	Assert(is_init || XLogRecPtrIsInvalid(start_lsn));

	LWLockAcquire(LOG_INDEX_IO_LOCK, LW_EXCLUSIVE);

	if (log_index_get_meta(logindex_snapshot, meta))
		new_max_idx_table_id = meta->max_idx_table_id;
	else
		elog(WARNING, "Failed to get logindex meta from storage");

	LWLockRelease(LOG_INDEX_IO_LOCK);

	/* No table in storage */
	if (meta->max_idx_table_id == LOG_INDEX_TABLE_INVALID_ID)
		return;

	/*
	 * POLAR: if we are initializing now, we load tables from big to small
	 * we cannot load all tables, because memtable is limited, so we should
	 * calculate min table to load
	 */
	if (is_init)
	{
		/* we load at most mem_tbl_size tables */
		if (new_max_idx_table_id > logindex_snapshot->mem_tbl_size)
			min_tid = Max(new_max_idx_table_id - logindex_snapshot->mem_tbl_size + 1,
						  meta->min_segment_info.min_idx_table_id);
		else
			min_tid = meta->min_segment_info.min_idx_table_id;

		/* min_segment_info.min_idx_table_id = 0 ? */
		if (min_tid == LOG_INDEX_TABLE_INVALID_ID)
			min_tid = new_max_idx_table_id;
	}
	else
	{
		Assert(POLAR_ENABLE_READ_ACTIVE_TABLE(logindex_snapshot));

		/*
		 * When db start, there is no table to load, so active table_id is
		 * still invalid, at this situation, we should assign tid to active
		 * table(tid = 1)
		 */
		if (LOG_INDEX_MEM_TBL_TID(active) == LOG_INDEX_TABLE_INVALID_ID)
			log_index_wait_active(logindex_snapshot, active, InvalidXLogRecPtr);

		min_tid = LOG_INDEX_MEM_TBL_TID(active);
		/* We cannot wrap active table slot, so limit max tid to load */
		new_max_idx_table_id = Min(new_max_idx_table_id,
								   min_tid + logindex_snapshot->mem_tbl_size - 1);
	}

	Assert(min_tid != LOG_INDEX_TABLE_INVALID_ID);

	/* If we have more flushed tables to loaded */
	if (new_max_idx_table_id >= min_tid)
	{
		/* tid from big to small */
		tid = new_max_idx_table_id;

		while (tid >= min_tid)
		{
			mid = (tid - 1) % logindex_snapshot->mem_tbl_size;
			mem_tbl = LOG_INDEX_MEM_TBL(mid);

			/* POLAR: mark current memory table FLUSHED */
			LWLockAcquire(LOG_INDEX_MEM_TBL_LOCK(mem_tbl), LW_EXCLUSIVE);
			log_index_read_table_data(logindex_snapshot, &table, tid, LOG);
			memcpy(&mem_tbl->data, &table, sizeof(log_idx_table_data_t));
			LOG_INDEX_MEM_TBL_SET_STATE(mem_tbl, LOG_INDEX_MEM_TBL_STATE_FLUSHED);
			LOG_INDEX_MEM_TBL_FREE_HEAD(mem_tbl) = LOG_INDEX_MEM_TBL_SEG_NUM;
			polar_log_index_invalid_bloom_cache(logindex_snapshot, tid);
			LWLockRelease(LOG_INDEX_MEM_TBL_LOCK(mem_tbl));

			Assert(table.idx_table_id == tid);
			tid--;

			if (!XLogRecPtrIsInvalid(start_lsn) &&
					start_lsn >= table.min_lsn)
				break;
		}

		SpinLockAcquire(LOG_INDEX_SNAPSHOT_LOCK);
		/* Switch to the old active table */
		LOG_INDEX_MEM_TBL_ACTIVE_ID = (new_max_idx_table_id - 1) % logindex_snapshot->mem_tbl_size;
		active = LOG_INDEX_MEM_TBL_ACTIVE();
		logindex_snapshot->max_idx_table_id = new_max_idx_table_id;
		Assert(!is_init || meta->max_lsn == active->data.max_lsn);
		logindex_snapshot->max_lsn = active->data.max_lsn;
		/* Switch to the new active table */
		LOG_INDEX_MEM_TBL_ACTIVE_ID = new_max_idx_table_id % logindex_snapshot->mem_tbl_size;
		active = LOG_INDEX_MEM_TBL_ACTIVE();
		SpinLockRelease(LOG_INDEX_SNAPSHOT_LOCK);

		/* wait active to become available */
		log_index_wait_active(logindex_snapshot, active, InvalidXLogRecPtr);
	}

	/* For normal wal logindex snapshot, we don't need read active table */
	if (!POLAR_ENABLE_READ_ACTIVE_TABLE(logindex_snapshot))
		return;

	Assert(active->data.idx_table_id != LOG_INDEX_TABLE_INVALID_ID);

	/*
	 * Read active mem table, maybe table has been reused, so must make sure
	 * table.idx_table_id == read_idx_table_id
	 */
	if (log_index_read_table_data(logindex_snapshot, &table, LOG_INDEX_MEM_TBL_TID(active), DEBUG1) &&
			table.idx_table_id == LOG_INDEX_MEM_TBL_TID(active))
	{
		/* Table data in storage is fresh */
		if (table.max_lsn > active->data.max_lsn)
		{
			LWLockAcquire(LOG_INDEX_MEM_TBL_LOCK(active), LW_EXCLUSIVE);
			memcpy(&active->data, &table, sizeof(log_idx_table_data_t));
			LWLockRelease(LOG_INDEX_MEM_TBL_LOCK(active));
		}

		SpinLockAcquire(LOG_INDEX_SNAPSHOT_LOCK);
		Assert(active->data.max_lsn >= logindex_snapshot->max_lsn);
		logindex_snapshot->max_idx_table_id = LOG_INDEX_MEM_TBL_TID(active);
		logindex_snapshot->max_lsn = active->data.max_lsn;
		SpinLockRelease(LOG_INDEX_SNAPSHOT_LOCK);

		/* When we read active memtable from storage, maybe free_head is too old */
		while (!LOG_INDEX_MEM_TBL_FULL(active))
		{
			log_item_head_t *item = LOG_INDEX_ITEM_HEAD(&active->data, LOG_INDEX_MEM_TBL_FREE_HEAD(active));

			if (item == NULL || item->head_seg == LOG_INDEX_TBL_INVALID_SEG)
				break;

			LOG_INDEX_MEM_TBL_UPDATE_FREE_HEAD(active);
		}
	}
}