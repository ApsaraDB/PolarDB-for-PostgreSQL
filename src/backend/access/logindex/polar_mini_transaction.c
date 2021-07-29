/*-------------------------------------------------------------------------
 *
 * polar_mini_transaction.c
 *      Polar mini transaction manager
 *
 * Copyright (c) 2019, Alibaba.inc
 *
 * src/backend/access/logindexm/polar_mini_transaction.
 *
 *-------------------------------------------------------------------------
 */
/*
 * In postgres a XLOG record may include multiple pages. We use mini transaction to keep data structure change in atomic way.
 * Without mini transaction maybe there exists different verions of data page in a structure.
 * The max number of pages in a XLOG record is XLR_MAX_BLOCK_ID and
 * we use coalesced hash to keep page lock during the mini transaction.
 * 1. When we add LSN to logindex  it's mini transaction page lock must be acquired firstly.
 * And the sequence of pages lcok acquisition is the same as redo XLOG for the specific data structure.
 * 2. When search logindex for a page to apply  its mini transaction lock must be acquired if it existsn
 */
#include "postgres.h"

#include "access/polar_logindex.h"
#include "access/polar_logindex_internal.h"
#include "access/xlogdefs.h"
#include "storage/buf_internals.h"
#include "utils/polar_bitpos.h"

static bool polar_acquired_lock[MINI_TRANSACTION_TABLE_SIZE] = {false};
#define MINI_TRANS_IS_OCCUPIED(key) POLAR_BIT_IS_OCCUPIED(POLAR_LOGINDEX_WAL_SNAPSHOT->mini_transaction.occupied, key)
#define MINI_TRANS_OCCUPY(key) (POLAR_BIT_OCCUPY(POLAR_LOGINDEX_WAL_SNAPSHOT->mini_transaction.occupied, key))

void
polar_log_index_abort_mini_transaction(void)
{
	polar_page_lock_t l;

	for (l = 1; l <= MINI_TRANSACTION_TABLE_SIZE; l++)
	{
		if (polar_acquired_lock[l - 1])
			polar_log_index_mini_trans_unlock(l);
	}
}

int
polar_log_index_mini_trans_start(XLogRecPtr lsn)
{
	mini_trans_t *trans = &(POLAR_LOGINDEX_WAL_SNAPSHOT->mini_transaction);

	LWLockAcquire(MINI_TRANSACTION_LOCK, LW_EXCLUSIVE);

	if (trans->lsn != InvalidXLogRecPtr || trans->started)
	{
		LWLockRelease(MINI_TRANSACTION_LOCK);
		ereport(PANIC, (errmsg("The previous lsn %ld is not end, started=%d", trans->lsn, trans->started)));
	}

	trans->started = true;
	trans->lsn = lsn;
	LWLockRelease(MINI_TRANSACTION_LOCK);

	return 0;
}

static inline void
mini_trans_set_info(mini_trans_info_t *info, BufferTag *tag)
{
	INIT_BUFFERTAG(info->tag, tag->rnode, tag->forkNum, tag->blockNum);
	pg_atomic_write_u32(&info->refcount, 1);
	info->next = 0;
}

static polar_page_lock_t
mini_trans_insert_tag(BufferTag *tag, uint32 key)
{
	mini_trans_t *trans = &POLAR_LOGINDEX_WAL_SNAPSHOT->mini_transaction;
	mini_trans_info_t *info = trans->info;

	if (trans->lsn == InvalidXLogRecPtr || trans->started == false)
		ereport(PANIC, (errmsg("The mini transaction lsn is not set")));

	if (key >= MINI_TRANSACTION_TABLE_SIZE)
		ereport(PANIC, (errmsg("The mini transaction key is incorrect %d", key)));

	if (!MINI_TRANS_IS_OCCUPIED(key + 1))
	{
		MINI_TRANS_OCCUPY(key + 1);
		mini_trans_set_info(&info[key], tag);
		return key + 1;
	}
	else
	{
		uint32 i = MINI_TRANSACTION_TABLE_SIZE;
		mini_trans_info_t *it;

		/* Find the first empty bucket */
		while (i > 0)
		{
			if (!MINI_TRANS_IS_OCCUPIED(i))
				break;

			i--;
		}

		/* The table is full, terminate unsuccessfully */
		if (i == 0)
			return i;

		MINI_TRANS_OCCUPY(i);
		mini_trans_set_info(&info[i - 1], tag);

		/* Find the last node in the chain and point to it */
		it = &info[key];

		while (it->next != 0)
			it = &info[it->next - 1];

		it->next = i;

		return i;
	}

}

static polar_page_lock_t
mini_trans_find(BufferTag *tag, uint32 key)
{
	mini_trans_t *trans = &POLAR_LOGINDEX_WAL_SNAPSHOT->mini_transaction;
	mini_trans_info_t *info = trans->info;

	if (trans->lsn == InvalidXLogRecPtr || trans->started == false)
		ereport(PANIC, (errmsg("The mini transaction lsn is not set")));

	if (MINI_TRANS_IS_OCCUPIED(key + 1))
	{
		uint32 i = key + 1;

		while (i != 0)
		{
			mini_trans_info_t *it = &info[i - 1];
			Assert(MINI_TRANS_IS_OCCUPIED(i));

			if (BUFFERTAGS_EQUAL(*tag, it->tag))
				return i;

			i = it->next;
		}
	}

	return POLAR_INVALID_PAGE_LOCK;
}

static polar_page_lock_t
mini_trans_increase_ref(BufferTag *tag, uint32 key)
{
	uint32 i;

	mini_trans_t *trans = &POLAR_LOGINDEX_WAL_SNAPSHOT->mini_transaction;
	mini_trans_info_t *info = trans->info;

	i = mini_trans_find(tag, key);

	if (i != POLAR_INVALID_PAGE_LOCK)
	{
		mini_trans_info_t *it;
		it = &info[i - 1];
		pg_atomic_add_fetch_u32(&(it->refcount), 1);
	}

	return i;
}

polar_page_lock_t
polar_log_index_mini_trans_cond_key_lock(BufferTag *tag, uint32 key,
										 LWLockMode mode, XLogRecPtr *lsn)
{
	polar_page_lock_t l = POLAR_INVALID_PAGE_LOCK;
	LWLock *lock = NULL;
	mini_trans_t *trans = &POLAR_LOGINDEX_WAL_SNAPSHOT->mini_transaction;

	LWLockAcquire(MINI_TRANSACTION_LOCK, LW_SHARED);

	if (trans->started == true && trans->lsn != InvalidXLogRecPtr)
	{
		l = mini_trans_increase_ref(tag, key);

		if (l != POLAR_INVALID_PAGE_LOCK)
		{
			lock = MINI_TRANSACTION_GET_TABLE_LOCK(l - 1);

			if (lsn != NULL)
				*lsn = POLAR_LOGINDEX_WAL_SNAPSHOT->mini_transaction.lsn;
		}
	}

	LWLockRelease(MINI_TRANSACTION_LOCK);

	if (lock)
	{
		LWLockAcquire(lock, mode);
		polar_acquired_lock[l - 1] = true;
	}

	return l;
}

polar_page_lock_t
polar_log_index_mini_trans_cond_lock(BufferTag *tag,
									 LWLockMode mode, XLogRecPtr *lsn)
{
	uint32 key = MINI_TRANSACTION_HASH_PAGE(tag);
	return polar_log_index_mini_trans_cond_key_lock(tag, key, mode, lsn);
}

XLogRecPtr
polar_log_index_mini_trans_key_find(BufferTag *tag, uint32 key)
{
	XLogRecPtr lsn = InvalidXLogRecPtr;
	mini_trans_t *trans = &POLAR_LOGINDEX_WAL_SNAPSHOT->mini_transaction;

	LWLockAcquire(MINI_TRANSACTION_LOCK, LW_SHARED);

	if (trans->started == true && trans->lsn != InvalidXLogRecPtr)
	{
		if (mini_trans_find(tag, key) != POLAR_INVALID_PAGE_LOCK)
			lsn = POLAR_LOGINDEX_WAL_SNAPSHOT->mini_transaction.lsn;
	}

	LWLockRelease(MINI_TRANSACTION_LOCK);

	return lsn;
}

XLogRecPtr
polar_log_index_mini_trans_find(BufferTag *tag)
{
	uint32 key = MINI_TRANSACTION_HASH_PAGE(tag);

	return polar_log_index_mini_trans_key_find(tag, key);
}

polar_page_lock_t
polar_log_index_mini_trans_key_lock(BufferTag *tag, uint32 key,
									LWLockMode mode, XLogRecPtr *lsn)
{
	polar_page_lock_t l;
	LWLock *lock = NULL;

	LWLockAcquire(MINI_TRANSACTION_LOCK, LW_EXCLUSIVE);
	l = mini_trans_increase_ref(tag, key);

	if (l != POLAR_INVALID_PAGE_LOCK)
	{
		lock = MINI_TRANSACTION_GET_TABLE_LOCK(l - 1);

		if (lsn != NULL)
			*lsn = POLAR_LOGINDEX_WAL_SNAPSHOT->mini_transaction.lsn;
	}
	else
	{
		l = mini_trans_insert_tag(tag, key);

		if (l != POLAR_INVALID_PAGE_LOCK)
		{
			lock = MINI_TRANSACTION_GET_TABLE_LOCK(l - 1);

			if (lsn != NULL)
				*lsn = POLAR_LOGINDEX_WAL_SNAPSHOT->mini_transaction.lsn;
		}
	}

	LWLockRelease(MINI_TRANSACTION_LOCK);

	if (lock)
	{
		LWLockAcquire(lock, mode);
		polar_acquired_lock[l - 1] = true;
	}

	return l;
}

polar_page_lock_t
polar_log_index_mini_trans_lock(BufferTag *tag, LWLockMode mode, XLogRecPtr *lsn)
{
	uint32 key = MINI_TRANSACTION_HASH_PAGE(tag);
	uint32 l = polar_log_index_mini_trans_key_lock(tag, key, mode, lsn);

	if (l == POLAR_INVALID_PAGE_LOCK)
		ereport(PANIC, (errmsg("The mini transaction hash table is full")));

	return l;
}

void
polar_log_index_mini_trans_unlock(polar_page_lock_t l)
{
	mini_trans_t *trans = &POLAR_LOGINDEX_WAL_SNAPSHOT->mini_transaction;
	mini_trans_info_t *info = trans->info;
	uint32 i ;

	if (l == POLAR_INVALID_PAGE_LOCK || l > MINI_TRANSACTION_TABLE_SIZE)
		ereport(PANIC, (errmsg("The mini transaction hash slot value is incorrect")));

	i = l - 1;

	if (!polar_acquired_lock[i])
		elog(PANIC, "Unlock mini transaction lock %d, but it's not acquired", l);

	LWLockRelease(MINI_TRANSACTION_GET_TABLE_LOCK(i));
	LWLockAcquire(MINI_TRANSACTION_LOCK, LW_SHARED);

	if (!MINI_TRANS_IS_OCCUPIED(l)
			|| pg_atomic_read_u32(&info[i].refcount) <= 0)
	{
		LWLockRelease(MINI_TRANSACTION_LOCK);
		ereport(PANIC, (errmsg("The mini transaction hash slot state is incorrect, occupied=%ld, refcount=%d",
							   MINI_TRANS_IS_OCCUPIED(l), pg_atomic_read_u32(&info[i].refcount))));
	}

	pg_atomic_sub_fetch_u32(&info[i].refcount, 1);
	LWLockRelease(MINI_TRANSACTION_LOCK);

	polar_acquired_lock[i] = false;
}

int
polar_log_index_mini_trans_end(XLogRecPtr lsn)
{
	mini_trans_t *trans = &(POLAR_LOGINDEX_WAL_SNAPSHOT->mini_transaction);
	mini_trans_info_t *info = trans->info;
	uint64_t occupied;
	bool unlock_all;
	int pos;

	LWLockAcquire(MINI_TRANSACTION_LOCK, LW_EXCLUSIVE);

	if (trans->lsn != lsn)
	{
		LWLockRelease(MINI_TRANSACTION_LOCK);
		ereport(PANIC, (errmsg("The previous lsn %ld is not finished", trans->lsn)));
	}

	trans->started = false;

	LWLockRelease(MINI_TRANSACTION_LOCK);

	do
	{
		unlock_all = true;

		LWLockAcquire(MINI_TRANSACTION_LOCK, LW_SHARED);
		occupied = trans->occupied;

		while (occupied)
		{
			/* Get position of the lowest bit */
			POLAR_BIT_LEAST_POS(occupied, pos);

			if (pg_atomic_read_u32(&info[pos - 1].refcount) > 0)
			{
				unlock_all = false;
				break;
			}

			/* Clear the lowest bit */
			POLAR_BIT_CLEAR_LEAST(occupied);
		}

		/* Release mini transaction lock and wait page lock to be released */
		LWLockRelease(MINI_TRANSACTION_LOCK);
	}
	while (!unlock_all);

	/* Clear all occupied lock */
	while (trans->occupied)
	{
		POLAR_BIT_LEAST_POS(trans->occupied, pos);
		MemSet(&info[pos - 1], 0, sizeof(mini_trans_info_t));
		POLAR_BIT_CLEAR_LEAST(trans->occupied);
	}

	trans->lsn = InvalidXLogRecPtr;

	return 0;
}

