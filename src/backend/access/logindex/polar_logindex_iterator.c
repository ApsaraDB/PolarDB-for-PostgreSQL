#include "postgres.h"

#include "access/polar_logindex.h"
#include "access/polar_logindex_internal.h"
#include "access/xlogdefs.h"
#include "miscadmin.h"
#include "storage/relfilenode.h"
#include "utils/memutils.h"
#include "utils/palloc.h"

/*
 * The backend process can have only one page iterator.
 * When backend process receive cancel query signal the memory allocated for
 * page iterator must be released.
 * This static variable is used to track allocated memory for page iterator and release
 * memory when receive signal.
 */
static log_index_page_iter_t page_iter = NULL;

static log_idx_table_data_t *log_index_get_table_data(log_index_snapshot_t *logindex_snapshot, log_index_lsn_iter_t iter, log_mem_table_t **r_table);
static void log_index_set_start_order(log_index_snapshot_t *logindex_snapshot, log_index_lsn_iter_t iter);

static log_index_page_lsn_t *
log_index_tbl_stack_next_item(log_index_tbl_stack_lsn_t *stack, uint16 *idx)
{
	log_index_page_lsn_t      *item;

	if (stack->head == NULL || stack->head->item_size == LOG_INDEX_PAGE_STACK_ITEM_SIZE)
	{
		item = palloc0(sizeof(log_index_page_lsn_t));
		item->next = stack->head;
		stack->head = item;
	}
	else
		item = stack->head;

	*idx = item->item_size++;
	item->iter_pos = item->item_size;

	return item;
}

static bool
log_index_tbl_stack_pop_lsn(log_index_tbl_stack_lsn_t *stack, log_index_lsn_t *lsn_info)
{
	log_index_page_lsn_t *item;
	uint8 idx;

	if (stack == NULL || stack->head == NULL)
		return false;

	item = stack->head;
	idx = item->iter_pos - 1;

	if (item->iter_pos == item->item_size)
		lsn_info->prev_lsn = item->prev_lsn;
	else
		lsn_info->prev_lsn = item->lsn[idx + 1];

	lsn_info->lsn = item->lsn[idx];
	item->iter_pos--;

	if (item->iter_pos == 0)
	{
		stack->head = item->next;
		pfree(item);
	}

	return true;
}

static void
log_index_tbl_stack_release(log_index_tbl_stack_lsn_t *stack)
{
	log_index_page_lsn_t *item;

	item = stack->head;

	while (item != NULL)
	{
		stack->head = item->next;
		pfree(item);

		item = stack->head;
	}
}

static log_index_tbl_stack_lsn_t *
log_index_iter_push_tbl_stack(log_index_page_stack_lsn_t *stack)
{
	log_index_tbl_stack_lsn_t *tbl_stack = palloc0(sizeof(log_index_tbl_stack_lsn_t));

	tbl_stack->next = stack->head;
	stack->head = tbl_stack;
	return tbl_stack;
}

static void
log_index_pop_tbl_stack(log_index_page_stack_lsn_t *stack)
{
	log_index_tbl_stack_lsn_t *tbl_stack = stack->head;

	if (tbl_stack != NULL)
	{
		stack->head = tbl_stack->next;
		log_index_tbl_stack_release(tbl_stack);
		pfree(tbl_stack);
	}
}

static void
log_index_stack_release(log_index_page_stack_lsn_t *stack)
{
	log_index_tbl_stack_lsn_t *tbl_stack = stack->head;

	while (tbl_stack != NULL)
	{
		log_index_tbl_stack_release(tbl_stack);
		stack->head = tbl_stack->next;
		log_index_pop_tbl_stack(stack);

		tbl_stack = stack->head;
	}
}

static BufferTag *
log_index_get_seg_page_tag(log_idx_table_data_t *table, log_seg_id_t seg_id)
{
	log_item_seg_t  *seg = LOG_INDEX_ITEM_SEG(table, seg_id);
	log_seg_id_t    head_seg;
	log_item_head_t *head;

	Assert(seg != NULL);
	head_seg = seg->head_seg;
	head = LOG_INDEX_ITEM_HEAD(table, head_seg);
	Assert(head != NULL);

	return &head->tag;
}

static XLogRecPtr
log_index_get_seg_prev_lsn(log_idx_table_data_t *table,
						   log_seg_id_t seg_id, uint8 idx)
{
	log_item_seg_t  *seg = LOG_INDEX_ITEM_SEG(table, seg_id);
	log_seg_id_t    head_seg;
	log_seg_id_t    prev_seg;
	log_item_head_t *head;

	Assert(seg != NULL);
	head_seg = seg->head_seg;

	if (head_seg == seg_id)
	{
		head = LOG_INDEX_ITEM_HEAD(table, seg_id);

		if (idx == 0)
			return head->prev_page_lsn;
		else
			return LOG_INDEX_COMBINE_LSN(table, head->suffix_lsn[idx - 1]);
	}

	if (idx > 0)
		return LOG_INDEX_COMBINE_LSN(table, seg->suffix_lsn[idx - 1]);

	prev_seg = seg->prev_seg;

	if (prev_seg == seg->head_seg)
	{
		head = LOG_INDEX_ITEM_HEAD(table, prev_seg);
		Assert(head != NULL);
		return LOG_INDEX_COMBINE_LSN(table, head->suffix_lsn[head->number - 1]);
	}
	else
	{
		seg = LOG_INDEX_ITEM_SEG(table, prev_seg);
		Assert(seg != NULL);
		return LOG_INDEX_COMBINE_LSN(table, seg->suffix_lsn[seg->number - 1]);
	}
}

static void
log_index_page_iterate_push_lsn(log_index_page_iter_t iter,
								log_index_tbl_stack_lsn_t *stack,
								log_idx_table_data_t *table,
								log_seg_id_t seg_id, bool *prev_correct)
{
	size_t i, idx, size;
	XLogRecPtr l;
	log_index_page_lsn_t *stack_item;
	uint16_t             stack_idx;
	log_item_head_t *head = NULL;
	log_item_seg_t *seg = LOG_INDEX_ITEM_SEG(table, seg_id);
	log_seg_id_t   head_seg = seg->head_seg;

	if (head_seg == seg_id)
	{
		head = LOG_INDEX_ITEM_HEAD(table, seg_id);
		size = head->number;
	}
	else
		size = seg->number;

	for (i = size; i > 0; i--)
	{
		idx = i - 1;
		l = (head != NULL) ? LOG_INDEX_COMBINE_LSN(table, head->suffix_lsn[idx])
			: LOG_INDEX_COMBINE_LSN(table, seg->suffix_lsn[idx]);

		if (*prev_correct == false && l == iter->iter_prev_lsn)
			*prev_correct = true;

		if (l >= iter->min_lsn && l <= iter->iter_max_lsn)
		{
			/*
			 * Push lsn only when previous page lsn is in this table.
			 * Otherwise the table contains previous page lsn is not flushed
			 */
			if (*prev_correct == false)
			{
				iter->state = ITERATE_STATE_HOLLOW;
				elog(ERROR, "Check previous lsn failed and there exists hollow");
				break;
			}

			stack_item = log_index_tbl_stack_next_item(stack, &stack_idx);
			stack_item->prev_lsn = log_index_get_seg_prev_lsn(table, seg_id, idx);

			if (head != NULL)
				LOG_INDEX_COPY_SEG_INFO(stack_item, stack_idx, table, head, idx);
			else
				LOG_INDEX_COPY_SEG_INFO(stack_item, stack_idx, table, seg, idx);

			if (l == iter->min_lsn)
			{
				iter->state = ITERATE_STATE_FINISHED;
				break;
			}
		}
		else if (l < iter->min_lsn)
		{
			/*
			 * If previous lsn page check failed and no need to search forward
			 * the table which contains previous page lsn is not flushed.
			 */
			iter->state = *prev_correct ?
						  ITERATE_STATE_FINISHED : ITERATE_STATE_HOLLOW;

			if (*prev_correct == false)
			{
				iter->state = ITERATE_STATE_HOLLOW;
				elog(ERROR, "There exists hollow");
			}
			else
				iter->state = ITERATE_STATE_FINISHED;

			break;
		}
	}
}

static void
log_index_iterate_table_data(log_index_page_iter_t iter, log_idx_table_data_t *table, log_item_head_t *item_head)
{
	log_item_seg_t *item;
	log_seg_id_t    item_id;
	log_index_tbl_stack_lsn_t *tbl_stack;
	log_index_page_stack_lsn_t *stack = &(iter->lsn_stack);
	bool prev_correct;

	if (item_head->number <= 0)
	{
		POLAR_LOG_LOGINDEX_TABLE_INFO(table);
		ereport(PANIC,
				(errmsg("The page's log index should be in this table,but we did't found it")));
	}

	prev_correct = iter->iter_prev_lsn == InvalidXLogRecPtr ? true : false;
	log_index_iter_push_tbl_stack(stack);
	tbl_stack = stack->head;

	tbl_stack->prev_page_lsn = item_head->prev_page_lsn;

	item_id = item_head->tail_seg;

	Assert(item_id != LOG_INDEX_TBL_INVALID_SEG);

	do
	{
		item = LOG_INDEX_ITEM_SEG(
				   table,
				   item_id);

		/*
		 * Compare pointer address to check whether it's head
		 */
		if ((void *)item == (void *)item_head)
			break;

		if (item == NULL || item->number <= 0)
		{
			POLAR_LOG_LOGINDEX_TABLE_INFO(table);
			ereport(PANIC,
					(errmsg("There should be one more log index segment in this table")));
		}


		log_index_page_iterate_push_lsn(iter, tbl_stack, table, item_id, &prev_correct);
		item_id = item->prev_seg;
	}
	while (iter->state == ITERATE_STATE_FORWARD);

	if (iter->state == ITERATE_STATE_FORWARD)
		log_index_page_iterate_push_lsn(iter, tbl_stack, table, item_id, &prev_correct);

#ifndef LOG_INDEX_SUPPORT_NO_PREVIOUS_LSN

	if (item_head->prev_page_lsn == InvalidXLogRecPtr)
		iter->state = ITERATE_STATE_FINISHED;

#endif
}

static void
log_index_push_tbl_lsn(log_index_page_iter_t iter, log_idx_table_data_t *table)
{
	log_item_head_t *item;
	item = log_index_tbl_find(&iter->tag, table, iter->key);

	if (item != NULL)
	{
		XLogRecPtr min_lsn = LOG_INDEX_SEG_MIN_LSN(table, item);
		XLogRecPtr max_lsn = log_index_item_max_lsn(table, item);
		log_index_page_stack_lsn_t *stack = &(iter->lsn_stack);
		log_index_tbl_stack_lsn_t *prev_stack = stack->head;

		if (prev_stack != NULL && prev_stack->prev_page_lsn != InvalidXLogRecPtr)
			iter->iter_prev_lsn = prev_stack->prev_page_lsn;

		/*
		 * If item's min_lsn larger than iter->max_lsn then we search forward
		 */
		if (min_lsn > iter->max_lsn)
			return;

		if (max_lsn < iter->min_lsn)
		{
			iter->state = ITERATE_STATE_FINISHED;
			return;
		}

		/*
		 * 1. Push lsn in set [iter->min_lsn, iter->iter_max_lsn]
		 * 2. If previous page lsn is set, we need to check it's in this table
		 * 3. Update iter->iter_max_lsn to avoid push overlap value
		 */
		log_index_iterate_table_data(iter, table, item);
		iter->iter_max_lsn = max_lsn + 1;
	}
}

static bool
log_index_table_in_range(log_index_page_iter_t iter, log_mem_table_t *table)
{
	if (LOG_INDEX_MEM_TBL_STATE(table) == LOG_INDEX_MEM_TBL_STATE_ACTIVE)
		return true;

	if (table->data.min_lsn > iter->max_lsn)
		return false;

	if (table->data.max_lsn < iter->min_lsn)
	{
		iter->state = ITERATE_STATE_FINISHED;
		return false;
	}

	return true;
}

static log_idx_table_id_t
log_index_push_mem_tbl_lsn(log_index_snapshot_t *logindex_snapshot, log_index_page_iter_t iter)
{
	uint32               mid, first_mid;
	log_mem_table_t     *table;
	log_idx_table_id_t  tid = LOG_INDEX_TABLE_INVALID_ID;
	bool                done = false;
	bool                hash_lock_held = false;

	SpinLockAcquire(LOG_INDEX_SNAPSHOT_LOCK);
	mid = LOG_INDEX_MEM_TBL_ACTIVE_ID;
	SpinLockRelease(LOG_INDEX_SNAPSHOT_LOCK);

	first_mid = mid;

	/* Care about > or >= */
	do
	{
		uint32     state;
		LWLock     *table_lock;

		table = LOG_INDEX_MEM_TBL(mid);
		table_lock = LOG_INDEX_MEM_TBL_LOCK(table);

		LWLockAcquire(table_lock, LW_SHARED);
		state = LOG_INDEX_MEM_TBL_STATE(table);

		if (state == LOG_INDEX_MEM_TBL_STATE_ACTIVE)
		{
			LWLockAcquire(LOG_INDEX_HASH_LOCK(iter->key), LW_SHARED);
			hash_lock_held = true;
		}

		/*
		 * We seach from big to small.If memory table id is bigger than previous id
		 * than this memory table data is changed
		 */
		if ((tid == LOG_INDEX_TABLE_INVALID_ID || tid > LOG_INDEX_MEM_TBL_TID(table))
				&& state != LOG_INDEX_MEM_TBL_STATE_FREE)
		{
			if (log_index_table_in_range(iter, table))
				log_index_push_tbl_lsn(iter, &table->data);

			tid = LOG_INDEX_MEM_TBL_TID(table);
		}
		else
			done = true;

		if (hash_lock_held)
		{
			LWLockRelease(LOG_INDEX_HASH_LOCK(iter->key));
			hash_lock_held = false;
		}

		LWLockRelease(table_lock);

		mid = LOG_INDEX_MEM_TBL_PREV_ID(mid);

		CHECK_FOR_INTERRUPTS();
	}
	while (mid != first_mid && !done && (iter->state == ITERATE_STATE_FORWARD));

	return tid;
}

static log_idx_table_id_t
log_index_next_search_file_tid(log_idx_table_id_t prev_idx_tid, log_index_meta_t *meta, bool *hollow)
{
	log_idx_table_id_t tid;

	*hollow = false;

	/*
	 * The tid is the last searched memory table id, so we search file table from tid-1.
	 * If tid is UINT64_MAX, then we search from saved max table id.
	 */
	if (prev_idx_tid == LOG_INDEX_TABLE_INVALID_ID)
		tid = meta->max_idx_table_id;
	else
	{
		tid = prev_idx_tid - 1;

		if (tid == LOG_INDEX_TABLE_INVALID_ID)
			return LOG_INDEX_TABLE_INVALID_ID;

		if (tid > meta->max_idx_table_id && !LOG_INDEX_ONLY_IN_MEM)
		{
			/*
			 * If destinated table id is larger than max saved table id,then destinated
			 * table is not saved
			 */
			*hollow = true;
			return LOG_INDEX_TABLE_INVALID_ID;
		}
	}

	return tid;
}

static void
log_index_push_file_tbl_lsn(log_index_snapshot_t *logindex_snapshot, log_index_page_iter_t iter, log_idx_table_id_t prev_idx_tid)
{
	bool hollow;
	log_file_table_bloom_t *bloom;
	log_idx_table_id_t tid;
	log_idx_table_data_t *table;
	log_index_meta_t meta;
	log_index_file_segment_t  *min_seg = &meta.min_segment_info;
	bool    retry = false;

	LOG_INDEX_COPY_META(&meta);

	if (prev_idx_tid == LOG_INDEX_TABLE_INVALID_ID ||
			meta.max_idx_table_id < (prev_idx_tid - 1))
	{
		LWLockAcquire(LOG_INDEX_IO_LOCK, LW_EXCLUSIVE);

		if (log_index_get_meta(logindex_snapshot, &logindex_snapshot->meta))
			memcpy(&meta, &logindex_snapshot->meta, sizeof(log_index_meta_t));
		else
			elog(FATAL, "Failed to get logindex meta from storage");

		LWLockRelease(LOG_INDEX_IO_LOCK);
	}

	tid = log_index_next_search_file_tid(prev_idx_tid, &meta, &hollow);

	if (hollow)
	{
		iter->state = ITERATE_STATE_HOLLOW;
		elog(ERROR, "Failed to search next tid for page iter, and prev tid is %ld", prev_idx_tid);
		return;
	}

	while (tid != LOG_INDEX_TABLE_INVALID_ID &&
			tid >= min_seg->min_idx_table_id && iter->state == ITERATE_STATE_FORWARD)
	{
		bloom_filter *filter;
		bool        not_exists;

		LWLockAcquire(LOG_INDEX_BLOOM_LRU_LOCK, LW_EXCLUSIVE);
		bloom = log_index_get_tbl_bloom(logindex_snapshot, tid);
		filter = bloom_init_struct(bloom->bloom_bytes, bloom->buf_size,
								   LOG_INDEX_BLOOM_ELEMS_NUM, 0);

		/*
		 * POLAR: a bloom page contains 2 table(t1, t2) logindex info, for fullpage
		 * snapshot logindex table when we read t1 bloom data, maybe t2 bloom data
		 * is still zero page, so we need force invalid bloom cache, and try again
		 */
		if (!retry && bloom->idx_table_id == LOG_INDEX_TABLE_INVALID_ID)
		{
			/*no cover begin*/
			LWLockRelease(LOG_INDEX_BLOOM_LRU_LOCK);
			polar_log_index_invalid_bloom_cache(logindex_snapshot, tid);
			retry = true;
			continue;
			/*no cover end*/
		}
		else
			retry = false;

		if (tid != bloom->idx_table_id)
			elog(PANIC, "Failed to get logindex bloom data,dest_tid %lu, got %lu", tid, bloom->idx_table_id);

		if (bloom->max_lsn < iter->min_lsn)
		{
			iter->state = ITERATE_STATE_FINISHED;
			LWLockRelease(LOG_INDEX_BLOOM_LRU_LOCK);
			continue;
		}

		not_exists = bloom_lacks_element(filter, (unsigned char *) & (iter->tag),
										 sizeof(BufferTag));

		LWLockRelease(LOG_INDEX_BLOOM_LRU_LOCK);

		if (!not_exists)
		{
			table = log_index_read_table(logindex_snapshot, tid);

			if (!table)
				elog(PANIC, "Failed to read table which id is %ld", tid);

			log_index_push_tbl_lsn(iter, table);
		}

		tid--;

		CHECK_FOR_INTERRUPTS();
	}
}

log_index_page_iter_t
polar_log_index_create_page_iterator(log_index_snapshot_t *logindex_snapshot, BufferTag *tag, XLogRecPtr min_lsn, XLogRecPtr max_lsn)
{
	log_idx_table_id_t        tid;
	log_index_page_iter_t iter = NULL;
	MemoryContext         oldcontext = MemoryContextSwitchTo(logindex_snapshot->mem_cxt);

	/* Large enough to save lsn which comes from one table */
	page_iter = iter = palloc0(sizeof(log_index_page_iter_data_t));

	memcpy(&(iter->tag), tag, sizeof(BufferTag));
	iter->max_lsn = max_lsn;
	iter->min_lsn = min_lsn;
	iter->iter_prev_lsn = InvalidXLogRecPtr;
	iter->iter_max_lsn = max_lsn;
	iter->lsn_stack.head = NULL;
	iter->key = LOG_INDEX_MEM_TBL_HASH_PAGE(tag);
	iter->state = ITERATE_STATE_FORWARD;
	iter->lsn_info.tag = &iter->tag;

	tid = log_index_push_mem_tbl_lsn(logindex_snapshot, iter);

	if (iter->state == ITERATE_STATE_FORWARD && !LOG_INDEX_ONLY_IN_MEM)
		log_index_push_file_tbl_lsn(logindex_snapshot, iter, tid);

	if (iter->state == ITERATE_STATE_FORWARD)
		iter->state = ITERATE_STATE_FINISHED;

	Assert(iter->state != ITERATE_STATE_CORRUPTED);

	MemoryContextSwitchTo(oldcontext);
	return iter;
}

void
polar_log_index_release_page_iterator(log_index_page_iter_t iter)
{
	if (iter == page_iter)
		page_iter = NULL;

	log_index_stack_release(&iter->lsn_stack);
	pfree(iter);
}

void
polar_log_index_abort_page_iterator(void)
{
	if (page_iter != NULL)
		polar_log_index_release_page_iterator(page_iter);
}

log_index_lsn_t *
polar_log_index_page_iterator_next(log_index_page_iter_t iter)
{
	log_index_page_stack_lsn_t *stack = &(iter->lsn_stack);
	log_index_lsn_t *lsn_info = &iter->lsn_info;
	bool got = log_index_tbl_stack_pop_lsn(stack->head, lsn_info);

	while (!got && stack->head != NULL)
	{
		log_index_pop_tbl_stack(stack);
		got = log_index_tbl_stack_pop_lsn(stack->head, lsn_info);
	}

	return got ? lsn_info : NULL;
}

bool
polar_log_index_page_iterator_end(log_index_page_iter_t iter)
{
	return iter->lsn_stack.head == NULL;
}

static void
log_index_set_search_table(log_index_lsn_iter_t iter, log_idx_table_data_t *table)
{
	log_idx_table_id_t tid = table->idx_table_id;

	if (iter->start_lsn > table->max_lsn)
	{
		/* If the first table search, */
		if (iter->last_search_tid == LOG_INDEX_TABLE_INVALID_ID)
			iter->state = ITERATE_STATE_FINISHED;
		else
		{
			iter->idx_table_id = table->idx_table_id + 1;
			iter->state = ITERATE_STATE_BACKWARD;
		}
	}
	else if (iter->start_lsn >= table->min_lsn && iter->start_lsn <= table->max_lsn)
	{
		iter->idx_table_id = table->idx_table_id;

		if (iter->start_lsn != table->min_lsn)
			iter->state = ITERATE_STATE_BACKWARD;
	}

	iter->last_search_tid = tid;
}

XLogRecPtr
log_index_get_order_lsn(log_idx_table_data_t *table, uint32 order, log_index_lsn_t *lsn_info)
{
	log_seg_id_t    seg_id;
	uint8           idx;
	log_item_seg_t  *seg;
	log_item_head_t *head;
	XLogRecPtr      lsn;

	if (order >= table->last_order)
		return InvalidXLogRecPtr;

	/*
	 * The valid order value start from 1 and the array index start from 0
	 */
	Assert(order >= 0);

	seg_id = LOG_INDEX_SEG_ORDER(table->idx_order[order]);
	idx = LOG_INDEX_ID_ORDER(table->idx_order[order]);
	seg = LOG_INDEX_ITEM_SEG(table, seg_id);

	Assert(seg != NULL);

	if (seg->head_seg == seg_id)
	{
		head = LOG_INDEX_ITEM_HEAD(table, seg_id);
		Assert(idx < head->number);
		lsn = LOG_INDEX_COMBINE_LSN(table, head->suffix_lsn[idx]);

		if (lsn_info != NULL)
			LOG_INDEX_COPY_LSN_INFO(lsn_info, table, head, idx);
	}
	else
	{
		Assert(idx < seg->number);
		lsn = LOG_INDEX_COMBINE_LSN(table, seg->suffix_lsn[idx]);

		if (lsn_info != NULL)
			LOG_INDEX_COPY_LSN_INFO(lsn_info, table, seg, idx);
	}

	if (lsn_info != NULL)
	{
		lsn_info->prev_lsn = log_index_get_seg_prev_lsn(table, seg_id, idx);
		memcpy(lsn_info->tag, log_index_get_seg_page_tag(table, seg_id),
			   sizeof(BufferTag));
	}

	return lsn;
}

static void
log_index_set_search_start_order(log_index_lsn_iter_t iter, log_idx_table_data_t *table)
{
	uint32      order = 0;

	/*
	 * When check overlap and find no overlapping data, the iterator's mem_table_id will
	 * not be changed, so no need to calc the start order for this table
	 */
	if (table->idx_table_id != iter->idx_table_id)
		return;

	/*
	 * The last_order points to next index to save data. And saved data length is last_order - 1
	 */
	while (order < table->last_order)
	{
		XLogRecPtr lsn = log_index_get_order_lsn(table, order, NULL);

		Assert(lsn != InvalidXLogRecPtr);

		if (lsn >= iter->start_lsn)
		{
			iter->idx = order;
			break;
		}

		order++;
	}
}

static void
log_index_set_start_order(log_index_snapshot_t *logindex_snapshot, log_index_lsn_iter_t iter)
{
	log_mem_table_t *table = NULL;
	log_idx_table_data_t *table_data;

	if (iter->state == ITERATE_STATE_BACKWARD)
	{
		if ((table_data = log_index_get_table_data(logindex_snapshot, iter, &table)) != NULL)
			log_index_set_search_start_order(iter, table_data);

		if (table != NULL)
			LWLockRelease(LOG_INDEX_MEM_TBL_LOCK(table));
	}
}

static void
log_index_search_mem_tbl_lsn(log_index_snapshot_t *logindex_snapshot, log_index_lsn_iter_t iter)
{
	uint32 mid, first_mid;
	log_mem_table_t *table;
	log_idx_table_id_t  tid = LOG_INDEX_TABLE_INVALID_ID;
	bool search_file = false;

	SpinLockAcquire(LOG_INDEX_SNAPSHOT_LOCK);
	mid = LOG_INDEX_MEM_TBL_ACTIVE_ID;
	SpinLockRelease(LOG_INDEX_SNAPSHOT_LOCK);

	first_mid = mid;

	do
	{
		uint32 state;
		LWLock *table_lock;

		table = LOG_INDEX_MEM_TBL(mid);
		table_lock = LOG_INDEX_MEM_TBL_LOCK(table);

		LWLockAcquire(table_lock, LW_SHARED);
		state = LOG_INDEX_MEM_TBL_STATE(table);

		/* When first table is an active table, it maybe free or new table */
		if ((state == LOG_INDEX_MEM_TBL_STATE_FREE ||
				LOG_INDEX_MEM_TBL_IS_NEW(table)) && mid == first_mid)
		{
			LWLockRelease(table_lock);
			mid = LOG_INDEX_MEM_TBL_PREV_ID(mid);
			continue;
		}

		if ((tid == LOG_INDEX_TABLE_INVALID_ID || tid > LOG_INDEX_MEM_TBL_TID(table))
				&& state != LOG_INDEX_MEM_TBL_STATE_FREE)
		{
			if (state == LOG_INDEX_MEM_TBL_STATE_ACTIVE)
			{
				SpinLockAcquire(LOG_INDEX_SNAPSHOT_LOCK);

				log_index_set_search_table(iter, &table->data);

				SpinLockRelease(LOG_INDEX_SNAPSHOT_LOCK);
			}
			else
				log_index_set_search_table(iter, &table->data);

			if (iter->state == ITERATE_STATE_FORWARD)
				tid = LOG_INDEX_MEM_TBL_TID(table);
		}
		else
			search_file = true;

		LWLockRelease(table_lock);

		mid = LOG_INDEX_MEM_TBL_PREV_ID(mid);
	}
	while (mid != first_mid && iter->state == ITERATE_STATE_FORWARD && !search_file);

	log_index_set_start_order(logindex_snapshot, iter);
}

static void
log_index_search_file_tbl_lsn(log_index_snapshot_t *logindex_snapshot, log_index_lsn_iter_t iter)
{
	log_idx_table_id_t tid;
	log_index_meta_t meta;
	bool            hollow;
	log_index_file_segment_t  *min_seg = &meta.min_segment_info;

	LOG_INDEX_COPY_META(&meta);

	if (iter->start_lsn > meta.max_lsn)
	{
		/* If the first table search, */
		if (iter->last_search_tid == LOG_INDEX_TABLE_INVALID_ID)
			iter->state = ITERATE_STATE_FINISHED;
		else
		{
			iter->idx_table_id = iter->last_search_tid;
			iter->state = ITERATE_STATE_BACKWARD;
		}

		return ;
	}

	tid = log_index_next_search_file_tid(iter->last_search_tid, &meta, &hollow);

	if (hollow)
	{
		iter->state = ITERATE_STATE_HOLLOW;
		elog(ERROR, "Failed to search next tid for lsn iter, and prev tid is %ld", iter->last_search_tid);
		return ;
	}

	while (tid != LOG_INDEX_TABLE_INVALID_ID
			&& tid >= min_seg->min_idx_table_id && iter->state == ITERATE_STATE_FORWARD)
	{
		log_idx_table_data_t *table = log_index_read_table(logindex_snapshot, tid);

		if (!table)
			elog(PANIC, "Failed to read table which id is %ld", tid);

		log_index_set_search_table(iter, table);

		tid--;
	}

	if (iter->state == ITERATE_STATE_FORWARD)
	{
		iter->idx_table_id = min_seg->min_idx_table_id;
		iter->state = ITERATE_STATE_BACKWARD;
	}

	log_index_set_start_order(logindex_snapshot, iter);
}


log_index_lsn_iter_t
polar_log_index_create_lsn_iterator(log_index_snapshot_t *logindex_snapshot, XLogRecPtr lsn)
{
	MemoryContext oldcontext = MemoryContextSwitchTo(logindex_snapshot->mem_cxt);
	log_index_lsn_iter_t iter = palloc0(sizeof(log_index_lsn_iter_data_t));

	iter->lsn_info.tag = &iter->tag;
	iter->start_lsn = lsn;

	/*
	 * The valid table id start from 1 and if the iterator start from InvalidXLogRecPtr
	 * then we need to return data from first table
	 */
	if (lsn == InvalidXLogRecPtr)
	{
		log_index_meta_t meta;
		log_index_file_segment_t  *min_seg = &meta.min_segment_info;

		LOG_INDEX_COPY_META(&meta);

		if (min_seg->min_idx_table_id > 1)
			ereport(PANIC,
					(errmsg("Incorrect lsn to create log index lsn iterator")));

		iter->idx_table_id = 1;
	}

	MemoryContextSwitchTo(oldcontext);
	return iter;
}

static log_idx_table_data_t *
log_index_get_table_data(log_index_snapshot_t *logindex_snapshot, log_index_lsn_iter_t iter, log_mem_table_t **r_table)
{
	uint32 mid;
	log_mem_table_t *table;
	log_index_meta_t meta;
	log_idx_table_data_t *table_data;

	table_data = LOG_INDEX_GET_CACHE_TABLE(&logindex_table_cache[logindex_snapshot->type], iter->idx_table_id);

	if (table_data != NULL)
		return table_data;

	/*
	 * The valid table id start from 1.
	 * And the ring memory table array index start from 0
	 */
	mid = (iter->idx_table_id - 1) % logindex_snapshot->mem_tbl_size;
	table = LOG_INDEX_MEM_TBL(mid);
	LWLockAcquire(LOG_INDEX_MEM_TBL_LOCK(table), LW_SHARED);

	if (LOG_INDEX_MEM_TBL_TID(table) == iter->idx_table_id)
	{
		*r_table = table;
		return &table->data;
	}

	LWLockRelease(LOG_INDEX_MEM_TBL_LOCK(table));

	if (LOG_INDEX_ONLY_IN_MEM)
		return NULL;

	LOG_INDEX_COPY_META(&meta);

	if (iter->idx_table_id > meta.max_idx_table_id)
		return NULL;

	table_data = log_index_read_table(logindex_snapshot, iter->idx_table_id);

	if (!table_data)
	{
		iter->state = ITERATE_STATE_HOLLOW;
		elog(ERROR, "Failed to read table %ld, and there is hollow", iter->idx_table_id);
	}

	return table_data;
}

static void
log_index_lsn_iterator_update(log_index_lsn_iter_t iter, log_mem_table_t *table)
{
	if (table == NULL ||
			LOG_INDEX_MEM_TBL_STATE(table) != LOG_INDEX_MEM_TBL_STATE_ACTIVE)
	{
		iter->idx = 0;
		iter->idx_table_id++;
	}
}

log_index_lsn_t *
polar_log_index_lsn_iterator_next(log_index_snapshot_t *logindex_snapshot, log_index_lsn_iter_t iter)
{
	log_idx_table_data_t *table_data;
	log_mem_table_t  *table = NULL;
	log_index_lsn_t *lsn_info = NULL;
	MemoryContext   oldcontext = MemoryContextSwitchTo(logindex_snapshot->mem_cxt);

	if (iter->idx_table_id == LOG_INDEX_TABLE_INVALID_ID)
	{
		iter->state = ITERATE_STATE_FORWARD;
		iter->last_search_tid = LOG_INDEX_TABLE_INVALID_ID;
		log_index_search_mem_tbl_lsn(logindex_snapshot, iter);
	}

	if (iter->state == ITERATE_STATE_FORWARD)
	{
		if (!LOG_INDEX_ONLY_IN_MEM)
			log_index_search_file_tbl_lsn(logindex_snapshot, iter);
	}

	if (iter->state != ITERATE_STATE_BACKWARD)
	{
		MemoryContextSwitchTo(oldcontext);
		return NULL;
	}

	if ((table_data = log_index_get_table_data(logindex_snapshot, iter, &table)) != NULL)
	{
		/*
		 * If table state is ACTIVE and idx equals last_order return InvalidXLogRecPtr
		 */
		if (log_index_get_order_lsn(table_data, iter->idx, &iter->lsn_info)
				!= InvalidXLogRecPtr)
		{
			lsn_info = &iter->lsn_info;
			iter->idx++;
		}

		if (iter->idx == table_data->last_order)
			log_index_lsn_iterator_update(iter, table);
	}

	if (table != NULL)
		LWLockRelease(LOG_INDEX_MEM_TBL_LOCK(table));

	MemoryContextSwitchTo(oldcontext);
	return lsn_info;
}

void
polar_log_index_release_lsn_iterator(log_index_lsn_iter_t iter)
{
	pfree(iter);
}
