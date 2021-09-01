/*-------------------------------------------------------------------------
 *
 * polar_brin_generic_idx.c
 *    WAL redo parse logic for generic index.
 *
 *
 * Copyright (c) 2020, Alibaba Group Holding Limited
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
 *           src/backend/access/logindex/polar_generic_xlog_idx.c
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/bufmask.h"
#include "access/generic_xlog.h"
#include "access/polar_logindex.h"
#include "access/polar_logindex_internal.h"
#include "access/xlogutils.h"
#include "access/xlog_internal.h"
#include "storage/buf_internals.h"

void
polar_generic_idx_save(XLogReaderState *record)
{
	int			block_id;

	for (block_id = 0; block_id <= MAX_GENERIC_XLOG_PAGES; block_id++)
	{
		if (XLogRecHasBlockRef(record, block_id))
			polar_log_index_save_block(record, block_id);
	}
}

bool
polar_generic_idx_parse(XLogReaderState *record)
{
	int			block_id;
	Buffer		buffers[MAX_GENERIC_XLOG_PAGES];
	polar_page_lock_t page_lock[MAX_GENERIC_XLOG_PAGES];

	/* Protect limited size of buffers[] array */
	Assert(record->max_block_id < MAX_GENERIC_XLOG_PAGES);

	for (block_id = 0; block_id <= record->max_block_id; block_id++)
	{
		BufferTag	tag;

		if (!XLogRecHasBlockRef(record, block_id))
		{
			buffers[block_id] = InvalidBuffer;
			page_lock[block_id] = 0;
			continue;
		}

		POLAR_MINI_TRANS_REDO_PARSE(record, block_id, tag, page_lock[block_id], buffers[block_id]);
	}

	for (block_id = 0; block_id <= record->max_block_id; block_id++)
	{
		if (XLogRecHasBlockRef(record, block_id))
		{
			if (BufferIsValid(buffers[block_id]))
				UnlockReleaseBuffer(buffers[block_id]);

			polar_log_index_mini_trans_unlock(page_lock[block_id]);
		}
	}

	return true;
}

XLogRedoAction
polar_generic_idx_redo(XLogReaderState *record, BufferTag *tag, Buffer *buffer)
{
	int			block_id;
	XLogRedoAction action = BLK_NOTFOUND;
	XLogRecPtr	lsn = record->EndRecPtr;

	/* Protect limited size of buffers[] array */
	Assert(record->max_block_id < MAX_GENERIC_XLOG_PAGES);

	for (block_id = 0; block_id <= record->max_block_id; block_id++)
	{
		BufferTag	page_tag;

		if (!XLogRecHasBlockRef(record, block_id))
			continue;

		POLAR_GET_LOG_TAG(record, page_tag, block_id);

		if (BUFFERTAGS_EQUAL(*tag, page_tag))
		{
			action = POLAR_READ_BUFFER_FOR_REDO(record, block_id, buffer);

			/* Apply redo to given block if needed */
			if (action == BLK_NEEDS_REDO)
			{
				Page		page;
				PageHeader	pageHeader;
				char	   *blockDelta;
				Size		blockDeltaSize;

				page = BufferGetPage(*buffer);
				blockDelta = XLogRecGetBlockData(record, block_id, &blockDeltaSize);
				applyPageRedo(page, blockDelta, blockDeltaSize);

				/*
				 * Since the delta contains no information about what's in the
				 * "hole" between pd_lower and pd_upper, set that to zero to
				 * ensure we produce the same page state that application of
				 * the logged action by GenericXLogFinish did.
				 */
				pageHeader = (PageHeader) page;
				memset(page + pageHeader->pd_lower, 0,
					   pageHeader->pd_upper - pageHeader->pd_lower);

				PageSetLSN(page, lsn);
			}

			break;
		}
	}

	return action;
}
