/*-------------------------------------------------------------------------
 *
 * polar_log.h
 *   Log data structure information
 *   
 * Copyright (c) 2019, Alibaba Group Holding Limited
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
 * limitations under the License.*
 * 
 * IDENTIFICATION
 *    src/include/access/polar_log.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef POLAR_LOG_H
#define POLAR_LOG_H

#include "storage/buf_internals.h"
#include "storage/polar_copybuf.h"
#include "storage/polar_flushlist.h"

#define POLAR_LOG_BUFFER_TAG_FORMAT "'[%u, %u, %u], %u, %u'"
#define POLAR_LOG_BUFFER_TAG(tag__) (tag__)->rnode.spcNode,(tag__)->rnode.dbNode,(tag__)->rnode.relNode,(tag__)->forkNum,(tag__)->blockNum

/* Log information of BufferTag */
#define POLAR_LOG_BUFFER_TAG_INFO(tag) \
	do { \
		ereport(LOG, (errmsg("%s buffer tag info: page_tag=" POLAR_LOG_BUFFER_TAG_FORMAT, \
							 __func__,  POLAR_LOG_BUFFER_TAG(tag)), \
					  errhidestmt(true), errhidecontext(true))); \
	} while (0)

/* Log information of CopyBUfferDesc */
#define POLAR_LOG_COPY_BUFFER_DESC(copy_buf) \
	do { \
		POLAR_LOG_BUFFER_TAG_INFO(&((copy_buf)->tag)); \
		ereport(LOG, (errmsg("%s copy buffer desc: buf_id=%d, oldest_lsn=%lX, origin_buffer=%u, state=%x, pass_count=%u, is_flushed=%d", \
							 __func__, (copy_buf)->buf_id, (copy_buf)->oldest_lsn, (copy_buf)->origin_buffer, \
							 (copy_buf)->state, pg_atomic_read_u32(&(copy_buf)->pass_count), (copy_buf)->is_flushed), \
					  errhidestmt(true), errhidecontext(true))); \
	} while (0)

/* Log information of BufferDesc */
#define POLAR_LOG_BUFFER_DESC(buf) \
	do { \
		Page page = BufferGetPage(BufferDescriptorGetBuffer(buf)); \
		ereport(LOG, (errmsg("%s buf=%d " POLAR_LOG_BUFFER_TAG_FORMAT " desc: state=%x, o_lsn=%lX, p_flags=%u, m_count=%u, p_lsn=%lX", \
							 __func__, (buf)->buf_id, POLAR_LOG_BUFFER_TAG(&((buf)->tag)), pg_atomic_read_u32(&((buf)->state)), \
							 (buf)->oldest_lsn, (buf)->polar_flags, (buf)->recently_modified_count, PageGetLSN(page)), \
					  errhidestmt(true), errhidecontext(true))); \
		if ((buf)->copy_buffer) \
			POLAR_LOG_COPY_BUFFER_DESC((buf)->copy_buffer); \
	} while (0)

/* Log information of BufferDesc and FlushList */
#define POLAR_LOG_BUFFER_DESC_WITH_FLUSHLIST(buf) \
    do { \
        elog(LOG, "%s buffer tag info: page_tag='[%u, %u, %u], %d, %u', state=%u, oldest_lsn=%lX, polar_flags=%u, flushlist size: %d, flush_prev: %d, flush_next: %d", \
             __func__, (buf)->tag.rnode.spcNode, (buf)->tag.rnode.dbNode, (buf)->tag.rnode.relNode, \
             (buf)->tag.forkNum, (buf)->tag.blockNum, pg_atomic_read_u32(&((buf)->state)), \
             (buf)->oldest_lsn, (buf)->polar_flags, FLUSH_LIST_LEN, (buf)->flush_prev, (buf)->flush_next); \
    } while (0)

#endif
