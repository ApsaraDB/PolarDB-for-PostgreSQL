/*-------------------------------------------------------------------------
 *
 * polar_bufmgr.h
 *	polardb buffer manager definitions.
 *
 * Copyright (c) 2018, Alibaba Group Holding Limited
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
 *	src/include/storage/polar_bufmgr.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef POLAR_BUFMGR_H
#define POLAR_BUFMGR_H

#include "postgres.h"

#include "storage/buf.h"
#include "storage/bufpage.h"

typedef struct BufferDesc BufferDesc;
typedef struct WritebackContext WritebackContext;

#define polar_buffer_get_oldest_lsn(buf_hdr) \
	(pg_atomic_read_u64((pg_atomic_uint64 *) &buf_hdr->oldest_lsn))

#define polar_buffer_first_touch_after_copy(buf_hdr) \
	(buf_hdr->copy_buffer && !(buf_hdr->polar_flags & POLAR_BUF_FIRST_TOUCHED_AFTER_COPY))

#define polar_lazy_checkpoint_enabled() \
	(polar_enable_shared_storage_mode && polar_enable_lazy_checkpoint && \
	 (!fullPageWrites) && (!RecoveryInProgress()))

/* Unconditionally set the oldest lsn. */
#define polar_buffer_set_oldest_lsn(bufHdr,lsn) \
	(pg_atomic_write_u64((pg_atomic_uint64 *) &((bufHdr)->oldest_lsn), (lsn)))

#define polar_buffer_set_fake_oldest_lsn(bufHdr) \
	(polar_buffer_set_oldest_lsn(bufHdr, GetXLogInsertRecPtr()))

extern void polar_redo_set_buffer_oldest_lsn(Buffer buffer, XLogRecPtr lsn);
extern void polar_set_buffer_fake_oldest_lsn(BufferDesc *buf_hdr);
extern void polar_reset_buffer_oldest_lsn(BufferDesc *buf_hdr);
extern bool polar_buffer_can_be_flushed(BufferDesc *buf_hdr, XLogRecPtr oldest_apply_lsn, bool use_cbuf);
extern bool polar_buffer_can_be_flushed_by_checkpoint(BufferDesc *buf_hdr, XLogRecPtr oldest_apply_lsn, int flags);
extern bool polar_bg_buffer_sync(WritebackContext *wb_context, int flags);
extern bool polar_donot_control_vm_buffer(BufferDesc *buf_hdr);
extern bool polar_buffer_sync(WritebackContext *wb_context, XLogRecPtr *consistent_lsn, bool is_normal_bgwriter, int flags);
extern void polar_set_reg_buffer_oldest_lsn(Buffer buffer);
extern bool polar_check_lazy_checkpoint(bool shutdown, int *flags, XLogRecPtr *lazy_redo);
extern bool polar_pin_buffer(BufferDesc *buf, BufferAccessStrategy strategy);
extern bool polar_is_future_page(BufferDesc *buf_hdr);
extern bool polar_buffer_need_fullpage_snapshot(BufferDesc *buf_hdr, XLogRecPtr oldest_apply_lsn);

#endif							/* POLAR_BUFMGR_H */
