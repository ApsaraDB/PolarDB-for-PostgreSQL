/*-------------------------------------------------------------------------
 *
 * polar_xlogbuf.h
 *	  xlog buffer manager data types.
 *
 *
 * Copyright (c) 2018, Alibaba inc.
 *
 * src/include/storage/polar_xlogbuf.h
 *
 * Xlog buffer will reserve recent xlog pages in memory to reduce io overhead.
 * When lookup a xlog page, if buffer matched or current buffer is olaer than 
 * requested page, we will return buffer id and hold content lock of buffer.
 * Caller should release content lock after read or update buffer.
 *
 * For now, replace strategy is to reserve newer xlog pages than old ones. So
 * in polar_xlog_buffer_should_evict() we will check lsn from buffer meta and 
 * requested page, if requested page is newer we will evict current buffer, 
 * otherwise requested page should be obtained by io directly.
 *
 *-------------------------------------------------------------------------
 */

#ifndef POLAR_XLOGBUF_H
#define POLAR_XLOGBUF_H

#include "access/xlogdefs.h"
#include "storage/buf.h"
#include "storage/lwlock.h"
#include "utils/guc.h"

/*
 * POLAR: polar_enable_master_xlog_read_ahead default set off, master will not ahead read xlog.
 * replica and standby always ahead read xlog.
 */
#define POLAR_ENABLE_XLOG_BUFFER() \
	(polar_enable_shared_storage_mode && polar_enable_xlog_buffer && \
	 (polar_enable_master_xlog_read_ahead || InHotStandby || polar_in_replica_mode()))

#define polar_get_xlog_buffer_id(lsn) (((lsn) / XLOG_BLCKSZ) % (polar_xlog_page_buffers * 1024 / (XLOG_BLCKSZ / 1024)))
#define polar_get_xlog_buffer_desc(buf_id) (&polar_xlog_buffer_descriptors[(buf_id)].desc)
#define polar_get_xlog_buffer(buf_id) ((char *)(polar_xlog_buffers + ((Size)(buf_id)) * XLOG_BLCKSZ))
#define polar_xlog_buffer_desc_get_lock(buf) ((LWLock*)&polar_xlog_buffer_lock_array[(buf)->buf_id].lock)

#define polar_xlog_offset_aligned(lsn) ((lsn) % XLOG_BLCKSZ == 0)

#define XLOGBUFFERDESC_PAD_TO_SIZE	(SIZEOF_VOID_P == 8 ? 64 : 1)

typedef struct XLogBufferDesc
{
	int		buf_id; /* buffer index */
	XLogRecPtr	start_lsn;
	XLogRecPtr	end_lsn;
} XLogBufferDesc;

typedef union XLogBufferDescPadded
{
	XLogBufferDesc	desc;
	char		pad[XLOGBUFFERDESC_PAD_TO_SIZE];
} XLogBufferDescPadded;

/* in polar_xlogbuf.c */
extern XLogBufferDescPadded 	*polar_xlog_buffer_descriptors;
extern char 			*polar_xlog_buffers;
extern LWLockMinimallyPadded 	*polar_xlog_buffer_lock_array;

extern Size polar_xlog_buffer_shmem_size(void);
extern void polar_init_xlog_buffer(void);

extern void polar_xlog_buffer_lock(int buf_id, LWLockMode mode);
extern void polar_xlog_buffer_unlock(int buf_id);

extern bool polar_xlog_buffer_lookup(XLogRecPtr start_lsn, int len, bool doEvict, bool doCount, int *buf_id);
extern void polar_xlog_buffer_update(XLogRecPtr lsn);
extern void polar_xlog_buffer_remove(XLogRecPtr lsn);

/* internal begin */
extern bool polar_xlog_buffer_should_evict(XLogBufferDesc *desc, XLogRecPtr lsn, int len);
/* internal end */

#endif /* POLAR_XLOGBUF_H */
