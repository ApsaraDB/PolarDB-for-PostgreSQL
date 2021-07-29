#ifndef POLAR_QUEUE_MANAGER_H
#define POLAR_QUEUE_MANAGER_H

#include "access/polar_ringbuf.h"
#include "access/xlogdefs.h"
#include "access/xlogreader.h"
#include "access/xlog_internal.h"

typedef struct log_index_snapshot_t		*logindex_snapshot_t;

extern polar_ringbuf_t  polar_xlog_queue;

extern int           polar_xlog_queue_buffers;

#define POLAR_XLOG_QUEUE_ENABLE() \
	(polar_streaming_xlog_meta && !IsBootstrapProcessingMode())

#define POLAR_XLOG_HEAD_SIZE (sizeof(XLogRecPtr) + sizeof(uint32))
#define POLAR_XLOG_PKT_SIZE(len) POLAR_RINGBUF_PKT_SIZE((len) + POLAR_XLOG_HEAD_SIZE)

#define POLAR_XLOG_QUEUE_NEW_REF(ref, strong, name) \
	polar_ringbuf_new_ref(polar_xlog_queue, strong, (ref), name)

#define POLAR_XLOG_QUEUE_FREE_SIZE(size) \
	(polar_ringbuf_free_size(polar_xlog_queue) >= POLAR_XLOG_PKT_SIZE(size))

#define POLAR_XLOG_QUEUE_FREE_UP(len) \
	polar_ringbuf_free_up(polar_xlog_queue, POLAR_XLOG_PKT_SIZE(len), NULL)

#define POLAR_XLOG_QUEUE_SET_PKT_LEN(idx, size) \
	polar_ringbuf_set_pkt_length(polar_xlog_queue, (idx), (size) + POLAR_XLOG_HEAD_SIZE)

#define POLAR_XLOG_QUEUE_RESERVE(size) \
	polar_ringbuf_pkt_reserve(polar_xlog_queue, POLAR_XLOG_PKT_SIZE(size))

extern Size polar_xlog_queue_size(void);
extern void polar_xlog_queue_init(void);
extern bool polar_xlog_send_queue_push(size_t rbuf_pos, struct XLogRecData *rdata, int copy_len,
									   XLogRecPtr end_lsn, uint32 xlog_len);
extern void polar_standby_xlog_send_queue_push(XLogReaderState *xlogreader);
extern Size polar_xlog_reserve_size(XLogRecData *rdata);

extern ssize_t polar_xlog_send_queue_pop(polar_ringbuf_ref_t *ref,  uint8 *data, size_t size, XLogRecPtr *max_lsn);

extern void polar_xlog_send_queue_save(polar_ringbuf_ref_t *ref);
extern void polar_xlog_send_queue_keep_data(polar_ringbuf_ref_t *ref);
extern bool polar_xlog_send_queue_check(polar_ringbuf_ref_t *ref, XLogRecPtr start_point);

extern bool polar_xlog_recv_queue_push(char *buf, size_t len, polar_interrupt_callback callback);
extern XLogRecord *polar_fullpage_bgworker_xlog_recv_queue_pop(logindex_snapshot_t logindex_snapshot, XLogReaderState *state, char **errormsg);
extern XLogRecord *polar_xlog_recv_queue_pop(XLogReaderState *state, XLogRecPtr RecPtr, char **errormsg);
extern void polar_xlog_decode_data(XLogReaderState *state);
extern void polar_xlog_recv_queue_push_storage_begin(polar_interrupt_callback callback);

extern XLogRecPtr polar_xlog_send_queue_next_lsn(polar_ringbuf_ref_t *ref, size_t *len);

extern bool polar_xlog_remove_payload(XLogRecord *record);
extern bool polar_xlog_queue_decode(XLogReaderState *state, XLogRecord *record, bool decode_payload, char **errormsg);
#endif

