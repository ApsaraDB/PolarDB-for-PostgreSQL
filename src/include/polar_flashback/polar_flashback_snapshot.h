/*-------------------------------------------------------------------------
 *
 * polar_flashback_snapshot.h
 *
 *
 * Copyright (c) 2021, Alibaba Group Holding limited
 *
 * src/include/polar_flashback/polar_flashback_snapshot.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef POLAR_FLASHBACK_SNAPSHOT_H
#define POLAR_FLASHBACK_SNAPSHOT_H

#include "access/htup.h"
#include "polar_flashback/polar_fast_recovery_area.h"
#include "utils/pg_crc.h"
#include "utils/tqual.h"

#define FLSHBAK_SNAPSHOT_DATA_VERSION (1)

#define POLAR_GET_FLSHBAK_SNAPSHOT_XIP(snapshot) ((char *) (snapshot) + FLSHBAK_SNAPSHOT_T_SIZE)

/* We will enlarge the xip size 256 one time */
#define ENLARGE_XIP_SIZE_ONCE (256)
#define GET_ENLARGE_XIP_SIZE(total_xcnt) ((total_xcnt) + ENLARGE_XIP_SIZE_ONCE)

typedef struct flashback_snapshot_data_t
{
	XLogRecPtr  lsn; /* The xlog insert lsn */
	TransactionId xmin;
	TransactionId xmax;
	uint32      xcnt;
	uint32  next_clog_subdir_no; /* The next clog subdir no. in fast recovery area */
	TransactionId next_xid; /* The next xid, must be the last */
} flashback_snapshot_data_t;

typedef flashback_snapshot_data_t *flashback_snapshot_t;

#define FLSHBAK_SNAPSHOT_T_SIZE (offsetof(flashback_snapshot_data_t, next_xid) + sizeof(TransactionId))

typedef struct flashback_snapshot_data_header_t
{
	uint32 info; /* high 24 bits is the end offset, low 8 bit is the version */
	uint32  data_size; /* Maybe the data size is over UINT_MAX, we think the database will be broken in this case */
	pg_crc32 crc; /* Must be the last */
} flashback_snapshot_data_header_t;

#define FLSHBAK_SNAPSHOT_HEADER_SIZE (offsetof(flashback_snapshot_data_header_t, crc) + sizeof(pg_crc32))

typedef flashback_snapshot_data_header_t *flashback_snapshot_header_t;

#define FLSHBAK_SNAPSHOT_VERSION_SHIFT 24
#define FLSHBAK_SNAPSHOT_VERSION_MASK 0xFFFFFF00
#define FLSHBAK_SNAPSHOT_END_POS_SHIFT 8
#define FLSHBAK_SNAPSHOT_END_POS_MASK 0x0FF

#define SET_FLSHBAK_SNAPSHOT_END_POS(info, end_pos) ((info) = (((uint64) (end_pos) << FLSHBAK_SNAPSHOT_END_POS_SHIFT) | ((info) & FLSHBAK_SNAPSHOT_END_POS_MASK)))
#define SET_FLSHBAK_SNAPSHOT_VERSION(info) ((info) = ((info) & FLSHBAK_SNAPSHOT_VERSION_MASK) | (FLSHBAK_SNAPSHOT_DATA_VERSION))

#define GET_FLSHBAK_SNAPSHOT_END_POS(info) ((info) >> FLSHBAK_SNAPSHOT_END_POS_SHIFT)
#define GET_FLSHBAK_SNAPSHOT_VERSION(info) ((uint8) (info))

typedef struct flashback_rel_snapshot_t
{
	Snapshot snapshot;
	uint32 *removed_xid_pos; /* The removed xid position in the xip */
	uint32 xip_size; /* The xid count has been allocated, it is large than xcnt */
	uint32  removed_size; /* The removed xid count */
	uint32  next_clog_subdir_no; /* The next clog subdir no. in fast recovery area */
	TransactionId next_xid; /* The next xid */
} flashback_rel_snapshot_t;

typedef flashback_rel_snapshot_t *flshbak_rel_snpsht_t;

#define FLSHBAK_SNAPSHOT_DATA_BASE_SIZE (FLSHBAK_SNAPSHOT_HEADER_SIZE + FLSHBAK_SNAPSHOT_T_SIZE)
#define FLSHBAK_GET_SNAPSHOT_DATA_SIZE(xcnt) (polar_get_snapshot_size(FLSHBAK_SNAPSHOT_T_SIZE, xcnt))
#define FLSHBAK_GET_SNAPSHOT_TOTAL_SIZE(xcnt) (polar_get_snapshot_size(FLSHBAK_SNAPSHOT_DATA_BASE_SIZE, xcnt))

#define FLSHBAK_GET_SNAPSHOT_DATA(header) ((flashback_snapshot_t) ((char *) header + FLSHBAK_SNAPSHOT_HEADER_SIZE))

#define MAX_KEEP_REMOVED_XIDS (128)

#define REMOVED_XID_POS_SIZE (MAX_KEEP_REMOVED_XIDS * sizeof(uint32))

extern void log_flashback_snapshot(Snapshot snapshot, int elevel);
extern Size polar_get_snapshot_size(Size size, uint32 xcnt);

extern flashback_snapshot_header_t polar_get_flashback_snapshot_data(fra_ctl_t ins, XLogRecPtr lsn);
extern fbpoint_pos_t polar_backup_snapshot_to_fra(flashback_snapshot_header_t header, fbpoint_pos_t *snapshot_end_pos, const char *fra_dir);
extern Snapshot polar_get_flashback_snapshot(const char *fra_dir, fbpoint_pos_t start_pos, uint32 *next_clog_subdir_no, TransactionId *next_xid);

extern void polar_update_flashback_snapshot(flshbak_rel_snpsht_t rsnapshot, TransactionId xid);

extern HTSV_Result polar_tuple_satisfies_flashback(HeapTuple htup, Buffer buffer, Snapshot snapshot, uint32 next_clog_subdir_no, TransactionId max_xid, const char *fra_dir);
extern bool polar_flashback_xact_redo(XLogRecord *record, flshbak_rel_snpsht_t rsnapshot, TimestampTz end_time, XLogReaderState *xlogreader);

extern void polar_free_snapshot_data(Snapshot snapshot);
extern void polar_compact_xip(flshbak_rel_snpsht_t rsnapshot);

extern void polar_fra_do_fbpoint(fra_ctl_t ctl, fbpoint_wal_info_data_t *wal_info,
								 polar_flog_rec_ptr *keep_ptr, flashback_snapshot_header_t snapshot);
#endif
