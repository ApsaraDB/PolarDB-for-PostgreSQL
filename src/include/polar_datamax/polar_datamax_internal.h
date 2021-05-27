/*-------------------------------------------------------------------------
 *
 * polar_datamax_internal.h
 *	  datamax corresponding definitions for client-side .c files.
 *
 * src/include/polar_datamax/polar_datamax_internal.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef POLAR_DATAMAX_INTERNAL_H
#define POLAR_DATAMAX_INTERNAL_H

#include "c.h"
#include "access/xlogdefs.h"
#include "lib/ilist.h"
#include "port/pg_crc32c.h"

#define POLAR_DATAMAX_MAGIC           (0X0518)
/* different version from that before POLARDB_11_REL_20201230 */
#define POLAR_DATAMAX_VERSION         (0x0002)

#define POLAR_DATAMAX_NOT_INITIAL_STREAM 0x00

#define POLAR_LOG_DATAMAX_META_INFO(meta) \
	do { \
		elog(LOG, "datamax meta: magic=%x, version=%x, min_received_lsn=%lx, last_received_lsn=%lx, last_valid_received_lsn=%lx, upstream_last_removed_segno=%lx", \
			 (meta)->magic, (meta)->version, \
			 (meta)->min_received_lsn, (meta)->last_received_lsn, \
			 (meta)->last_valid_received_lsn, (meta)->upstream_last_removed_segno); \
	} while (0)

#define POLAR_DATAMAX_META  (&polar_datamax_ctl->meta)
#define POLAR_DATAMAX_STREAMING_TIMELINE (polar_datamax_ctl->meta.last_timeline_id)
#define POLAR_DATAMAX_STREAMING_LSN (polar_datamax_ctl->meta.last_valid_received_lsn)

#define POLAR_INVALID_TIMELINE_ID     ((uint32)0)

#define POLAR_DATAMAX_WAL_PATH          (polar_is_dma_logger_mode ? POLAR_DATAMAX_WAL_DIR : XLOGDIR)

typedef struct polar_datamax_meta_data_t
{
	uint32      magic;
	uint32      version;
	TimeLineID  min_timeline_id;
	XLogRecPtr  min_received_lsn;
	TimeLineID  last_timeline_id; /* last received timeline id */
	XLogRecPtr  last_received_lsn; /* last received lsn */
	XLogRecPtr  last_valid_received_lsn; /* end of the last valid record received from primay */
	XLogSegNo	upstream_last_removed_segno; /* last removed segno in upstream */

	pg_crc32c   crc;
} polar_datamax_meta_data_t;

#endif /* !POLAR_DATAMAX_INTERNAL_H */
