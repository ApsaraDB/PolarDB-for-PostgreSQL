/*-------------------------------------------------------------------------
 *
 * xlogreader.h
 *		Definitions for the generic XLog reading facility
 *
 * Portions Copyright (c) 2013-2018, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		src/include/access/xlogreader.h
 *
 * NOTES
 *		See the definition of the XLogReaderState struct for instructions on
 *		how to use the XLogReader infrastructure.
 *
 *		The basic idea is to allocate an XLogReaderState via
 *		XLogReaderAllocate(), and call XLogReadRecord() until it returns NULL.
 *
 *		After reading a record with XLogReadRecord(), it's decomposed into
 *		the per-block and main data parts, and the parts can be accessed
 *		with the XLogRec* macros and functions. You can also decode a
 *		record that's already constructed in memory, without reading from
 *		disk, by calling the DecodeXLogRecord() function.
 *-------------------------------------------------------------------------
 */
#ifndef XLOGREADER_H
#define XLOGREADER_H

#include "access/xlogrecord.h"

typedef struct XLogReaderState XLogReaderState;

/* Function type definition for the read_page callback */
typedef int (*XLogPageReadCB) (XLogReaderState *xlogreader,
							   XLogRecPtr targetPagePtr,
							   int reqLen,
							   XLogRecPtr targetRecPtr,
							   char *readBuf,
							   TimeLineID *pageTLI);

typedef struct
{
	/* Is this block ref in use? */
	bool		in_use;

	/* Identify the block this refers to */
	RelFileNode rnode;
	ForkNumber	forknum;
	BlockNumber blkno;

	/* copy of the fork_flags field from the XLogRecordBlockHeader */
	uint8		flags;

	/* Information on full-page image, if any */
	bool		has_image;		/* has image, even for consistency checking */
	bool		apply_image;	/* has image that should be restored */
	char	   *bkp_image;
	uint16		hole_offset;
	uint16		hole_length;
	uint16		bimg_len;
	uint8		bimg_info;

	/* Buffer holding the rmgr-specific data associated with this block */
	bool		has_data;
	char	   *data;
	uint16		data_len;
	uint16		data_bufsz;
} DecodedBkpBlock;

struct XLogReaderState
{
	/* ----------------------------------------
	 * Public parameters
	 * ----------------------------------------
	 */

	/*
	 * Segment size of the to-be-parsed data (mandatory).
	 */
	int			wal_segment_size;

	/*
	 * Data input callback (mandatory).
	 *
	 * This callback shall read at least reqLen valid bytes of the xlog page
	 * starting at targetPagePtr, and store them in readBuf.  The callback
	 * shall return the number of bytes read (never more than XLOG_BLCKSZ), or
	 * -1 on failure.  The callback shall sleep, if necessary, to wait for the
	 * requested bytes to become available.  The callback will not be invoked
	 * again for the same page unless more than the returned number of bytes
	 * are needed.
	 *
	 * targetRecPtr is the position of the WAL record we're reading.  Usually
	 * it is equal to targetPagePtr + reqLen, but sometimes xlogreader needs
	 * to read and verify the page or segment header, before it reads the
	 * actual WAL record it's interested in.  In that case, targetRecPtr can
	 * be used to determine which timeline to read the page from.
	 *
	 * The callback shall set *pageTLI to the TLI of the file the page was
	 * read from.  It is currently used only for error reporting purposes, to
	 * reconstruct the name of the WAL file where an error occurred.
	 */
	XLogPageReadCB read_page;

	/*
	 * System identifier of the xlog files we're about to read.  Set to zero
	 * (the default value) if unknown or unimportant.
	 */
	uint64		system_identifier;

	/*
	 * Opaque data for callbacks to use.  Not used by XLogReader.
	 */
	void	   *private_data;

	/*
	 * Start and end point of last record read.  EndRecPtr is also used as the
	 * position to read next, if XLogReadRecord receives an invalid recptr.
	 */
	XLogRecPtr	ReadRecPtr;		/* start of last record read */
	XLogRecPtr	EndRecPtr;		/* end+1 of last record read */


	/* ----------------------------------------
	 * Decoded representation of current record
	 *
	 * Use XLogRecGet* functions to investigate the record; these fields
	 * should not be accessed directly.
	 * ----------------------------------------
	 */
	XLogRecord *decoded_record; /* currently decoded record */

	char	   *main_data;		/* record's main data portion */
	uint32		main_data_len;	/* main data portion's length */
	uint32		main_data_bufsz;	/* allocated size of the buffer */

	RepOriginId record_origin;

	/* information about blocks referenced by the record. */
	DecodedBkpBlock blocks[XLR_MAX_BLOCK_ID + 1];

	int			max_block_id;	/* highest block_id in use (-1 if none) */

	/* ----------------------------------------
	 * private/internal state
	 * ----------------------------------------
	 */

	/*
	 * Buffer for currently read page (XLOG_BLCKSZ bytes, valid up to at least
	 * readLen bytes)
	 */
	char	   *readBuf;
	uint32		readLen;

	/* last read segment, segment offset, TLI for data currently in readBuf */
	XLogSegNo	readSegNo;
	uint32		readOff;
	TimeLineID	readPageTLI;

	/*
	 * beginning of prior page read, and its TLI.  Doesn't necessarily
	 * correspond to what's in readBuf; used for timeline sanity checks.
	 */
	XLogRecPtr	latestPagePtr;
	TimeLineID	latestPageTLI;

	/* beginning of the WAL record being read. */
	XLogRecPtr	currRecPtr;
	/* timeline to read it from, 0 if a lookup is required */
	TimeLineID	currTLI;

	/*
	 * Safe point to read to in currTLI if current TLI is historical
	 * (tliSwitchPoint) or InvalidXLogRecPtr if on current timeline.
	 *
	 * Actually set to the start of the segment containing the timeline switch
	 * that ends currTLI's validity, not the LSN of the switch its self, since
	 * we can't assume the old segment will be present.
	 */
	XLogRecPtr	currTLIValidUntil;

	/*
	 * If currTLI is not the most recent known timeline, the next timeline to
	 * read from when currTLIValidUntil is reached.
	 */
	TimeLineID	nextTLI;

	/* Buffer for current ReadRecord result (expandable) */
	char	   *readRecordBuf;
	uint32		readRecordBufSize;

	/* Buffer to hold error message */
	char	   *errormsg_buf;

	/* POLAR: Is this only have meta data? */
	bool noPayload;

	/*
	 * POLAR: Save the length of record's XLogRecordHeader, XLogRecordBlockHeader, XLogOrigin and XLogRecordDataHaeder.
	 * In order to reserve XLOG queue space to store logindex meta.
	 */
	uint32		polar_logindex_meta_size;
};

/* Get a new XLogReader */
extern XLogReaderState *XLogReaderAllocate(int wal_segment_size,
				   XLogPageReadCB pagereadfunc,
				   void *private_data);

/* Free an XLogReader */
extern void XLogReaderFree(XLogReaderState *state);

/* Read the next XLog record. Returns NULL on end-of-WAL or failure */
extern struct XLogRecord *XLogReadRecord(XLogReaderState *state,
			   XLogRecPtr recptr, char **errormsg);

/* Validate a page */
extern bool XLogReaderValidatePageHeader(XLogReaderState *state,
							 XLogRecPtr recptr, char *phdr);

/* Invalidate read state */
extern void XLogReaderInvalReadState(XLogReaderState *state);

extern XLogRecPtr XLogFindNextRecord(XLogReaderState *state, XLogRecPtr RecPtr);

/* Functions for decoding an XLogRecord */

extern bool DecodeXLogRecord(XLogReaderState *state, XLogRecord *record,
				 char **errmsg);

#define XLogRecGetTotalLen(decoder) ((decoder)->decoded_record->xl_tot_len)
#define XLogRecGetPrev(decoder) ((decoder)->decoded_record->xl_prev)
#define XLogRecGetInfo(decoder) ((decoder)->decoded_record->xl_info)
#define XLogRecGetRmid(decoder) ((decoder)->decoded_record->xl_rmid)
#define XLogRecGetXid(decoder) ((decoder)->decoded_record->xl_xid)
#define XLogRecGetOrigin(decoder) ((decoder)->record_origin)
#define XLogRecGetData(decoder) ((decoder)->main_data)
#define XLogRecGetDataLen(decoder) ((decoder)->main_data_len)
#define XLogRecHasAnyBlockRefs(decoder) ((decoder)->max_block_id >= 0)
#define XLogRecHasBlockRef(decoder, block_id) \
	((decoder)->blocks[block_id].in_use)
#define XLogRecHasBlockImage(decoder, block_id) \
	((decoder)->blocks[block_id].has_image)
#define XLogRecBlockImageApply(decoder, block_id) \
	((decoder)->blocks[block_id].apply_image)

extern bool RestoreBlockImage(XLogReaderState *recoder, uint8 block_id, char *dst);
extern char *XLogRecGetBlockData(XLogReaderState *record, uint8 block_id, Size *len);
extern bool XLogRecGetBlockTag(XLogReaderState *record, uint8 block_id,
				   RelFileNode *rnode, ForkNumber *forknum,
				   BlockNumber *blknum);

/* POLAR */
extern bool ValidXLogRecord(XLogReaderState *state, XLogRecord *record,
				XLogRecPtr recptr);
extern bool allocate_recordbuf(XLogReaderState *state, uint32 reclength);

extern void report_invalid_record(XLogReaderState *state, const char *fmt,...) pg_attribute_printf(2, 3);
#endif							/* XLOGREADER_H */
