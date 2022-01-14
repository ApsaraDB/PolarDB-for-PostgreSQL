/*-------------------------------------------------------------------------
 *
 * pg_waldump.c - decode and display WAL
 *
 * Copyright (c) 2013-2018, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		  src/bin/pg_waldump/pg_waldump.c
 *-------------------------------------------------------------------------
 */

#define FRONTEND 1
#include "postgres.h"

#include <dirent.h>
#include <sys/stat.h>
#include <unistd.h>

#include "access/xlogreader.h"
#include "access/xlogrecord.h"
#include "access/xlog_internal.h"
#include "access/transam.h"
#include "common/fe_memutils.h"
#include "getopt_long.h"
#include "rmgrdesc.h"


static const char *progname;

static int	WalSegSz;

/* POLAR */
typedef struct polar_xlog_dump_private
{
	char 		*path;
	XLogRecPtr	cur_lsn;
	int 		total_len;
	int			file_size;
	int			wal_segment_size;
} polar_xlog_dump_private;
/* POLAR end */

typedef struct XLogDumpPrivate
{
	TimeLineID	timeline;
	char	   *inpath;
	XLogRecPtr	startptr;
	XLogRecPtr	endptr;
	bool		endptr_reached;
} XLogDumpPrivate;

typedef struct XLogDumpConfig
{
	/* display options */
	bool		bkp_details;
	int			stop_after_records;
	int			already_displayed_records;
	bool		follow;
	bool		stats;
	bool		stats_per_record;

	/* filter options */
	int			filter_by_rmgr;
	TransactionId filter_by_xid;
	bool		filter_by_xid_enabled;
} XLogDumpConfig;

typedef struct Stats
{
	uint64		count;
	uint64		rec_len;
	uint64		fpi_len;
} Stats;

#define MAX_XLINFO_TYPES 16

typedef struct XLogDumpStats
{
	uint64		count;
	Stats		rmgr_stats[RM_NEXT_ID];
	Stats		record_stats[RM_NEXT_ID][MAX_XLINFO_TYPES];
} XLogDumpStats;

static void fatal_error(const char *fmt,...) pg_attribute_printf(1, 2);

/*
 * Big red button to push when things go horribly wrong.
 */
static void
fatal_error(const char *fmt,...)
{
	va_list		args;

	fflush(stdout);

	fprintf(stderr, _("%s: FATAL:  "), progname);
	va_start(args, fmt);
	vfprintf(stderr, _(fmt), args);
	va_end(args);
	fputc('\n', stderr);

	exit(EXIT_FAILURE);
}

static void
print_rmgr_list(void)
{
	int			i;

	for (i = 0; i <= RM_MAX_ID; i++)
	{
		printf("%s\n", RmgrDescTable[i].rm_name);
	}
}

/*
 * Check whether directory exists and whether we can open it. Keep errno set so
 * that the caller can report errors somewhat more accurately.
 */
static bool
verify_directory(const char *directory)
{
	DIR		   *dir = opendir(directory);

	if (dir == NULL)
		return false;
	closedir(dir);
	return true;
}

/*
 * Split a pathname as dirname(1) and basename(1) would.
 *
 * XXX this probably doesn't do very well on Windows.  We probably need to
 * apply canonicalize_path(), at the very least.
 */
static void
split_path(const char *path, char **dir, char **fname)
{
	char	   *sep;

	/* split filepath into directory & filename */
	sep = strrchr(path, '/');

	/* directory path */
	if (sep != NULL)
	{
		*dir = pg_strdup(path);
		(*dir)[(sep - path) + 1] = '\0';	/* no strndup */
		*fname = pg_strdup(sep + 1);
	}
	/* local directory */
	else
	{
		*dir = NULL;
		*fname = pg_strdup(path);
	}
}

/*
 * Open the file in the valid target directory.
 *
 * return a read only fd
 */
static int
open_file_in_directory(const char *directory, const char *fname)
{
	int			fd = -1;
	char		fpath[MAXPGPATH];

	Assert(directory != NULL);

	snprintf(fpath, MAXPGPATH, "%s/%s", directory, fname);
	fd = open(fpath, O_RDONLY | PG_BINARY, 0);

	if (fd < 0 && errno != ENOENT)
		fatal_error("could not open file \"%s\": %s",
					fname, strerror(errno));
	return fd;
}

/*
 * Try to find fname in the given directory. Returns true if it is found,
 * false otherwise. If fname is NULL, search the complete directory for any
 * file with a valid WAL file name. If file is successfully opened, set the
 * wal segment size.
 */
static bool
search_directory(const char *directory, const char *fname)
{
	int			fd = -1;
	DIR		   *xldir;

	/* open file if valid filename is provided */
	if (fname != NULL)
		fd = open_file_in_directory(directory, fname);

	/*
	 * A valid file name is not passed, so search the complete directory.  If
	 * we find any file whose name is a valid WAL file name then try to open
	 * it.  If we cannot open it, bail out.
	 */
	else if ((xldir = opendir(directory)) != NULL)
	{
		struct dirent *xlde;

		while ((xlde = readdir(xldir)) != NULL)
		{
			if (IsXLogFileName(xlde->d_name))
			{
				fd = open_file_in_directory(directory, xlde->d_name);
				fname = xlde->d_name;
				break;
			}
		}

		closedir(xldir);
	}

	/* set WalSegSz if file is successfully opened */
	if (fd >= 0)
	{
		PGAlignedXLogBlock buf;

		if (read(fd, buf.data, XLOG_BLCKSZ) == XLOG_BLCKSZ)
		{
			XLogLongPageHeader longhdr = (XLogLongPageHeader) buf.data;

			WalSegSz = longhdr->xlp_seg_size;

			if (!IsValidWalSegSize(WalSegSz))
				fatal_error(ngettext("WAL segment size must be a power of two between 1 MB and 1 GB, but the WAL file \"%s\" header specifies %d byte",
									 "WAL segment size must be a power of two between 1 MB and 1 GB, but the WAL file \"%s\" header specifies %d bytes",
									 WalSegSz),
							fname, WalSegSz);
		}
		else
		{
			if (errno != 0)
				fatal_error("could not read file \"%s\": %s",
							fname, strerror(errno));
			else
				fatal_error("not enough data in file \"%s\"", fname);
		}
		close(fd);
		return true;
	}

	return false;
}

/*
 * Identify the target directory and set WalSegSz.
 *
 * Try to find the file in several places:
 * if directory != NULL:
 *	 directory /
 *	 directory / XLOGDIR /
 * else
 *	 .
 *	 XLOGDIR /
 *	 $PGDATA / XLOGDIR /
 *
 * Set the valid target directory in private->inpath.
 */
static void
identify_target_directory(XLogDumpPrivate *private, char *directory,
						  char *fname)
{
	char		fpath[MAXPGPATH];

	if (directory != NULL)
	{
		if (search_directory(directory, fname))
		{
			private->inpath = strdup(directory);
			return;
		}

		/* directory / XLOGDIR */
		snprintf(fpath, MAXPGPATH, "%s/%s", directory, XLOGDIR);
		if (search_directory(fpath, fname))
		{
			private->inpath = strdup(fpath);
			return;
		}
	}
	else
	{
		const char *datadir;

		/* current directory */
		if (search_directory(".", fname))
		{
			private->inpath = strdup(".");
			return;
		}
		/* XLOGDIR */
		if (search_directory(XLOGDIR, fname))
		{
			private->inpath = strdup(XLOGDIR);
			return;
		}

		datadir = getenv("PGDATA");
		/* $PGDATA / XLOGDIR */
		if (datadir != NULL)
		{
			snprintf(fpath, MAXPGPATH, "%s/%s", datadir, XLOGDIR);
			if (search_directory(fpath, fname))
			{
				private->inpath = strdup(fpath);
				return;
			}
		}
	}

	/* could not locate WAL file */
	if (fname)
		fatal_error("could not locate WAL file \"%s\"", fname);
	else
		fatal_error("could not find any WAL file");
}

/*
 * Read count bytes from a segment file in the specified directory, for the
 * given timeline, containing the specified record pointer; store the data in
 * the passed buffer.
 */
static void
XLogDumpXLogRead(const char *directory, TimeLineID timeline_id,
				 XLogRecPtr startptr, char *buf, Size count)
{
	char	   *p;
	XLogRecPtr	recptr;
	Size		nbytes;

	static int	sendFile = -1;
	static XLogSegNo sendSegNo = 0;
	static uint32 sendOff = 0;

	p = buf;
	recptr = startptr;
	nbytes = count;

	while (nbytes > 0)
	{
		uint32		startoff;
		int			segbytes;
		int			readbytes;

		startoff = XLogSegmentOffset(recptr, WalSegSz);

		if (sendFile < 0 || !XLByteInSeg(recptr, sendSegNo, WalSegSz))
		{
			char		fname[MAXFNAMELEN];
			int			tries;

			/* Switch to another logfile segment */
			if (sendFile >= 0)
				close(sendFile);

			XLByteToSeg(recptr, sendSegNo, WalSegSz);

			XLogFileName(fname, timeline_id, sendSegNo, WalSegSz);

			/*
			 * In follow mode there is a short period of time after the server
			 * has written the end of the previous file before the new file is
			 * available. So we loop for 5 seconds looking for the file to
			 * appear before giving up.
			 */
			for (tries = 0; tries < 10; tries++)
			{
				sendFile = open_file_in_directory(directory, fname);
				if (sendFile >= 0)
					break;
				if (errno == ENOENT)
				{
					int			save_errno = errno;

					/* File not there yet, try again */
					pg_usleep(500 * 1000);

					errno = save_errno;
					continue;
				}
				/* Any other error, fall through and fail */
				break;
			}

			if (sendFile < 0)
				fatal_error("could not find file \"%s\": %s",
							fname, strerror(errno));
			sendOff = 0;
		}

		/* Need to seek in the file? */
		if (sendOff != startoff)
		{
			if (lseek(sendFile, (off_t) startoff, SEEK_SET) < 0)
			{
				int			err = errno;
				char		fname[MAXPGPATH];

				XLogFileName(fname, timeline_id, sendSegNo, WalSegSz);

				fatal_error("could not seek in log file %s to offset %u: %s",
							fname, startoff, strerror(err));
			}
			sendOff = startoff;
		}

		/* How many bytes are within this segment? */
		if (nbytes > (WalSegSz - startoff))
			segbytes = WalSegSz - startoff;
		else
			segbytes = nbytes;

		readbytes = read(sendFile, p, segbytes);
		if (readbytes <= 0)
		{
			int			err = errno;
			char		fname[MAXPGPATH];

			XLogFileName(fname, timeline_id, sendSegNo, WalSegSz);

			fatal_error("could not read from log file %s, offset %u, length %d: %s",
						fname, sendOff, segbytes, strerror(err));
		}

		/* Update state for read */
		recptr += readbytes;

		sendOff += readbytes;
		nbytes -= readbytes;
		p += readbytes;
	}
}

/*
 * XLogReader read_page callback
 */
static int
XLogDumpReadPage(XLogReaderState *state, XLogRecPtr targetPagePtr, int reqLen,
				 XLogRecPtr targetPtr, char *readBuff, TimeLineID *curFileTLI)
{
	XLogDumpPrivate *private = state->private_data;
	int			count = XLOG_BLCKSZ;

	if (private->endptr != InvalidXLogRecPtr)
	{
		if (targetPagePtr + XLOG_BLCKSZ <= private->endptr)
			count = XLOG_BLCKSZ;
		else if (targetPagePtr + reqLen <= private->endptr)
			count = private->endptr - targetPagePtr;
		else
		{
			private->endptr_reached = true;
			return -1;
		}
	}

	XLogDumpXLogRead(private->inpath, private->timeline, targetPagePtr,
					 readBuff, count);

	return count;
}

/*
 * Calculate the size of a record, split into !FPI and FPI parts.
 */
static void
XLogDumpRecordLen(XLogReaderState *record, uint32 *rec_len, uint32 *fpi_len)
{
	int			block_id;

	/*
	 * Calculate the amount of FPI data in the record.
	 *
	 * XXX: We peek into xlogreader's private decoded backup blocks for the
	 * bimg_len indicating the length of FPI data. It doesn't seem worth it to
	 * add an accessor macro for this.
	 */
	*fpi_len = 0;
	for (block_id = 0; block_id <= record->max_block_id; block_id++)
	{
		if (XLogRecHasBlockImage(record, block_id))
			*fpi_len += record->blocks[block_id].bimg_len;
	}

	/*
	 * Calculate the length of the record as the total length - the length of
	 * all the block images.
	 */
	*rec_len = XLogRecGetTotalLen(record) - *fpi_len;
}

/*
 * Store per-rmgr and per-record statistics for a given record.
 */
static void
XLogDumpCountRecord(XLogDumpConfig *config, XLogDumpStats *stats,
					XLogReaderState *record)
{
	RmgrId		rmid;
	uint8		recid;
	uint32		rec_len;
	uint32		fpi_len;

	stats->count++;

	rmid = XLogRecGetRmid(record);

	XLogDumpRecordLen(record, &rec_len, &fpi_len);

	/* Update per-rmgr statistics */

	stats->rmgr_stats[rmid].count++;
	stats->rmgr_stats[rmid].rec_len += rec_len;
	stats->rmgr_stats[rmid].fpi_len += fpi_len;

	/*
	 * Update per-record statistics, where the record is identified by a
	 * combination of the RmgrId and the four bits of the xl_info field that
	 * are the rmgr's domain (resulting in sixteen possible entries per
	 * RmgrId).
	 */

	recid = XLogRecGetInfo(record) >> 4;

	stats->record_stats[rmid][recid].count++;
	stats->record_stats[rmid][recid].rec_len += rec_len;
	stats->record_stats[rmid][recid].fpi_len += fpi_len;
}

/*
 * Print a record to stdout
 */
static void
XLogDumpDisplayRecord(XLogDumpConfig *config, XLogReaderState *record)
{
	const char *id;
	const RmgrDescData *desc = &RmgrDescTable[XLogRecGetRmid(record)];
	uint32		rec_len;
	uint32		fpi_len;
	RelFileNode rnode;
	ForkNumber	forknum;
	BlockNumber blk;
	int			block_id;
	uint8		info = XLogRecGetInfo(record);
	XLogRecPtr	xl_prev = XLogRecGetPrev(record);

	XLogDumpRecordLen(record, &rec_len, &fpi_len);

	printf("rmgr: %-11s len (rec/tot): %6u/%6u, tx: %10u, lsn: %X/%08X, prev %X/%08X, ",
		   desc->rm_name,
		   rec_len, XLogRecGetTotalLen(record),
		   XLogRecGetXid(record),
		   (uint32) (record->ReadRecPtr >> 32), (uint32) record->ReadRecPtr,
		   (uint32) (xl_prev >> 32), (uint32) xl_prev);

	id = desc->rm_identify(info);
	if (id == NULL)
		printf("desc: UNKNOWN (%x) ", info & ~XLR_INFO_MASK);
	else
		printf("desc: %s ", id);

	/* the desc routine will printf the description directly to stdout */
	desc->rm_desc(NULL, record);

	if (!config->bkp_details)
	{
		/* print block references (short format) */
		for (block_id = 0; block_id <= record->max_block_id; block_id++)
		{
			if (!XLogRecHasBlockRef(record, block_id))
				continue;

			XLogRecGetBlockTag(record, block_id, &rnode, &forknum, &blk);
			if (forknum != MAIN_FORKNUM)
				printf(", blkref #%u: rel %u/%u/%u fork %s blk %u",
					   block_id,
					   rnode.spcNode, rnode.dbNode, rnode.relNode,
					   forkNames[forknum],
					   blk);
			else
				printf(", blkref #%u: rel %u/%u/%u blk %u",
					   block_id,
					   rnode.spcNode, rnode.dbNode, rnode.relNode,
					   blk);
			if (XLogRecHasBlockImage(record, block_id))
			{
				if (XLogRecBlockImageApply(record, block_id))
					printf(" FPW");
				else
					printf(" FPW for WAL verification");
			}
		}
		putchar('\n');
	}
	else
	{
		/* print block references (detailed format) */
		putchar('\n');
		for (block_id = 0; block_id <= record->max_block_id; block_id++)
		{
			if (!XLogRecHasBlockRef(record, block_id))
				continue;

			XLogRecGetBlockTag(record, block_id, &rnode, &forknum, &blk);
			printf("\tblkref #%u: rel %u/%u/%u fork %s blk %u",
				   block_id,
				   rnode.spcNode, rnode.dbNode, rnode.relNode,
				   forkNames[forknum],
				   blk);
			if (XLogRecHasBlockImage(record, block_id))
			{
				if (record->blocks[block_id].bimg_info &
					BKPIMAGE_IS_COMPRESSED)
				{
					printf(" (FPW%s); hole: offset: %u, length: %u, "
						   "compression saved: %u",
						   XLogRecBlockImageApply(record, block_id) ?
						   "" : " for WAL verification",
						   record->blocks[block_id].hole_offset,
						   record->blocks[block_id].hole_length,
						   BLCKSZ -
						   record->blocks[block_id].hole_length -
						   record->blocks[block_id].bimg_len);
				}
				else
				{
					printf(" (FPW%s); hole: offset: %u, length: %u",
						   XLogRecBlockImageApply(record, block_id) ?
						   "" : " for WAL verification",
						   record->blocks[block_id].hole_offset,
						   record->blocks[block_id].hole_length);
				}
			}
			putchar('\n');
		}
	}
}

/*
 * Display a single row of record counts and sizes for an rmgr or record.
 */
static void
XLogDumpStatsRow(const char *name,
				 uint64 n, uint64 total_count,
				 uint64 rec_len, uint64 total_rec_len,
				 uint64 fpi_len, uint64 total_fpi_len,
				 uint64 tot_len, uint64 total_len)
{
	double		n_pct,
				rec_len_pct,
				fpi_len_pct,
				tot_len_pct;

	n_pct = 0;
	if (total_count != 0)
		n_pct = 100 * (double) n / total_count;

	rec_len_pct = 0;
	if (total_rec_len != 0)
		rec_len_pct = 100 * (double) rec_len / total_rec_len;

	fpi_len_pct = 0;
	if (total_fpi_len != 0)
		fpi_len_pct = 100 * (double) fpi_len / total_fpi_len;

	tot_len_pct = 0;
	if (total_len != 0)
		tot_len_pct = 100 * (double) tot_len / total_len;

	printf("%-27s "
		   "%20" INT64_MODIFIER "u (%6.02f) "
		   "%20" INT64_MODIFIER "u (%6.02f) "
		   "%20" INT64_MODIFIER "u (%6.02f) "
		   "%20" INT64_MODIFIER "u (%6.02f)\n",
		   name, n, n_pct, rec_len, rec_len_pct, fpi_len, fpi_len_pct,
		   tot_len, tot_len_pct);
}


/*
 * Display summary statistics about the records seen so far.
 */
static void
XLogDumpDisplayStats(XLogDumpConfig *config, XLogDumpStats *stats)
{
	int			ri,
				rj;
	uint64		total_count = 0;
	uint64		total_rec_len = 0;
	uint64		total_fpi_len = 0;
	uint64		total_len = 0;
	double		rec_len_pct,
				fpi_len_pct;

	/* ---
	 * Make a first pass to calculate column totals:
	 * count(*),
	 * sum(xl_len+SizeOfXLogRecord),
	 * sum(xl_tot_len-xl_len-SizeOfXLogRecord), and
	 * sum(xl_tot_len).
	 * These are used to calculate percentages for each record type.
	 * ---
	 */

	for (ri = 0; ri < RM_NEXT_ID; ri++)
	{
		total_count += stats->rmgr_stats[ri].count;
		total_rec_len += stats->rmgr_stats[ri].rec_len;
		total_fpi_len += stats->rmgr_stats[ri].fpi_len;
	}
	total_len = total_rec_len + total_fpi_len;

	/*
	 * 27 is strlen("Transaction/COMMIT_PREPARED"), 20 is strlen(2^64), 8 is
	 * strlen("(100.00%)")
	 */

	printf("%-27s %20s %8s %20s %8s %20s %8s %20s %8s\n"
		   "%-27s %20s %8s %20s %8s %20s %8s %20s %8s\n",
		   "Type", "N", "(%)", "Record size", "(%)", "FPI size", "(%)", "Combined size", "(%)",
		   "----", "-", "---", "-----------", "---", "--------", "---", "-------------", "---");

	for (ri = 0; ri < RM_NEXT_ID; ri++)
	{
		uint64		count,
					rec_len,
					fpi_len,
					tot_len;
		const RmgrDescData *desc = &RmgrDescTable[ri];

		if (!config->stats_per_record)
		{
			count = stats->rmgr_stats[ri].count;
			rec_len = stats->rmgr_stats[ri].rec_len;
			fpi_len = stats->rmgr_stats[ri].fpi_len;
			tot_len = rec_len + fpi_len;

			XLogDumpStatsRow(desc->rm_name,
							 count, total_count, rec_len, total_rec_len,
							 fpi_len, total_fpi_len, tot_len, total_len);
		}
		else
		{
			for (rj = 0; rj < MAX_XLINFO_TYPES; rj++)
			{
				const char *id;

				count = stats->record_stats[ri][rj].count;
				rec_len = stats->record_stats[ri][rj].rec_len;
				fpi_len = stats->record_stats[ri][rj].fpi_len;
				tot_len = rec_len + fpi_len;

				/* Skip undefined combinations and ones that didn't occur */
				if (count == 0)
					continue;

				/* the upper four bits in xl_info are the rmgr's */
				id = desc->rm_identify(rj << 4);
				if (id == NULL)
					id = psprintf("UNKNOWN (%x)", rj << 4);

				XLogDumpStatsRow(psprintf("%s/%s", desc->rm_name, id),
								 count, total_count, rec_len, total_rec_len,
								 fpi_len, total_fpi_len, tot_len, total_len);
			}
		}
	}

	printf("%-27s %20s %8s %20s %8s %20s %8s %20s\n",
		   "", "--------", "", "--------", "", "--------", "", "--------");

	/*
	 * The percentages in earlier rows were calculated against the column
	 * total, but the ones that follow are against the row total. Note that
	 * these are displayed with a % symbol to differentiate them from the
	 * earlier ones, and are thus up to 9 characters long.
	 */

	rec_len_pct = 0;
	if (total_len != 0)
		rec_len_pct = 100 * (double) total_rec_len / total_len;

	fpi_len_pct = 0;
	if (total_len != 0)
		fpi_len_pct = 100 * (double) total_fpi_len / total_len;

	printf("%-27s "
		   "%20" INT64_MODIFIER "u %-9s"
		   "%20" INT64_MODIFIER "u %-9s"
		   "%20" INT64_MODIFIER "u %-9s"
		   "%20" INT64_MODIFIER "u %-6s\n",
		   "Total", stats->count, "",
		   total_rec_len, psprintf("[%.02f%%]", rec_len_pct),
		   total_fpi_len, psprintf("[%.02f%%]", fpi_len_pct),
		   total_len, "[100%]");
}

/* POLAR */
static void
polar_print_buf(char *buf, int size)
{
	int i = 0;

	Assert(buf);
	for(i = 0; i < size; i++)
	{
		fprintf(stderr, _("0x%02x "), (unsigned char) buf[i]);
		if ((i + 1) % 16 == 0)
			fprintf(stderr, _("\n"));
	}
	fprintf(stderr, _("\n"));
}

/* read page content from polar_xlog_file */
static int
polar_read_xlog_page(char *path, char *buf, Size off)
{
	int fd = -1;
	int readlen = -1;

	Assert(path);
	Assert(buf);
	Assert((off % XLOG_BLCKSZ) == 0);

	fd = open(path, O_RDONLY | PG_BINARY, 0);
	if (fd < 0)
		fatal_error("failed to open file \"%s\"", path);
	if (lseek(fd, off, SEEK_SET) < 0)
	{
		close(fd);
		fatal_error("failed to seek in file \"%s\"", path);
	}
	readlen = read(fd, buf, XLOG_BLCKSZ);
	if (readlen != XLOG_BLCKSZ)
	{
		readlen = -1;
		close(fd);
		fatal_error("failed to read file \"%s\"", path);
	}
	if (fd >= 0)
		close(fd);
	return readlen;
}

/* callback of readpagefunc of xlogreader */
static int
polar_read_page(XLogReaderState *state, XLogRecPtr targetpage, int reqlen,
				XLogRecPtr targetptr, char *readbuf, TimeLineID *curtli)
{
	polar_xlog_dump_private *private = state->private_data;
	XLogRecPtr curlsn = private->cur_lsn;
	int total_len = private->total_len;
	int offset;

	XLogRecPtr startpage = curlsn - (curlsn % XLOG_BLCKSZ);
	
	/* polar_xlog_file only save content from startpage to endpage of curlsn */
	if (targetpage < startpage)
	{
		fprintf(stderr, _("no content before %X/%X"), (uint32) (targetpage >> 32), (uint32) targetpage);
		return -1;
	}

	/* get the actual offset in polar_xlog_file */
	offset = (total_len / XLOG_BLCKSZ + 3) * XLOG_BLCKSZ + targetpage - startpage;
	/* read content from polar_xlog_file */
	return polar_read_xlog_page(private->path, readbuf, offset);
}

static void
polar_read_private_data(char *path, polar_xlog_dump_private *private_data)
{
	struct  stat  stat_info;
	char  	*page_info = NULL;

	memset(private_data, 0, sizeof(polar_xlog_dump_private));
	private_data->path = path;

	/* get size of polar_xlog_file */
	if (stat(path, &stat_info) < 0)
		fatal_error("could not stat file \"%s\"", path);
	private_data->file_size = stat_info.st_size;

	/* read current lsn and wal_segment_size */
	page_info = (char *)palloc0(XLOG_BLCKSZ);
	if (!page_info)
		fatal_error("failed to allocate memory");
	if (polar_read_xlog_page(path, page_info, 0) < 0)
	{
		pfree(page_info);
		fatal_error("failed to read from file \"%s\"", path);
	}
	memcpy(&private_data->cur_lsn, page_info, sizeof(XLogRecPtr));
	memcpy(&private_data->wal_segment_size, page_info + sizeof(XLogRecPtr), sizeof(int));

	pfree(page_info);
	return;	
}

static void
polar_read_memory_xlog(char **mem_record, polar_xlog_dump_private *private_data)
{
	char *page_info = NULL, *zero_page = NULL;
	int offset = XLOG_BLCKSZ;
	int read_len = 0, total_len = 0;
	int res = -1;

	page_info = (char *)palloc0(XLOG_BLCKSZ);
	if (!page_info)
		fatal_error("failed to allocate memory");

	if (polar_read_xlog_page(private_data->path, page_info, offset) < 0)
	{
		fprintf(stderr, _("failed to read from file \"%s\"\n"), private_data->path);
		goto err;
	}
	private_data->total_len = ((XLogRecord *)page_info)->xl_tot_len;
	fprintf(stderr, _("current lsn: %X/%X, total_len: %d\n"), 
			(uint32) (private_data->cur_lsn >> 32), (uint32) private_data->cur_lsn, private_data->total_len);

	read_len = 0;
	total_len = private_data->total_len;
	*mem_record = palloc(total_len);
	if (!*mem_record)
	{
		fprintf(stderr, _("failed to allocate memory\n"));
		goto err;
	}
	while( total_len > 0)
	{
		int copylen = total_len > XLOG_BLCKSZ ? XLOG_BLCKSZ : total_len;
		memcpy(*mem_record + read_len, page_info, copylen);
		total_len -= copylen;
		read_len += copylen;
		offset += XLOG_BLCKSZ;
		if (polar_read_xlog_page(private_data->path, page_info, offset) < 0)
		{
			fprintf(stderr, _("failed to read from file \"%s\"\n"), private_data->path);
			goto err;
		}
	}

	/* check the last page is zero page */
	zero_page = palloc(XLOG_BLCKSZ);
	if (!zero_page)
	{
		fprintf(stderr, _("failed to allocate memory\n"));
		goto err;
	}
	memset(zero_page, 0, XLOG_BLCKSZ);
	if (memcmp(zero_page, page_info, XLOG_BLCKSZ))
	{
		fprintf(stderr, _("content in file \"%s\" is not matched\n"), private_data->path);
		goto err;
	}
	res = 0;		

err:
	if (page_info)
		pfree(page_info);
	if (zero_page)
		pfree(zero_page);
	if (res < 0)
	{
		pfree(*mem_record);
		exit(EXIT_FAILURE);
	}
}

/* dump polar_xlog_file */
static int
polar_dump_xlog_file(char *path, XLogDumpConfig *config)
{
	polar_xlog_dump_private private;
	XLogReaderState *xlogreader_state = NULL;
	XLogRecord *mem_record = NULL;
	XLogRecord *disk_record = NULL;
	XLogRecPtr start_pageptr;
	XLogSegNo	readsegno;
	char	*errormsg = NULL;
	int		fd = -1;
	int 	res = EXIT_FAILURE;

	/* validate path */
	fd = open(path, O_RDONLY | PG_BINARY, 0);
	if (fd < 0)
		fatal_error("could not open file \"%s\"", path);
	close(fd);

	/* set private data */
	polar_read_private_data(path, &private);

	/* read memory xlog record */
	polar_read_memory_xlog((char **)&mem_record, &private);

	/* allocate xlogreader state to get disk xlogrecord */ 
	xlogreader_state = XLogReaderAllocate(private.wal_segment_size, polar_read_page, &private);
	if (!xlogreader_state)
	{
		fprintf(stderr, _("failed to allocate xlogreader, out of memory\n"));
		goto err;
	}
	xlogreader_state->ReadRecPtr = private.cur_lsn;
	
	/* set state->readsegno to avoid reading the first page of a segment, which may not exist in the file */
	start_pageptr = private.cur_lsn - (private.cur_lsn % XLOG_BLCKSZ);
	XLByteToSeg(start_pageptr, readsegno, private.wal_segment_size);
	xlogreader_state->readSegNo = readsegno;

	/* read and dump disk xlogrecord */
	disk_record = XLogReadRecord(xlogreader_state, private.cur_lsn, &errormsg);
	/* decode success */
	if (disk_record)
	{
		fprintf(stderr, _("dump display the disk xlog record:\n"));
		XLogDumpDisplayRecord(config, xlogreader_state);
	}
	else
	{
		/* decode fail, disk_record must be in state->readRecordBuf or state->readBuf */
		int size = (private.total_len / XLOG_BLCKSZ + 3) * XLOG_BLCKSZ;
		if (errormsg)
			fprintf(stderr, _("error in read disk record from file \"%s\": %s\n"), path, errormsg);
		
		Assert(private.file_size > size);
		/* disk record crossed pages */
		if (private.file_size - size > XLOG_BLCKSZ)
			disk_record = (XLogRecord *) xlogreader_state->readRecordBuf;
		else
			disk_record = (XLogRecord *)(xlogreader_state->readBuf + private.cur_lsn % XLOG_BLCKSZ);
	}
	
	/* compare disk_record and memory_record */
	if (memcmp(mem_record, disk_record, private.total_len))
	{
		/* Valid memory xlog record if it's different from disk xlog record */
		if (ValidXLogRecord(xlogreader_state, mem_record, private.cur_lsn))
		{
			errormsg = NULL;
			if (DecodeXLogRecord(xlogreader_state, mem_record, &errormsg))
			{
				/* decode success */
				fprintf(stderr, _("dump display the memory xlog record:\n"));
				XLogDumpDisplayRecord(config, xlogreader_state);
			} 
			else if (errormsg)
				fprintf(stderr, _("error in decode memory xlog record: %s\n"), errormsg);
		}
		else
			fprintf(stderr, _("error in validate memory wal record: %s\n"), xlogreader_state->errormsg_buf);
		
		/* print disk xlog record */
		fprintf(stderr, _("xlog record in disk is different from that in memory\n"));
		fprintf(stderr, _("disk xlog record is: \n"));
		polar_print_buf((char *)disk_record, private.total_len);
	}
	else
		fprintf(stderr,_("xlog record in disk is the same as that in memory\n"));
	
	/* print mem xlog record */
	fprintf(stderr, _("memory xlog record is: \n"));
	polar_print_buf((char *)mem_record, private.total_len);

	res = EXIT_SUCCESS;

err:
	if (xlogreader_state)	
		XLogReaderFree(xlogreader_state);
	if (mem_record)
		pfree(mem_record);
	return res;
}
/* POLAR end */

static void
usage(void)
{
	printf(_("%s decodes and displays PostgreSQL write-ahead logs for debugging.\n\n"),
		   progname);
	printf(_("Usage:\n"));
	printf(_("  %s [OPTION]... [STARTSEG [ENDSEG]]\n"), progname);
	printf(_("\nOptions:\n"));
	printf(_("  -b, --bkp-details      output detailed information about backup blocks\n"));
	printf(_("  -e, --end=RECPTR       stop reading at WAL location RECPTR\n"));
	printf(_("  -f, --follow           keep retrying after reaching end of WAL\n"));
	printf(_("  -n, --limit=N          number of records to display\n"));
	printf(_("  -p, --path=PATH        directory in which to find log segment files or a\n"
			 "                         directory with a ./pg_wal that contains such files\n"
			 "                         (default: current directory, ./pg_wal, $PGDATA/pg_wal)\n"));
	printf(_("  -r, --rmgr=RMGR        only show records generated by resource manager RMGR;\n"
			 "                         use --rmgr=list to list valid resource manager names\n"));
	printf(_("  -s, --start=RECPTR     start reading at WAL location RECPTR\n"));
	printf(_("  -t, --timeline=TLI     timeline from which to read log records\n"
			 "                         (default: 1 or the value used in STARTSEG)\n"));
	printf(_("  -V, --version          output version information, then exit\n"));
	printf(_("  -x, --xid=XID          only show records with transaction ID XID\n"));
	printf(_("  -z, --stats[=record]   show statistics instead of records\n"
			 "                         (optionally, show per-record statistics)\n"));
	/* POLAR: dump polar_xlog_info file */
	printf(_("  -d, --dump-file=file   dump polar_xlog_info file\n"));
	/* POLAR end */
	printf(_("  -?, --help             show this help, then exit\n"));
}

int
main(int argc, char **argv)
{
	uint32		xlogid;
	uint32		xrecoff;
	XLogReaderState *xlogreader_state;
	XLogDumpPrivate private;
	XLogDumpConfig config;
	XLogDumpStats stats;
	XLogRecord *record;
	XLogRecPtr	first_record;
	char	   *errormsg;
	/* POLAR */
	char	*polar_xlog_file = NULL;
	/* POLAR end */

	static struct option long_options[] = {
		{"bkp-details", no_argument, NULL, 'b'},
		{"end", required_argument, NULL, 'e'},
		{"follow", no_argument, NULL, 'f'},
		{"help", no_argument, NULL, '?'},
		{"limit", required_argument, NULL, 'n'},
		{"path", required_argument, NULL, 'p'},
		{"rmgr", required_argument, NULL, 'r'},
		{"start", required_argument, NULL, 's'},
		{"timeline", required_argument, NULL, 't'},
		{"xid", required_argument, NULL, 'x'},
		{"version", no_argument, NULL, 'V'},
		{"stats", optional_argument, NULL, 'z'},
		/* POLAR */
		{"dump-file", optional_argument, NULL, 'd'},
		/* POLAR end */
		{NULL, 0, NULL, 0}
	};

	int			option;
	int			optindex = 0;

	set_pglocale_pgservice(argv[0], PG_TEXTDOMAIN("pg_waldump"));
	progname = get_progname(argv[0]);

	memset(&private, 0, sizeof(XLogDumpPrivate));
	memset(&config, 0, sizeof(XLogDumpConfig));
	memset(&stats, 0, sizeof(XLogDumpStats));

	private.timeline = 1;
	private.startptr = InvalidXLogRecPtr;
	private.endptr = InvalidXLogRecPtr;
	private.endptr_reached = false;

	config.bkp_details = false;
	config.stop_after_records = -1;
	config.already_displayed_records = 0;
	config.follow = false;
	config.filter_by_rmgr = -1;
	config.filter_by_xid = InvalidTransactionId;
	config.filter_by_xid_enabled = false;
	config.stats = false;
	config.stats_per_record = false;

	if (argc <= 1)
	{
		fprintf(stderr, _("%s: no arguments specified\n"), progname);
		goto bad_argument;
	}

	while ((option = getopt_long(argc, argv, "be:?fn:p:r:s:t:Vx:zd:",
								 long_options, &optindex)) != -1)
	{
		switch (option)
		{
			case 'b':
				config.bkp_details = true;
				break;
			case 'e':
				if (sscanf(optarg, "%X/%X", &xlogid, &xrecoff) != 2)
				{
					fprintf(stderr, _("%s: could not parse end WAL location \"%s\"\n"),
							progname, optarg);
					goto bad_argument;
				}
				private.endptr = (uint64) xlogid << 32 | xrecoff;
				break;
			case 'f':
				config.follow = true;
				break;
			case '?':
				usage();
				exit(EXIT_SUCCESS);
				break;
			case 'n':
				if (sscanf(optarg, "%d", &config.stop_after_records) != 1)
				{
					fprintf(stderr, _("%s: could not parse limit \"%s\"\n"),
							progname, optarg);
					goto bad_argument;
				}
				break;
			case 'p':
				private.inpath = pg_strdup(optarg);
				break;
			case 'r':
				{
					int			i;

					if (pg_strcasecmp(optarg, "list") == 0)
					{
						print_rmgr_list();
						exit(EXIT_SUCCESS);
					}

					for (i = 0; i <= RM_MAX_ID; i++)
					{
						if (pg_strcasecmp(optarg, RmgrDescTable[i].rm_name) == 0)
						{
							config.filter_by_rmgr = i;
							break;
						}
					}

					if (config.filter_by_rmgr == -1)
					{
						fprintf(stderr, _("%s: resource manager \"%s\" does not exist\n"),
								progname, optarg);
						goto bad_argument;
					}
				}
				break;
			case 's':
				if (sscanf(optarg, "%X/%X", &xlogid, &xrecoff) != 2)
				{
					fprintf(stderr, _("%s: could not parse start WAL location \"%s\"\n"),
							progname, optarg);
					goto bad_argument;
				}
				else
					private.startptr = (uint64) xlogid << 32 | xrecoff;
				break;
			case 't':
				if (sscanf(optarg, "%d", &private.timeline) != 1)
				{
					fprintf(stderr, _("%s: could not parse timeline \"%s\"\n"),
							progname, optarg);
					goto bad_argument;
				}
				break;
			case 'V':
				puts("pg_waldump (PostgreSQL) " PG_VERSION);
				exit(EXIT_SUCCESS);
				break;
			case 'x':
				if (sscanf(optarg, "%u", &config.filter_by_xid) != 1)
				{
					fprintf(stderr, _("%s: could not parse \"%s\" as a transaction ID\n"),
							progname, optarg);
					goto bad_argument;
				}
				config.filter_by_xid_enabled = true;
				break;
			case 'z':
				config.stats = true;
				config.stats_per_record = false;
				if (optarg)
				{
					if (strcmp(optarg, "record") == 0)
						config.stats_per_record = true;
					else if (strcmp(optarg, "rmgr") != 0)
					{
						fprintf(stderr, _("%s: unrecognized argument to --stats: %s\n"),
								progname, optarg);
						goto bad_argument;
					}
				}
				break;
			/* POLAR */
			case 'd':
				polar_xlog_file = pg_strdup(optarg);
				break;
			/* POLAR end */
			default:
				goto bad_argument;
		}
	}

	if ((optind + 2) < argc)
	{
		fprintf(stderr,
				_("%s: too many command-line arguments (first is \"%s\")\n"),
				progname, argv[optind + 2]);
		goto bad_argument;
	}

	/* POLAR: dump polar_xlog_file */
	if (polar_xlog_file != NULL)
	{
		int ret = -1;
		ret = polar_dump_xlog_file(polar_xlog_file, &config);
		return ret;
	}
	/* POLAR end */

	if (private.inpath != NULL)
	{
		/* validate path points to directory */
		if (!verify_directory(private.inpath))
		{
			fprintf(stderr,
					_("%s: path \"%s\" could not be opened: %s\n"),
					progname, private.inpath, strerror(errno));
			goto bad_argument;
		}
	}

	/* parse files as start/end boundaries, extract path if not specified */
	if (optind < argc)
	{
		char	   *directory = NULL;
		char	   *fname = NULL;
		int			fd;
		XLogSegNo	segno;

		split_path(argv[optind], &directory, &fname);

		if (private.inpath == NULL && directory != NULL)
		{
			private.inpath = directory;

			if (!verify_directory(private.inpath))
				fatal_error("could not open directory \"%s\": %s",
							private.inpath, strerror(errno));
		}

		identify_target_directory(&private, private.inpath, fname);
		fd = open_file_in_directory(private.inpath, fname);
		if (fd < 0)
			fatal_error("could not open file \"%s\"", fname);
		close(fd);

		/* parse position from file */
		XLogFromFileName(fname, &private.timeline, &segno, WalSegSz);

		if (XLogRecPtrIsInvalid(private.startptr))
			XLogSegNoOffsetToRecPtr(segno, 0, WalSegSz, private.startptr);
		else if (!XLByteInSeg(private.startptr, segno, WalSegSz))
		{
			fprintf(stderr,
					_("%s: start WAL location %X/%X is not inside file \"%s\"\n"),
					progname,
					(uint32) (private.startptr >> 32),
					(uint32) private.startptr,
					fname);
			goto bad_argument;
		}

		/* no second file specified, set end position */
		if (!(optind + 1 < argc) && XLogRecPtrIsInvalid(private.endptr))
			XLogSegNoOffsetToRecPtr(segno + 1, 0, WalSegSz, private.endptr);

		/* parse ENDSEG if passed */
		if (optind + 1 < argc)
		{
			XLogSegNo	endsegno;

			/* ignore directory, already have that */
			split_path(argv[optind + 1], &directory, &fname);

			fd = open_file_in_directory(private.inpath, fname);
			if (fd < 0)
				fatal_error("could not open file \"%s\"", fname);
			close(fd);

			/* parse position from file */
			XLogFromFileName(fname, &private.timeline, &endsegno, WalSegSz);

			if (endsegno < segno)
				fatal_error("ENDSEG %s is before STARTSEG %s",
							argv[optind + 1], argv[optind]);

			if (XLogRecPtrIsInvalid(private.endptr))
				XLogSegNoOffsetToRecPtr(endsegno + 1, 0, WalSegSz,
										private.endptr);

			/* set segno to endsegno for check of --end */
			segno = endsegno;
		}


		if (!XLByteInSeg(private.endptr, segno, WalSegSz) &&
			private.endptr != (segno + 1) * WalSegSz)
		{
			fprintf(stderr,
					_("%s: end WAL location %X/%X is not inside file \"%s\"\n"),
					progname,
					(uint32) (private.endptr >> 32),
					(uint32) private.endptr,
					argv[argc - 1]);
			goto bad_argument;
		}
	}
	else
		identify_target_directory(&private, private.inpath, NULL);

	/* we don't know what to print */
	if (XLogRecPtrIsInvalid(private.startptr))
	{
		fprintf(stderr, _("%s: no start WAL location given\n"), progname);
		goto bad_argument;
	}

	/* done with argument parsing, do the actual work */

	/* we have everything we need, start reading */
	xlogreader_state = XLogReaderAllocate(WalSegSz, XLogDumpReadPage,
										  &private);
	if (!xlogreader_state)
		fatal_error("out of memory");

	/* first find a valid recptr to start from */
	first_record = XLogFindNextRecord(xlogreader_state, private.startptr);

	if (first_record == InvalidXLogRecPtr)
		fatal_error("could not find a valid record after %X/%X",
					(uint32) (private.startptr >> 32),
					(uint32) private.startptr);

	/*
	 * Display a message that we're skipping data if `from` wasn't a pointer
	 * to the start of a record and also wasn't a pointer to the beginning of
	 * a segment (e.g. we were used in file mode).
	 */
	if (first_record != private.startptr &&
		XLogSegmentOffset(private.startptr, WalSegSz) != 0)
		printf(ngettext("first record is after %X/%X, at %X/%X, skipping over %u byte\n",
						"first record is after %X/%X, at %X/%X, skipping over %u bytes\n",
						(first_record - private.startptr)),
			   (uint32) (private.startptr >> 32), (uint32) private.startptr,
			   (uint32) (first_record >> 32), (uint32) first_record,
			   (uint32) (first_record - private.startptr));

	for (;;)
	{
		/* try to read the next record */
		record = XLogReadRecord(xlogreader_state, first_record, &errormsg);
		if (!record)
		{
			if (!config.follow || private.endptr_reached)
				break;
			else
			{
				pg_usleep(1000000L);	/* 1 second */
				continue;
			}
		}

		/* after reading the first record, continue at next one */
		first_record = InvalidXLogRecPtr;

		/* apply all specified filters */
		if (config.filter_by_rmgr != -1 &&
			config.filter_by_rmgr != record->xl_rmid)
			continue;

		if (config.filter_by_xid_enabled &&
			config.filter_by_xid != record->xl_xid)
			continue;

		/* process the record */
		if (config.stats == true)
			XLogDumpCountRecord(&config, &stats, xlogreader_state);
		else
			XLogDumpDisplayRecord(&config, xlogreader_state);

		/* check whether we printed enough */
		config.already_displayed_records++;
		if (config.stop_after_records > 0 &&
			config.already_displayed_records >= config.stop_after_records)
			break;
	}

	if (config.stats == true)
		XLogDumpDisplayStats(&config, &stats);

	if (errormsg)
		fatal_error("error in WAL record at %X/%X: %s",
					(uint32) (xlogreader_state->ReadRecPtr >> 32),
					(uint32) xlogreader_state->ReadRecPtr,
					errormsg);

	XLogReaderFree(xlogreader_state);
	return EXIT_SUCCESS;

bad_argument:
	fprintf(stderr, _("Try \"%s --help\" for more information.\n"), progname);
	return EXIT_FAILURE;
}
