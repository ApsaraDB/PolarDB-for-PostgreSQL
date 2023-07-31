/*-------------------------------------------------------------------------
 *
 * flashback_log_file_dump.c - decode and display flashback log
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
 *        src/bin/polar_tools/flashback_log_file_dump.c
 *-------------------------------------------------------------------------
 */
#include <dirent.h>
#include <libgen.h>
#include <stdio.h>
#include <sys/stat.h>
#include <unistd.h>

#include "postgres.h"

#include "access/xlogdefs.h"
#include "access/transam.h"
#include "common/pg_lzcompress.h"
#include "fe_utils/timestamp.h"
#include "polar_flashback/polar_flashback_log_file.h"
#include "polar_flashback/polar_flashback_log_reader.h"
#include "polar_flashback/polar_flashback_log_record.h"
#include "polar_flashback/polar_flashback_rel_filenode.h"
#include "polar_tools.h"
#include "utils/datetime.h"
/*no cover begin*/
/*  checksum_impl.h uses Assert, which doesn't work outside the server */
#undef Assert
#define Assert(X)
#include "storage/checksum.h"
#include "storage/checksum_impl.h"
#include "storage/bufpage.h"

/* size of the buffer allocated for error message. */
#define MAX_ERRORMSG_LEN 1000
static const char *progname;
static int  segment_size;
static bool just_version = false;
static bool just_one_record_without_check = false;

/* load the flashback log switch ptrs */
#define load_switch_ptrs(dir, ptrs) \
	(ptrs != NIL? ptrs : flog_read_history_file(dir))

typedef struct dump_private
{
	char       *inpath;
	polar_flog_rec_ptr  startptr;
	polar_flog_rec_ptr  endptr;
	bool        endptr_reached;
} dump_private;

typedef struct dump_config
{
	/* display options */
	bool        bkp_details;
	int         stop_after_records;
	int         already_displayed_records;
	bool        follow;
	bool        checksum;

	/* filter options */
	uint8           filter_by_type;
	RelFileNode     rel_file_node;
} dump_config;

/* an flashback log_BLCKSZ-sized buffer */
typedef union pg_aligned_flashback_log_blk
{
	char        data[POLAR_FLOG_BLCKSZ];
	double      force_align_d;
	int64       force_align_i64;
} pg_aligned_flashback_log_blk;

static List *switch_ptr_list = NIL;

const char *flog_record_types[FLOG_REC_TYPES + 1] = FLOG_RECORD_TYPES;

static void fatal_error(const char *fmt, ...) pg_attribute_printf(1, 2);
static void report_invalid_flog_record(flog_reader_state *state,
									   const char *fmt, ...) pg_attribute_printf(2, 3);

/*
 * Big red button to push when things go horribly wrong.
 */
static void
fatal_error(const char *fmt, ...)
{
	va_list     args;

	fflush(stdout);

	fprintf(stderr, _("\n%s: FATAL:  "), progname);
	va_start(args, fmt);
	vfprintf(stderr, _(fmt), args);
	va_end(args);
	fputc('\n', stderr);

	exit(EXIT_FAILURE);
}

static void
print_type_list(void)
{
	int         i;

	for (i = 0; i < FLOG_REC_TYPES; i++)
		printf("%s\n", flog_record_types[i]);
}

/*
 * Check whether directory exists and whether we can open it. Keep errno set so
 * that the caller can report errors somewhat more accurately.
 */
static bool
verify_directory(const char *directory)
{
	DIR        *dir = opendir(directory);

	if (dir == NULL)
		return false;

	closedir(dir);
	return true;
}

/*
 * Allocate readRecordBuf to fit a record of at least the given length.
 * Returns true if successful, false if out of memory.
 *
 * readRecordBufSize is set to the new buffer size.
 *
 * To avoid useless small increases, round its size to a multiple of
 * POLAR_FLASHBACK_LOG_BLCKSZ, and make sure it's at least 3*Max(BLCKSZ, POLAR_FLASHBACK_LOG_BLCKSZ) to start
 * with.  (That is enough for all "normal" records, but very large commit or
 * abort records might need more space.)
 */
static bool
allocate_flog_recordbuf(flog_reader_state *state, uint32 reclength)
{
	uint32      newSize = reclength;

	newSize += POLAR_FLOG_BLCKSZ - (newSize % POLAR_FLOG_BLCKSZ);
	newSize = Max(newSize, 3 * Max(BLCKSZ, POLAR_FLOG_BLCKSZ));

	if (state->read_record_buf)
		pfree(state->read_record_buf);

	state->read_record_buf =
		(char *) palloc_extended(newSize, MCXT_ALLOC_NO_OOM);

	if (state->read_record_buf == NULL)
	{
		state->read_record_buf_size = 0;
		return false;
	}

	state->read_record_buf_size = newSize;
	return true;
}

/*
 * Allocate and initialize a new flashback log reader.
 * A copy from polar_flashback_reader.c.
 *
 * Returns NULL if the flashback log reader couldn't be allocated.
 */
static flog_reader_state *
flog_reader_allocate(int segment_size, page_read_callback pagereadfunc,
					 void *private_data)
{
	flog_reader_state *state;

	state = (flog_reader_state *)
			palloc_extended(sizeof(flog_reader_state),
							MCXT_ALLOC_NO_OOM | MCXT_ALLOC_ZERO);

	if (!state)
		return NULL;

	/*
	 * Permanently allocate readBuf.  We do it this way, rather than just
	 * making a static array, for two reasons: (1) no need to waste the
	 * storage in most instantiations of the backend; (2) a static char array
	 * isn't guaranteed to have any particular alignment, whereas
	 * palloc_extended() will provide MAXALIGN'd storage.
	 */
	state->read_buf = (char *) palloc_extended(POLAR_FLOG_BLCKSZ,
											   MCXT_ALLOC_NO_OOM);

	if (!state->read_buf)
	{
		pfree(state);
		return NULL;
	}

	state->segment_size = segment_size;
	state->read_page = pagereadfunc;
	/* system_identifier initialized to zeroes above */
	state->private_data = private_data;
	/* ReadRecPtr and EndRecPtr initialized to zeroes above */
	/* readSegNo, readOff, readLen, readPageTLI initialized to zeroes above */
	state->errormsg_buf = palloc_extended(MAX_ERRORMSG_LEN + 1,
										  MCXT_ALLOC_NO_OOM);

	if (!state->errormsg_buf)
	{
		pfree(state->read_buf);
		pfree(state);
		return NULL;
	}

	state->errormsg_buf[0] = '\0';
	state->switch_ptrs = NIL;
	state->in_switch_region = false;

	/*
	 * Allocate an initial readRecordBuf of minimal size, which can later be
	 * enlarged if necessary.
	 */
	if (!allocate_flog_recordbuf(state, 0))
	{
		pfree(state->errormsg_buf);
		pfree(state->read_buf);
		pfree(state);
		return NULL;
	}

	return state;
}

static void
flog_reader_free(flog_reader_state *state)
{
	pfree(state->errormsg_buf);

	if (state->read_record_buf)
		pfree(state->read_record_buf);

	pfree(state->read_buf);
	pfree(state);
}

/*
 * Invalidate the flashback_log_reader's read state to force a re-read.
 */
static void
flog_inval_read_state(flog_reader_state *state)
{
	state->read_seg_no = 0;
	state->read_off = 0;
	state->read_len = 0;
}

/*
 * Construct a string in state->errormsg_buf explaining what's wrong with
 * the current record being read.
 */
static void
report_invalid_flog_record(flog_reader_state *state,
						   const char *fmt, ...)
{
	va_list     args;

	fmt = _(fmt);

	va_start(args, fmt);
	vsnprintf(state->errormsg_buf, MAX_ERRORMSG_LEN, fmt, args);
	va_end(args);
}

static bool
is_file_exist(const char *path)
{
	struct stat st;

	if (stat(path, &st) == 0)
		return true;
	else
		return false;
}

static ssize_t
read_line(FILE *fp, void *buffer, size_t n)
{
	size_t tot_read;                     /* Total bytes read so far */
	char *buf;
	char ch;

	if (n <= 0 || buffer == NULL)
	{
		errno = EINVAL;
		return -1;
	}

	buf = buffer;                       /* No pointer arithmetic on "void *" */

	tot_read = 0;

	for (;;)
	{
		ch = fgetc(fp);

		if (ch == EOF)       /* EOF */
		{
			if (tot_read == 0)           /* No bytes read; return 0 */
				return 0;
			else                        /* Some bytes read; add '\0' */
				break;
		}
		else                            /* 'numRead' must be 1 if we get here */
		{
			if (tot_read < n - 1)        /* Discard > (n - 1) bytes */
			{
				tot_read++;
				*buf++ = ch;
			}

			if (ch == '\n')
				break;
		}
	}

	*buf = '\0';
	return tot_read;
}

/* ---------------------List ------------------------ */

/*
 * -------------
 *  Definitions
 * -------------
 */

static List *
new_list(NodeTag type)
{
	List       *new_list;
	ListCell   *new_head;

	new_head = (ListCell *) palloc(sizeof(*new_head));
	new_head->next = NULL;
	/* new_head->data is left undefined! */

	new_list = (List *) palloc(sizeof(*new_list));
	new_list->type = type;
	new_list->length = 1;
	new_list->head = new_head;
	new_list->tail = new_head;

	return new_list;
}

static void
new_head_cell(List *list)
{
	ListCell   *new_head;

	new_head = (ListCell *) palloc(sizeof(*new_head));
	new_head->next = list->head;

	list->head = new_head;
	list->length++;
}

static void
check_list_invariants(const List *list)
{
	if (list == NIL)
		return;

	Assert(list->length > 0);
	Assert(list->head != NULL);
	Assert(list->tail != NULL);

	Assert(list->type == T_List ||
		   list->type == T_IntList ||
		   list->type == T_OidList);

	if (list->length == 1)
		Assert(list->head == list->tail);

	if (list->length == 2)
		Assert(list->head->next == list->tail);

	Assert(list->tail->next == NULL);
}

List *
lcons(void *datum, List *list)
{
	Assert(IsPointerList(list));

	if (list == NIL)
		list = new_list(T_List);
	else
		new_head_cell(list);

	lfirst(list->head) = datum;
	check_list_invariants(list);
	return list;
}

/*
 * POLAR: Try to read a flashback log history file.
 *
 * The history content is stored in the switch_ptr_list.
 */
static List *
flog_read_history_file(const char *dir)
{
	char        path[MAXPGPATH];
	char        fline[MAXPGPATH];
	polar_flog_rec_ptr switch_ptr = POLAR_INVALID_FLOG_REC_PTR;
	polar_flog_rec_ptr last_ptr = POLAR_INVALID_FLOG_REC_PTR;
	polar_flog_rec_ptr next_ptr = POLAR_INVALID_FLOG_REC_PTR;
	FILE *fp;
	List *list = NIL;

	snprintf(path, MAXPGPATH, "%s/%s", dir, FLOG_HISTORY_FILE);

	if (!is_file_exist(path))
		return list;

	fp = fopen(path, "r");

	if(fp == NULL)
		fatal_error("could not open file \"%s\": %s",
					path, strerror(errno));

	while (read_line(fp, fline, sizeof(fline)) > 0)
	{
		uint32      switchpoint_hi;
		uint32      switchpoint_lo;
		uint32      nextpoint_hi;
		uint32      nextpoint_lo;
		TimeLineID  tli;
		int         nfields;
		char       *ptr;
		flog_history_entry *entry;

		/* skip leading whitespace and check for # comment */
		for (ptr = fline; *ptr; ptr++)
		{
			if (!isspace((unsigned char) *ptr))
				break;
		}

		if (*ptr == '\0' || *ptr == '#')
			continue;

		nfields = sscanf(fline, "%u\t%08X/%08X\t%08X/%08X", &tli, &switchpoint_hi, &switchpoint_lo,
						 &nextpoint_hi, &nextpoint_lo);

		if (nfields != HISTORY_FILE_FIELDS)
			fatal_error("syntax error in flashback log history file: %s. "
						"Expected a flashback log switch point and next point location.",
						fline);

		switch_ptr = ((polar_flog_rec_ptr)(switchpoint_hi)) << 32 |
					 (polar_flog_rec_ptr) switchpoint_lo;
		next_ptr = ((polar_flog_rec_ptr)(nextpoint_hi)) << 32 |
				   (polar_flog_rec_ptr) nextpoint_lo;

		if (next_ptr == POLAR_INVALID_FLOG_REC_PTR ||
				switch_ptr >= next_ptr)
			fatal_error("invalid data in flashback log history file: %s. "
						"switch point and next point must be valid and next point"
						" is larger than or equal to switch point.", fline);

		if (last_ptr && last_ptr > switch_ptr)
			fatal_error("invalid data in flashback log history file: %s. "
						"switch point location must be in increasing sequence", fline);

		entry = (flog_history_entry *) palloc(sizeof(flog_history_entry));
		entry->tli = tli;
		entry->switch_ptr = switch_ptr;
		entry->next_ptr = next_ptr;
		/* Build list with newest item first */
		list = lcons(entry, list);
		last_ptr = next_ptr;
		/* we ignore the remainder of each line */
	}
	return list;
}

/*
 * Converts a "usable byte position" to polar_flog_rec_ptr. A usable byte position
 * is the position starting from the beginning of flashback log, excluding all flashback log
 * page headers.
 */
static polar_flog_rec_ptr
flog_pos2ptr(uint64 bytepos)
{
	uint64      fullsegs;
	uint64      fullpages;
	uint64      bytesleft;
	uint32      seg_offset;
	polar_flog_rec_ptr  result;

	fullsegs = bytepos / USABLE_BYTES_IN_SEG;
	bytesleft = bytepos % USABLE_BYTES_IN_SEG;

	if (bytesleft < POLAR_FLOG_BLCKSZ - FLOG_LONG_PHD_SIZE)
	{
		/* fits on first page of segment */
		seg_offset = bytesleft + FLOG_LONG_PHD_SIZE;
	}
	else
	{
		/* account for the first page on segment with long header */
		seg_offset = POLAR_FLOG_BLCKSZ;
		bytesleft -= POLAR_FLOG_BLCKSZ - FLOG_LONG_PHD_SIZE;

		fullpages = bytesleft / USABLE_BYTES_IN_PAGE;
		bytesleft = bytesleft % USABLE_BYTES_IN_PAGE;

		seg_offset += fullpages * POLAR_FLOG_BLCKSZ + bytesleft + FLOG_SHORT_PHD_SIZE;
	}

	FLOG_SEG_OFFSET_TO_PTR(fullsegs, seg_offset, POLAR_FLOG_SEG_SIZE, result);
	return result;
}

/*
 * Convert an flashback log ptr to a "usable byte position".
 */
static uint64
flog_ptr2pos(polar_flog_rec_ptr ptr)
{
	uint64      fullsegs;
	uint32      fullpages;
	uint32      offset;
	uint64      result;

	fullsegs = FLOG_PTR_TO_SEG(ptr, POLAR_FLOG_SEG_SIZE);

	fullpages = (FLOG_SEGMENT_OFFSET(ptr, POLAR_FLOG_SEG_SIZE)) / POLAR_FLOG_BLCKSZ;
	offset = ptr % POLAR_FLOG_BLCKSZ;

	if (fullpages == 0)
	{
		result = fullsegs * USABLE_BYTES_IN_SEG;

		if (offset > 0)
		{
			Assert(offset >= FLOG_LONG_PHD_SIZE);
			result += offset - FLOG_LONG_PHD_SIZE;
		}
	}
	else
	{
		result = fullsegs * USABLE_BYTES_IN_SEG +
				 (POLAR_FLOG_BLCKSZ - FLOG_LONG_PHD_SIZE) + /* account for first page */
				 (fullpages - 1) * USABLE_BYTES_IN_PAGE;    /* full pages */

		if (offset > 0)
		{
			Assert(offset >= FLOG_SHORT_PHD_SIZE);
			result += offset - FLOG_SHORT_PHD_SIZE;
		}
	}

	return result;
}

/*
 * POLAR: If the flashback log point a switch point whose prev_lsn is wrong.
 *
 */
static bool
check_switch_ptr_prev_lsn(polar_flog_rec_ptr ptr,
						  flog_record *rec)
{
	ListCell   *cell;

	ptr -= FLOG_LONG_PHD_SIZE;

	/* Check first */
	if (ptr % POLAR_FLOG_SEG_SIZE != 0 ||
			rec->xl_prev != POLAR_INVALID_FLOG_REC_PTR)
		return false;

	/* Check strictly */
	foreach (cell, switch_ptr_list)
	{
		flog_history_entry *tle = (flog_history_entry *) lfirst(cell);

		if (tle->next_ptr == ptr)
			return true;
	}

	return false;
}

/*
 * Validate an flashback log record header.
 *
 * This is just a convenience subroutine to avoid duplicated code in
 * polar_read_flashback_log_record.  It's not intended for use from anywhere else.
 */
static bool
valid_flog_rec_header(flog_reader_state *state,
					  polar_flog_rec_ptr RecPtr, polar_flog_rec_ptr PrevRecPtr,
					  flog_record *record, bool randAccess)
{
	if (record->xl_tot_len < FLOG_REC_HEADER_SIZE)
	{
		report_invalid_flog_record(state,
								   "invalid flashback log record length at %X/%X: wanted larger than or "
								   "equal to %u, got %u",
								   (uint32)(RecPtr >> 32), (uint32) RecPtr,
								   (uint32) FLOG_REC_HEADER_SIZE, record->xl_tot_len);
		return false;
	}

	if (randAccess)
	{
		/*
		 * We can't exactly verify the prev-link, but surely it should be less
		 * than the record's own address.
		 */
		if (record->xl_prev >= RecPtr)
		{
			report_invalid_flog_record(state,
									   "flashback log record with incorrect prev-link %X/%X at %X/%X",
									   (uint32)(record->xl_prev >> 32),
									   (uint32) record->xl_prev,
									   (uint32)(RecPtr >> 32), (uint32) RecPtr);
			return false;
		}
	}
	else
	{
		/*
		 * Record's prev-link should exactly match our previous location. This
		 * check guards against torn flashbakc log pages where a stale but valid-looking
		 * flashback log record starts on a sector boundary.
		 */
		if (record->xl_prev != PrevRecPtr)
		{
			if (check_switch_ptr_prev_lsn(RecPtr, record))
				return true;

			report_invalid_flog_record(state,
									   "flashback log record with incorrect prev-link %X/%X at %X/%X",
									   (uint32)(record->xl_prev >> 32),
									   (uint32) record->xl_prev,
									   (uint32)(RecPtr >> 32), (uint32) RecPtr);
			return false;
		}
	}

	return true;
}

/*
 * CRC-check an flashback log record.  We do not believe the contents of an
 * flashback log record (other than to the minimal extent of computing
 * the amount of data to read in) until we've checked the CRCs.
 *
 * We assume all of the record (that is, xl_tot_len bytes) has been read
 * into memory at *record.  Also, flog_page_header_validate()
 * has accepted the record's header, which means in particular that
 * xl_tot_len is at least FLASHBACK_LOG_REC_SIZE.
 */
static bool
valid_flog_rec(flog_reader_state *state,
			   flog_record *record, polar_flog_rec_ptr recptr)
{
	pg_crc32c   crc;

	/* Calculate the CRC */
	INIT_CRC32C(crc);
	COMP_CRC32C(crc, ((char *) record) + FLOG_REC_HEADER_SIZE,
				record->xl_tot_len - FLOG_REC_HEADER_SIZE);
	/* include the record header last */
	COMP_CRC32C(crc, (char *) record, offsetof(flog_record, xl_crc));
	FIN_CRC32C(crc);

	if (!EQ_CRC32C(record->xl_crc, crc))
	{
		report_invalid_flog_record(state,
								   "incorrect data crc in flashback log "
								   "record at %X/%X",
								   (uint32)(recptr >> 32), (uint32) recptr);
		return false;
	}

	return true;
}

/*
 * Validate a flsahback log page header.
 *
 * Check if 'phdr' is valid as the header of the flashback log page at position
 * 'recptr'.
 */
static bool
flog_page_header_validate(flog_reader_state *state,
						  polar_flog_rec_ptr recptr, char *phdr)
{
	polar_flog_rec_ptr  recaddr;
	uint64  segno;
	int32       offset;
	flog_page_header hdr = (flog_page_header) phdr;

	Assert((recptr % POLAR_FLOG_BLCKSZ) == 0);

	segno = FLOG_PTR_TO_SEG(recptr, state->segment_size);
	offset = FLOG_SEGMENT_OFFSET(recptr, state->segment_size);

	FLOG_SEG_OFFSET_TO_PTR(segno, offset, state->segment_size, recaddr);

	if (hdr->xlp_magic != FLOG_PAGE_MAGIC)
	{
		char        fname[FLOG_MAX_FNAME_LEN];

		FLOG_GET_FNAME(fname, segno, state->segment_size, FLOG_DEFAULT_TIMELINE);

		report_invalid_flog_record(state,
								   "invalid magic number %04X in flashback log segment %s, offset %u",
								   hdr->xlp_magic,
								   fname,
								   offset);
		return false;
	}

	if (hdr->xlp_version < FLOG_PAGE_VERSION)
	{
		char        fname[FLOG_MAX_FNAME_LEN];
		FLOG_GET_FNAME(fname, segno, state->segment_size, FLOG_DEFAULT_TIMELINE);
		report_invalid_flog_record(state,
								   "invalid version %04X in flashback log segment %s, offset %u",
								   hdr->xlp_version,
								   fname,
								   offset);
		return false;
	}

	if ((hdr->xlp_info & ~FLOG_ALL_FLAGS) != 0)
	{
		char        fname[FLOG_MAX_FNAME_LEN];

		FLOG_GET_FNAME(fname, segno, state->segment_size, FLOG_DEFAULT_TIMELINE);

		report_invalid_flog_record(state,
								   "invalid info bits %04X in flashback log segment %s, offset %u",
								   hdr->xlp_info,
								   fname,
								   offset);
		return false;
	}

	if (hdr->xlp_info & FLOG_LONG_PAGE_HEADER)
	{
		polar_long_page_header longhdr = (polar_long_page_header) hdr;

		if (state->system_identifier &&
				longhdr->xlp_sysid != state->system_identifier)
		{
			char        fhdrident_str[32];
			char        sysident_str[32];

			/*
			 * Format sysids separately to keep platform-dependent format code
			 * out of the translatable message string.
			 */
			snprintf(fhdrident_str, sizeof(fhdrident_str), UINT64_FORMAT,
					 longhdr->xlp_sysid);
			snprintf(sysident_str, sizeof(sysident_str), UINT64_FORMAT,
					 state->system_identifier);
			report_invalid_flog_record(state,
									   "flashback log file is from different database system: "
									   "flashback log file database system identifier is %s, "
									   "pg_control database system identifier is %s",
									   fhdrident_str, sysident_str);
			return false;
		}
		else if (longhdr->xlp_seg_size != state->segment_size)
		{
			report_invalid_flog_record(state,
									   "flashback log file is from different database system: "
									   "incorrect segment size in page header");
			return false;
		}
		else if (longhdr->xlp_blcksz != POLAR_FLOG_BLCKSZ)
		{
			report_invalid_flog_record(state,
									   "flashback log file is from different database system: "
									   "incorrect POLAR_FLASHBACK_LOG_BLCKSZ in page header");
			return false;
		}
	}
	else if (offset == 0)
	{
		char        fname[FLOG_MAX_FNAME_LEN];

		FLOG_GET_FNAME(fname, segno, state->segment_size, FLOG_DEFAULT_TIMELINE);

		/* hmm, first page of file doesn't have a long header? */
		report_invalid_flog_record(state,
								   "invalid info bits %04X in flashback log segment %s, offset %u",
								   hdr->xlp_info,
								   fname,
								   offset);
		return false;
	}

	/*
	 * Check that the address on the page agrees with what we expected. This
	 * check typically fails when an old flashback log segment is recycled,
	 * and hasn't yet been overwritten with new data yet.
	 */
	if (hdr->xlp_pageaddr != recaddr)
	{
		char        fname[FLOG_MAX_FNAME_LEN];

		FLOG_GET_FNAME(fname, segno, state->segment_size, FLOG_DEFAULT_TIMELINE);

		report_invalid_flog_record(state,
								   "unexpected pageaddr %X/%X in flashback log segment %s, offset %u",
								   (uint32)(hdr->xlp_pageaddr >> 32), (uint32) hdr->xlp_pageaddr,
								   fname,
								   offset);
		return false;
	}

	state->latest_page_ptr = recptr;

	return true;
}

/* POLAR: Get the end+1 of the flashback log */
static polar_flog_rec_ptr
get_flog_end_ptr(polar_flog_rec_ptr ptr, uint32 log_len)
{
	polar_flog_rec_ptr next_ptr = POLAR_INVALID_FLOG_REC_PTR;
	polar_flog_rec_ptr pos;

	if (log_len == 0)
		return ptr;

	pos = flog_ptr2pos(ptr) + MAXALIGN(log_len);
	next_ptr = flog_pos2ptr(pos);
	return next_ptr;
}

/*
 * POLAR: Check the invaild flashback log record is expected?
 *
 * Change the ptr to next_ptr.
 */
static bool
is_flog_rec_ignore(polar_flog_rec_ptr *ptr, uint32 log_len)
{
	ListCell   *cell;
	polar_flog_rec_ptr end_ptr = 0;

	end_ptr = get_flog_end_ptr(*ptr, log_len);

	foreach (cell, switch_ptr_list)
	{
		flog_history_entry *tle = (flog_history_entry *) lfirst(cell);

		if ((end_ptr > tle->switch_ptr && *ptr < tle->next_ptr) ||
				*ptr == tle->switch_ptr)
		{
			printf("\nThe flashback log record at %X/%X will be ignore. and switch to "
				   "%X/%X\n", (uint32)(*ptr >> 32), (uint32)(*ptr),
				   (uint32)((tle->next_ptr + FLOG_LONG_PHD_SIZE) >> 32),
				   (uint32)(tle->next_ptr + FLOG_LONG_PHD_SIZE));
			*ptr = tle->next_ptr + FLOG_LONG_PHD_SIZE;
			return true;
		}
		/* We have check it in the before */
		else if (*ptr > tle->next_ptr)
			return false;
	}

	return false;
}

/* Make the invalid ptr to first record ptr in flashback log */
static polar_flog_rec_ptr
valid_flog_ptr(polar_flog_rec_ptr ptr)
{
	polar_flog_rec_ptr valid_ptr = ptr;

	if (valid_ptr % POLAR_FLOG_SEG_SIZE == 0)
		valid_ptr += FLOG_LONG_PHD_SIZE;
	else if (valid_ptr % POLAR_FLOG_BLCKSZ == 0)
		valid_ptr += FLOG_SHORT_PHD_SIZE;

	return valid_ptr;
}

/*
 * Read a single flashback log page including at least [pageptr, reqLen] of valid data
 * via the read_page() callback.
 *
 * Returns -1 if the required page cannot be read for some reason; errormsg_buf
 * is set in that case (unless the error occurs in the read_page callback).
 *
 * We fetch the page from a reader-local cache if we know we have the required
 * data and if there hasn't been any error since caching the data.
 */
static int
read_flog_page_internal(flog_reader_state *state,
						polar_flog_rec_ptr pageptr, int reqLen)
{
	int         readLen;
	uint32      targetPageOff;
	uint64      targetSegNo;
	flog_page_header hdr;

	Assert((pageptr % POLAR_FLOG_BLCKSZ) == 0);

	targetSegNo = FLOG_PTR_TO_SEG(pageptr, state->segment_size);

	targetPageOff = FLOG_SEGMENT_OFFSET(pageptr, state->segment_size);

	/* check whether we have all the requested data already */
	if (targetSegNo == state->read_seg_no && targetPageOff == state->read_off &&
			reqLen < state->read_len)
		return state->read_len;

	/*
	 * Data is not in our buffer.
	 *
	 * Every time we actually read the page, even if we looked at parts of it
	 * before, we need to do verification as the read_page callback might now
	 * be rereading data from a different source.
	 *
	 * Whenever switching to a new segment, we read the first page of the
	 * file and validate its header, even if that's not where the target
	 * record is.  This is so that we can check the additional identification
	 * info that is present in the first page's "long" header.
	 */
	if (targetSegNo != state->read_seg_no && targetPageOff != 0)
	{
		polar_flog_rec_ptr  targetSegmentPtr = pageptr - targetPageOff;

		readLen = state->read_page(state, targetSegmentPtr, POLAR_FLOG_BLCKSZ,
								   state->curr_rec_ptr, state->read_buf);

		if (readLen < 0)
			goto err;

		/* we can be sure to have enough flashback log available, we scrolled back */
		Assert(read_len == POLAR_FLOG_BLCKSZ);

		if (!flog_page_header_validate(state, targetSegmentPtr,
									   state->read_buf))
			goto err;
	}

	/*
	 * First, read the requested data length, but at least a short page header
	 * so that we can validate it.
	 */
	readLen = state->read_page(state, pageptr, Max(reqLen, FLOG_SHORT_PHD_SIZE),
							   state->curr_rec_ptr, state->read_buf);

	if (readLen < 0)
		goto err;

	Assert(read_len <= POLAR_FLOG_BLCKSZ);

	/* Do we have enough data to check the header length? */
	if (readLen <= FLOG_SHORT_PHD_SIZE)
		goto err;

	Assert(read_len >= reqLen);

	hdr = (flog_page_header) state->read_buf;

	/* still not enough */
	if (readLen < FLOG_PAGE_HEADER_SIZE(hdr))
	{
		readLen = state->read_page(state, pageptr, FLOG_PAGE_HEADER_SIZE(hdr),
								   state->curr_rec_ptr, state->read_buf);

		if (readLen < 0)
			goto err;
	}

	/*
	 * Now that we know we have the full header, validate it.
	 */
	if (!flog_page_header_validate(state, pageptr, (char *) hdr))
		goto err;

	/* update read state information */
	state->read_seg_no = targetSegNo;
	state->read_off = targetPageOff;
	state->read_len = readLen;

	return readLen;

err:
	flog_inval_read_state(state);
	return -1;
}

/*
 * Attempt to read an flashback log record. A copy from polar_flashback_log_reader.c.
 *
 * If RecPtr is valid, try to read a record at that position.  Otherwise
 * try to read a record just after the last one previously read.
 *
 * If the read_page callback fails to read the requested data, NULL is
 * returned.  The callback is expected to have reported the error; errormsg
 * is set to NULL.
 *
 * If the reading fails for some other reason, NULL is also returned, and
 * *errormsg is set to a string with details of the failure.
 *
 * The returned pointer (or *errormsg) points to an internal buffer that's
 * valid until the next call to read_flog_record.
 */
static flog_record *
read_flog_record(flog_reader_state *state,
				 polar_flog_rec_ptr RecPtr, char **errormsg)
{
	flog_record *record;
	polar_flog_rec_ptr  targetPagePtr;
	uint32      len,
				total_len = 0;
	uint32      targetRecOff;
	uint32      pageHeaderSize;
	bool        gotheader;
	int         readOff;
	uint32      gotlen = 0;
	bool randAccess = false;
	polar_flog_rec_ptr next_ptr;

	/* reset error state */
	*errormsg = NULL;
	state->errormsg_buf[0] = '\0';
	state->in_switch_region = false;

	if (RecPtr == POLAR_INVALID_FLOG_REC_PTR)
	{
		/* No explicit start point; read the record after the one we just read */
		RecPtr = state->end_rec_ptr;

		if (state->read_rec_ptr == POLAR_INVALID_FLOG_REC_PTR)
			randAccess = true;

		/*
		 * RecPtr is pointing to end+1 of the previous flashback log record.  If we're
		 * at a page boundary, no more records can fit on the current page. We
		 * must skip over the page header, but we can't do that until we've
		 * read in the page, since the header size is variable.
		 */
	}
	else
	{
		/*
		 * Caller supplied a position to start at.
		 *
		 * In this case, the passed-in record pointer should already be
		 * pointing to a valid record starting position.
		 */
		Assert(FLOG_REC_PTR_IS_VAILD(RecPtr));
		randAccess = true;
	}

	state->curr_rec_ptr = RecPtr;

	targetPagePtr = RecPtr - (RecPtr % POLAR_FLOG_BLCKSZ);
	targetRecOff = RecPtr % POLAR_FLOG_BLCKSZ;

	/*
	 * Read the page containing the record into state->readBuf. Request enough
	 * byte to cover the whole record header, or at least the part of it that
	 * fits on the same page.
	 */
	readOff = read_flog_page_internal(state,
									  targetPagePtr,
									  Min(targetRecOff + FLOG_REC_HEADER_SIZE, POLAR_FLOG_BLCKSZ));

	if (readOff < 0)
		goto err;

	gotlen = readOff - targetRecOff;

	/*
	 * ReadPageInternal always returns at least the page header, so we can
	 * examine it now.
	 */
	pageHeaderSize = FLOG_PAGE_HEADER_SIZE((flog_page_header) state->read_buf);

	if (targetRecOff == 0)
	{
		/*
		 * At page start, so skip over page header.
		 */
		RecPtr += pageHeaderSize;
		targetRecOff = pageHeaderSize;
	}
	else if (targetRecOff < pageHeaderSize)
	{
		report_invalid_flog_record(state, "invalid flashback log record offset at %X/%X",
								   (uint32)(RecPtr >> 32), (uint32) RecPtr);
		goto err;
	}

	/* Can't read flashback log from contrecord ptr */
	if ((((flog_page_header) state->read_buf)->xlp_info & FLOG_FIRST_IS_CONTRECORD) &&
			targetRecOff == pageHeaderSize)
	{
		report_invalid_flog_record(state, "contrecord flashback log is requested by %X/%X",
								   (uint32)(RecPtr >> 32), (uint32) RecPtr);
		goto err;
	}

	/* ReadPageInternal has verified the page header */
	Assert(pageHeaderSize <= read_off);

	/*
	 * Read the record length.
	 *
	 * NB: Even though we use an polar_flashback_log_record pointer here, the whole record
	 * header might not fit on this page. xl_tot_len is the first field of the
	 * struct, so it must be on this page (the records are MAXALIGNed), but we
	 * cannot access any other fields until we've verified that we got the
	 * whole header.
	 */
	record = (flog_record *)(state->read_buf + RecPtr % POLAR_FLOG_BLCKSZ);
	total_len = record->xl_tot_len;

	/*
	 * If the whole record header is on this page, validate it immediately.
	 * Otherwise do just a basic sanity check on xl_tot_len, and validate the
	 * rest of the header after reading it from the next page.  The xl_tot_len
	 * check is necessary here to ensure that we enter the "Need to reassemble
	 * record" code path below; otherwise we might fail to apply
	 * valid_flog_rec_header at all.
	 */
	if (targetRecOff <= POLAR_FLOG_BLCKSZ - FLOG_REC_HEADER_SIZE)
	{
		if (!just_one_record_without_check &&
				!valid_flog_rec_header(state, RecPtr, state->read_rec_ptr,
									   record, randAccess))
			goto err;

		gotheader = true;
	}
	else
	{
		/* XXX: more validation should be done here */
		if (total_len < FLOG_REC_HEADER_SIZE)
		{
			report_invalid_flog_record(state,
									   "invalid flashback log record length at %X/%X: wanted %u, got %u",
									   (uint32)(RecPtr >> 32), (uint32) RecPtr,
									   (uint32) FLOG_REC_HEADER_SIZE, total_len);
			goto err;
		}

		gotheader = false;
	}

	/*
	 * Enlarge readRecordBuf as needed.
	 */
	if (total_len > state->read_record_buf_size &&
			!allocate_flog_recordbuf(state, total_len))
	{
		/* We treat this as a "bogus data" condition */
		report_invalid_flog_record(state, "record length %u at %X/%X too long",
								   total_len,
								   (uint32)(RecPtr >> 32), (uint32) RecPtr);
		goto err;
	}

	len = POLAR_FLOG_BLCKSZ - RecPtr % POLAR_FLOG_BLCKSZ;

	if (total_len > len)
	{
		/* Need to reassemble record */
		char       *contdata;
		flog_page_header pageHeader;
		char       *buffer;

		/* Copy the first fragment of the record from the first page. */
		memcpy(state->read_record_buf,
			   state->read_buf + RecPtr % POLAR_FLOG_BLCKSZ, len);
		buffer = state->read_record_buf + len;
		gotlen = len;

		do
		{
			/* Calculate pointer to beginning of next page */
			targetPagePtr += POLAR_FLOG_BLCKSZ;

			/* Wait for the next page to become available */
			readOff = read_flog_page_internal(state, targetPagePtr,
											  Min(total_len - gotlen + FLOG_SHORT_PHD_SIZE,
												  POLAR_FLOG_BLCKSZ));

			if (readOff < 0)
				goto err;

			Assert(FLOG_SHORT_PHD_SIZE <= read_off);

			/* Check that the continuation on next page looks valid */
			pageHeader = (flog_page_header) state->read_buf;

			if (!(pageHeader->xlp_info & FLOG_FIRST_IS_CONTRECORD))
			{
				report_invalid_flog_record(state,
										   "there is no contrecord flag at flashback log %X/%X",
										   (uint32)(RecPtr >> 32), (uint32) RecPtr);
				goto err;
			}

			/*
			 * Cross-check that xlp_rem_len agrees with how much of the record
			 * we expect there to be left.
			 */
			if (pageHeader->xlp_rem_len == 0 ||
					total_len != (pageHeader->xlp_rem_len + gotlen))
			{
				report_invalid_flog_record(state,
										   "invalid contrecord length %u at flashback log %X/%X",
										   pageHeader->xlp_rem_len,
										   (uint32)(RecPtr >> 32), (uint32) RecPtr);
				goto err;
			}

			/* Append the continuation from this page to the buffer */
			pageHeaderSize = FLOG_PAGE_HEADER_SIZE(pageHeader);

			if (readOff < pageHeaderSize)
				readOff = read_flog_page_internal(state, targetPagePtr,
												  pageHeaderSize);

			Assert(pageHeaderSize <= read_off);

			contdata = (char *) state->read_buf + pageHeaderSize;
			len = POLAR_FLOG_BLCKSZ - pageHeaderSize;

			if (pageHeader->xlp_rem_len < len)
				len = pageHeader->xlp_rem_len;

			if (readOff < pageHeaderSize + len)
				readOff = read_flog_page_internal(state, targetPagePtr,
												  pageHeaderSize + len);

			memcpy(buffer, (char *) contdata, len);
			buffer += len;
			gotlen += len;

			/* If we just reassembled the record header, validate it. */
			if (!gotheader)
			{
				record = (flog_record *) state->read_record_buf;

				if (!just_one_record_without_check &&
						!valid_flog_rec_header(state, RecPtr, state->read_rec_ptr,
											   record, randAccess))
					goto err;

				gotheader = true;
			}
		}
		while (gotlen < total_len);

		Assert(gotheader);

		record = (flog_record *) state->read_record_buf;

		if (!just_one_record_without_check &&
				!valid_flog_rec(state, record, RecPtr))
			goto err;

		pageHeaderSize = FLOG_PAGE_HEADER_SIZE((flog_page_header) state->read_buf);
		state->read_rec_ptr = RecPtr;
		state->end_rec_ptr = targetPagePtr + pageHeaderSize
							 + MAXALIGN(pageHeader->xlp_rem_len);
	}
	else
	{
		/* Wait for the record data to become available */
		readOff = read_flog_page_internal(state, targetPagePtr,
										  Min(targetRecOff + total_len, POLAR_FLOG_BLCKSZ));

		if (readOff < 0)
			goto err;

		/* Record does not cross a page boundary */
		if (!just_one_record_without_check &&
				!valid_flog_rec(state, record, RecPtr))
			goto err;

		state->end_rec_ptr = RecPtr + MAXALIGN(total_len);

		state->read_rec_ptr = RecPtr;
		memcpy(state->read_record_buf, record, total_len);
	}

	return record;

err:

	/*
	 * Invalidate the read state. We might read from a different source after
	 * failure.
	 */
	flog_inval_read_state(state);


	if (state->errormsg_buf[0] != '\0')
		*errormsg = state->errormsg_buf;

	/* Is the record in the switch region */
	next_ptr = state->curr_rec_ptr;

	if (is_flog_rec_ignore(&next_ptr, total_len))
	{
		state->in_switch_region = true;
		state->end_rec_ptr = next_ptr;
	}

	return NULL;
}

/*
 * Find the first record with an lsn >= RecPtr.
 *
 * Useful for checking whether RecPtr is a valid flashback log address for reading, and
 * to find the first valid address after some address when dumping records for
 * debugging purposes.
 */
static polar_flog_rec_ptr
find_first_flog_rec(flog_reader_state *state,
					polar_flog_rec_ptr RecPtr)
{
	flog_reader_state saved_state = *state;
	polar_flog_rec_ptr  tmpRecPtr;
	polar_flog_rec_ptr  found = POLAR_INVALID_FLOG_REC_PTR;
	flog_page_header header;
	char       *errormsg;

	Assert(RecPtr != POLAR_INVALID_FLOG_REC_PTR);

	/*
	 * skip over potential continuation data, keeping in mind that it may span
	 * multiple pages
	 */
	tmpRecPtr = RecPtr;

	while (true)
	{
		polar_flog_rec_ptr  targetPagePtr;
		int         targetRecOff;
		uint32      pageHeaderSize;
		int         readLen;

		/*
		 * Compute targetRecOff. It should typically be equal or greater than
		 * short page-header since a valid record can't start anywhere before
		 * that, except when caller has explicitly specified the offset that
		 * falls somewhere there or when we are skipping multi-page
		 * continuation record. It doesn't matter though because
		 * ReadPageInternal() is prepared to handle that and will read at
		 * least short page-header worth of data
		 */
		targetRecOff = tmpRecPtr % POLAR_FLOG_BLCKSZ;

		/* scroll back to page boundary */
		targetPagePtr = tmpRecPtr - targetRecOff;

		/* Read the page containing the record */
		readLen = read_flog_page_internal(state, targetPagePtr, targetRecOff);

		if (readLen < 0)
			goto err;

		header = (flog_page_header) state->read_buf;

		pageHeaderSize = FLOG_PAGE_HEADER_SIZE(header);

		/* make sure we have enough data for the page header */
		readLen = read_flog_page_internal(state, targetPagePtr, pageHeaderSize);

		if (readLen < 0)
			goto err;

		/* skip over potential continuation data */
		if (header->xlp_info & FLOG_FIRST_IS_CONTRECORD)
		{
			/*
			 * If the length of the remaining continuation data is more than
			 * what can fit in this page, the continuation record crosses over
			 * this page. Read the next page and try again. xlp_rem_len in the
			 * next page header will contain the remaining length of the
			 * continuation data
			 *
			 * Note that record headers are MAXALIGN'ed
			 */
			if (MAXALIGN(header->xlp_rem_len) >= (POLAR_FLOG_BLCKSZ - pageHeaderSize))
				tmpRecPtr = targetPagePtr + POLAR_FLOG_BLCKSZ;
			else
			{
				/*
				 * The previous continuation record ends in this page. Set
				 * tmpRecPtr to point to the first valid record
				 */
				tmpRecPtr = targetPagePtr + pageHeaderSize
							+ MAXALIGN(header->xlp_rem_len);
				break;
			}
		}
		else
		{
			tmpRecPtr = targetPagePtr + pageHeaderSize;
			break;
		}
	}

	/*
	 * we know now that tmpRecPtr is an address pointing to a valid XLogRecord
	 * because either we're at the first record after the beginning of a page
	 * or we just jumped over the remaining data of a continuation.
	 */
	while (read_flog_record(state, tmpRecPtr, &errormsg) != NULL)
	{
		/* continue after the record */
		tmpRecPtr = POLAR_INVALID_FLOG_REC_PTR;

		/* past the record we've found, break out */
		if (RecPtr <= state->read_rec_ptr)
		{
			found = state->read_rec_ptr;
			goto out;
		}
	}

err:

	if (state->in_switch_region)
	{
		found = state->end_rec_ptr;
		goto out;
	}

	if (state->errormsg_buf[0] != '\0')
		errormsg = state->errormsg_buf;

	fatal_error("Read the flashback log record error at %X/%X: %s",
				(uint32)(state->curr_rec_ptr >> 32),
				(uint32) state->curr_rec_ptr, errormsg);
out:
	/* Reset state to what we had before finding the record */
	state->read_rec_ptr = saved_state.read_rec_ptr;
	state->end_rec_ptr = saved_state.end_rec_ptr;
	flog_inval_read_state(state);

	return found;
}

/*
 * Open the file in the valid target directory.
 *
 * return a read only fd
 */
static int
open_file_in_directory(const char *directory, const char *fname)
{
	int         fd = -1;
	char        fpath[MAXPGPATH];

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
	int         fd = -1;
	DIR        *xldir;

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
			if (FLOG_IS_LOG_FILE(xlde->d_name))
			{
				fd = open_file_in_directory(directory, xlde->d_name);
				fname = xlde->d_name;
				break;
			}
		}

		closedir(xldir);
	}

	/* set segment_size if file is successfully opened */
	if (fd >= 0)
	{
		pg_aligned_flashback_log_blk buf;

		if (read(fd, buf.data, POLAR_FLOG_BLCKSZ) == POLAR_FLOG_BLCKSZ)
		{
			polar_long_page_header longhdr = (polar_long_page_header) buf.data;

			segment_size = longhdr->xlp_seg_size;

			if (segment_size != POLAR_FLOG_SEG_SIZE)
				fatal_error("Flashback log segment size in the binary is %d, "
							"but the flashback log file \"%s\" header specifies %d byte",
							POLAR_FLOG_SEG_SIZE, fname, segment_size);

			if (just_version)
			{
				printf("The flashback log segment file page struct version no is: %hu",
					   longhdr->std.xlp_version);
				exit(EXIT_SUCCESS);
			}
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
 * Identify the target directory and set segment size.
 *
 * Try to find the file in several places:
 * if directory != NULL:
 *   directory /
 *   directory / POLAR_FLASHBACK_LOG_DEFAULT_DIR /
 * else
 *   .
 *   POLAR_FLASHBACK_LOG_DEFAULT_DIR /
 *   $PGDATA / POLAR_FLASHBACK_LOG_DEFAULT_DIR /
 *
 * Set the valid target directory in private->inpath.
 */
static void
identify_target_directory(dump_private *private, char *directory,
						  char *fname)
{
	char        fpath[MAXPGPATH];

	if (directory != NULL)
	{
		if (search_directory(directory, fname))
		{
			private->inpath = strdup(directory);
			return;
		}

		/* directory / POLAR_FLASHBACK_LOG_DEFAULT_DIR */
		snprintf(fpath, MAXPGPATH, "%s/%s", directory, POLAR_FL_DEFAULT_DIR);

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

		/* POLAR_FLASHBACK_LOG_DEFAULT_DIR */
		if (search_directory(POLAR_FL_DEFAULT_DIR, fname))
		{
			private->inpath = strdup(POLAR_FL_DEFAULT_DIR);
			return;
		}

		datadir = getenv("PGDATA");

		/* $PGDATA / POLAR_FLASHBACK_LOG_DEFAULT_DIR */
		if (datadir != NULL)
		{
			snprintf(fpath, MAXPGPATH, "%s/%s", datadir, POLAR_FL_DEFAULT_DIR);

			if (search_directory(fpath, fname))
			{
				private->inpath = strdup(fpath);
				return;
			}
		}
	}

	/* could not locate WAL file */
	if (fname)
		fatal_error("could not locate flashback log file \"%s\"", fname);
	else
		fatal_error("could not find any flashback log file");
}

/*
 * Read count bytes from a segment file in the specified directory,
 * containing the specified record pointer; store the data in
 * the passed buffer.
 */
static void
flog_dump_read(const char *directory, polar_flog_rec_ptr startptr,
			   char *buf, Size count)
{
	char       *p;
	polar_flog_rec_ptr  recptr;
	Size        nbytes;

	static int  sendFile = -1;
	static uint64 sendSegNo = 0;
	static uint32 sendOff = 0;

	p = buf;
	recptr = startptr;
	nbytes = count;

	while (nbytes > 0)
	{
		uint32      startoff;
		int         segbytes;
		int         readbytes;

		startoff = FLOG_SEGMENT_OFFSET(recptr, segment_size);

		if (sendFile < 0 || !FLOG_PTR_IN_SEG(recptr, sendSegNo, segment_size))
		{
			char        fname[FLOG_MAX_FNAME_LEN];
			int         tries;

			/* Switch to another logfile segment */
			if (sendFile >= 0)
				close(sendFile);

			sendSegNo = FLOG_PTR_TO_SEG(recptr, segment_size);

			FLOG_GET_FNAME(fname, sendSegNo, segment_size, FLOG_DEFAULT_TIMELINE);

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
					int         save_errno = errno;

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
				int         err = errno;
				char        fname[MAXPGPATH];

				FLOG_GET_FNAME(fname, sendSegNo, segment_size, FLOG_DEFAULT_TIMELINE);

				fatal_error("could not seek in log file %s to offset %u: %s",
							fname, startoff, strerror(err));
			}

			sendOff = startoff;
		}

		/* How many bytes are within this segment? */
		if (nbytes > (segment_size - startoff))
			segbytes = segment_size - startoff;
		else
			segbytes = nbytes;

		readbytes = read(sendFile, p, segbytes);

		if (readbytes <= 0)
		{
			int         err = errno;
			char        fname[MAXPGPATH];

			FLOG_GET_FNAME(fname, sendSegNo, segment_size, FLOG_DEFAULT_TIMELINE);

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
 * flashback log read_page callback
 */
static int
flog_dump_read_page(flog_reader_state *state,
					polar_flog_rec_ptr targetPagePtr, int reqLen,
					polar_flog_rec_ptr targetRecPtr, char *cur_page)
{
	dump_private *private = state->private_data;
	int         count = POLAR_FLOG_BLCKSZ;

	if (private->endptr != POLAR_INVALID_FLOG_REC_PTR)
	{
		if (targetPagePtr + POLAR_FLOG_BLCKSZ <= private->endptr)
			count = POLAR_FLOG_BLCKSZ;
		else if (targetPagePtr + reqLen <= private->endptr)
			count = private->endptr - targetPagePtr;
		else
		{
			private->endptr_reached = true;
			return -1;
		}
	}

	flog_dump_read(private->inpath, targetPagePtr,
				   cur_page, count);

	return count;
}

static const char *
get_flog_rec_type(flog_record *record, polar_flog_rec_ptr lsn)
{
	RmgrId      xl_rmgr = record->xl_rmid;

	if (xl_rmgr < FLOG_REC_TYPES)
		return flog_record_types[xl_rmgr];
	else
		fatal_error("The type of the record %X/%08X is wrong, \n",
					(uint32)(lsn >> 32), (uint32)lsn);

	return NULL;
}

static void
print_rel_filenode_rec(flog_record *record)
{
	fl_filenode_rec_data_t *filenode_rec;
	bool can_be_flashback;

	can_be_flashback = (record->xl_info & REL_FILENODE_TYPE_MASK) & REL_CAN_FLASHBACK;
	filenode_rec = FL_GET_FILENODE_REC_DATA(record);
	printf("The previous relation filenode is [%u, %u, %u], "
			"the current relation filenode is [%u, %u, %u], the time is %s, "
			"can%s flashback the relation to past",
			filenode_rec->old_filenode.spcNode, filenode_rec->old_filenode.dbNode,
			filenode_rec->old_filenode.relNode, filenode_rec->new_filenode.spcNode,
			filenode_rec->new_filenode.dbNode, filenode_rec->new_filenode.relNode,
			timestamptz_to_str(filenode_rec->time), (can_be_flashback)? "":"'t");
}

/*
 * Print a record to stdout
 */
static void
flog_display_rec(dump_config *config, flog_record *record,
				 polar_flog_rec_ptr lsn)
{
	uint32      rec_len;
	uint8       info = record->xl_info;
	polar_flog_rec_ptr  xl_prev = record->xl_prev;
	BufferTag   tag;
	RmgrId      rmid;

	fl_rec_img_header *img;
	fl_rec_img_comp_header *c_img;
	char *record_data;
	int record_data_len;
	uint16 hole_length = 0;
	PGAlignedBlock tmp;
	PGAlignedBlock page;
	uint16 checksum;
	PageHeader  p;
	XLogRecPtr  redo_lsn = InvalidXLogRecPtr;
	bool from_origin_buf = false;

	CLEAR_BUFFERTAG(tag);
	rec_len = record->xl_tot_len;
	rmid = record->xl_rmid;
	record_data = (char *)record;
	printf("type: %24s total_len: %6u, tx: %10u, lsn: %X/%08X, prev %X/%08X, ",
		   get_flog_rec_type(record, lsn),
		   rec_len, record->xl_xid, (uint32)(lsn >> 32), (uint32) lsn,
		   (uint32)(xl_prev >> 32), (uint32) xl_prev);

	if (rmid == ORIGIN_PAGE_ID)
	{
		uint8 type;

		tag = FL_GET_ORIGIN_PAGE_REC_DATA(record)->tag;
		redo_lsn = FL_GET_ORIGIN_PAGE_REC_DATA(record)->redo_lsn;
		type = info & ORIGIN_PAGE_TYPE_MASK;
		from_origin_buf = info & FROM_ORIGIN_BUF;
		printf("desc: %s, ", type == ORIGIN_PAGE_EMPTY ? "empty page" : "full  page");
	}
	else if (rmid == REL_FILENODE_ID)
	{
		printf("desc: change relation file node, ");
	}
	else
		fatal_error("invalid rmid %d for the record at %X/%X", rmid,
					(uint32)(lsn >> 32), (uint32) lsn);

	if (!config->bkp_details)
	{
		if (rmid == ORIGIN_PAGE_ID)
			printf("rel %u/%u/%u fork %s blk %u; WAL redo lsn %X/%08X",
				   tag.rnode.spcNode, tag.rnode.dbNode, tag.rnode.relNode,
				   forkNames[tag.forkNum], tag.blockNum,
				   (uint32)(redo_lsn >> 32), (uint32) redo_lsn);
		else if (rmid == REL_FILENODE_ID)
			print_rel_filenode_rec(record);

		putchar('\n');
	}
	else
	{
		/* print block references (detailed format) */
		putchar('\n');

		if (rmid == ORIGIN_PAGE_ID)
		{
			char *origin_page_source;

			if (from_origin_buf)
				origin_page_source = "from origin page buffer";
			else
				origin_page_source = "from disk";

			info &= ORIGIN_PAGE_TYPE_MASK;
			printf("\trel %u/%u/%u fork %s blk %u; WAL redo lsn %X/%08X",
				   tag.rnode.spcNode, tag.rnode.dbNode, tag.rnode.relNode,
				   forkNames[tag.forkNum], tag.blockNum,
				   (uint32)(redo_lsn >> 32), (uint32) redo_lsn);

			if (info == ORIGIN_PAGE_EMPTY)
				printf(" (empty original page %s)", origin_page_source);
			else if (info == ORIGIN_PAGE_FULL)
			{
				img = FL_GET_ORIGIN_PAGE_IMG_HEADER(record);
				record_data = (char *)img + FL_REC_IMG_HEADER_SIZE;
				record_data_len = img->length;

				if (img->bimg_info & IMAGE_IS_COMPRESSED)
				{
					if (img->bimg_info & IMAGE_HAS_HOLE)
					{
						c_img = (fl_rec_img_comp_header *) record_data;
						record_data += FL_REC_IMG_COMP_HEADER_SIZE;
						hole_length = c_img->hole_length;
					}

					printf(" (full original page %s); hole: offset: %u, length: %u, "
						   "compression saved: %u", origin_page_source, img->hole_offset,
						   hole_length, img->length);

					if (config->checksum)
					{
						if (pglz_decompress(record_data, record_data_len, tmp.data,
											BLCKSZ - hole_length) < 0)
							fatal_error("invalid compressed origin page at %X/%X",
										(uint32)(lsn >> 32), (uint32) lsn);

						record_data = tmp.data;
					}
				}
				else if (img->bimg_info & IMAGE_HAS_HOLE)
				{
					hole_length = BLCKSZ - img->length;
					printf(" (full original page %s); hole: offset: %u, length: %u; data length: %u",
							origin_page_source, img->hole_offset, hole_length, img->length);
				}
				else
					printf(" (full original page %s); data length: %u", origin_page_source, img->length);

				if (config->checksum)
				{
					/* generate page, taking into account hole if necessary */
					if (hole_length == 0)
					{
						Assert(img->length == BLCKSZ);
						memcpy(page.data, record_data, BLCKSZ);
					}
					else
					{
						memcpy(page.data, record_data, img->hole_offset);
						/* must zero-fill the hole */
						MemSet(page.data + img->hole_offset, 0, hole_length);
						memcpy(page.data + (img->hole_offset + hole_length),
							   record_data + img->hole_offset,
							   BLCKSZ - (img->hole_offset + hole_length));
					}

					/* Checksum again */
					if (!PageIsNew(page.data))
					{
						checksum = pg_checksum_page(page.data, tag.blockNum);
						p = (PageHeader) page.data;

						if (checksum != p->pd_checksum)
							fatal_error("\nError: invalid checksum of origin page at %X/%X, "
										"expected is %d, but is %d",
										(uint32)(lsn >> 32), (uint32) lsn,
										checksum, p->pd_checksum);
					}
				}
			}
			else
				fatal_error("invalid xl_info %d for the record at %X/%X", info,
							(uint32)(lsn >> 32), (uint32) lsn);
		}
		else if (rmid == REL_FILENODE_ID)
			print_rel_filenode_rec(record);
		putchar('\n');
	}
}

static void
usage(void)
{
	printf(_("%s decodes and displays PolarDB flashback logs for debugging.\n\n"),
		   progname);
	printf(_("Usage:\n"));
	printf(_("  %s [OPTION]... [STARTSEG [ENDSEG]]\n"), progname);
	printf(_("\nOptions:\n"));
	printf(_("  -b, --bkp-details      output detailed information about original pages\n"));
	printf(_("  -c, --checksum         check the checksum of the original page \n"));
	printf(_("  -e, --end=RECPTR       stop reading at flashback log location xx/xx\n"));
	printf(_("  -f, --follow           keep retrying after reaching end of flashback log \n"));
	printf(_("  -n, --limit=N          number of records to display\n"));
	printf(_("  -p, --path=PATH        directory in which to find log segment files or a\n"
			 "                         directory with a ./polar_flshbak_log that contains such files\n"
			 "                         (default: current directory, ./polar_flshbak_log, $PGDATA/polar_flshbak_log)\n"));
	printf(_("  -t, --type=type        only show records within one type,\n"
			 "                         use --type=list to list valid flashback log record types\n"));
	printf(_("  -s, --start=RECPTR     start reading at flashback log location xx/xx\n"));
	printf(_("  -o, --just-one-rec=PTR just parse one record without check at flashback log location xx/xx\n"));
	printf(_("  -V, --version          output flashback log page version information, then exit\n"));
	printf(_("  -r, --relfilenode      only show records within the relfilenode which format is xx/xx/xx\n"));
	printf(_("  -?, --help             show this help, then exit\n"));
}

int
flashback_log_file_dump_main(int argc, char **argv)
{
	dump_config config;
	uint32      xlogid;
	uint32      xrecoff;
	dump_private private;

	flog_reader_state *xlogreader_state;
	flog_record *record;
	polar_flog_rec_ptr  first_record;
	char       *errormsg;

	static struct option long_options[] =
	{
		{"bkp-details", no_argument, NULL, 'b'},
		{"checksum", no_argument, NULL, 'c'},
		{"end", required_argument, NULL, 'e'},
		{"follow", no_argument, NULL, 'f'},
		{"help", no_argument, NULL, '?'},
		{"limit", required_argument, NULL, 'n'},
		{"path", required_argument, NULL, 'p'},
		{"type", required_argument, NULL, 't'},
		{"start", required_argument, NULL, 's'},
		{"version", no_argument, NULL, 'V'},
		{"relfilenode", required_argument, NULL, 'r'},
		{"just-one-rec", required_argument, NULL, 'o'},
		{NULL, 0, NULL, 0}
	};

	int         option;
	int         optindex = 0;
	uint32      spcnode;
	uint32      dbnode;
	uint32      relnode;

	progname = get_progname(argv[0]);

	private.startptr = POLAR_INVALID_FLOG_REC_PTR;
	private.endptr = POLAR_INVALID_FLOG_REC_PTR;
	private.endptr_reached = false;

	config.bkp_details = false;
	config.stop_after_records = -1;
	config.already_displayed_records = 0;
	config.follow = false;
	/* Set filter_by_type max_uint8 */
	config.filter_by_type = 255;
	config.checksum = false;
	config.rel_file_node.spcNode = 0;

	if (argc <= 1)
	{
		fprintf(stderr, _("%s: no arguments specified\n"), progname);
		goto bad_argument;
	}

	while ((option = getopt_long(argc, argv, "bce:?fn:o:p:s:t:Vr:",
								 long_options, &optindex)) != -1)
	{
		switch (option)
		{
			case 'b':
				config.bkp_details = true;
				break;

			case 'c':
				config.checksum = true;
				break;

			case 'e':
				if (sscanf(optarg, "%X/%X", &xlogid, &xrecoff) != 2)
				{
					fprintf(stderr, _("%s: could not parse end flashback log location \"%s\"\n"),
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

			case 't':
			{
				uint8           i;

				if (pg_strcasecmp(optarg, "list") == 0)
				{
					print_type_list();
					exit(EXIT_SUCCESS);
				}

				for (i = 0; i < FLOG_REC_TYPES; i++)
				{
					if (pg_strcasecmp(optarg, flog_record_types[i]) == 0)
					{
						config.filter_by_type = i;
						break;
					}
				}

				if (config.filter_by_type > FLOG_REC_TYPES)
				{
					fprintf(stderr, _("%s: type \"%s\" does not exist\n"),
							progname, optarg);
					goto bad_argument;
				}
			}
			break;

			case 's':
				if (private.startptr != POLAR_INVALID_FLOG_REC_PTR)
				{
					fprintf(stderr, _("%s: could not run with option -s and option -o\n"), progname);
					goto bad_argument;
				}

				if (sscanf(optarg, "%X/%X", &xlogid, &xrecoff) != 2)
				{
					fprintf(stderr, _("%s: could not parse start flashback log location \"%s\"\n"),
							progname, optarg);
					goto bad_argument;
				}
				else
					private.startptr = (uint64) xlogid << 32 | xrecoff;

				break;

			case 'V':
				just_version = true;
				break;

			case 'o':
				if (private.startptr != POLAR_INVALID_FLOG_REC_PTR)
				{
					fprintf(stderr, _("%s: could not run with option -s and option -o\n"), progname);
					goto bad_argument;
				}

				if (sscanf(optarg, "%X/%X", &xlogid, &xrecoff) != 2)
				{
					fprintf(stderr, _("%s: could not parse the flashback log location \"%s\"\n"),
							progname, optarg);
					goto bad_argument;
				}
				else
					private.startptr = (uint64) xlogid << 32 | xrecoff;

				just_one_record_without_check = true;
				break;

			case 'r':
				if (sscanf(optarg, "%u/%u/%u", &spcnode, &dbnode, &relnode) != 3)
				{
					fprintf(stderr, _("%s: could not parse relation flie node filter \"%s\"\n"),
							progname, optarg);
					goto bad_argument;
				}

				config.rel_file_node.spcNode = spcnode;
				config.rel_file_node.dbNode = dbnode;
				config.rel_file_node.relNode = relnode;
				break;

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
		char       *directory = NULL;
		char       *fname = NULL;
		int         fd;
		uint64  segno;

		fname = basename(argv[optind]);
		directory = dirname(argv[optind]);

		if (private.inpath == NULL && directory != NULL)
		{
			private.inpath = directory;

			if (!verify_directory(private.inpath))
				fatal_error("%s: could not open directory \"%s\": %s",
							progname, private.inpath, strerror(errno));
		}

		identify_target_directory(&private, private.inpath, fname);
		fd = open_file_in_directory(private.inpath, fname);

		if (fd < 0)
			fatal_error("could not open file \"%s\"", fname);

		close(fd);

		/* parse position from file */
		FLOG_GET_SEG_FROM_FNAME(fname, &segno, segment_size);

		if (private.startptr == POLAR_INVALID_FLOG_REC_PTR)
		{
			FLOG_SEG_OFFSET_TO_PTR(segno, 0,
								   segment_size, private.startptr);
			private.startptr = private.startptr == POLAR_INVALID_FLOG_REC_PTR ?
							   FLOG_LONG_PHD_SIZE : private.startptr;
		}
		else if (!FLOG_PTR_IN_SEG(private.startptr, segno, segment_size))
		{
			fprintf(stderr,
					_("%s: start flashback log location "
					  "%X/%X is not inside file \"%s\"\n"),
					progname,
					(uint32)(private.startptr >> 32),
					(uint32) private.startptr,
					fname);
			goto bad_argument;
		}

		/* no second file specified, set end position */
		if (!(optind + 1 < argc) && private.endptr == POLAR_INVALID_FLOG_REC_PTR)
			FLOG_SEG_OFFSET_TO_PTR(segno + 1, 0,
								   segment_size, private.endptr);

		/* parse ENDSEG if passed */
		if (optind + 1 < argc)
		{
			uint64  endsegno;

			/* ignore directory, already have that */
			fname = basename(argv[optind]);
			fd = open_file_in_directory(private.inpath, fname);

			if (fd < 0)
				fatal_error("could not open file \"%s\"", fname);

			close(fd);

			/* parse position from file */
			FLOG_GET_SEG_FROM_FNAME(fname, &endsegno, segment_size);

			if (endsegno < segno)
				fatal_error("ENDSEG %s is before STARTSEG %s",
							argv[optind + 1], argv[optind]);

			if (private.endptr == POLAR_INVALID_FLOG_REC_PTR)
				FLOG_SEG_OFFSET_TO_PTR(endsegno + 1, 0,
									   segment_size, private.endptr);

			/* set segno to endsegno for check of --end */
			segno = endsegno;
		}

		if (!FLOG_PTR_IN_SEG(private.endptr, segno, segment_size) &&
				private.endptr != (segno + 1) * segment_size)
		{
			fprintf(stderr,
					_("%s: end flashback log location %X/%X is not inside file \"%s\"\n"),
					progname, (uint32)(private.endptr >> 32),
					(uint32) private.endptr,
					argv[argc - 1]);
			goto bad_argument;
		}
	}
	else
		identify_target_directory(&private, private.inpath, NULL);

	/* we don't know what to print */
	if (private.startptr == POLAR_INVALID_FLOG_REC_PTR)
	{
		fprintf(stderr, _("%s: no start flashback log location given\n"), progname);
		goto bad_argument;
	}

	/*
	 * Get the switch ptr list first.
	 */
	switch_ptr_list = load_switch_ptrs(private.inpath, switch_ptr_list);

	/* done with argument parsing, do the actual work */

	/* we have everything we need, start reading */
	xlogreader_state = flog_reader_allocate(segment_size,
											flog_dump_read_page, &private);

	if (!xlogreader_state)
		fatal_error("out of memory");

	/* first find a valid recptr to start from */
	if (!just_one_record_without_check)
		first_record = find_first_flog_rec(xlogreader_state, private.startptr);
	else
		first_record = valid_flog_ptr(private.startptr);

	if (first_record == POLAR_INVALID_FLOG_REC_PTR)
		fatal_error("could not find a valid record after %X/%X",
					(uint32)(private.startptr >> 32),
					(uint32) private.startptr);

	if (first_record >= private.endptr)
		fatal_error("could not find a valid record between %X/%X and %X/%X",
					(uint32)(private.startptr >> 32),
					(uint32) private.startptr,
					(uint32)(private.endptr >> 32),
					(uint32) private.endptr);

	/*
	 * Display a message that we're skipping data if `from` wasn't a pointer
	 * to the start of a record and also wasn't a pointer to the beginning of
	 * a segment (e.g. we were used in file mode).
	 */
	if (first_record != private.startptr &&
			FLOG_SEGMENT_OFFSET(private.startptr, segment_size) != 0)
		printf("first record is after %X/%X, at %X/%X, skipping over %u byte\n",
			   (uint32)(private.startptr >> 32), (uint32) private.startptr,
			   (uint32)(first_record >> 32), (uint32) first_record,
			   (uint32)(first_record - private.startptr));

	for (;;)
	{
		/* try to read the next record */
		record = read_flog_record(xlogreader_state, first_record, &errormsg);

		if (!record)
		{
			if (xlogreader_state->in_switch_region)
			{
				first_record = POLAR_INVALID_FLOG_REC_PTR;
				continue;
			}
			else if (!config.follow || private.endptr_reached)
				break;
			else
			{
				pg_usleep(1000000L);    /* 1 second */
				continue;
			}
		}

		/* after reading the first record, continue at next one */
		first_record = POLAR_INVALID_FLOG_REC_PTR;

		/* apply all specified filters */
		if (config.filter_by_type != 255 &&
				config.filter_by_type != record->xl_rmid)
			continue;

		if (config.rel_file_node.spcNode != 0)
		{
			if (record->xl_rmid == ORIGIN_PAGE_ID)
			{
				fl_origin_page_rec_data       *rec_data;

				rec_data = FL_GET_ORIGIN_PAGE_REC_DATA(record);

				if (config.rel_file_node.spcNode != rec_data->tag.rnode.spcNode ||
						config.rel_file_node.dbNode != rec_data->tag.rnode.dbNode ||
						config.rel_file_node.relNode != rec_data->tag.rnode.relNode)
					continue;
			}
			else if (record->xl_rmid == REL_FILENODE_ID)
			{
				fl_filenode_rec_data_t *rec_data;

				rec_data = FL_GET_FILENODE_REC_DATA(record);

				if (config.rel_file_node.spcNode != rec_data->new_filenode.spcNode||
						config.rel_file_node.dbNode != rec_data->new_filenode.dbNode ||
						config.rel_file_node.relNode != rec_data->new_filenode.relNode)
					continue;
			}
			else
				continue;
		}

		flog_display_rec(&config, record, xlogreader_state->read_rec_ptr);

		/* check whether we printed enough */
		config.already_displayed_records++;

		if (config.stop_after_records > 0 &&
				config.already_displayed_records >= config.stop_after_records)
			break;

		if (just_one_record_without_check)
			break;
	}

	if (errormsg)
		fatal_error("error in flashback log record at %X/%X: %s",
					(uint32)(xlogreader_state->curr_rec_ptr >> 32),
					(uint32) xlogreader_state->curr_rec_ptr,
					errormsg);

	flog_reader_free(xlogreader_state);

	return EXIT_SUCCESS;

bad_argument:
	fprintf(stderr, _("Try \"%s --help\" for more information.\n"), progname);
	return EXIT_FAILURE;
}
/*no cover end*/
