/*-------------------------------------------------------------------------
 *
 * polar_flashback_snapshot.c
 *
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2021, Alibaba Group Holding limited
 *
 * IDENTIFICATION
 *    src/backend/polar_flashback/polar_flashback_snapshot.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/xact.h"
#include "polar_flashback/polar_fast_recovery_area.h"
#include "polar_flashback/polar_flashback_snapshot.h"
#include "utils/builtins.h"
#include "utils/snapmgr.h"
#include "storage/procarray.h"

#define COMPUTE_SNAPSHOT_DATA_CRC(result, header, data, size) \
	do{ \
		INIT_CRC32C(result); \
		COMP_CRC32C((result), (char *) (data), (size)); \
		COMP_CRC32C((result), (char *) (header), offsetof(flashback_snapshot_data_header_t, crc)); \
		FIN_CRC32C(result); \
	} while (0)

inline Size
polar_get_snapshot_size(Size size, uint32 xcnt)
{
	size = add_size(size, mul_size(xcnt, sizeof(TransactionId)));

	return size;
}

/*
 * flashback_xid_comparator
 *      qsort comparison function for XIDs
 *
 * NB: We assume that the xid can't be wraparound twice in one snapshot, so
 * we can use TransactionIdPrecedes to compare.
 */
static int
flashback_xid_comparator(const void *arg1, const void *arg2)
{
	TransactionId xid1 = *(const TransactionId *) arg1;
	TransactionId xid2 = *(const TransactionId *) arg2;

	if (xid1 == xid2)
		return 0;

	if (TransactionIdPrecedes(xid1, xid2))
		return -1;
	else
		return 1;
}

static Snapshot
restore_flashback_snapshot(flashback_snapshot_t flashback_snapshot)
{
	Snapshot snapshot;
	TransactionId *serialize_xip;
	TransactionId *xip;

	snapshot = palloc0(sizeof(SnapshotData));
	snapshot->lsn = flashback_snapshot->lsn;
	snapshot->takenDuringRecovery = false;
	snapshot->xcnt = flashback_snapshot->xcnt;
	snapshot->xmax = flashback_snapshot->xmax;
	snapshot->xmin = flashback_snapshot->xmin;
	/* It means nothing, but just to make us to use EstimateSnapshotSpace */
	snapshot->satisfies = HeapTupleSatisfiesMVCC;

	serialize_xip = (TransactionId *) POLAR_GET_FLSHBAK_SNAPSHOT_XIP(flashback_snapshot);
	/* sort so we can bsearch() */
	qsort(serialize_xip, snapshot->xcnt, sizeof(TransactionId), flashback_xid_comparator);
	xip = palloc(snapshot->xcnt * sizeof(TransactionId));
	memcpy(xip, serialize_xip, mul_size(snapshot->xcnt, sizeof(TransactionId)));
	snapshot->xip = xip;

	return snapshot;
}

static void
read_flashback_snapshot_data(const char *fra_dir, uint32 seg_no, uint32 offset,
							 flashback_snapshot_header_t header, char *data)
{
	char path[MAXPGPATH];
	uint32 read_size;
	uint32 end_pos;
	uint32 data_size;
	pg_crc32c   crc;
	char *start = data;

	/* We read the header first, so normally we can use the same fd */
	data_size = header->data_size;
	end_pos = GET_FLSHBAK_SNAPSHOT_END_POS(header->info);
	Assert(end_pos <= FBPOINT_SEG_SIZE);

	do
	{
		fbpoint_io_error_t io_error;

		read_size = end_pos - offset;
		Assert(read_size < FBPOINT_SEG_SIZE);

		if (read_size && !polar_read_fbpoint_file(fra_dir, data, seg_no, offset, read_size, &io_error))
			/*no cover line*/
			polar_fbpoint_report_io_error(fra_dir, &io_error, ERROR);

		data_size -= read_size;

		/* Can break the loop only in here */
		if (data_size == 0)
			break;

		end_pos = FBPOINT_SEG_SIZE;

		if (data_size > (FBPOINT_SEG_SIZE - FBPOINT_REC_END_POS))
			offset = FBPOINT_REC_END_POS;
		else
			offset = FBPOINT_SEG_SIZE - data_size;

		seg_no++;
		data += read_size;
	}
	while (data_size > 0);

	Assert(!data_size);

	/* Check the crc */
	COMPUTE_SNAPSHOT_DATA_CRC(crc, header, start, header->data_size);

	if (!EQ_CRC32C(crc, header->crc))
	{
		/*no cover line*/
		ereport(FATAL, (errcode(ERRCODE_DATA_CORRUPTED),
						errmsg("calculated CRC checksum does not match value stored in file \"%s\"", path)));
	}
}

/* Like XidInMVCCSnapshot, but ignore the sub transaction in the RW */
static bool
xid_in_flashback_snapshot(Snapshot snapshot, TransactionId xid)
{
	/* Any xid < xmin is not in-progress */
	if (TransactionIdPrecedes(xid, snapshot->xmin))
		return false;

	/* Any xid >= xmax is in-progress */
	if (TransactionIdFollowsOrEquals(xid, snapshot->xmax))
		return true;

	return bsearch(&xid, snapshot->xip, snapshot->xcnt, sizeof(TransactionId), flashback_xid_comparator) != NULL;
}

/* true if given transaction committed */
static bool
polar_flashback_did_xid_commit(Snapshot snapshot, TransactionId xid, TransactionId max_xid,
							   uint32 next_clog_subdir_no, const char *fra_dir)
{
	XidStatus   xidstatus;

	if (!TransactionIdIsNormal(xid))
	{
		/*no cover begin*/
		if (TransactionIdEquals(xid, BootstrapTransactionId))
			return true;

		if (TransactionIdEquals(xid, FrozenTransactionId))
			return true;

		return false;
		/*no cover end*/
	}

	if (xid_in_flashback_snapshot(snapshot, xid))
		return false;

	xidstatus = polar_flashback_get_xid_status(xid, max_xid, next_clog_subdir_no, fra_dir);

	return xidstatus == TRANSACTION_STATUS_COMMITTED;
}

static TransactionId
get_next_valid_xid(TransactionId *xip, uint32 *xip_pos, uint32 xcnt,
				   uint32 *removed_xid_pos, uint32 removed_size, uint8 *removed_pos)
{
	int i;
	TransactionId next = InvalidTransactionId;

	for (i = *xip_pos + 1; i < xcnt + removed_size; i++)
	{
		/* Next is removed, so skip */
		if (removed_size &&
				*removed_pos < removed_size - 1 &&
				i == removed_xid_pos[*removed_pos + 1])
		{
			*removed_pos = *removed_pos + 1;
			continue;
		}
		else
		{
			next = xip[i];
			*xip_pos = i;
			break;
		}
	}

	return next;
}

/*
 * POLAR: Compact the removed xids from xip.
 */
static void
compact_removed_xids(TransactionId *xip, uint32 xcnt, flshbak_rel_snpsht_t rsnapshot)
{
	uint8 i = 0;
	uint32 new_pos = 0;
	uint32 old_pos = 0;

	Assert(xcnt);

	/* No removed xid kept */
	if (rsnapshot->removed_size == 0)
		return;

	/* Removed size is larger than zero */
	old_pos = new_pos = rsnapshot->removed_xid_pos[0];

	for (; new_pos < xcnt; new_pos++)
		xip[new_pos] = get_next_valid_xid(xip, &old_pos, xcnt, rsnapshot->removed_xid_pos, rsnapshot->removed_size, &i);

	/* Reset the removed size */
	rsnapshot->removed_size = 0;
}

static TransactionId *
bsearch_xip(TransactionId *xid, TransactionId *xip, uint32 xcnt, uint32 *removed_xid_pos,
			uint32 removed_size, uint8 *removed_pos)
{
	uint32 start = 0;
	uint32 end;

	Assert(xcnt);

	for (*removed_pos = 0; *removed_pos < removed_size;
			*removed_pos = *removed_pos + 1)
	{
		end = removed_xid_pos[(*removed_pos)];

		if (end == 0)
			continue;

		if (TransactionIdPrecedes(*xid, xip[end]))
			return bsearch(xid, xip + start, end - start,
						   sizeof(TransactionId), flashback_xid_comparator);

		/* The start has been searched */
		start = end;
	}

	/* In the last partion */
	return bsearch(xid, xip + start, xcnt + removed_size - start, sizeof(TransactionId), flashback_xid_comparator);
}

/*
 * Remove the xid from running xids.
 *
 * NB: We just insert the xid pos to removed_xid_pos array and compact the xip while the
 * removed_xid_pos is full.
 */
static void
remove_xid_from_running_xids(flshbak_rel_snpsht_t rsnapshot, TransactionId xid)
{
	TransactionId *xip;
	TransactionId *target_xid;
	uint32 xcnt;
	uint32 i = 0;
	uint8 removed_pos;
	uint32 xip_pos;

	/* Sometimes we get some xids less than xmin, just skip */
	if (unlikely(TransactionIdPrecedes(xid, rsnapshot->snapshot->xmin)))
		return;

	/* When we come here, xmin <= xid < xmax */
	Assert(TransactionIdPrecedes(rsnapshot->snapshot->xmin, rsnapshot->snapshot->xmax));
	xip = rsnapshot->snapshot->xip;
	xcnt = rsnapshot->snapshot->xcnt;
	Assert(xcnt > 0);

	/* Compact the xids when it is full */
	if (rsnapshot->removed_size == MAX_KEEP_REMOVED_XIDS)
		compact_removed_xids(xip, xcnt, rsnapshot);

	target_xid = bsearch_xip(&xid, xip, xcnt, rsnapshot->removed_xid_pos, rsnapshot->removed_size, &removed_pos);

	/*
	 * Because the snapshot taken lsn is before snapshot taken,
	 * so sometimes the xid has been committed, ignore it.
	 */
	if (unlikely(target_xid == NULL))
		return;

	Assert(target_xid && (*target_xid == xid));

	/* update the xcnt */
	if (rsnapshot->snapshot->xcnt)
		rsnapshot->snapshot->xcnt--;

	/* Insert into the removed xip */
	if (rsnapshot->removed_size)
	{
		uint32 prev = rsnapshot->removed_xid_pos[removed_pos];
		uint32 next;

		Assert(rsnapshot->removed_size < MAX_KEEP_REMOVED_XIDS);

		/* The length is less than MAX_KEEP_REMOVED_XIDS, so removed_xid_pos[i + 1] is safe to process */
		for (i = removed_pos; i < rsnapshot->removed_size; i++)
		{
			next = rsnapshot->removed_xid_pos[i + 1];
			rsnapshot->removed_xid_pos[i + 1] = prev;
			prev = next;
		}
	}

	/* Update the removed xids */
	xip_pos = target_xid - xip;
	rsnapshot->removed_xid_pos[removed_pos] = xip_pos;
	rsnapshot->removed_size++;
	Assert(rsnapshot->removed_size <= MAX_KEEP_REMOVED_XIDS);

	/* Update the xmin to next valid xip */
	if (xid == rsnapshot->snapshot->xmin)
	{
		TransactionId next_xid;

		next_xid = get_next_valid_xid(xip, &xip_pos, xcnt, rsnapshot->removed_xid_pos,
									  rsnapshot->removed_size, &removed_pos);

		if (TransactionIdIsValid(next_xid))
			rsnapshot->snapshot->xmin = next_xid;
		else
			rsnapshot->snapshot->xmin = rsnapshot->snapshot->xmax;
	}
}

static void
insert_xid_into_running_xids(flshbak_rel_snpsht_t rsnapshot, TransactionId xid)
{
	TransactionId *xip;
	TransactionId tmp = rsnapshot->snapshot->xmax;
	uint32 i;
	int32 insert_cnt;
	uint32 xcnt;

	Assert(TransactionIdFollowsOrEquals(xid, tmp));
	insert_cnt = (int32)(xid - tmp);

	/* The [tmp, xid] must contain InvalidTransactionId BootstrapTransactionId FrozenTransactionId */
	if (xid < tmp)
		insert_cnt -= FirstNormalTransactionId;

	/* Do nothing */
	if (insert_cnt == 0)
		return;

	xip = rsnapshot->snapshot->xip;
	xcnt = rsnapshot->snapshot->xcnt;
	rsnapshot->snapshot->xcnt += insert_cnt;

	if (unlikely(rsnapshot->xip_size == 0))
		rsnapshot->xip_size = xcnt;

	/* Enlarge the xip and pfree old one */
	if (rsnapshot->xip_size < xcnt + rsnapshot->removed_size + insert_cnt)
	{
		uint32 new_xip_size = GET_ENLARGE_XIP_SIZE(xcnt + rsnapshot->removed_size + insert_cnt);

		if (xip)
			xip = repalloc(xip, new_xip_size * sizeof(TransactionId));
		else
			xip = palloc0(new_xip_size * sizeof(TransactionId));

		rsnapshot->xip_size = new_xip_size;
		rsnapshot->snapshot->xip = xip;
	}

	xip += (xcnt + rsnapshot->removed_size);

	for (i = 0; i < insert_cnt; i++)
	{
		xip[i] = tmp;
		TransactionIdAdvance(tmp);
	}
}

static inline MemoryContext
get_snapshot_memory_context(void)
{
	static MemoryContext context = NULL;

	if (unlikely(context == NULL))
	{
		context = AllocSetContextCreate(TopMemoryContext,
				"flashback snapshot memory context",
				ALLOCSET_DEFAULT_SIZES);
		MemoryContextAllowInCriticalSection(context, true);
	}

	return context;
}

void
polar_update_flashback_snapshot(flshbak_rel_snpsht_t rsnapshot, TransactionId xid)
{
	if (TransactionIdPrecedes(xid, rsnapshot->snapshot->xmax))
		/* Remove it from running transaction xid */
		remove_xid_from_running_xids(rsnapshot, xid);
	else
	{
		/* Insert it into running xids */
		insert_xid_into_running_xids(rsnapshot, xid);
		/* Update the xmax to xid + 1 */
		TransactionIdAdvance(xid);
		rsnapshot->snapshot->xmax = xid;
	}
}

/*
 * Redo the transaction xlog record whose commit/abort time
 * is older than or equal to the end_time.
 *
 * Return true when the commit/abort time is newer than or
 * equal to the end_time.
 *
 * NB: last_xact_lsn will be the lsn of the transaction whose
 * commit/abort time is older than or equal to the end_time.
 */
bool
polar_flashback_xact_redo(XLogRecord *record, flshbak_rel_snpsht_t rsnapshot,
						  TimestampTz end_time, XLogReaderState *xlogreader)
{
	uint8 info;
	TransactionId xid = InvalidTransactionId;
	int nsubxacts = 0;
	TransactionId *subxacts;
	int i;
	bool finish = false;
	bool update_snapshot = false;

	xid = record->xl_xid;

	if (record->xl_rmid == RM_XACT_ID)
	{
		Assert(record->xl_rmid == RM_XACT_ID);
		info = record->xl_info & XLOG_XACT_OPMASK;
		/* Backup blocks are not used in xact records */
		Assert(!XLogRecHasAnyBlockRefs(xlogreader));

		if (info == XLOG_XACT_COMMIT || info == XLOG_XACT_COMMIT_PREPARED)
		{
			xl_xact_commit *xlrec = (xl_xact_commit *) XLogRecGetData(xlogreader);
			xl_xact_parsed_commit parsed_commit;

			/* It is finished while the xact time is newer than or equal to end time */
			if (xlrec->xact_time > end_time)
				return true;
			else if (unlikely(xlrec->xact_time == end_time))
				finish = true;

			/* Do something like xact_redo_commit */
			ParseCommitRecord(info, xlrec, &parsed_commit);
			nsubxacts = parsed_commit.nsubxacts;
			subxacts = parsed_commit.subxacts;
			update_snapshot = true;
		}
		else if (info == XLOG_XACT_ABORT || info == XLOG_XACT_ABORT_PREPARED)
		{
			xl_xact_abort *xlrec = (xl_xact_abort *) XLogRecGetData(xlogreader);
			xl_xact_parsed_abort parsed_abort;

			/* It is finished while the xact time is newer than or equal to end time */
			if (xlrec->xact_time > end_time)
				return true;
			else if (unlikely(xlrec->xact_time == end_time))
				finish = true;

			ParseAbortRecord(info, xlrec, &parsed_abort);
			nsubxacts = parsed_abort.nsubxacts;
			subxacts = parsed_abort.subxacts;
			update_snapshot = true;
		}

		/* Others we don't care */

		/* Update the snapshot */
		if (update_snapshot)
		{
			polar_update_flashback_snapshot(rsnapshot, xid);

			/* Process the sub transactions */
			for (i = 0; i < nsubxacts; i++)
			{
				TransactionId tmp;

				tmp = subxacts[i];
				polar_update_flashback_snapshot(rsnapshot, tmp);
			}
		}
	}

	/* Update the next xid and clog sub directory */
	if (TransactionIdIsValid(xid) &&
			TransactionIdEquals(rsnapshot->next_xid, xid))
	{
		TransactionIdAdvance(rsnapshot->next_xid);

		/* The xid is wrapped */
		if (TransactionIdEquals(xid, FirstNormalTransactionId))
			rsnapshot->next_clog_subdir_no++;
	}

	return finish;
}

/*
 * POLAR: Just like HeapTupleSatisfiesMVCC, but don't care about the sub xact
 * and multi xact.
 */
HTSV_Result
polar_tuple_satisfies_flashback(HeapTuple htup, Buffer buffer, Snapshot snapshot,
								uint32 next_clog_subdir_no, TransactionId max_xid, const char *fra_dir)
{
	HeapTupleHeader tuple = htup->t_data;

	Assert(ItemPointerIsValid(&htup->t_self));
	Assert(htup->t_tableOid != InvalidOid);

	/*
	 * Has inserting transaction committed?
	 *
	 * If the inserting transaction aborted, then the tuple was never visible
	 * to any other transaction, so we can delete it immediately.
	 */
	if (!HeapTupleHeaderXminCommitted(tuple))
	{
		/* Check Hint bits at first, it will speed up in many cases. */
		if (HeapTupleHeaderXminInvalid(tuple))
			return HEAPTUPLE_DEAD;
		else if (polar_flashback_did_xid_commit(snapshot, HeapTupleHeaderGetRawXmin(tuple), max_xid,
												next_clog_subdir_no, fra_dir))
			HeapTupleSetHintBits(tuple, buffer, HEAP_XMIN_COMMITTED,
								 HeapTupleHeaderGetRawXmin(tuple));
		else
		{
			/*
			 * In Progress, or Not Committed, we think the xmin is invalid.
			 */
			HeapTupleSetHintBits(tuple, buffer, HEAP_XMIN_INVALID,
								 InvalidTransactionId);
			return HEAPTUPLE_DEAD;
		}

		/*
		 * At this point the xmin is known committed, but we might not have
		 * been able to set the hint bit yet; so we can no longer Assert that
		 * it's set.
		 */
	}

	/*
	 * Okay, the inserter committed, so it was good at some point.  Now what
	 * about the deleting transaction?
	 */
	if (tuple->t_infomask & HEAP_XMAX_INVALID)
		return HEAPTUPLE_LIVE;

	if (HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask))
	{
		/*
		 * We don't really care whether xmax did commit, abort or crash. We
		 * know that xmax did lock the tuple, but it did not and will never
		 * actually update it.
		 */
		HeapTupleSetHintBits(tuple, buffer, HEAP_XMAX_INVALID,
							 InvalidTransactionId);
		return HEAPTUPLE_LIVE;
	}

	if (tuple->t_infomask & HEAP_XMAX_IS_MULTI)
	{
		HeapTupleSetHintBits(tuple, buffer, HEAP_XMAX_INVALID,
							 InvalidTransactionId);
		return HEAPTUPLE_LIVE;
	}

	if (!(tuple->t_infomask & HEAP_XMAX_COMMITTED))
	{
		if (polar_flashback_did_xid_commit(snapshot, HeapTupleHeaderGetRawXmax(tuple), max_xid,
										   next_clog_subdir_no, fra_dir))
			HeapTupleSetHintBits(tuple, buffer, HEAP_XMAX_COMMITTED,
								 HeapTupleHeaderGetRawXmax(tuple));
		else
		{
			/*
			 * Not in Progress, Not Committed, so either Aborted or crashed
			 */
			HeapTupleSetHintBits(tuple, buffer, HEAP_XMAX_INVALID,
								 InvalidTransactionId);
			return HEAPTUPLE_LIVE;
		}

		/*
		 * At this point the xmax is known committed, but we might not have
		 * been able to set the hint bit yet; so we can no longer Assert that
		 * it's set.
		 */
	}

	/* Otherwise, it's dead and removable */
	return HEAPTUPLE_DEAD;
}

void
log_flashback_snapshot(Snapshot snapshot, int elevel)
{
	int i;
	StringInfoData buf;

	/*
	 * Fill buf with a text serialization of the snapshot, plus identification
	 * data about this transaction.  The format expected by ImportSnapshot is
	 * pretty rigid: each line must be fieldname:value.
	 */
	initStringInfo(&buf);

	appendStringInfo(&buf, "Flashback snapshot info --  ");
	appendStringInfo(&buf, "insert wal lsn:%X/%X ", (uint32)(snapshot->lsn >> 32), (uint32) snapshot->lsn);
	appendStringInfo(&buf, "xmin:%u ", snapshot->xmin);
	appendStringInfo(&buf, "xmax:%u ", snapshot->xmax);
	appendStringInfo(&buf, "xcnt:%d ", snapshot->xcnt);

	for (i = 0; i < snapshot->xcnt; i++)
		appendStringInfo(&buf, "xip:%u ", snapshot->xip[i]);

	/*
	 * Similarly, we add our subcommitted child XIDs to the subxid data. Here,
	 * we have to cope with possible overflow.
	 */
	if (snapshot->suboverflowed)
		appendStringInfoString(&buf, "sof:1 ");
	else
	{
		appendStringInfoString(&buf, "sof:0 ");
		appendStringInfo(&buf, "sxcnt:%d ", snapshot->subxcnt);

		for (i = 0; i < snapshot->subxcnt; i++)
			appendStringInfo(&buf, "sxp:%u ", snapshot->subxip[i]);
	}

	if (unlikely(snapshot->takenDuringRecovery))
		appendStringInfo(&buf, "take during recovery.");

	elog(elevel, "%s", buf.data);

	/* free the buffer */
	pfree(buf.data);
}

flashback_snapshot_header_t
polar_get_flashback_snapshot_data(fra_ctl_t ins, XLogRecPtr lsn)
{
#define SNAPSHOT_DEFUALT_SIZE 1024
	static flashback_snapshot_header_t header = NULL; /* Keep it, it is smaller than 5kB */
	static Size size = 0;
	flashback_snapshot_t snapshot = NULL;
	RunningTransactions trans;
	TransactionId xmax;
	TransactionId *xip;
	uint32 data_size;
	MemoryContext old_context;

	if (!polar_enable_fra(ins))
		return NULL;

	/* Use context created by myself to avoid to delete by others */
	old_context = MemoryContextSwitchTo(get_snapshot_memory_context());
	trans = polar_get_running_top_trans();
	xmax = trans->latestCompletedXid;
	TransactionIdAdvance(xmax);
	data_size = FLSHBAK_GET_SNAPSHOT_DATA_SIZE(trans->xcnt);

	if (unlikely(size < data_size + FLSHBAK_SNAPSHOT_HEADER_SIZE))
	{
		if (header)
			pfree(header);

		size = Max(SNAPSHOT_DEFUALT_SIZE, data_size + FLSHBAK_SNAPSHOT_HEADER_SIZE);
		/* Use malloc, so avoid to be delete by others */
		header = (flashback_snapshot_header_t) palloc(size);
	}

	header->data_size = data_size;
	SET_FLSHBAK_SNAPSHOT_VERSION(header->info);

	snapshot = FLSHBAK_GET_SNAPSHOT_DATA(header);
	snapshot->lsn = lsn;
	snapshot->xcnt = trans->xcnt;
	snapshot->xmax = xmax;
	snapshot->xmin = trans->oldestRunningXid;
	snapshot->next_xid = trans->nextXid;
	snapshot->next_clog_subdir_no = pg_atomic_read_u32(&(ins->clog_ctl->next_clog_subdir_no));

	/* Don't care about the sub transaction in flashback snapshot */
	xip = (TransactionId *) POLAR_GET_FLSHBAK_SNAPSHOT_XIP(snapshot);
	memcpy(xip, trans->xids, snapshot->xcnt * sizeof(TransactionId));

	MemoryContextSwitchTo(old_context);
	return header;
}

/*
 * POLAR: Backup the transaction snapshot to fast recovery area.
 */
fbpoint_pos_t
polar_backup_snapshot_to_fra(flashback_snapshot_header_t header, fbpoint_pos_t *snapshot_end_pos, const char *fra_dir)
{
	uint32 data_size = 0;
	Size total_size = 0;
	pg_crc32 crc;
	uint32 seg_no;
	uint32 write_size = 0;
	char *data;
	uint32 offset;
	fbpoint_pos_t snapshot_pos;
	fbpoint_io_error_t io_error;

	Assert(header);
	if (unlikely(header->data_size > UINT_MAX))
		elog(ERROR, "The flashback snapshot size is over %u, cancel the flashback point and wait the next", UINT_MAX);

	data_size = header->data_size;
	total_size = FLSHBAK_SNAPSHOT_HEADER_SIZE + data_size;

	offset = snapshot_end_pos->offset;
	seg_no = snapshot_end_pos->seg_no;

	/* The flashback snapshot data header can't be splitted */
	if (offset - FBPOINT_REC_END_POS < FLSHBAK_SNAPSHOT_HEADER_SIZE)
	{
		seg_no++;
		offset = FBPOINT_SEG_SIZE;
	}

	SET_FLSHBAK_SNAPSHOT_END_POS(header->info, offset);
	/* Compute the CRC */
	COMPUTE_SNAPSHOT_DATA_CRC(crc, header, (char *) header + FLSHBAK_SNAPSHOT_HEADER_SIZE, data_size);
	header->crc = crc;

	if (offset - FBPOINT_REC_END_POS < total_size)
		SET_FBPOINT_POS(snapshot_pos, seg_no, FBPOINT_REC_END_POS);

	else
		SET_FBPOINT_POS(snapshot_pos, seg_no, (offset - total_size));

	data = (char *) header;

	/* Write the snapshot data and fsync file, the snapshot data may be split into many files */
	do
	{
		write_size = Min(total_size, offset - FBPOINT_REC_END_POS);
		offset -= write_size;

		if (!polar_write_fbpoint_file(fra_dir, data, seg_no, offset, write_size, &io_error))
			/*no cover line*/
			polar_fbpoint_report_io_error(fra_dir, &io_error, ERROR);

		SET_FBPOINT_POS(*snapshot_end_pos, seg_no, offset);
		total_size -= write_size;
		offset = FBPOINT_SEG_SIZE;
		seg_no++;
		data += write_size;
	}
	while (total_size > 0);

	return snapshot_pos;
}

/*
 * POLAR: Get snapshot from flashback snapshot file.
 *
 * However, some values in the snapshot we don't care.
 */
Snapshot
polar_get_flashback_snapshot(const char *fra_dir, fbpoint_pos_t start_pos,
		uint32 *next_clog_subdir_no, TransactionId *next_xid)
{
	flashback_snapshot_data_header_t header;
	uint32 data_size;
	uint32 offset;
	uint32 seg_no;
	char *data = NULL;
	Snapshot snapshot;
	fbpoint_io_error_t io_error;

	seg_no = start_pos.seg_no;
	offset = start_pos.offset;

	if (polar_read_fbpoint_file(fra_dir, (char *) &header, seg_no, offset, FLSHBAK_SNAPSHOT_HEADER_SIZE, &io_error))
	{
		char    path[MAXPGPATH];

		polar_get_fbpoint_file_path(seg_no, fra_dir, path);

		if (GET_FLSHBAK_SNAPSHOT_VERSION(header.info) != FLSHBAK_SNAPSHOT_DATA_VERSION)
			/*no cover line*/
			elog(ERROR, "The version of snapshot data in %s is %d not match with binary %d", path,
				 GET_FLSHBAK_SNAPSHOT_VERSION(header.info), FLSHBAK_SNAPSHOT_DATA_VERSION);
	}
	else
		/*no cover line*/
		polar_fbpoint_report_io_error(fra_dir, &io_error, FATAL);

	/* Read the snapshot data */
	data_size = header.data_size;
	data = palloc(data_size);
	offset = offset + FLSHBAK_SNAPSHOT_HEADER_SIZE;
	read_flashback_snapshot_data(fra_dir, seg_no, offset, &header, data);

	/* Convert the data to snapshot */
	snapshot = restore_flashback_snapshot((flashback_snapshot_t) data);
	*next_clog_subdir_no = ((flashback_snapshot_t) data)->next_clog_subdir_no;
	*next_xid = ((flashback_snapshot_t) data)->next_xid;

	pfree(data);
	log_flashback_snapshot(snapshot, DEBUG2);
	return snapshot;
}

inline void
polar_compact_xip(flshbak_rel_snpsht_t rsnapshot)
{
	if (rsnapshot->snapshot->xcnt)
		compact_removed_xids(rsnapshot->snapshot->xip, rsnapshot->snapshot->xcnt, rsnapshot);
}
