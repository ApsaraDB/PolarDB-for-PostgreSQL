/*-------------------------------------------------------------------------
 *
 * polar_flashback_rel_filenode.c
 *
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2021, Alibaba Group Holding limited
 *
 * IDENTIFICATION
 *    src/backend/polar_flashback/polar_flashback_rel_filenode.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/heapam.h"
#include "catalog/catalog.h"
#include "common/relpath.h"
#include "polar_flashback/polar_fast_recovery_area.h"
#include "polar_flashback/polar_flashback_log.h"
#include "polar_flashback/polar_flashback_log_reader.h"
#include "polar_flashback/polar_flashback_rel_filenode.h"
#include "utils/palloc.h"

#define NEED_LOG_RELNODE_UPDATE(rel, change_persistence) (polar_can_rel_flashback((rel)->rd_rel, (rel)->rd_id, (change_persistence)))

static void
decode_rel_filenode_rec(flog_buf_ctl_t buf_ctl, polar_flog_rec_ptr ptr, fl_filenode_rec_data_t *result,
						bool *can_flashback, flog_reader_state *reader)
{
	flog_record *rec;
	fl_filenode_rec_data_t *rec_data;

	rec = polar_decode_flog_rec_common(reader, ptr, REL_FILENODE_ID);

	Assert(rec->xl_rmid == REL_FILENODE_ID);

	rec_data = FL_GET_FILENODE_REC_DATA(rec);
	memcpy(result, rec_data, FL_FILENODE_REC_SIZE);
	*can_flashback = ((rec->xl_info) & REL_CAN_FLASHBACK);
}

/*
 * POLAR: Insert the rel filenode record in the flashback log.
 */
static polar_flog_rec_ptr
polar_insert_filenode_rec(flog_ctl_t ins, RelFileNode *old, RelFileNode *new, uint8 info)
{
	BufferTag new_tag;
	flog_insert_context insert_context;

	INIT_BUFFERTAG(new_tag, *new, FILENODE_FORK, 0);
	/* Construct the insert context */
	insert_context.buf_tag = &new_tag;
	insert_context.data = old;
	insert_context.rmgr = REL_FILENODE_ID;
	/* This will be update in the flashback log record */
	insert_context.info = info;

	return polar_flog_insert_into_buffer(ins, &insert_context);
}

flog_record *
polar_assemble_filenode_rec(flog_insert_context *insert_context, uint32 xl_tot_len)
{

	flog_record *rec;
	fl_filenode_rec_data_t *rec_data;

	xl_tot_len = POLAR_GET_FILENODE_REC_LEN(xl_tot_len);

	/* Construct the flashback log record */
	rec = polar_palloc_in_crit(xl_tot_len);
	rec->xl_tot_len = xl_tot_len;
	rec->xl_info = insert_context->info;

	rec_data = FL_GET_FILENODE_REC_DATA(rec);
	COPY_REL_FILENODE(*(RelFileNode *)(insert_context->data), rec_data->old_filenode);
	COPY_REL_FILENODE(insert_context->buf_tag->rnode, rec_data->new_filenode);
	rec_data->time = GetCurrentTimestamp();
	return rec;
}

/*
 * POLAR: Log the filenode update in the flashback log.
 *
 * It is used by rewrite the table or alter table set tablespace.
 *
 * NB: We think rewrite the table by vacuum full or alter table
 * change the page content, so we can't use it as origin page to flashback.
 */
void
polar_flog_filenode_update(flog_ctl_t flog_ins, fra_ctl_t fra_ins, Oid relid, Oid new_rnode,
						   Oid new_tablespace, bool change_persistence, bool can_flashback)
{
	Relation rel = NULL;
	uint8 info = 0;

	/* The fast recovery area is disable, so just skip */
	if (!polar_enable_fra(fra_ins))
		return;

	Assert(OidIsValid(new_rnode) || OidIsValid(new_tablespace));

	rel = relation_open(relid, AccessShareLock);

	/* When change the relation persistence, don't need to check the relation persistence. */
	if (NEED_LOG_RELNODE_UPDATE(rel, change_persistence))
	{
		RelFileNode old_filenode;
		RelFileNode new_filenode;
		polar_flog_rec_ptr ptr;

		old_filenode = rel->rd_node;
		new_filenode = old_filenode;

		if (OidIsValid(new_rnode))
			new_filenode.relNode = new_rnode;

		if (OidIsValid(new_tablespace))
			new_filenode.spcNode = new_tablespace;

		if (can_flashback)
			info |= REL_CAN_FLASHBACK;

		ptr = polar_insert_filenode_rec(flog_ins, &old_filenode, &new_filenode, info);
		/* Flush it right now to avoid to miss it */
		polar_flog_flush(flog_ins->buf_ctl, ptr);
	}

	relation_close(rel, AccessShareLock);
}

/*
 * POLAR: Get the original relation file node.
 */
bool
polar_find_origin_filenode(flog_ctl_t ins, RelFileNode *filenode, TimestampTz target_time,
						   polar_flog_rec_ptr start_ptr, polar_flog_rec_ptr end_ptr, flog_reader_state *reader)
{
	log_index_page_iter_t filenode_iter;
	logindex_snapshot_t snapshot = ins->logindex_snapshot;
	BufferTag tag;
	log_index_lsn_t *lsn_info;
	polar_flog_rec_ptr ptr;
	bool result = false;

	INIT_BUFFERTAG(tag, *filenode, FILENODE_FORK, 0);

	filenode_iter =
		polar_logindex_create_page_iterator(snapshot, &tag, start_ptr, end_ptr, false);

	if (polar_logindex_page_iterator_state(filenode_iter) != ITERATE_STATE_FINISHED)
	{
		/*no cover begin*/
		polar_logindex_release_page_iterator(filenode_iter);
		elog(ERROR, "Failed to iterate data for [%u, %u, %u] relation file node, "
			 "which start pointer =%X/%X and end pointer =%X/%X",
			 tag.rnode.spcNode,
			 tag.rnode.dbNode,
			 tag.rnode.relNode,
			 (uint32)((start_ptr) >> 32), (uint32)start_ptr,
			 (uint32)((end_ptr - 1) >> 32), (uint32)(end_ptr - 1));
		return false;
		/*no cover end*/
	}

	/* It is just one record */
	if ((lsn_info = polar_logindex_page_iterator_next(filenode_iter)) != NULL)
	{
		fl_filenode_rec_data_t rec_data;
		bool can_flashback = false;

		ptr = (polar_flog_rec_ptr) lsn_info->lsn;
		Assert(BUFFERTAGS_EQUAL(*(lsn_info->tag), tag));

		decode_rel_filenode_rec(ins->buf_ctl, ptr, &rec_data, &can_flashback, reader);

		if (!RelFileNodeEquals(*filenode, rec_data.new_filenode))
			/*no cover line*/
			elog(ERROR, "The relation file node in flashback log record %X/%X is "
				 "([%u, %u, %u]) not ([%u, %u, %u])",
				 (uint32)(ptr >> 32), (uint32) ptr,
				 rec_data.new_filenode.spcNode, rec_data.new_filenode.dbNode,
				 rec_data.new_filenode.relNode, filenode->spcNode, filenode->dbNode,
				 filenode->relNode);

		elog(DEBUG2, "The original relation file node for [%u, %u, %u] is [%u, %u, %u] before %s",
			 filenode->spcNode, filenode->dbNode, filenode->relNode,
			 rec_data.old_filenode.spcNode, rec_data.old_filenode.dbNode,
			 rec_data.old_filenode.relNode, timestamptz_to_str(rec_data.time));

		/* It is what we want */
		if (timestamptz_cmp_internal(rec_data.time, target_time) > 0)
		{
			if (!can_flashback)
			{
				polar_logindex_release_page_iterator(filenode_iter);
				elog(ERROR, "The relation file node has been changed by vacuum full "
					 "or alter table or truncate table in the past, "
					 "we can't flashback the relation.");
			}

			result = true;
			COPY_REL_FILENODE(rec_data.old_filenode, *filenode);
		}
	}

	polar_logindex_release_page_iterator(filenode_iter);
	return result;
}

