/*-------------------------------------------------------------------------
 *
 * pg_prewarm.c
 *		  prewarming utilities
 *
 * Copyright (c) 2010-2025, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		  contrib/pg_prewarm/pg_prewarm.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <sys/stat.h>
#include <unistd.h>

#include "access/relation.h"
#include "catalog/index.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "storage/bufmgr.h"
#include "storage/lmgr.h"
#include "storage/read_stream.h"
#include "storage/smgr.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"

PG_MODULE_MAGIC_EXT(
					.name = "pg_prewarm",
					.version = PG_VERSION
);

PG_FUNCTION_INFO_V1(pg_prewarm);

typedef enum
{
	PREWARM_PREFETCH,
	PREWARM_READ,
	PREWARM_BUFFER,
} PrewarmType;

static PGIOAlignedBlock blockbuffer;

/*
 * pg_prewarm(regclass, mode text, fork text,
 *			  first_block int8, last_block int8)
 *
 * The first argument is the relation to be prewarmed; the second controls
 * how prewarming is done; legal options are 'prefetch', 'read', and 'buffer'.
 * The third is the name of the relation fork to be prewarmed.  The fourth
 * and fifth arguments specify the first and last block to be prewarmed.
 * If the fourth argument is NULL, it will be taken as 0; if the fifth argument
 * is NULL, it will be taken as the number of blocks in the relation.  The
 * return value is the number of blocks successfully prewarmed.
 */
Datum
pg_prewarm(PG_FUNCTION_ARGS)
{
	Oid			relOid;
	text	   *forkName;
	text	   *type;
	int64		first_block;
	int64		last_block;
	int64		nblocks;
	int64		blocks_done = 0;
	int64		block;
	Relation	rel;
	ForkNumber	forkNumber;
	char	   *forkString;
	char	   *ttype;
	PrewarmType ptype;
	AclResult	aclresult;
	char		relkind;
	Oid			privOid;

	/* Basic sanity checking. */
	if (PG_ARGISNULL(0))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("relation cannot be null")));
	relOid = PG_GETARG_OID(0);
	if (PG_ARGISNULL(1))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("prewarm type cannot be null")));
	type = PG_GETARG_TEXT_PP(1);
	ttype = text_to_cstring(type);
	if (strcmp(ttype, "prefetch") == 0)
		ptype = PREWARM_PREFETCH;
	else if (strcmp(ttype, "read") == 0)
		ptype = PREWARM_READ;
	else if (strcmp(ttype, "buffer") == 0)
		ptype = PREWARM_BUFFER;
	else
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid prewarm type"),
				 errhint("Valid prewarm types are \"prefetch\", \"read\", and \"buffer\".")));
		PG_RETURN_INT64(0);		/* Placate compiler. */
	}
	if (PG_ARGISNULL(2))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("relation fork cannot be null")));
	forkName = PG_GETARG_TEXT_PP(2);
	forkString = text_to_cstring(forkName);
	forkNumber = forkname_to_number(forkString);

	/*
	 * Open relation and check privileges.  If the relation is an index, we
	 * must check the privileges on its parent table instead.
	 */
	relkind = get_rel_relkind(relOid);
	if (relkind == RELKIND_INDEX ||
		relkind == RELKIND_PARTITIONED_INDEX)
	{
		privOid = IndexGetRelation(relOid, true);

		/* Lock table before index to avoid deadlock. */
		if (OidIsValid(privOid))
			LockRelationOid(privOid, AccessShareLock);
	}
	else
		privOid = relOid;

	rel = relation_open(relOid, AccessShareLock);

	/*
	 * It's possible that the relation with OID "privOid" was dropped and the
	 * OID was reused before we locked it.  If that happens, we could be left
	 * with the wrong parent table OID, in which case we must ERROR.  It's
	 * possible that such a race would change the outcome of
	 * get_rel_relkind(), too, but the worst case scenario there is that we'll
	 * check privileges on the index instead of its parent table, which isn't
	 * too terrible.
	 */
	if (!OidIsValid(privOid) ||
		(privOid != relOid &&
		 privOid != IndexGetRelation(relOid, true)))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_TABLE),
				 errmsg("could not find parent table of index \"%s\"",
						RelationGetRelationName(rel))));

	aclresult = pg_class_aclcheck(privOid, GetUserId(), ACL_SELECT);
	if (aclresult != ACLCHECK_OK)
		aclcheck_error(aclresult, get_relkind_objtype(rel->rd_rel->relkind), get_rel_name(relOid));

	/* Check that the relation has storage. */
	if (!RELKIND_HAS_STORAGE(rel->rd_rel->relkind))
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("relation \"%s\" does not have storage",
						RelationGetRelationName(rel)),
				 errdetail_relkind_not_supported(rel->rd_rel->relkind)));

	/* Check that the fork exists. */
	if (!smgrexists(RelationGetSmgr(rel), forkNumber))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("fork \"%s\" does not exist for this relation",
						forkString)));

	/* Validate block numbers, or handle nulls. */
	nblocks = RelationGetNumberOfBlocksInFork(rel, forkNumber);
	if (PG_ARGISNULL(3))
		first_block = 0;
	else
	{
		first_block = PG_GETARG_INT64(3);
		if (first_block < 0 || first_block >= nblocks)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("starting block number must be between 0 and %" PRId64,
							(nblocks - 1))));
	}
	if (PG_ARGISNULL(4))
		last_block = nblocks - 1;
	else
	{
		last_block = PG_GETARG_INT64(4);
		if (last_block < 0 || last_block >= nblocks)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("ending block number must be between 0 and %" PRId64,
							(nblocks - 1))));
	}

	/* Now we're ready to do the real work. */
	if (ptype == PREWARM_PREFETCH)
	{
#ifdef USE_PREFETCH

		/*
		 * In prefetch mode, we just hint the OS to read the blocks, but we
		 * don't know whether it really does it, and we don't wait for it to
		 * finish.
		 *
		 * It would probably be better to pass our prefetch requests in chunks
		 * of a megabyte or maybe even a whole segment at a time, but there's
		 * no practical way to do that at present without a gross modularity
		 * violation, so we just do this.
		 */
		for (block = first_block; block <= last_block; ++block)
		{
			CHECK_FOR_INTERRUPTS();
			PrefetchBuffer(rel, forkNumber, block);
			++blocks_done;
		}
#else
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("prefetch is not supported by this build")));
#endif
	}
	else if (ptype == PREWARM_READ)
	{
		/*
		 * In read mode, we actually read the blocks, but not into shared
		 * buffers.  This is more portable than prefetch mode (it works
		 * everywhere) and is synchronous.
		 */
		for (block = first_block; block <= last_block; ++block)
		{
			CHECK_FOR_INTERRUPTS();
			smgrread(RelationGetSmgr(rel), forkNumber, block, blockbuffer.data);
			++blocks_done;
		}
	}
	else if (ptype == PREWARM_BUFFER)
	{
		BlockRangeReadStreamPrivate p;
		ReadStream *stream;

		/*
		 * In buffer mode, we actually pull the data into shared_buffers.
		 */

		/* Set up the private state for our streaming buffer read callback. */
		p.current_blocknum = first_block;
		p.last_exclusive = last_block + 1;

		/*
		 * It is safe to use batchmode as block_range_read_stream_cb takes no
		 * locks.
		 */
		stream = read_stream_begin_relation(READ_STREAM_MAINTENANCE |
											READ_STREAM_FULL |
											READ_STREAM_USE_BATCHING,
											NULL,
											rel,
											forkNumber,
											block_range_read_stream_cb,
											&p,
											0);

		for (block = first_block; block <= last_block; ++block)
		{
			Buffer		buf;

			CHECK_FOR_INTERRUPTS();
			buf = read_stream_next_buffer(stream, NULL);
			ReleaseBuffer(buf);
			++blocks_done;
		}
		Assert(read_stream_next_buffer(stream, NULL) == InvalidBuffer);
		read_stream_end(stream);
	}

	/* Close relation, release locks. */
	relation_close(rel, AccessShareLock);

	if (privOid != relOid)
		UnlockRelationOid(privOid, AccessShareLock);

	PG_RETURN_INT64(blocks_done);
}
