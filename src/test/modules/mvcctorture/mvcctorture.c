/*-------------------------------------------------------------------------
 *
 * mvctorture.c
 *
 * Copyright (c) 2012, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/modules/mvcctorture.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/heapam.h"
#include "access/hio.h"
#include "access/htup_details.h"
#include "access/transam.h"
#include "access/xact.h"
#include "access/visibilitymap.h"
#include "catalog/pg_am.h"
#include "executor/spi.h"
#include "funcapi.h"
#include "nodes/makefuncs.h"
#include "storage/bufmgr.h"
#include "utils/rel.h"

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(populate_mvcc_test_table);

Datum
populate_mvcc_test_table(PG_FUNCTION_ARGS)
{
	uint32		nrows = PG_GETARG_UINT32(0);
	bool		set_xmin_committed = PG_GETARG_BOOL(1);
	RangeVar   *rv;
	Relation	rel;
	Datum		values[1];
	bool		isnull[1];
	HeapTuple	tup;
	TransactionId *xids;
	int			ret;
	int			i;
	Buffer		buffer;
	Buffer		vmbuffer = InvalidBuffer;

	/* Connect to SPI manager */
	if ((ret = SPI_connect()) < 0)
		/* internal error */
		elog(ERROR, "populate_mvcc_test_table: SPI_connect returned %d", ret);

	SPI_execute("CREATE TABLE mvcc_test_table(i int4)", false, 0);

	SPI_finish();

	/* Generate a different XID for each tuple */
	xids = (TransactionId *) palloc0(nrows * sizeof(TransactionId));
	for (i = 0; i < nrows; i++)
	{
		BeginInternalSubTransaction(NULL);
		xids[i] = GetCurrentTransactionId();
		ReleaseCurrentSubTransaction();
	}

	rv = makeRangeVar(NULL, "mvcc_test_table", -1);

	rel = heap_openrv(rv, RowExclusiveLock);

	/* shuffle */
	for (i = 0; i < nrows - 1; i++)
	{
		int x = i + (random() % (nrows - i));
		TransactionId tmp;

		tmp = xids[i];
		xids[i] = xids[x];
		xids[x] = tmp;
	}

	for (i = 0; i < nrows; i++)
	{
		values[0] = Int32GetDatum(i);
		isnull[0] = false;

		tup = heap_form_tuple(RelationGetDescr(rel), values, isnull);

		/* Fill the header fields, like heap_prepare_insert does */
		tup->t_data->t_infomask &= ~(HEAP_XACT_MASK);
		tup->t_data->t_infomask2 &= ~(HEAP2_XACT_MASK);
		tup->t_data->t_infomask |= HEAP_XMAX_INVALID;
		if (set_xmin_committed)
			tup->t_data->t_infomask |= HEAP_XMIN_COMMITTED;
		HeapTupleHeaderSetXmin(tup->t_data, xids[i]);
		HeapTupleHeaderSetCmin(tup->t_data, 1);
		HeapTupleHeaderSetXmax(tup->t_data, 0);		/* for cleanliness */
		tup->t_tableOid = RelationGetRelid(rel);


		/*
		 * Find buffer to insert this tuple into.  If the page is all visible,
		 * this will also pin the requisite visibility map page.
		 */
		buffer = RelationGetBufferForTuple(rel, tup->t_len,
										   InvalidBuffer,
										   0, NULL,
										   &vmbuffer, NULL);
		RelationPutHeapTuple(rel, buffer, tup, false);

		if (PageIsAllVisible(BufferGetPage(buffer)))
		{
			PageClearAllVisible(BufferGetPage(buffer));
			visibilitymap_clear(rel,
								ItemPointerGetBlockNumber(&(tup->t_self)),
								vmbuffer, VISIBILITYMAP_VALID_BITS, NULL);
		}

		MarkBufferDirty(buffer);
		UnlockReleaseBuffer(buffer);
		heap_freetuple(tup);
	}

	if (vmbuffer != InvalidBuffer)
		ReleaseBuffer(vmbuffer);

	heap_close(rel, NoLock);

	PG_RETURN_VOID();
}
