/*-------------------------------------------------------------------------
 *
 * csnlogdesc.c
 *	  rmgr descriptor routines for access/transam/csnlog.c
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/access/rmgrdesc/ctslogdesc.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/ctslog.h"


void
ctslog_desc(StringInfo buf, XLogReaderState *record)
{
	char	   *rec = XLogRecGetData(record);
	uint8		info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

	if (info == CTSLOG_ZEROPAGE)
	{
		int			pageno;

		memcpy(&pageno, rec, sizeof(int));
		appendStringInfo(buf, "page %d", pageno);
	}
	else if (info == CTSLOG_TRUNCATE)
	{
		xl_ctslog_truncate xlrec;

		memcpy(&xlrec, rec, sizeof(xl_ctslog_truncate));
		appendStringInfo(buf, "page %d; oldestXact %u",
						 xlrec.pageno, xlrec.oldestXact);
	}
}

const char *
ctslog_identify(uint8 info)
{
	const char *id = NULL;

	switch (info & ~XLR_INFO_MASK)
	{
		case CTSLOG_ZEROPAGE:
			id = "ZEROPAGE";
			break;
		case CTSLOG_TRUNCATE:
			id = "TRUNCATE";
			break;
	}

	return id;
}
