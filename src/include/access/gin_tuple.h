/*--------------------------------------------------------------------------
 * gin_tuple.h
 *	  Public header file for Generalized Inverted Index access method.
 *
 *	Copyright (c) 2006-2025, PostgreSQL Global Development Group
 *
 *	src/include/access/gin_tuple.h
 *--------------------------------------------------------------------------
 */
#ifndef GIN_TUPLE_H
#define GIN_TUPLE_H

#include "access/ginblock.h"
#include "storage/itemptr.h"
#include "utils/sortsupport.h"

/*
 * Data for one key in a GIN index.  (This is not the permanent in-index
 * representation, but just a convenient format to use during the tuplesort
 * stage of building a new GIN index.)
 */
typedef struct GinTuple
{
	int			tuplen;			/* length of the whole tuple */
	OffsetNumber attrnum;		/* attnum of index key */
	uint16		keylen;			/* bytes in data for key value */
	int16		typlen;			/* typlen for key */
	bool		typbyval;		/* typbyval for key */
	signed char category;		/* category: normal or NULL? */
	int			nitems;			/* number of TIDs in the data */
	char		data[FLEXIBLE_ARRAY_MEMBER];
} GinTuple;

static inline ItemPointer
GinTupleGetFirst(GinTuple *tup)
{
	GinPostingList *list;

	list = (GinPostingList *) SHORTALIGN(tup->data + tup->keylen);

	return &list->first;
}

extern int	_gin_compare_tuples(GinTuple *a, GinTuple *b, SortSupport ssup);

#endif							/* GIN_TUPLE_H */
