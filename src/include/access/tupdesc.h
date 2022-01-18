/*-------------------------------------------------------------------------
 *
 * tupdesc.h
 *	  POSTGRES tuple descriptor definitions.
 *
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/access/tupdesc.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef TUPDESC_H
#define TUPDESC_H

#include "access/attnum.h"
#include "catalog/pg_attribute.h"
#include "nodes/pg_list.h"


typedef struct attrDefault
{
	AttrNumber	adnum;
	char	   *adbin;			/* nodeToString representation of expr */
} AttrDefault;

typedef struct attrMissing *MissingPtr;

typedef struct constrCheck
{
	char	   *ccname;
	char	   *ccbin;			/* nodeToString representation of expr */
	bool		ccvalid;
	bool		ccnoinherit;	/* this is a non-inheritable constraint */
} ConstrCheck;

/* This structure contains constraints of a tuple */
typedef struct tupleConstr
{
	AttrDefault *defval;		/* array */
	ConstrCheck *check;			/* array */
	MissingPtr	missing;		/* missing attributes values, NULL if none */
	uint16		num_defval;
	uint16		num_check;
	bool		has_not_null;
} TupleConstr;

/*
 * This struct is passed around within the backend to describe the structure
 * of tuples.  For tuples coming from on-disk relations, the information is
 * collected from the pg_attribute, pg_attrdef, and pg_constraint catalogs.
 * Transient row types (such as the result of a join query) have anonymous
 * TupleDesc structs that generally omit any constraint info; therefore the
 * structure is designed to let the constraints be omitted efficiently.
 *
 * Note that only user attributes, not system attributes, are mentioned in
 * TupleDesc; with the exception that tdhasoid indicates if OID is present.
 *
 * If the tupdesc is known to correspond to a named rowtype (such as a table's
 * rowtype) then tdtypeid identifies that type and tdtypmod is -1.  Otherwise
 * tdtypeid is RECORDOID, and tdtypmod can be either -1 for a fully anonymous
 * row type, or a value >= 0 to allow the rowtype to be looked up in the
 * typcache.c type cache.
 *
 * Note that tdtypeid is never the OID of a domain over composite, even if
 * we are dealing with values that are known (at some higher level) to be of
 * a domain-over-composite type.  This is because tdtypeid/tdtypmod need to
 * match up with the type labeling of composite Datums, and those are never
 * explicitly marked as being of a domain type, either.
 *
 * Tuple descriptors that live in caches (relcache or typcache, at present)
 * are reference-counted: they can be deleted when their reference count goes
 * to zero.  Tuple descriptors created by the executor need no reference
 * counting, however: they are simply created in the appropriate memory
 * context and go away when the context is freed.  We set the tdrefcount
 * field of such a descriptor to -1, while reference-counted descriptors
 * always have tdrefcount >= 0.
 */
typedef struct tupleDesc
{
	int			natts;			/* number of attributes in the tuple */
	Oid			tdtypeid;		/* composite type ID for tuple type */
	int32		tdtypmod;		/* typmod for tuple type */
	bool		tdhasoid;		/* tuple has oid attribute in its header */
	int			tdrefcount;		/* reference count, or -1 if not counting */
	TupleConstr *constr;		/* constraints, or NULL if none */
	/* attrs[N] is the description of Attribute Number N+1 */
	FormData_pg_attribute attrs[FLEXIBLE_ARRAY_MEMBER];
}		   *TupleDesc;

/* POLAR px
 * When dispatching a planned statement from QC to PXs, we need to be able
 * to transmit TupleDescs. TupleDesc doesn't have the Node header, so for
 * convenience of the read and out functions, we wrap them in TupleDescNode
 * structs, which do.
 *
 * These are never serialized on disk, only in the read/outfast protocol,
 * as part of PlannedStmts.
 */
typedef struct tupleDescNode
{
	NodeTag		type;
	int			natts;
	TupleDesc	tuple;
} TupleDescNode;
/* POLAR end */

/* Accessor for the i'th attribute of tupdesc. */
#define TupleDescAttr(tupdesc, i) (&(tupdesc)->attrs[(i)])

extern TupleDesc CreateTemplateTupleDesc(int natts, bool hasoid);

extern TupleDesc CreateTupleDesc(int natts, bool hasoid,
				Form_pg_attribute *attrs);

extern TupleDesc CreateTupleDescCopy(TupleDesc tupdesc);

extern TupleDesc CreateTupleDescCopyConstr(TupleDesc tupdesc);

#define TupleDescSize(src) \
	(offsetof(struct tupleDesc, attrs) + \
	 (src)->natts * sizeof(FormData_pg_attribute))

extern void TupleDescCopy(TupleDesc dst, TupleDesc src);

extern void TupleDescCopyEntry(TupleDesc dst, AttrNumber dstAttno,
				   TupleDesc src, AttrNumber srcAttno);

extern void FreeTupleDesc(TupleDesc tupdesc);

extern void IncrTupleDescRefCount(TupleDesc tupdesc);
extern void DecrTupleDescRefCount(TupleDesc tupdesc);

#define PinTupleDesc(tupdesc) \
	do { \
		if ((tupdesc)->tdrefcount >= 0) \
			IncrTupleDescRefCount(tupdesc); \
	} while (0)

#define ReleaseTupleDesc(tupdesc) \
	do { \
		if ((tupdesc)->tdrefcount >= 0) \
			DecrTupleDescRefCount(tupdesc); \
	} while (0)

extern bool equalTupleDescs(TupleDesc tupdesc1, TupleDesc tupdesc2);

extern uint32 hashTupleDesc(TupleDesc tupdesc);

extern void TupleDescInitEntry(TupleDesc desc,
				   AttrNumber attributeNumber,
				   const char *attributeName,
				   Oid oidtypeid,
				   int32 typmod,
				   int attdim);

extern void TupleDescInitBuiltinEntry(TupleDesc desc,
						  AttrNumber attributeNumber,
						  const char *attributeName,
						  Oid oidtypeid,
						  int32 typmod,
						  int attdim);

extern void TupleDescInitEntryCollation(TupleDesc desc,
							AttrNumber attributeNumber,
							Oid collationid);

extern TupleDesc BuildDescForRelation(List *schema);

extern TupleDesc BuildDescFromLists(List *names, List *types, List *typmods, List *collations);

#endif							/* TUPDESC_H */
