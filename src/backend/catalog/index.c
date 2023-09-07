/*-------------------------------------------------------------------------
 *
 * index.c
 *	  code to create and destroy POSTGRES index relations
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/catalog/index.c
 *
 *
 * INTERFACE ROUTINES
 *		index_create()			- Create a cataloged index relation
 *		index_drop()			- Removes index relation from catalogs
 *		BuildIndexInfo()		- Prepare to insert index tuples
 *		FormIndexDatum()		- Construct datum vector for one index tuple
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <unistd.h>

#include "access/amapi.h"
#include "access/multixact.h"
#include "access/relscan.h"
#include "access/reloptions.h"
#include "access/sysattr.h"
#include "access/transam.h"
#include "access/visibilitymap.h"
#include "access/xact.h"
#include "bootstrap/bootstrap.h"
#include "catalog/binary_upgrade.h"
#include "catalog/catalog.h"
#include "catalog/dependency.h"
#include "catalog/heap.h"
#include "catalog/index.h"
#include "catalog/objectaccess.h"
#include "catalog/pg_am.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_constraint.h"
#include "catalog/pg_depend.h"
#include "catalog/pg_inherits.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_opclass.h"
#include "catalog/pg_tablespace.h"
#include "catalog/pg_trigger.h"
#include "catalog/pg_type.h"
#include "catalog/storage.h"
#include "commands/tablecmds.h"
#include "commands/event_trigger.h"
#include "commands/trigger.h"
#include "executor/executor.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/clauses.h"
#include "optimizer/planner.h"
#include "parser/parser.h"
#include "rewrite/rewriteManip.h"
#include "storage/bufmgr.h"
#include "storage/lmgr.h"
#include "storage/polar_bufmgr.h"
#include "storage/predicate.h"
#include "storage/procarray.h"
#include "storage/smgr.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/guc.h"
#include "utils/inval.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/pg_rusage.h"
#include "utils/syscache.h"
#include "utils/tuplesort.h"
#include "utils/snapmgr.h"
#include "utils/tqual.h"

/* POLAR */
#include "access/px_btbuild.h"
#include "executor/spi.h"

/* Potentially set by pg_upgrade_support functions */
Oid			binary_upgrade_next_index_pg_class_oid = InvalidOid;

/* state info for validate_index bulkdelete callback */
typedef struct
{
	Tuplesortstate *tuplesort;	/* for sorting the index TIDs */
	/* statistics (for debug purposes only): */
	double		htups,
				itups,
				tups_inserted;
} v_i_state;

/*
 * Pointer-free representation of variables used when reindexing system
 * catalogs; we use this to propagate those values to parallel workers.
 */
typedef struct
{
	Oid			currentlyReindexedHeap;
	Oid			currentlyReindexedIndex;
	int			numPendingReindexedIndexes;
	Oid			pendingReindexedIndexes[FLEXIBLE_ARRAY_MEMBER];
} SerializedReindexState;

/* non-export function prototypes */
static bool relationHasPrimaryKey(Relation rel);
static TupleDesc ConstructTupleDescriptor(Relation heapRelation,
						 IndexInfo *indexInfo,
						 List *indexColNames,
						 Oid accessMethodObjectId,
						 Oid *collationObjectId,
						 Oid *classObjectId);
static void InitializeAttributeOids(Relation indexRelation,
						int numatts, Oid indexoid);
static void AppendAttributeTuples(Relation indexRelation, int numatts);
static void UpdateIndexRelation(Oid indexoid, Oid heapoid,
					Oid parentIndexId,
					IndexInfo *indexInfo,
					Oid *collationOids,
					Oid *classOids,
					int16 *coloptions,
					bool primary,
					bool isexclusion,
					bool immediate,
					bool isvalid,
					bool isready);
static void index_update_stats(Relation rel,
				   bool hasindex,
				   double reltuples);
static void IndexCheckExclusion(Relation heapRelation,
					Relation indexRelation,
					IndexInfo *indexInfo);
static inline int64 itemptr_encode(ItemPointer itemptr);
static inline void itemptr_decode(ItemPointer itemptr, int64 encoded);
static bool validate_index_callback(ItemPointer itemptr, void *opaque);
static void validate_index_heapscan(Relation heapRelation,
						Relation indexRelation,
						IndexInfo *indexInfo,
						Snapshot snapshot,
						v_i_state *state);
static bool ReindexIsCurrentlyProcessingIndex(Oid indexOid);
static void SetReindexProcessing(Oid heapOid, Oid indexOid);
static void ResetReindexProcessing(void);
static void SetReindexPending(List *indexes);
static void RemoveReindexPending(Oid indexOid);

/* POLAR */
Datum polar_get_root_ctid(HeapTuple tuple, Buffer buffer, ExprContext *econtext);
static void polar_px_validate_index_heapscan(Relation heapRelation,
						Relation indexRelation,
						IndexInfo *indexInfo,
						Snapshot snapshot,
						v_i_state *state);

/*
 * relationHasPrimaryKey
 *		See whether an existing relation has a primary key.
 *
 * Caller must have suitable lock on the relation.
 *
 * Note: we intentionally do not check IndexIsValid here; that's because this
 * is used to enforce the rule that there can be only one indisprimary index,
 * and we want that to be true even if said index is invalid.
 */
static bool
relationHasPrimaryKey(Relation rel)
{
	bool		result = false;
	List	   *indexoidlist;
	ListCell   *indexoidscan;

	/*
	 * Get the list of index OIDs for the table from the relcache, and look up
	 * each one in the pg_index syscache until we find one marked primary key
	 * (hopefully there isn't more than one such).
	 */
	indexoidlist = RelationGetIndexList(rel);

	foreach(indexoidscan, indexoidlist)
	{
		Oid			indexoid = lfirst_oid(indexoidscan);
		HeapTuple	indexTuple;

		indexTuple = SearchSysCache1(INDEXRELID, ObjectIdGetDatum(indexoid));
		if (!HeapTupleIsValid(indexTuple))	/* should not happen */
			elog(ERROR, "cache lookup failed for index %u", indexoid);
		result = ((Form_pg_index) GETSTRUCT(indexTuple))->indisprimary;
		ReleaseSysCache(indexTuple);
		if (result)
			break;
	}

	list_free(indexoidlist);

	return result;
}

/*
 * index_check_primary_key
 *		Apply special checks needed before creating a PRIMARY KEY index
 *
 * This processing used to be in DefineIndex(), but has been split out
 * so that it can be applied during ALTER TABLE ADD PRIMARY KEY USING INDEX.
 *
 * We check for a pre-existing primary key, and that all columns of the index
 * are simple column references (not expressions), and that all those
 * columns are marked NOT NULL.  If they aren't (which can only happen during
 * ALTER TABLE ADD CONSTRAINT, since the parser forces such columns to be
 * created NOT NULL during CREATE TABLE), do an ALTER SET NOT NULL to mark
 * them so --- or fail if they are not in fact nonnull.
 *
 * As of PG v10, the SET NOT NULL is applied to child tables as well, so
 * that the behavior is like a manual SET NOT NULL.
 *
 * Caller had better have at least ShareLock on the table, else the not-null
 * checking isn't trustworthy.
 */
void
index_check_primary_key(Relation heapRel,
						IndexInfo *indexInfo,
						bool is_alter_table,
						IndexStmt *stmt)
{
	List	   *cmds;
	int			i;

	/*
	 * If ALTER TABLE and CREATE TABLE .. PARTITION OF, check that there isn't
	 * already a PRIMARY KEY.  In CREATE TABLE for an ordinary relations, we
	 * have faith that the parser rejected multiple pkey clauses; and CREATE
	 * INDEX doesn't have a way to say PRIMARY KEY, so it's no problem either.
	 */
	if ((is_alter_table || heapRel->rd_rel->relispartition) &&
		relationHasPrimaryKey(heapRel))
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_TABLE_DEFINITION),
				 errmsg("multiple primary keys for table \"%s\" are not allowed",
						RelationGetRelationName(heapRel))));
	}

	/*
	 * Check that all of the attributes in a primary key are marked as not
	 * null, otherwise attempt to ALTER TABLE .. SET NOT NULL
	 */
	cmds = NIL;
	for (i = 0; i < indexInfo->ii_NumIndexKeyAttrs; i++)
	{
		AttrNumber	attnum = indexInfo->ii_IndexAttrNumbers[i];
		HeapTuple	atttuple;
		Form_pg_attribute attform;

		if (attnum == 0)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("primary keys cannot be expressions")));

		/* System attributes are never null, so no need to check */
		if (attnum < 0)
			continue;

		atttuple = SearchSysCache2(ATTNUM,
								   ObjectIdGetDatum(RelationGetRelid(heapRel)),
								   Int16GetDatum(attnum));
		if (!HeapTupleIsValid(atttuple))
			elog(ERROR, "cache lookup failed for attribute %d of relation %u",
				 attnum, RelationGetRelid(heapRel));
		attform = (Form_pg_attribute) GETSTRUCT(atttuple);

		if (!attform->attnotnull)
		{
			/* Add a subcommand to make this one NOT NULL */
			AlterTableCmd *cmd = makeNode(AlterTableCmd);

			cmd->subtype = AT_SetNotNull;
			cmd->name = pstrdup(NameStr(attform->attname));
			cmds = lappend(cmds, cmd);
		}

		ReleaseSysCache(atttuple);
	}

	/*
	 * XXX: possible future improvement: when being called from ALTER TABLE,
	 * it would be more efficient to merge this with the outer ALTER TABLE, so
	 * as to avoid two scans.  But that seems to complicate DefineIndex's API
	 * unduly.
	 */
	if (cmds)
	{
		EventTriggerAlterTableStart((Node *) stmt);
		AlterTableInternal(RelationGetRelid(heapRel), cmds, true);
		EventTriggerAlterTableEnd();
	}
}

/*
 *		ConstructTupleDescriptor
 *
 * Build an index tuple descriptor for a new index
 */
static TupleDesc
ConstructTupleDescriptor(Relation heapRelation,
						 IndexInfo *indexInfo,
						 List *indexColNames,
						 Oid accessMethodObjectId,
						 Oid *collationObjectId,
						 Oid *classObjectId)
{
	int			numatts = indexInfo->ii_NumIndexAttrs;
	int			numkeyatts = indexInfo->ii_NumIndexKeyAttrs;
	ListCell   *colnames_item = list_head(indexColNames);
	ListCell   *indexpr_item = list_head(indexInfo->ii_Expressions);
	IndexAmRoutine *amroutine;
	TupleDesc	heapTupDesc;
	TupleDesc	indexTupDesc;
	int			natts;			/* #atts in heap rel --- for error checks */
	int			i;

	/* We need access to the index AM's API struct */
	amroutine = GetIndexAmRoutineByAmId(accessMethodObjectId, false);

	/* ... and to the table's tuple descriptor */
	heapTupDesc = RelationGetDescr(heapRelation);
	natts = RelationGetForm(heapRelation)->relnatts;

	/*
	 * allocate the new tuple descriptor
	 */
	indexTupDesc = CreateTemplateTupleDesc(numatts, false);

	/*
	 * For simple index columns, we copy the pg_attribute row from the parent
	 * relation and modify it as necessary.  For expressions we have to cons
	 * up a pg_attribute row the hard way.
	 */
	for (i = 0; i < numatts; i++)
	{
		AttrNumber	atnum = indexInfo->ii_IndexAttrNumbers[i];
		Form_pg_attribute to = TupleDescAttr(indexTupDesc, i);
		HeapTuple	tuple;
		Form_pg_type typeTup;
		Form_pg_opclass opclassTup;
		Oid			keyType;

		if (atnum != 0)
		{
			/* Simple index column */
			Form_pg_attribute from;

			if (atnum < 0)
			{
				/*
				 * here we are indexing on a system attribute (-1...-n)
				 */
				from = SystemAttributeDefinition(atnum,
												 heapRelation->rd_rel->relhasoids);
			}
			else
			{
				/*
				 * here we are indexing on a normal attribute (1...n)
				 */
				if (atnum > natts)	/* safety check */
					elog(ERROR, "invalid column number %d", atnum);
				from = TupleDescAttr(heapTupDesc,
									 AttrNumberGetAttrOffset(atnum));
			}

			/*
			 * now that we've determined the "from", let's copy the tuple desc
			 * data...
			 */
			memcpy(to, from, ATTRIBUTE_FIXED_PART_SIZE);

			/*
			 * Set the attribute name as specified by caller.
			 */
			if (colnames_item == NULL)	/* shouldn't happen */
				elog(ERROR, "too few entries in colnames list");
			namestrcpy(&to->attname, (const char *) lfirst(colnames_item));
			colnames_item = lnext(colnames_item);

			/*
			 * Fix the stuff that should not be the same as the underlying
			 * attr
			 */
			to->attnum = i + 1;

			to->attstattarget = -1;
			to->attcacheoff = -1;
			to->attnotnull = false;
			to->atthasdef = false;
			to->atthasmissing = false;
			to->attidentity = '\0';
			to->attislocal = true;
			to->attinhcount = 0;
			to->attcollation = (i < numkeyatts) ?
				collationObjectId[i] : InvalidOid;
		}
		else
		{
			/* Expressional index */
			Node	   *indexkey;

			MemSet(to, 0, ATTRIBUTE_FIXED_PART_SIZE);

			/*
			 * Set the attribute name as specified by caller.
			 */
			if (colnames_item == NULL)	/* shouldn't happen */
				elog(ERROR, "too few entries in colnames list");
			namestrcpy(&to->attname, (const char *) lfirst(colnames_item));
			colnames_item = lnext(colnames_item);

			if (indexpr_item == NULL)	/* shouldn't happen */
				elog(ERROR, "too few entries in indexprs list");
			indexkey = (Node *) lfirst(indexpr_item);
			indexpr_item = lnext(indexpr_item);

			/*
			 * Lookup the expression type in pg_type for the type length etc.
			 */
			keyType = exprType(indexkey);
			tuple = SearchSysCache1(TYPEOID, ObjectIdGetDatum(keyType));
			if (!HeapTupleIsValid(tuple))
				elog(ERROR, "cache lookup failed for type %u", keyType);
			typeTup = (Form_pg_type) GETSTRUCT(tuple);

			/*
			 * Assign some of the attributes values. Leave the rest as 0.
			 */
			to->attnum = i + 1;
			to->atttypid = keyType;
			to->attlen = typeTup->typlen;
			to->attbyval = typeTup->typbyval;
			to->attstorage = typeTup->typstorage;
			to->attalign = typeTup->typalign;
			to->attstattarget = -1;
			to->attcacheoff = -1;
			to->atttypmod = exprTypmod(indexkey);
			to->attislocal = true;
			to->attcollation = (i < numkeyatts) ?
				collationObjectId[i] : InvalidOid;

			ReleaseSysCache(tuple);

			/*
			 * Make sure the expression yields a type that's safe to store in
			 * an index.  We need this defense because we have index opclasses
			 * for pseudo-types such as "record", and the actually stored type
			 * had better be safe; eg, a named composite type is okay, an
			 * anonymous record type is not.  The test is the same as for
			 * whether a table column is of a safe type (which is why we
			 * needn't check for the non-expression case).
			 */
			CheckAttributeType(NameStr(to->attname),
							   to->atttypid, to->attcollation,
							   NIL, false);
		}

		/*
		 * We do not yet have the correct relation OID for the index, so just
		 * set it invalid for now.  InitializeAttributeOids() will fix it
		 * later.
		 */
		to->attrelid = InvalidOid;

		/*
		 * Check the opclass and index AM to see if either provides a keytype
		 * (overriding the attribute type).  Opclass (if exists) takes
		 * precedence.
		 */
		keyType = amroutine->amkeytype;

		/*
		 * Code below is concerned to the opclasses which are not used with
		 * the included columns.
		 */
		if (i < indexInfo->ii_NumIndexKeyAttrs)
		{
			tuple = SearchSysCache1(CLAOID, ObjectIdGetDatum(classObjectId[i]));
			if (!HeapTupleIsValid(tuple))
				elog(ERROR, "cache lookup failed for opclass %u",
					 classObjectId[i]);
			opclassTup = (Form_pg_opclass) GETSTRUCT(tuple);
			if (OidIsValid(opclassTup->opckeytype))
				keyType = opclassTup->opckeytype;

			/*
			 * If keytype is specified as ANYELEMENT, and opcintype is
			 * ANYARRAY, then the attribute type must be an array (else it'd
			 * not have matched this opclass); use its element type.
			 */
			if (keyType == ANYELEMENTOID && opclassTup->opcintype == ANYARRAYOID)
			{
				keyType = get_base_element_type(to->atttypid);
				if (!OidIsValid(keyType))
					elog(ERROR, "could not get element type of array type %u",
						 to->atttypid);
			}

			ReleaseSysCache(tuple);
		}

		/*
		 * If a key type different from the heap value is specified, update
		 * the type-related fields in the index tupdesc.
		 */
		if (OidIsValid(keyType) && keyType != to->atttypid)
		{
			tuple = SearchSysCache1(TYPEOID, ObjectIdGetDatum(keyType));
			if (!HeapTupleIsValid(tuple))
				elog(ERROR, "cache lookup failed for type %u", keyType);
			typeTup = (Form_pg_type) GETSTRUCT(tuple);

			to->atttypid = keyType;
			to->atttypmod = -1;
			to->attlen = typeTup->typlen;
			to->attbyval = typeTup->typbyval;
			to->attalign = typeTup->typalign;
			to->attstorage = typeTup->typstorage;

			ReleaseSysCache(tuple);
		}
	}

	pfree(amroutine);

	return indexTupDesc;
}

/* ----------------------------------------------------------------
 *		InitializeAttributeOids
 * ----------------------------------------------------------------
 */
static void
InitializeAttributeOids(Relation indexRelation,
						int numatts,
						Oid indexoid)
{
	TupleDesc	tupleDescriptor;
	int			i;

	tupleDescriptor = RelationGetDescr(indexRelation);

	for (i = 0; i < numatts; i += 1)
		TupleDescAttr(tupleDescriptor, i)->attrelid = indexoid;
}

/* ----------------------------------------------------------------
 *		AppendAttributeTuples
 * ----------------------------------------------------------------
 */
static void
AppendAttributeTuples(Relation indexRelation, int numatts)
{
	Relation	pg_attribute;
	CatalogIndexState indstate;
	TupleDesc	indexTupDesc;
	int			i;

	/*
	 * open the attribute relation and its indexes
	 */
	pg_attribute = heap_open(AttributeRelationId, RowExclusiveLock);

	indstate = CatalogOpenIndexes(pg_attribute);

	/*
	 * insert data from new index's tupdesc into pg_attribute
	 */
	indexTupDesc = RelationGetDescr(indexRelation);

	for (i = 0; i < numatts; i++)
	{
		Form_pg_attribute attr = TupleDescAttr(indexTupDesc, i);

		/*
		 * There used to be very grotty code here to set these fields, but I
		 * think it's unnecessary.  They should be set already.
		 */
		Assert(attr->attnum == i + 1);
		Assert(attr->attcacheoff == -1);

		InsertPgAttributeTuple(pg_attribute, attr, indstate);
	}

	CatalogCloseIndexes(indstate);

	heap_close(pg_attribute, RowExclusiveLock);
}

/* ----------------------------------------------------------------
 *		UpdateIndexRelation
 *
 * Construct and insert a new entry in the pg_index catalog
 * ----------------------------------------------------------------
 */
static void
UpdateIndexRelation(Oid indexoid,
					Oid heapoid,
					Oid parentIndexOid,
					IndexInfo *indexInfo,
					Oid *collationOids,
					Oid *classOids,
					int16 *coloptions,
					bool primary,
					bool isexclusion,
					bool immediate,
					bool isvalid,
					bool isready)
{
	int2vector *indkey;
	oidvector  *indcollation;
	oidvector  *indclass;
	int2vector *indoption;
	Datum		exprsDatum;
	Datum		predDatum;
	Datum		values[Natts_pg_index];
	bool		nulls[Natts_pg_index];
	Relation	pg_index;
	HeapTuple	tuple;
	int			i;

	/*
	 * Copy the index key, opclass, and indoption info into arrays (should we
	 * make the caller pass them like this to start with?)
	 */
	indkey = buildint2vector(NULL, indexInfo->ii_NumIndexAttrs);
	for (i = 0; i < indexInfo->ii_NumIndexAttrs; i++)
		indkey->values[i] = indexInfo->ii_IndexAttrNumbers[i];
	indcollation = buildoidvector(collationOids, indexInfo->ii_NumIndexKeyAttrs);
	indclass = buildoidvector(classOids, indexInfo->ii_NumIndexKeyAttrs);
	indoption = buildint2vector(coloptions, indexInfo->ii_NumIndexKeyAttrs);

	/*
	 * Convert the index expressions (if any) to a text datum
	 */
	if (indexInfo->ii_Expressions != NIL)
	{
		char	   *exprsString;

		exprsString = nodeToString(indexInfo->ii_Expressions);
		exprsDatum = CStringGetTextDatum(exprsString);
		pfree(exprsString);
	}
	else
		exprsDatum = (Datum) 0;

	/*
	 * Convert the index predicate (if any) to a text datum.  Note we convert
	 * implicit-AND format to normal explicit-AND for storage.
	 */
	if (indexInfo->ii_Predicate != NIL)
	{
		char	   *predString;

		predString = nodeToString(make_ands_explicit(indexInfo->ii_Predicate));
		predDatum = CStringGetTextDatum(predString);
		pfree(predString);
	}
	else
		predDatum = (Datum) 0;

	/*
	 * open the system catalog index relation
	 */
	pg_index = heap_open(IndexRelationId, RowExclusiveLock);

	/*
	 * Build a pg_index tuple
	 */
	MemSet(nulls, false, sizeof(nulls));

	values[Anum_pg_index_indexrelid - 1] = ObjectIdGetDatum(indexoid);
	values[Anum_pg_index_indrelid - 1] = ObjectIdGetDatum(heapoid);
	values[Anum_pg_index_indnatts - 1] = Int16GetDatum(indexInfo->ii_NumIndexAttrs);
	values[Anum_pg_index_indnkeyatts - 1] = Int16GetDatum(indexInfo->ii_NumIndexKeyAttrs);
	values[Anum_pg_index_indisunique - 1] = BoolGetDatum(indexInfo->ii_Unique);
	values[Anum_pg_index_indisprimary - 1] = BoolGetDatum(primary);
	values[Anum_pg_index_indisexclusion - 1] = BoolGetDatum(isexclusion);
	values[Anum_pg_index_indimmediate - 1] = BoolGetDatum(immediate);
	values[Anum_pg_index_indisclustered - 1] = BoolGetDatum(false);
	values[Anum_pg_index_indisvalid - 1] = BoolGetDatum(isvalid);
	values[Anum_pg_index_indcheckxmin - 1] = BoolGetDatum(false);
	values[Anum_pg_index_indisready - 1] = BoolGetDatum(isready);
	values[Anum_pg_index_indislive - 1] = BoolGetDatum(true);
	values[Anum_pg_index_indisreplident - 1] = BoolGetDatum(false);
	values[Anum_pg_index_indkey - 1] = PointerGetDatum(indkey);
	values[Anum_pg_index_indcollation - 1] = PointerGetDatum(indcollation);
	values[Anum_pg_index_indclass - 1] = PointerGetDatum(indclass);
	values[Anum_pg_index_indoption - 1] = PointerGetDatum(indoption);
	values[Anum_pg_index_indexprs - 1] = exprsDatum;
	if (exprsDatum == (Datum) 0)
		nulls[Anum_pg_index_indexprs - 1] = true;
	values[Anum_pg_index_indpred - 1] = predDatum;
	if (predDatum == (Datum) 0)
		nulls[Anum_pg_index_indpred - 1] = true;

	tuple = heap_form_tuple(RelationGetDescr(pg_index), values, nulls);

	/*
	 * insert the tuple into the pg_index catalog
	 */
	CatalogTupleInsert(pg_index, tuple);

	/*
	 * close the relation and free the tuple
	 */
	heap_close(pg_index, RowExclusiveLock);
	heap_freetuple(tuple);
}


/*
 * index_create
 *
 * heapRelation: table to build index on (suitably locked by caller)
 * indexRelationName: what it say
 * indexRelationId: normally, pass InvalidOid to let this routine
 *		generate an OID for the index.  During bootstrap this may be
 *		nonzero to specify a preselected OID.
 * parentIndexRelid: if creating an index partition, the OID of the
 *		parent index; otherwise InvalidOid.
 * parentConstraintId: if creating a constraint on a partition, the OID
 *		of the constraint in the parent; otherwise InvalidOid.
 * relFileNode: normally, pass InvalidOid to get new storage.  May be
 *		nonzero to attach an existing valid build.
 * indexInfo: same info executor uses to insert into the index
 * indexColNames: column names to use for index (List of char *)
 * accessMethodObjectId: OID of index AM to use
 * tableSpaceId: OID of tablespace to use
 * collationObjectId: array of collation OIDs, one per index column
 * classObjectId: array of index opclass OIDs, one per index column
 * coloptions: array of per-index-column indoption settings
 * reloptions: AM-specific options
 * flags: bitmask that can include any combination of these bits:
 *		INDEX_CREATE_IS_PRIMARY
 *			the index is a primary key
 *		INDEX_CREATE_ADD_CONSTRAINT:
 *			invoke index_constraint_create also
 *		INDEX_CREATE_SKIP_BUILD:
 *			skip the index_build() step for the moment; caller must do it
 *			later (typically via reindex_index())
 *		INDEX_CREATE_CONCURRENT:
 *			do not lock the table against writers.  The index will be
 *			marked "invalid" and the caller must take additional steps
 *			to fix it up.
 *		INDEX_CREATE_IF_NOT_EXISTS:
 *			do not throw an error if a relation with the same name
 *			already exists.
 *		INDEX_CREATE_PARTITIONED:
 *			create a partitioned index (table must be partitioned)
 * constr_flags: flags passed to index_constraint_create
 *		(only if INDEX_CREATE_ADD_CONSTRAINT is set)
 * allow_system_table_mods: allow table to be a system catalog
 * is_internal: if true, post creation hook for new index
 * constraintId: if not NULL, receives OID of created constraint
 *
 * Returns the OID of the created index.
 */
Oid
index_create(Relation heapRelation,
			 const char *indexRelationName,
			 Oid indexRelationId,
			 Oid parentIndexRelid,
			 Oid parentConstraintId,
			 Oid relFileNode,
			 IndexInfo *indexInfo,
			 List *indexColNames,
			 Oid accessMethodObjectId,
			 Oid tableSpaceId,
			 Oid *collationObjectId,
			 Oid *classObjectId,
			 int16 *coloptions,
			 Datum reloptions,
			 bits16 flags,
			 bits16 constr_flags,
			 bool allow_system_table_mods,
			 bool is_internal,
			 Oid *constraintId)
{
	Oid			heapRelationId = RelationGetRelid(heapRelation);
	Relation	pg_class;
	Relation	indexRelation;
	TupleDesc	indexTupDesc;
	bool		shared_relation;
	bool		mapped_relation;
	bool		is_exclusion;
	Oid			namespaceId;
	int			i;
	char		relpersistence;
	bool		isprimary = (flags & INDEX_CREATE_IS_PRIMARY) != 0;
	bool		invalid = (flags & INDEX_CREATE_INVALID) != 0;
	bool		concurrent = (flags & INDEX_CREATE_CONCURRENT) != 0;
	bool		partitioned = (flags & INDEX_CREATE_PARTITIONED) != 0;
	char		relkind;

	/* constraint flags can only be set when a constraint is requested */
	Assert((constr_flags == 0) ||
		   ((flags & INDEX_CREATE_ADD_CONSTRAINT) != 0));
	/* partitioned indexes must never be "built" by themselves */
	Assert(!partitioned || (flags & INDEX_CREATE_SKIP_BUILD));

	relkind = partitioned ? RELKIND_PARTITIONED_INDEX : RELKIND_INDEX;
	is_exclusion = (indexInfo->ii_ExclusionOps != NULL);

	pg_class = heap_open(RelationRelationId, RowExclusiveLock);

	/*
	 * The index will be in the same namespace as its parent table, and is
	 * shared across databases if and only if the parent is.  Likewise, it
	 * will use the relfilenode map if and only if the parent does; and it
	 * inherits the parent's relpersistence.
	 */
	namespaceId = RelationGetNamespace(heapRelation);
	shared_relation = heapRelation->rd_rel->relisshared;
	mapped_relation = RelationIsMapped(heapRelation);
	relpersistence = heapRelation->rd_rel->relpersistence;

	/*
	 * check parameters
	 */
	if (indexInfo->ii_NumIndexAttrs < 1)
		elog(ERROR, "must index at least one column");

	if (!allow_system_table_mods &&
		IsSystemRelation(heapRelation) &&
		IsNormalProcessingMode())
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("user-defined indexes on system catalog tables are not supported")));

	/*
	 * concurrent index build on a system catalog is unsafe because we tend to
	 * release locks before committing in catalogs
	 */
	if (concurrent &&
		IsSystemRelation(heapRelation))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("concurrent index creation on system catalog tables is not supported")));

	/*
	 * This case is currently not supported, but there's no way to ask for it
	 * in the grammar anyway, so it can't happen.
	 */
	if (concurrent && is_exclusion)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg_internal("concurrent index creation for exclusion constraints is not supported")));

	/*
	 * We cannot allow indexing a shared relation after initdb (because
	 * there's no way to make the entry in other databases' pg_class).
	 */
	if (shared_relation && !IsBootstrapProcessingMode())
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("shared indexes cannot be created after initdb")));

	/*
	 * Shared relations must be in pg_global, too (last-ditch check)
	 */
	if (shared_relation && tableSpaceId != GLOBALTABLESPACE_OID)
		elog(ERROR, "shared relations must be placed in pg_global tablespace");

	/*
	 * Check for duplicate name (both as to the index, and as to the
	 * associated constraint if any).  Such cases would fail on the relevant
	 * catalogs' unique indexes anyway, but we prefer to give a friendlier
	 * error message.
	 */
	if (get_relname_relid(indexRelationName, namespaceId))
	{
		if ((flags & INDEX_CREATE_IF_NOT_EXISTS) != 0)
		{
			ereport(NOTICE,
					(errcode(ERRCODE_DUPLICATE_TABLE),
					 errmsg("relation \"%s\" already exists, skipping",
							indexRelationName)));
			heap_close(pg_class, RowExclusiveLock);
			return InvalidOid;
		}

		ereport(ERROR,
				(errcode(ERRCODE_DUPLICATE_TABLE),
				 errmsg("relation \"%s\" already exists",
						indexRelationName)));
	}

	if ((flags & INDEX_CREATE_ADD_CONSTRAINT) != 0 &&
		ConstraintNameIsUsed(CONSTRAINT_RELATION, heapRelationId,
							 indexRelationName))
	{
		/*
		 * INDEX_CREATE_IF_NOT_EXISTS does not apply here, since the
		 * conflicting constraint is not an index.
		 */
		ereport(ERROR,
				(errcode(ERRCODE_DUPLICATE_OBJECT),
				 errmsg("constraint \"%s\" for relation \"%s\" already exists",
						indexRelationName, RelationGetRelationName(heapRelation))));
	}

	/*
	 * construct tuple descriptor for index tuples
	 */
	indexTupDesc = ConstructTupleDescriptor(heapRelation,
											indexInfo,
											indexColNames,
											accessMethodObjectId,
											collationObjectId,
											classObjectId);

	/*
	 * Allocate an OID for the index, unless we were told what to use.
	 *
	 * The OID will be the relfilenode as well, so make sure it doesn't
	 * collide with either pg_class OIDs or existing physical files.
	 */
	if (!OidIsValid(indexRelationId))
	{
		/* Use binary-upgrade override for pg_class.oid/relfilenode? */
		if (IsBinaryUpgrade)
		{
			if (!OidIsValid(binary_upgrade_next_index_pg_class_oid))
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("pg_class index OID value not set when in binary upgrade mode")));

			indexRelationId = binary_upgrade_next_index_pg_class_oid;
			binary_upgrade_next_index_pg_class_oid = InvalidOid;
		}
		else
		{
			indexRelationId =
				GetNewRelFileNode(tableSpaceId, pg_class, relpersistence);
		}
	}

	/*
	 * create the index relation's relcache entry and, if necessary, the
	 * physical disk file. (If we fail further down, it's the smgr's
	 * responsibility to remove the disk file again, if any.)
	 */
	indexRelation = heap_create(indexRelationName,
								namespaceId,
								tableSpaceId,
								indexRelationId,
								relFileNode,
								indexTupDesc,
								relkind,
								relpersistence,
								shared_relation,
								mapped_relation,
								allow_system_table_mods);

	Assert(indexRelationId == RelationGetRelid(indexRelation));

	/*
	 * Obtain exclusive lock on it.  Although no other transactions can see it
	 * until we commit, this prevents deadlock-risk complaints from lock
	 * manager in cases such as CLUSTER.
	 */
	LockRelation(indexRelation, AccessExclusiveLock);

	/*
	 * Fill in fields of the index's pg_class entry that are not set correctly
	 * by heap_create.
	 *
	 * XXX should have a cleaner way to create cataloged indexes
	 */
	indexRelation->rd_rel->relowner = heapRelation->rd_rel->relowner;
	indexRelation->rd_rel->relam = accessMethodObjectId;
	indexRelation->rd_rel->relhasoids = false;
	indexRelation->rd_rel->relispartition = OidIsValid(parentIndexRelid);

	/*
	 * store index's pg_class entry
	 */
	InsertPgClassTuple(pg_class, indexRelation,
					   RelationGetRelid(indexRelation),
					   (Datum) 0,
					   reloptions);

	/* done with pg_class */
	heap_close(pg_class, RowExclusiveLock);

	/*
	 * now update the object id's of all the attribute tuple forms in the
	 * index relation's tuple descriptor
	 */
	InitializeAttributeOids(indexRelation,
							indexInfo->ii_NumIndexAttrs,
							indexRelationId);

	/*
	 * append ATTRIBUTE tuples for the index
	 */
	AppendAttributeTuples(indexRelation, indexInfo->ii_NumIndexAttrs);

	/* ----------------
	 *	  update pg_index
	 *	  (append INDEX tuple)
	 *
	 *	  Note that this stows away a representation of "predicate".
	 *	  (Or, could define a rule to maintain the predicate) --Nels, Feb '92
	 * ----------------
	 */
	UpdateIndexRelation(indexRelationId, heapRelationId, parentIndexRelid,
						indexInfo,
						collationObjectId, classObjectId, coloptions,
						isprimary, is_exclusion,
						(constr_flags & INDEX_CONSTR_CREATE_DEFERRABLE) == 0,
						!concurrent && !invalid,
						!concurrent);

	/*
	 * Register relcache invalidation on the indexes' heap relation, to
	 * maintain consistency of its index list
	 */
	CacheInvalidateRelcache(heapRelation);

	/* update pg_inherits, if needed */
	if (OidIsValid(parentIndexRelid))
		StoreSingleInheritance(indexRelationId, parentIndexRelid, 1);

	/*
	 * Register constraint and dependencies for the index.
	 *
	 * If the index is from a CONSTRAINT clause, construct a pg_constraint
	 * entry.  The index will be linked to the constraint, which in turn is
	 * linked to the table.  If it's not a CONSTRAINT, we need to make a
	 * dependency directly on the table.
	 *
	 * We don't need a dependency on the namespace, because there'll be an
	 * indirect dependency via our parent table.
	 *
	 * During bootstrap we can't register any dependencies, and we don't try
	 * to make a constraint either.
	 */
	if (!IsBootstrapProcessingMode())
	{
		ObjectAddress myself,
					referenced;

		myself.classId = RelationRelationId;
		myself.objectId = indexRelationId;
		myself.objectSubId = 0;

		if ((flags & INDEX_CREATE_ADD_CONSTRAINT) != 0)
		{
			char		constraintType;
			ObjectAddress localaddr;

			if (isprimary)
				constraintType = CONSTRAINT_PRIMARY;
			else if (indexInfo->ii_Unique)
				constraintType = CONSTRAINT_UNIQUE;
			else if (is_exclusion)
				constraintType = CONSTRAINT_EXCLUSION;
			else
			{
				elog(ERROR, "constraint must be PRIMARY, UNIQUE or EXCLUDE");
				constraintType = 0; /* keep compiler quiet */
			}

			localaddr = index_constraint_create(heapRelation,
												indexRelationId,
												parentConstraintId,
												indexInfo,
												indexRelationName,
												constraintType,
												constr_flags,
												allow_system_table_mods,
												is_internal);
			if (constraintId)
				*constraintId = localaddr.objectId;
		}
		else
		{
			bool		have_simple_col = false;
			DependencyType deptype;

			deptype = OidIsValid(parentIndexRelid) ? DEPENDENCY_INTERNAL_AUTO : DEPENDENCY_AUTO;

			/* Create auto dependencies on simply-referenced columns */
			for (i = 0; i < indexInfo->ii_NumIndexAttrs; i++)
			{
				if (indexInfo->ii_IndexAttrNumbers[i] != 0)
				{
					referenced.classId = RelationRelationId;
					referenced.objectId = heapRelationId;
					referenced.objectSubId = indexInfo->ii_IndexAttrNumbers[i];

					recordDependencyOn(&myself, &referenced, deptype);

					have_simple_col = true;
				}
			}

			/*
			 * If there are no simply-referenced columns, give the index an
			 * auto dependency on the whole table.  In most cases, this will
			 * be redundant, but it might not be if the index expressions and
			 * predicate contain no Vars or only whole-row Vars.
			 */
			if (!have_simple_col)
			{
				referenced.classId = RelationRelationId;
				referenced.objectId = heapRelationId;
				referenced.objectSubId = 0;

				recordDependencyOn(&myself, &referenced, deptype);
			}
		}

		/* Store dependency on parent index, if any */
		if (OidIsValid(parentIndexRelid))
		{
			referenced.classId = RelationRelationId;
			referenced.objectId = parentIndexRelid;
			referenced.objectSubId = 0;

			recordDependencyOn(&myself, &referenced, DEPENDENCY_INTERNAL_AUTO);
		}

		/* Store dependency on collations */
		/* The default collation is pinned, so don't bother recording it */
		for (i = 0; i < indexInfo->ii_NumIndexKeyAttrs; i++)
		{
			if (OidIsValid(collationObjectId[i]) &&
				collationObjectId[i] != DEFAULT_COLLATION_OID)
			{
				referenced.classId = CollationRelationId;
				referenced.objectId = collationObjectId[i];
				referenced.objectSubId = 0;

				recordDependencyOn(&myself, &referenced, DEPENDENCY_NORMAL);
			}
		}

		/* Store dependency on operator classes */
		for (i = 0; i < indexInfo->ii_NumIndexKeyAttrs; i++)
		{
			referenced.classId = OperatorClassRelationId;
			referenced.objectId = classObjectId[i];
			referenced.objectSubId = 0;

			recordDependencyOn(&myself, &referenced, DEPENDENCY_NORMAL);
		}

		/* Store dependencies on anything mentioned in index expressions */
		if (indexInfo->ii_Expressions)
		{
			recordDependencyOnSingleRelExpr(&myself,
											(Node *) indexInfo->ii_Expressions,
											heapRelationId,
											DEPENDENCY_NORMAL,
											DEPENDENCY_AUTO, false);
		}

		/* Store dependencies on anything mentioned in predicate */
		if (indexInfo->ii_Predicate)
		{
			recordDependencyOnSingleRelExpr(&myself,
											(Node *) indexInfo->ii_Predicate,
											heapRelationId,
											DEPENDENCY_NORMAL,
											DEPENDENCY_AUTO, false);
		}
	}
	else
	{
		/* Bootstrap mode - assert we weren't asked for constraint support */
		Assert((flags & INDEX_CREATE_ADD_CONSTRAINT) == 0);
	}

	/* Post creation hook for new index */
	InvokeObjectPostCreateHookArg(RelationRelationId,
								  indexRelationId, 0, is_internal);

	/*
	 * Advance the command counter so that we can see the newly-entered
	 * catalog tuples for the index.
	 */
	CommandCounterIncrement();

	/*
	 * In bootstrap mode, we have to fill in the index strategy structure with
	 * information from the catalogs.  If we aren't bootstrapping, then the
	 * relcache entry has already been rebuilt thanks to sinval update during
	 * CommandCounterIncrement.
	 */
	if (IsBootstrapProcessingMode())
		RelationInitIndexAccessInfo(indexRelation);
	else
		Assert(indexRelation->rd_indexcxt != NULL);

	indexRelation->rd_index->indnkeyatts = indexInfo->ii_NumIndexKeyAttrs;

	/*
	 * If this is bootstrap (initdb) time, then we don't actually fill in the
	 * index yet.  We'll be creating more indexes and classes later, so we
	 * delay filling them in until just before we're done with bootstrapping.
	 * Similarly, if the caller specified to skip the build then filling the
	 * index is delayed till later (ALTER TABLE can save work in some cases
	 * with this).  Otherwise, we call the AM routine that constructs the
	 * index.
	 */
	if (IsBootstrapProcessingMode())
	{
		index_register(heapRelationId, indexRelationId, indexInfo);
	}
	else if ((flags & INDEX_CREATE_SKIP_BUILD) != 0)
	{
		/*
		 * Caller is responsible for filling the index later on.  However,
		 * we'd better make sure that the heap relation is correctly marked as
		 * having an index.
		 */
		index_update_stats(heapRelation,
						   true,
						   -1.0);
		/* Make the above update visible */
		CommandCounterIncrement();
	}
	else
	{
		index_build(heapRelation, indexRelation, indexInfo, isprimary, false,
					true);
	}

	/*
	 * Close the index; but we keep the lock that we acquired above until end
	 * of transaction.  Closing the heap is caller's responsibility.
	 */
	index_close(indexRelation, NoLock);

	return indexRelationId;
}

/*
 * index_constraint_create
 *
 * Set up a constraint associated with an index.  Return the new constraint's
 * address.
 *
 * heapRelation: table owning the index (must be suitably locked by caller)
 * indexRelationId: OID of the index
 * parentConstraintId: if constraint is on a partition, the OID of the
 *		constraint in the parent.
 * indexInfo: same info executor uses to insert into the index
 * constraintName: what it say (generally, should match name of index)
 * constraintType: one of CONSTRAINT_PRIMARY, CONSTRAINT_UNIQUE, or
 *		CONSTRAINT_EXCLUSION
 * flags: bitmask that can include any combination of these bits:
 *		INDEX_CONSTR_CREATE_MARK_AS_PRIMARY: index is a PRIMARY KEY
 *		INDEX_CONSTR_CREATE_DEFERRABLE: constraint is DEFERRABLE
 *		INDEX_CONSTR_CREATE_INIT_DEFERRED: constraint is INITIALLY DEFERRED
 *		INDEX_CONSTR_CREATE_UPDATE_INDEX: update the pg_index row
 *		INDEX_CONSTR_CREATE_REMOVE_OLD_DEPS: remove existing dependencies
 *			of index on table's columns
 * allow_system_table_mods: allow table to be a system catalog
 * is_internal: index is constructed due to internal process
 */
ObjectAddress
index_constraint_create(Relation heapRelation,
						Oid indexRelationId,
						Oid parentConstraintId,
						IndexInfo *indexInfo,
						const char *constraintName,
						char constraintType,
						bits16 constr_flags,
						bool allow_system_table_mods,
						bool is_internal)
{
	Oid			namespaceId = RelationGetNamespace(heapRelation);
	ObjectAddress myself,
				referenced;
	Oid			conOid;
	bool		deferrable;
	bool		initdeferred;
	bool		mark_as_primary;
	bool		islocal;
	bool		noinherit;
	int			inhcount;

	deferrable = (constr_flags & INDEX_CONSTR_CREATE_DEFERRABLE) != 0;
	initdeferred = (constr_flags & INDEX_CONSTR_CREATE_INIT_DEFERRED) != 0;
	mark_as_primary = (constr_flags & INDEX_CONSTR_CREATE_MARK_AS_PRIMARY) != 0;

	/* constraint creation support doesn't work while bootstrapping */
	Assert(!IsBootstrapProcessingMode());

	/* enforce system-table restriction */
	if (!allow_system_table_mods &&
		IsSystemRelation(heapRelation) &&
		IsNormalProcessingMode())
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("user-defined indexes on system catalog tables are not supported")));

	/* primary/unique constraints shouldn't have any expressions */
	if (indexInfo->ii_Expressions &&
		constraintType != CONSTRAINT_EXCLUSION)
		elog(ERROR, "constraints cannot have index expressions");

	/*
	 * If we're manufacturing a constraint for a pre-existing index, we need
	 * to get rid of the existing auto dependencies for the index (the ones
	 * that index_create() would have made instead of calling this function).
	 *
	 * Note: this code would not necessarily do the right thing if the index
	 * has any expressions or predicate, but we'd never be turning such an
	 * index into a UNIQUE or PRIMARY KEY constraint.
	 */
	if (constr_flags & INDEX_CONSTR_CREATE_REMOVE_OLD_DEPS)
		deleteDependencyRecordsForClass(RelationRelationId, indexRelationId,
										RelationRelationId, DEPENDENCY_AUTO);

	if (OidIsValid(parentConstraintId))
	{
		islocal = false;
		inhcount = 1;
		noinherit = false;
	}
	else
	{
		islocal = true;
		inhcount = 0;
		noinherit = true;
	}

	/*
	 * Construct a pg_constraint entry.
	 */
	conOid = CreateConstraintEntry(constraintName,
								   namespaceId,
								   constraintType,
								   deferrable,
								   initdeferred,
								   true,
								   parentConstraintId,
								   RelationGetRelid(heapRelation),
								   indexInfo->ii_IndexAttrNumbers,
								   indexInfo->ii_NumIndexKeyAttrs,
								   indexInfo->ii_NumIndexAttrs,
								   InvalidOid,	/* no domain */
								   indexRelationId, /* index OID */
								   InvalidOid,	/* no foreign key */
								   NULL,
								   NULL,
								   NULL,
								   NULL,
								   0,
								   ' ',
								   ' ',
								   ' ',
								   indexInfo->ii_ExclusionOps,
								   NULL,	/* no check constraint */
								   NULL,
								   NULL,
								   islocal,
								   inhcount,
								   noinherit,
								   is_internal);

	/*
	 * Register the index as internally dependent on the constraint.
	 *
	 * Note that the constraint has a dependency on the table, so we don't
	 * need (or want) any direct dependency from the index to the table.
	 */
	myself.classId = RelationRelationId;
	myself.objectId = indexRelationId;
	myself.objectSubId = 0;

	referenced.classId = ConstraintRelationId;
	referenced.objectId = conOid;
	referenced.objectSubId = 0;

	recordDependencyOn(&myself, &referenced, DEPENDENCY_INTERNAL);

	/*
	 * Also, if this is a constraint on a partition, mark it as depending on
	 * the constraint in the parent.
	 */
	if (OidIsValid(parentConstraintId))
	{
		ObjectAddress parentConstr;

		ObjectAddressSet(parentConstr, ConstraintRelationId, parentConstraintId);
		recordDependencyOn(&referenced, &parentConstr, DEPENDENCY_INTERNAL_AUTO);
	}

	/*
	 * If the constraint is deferrable, create the deferred uniqueness
	 * checking trigger.  (The trigger will be given an internal dependency on
	 * the constraint by CreateTrigger.)
	 */
	if (deferrable)
	{
		CreateTrigStmt *trigger;

		trigger = makeNode(CreateTrigStmt);
		trigger->trigname = (constraintType == CONSTRAINT_PRIMARY) ?
			"PK_ConstraintTrigger" :
			"Unique_ConstraintTrigger";
		trigger->relation = NULL;
		trigger->funcname = SystemFuncName("unique_key_recheck");
		trigger->args = NIL;
		trigger->row = true;
		trigger->timing = TRIGGER_TYPE_AFTER;
		trigger->events = TRIGGER_TYPE_INSERT | TRIGGER_TYPE_UPDATE;
		trigger->columns = NIL;
		trigger->whenClause = NULL;
		trigger->isconstraint = true;
		trigger->deferrable = true;
		trigger->initdeferred = initdeferred;
		trigger->constrrel = NULL;

		(void) CreateTrigger(trigger, NULL, RelationGetRelid(heapRelation),
							 InvalidOid, conOid, indexRelationId, InvalidOid,
							 InvalidOid, NULL, true, false);
	}

	/*
	 * If needed, mark the index as primary and/or deferred in pg_index.
	 *
	 * Note: When making an existing index into a constraint, caller must have
	 * a table lock that prevents concurrent table updates; otherwise, there
	 * is a risk that concurrent readers of the table will miss seeing this
	 * index at all.
	 */
	if ((constr_flags & INDEX_CONSTR_CREATE_UPDATE_INDEX) &&
		(mark_as_primary || deferrable))
	{
		Relation	pg_index;
		HeapTuple	indexTuple;
		Form_pg_index indexForm;
		bool		dirty = false;

		pg_index = heap_open(IndexRelationId, RowExclusiveLock);

		indexTuple = SearchSysCacheCopy1(INDEXRELID,
										 ObjectIdGetDatum(indexRelationId));
		if (!HeapTupleIsValid(indexTuple))
			elog(ERROR, "cache lookup failed for index %u", indexRelationId);
		indexForm = (Form_pg_index) GETSTRUCT(indexTuple);

		if (mark_as_primary && !indexForm->indisprimary)
		{
			indexForm->indisprimary = true;
			dirty = true;
		}

		if (deferrable && indexForm->indimmediate)
		{
			indexForm->indimmediate = false;
			dirty = true;
		}

		if (dirty)
		{
			CatalogTupleUpdate(pg_index, &indexTuple->t_self, indexTuple);

			InvokeObjectPostAlterHookArg(IndexRelationId, indexRelationId, 0,
										 InvalidOid, is_internal);
		}

		heap_freetuple(indexTuple);
		heap_close(pg_index, RowExclusiveLock);
	}

	return referenced;
}

/*
 *		index_drop
 *
 * NOTE: this routine should now only be called through performDeletion(),
 * else associated dependencies won't be cleaned up.
 */
void
index_drop(Oid indexId, bool concurrent)
{
	Oid			heapId;
	Relation	userHeapRelation;
	Relation	userIndexRelation;
	Relation	indexRelation;
	HeapTuple	tuple;
	bool		hasexprs;
	LockRelId	heaprelid,
				indexrelid;
	LOCKTAG		heaplocktag;
	LOCKMODE	lockmode;

	/*
	 * A temporary relation uses a non-concurrent DROP.  Other backends can't
	 * access a temporary relation, so there's no harm in grabbing a stronger
	 * lock (see comments in RemoveRelations), and a non-concurrent DROP is
	 * more efficient.
	 */
	Assert(get_rel_persistence(indexId) != RELPERSISTENCE_TEMP ||
		   !concurrent);

	/*
	 * To drop an index safely, we must grab exclusive lock on its parent
	 * table.  Exclusive lock on the index alone is insufficient because
	 * another backend might be about to execute a query on the parent table.
	 * If it relies on a previously cached list of index OIDs, then it could
	 * attempt to access the just-dropped index.  We must therefore take a
	 * table lock strong enough to prevent all queries on the table from
	 * proceeding until we commit and send out a shared-cache-inval notice
	 * that will make them update their index lists.
	 *
	 * In the concurrent case we avoid this requirement by disabling index use
	 * in multiple steps and waiting out any transactions that might be using
	 * the index, so we don't need exclusive lock on the parent table. Instead
	 * we take ShareUpdateExclusiveLock, to ensure that two sessions aren't
	 * doing CREATE/DROP INDEX CONCURRENTLY on the same index.  (We will get
	 * AccessExclusiveLock on the index below, once we're sure nobody else is
	 * using it.)
	 */
	heapId = IndexGetRelation(indexId, false);
	lockmode = concurrent ? ShareUpdateExclusiveLock : AccessExclusiveLock;
	userHeapRelation = heap_open(heapId, lockmode);
	userIndexRelation = index_open(indexId, lockmode);

	/*
	 * We might still have open queries using it in our own session, which the
	 * above locking won't prevent, so test explicitly.
	 */
	CheckTableNotInUse(userIndexRelation, "DROP INDEX");

	/*
	 * Drop Index Concurrently is more or less the reverse process of Create
	 * Index Concurrently.
	 *
	 * First we unset indisvalid so queries starting afterwards don't use the
	 * index to answer queries anymore.  We have to keep indisready = true so
	 * transactions that are still scanning the index can continue to see
	 * valid index contents.  For instance, if they are using READ COMMITTED
	 * mode, and another transaction makes changes and commits, they need to
	 * see those new tuples in the index.
	 *
	 * After all transactions that could possibly have used the index for
	 * queries end, we can unset indisready and indislive, then wait till
	 * nobody could be touching it anymore.  (Note: we need indislive because
	 * this state must be distinct from the initial state during CREATE INDEX
	 * CONCURRENTLY, which has indislive true while indisready and indisvalid
	 * are false.  That's because in that state, transactions must examine the
	 * index for HOT-safety decisions, while in this state we don't want them
	 * to open it at all.)
	 *
	 * Since all predicate locks on the index are about to be made invalid, we
	 * must promote them to predicate locks on the heap.  In the
	 * non-concurrent case we can just do that now.  In the concurrent case
	 * it's a bit trickier.  The predicate locks must be moved when there are
	 * no index scans in progress on the index and no more can subsequently
	 * start, so that no new predicate locks can be made on the index.  Also,
	 * they must be moved before heap inserts stop maintaining the index, else
	 * the conflict with the predicate lock on the index gap could be missed
	 * before the lock on the heap relation is in place to detect a conflict
	 * based on the heap tuple insert.
	 */
	if (concurrent)
	{
		/*
		 * We must commit our transaction in order to make the first pg_index
		 * state update visible to other sessions.  If the DROP machinery has
		 * already performed any other actions (removal of other objects,
		 * pg_depend entries, etc), the commit would make those actions
		 * permanent, which would leave us with inconsistent catalog state if
		 * we fail partway through the following sequence.  Since DROP INDEX
		 * CONCURRENTLY is restricted to dropping just one index that has no
		 * dependencies, we should get here before anything's been done ---
		 * but let's check that to be sure.  We can verify that the current
		 * transaction has not executed any transactional updates by checking
		 * that no XID has been assigned.
		 */
		if (GetTopTransactionIdIfAny() != InvalidTransactionId)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("DROP INDEX CONCURRENTLY must be first action in transaction")));

		/*
		 * Mark index invalid by updating its pg_index entry
		 */
		index_set_state_flags(indexId, INDEX_DROP_CLEAR_VALID);

		/*
		 * Invalidate the relcache for the table, so that after this commit
		 * all sessions will refresh any cached plans that might reference the
		 * index.
		 */
		CacheInvalidateRelcache(userHeapRelation);

		/* save lockrelid and locktag for below, then close but keep locks */
		heaprelid = userHeapRelation->rd_lockInfo.lockRelId;
		SET_LOCKTAG_RELATION(heaplocktag, heaprelid.dbId, heaprelid.relId);
		indexrelid = userIndexRelation->rd_lockInfo.lockRelId;

		heap_close(userHeapRelation, NoLock);
		index_close(userIndexRelation, NoLock);

		/*
		 * We must commit our current transaction so that the indisvalid
		 * update becomes visible to other transactions; then start another.
		 * Note that any previously-built data structures are lost in the
		 * commit.  The only data we keep past here are the relation IDs.
		 *
		 * Before committing, get a session-level lock on the table, to ensure
		 * that neither it nor the index can be dropped before we finish. This
		 * cannot block, even if someone else is waiting for access, because
		 * we already have the same lock within our transaction.
		 */
		LockRelationIdForSession(&heaprelid, ShareUpdateExclusiveLock);
		LockRelationIdForSession(&indexrelid, ShareUpdateExclusiveLock);

		PopActiveSnapshot();
		CommitTransactionCommand();
		StartTransactionCommand();

		/*
		 * Now we must wait until no running transaction could be using the
		 * index for a query.  Use AccessExclusiveLock here to check for
		 * running transactions that hold locks of any kind on the table. Note
		 * we do not need to worry about xacts that open the table for reading
		 * after this point; they will see the index as invalid when they open
		 * the relation.
		 *
		 * Note: the reason we use actual lock acquisition here, rather than
		 * just checking the ProcArray and sleeping, is that deadlock is
		 * possible if one of the transactions in question is blocked trying
		 * to acquire an exclusive lock on our table.  The lock code will
		 * detect deadlock and error out properly.
		 */
		WaitForLockers(heaplocktag, AccessExclusiveLock);

		/*
		 * No more predicate locks will be acquired on this index, and we're
		 * about to stop doing inserts into the index which could show
		 * conflicts with existing predicate locks, so now is the time to move
		 * them to the heap relation.
		 */
		userHeapRelation = heap_open(heapId, ShareUpdateExclusiveLock);
		userIndexRelation = index_open(indexId, ShareUpdateExclusiveLock);
		TransferPredicateLocksToHeapRelation(userIndexRelation);

		/*
		 * Now we are sure that nobody uses the index for queries; they just
		 * might have it open for updating it.  So now we can unset indisready
		 * and indislive, then wait till nobody could be using it at all
		 * anymore.
		 */
		index_set_state_flags(indexId, INDEX_DROP_SET_DEAD);

		/*
		 * Invalidate the relcache for the table, so that after this commit
		 * all sessions will refresh the table's index list.  Forgetting just
		 * the index's relcache entry is not enough.
		 */
		CacheInvalidateRelcache(userHeapRelation);

		/*
		 * Close the relations again, though still holding session lock.
		 */
		heap_close(userHeapRelation, NoLock);
		index_close(userIndexRelation, NoLock);

		/*
		 * Again, commit the transaction to make the pg_index update visible
		 * to other sessions.
		 */
		CommitTransactionCommand();
		StartTransactionCommand();

		/*
		 * Wait till every transaction that saw the old index state has
		 * finished.
		 */
		WaitForLockers(heaplocktag, AccessExclusiveLock);

		/*
		 * Re-open relations to allow us to complete our actions.
		 *
		 * At this point, nothing should be accessing the index, but lets
		 * leave nothing to chance and grab AccessExclusiveLock on the index
		 * before the physical deletion.
		 */
		userHeapRelation = heap_open(heapId, ShareUpdateExclusiveLock);
		userIndexRelation = index_open(indexId, AccessExclusiveLock);
	}
	else
	{
		/* Not concurrent, so just transfer predicate locks and we're good */
		TransferPredicateLocksToHeapRelation(userIndexRelation);
	}

	/*
	 * Schedule physical removal of the files (if any)
	 */
	if (userIndexRelation->rd_rel->relkind != RELKIND_PARTITIONED_INDEX)
		RelationDropStorage(userIndexRelation);

	/*
	 * Close and flush the index's relcache entry, to ensure relcache doesn't
	 * try to rebuild it while we're deleting catalog entries. We keep the
	 * lock though.
	 */
	index_close(userIndexRelation, NoLock);

	RelationForgetRelation(indexId);

	/*
	 * fix INDEX relation, and check for expressional index
	 */
	indexRelation = heap_open(IndexRelationId, RowExclusiveLock);

	tuple = SearchSysCache1(INDEXRELID, ObjectIdGetDatum(indexId));
	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "cache lookup failed for index %u", indexId);

	hasexprs = !heap_attisnull(tuple, Anum_pg_index_indexprs,
							   RelationGetDescr(indexRelation));

	CatalogTupleDelete(indexRelation, &tuple->t_self);

	ReleaseSysCache(tuple);
	heap_close(indexRelation, RowExclusiveLock);

	/*
	 * if it has any expression columns, we might have stored statistics about
	 * them.
	 */
	if (hasexprs)
		RemoveStatistics(indexId, 0);

	/*
	 * fix ATTRIBUTE relation
	 */
	DeleteAttributeTuples(indexId);

	/*
	 * fix RELATION relation
	 */
	DeleteRelationTuple(indexId);

	/*
	 * fix INHERITS relation
	 */
	DeleteInheritsTuple(indexId, InvalidOid);

	/*
	 * We are presently too lazy to attempt to compute the new correct value
	 * of relhasindex (the next VACUUM will fix it if necessary). So there is
	 * no need to update the pg_class tuple for the owning relation. But we
	 * must send out a shared-cache-inval notice on the owning relation to
	 * ensure other backends update their relcache lists of indexes.  (In the
	 * concurrent case, this is redundant but harmless.)
	 */
	CacheInvalidateRelcache(userHeapRelation);

	/*
	 * Close owning rel, but keep lock
	 */
	heap_close(userHeapRelation, NoLock);

	/*
	 * Release the session locks before we go.
	 */
	if (concurrent)
	{
		UnlockRelationIdForSession(&heaprelid, ShareUpdateExclusiveLock);
		UnlockRelationIdForSession(&indexrelid, ShareUpdateExclusiveLock);
	}
}

/* ----------------------------------------------------------------
 *						index_build support
 * ----------------------------------------------------------------
 */

/* ----------------
 *		BuildIndexInfo
 *			Construct an IndexInfo record for an open index
 *
 * IndexInfo stores the information about the index that's needed by
 * FormIndexDatum, which is used for both index_build() and later insertion
 * of individual index tuples.  Normally we build an IndexInfo for an index
 * just once per command, and then use it for (potentially) many tuples.
 * ----------------
 */
IndexInfo *
BuildIndexInfo(Relation index)
{
	IndexInfo  *ii = makeNode(IndexInfo);
	Form_pg_index indexStruct = index->rd_index;
	int			i;
	int			numAtts;

	/* check the number of keys, and copy attr numbers into the IndexInfo */
	numAtts = indexStruct->indnatts;
	if (numAtts < 1 || numAtts > INDEX_MAX_KEYS)
		elog(ERROR, "invalid indnatts %d for index %u",
			 numAtts, RelationGetRelid(index));
	ii->ii_NumIndexAttrs = numAtts;
	ii->ii_NumIndexKeyAttrs = indexStruct->indnkeyatts;
	Assert(ii->ii_NumIndexKeyAttrs != 0);
	Assert(ii->ii_NumIndexKeyAttrs <= ii->ii_NumIndexAttrs);

	for (i = 0; i < numAtts; i++)
		ii->ii_IndexAttrNumbers[i] = indexStruct->indkey.values[i];

	/* fetch any expressions needed for expressional indexes */
	ii->ii_Expressions = RelationGetIndexExpressions(index);
	ii->ii_ExpressionsState = NIL;

	/* fetch index predicate if any */
	ii->ii_Predicate = RelationGetIndexPredicate(index);
	ii->ii_PredicateState = NULL;

	/* fetch exclusion constraint info if any */
	if (indexStruct->indisexclusion)
	{
		RelationGetExclusionInfo(index,
								 &ii->ii_ExclusionOps,
								 &ii->ii_ExclusionProcs,
								 &ii->ii_ExclusionStrats);
	}
	else
	{
		ii->ii_ExclusionOps = NULL;
		ii->ii_ExclusionProcs = NULL;
		ii->ii_ExclusionStrats = NULL;
	}

	/* other info */
	ii->ii_Unique = indexStruct->indisunique;
	ii->ii_ReadyForInserts = IndexIsReady(indexStruct);
	/* assume not doing speculative insertion for now */
	ii->ii_UniqueOps = NULL;
	ii->ii_UniqueProcs = NULL;
	ii->ii_UniqueStrats = NULL;

	/* initialize index-build state to default */
	ii->ii_Concurrent = false;
	ii->ii_BrokenHotChain = false;
	ii->ii_ParallelWorkers = 0;

	/* set up for possible use by index AM */
	ii->ii_Am = index->rd_rel->relam;
	ii->ii_AmCache = NULL;
	ii->ii_Context = CurrentMemoryContext;

	return ii;
}

/* ----------------
 *		BuildDummyIndexInfo
 *			Construct a dummy IndexInfo record for an open index
 *
 * This differs from the real BuildIndexInfo in that it will never run any
 * user-defined code that might exist in index expressions or predicates.
 * Instead of the real index expressions, we return null constants that have
 * the right types/typmods/collations.  Predicates and exclusion clauses are
 * just ignored.  This is sufficient for the purpose of truncating an index,
 * since we will not need to actually evaluate the expressions or predicates;
 * the only thing that's likely to be done with the data is construction of
 * a tupdesc describing the index's rowtype.
 * ----------------
 */
IndexInfo *
BuildDummyIndexInfo(Relation index)
{
	IndexInfo  *ii = makeNode(IndexInfo);
	Form_pg_index indexStruct = index->rd_index;
	int			i;
	int			numAtts;

	/* check the number of keys, and copy attr numbers into the IndexInfo */
	numAtts = indexStruct->indnatts;
	if (numAtts < 1 || numAtts > INDEX_MAX_KEYS)
		elog(ERROR, "invalid indnatts %d for index %u",
			 numAtts, RelationGetRelid(index));
	ii->ii_NumIndexAttrs = numAtts;
	ii->ii_NumIndexKeyAttrs = indexStruct->indnkeyatts;
	Assert(ii->ii_NumIndexKeyAttrs != 0);
	Assert(ii->ii_NumIndexKeyAttrs <= ii->ii_NumIndexAttrs);

	for (i = 0; i < numAtts; i++)
		ii->ii_IndexAttrNumbers[i] = indexStruct->indkey.values[i];

	/* fetch dummy expressions for expressional indexes */
	ii->ii_Expressions = RelationGetDummyIndexExpressions(index);
	ii->ii_ExpressionsState = NIL;

	/* pretend there is no predicate */
	ii->ii_Predicate = NIL;
	ii->ii_PredicateState = NULL;

	/* We ignore the exclusion constraint if any */
	ii->ii_ExclusionOps = NULL;
	ii->ii_ExclusionProcs = NULL;
	ii->ii_ExclusionStrats = NULL;

	/* other info */
	ii->ii_Unique = indexStruct->indisunique;
	ii->ii_ReadyForInserts = IndexIsReady(indexStruct);
	/* assume not doing speculative insertion for now */
	ii->ii_UniqueOps = NULL;
	ii->ii_UniqueProcs = NULL;
	ii->ii_UniqueStrats = NULL;

	/* initialize index-build state to default */
	ii->ii_Concurrent = false;
	ii->ii_BrokenHotChain = false;
	ii->ii_ParallelWorkers = 0;

	/* set up for possible use by index AM */
	ii->ii_Am = index->rd_rel->relam;
	ii->ii_AmCache = NULL;
	ii->ii_Context = CurrentMemoryContext;

	return ii;
}

/*
 * CompareIndexInfo
 *		Return whether the properties of two indexes (in different tables)
 *		indicate that they have the "same" definitions.
 *
 * Note: passing collations and opfamilies separately is a kludge.  Adding
 * them to IndexInfo may result in better coding here and elsewhere.
 *
 * Use convert_tuples_by_name_map(index2, index1) to build the attmap.
 */
bool
CompareIndexInfo(IndexInfo *info1, IndexInfo *info2,
				 Oid *collations1, Oid *collations2,
				 Oid *opfamilies1, Oid *opfamilies2,
				 AttrNumber *attmap, int maplen)
{
	int			i;

	if (info1->ii_Unique != info2->ii_Unique)
		return false;

	/* indexes are only equivalent if they have the same access method */
	if (info1->ii_Am != info2->ii_Am)
		return false;

	/* and same number of attributes */
	if (info1->ii_NumIndexAttrs != info2->ii_NumIndexAttrs)
		return false;

	/* and same number of key attributes */
	if (info1->ii_NumIndexKeyAttrs != info2->ii_NumIndexKeyAttrs)
		return false;

	/*
	 * and columns match through the attribute map (actual attribute numbers
	 * might differ!)  Note that this implies that index columns that are
	 * expressions appear in the same positions.  We will next compare the
	 * expressions themselves.
	 */
	for (i = 0; i < info1->ii_NumIndexAttrs; i++)
	{
		if (maplen < info2->ii_IndexAttrNumbers[i])
			elog(ERROR, "incorrect attribute map");

		/* ignore expressions at this stage */
		if ((info1->ii_IndexAttrNumbers[i] != InvalidAttrNumber) &&
			(attmap[info2->ii_IndexAttrNumbers[i] - 1] !=
			 info1->ii_IndexAttrNumbers[i]))
			return false;

		/* collation and opfamily is not valid for including columns */
		if (i >= info1->ii_NumIndexKeyAttrs)
			continue;

		if (collations1[i] != collations2[i])
			return false;
		if (opfamilies1[i] != opfamilies2[i])
			return false;
	}

	/*
	 * For expression indexes: either both are expression indexes, or neither
	 * is; if they are, make sure the expressions match.
	 */
	if ((info1->ii_Expressions != NIL) != (info2->ii_Expressions != NIL))
		return false;
	if (info1->ii_Expressions != NIL)
	{
		bool		found_whole_row;
		Node	   *mapped;

		mapped = map_variable_attnos((Node *) info2->ii_Expressions,
									 1, 0, attmap, maplen,
									 InvalidOid, &found_whole_row);
		if (found_whole_row)
		{
			/*
			 * we could throw an error here, but seems out of scope for this
			 * routine.
			 */
			return false;
		}

		if (!equal(info1->ii_Expressions, mapped))
			return false;
	}

	/* Partial index predicates must be identical, if they exist */
	if ((info1->ii_Predicate == NULL) != (info2->ii_Predicate == NULL))
		return false;
	if (info1->ii_Predicate != NULL)
	{
		bool		found_whole_row;
		Node	   *mapped;

		mapped = map_variable_attnos((Node *) info2->ii_Predicate,
									 1, 0, attmap, maplen,
									 InvalidOid, &found_whole_row);
		if (found_whole_row)
		{
			/*
			 * we could throw an error here, but seems out of scope for this
			 * routine.
			 */
			return false;
		}
		if (!equal(info1->ii_Predicate, mapped))
			return false;
	}

	/* No support currently for comparing exclusion indexes. */
	if (info1->ii_ExclusionOps != NULL || info2->ii_ExclusionOps != NULL)
		return false;

	return true;
}

/* ----------------
 *		BuildSpeculativeIndexInfo
 *			Add extra state to IndexInfo record
 *
 * For unique indexes, we usually don't want to add info to the IndexInfo for
 * checking uniqueness, since the B-Tree AM handles that directly.  However,
 * in the case of speculative insertion, additional support is required.
 *
 * Do this processing here rather than in BuildIndexInfo() to not incur the
 * overhead in the common non-speculative cases.
 * ----------------
 */
void
BuildSpeculativeIndexInfo(Relation index, IndexInfo *ii)
{
	int			indnkeyatts;
	int			i;

	indnkeyatts = IndexRelationGetNumberOfKeyAttributes(index);

	/*
	 * fetch info for checking unique indexes
	 */
	Assert(ii->ii_Unique);

	if (index->rd_rel->relam != BTREE_AM_OID)
		elog(ERROR, "unexpected non-btree speculative unique index");

	ii->ii_UniqueOps = (Oid *) palloc(sizeof(Oid) * indnkeyatts);
	ii->ii_UniqueProcs = (Oid *) palloc(sizeof(Oid) * indnkeyatts);
	ii->ii_UniqueStrats = (uint16 *) palloc(sizeof(uint16) * indnkeyatts);

	/*
	 * We have to look up the operator's strategy number.  This provides a
	 * cross-check that the operator does match the index.
	 */
	/* We need the func OIDs and strategy numbers too */
	for (i = 0; i < indnkeyatts; i++)
	{
		ii->ii_UniqueStrats[i] = BTEqualStrategyNumber;
		ii->ii_UniqueOps[i] =
			get_opfamily_member(index->rd_opfamily[i],
								index->rd_opcintype[i],
								index->rd_opcintype[i],
								ii->ii_UniqueStrats[i]);
		if (!OidIsValid(ii->ii_UniqueOps[i]))
			elog(ERROR, "missing operator %d(%u,%u) in opfamily %u",
				 ii->ii_UniqueStrats[i], index->rd_opcintype[i],
				 index->rd_opcintype[i], index->rd_opfamily[i]);
		ii->ii_UniqueProcs[i] = get_opcode(ii->ii_UniqueOps[i]);
	}
}

/* ----------------
 *		FormIndexDatum
 *			Construct values[] and isnull[] arrays for a new index tuple.
 *
 *	indexInfo		Info about the index
 *	slot			Heap tuple for which we must prepare an index entry
 *	estate			executor state for evaluating any index expressions
 *	values			Array of index Datums (output area)
 *	isnull			Array of is-null indicators (output area)
 *
 * When there are no index expressions, estate may be NULL.  Otherwise it
 * must be supplied, *and* the ecxt_scantuple slot of its per-tuple expr
 * context must point to the heap tuple passed in.
 *
 * Notice we don't actually call index_form_tuple() here; we just prepare
 * its input arrays values[] and isnull[].  This is because the index AM
 * may wish to alter the data before storage.
 * ----------------
 */
void
FormIndexDatum(IndexInfo *indexInfo,
			   TupleTableSlot *slot,
			   EState *estate,
			   Datum *values,
			   bool *isnull)
{
	ListCell   *indexpr_item;
	int			i;

	if (indexInfo->ii_Expressions != NIL &&
		indexInfo->ii_ExpressionsState == NIL)
	{
		/* First time through, set up expression evaluation state */
		indexInfo->ii_ExpressionsState =
			ExecPrepareExprList(indexInfo->ii_Expressions, estate);
		/* Check caller has set up context correctly */
		Assert(GetPerTupleExprContext(estate)->ecxt_scantuple == slot);
	}
	indexpr_item = list_head(indexInfo->ii_ExpressionsState);

	for (i = 0; i < indexInfo->ii_NumIndexAttrs; i++)
	{
		int			keycol = indexInfo->ii_IndexAttrNumbers[i];
		Datum		iDatum;
		bool		isNull;

		if (keycol != 0)
		{
			/*
			 * Plain index column; get the value we need directly from the
			 * heap tuple.
			 */
			iDatum = slot_getattr(slot, keycol, &isNull);
		}
		else
		{
			/*
			 * Index expression --- need to evaluate it.
			 */
			if (indexpr_item == NULL)
				elog(ERROR, "wrong number of index expressions");
			iDatum = ExecEvalExprSwitchContext((ExprState *) lfirst(indexpr_item),
											   GetPerTupleExprContext(estate),
											   &isNull);
			indexpr_item = lnext(indexpr_item);
		}
		values[i] = iDatum;
		isnull[i] = isNull;
	}

	if (indexpr_item != NULL)
		elog(ERROR, "wrong number of index expressions");
}


/*
 * index_update_stats --- update pg_class entry after CREATE INDEX or REINDEX
 *
 * This routine updates the pg_class row of either an index or its parent
 * relation after CREATE INDEX or REINDEX.  Its rather bizarre API is designed
 * to ensure we can do all the necessary work in just one update.
 *
 * hasindex: set relhasindex to this value
 * reltuples: if >= 0, set reltuples to this value; else no change
 *
 * If reltuples >= 0, relpages and relallvisible are also updated (using
 * RelationGetNumberOfBlocks() and visibilitymap_count()).
 *
 * NOTE: an important side-effect of this operation is that an SI invalidation
 * message is sent out to all backends --- including me --- causing relcache
 * entries to be flushed or updated with the new data.  This must happen even
 * if we find that no change is needed in the pg_class row.  When updating
 * a heap entry, this ensures that other backends find out about the new
 * index.  When updating an index, it's important because some index AMs
 * expect a relcache flush to occur after REINDEX.
 */
static void
index_update_stats(Relation rel,
				   bool hasindex,
				   double reltuples)
{
	Oid			relid = RelationGetRelid(rel);
	Relation	pg_class;
	HeapTuple	tuple;
	Form_pg_class rd_rel;
	bool		dirty;

	/*
	 * We always update the pg_class row using a non-transactional,
	 * overwrite-in-place update.  There are several reasons for this:
	 *
	 * 1. In bootstrap mode, we have no choice --- UPDATE wouldn't work.
	 *
	 * 2. We could be reindexing pg_class itself, in which case we can't move
	 * its pg_class row because CatalogTupleInsert/CatalogTupleUpdate might
	 * not know about all the indexes yet (see reindex_relation).
	 *
	 * 3. Because we execute CREATE INDEX with just share lock on the parent
	 * rel (to allow concurrent index creations), an ordinary update could
	 * suffer a tuple-concurrently-updated failure against another CREATE
	 * INDEX committing at about the same time.  We can avoid that by having
	 * them both do nontransactional updates (we assume they will both be
	 * trying to change the pg_class row to the same thing, so it doesn't
	 * matter which goes first).
	 *
	 * It is safe to use a non-transactional update even though our
	 * transaction could still fail before committing.  Setting relhasindex
	 * true is safe even if there are no indexes (VACUUM will eventually fix
	 * it).  And of course the new relpages and reltuples counts are correct
	 * regardless.  However, we don't want to change relpages (or
	 * relallvisible) if the caller isn't providing an updated reltuples
	 * count, because that would bollix the reltuples/relpages ratio which is
	 * what's really important.
	 */

	pg_class = heap_open(RelationRelationId, RowExclusiveLock);

	/*
	 * Make a copy of the tuple to update.  Normally we use the syscache, but
	 * we can't rely on that during bootstrap or while reindexing pg_class
	 * itself.
	 */
	if (IsBootstrapProcessingMode() ||
		ReindexIsProcessingHeap(RelationRelationId))
	{
		/* don't assume syscache will work */
		HeapScanDesc pg_class_scan;
		ScanKeyData key[1];

		ScanKeyInit(&key[0],
					ObjectIdAttributeNumber,
					BTEqualStrategyNumber, F_OIDEQ,
					ObjectIdGetDatum(relid));

		pg_class_scan = heap_beginscan_catalog(pg_class, 1, key);
		tuple = heap_getnext(pg_class_scan, ForwardScanDirection);
		tuple = heap_copytuple(tuple);
		heap_endscan(pg_class_scan);
	}
	else
	{
		/* normal case, use syscache */
		tuple = SearchSysCacheCopy1(RELOID, ObjectIdGetDatum(relid));
	}

	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "could not find tuple for relation %u", relid);
	rd_rel = (Form_pg_class) GETSTRUCT(tuple);

	/* Should this be a more comprehensive test? */
	Assert(rd_rel->relkind != RELKIND_PARTITIONED_INDEX);

	/* Apply required updates, if any, to copied tuple */

	dirty = false;
	if (rd_rel->relhasindex != hasindex)
	{
		rd_rel->relhasindex = hasindex;
		dirty = true;
	}

	if (reltuples >= 0)
	{
		BlockNumber relpages = RelationGetNumberOfBlocks(rel);
		BlockNumber relallvisible;

		if (rd_rel->relkind != RELKIND_INDEX)
			visibilitymap_count(rel, &relallvisible, NULL);
		else					/* don't bother for indexes */
			relallvisible = 0;

		if (rd_rel->relpages != (int32) relpages)
		{
			rd_rel->relpages = (int32) relpages;
			dirty = true;
		}
		if (rd_rel->reltuples != (float4) reltuples)
		{
			rd_rel->reltuples = (float4) reltuples;
			dirty = true;
		}
		if (rd_rel->relallvisible != (int32) relallvisible)
		{
			rd_rel->relallvisible = (int32) relallvisible;
			dirty = true;
		}
	}

	/*
	 * If anything changed, write out the tuple
	 */
	if (dirty)
	{
		heap_inplace_update(pg_class, tuple);
		/* the above sends a cache inval message */
	}
	else
	{
		/* no need to change tuple, but force relcache inval anyway */
		CacheInvalidateRelcacheByTuple(tuple);
	}

	heap_freetuple(tuple);

	heap_close(pg_class, RowExclusiveLock);
}


/*
 * index_build - invoke access-method-specific index build procedure
 *
 * On entry, the index's catalog entries are valid, and its physical disk
 * file has been created but is empty.  We call the AM-specific build
 * procedure to fill in the index contents.  We then update the pg_class
 * entries of the index and heap relation as needed, using statistics
 * returned by ambuild as well as data passed by the caller.
 *
 * isprimary tells whether to mark the index as a primary-key index.
 * isreindex indicates we are recreating a previously-existing index.
 * parallel indicates if parallelism may be useful.
 *
 * Note: when reindexing an existing index, isprimary can be false even if
 * the index is a PK; it's already properly marked and need not be re-marked.
 *
 * Note: before Postgres 8.2, the passed-in heap and index Relations
 * were automatically closed by this routine.  This is no longer the case.
 * The caller opened 'em, and the caller should close 'em.
 */
void
index_build(Relation heapRelation,
			Relation indexRelation,
			IndexInfo *indexInfo,
			bool isprimary,
			bool isreindex,
			bool parallel)
{
	IndexBuildResult *stats;
	Oid			save_userid;
	int			save_sec_context;
	int			save_nestlevel;

	/*
	 * sanity checks
	 */
	Assert(RelationIsValid(indexRelation));
	Assert(PointerIsValid(indexRelation->rd_amroutine));
	Assert(PointerIsValid(indexRelation->rd_amroutine->ambuild));
	Assert(PointerIsValid(indexRelation->rd_amroutine->ambuildempty));

	/*
	 * Determine worker process details for parallel CREATE INDEX.  Currently,
	 * only btree has support for parallel builds.
	 *
	 * Note that planner considers parallel safety for us.
	 */
	if (parallel && IsNormalProcessingMode() &&
		indexRelation->rd_rel->relam == BTREE_AM_OID)
		indexInfo->ii_ParallelWorkers =
			plan_create_index_workers(RelationGetRelid(heapRelation),
									  RelationGetRelid(indexRelation));

	if (indexInfo->ii_ParallelWorkers == 0)
		ereport(DEBUG1,
				(errmsg("building index \"%s\" on table \"%s\" serially",
						RelationGetRelationName(indexRelation),
						RelationGetRelationName(heapRelation))));
	else
		ereport(DEBUG1,
				(errmsg_plural("building index \"%s\" on table \"%s\" with request for %d parallel worker",
							   "building index \"%s\" on table \"%s\" with request for %d parallel workers",
							   indexInfo->ii_ParallelWorkers,
							   RelationGetRelationName(indexRelation),
							   RelationGetRelationName(heapRelation),
							   indexInfo->ii_ParallelWorkers)));

	/*
	 * Switch to the table owner's userid, so that any index functions are run
	 * as that user.  Also lock down security-restricted operations and
	 * arrange to make GUC variable changes local to this command.
	 */
	GetUserIdAndSecContext(&save_userid, &save_sec_context);
	SetUserIdAndSecContext(heapRelation->rd_rel->relowner,
						   save_sec_context | SECURITY_RESTRICTED_OPERATION);
	save_nestlevel = NewGUCNestLevel();

	/*
	 * Call the access method's build procedure
	 */
	stats = indexRelation->rd_amroutine->ambuild(heapRelation, indexRelation,
												 indexInfo);
	Assert(PointerIsValid(stats));

	/*
	 * If this is an unlogged index, we may need to write out an init fork for
	 * it -- but we must first check whether one already exists.  If, for
	 * example, an unlogged relation is truncated in the transaction that
	 * created it, or truncated twice in a subsequent transaction, the
	 * relfilenode won't change, and nothing needs to be done here.
	 */
	if (indexRelation->rd_rel->relpersistence == RELPERSISTENCE_UNLOGGED &&
		!smgrexists(indexRelation->rd_smgr, INIT_FORKNUM))
	{
		RelationOpenSmgr(indexRelation);
		smgrcreate(indexRelation->rd_smgr, INIT_FORKNUM, false);
		indexRelation->rd_amroutine->ambuildempty(indexRelation);
	}

	/*
	 * If we found any potentially broken HOT chains, mark the index as not
	 * being usable until the current transaction is below the event horizon.
	 * See src/backend/access/heap/README.HOT for discussion.  Also set this
	 * if early pruning/vacuuming is enabled for the heap relation.  While it
	 * might become safe to use the index earlier based on actual cleanup
	 * activity and other active transactions, the test for that would be much
	 * more complex and would require some form of blocking, so keep it simple
	 * and fast by just using the current transaction.
	 *
	 * However, when reindexing an existing index, we should do nothing here.
	 * Any HOT chains that are broken with respect to the index must predate
	 * the index's original creation, so there is no need to change the
	 * index's usability horizon.  Moreover, we *must not* try to change the
	 * index's pg_index entry while reindexing pg_index itself, and this
	 * optimization nicely prevents that.  The more complex rules needed for a
	 * reindex are handled separately after this function returns.
	 *
	 * We also need not set indcheckxmin during a concurrent index build,
	 * because we won't set indisvalid true until all transactions that care
	 * about the broken HOT chains or early pruning/vacuuming are gone.
	 *
	 * Therefore, this code path can only be taken during non-concurrent
	 * CREATE INDEX.  Thus the fact that heap_update will set the pg_index
	 * tuple's xmin doesn't matter, because that tuple was created in the
	 * current transaction anyway.  That also means we don't need to worry
	 * about any concurrent readers of the tuple; no other transaction can see
	 * it yet.
	 */
	if ((indexInfo->ii_BrokenHotChain || EarlyPruningEnabled(heapRelation)) &&
		!isreindex &&
		!indexInfo->ii_Concurrent)
	{
		Oid			indexId = RelationGetRelid(indexRelation);
		Relation	pg_index;
		HeapTuple	indexTuple;
		Form_pg_index indexForm;

		pg_index = heap_open(IndexRelationId, RowExclusiveLock);

		indexTuple = SearchSysCacheCopy1(INDEXRELID,
										 ObjectIdGetDatum(indexId));
		if (!HeapTupleIsValid(indexTuple))
			elog(ERROR, "cache lookup failed for index %u", indexId);
		indexForm = (Form_pg_index) GETSTRUCT(indexTuple);

		/* If it's a new index, indcheckxmin shouldn't be set ... */
		Assert(!indexForm->indcheckxmin);

		indexForm->indcheckxmin = true;
		CatalogTupleUpdate(pg_index, &indexTuple->t_self, indexTuple);

		heap_freetuple(indexTuple);
		heap_close(pg_index, RowExclusiveLock);
	}

	/*
	 * Update heap and index pg_class rows
	 */
	index_update_stats(heapRelation,
					   true,
					   stats->heap_tuples);

	index_update_stats(indexRelation,
					   false,
					   stats->index_tuples);

	/* Make the updated catalog row versions visible */
	CommandCounterIncrement();

	/*
	 * If it's for an exclusion constraint, make a second pass over the heap
	 * to verify that the constraint is satisfied.  We must not do this until
	 * the index is fully valid.  (Broken HOT chains shouldn't matter, though;
	 * see comments for IndexCheckExclusion.)
	 */
	if (indexInfo->ii_ExclusionOps != NULL)
		IndexCheckExclusion(heapRelation, indexRelation, indexInfo);

	/* Roll back any GUC changes executed by index functions */
	AtEOXact_GUC(false, save_nestlevel);

	/* Restore userid and security context */
	SetUserIdAndSecContext(save_userid, save_sec_context);
}


/*
 * IndexBuildHeapScan - scan the heap relation to find tuples to be indexed
 *
 * This is called back from an access-method-specific index build procedure
 * after the AM has done whatever setup it needs.  The parent heap relation
 * is scanned to find tuples that should be entered into the index.  Each
 * such tuple is passed to the AM's callback routine, which does the right
 * things to add it to the new index.  After we return, the AM's index
 * build procedure does whatever cleanup it needs.
 *
 * The total count of live heap tuples is returned.  This is for updating
 * pg_class statistics.  (It's annoying not to be able to do that here, but we
 * want to merge that update with others; see index_update_stats.)  Note that
 * the index AM itself must keep track of the number of index tuples; we don't
 * do so here because the AM might reject some of the tuples for its own
 * reasons, such as being unable to store NULLs.
 *
 * A side effect is to set indexInfo->ii_BrokenHotChain to true if we detect
 * any potentially broken HOT chains.  Currently, we set this if there are
 * any RECENTLY_DEAD or DELETE_IN_PROGRESS entries in a HOT chain, without
 * trying very hard to detect whether they're really incompatible with the
 * chain tip.
 */
double
IndexBuildHeapScan(Relation heapRelation,
				   Relation indexRelation,
				   IndexInfo *indexInfo,
				   bool allow_sync,
				   IndexBuildCallback callback,
				   void *callback_state,
				   HeapScanDesc scan)
{
	return IndexBuildHeapRangeScan(heapRelation, indexRelation,
								   indexInfo, allow_sync,
								   false,
								   0, InvalidBlockNumber,
								   callback, callback_state, scan);
}

/*
 * As above, except that instead of scanning the complete heap, only the given
 * number of blocks are scanned.  Scan to end-of-rel can be signalled by
 * passing InvalidBlockNumber as numblocks.  Note that restricting the range
 * to scan cannot be done when requesting syncscan.
 *
 * When "anyvisible" mode is requested, all tuples visible to any transaction
 * are indexed and counted as live, including those inserted or deleted by
 * transactions that are still in progress.
 */
double
IndexBuildHeapRangeScan(Relation heapRelation,
						Relation indexRelation,
						IndexInfo *indexInfo,
						bool allow_sync,
						bool anyvisible,
						BlockNumber start_blockno,
						BlockNumber numblocks,
						IndexBuildCallback callback,
						void *callback_state,
						HeapScanDesc scan)
{
	bool		is_system_catalog;
	bool		checking_uniqueness;
	HeapTuple	heapTuple;
	Datum		values[INDEX_MAX_KEYS];
	bool		isnull[INDEX_MAX_KEYS];
	double		reltuples;
	ExprState  *predicate;
	TupleTableSlot *slot;
	EState	   *estate;
	ExprContext *econtext;
	Snapshot	snapshot;
	bool		need_unregister_snapshot = false;
	TransactionId OldestXmin;
	BlockNumber root_blkno = InvalidBlockNumber;
	OffsetNumber root_offsets[MaxHeapTuplesPerPage];

	/*
	 * sanity checks
	 */
	Assert(OidIsValid(indexRelation->rd_rel->relam));

	/* Remember if it's a system catalog */
	is_system_catalog = IsSystemRelation(heapRelation);

	/* See whether we're verifying uniqueness/exclusion properties */
	checking_uniqueness = (indexInfo->ii_Unique ||
						   indexInfo->ii_ExclusionOps != NULL);

	/*
	 * "Any visible" mode is not compatible with uniqueness checks; make sure
	 * only one of those is requested.
	 */
	Assert(!(anyvisible && checking_uniqueness));

	/*
	 * Need an EState for evaluation of index expressions and partial-index
	 * predicates.  Also a slot to hold the current tuple.
	 */
	estate = CreateExecutorState();
	econtext = GetPerTupleExprContext(estate);
	slot = MakeSingleTupleTableSlot(RelationGetDescr(heapRelation));

	/* Arrange for econtext's scan tuple to be the tuple under test */
	econtext->ecxt_scantuple = slot;

	/* Set up execution state for predicate, if any. */
	predicate = ExecPrepareQual(indexInfo->ii_Predicate, estate);

	/*
	 * Prepare for scan of the base relation.  In a normal index build, we use
	 * SnapshotAny because we must retrieve all tuples and do our own time
	 * qual checks (because we have to index RECENTLY_DEAD tuples). In a
	 * concurrent build, or during bootstrap, we take a regular MVCC snapshot
	 * and index whatever's live according to that.
	 */
	OldestXmin = InvalidTransactionId;

	/* okay to ignore lazy VACUUMs here */
	if (!IsBootstrapProcessingMode() && !indexInfo->ii_Concurrent)
		OldestXmin = GetOldestXmin(heapRelation, PROCARRAY_FLAGS_VACUUM);

	if (!scan)
	{
		/*
		 * Serial index build.
		 *
		 * Must begin our own heap scan in this case.  We may also need to
		 * register a snapshot whose lifetime is under our direct control.
		 */
		if (!TransactionIdIsValid(OldestXmin))
		{
			snapshot = RegisterSnapshot(GetTransactionSnapshot());
			need_unregister_snapshot = true;
		}
		else
			snapshot = SnapshotAny;

		scan = heap_beginscan_strat(heapRelation,	/* relation */
									snapshot,	/* snapshot */
									0,	/* number of keys */
									NULL,	/* scan key */
									true,	/* buffer access strategy OK */
									allow_sync);	/* syncscan OK? */
	}
	else
	{
		/*
		 * Parallel index build.
		 *
		 * Parallel case never registers/unregisters own snapshot.  Snapshot
		 * is taken from parallel heap scan, and is SnapshotAny or an MVCC
		 * snapshot, based on same criteria as serial case.
		 */
		Assert(!IsBootstrapProcessingMode());
		Assert(allow_sync);
		snapshot = scan->rs_snapshot;
	}

	/*
	 * Must call GetOldestXmin() with SnapshotAny.  Should never call
	 * GetOldestXmin() with MVCC snapshot. (It's especially worth checking
	 * this for parallel builds, since ambuild routines that support parallel
	 * builds must work these details out for themselves.)
	 */
	Assert(snapshot == SnapshotAny || IsMVCCSnapshot(snapshot));
	Assert(snapshot == SnapshotAny ? TransactionIdIsValid(OldestXmin) :
		   !TransactionIdIsValid(OldestXmin));
	Assert(snapshot == SnapshotAny || !anyvisible);

	/* set our scan endpoints */
	if (!allow_sync)
		heap_setscanlimits(scan, start_blockno, numblocks);
	else
	{
		/* syncscan can only be requested on whole relation */
		Assert(start_blockno == 0);
		Assert(numblocks == InvalidBlockNumber);
	}

	reltuples = 0;

	/*
	 * Scan all tuples in the base relation.
	 */
	while ((heapTuple = heap_getnext(scan, ForwardScanDirection)) != NULL)
	{
		bool		tupleIsAlive;

		CHECK_FOR_INTERRUPTS();

		/*
		 * When dealing with a HOT-chain of updated tuples, we want to index
		 * the values of the live tuple (if any), but index it under the TID
		 * of the chain's root tuple.  This approach is necessary to preserve
		 * the HOT-chain structure in the heap. So we need to be able to find
		 * the root item offset for every tuple that's in a HOT-chain.  When
		 * first reaching a new page of the relation, call
		 * heap_get_root_tuples() to build a map of root item offsets on the
		 * page.
		 *
		 * It might look unsafe to use this information across buffer
		 * lock/unlock.  However, we hold ShareLock on the table so no
		 * ordinary insert/update/delete should occur; and we hold pin on the
		 * buffer continuously while visiting the page, so no pruning
		 * operation can occur either.
		 *
		 * Also, although our opinions about tuple liveness could change while
		 * we scan the page (due to concurrent transaction commits/aborts),
		 * the chain root locations won't, so this info doesn't need to be
		 * rebuilt after waiting for another transaction.
		 *
		 * Note the implied assumption that there is no more than one live
		 * tuple per HOT-chain --- else we could create more than one index
		 * entry pointing to the same root tuple.
		 */
		if (scan->rs_cblock != root_blkno)
		{
			Page		page = BufferGetPage(scan->rs_cbuf);

			LockBuffer(scan->rs_cbuf, BUFFER_LOCK_SHARE);
			heap_get_root_tuples(page, root_offsets);
			LockBuffer(scan->rs_cbuf, BUFFER_LOCK_UNLOCK);

			root_blkno = scan->rs_cblock;
		}

		if (snapshot == SnapshotAny)
		{
			/* do our own time qual check */
			bool		indexIt;
			TransactionId xwait;

	recheck:

			/*
			 * We could possibly get away with not locking the buffer here,
			 * since caller should hold ShareLock on the relation, but let's
			 * be conservative about it.  (This remark is still correct even
			 * with HOT-pruning: our pin on the buffer prevents pruning.)
			 */
			LockBuffer(scan->rs_cbuf, BUFFER_LOCK_SHARE);

			/*
			 * The criteria for counting a tuple as live in this block need to
			 * match what analyze.c's acquire_sample_rows() does, otherwise
			 * CREATE INDEX and ANALYZE may produce wildly different reltuples
			 * values, e.g. when there are many recently-dead tuples.
			 */
			switch (HeapTupleSatisfiesVacuum(heapTuple, OldestXmin,
											 scan->rs_cbuf))
			{
				case HEAPTUPLE_DEAD:
					/* Definitely dead, we can ignore it */
					indexIt = false;
					tupleIsAlive = false;
					break;
				case HEAPTUPLE_LIVE:
					/* Normal case, index and unique-check it */
					indexIt = true;
					tupleIsAlive = true;
					/* Count it as live, too */
					reltuples += 1;
					break;
				case HEAPTUPLE_RECENTLY_DEAD:

					/*
					 * If tuple is recently deleted then we must index it
					 * anyway to preserve MVCC semantics.  (Pre-existing
					 * transactions could try to use the index after we finish
					 * building it, and may need to see such tuples.)
					 *
					 * However, if it was HOT-updated then we must only index
					 * the live tuple at the end of the HOT-chain.  Since this
					 * breaks semantics for pre-existing snapshots, mark the
					 * index as unusable for them.
					 *
					 * We don't count recently-dead tuples in reltuples, even
					 * if we index them; see acquire_sample_rows().
					 */
					if (HeapTupleIsHotUpdated(heapTuple))
					{
						indexIt = false;
						/* mark the index as unsafe for old snapshots */
						indexInfo->ii_BrokenHotChain = true;
					}
					else
						indexIt = true;
					/* In any case, exclude the tuple from unique-checking */
					tupleIsAlive = false;
					break;
				case HEAPTUPLE_INSERT_IN_PROGRESS:

					/*
					 * In "anyvisible" mode, this tuple is visible and we
					 * don't need any further checks.
					 */
					if (anyvisible)
					{
						indexIt = true;
						tupleIsAlive = true;
						reltuples += 1;
						break;
					}

					/*
					 * Since caller should hold ShareLock or better, normally
					 * the only way to see this is if it was inserted earlier
					 * in our own transaction.  However, it can happen in
					 * system catalogs, since we tend to release write lock
					 * before commit there.  Give a warning if neither case
					 * applies.
					 */
					xwait = HeapTupleHeaderGetXmin(heapTuple->t_data);
					if (!TransactionIdIsCurrentTransactionId(xwait))
					{
						if (!is_system_catalog)
							elog(WARNING, "concurrent insert in progress within table \"%s\"",
								 RelationGetRelationName(heapRelation));

						/*
						 * If we are performing uniqueness checks, indexing
						 * such a tuple could lead to a bogus uniqueness
						 * failure.  In that case we wait for the inserting
						 * transaction to finish and check again.
						 */
						if (checking_uniqueness)
						{
							/*
							 * Must drop the lock on the buffer before we wait
							 */
							LockBuffer(scan->rs_cbuf, BUFFER_LOCK_UNLOCK);
							XactLockTableWait(xwait, heapRelation,
											  &heapTuple->t_self,
											  XLTW_InsertIndexUnique);
							CHECK_FOR_INTERRUPTS();
							goto recheck;
						}
					}
					else
					{
						/*
						 * For consistency with acquire_sample_rows(), count
						 * HEAPTUPLE_INSERT_IN_PROGRESS tuples as live only
						 * when inserted by our own transaction.
						 */
						reltuples += 1;
					}

					/*
					 * We must index such tuples, since if the index build
					 * commits then they're good.
					 */
					indexIt = true;
					tupleIsAlive = true;
					break;
				case HEAPTUPLE_DELETE_IN_PROGRESS:

					/*
					 * As with INSERT_IN_PROGRESS case, this is unexpected
					 * unless it's our own deletion or a system catalog; but
					 * in anyvisible mode, this tuple is visible.
					 */
					if (anyvisible)
					{
						indexIt = true;
						tupleIsAlive = false;
						reltuples += 1;
						break;
					}

					xwait = HeapTupleHeaderGetUpdateXid(heapTuple->t_data);
					if (!TransactionIdIsCurrentTransactionId(xwait))
					{
						if (!is_system_catalog)
							elog(WARNING, "concurrent delete in progress within table \"%s\"",
								 RelationGetRelationName(heapRelation));

						/*
						 * If we are performing uniqueness checks, assuming
						 * the tuple is dead could lead to missing a
						 * uniqueness violation.  In that case we wait for the
						 * deleting transaction to finish and check again.
						 *
						 * Also, if it's a HOT-updated tuple, we should not
						 * index it but rather the live tuple at the end of
						 * the HOT-chain.  However, the deleting transaction
						 * could abort, possibly leaving this tuple as live
						 * after all, in which case it has to be indexed. The
						 * only way to know what to do is to wait for the
						 * deleting transaction to finish and check again.
						 */
						if (checking_uniqueness ||
							HeapTupleIsHotUpdated(heapTuple))
						{
							/*
							 * Must drop the lock on the buffer before we wait
							 */
							LockBuffer(scan->rs_cbuf, BUFFER_LOCK_UNLOCK);
							XactLockTableWait(xwait, heapRelation,
											  &heapTuple->t_self,
											  XLTW_InsertIndexUnique);
							CHECK_FOR_INTERRUPTS();
							goto recheck;
						}

						/*
						 * Otherwise index it but don't check for uniqueness,
						 * the same as a RECENTLY_DEAD tuple.
						 */
						indexIt = true;

						/*
						 * Count HEAPTUPLE_DELETE_IN_PROGRESS tuples as live,
						 * if they were not deleted by the current
						 * transaction.  That's what acquire_sample_rows()
						 * does, and we want the behavior to be consistent.
						 */
						reltuples += 1;
					}
					else if (HeapTupleIsHotUpdated(heapTuple))
					{
						/*
						 * It's a HOT-updated tuple deleted by our own xact.
						 * We can assume the deletion will commit (else the
						 * index contents don't matter), so treat the same as
						 * RECENTLY_DEAD HOT-updated tuples.
						 */
						indexIt = false;
						/* mark the index as unsafe for old snapshots */
						indexInfo->ii_BrokenHotChain = true;
					}
					else
					{
						/*
						 * It's a regular tuple deleted by our own xact. Index
						 * it, but don't check for uniqueness nor count in
						 * reltuples, the same as a RECENTLY_DEAD tuple.
						 */
						indexIt = true;
					}
					/* In any case, exclude the tuple from unique-checking */
					tupleIsAlive = false;
					break;
				default:
					elog(ERROR, "unexpected HeapTupleSatisfiesVacuum result");
					indexIt = tupleIsAlive = false; /* keep compiler quiet */
					break;
			}

			LockBuffer(scan->rs_cbuf, BUFFER_LOCK_UNLOCK);

			if (!indexIt)
				continue;
		}
		else
		{
			/* heap_getnext did the time qual check */
			tupleIsAlive = true;
			reltuples += 1;
		}

		MemoryContextReset(econtext->ecxt_per_tuple_memory);

		/* Set up for predicate or expression evaluation */
		ExecStoreTuple(heapTuple, slot, InvalidBuffer, false);

		/*
		 * In a partial index, discard tuples that don't satisfy the
		 * predicate.
		 */
		if (predicate != NULL)
		{
			if (!ExecQual(predicate, econtext))
				continue;
		}

		/*
		 * For the current heap tuple, extract all the attributes we use in
		 * this index, and note which are null.  This also performs evaluation
		 * of any expressions needed.
		 */
		FormIndexDatum(indexInfo,
					   slot,
					   estate,
					   values,
					   isnull);

		/*
		 * You'd think we should go ahead and build the index tuple here, but
		 * some index AMs want to do further processing on the data first.  So
		 * pass the values[] and isnull[] arrays, instead.
		 */

		if (HeapTupleIsHeapOnly(heapTuple))
		{
			/*
			 * For a heap-only tuple, pretend its TID is that of the root. See
			 * src/backend/access/heap/README.HOT for discussion.
			 */
			HeapTupleData rootTuple;
			OffsetNumber offnum;

			rootTuple = *heapTuple;
			offnum = ItemPointerGetOffsetNumber(&heapTuple->t_self);

			if (!OffsetNumberIsValid(root_offsets[offnum - 1]))
				ereport(ERROR,
						(errcode(ERRCODE_DATA_CORRUPTED),
						 errmsg_internal("failed to find parent tuple for heap-only tuple at (%u,%u) in table \"%s\"",
										 ItemPointerGetBlockNumber(&heapTuple->t_self),
										 offnum,
										 RelationGetRelationName(heapRelation))));

			ItemPointerSetOffsetNumber(&rootTuple.t_self,
									   root_offsets[offnum - 1]);

			/* Call the AM's callback routine to process the tuple */
			callback(indexRelation, &rootTuple, values, isnull, tupleIsAlive,
					 callback_state);
		}
		else
		{
			/* Call the AM's callback routine to process the tuple */
			callback(indexRelation, heapTuple, values, isnull, tupleIsAlive,
					 callback_state);
		}
	}

	heap_endscan(scan);

	/* we can now forget our snapshot, if set and registered by us */
	if (need_unregister_snapshot)
		UnregisterSnapshot(snapshot);

	ExecDropSingleTupleTableSlot(slot);

	FreeExecutorState(estate);

	/* These may have been pointing to the now-gone estate */
	indexInfo->ii_ExpressionsState = NIL;
	indexInfo->ii_PredicateState = NULL;

	return reltuples;
}


/*
 * IndexCheckExclusion - verify that a new exclusion constraint is satisfied
 *
 * When creating an exclusion constraint, we first build the index normally
 * and then rescan the heap to check for conflicts.  We assume that we only
 * need to validate tuples that are live according to an up-to-date snapshot,
 * and that these were correctly indexed even in the presence of broken HOT
 * chains.  This should be OK since we are holding at least ShareLock on the
 * table, meaning there can be no uncommitted updates from other transactions.
 * (Note: that wouldn't necessarily work for system catalogs, since many
 * operations release write lock early on the system catalogs.)
 */
static void
IndexCheckExclusion(Relation heapRelation,
					Relation indexRelation,
					IndexInfo *indexInfo)
{
	HeapScanDesc scan;
	HeapTuple	heapTuple;
	Datum		values[INDEX_MAX_KEYS];
	bool		isnull[INDEX_MAX_KEYS];
	ExprState  *predicate;
	TupleTableSlot *slot;
	EState	   *estate;
	ExprContext *econtext;
	Snapshot	snapshot;

	/*
	 * If we are reindexing the target index, mark it as no longer being
	 * reindexed, to forestall an Assert in index_beginscan when we try to use
	 * the index for probes.  This is OK because the index is now fully valid.
	 */
	if (ReindexIsCurrentlyProcessingIndex(RelationGetRelid(indexRelation)))
		ResetReindexProcessing();

	/*
	 * Need an EState for evaluation of index expressions and partial-index
	 * predicates.  Also a slot to hold the current tuple.
	 */
	estate = CreateExecutorState();
	econtext = GetPerTupleExprContext(estate);
	slot = MakeSingleTupleTableSlot(RelationGetDescr(heapRelation));

	/* Arrange for econtext's scan tuple to be the tuple under test */
	econtext->ecxt_scantuple = slot;

	/* Set up execution state for predicate, if any. */
	predicate = ExecPrepareQual(indexInfo->ii_Predicate, estate);

	/*
	 * Scan all live tuples in the base relation.
	 */
	snapshot = RegisterSnapshot(GetLatestSnapshot());
	scan = heap_beginscan_strat(heapRelation,	/* relation */
								snapshot,	/* snapshot */
								0,	/* number of keys */
								NULL,	/* scan key */
								true,	/* buffer access strategy OK */
								true);	/* syncscan OK */

	while ((heapTuple = heap_getnext(scan, ForwardScanDirection)) != NULL)
	{
		CHECK_FOR_INTERRUPTS();

		MemoryContextReset(econtext->ecxt_per_tuple_memory);

		/* Set up for predicate or expression evaluation */
		ExecStoreTuple(heapTuple, slot, InvalidBuffer, false);

		/*
		 * In a partial index, ignore tuples that don't satisfy the predicate.
		 */
		if (predicate != NULL)
		{
			if (!ExecQual(predicate, econtext))
				continue;
		}

		/*
		 * Extract index column values, including computing expressions.
		 */
		FormIndexDatum(indexInfo,
					   slot,
					   estate,
					   values,
					   isnull);

		/*
		 * Check that this tuple has no conflicts.
		 */
		check_exclusion_constraint(heapRelation,
								   indexRelation, indexInfo,
								   &(heapTuple->t_self), values, isnull,
								   estate, true);
	}

	heap_endscan(scan);
	UnregisterSnapshot(snapshot);

	ExecDropSingleTupleTableSlot(slot);

	FreeExecutorState(estate);

	/* These may have been pointing to the now-gone estate */
	indexInfo->ii_ExpressionsState = NIL;
	indexInfo->ii_PredicateState = NULL;
}


/*
 * validate_index - support code for concurrent index builds
 *
 * We do a concurrent index build by first inserting the catalog entry for the
 * index via index_create(), marking it not indisready and not indisvalid.
 * Then we commit our transaction and start a new one, then we wait for all
 * transactions that could have been modifying the table to terminate.  Now
 * we know that any subsequently-started transactions will see the index and
 * honor its constraints on HOT updates; so while existing HOT-chains might
 * be broken with respect to the index, no currently live tuple will have an
 * incompatible HOT update done to it.  We now build the index normally via
 * index_build(), while holding a weak lock that allows concurrent
 * insert/update/delete.  Also, we index only tuples that are valid
 * as of the start of the scan (see IndexBuildHeapScan), whereas a normal
 * build takes care to include recently-dead tuples.  This is OK because
 * we won't mark the index valid until all transactions that might be able
 * to see those tuples are gone.  The reason for doing that is to avoid
 * bogus unique-index failures due to concurrent UPDATEs (we might see
 * different versions of the same row as being valid when we pass over them,
 * if we used HeapTupleSatisfiesVacuum).  This leaves us with an index that
 * does not contain any tuples added to the table while we built the index.
 *
 * Next, we mark the index "indisready" (but still not "indisvalid") and
 * commit the second transaction and start a third.  Again we wait for all
 * transactions that could have been modifying the table to terminate.  Now
 * we know that any subsequently-started transactions will see the index and
 * insert their new tuples into it.  We then take a new reference snapshot
 * which is passed to validate_index().  Any tuples that are valid according
 * to this snap, but are not in the index, must be added to the index.
 * (Any tuples committed live after the snap will be inserted into the
 * index by their originating transaction.  Any tuples committed dead before
 * the snap need not be indexed, because we will wait out all transactions
 * that might care about them before we mark the index valid.)
 *
 * validate_index() works by first gathering all the TIDs currently in the
 * index, using a bulkdelete callback that just stores the TIDs and doesn't
 * ever say "delete it".  (This should be faster than a plain indexscan;
 * also, not all index AMs support full-index indexscan.)  Then we sort the
 * TIDs, and finally scan the table doing a "merge join" against the TID list
 * to see which tuples are missing from the index.  Thus we will ensure that
 * all tuples valid according to the reference snapshot are in the index.
 *
 * Building a unique index this way is tricky: we might try to insert a
 * tuple that is already dead or is in process of being deleted, and we
 * mustn't have a uniqueness failure against an updated version of the same
 * row.  We could try to check the tuple to see if it's already dead and tell
 * index_insert() not to do the uniqueness check, but that still leaves us
 * with a race condition against an in-progress update.  To handle that,
 * we expect the index AM to recheck liveness of the to-be-inserted tuple
 * before it declares a uniqueness error.
 *
 * After completing validate_index(), we wait until all transactions that
 * were alive at the time of the reference snapshot are gone; this is
 * necessary to be sure there are none left with a transaction snapshot
 * older than the reference (and hence possibly able to see tuples we did
 * not index).  Then we mark the index "indisvalid" and commit.  Subsequent
 * transactions will be able to use it for queries.
 *
 * Doing two full table scans is a brute-force strategy.  We could try to be
 * cleverer, eg storing new tuples in a special area of the table (perhaps
 * making the table append-only by setting use_fsm).  However that would
 * add yet more locking issues.
 */
void
validate_index(Oid heapId, Oid indexId, Snapshot snapshot)
{
	Relation	heapRelation,
				indexRelation;
	IndexInfo  *indexInfo;
	IndexVacuumInfo ivinfo;
	v_i_state	state;
	Oid			save_userid;
	int			save_sec_context;
	int			save_nestlevel;

	/* Open and lock the parent heap relation */
	heapRelation = heap_open(heapId, ShareUpdateExclusiveLock);

	/*
	 * Switch to the table owner's userid, so that any index functions are run
	 * as that user.  Also lock down security-restricted operations and
	 * arrange to make GUC variable changes local to this command.
	 */
	GetUserIdAndSecContext(&save_userid, &save_sec_context);
	SetUserIdAndSecContext(heapRelation->rd_rel->relowner,
						   save_sec_context | SECURITY_RESTRICTED_OPERATION);
	save_nestlevel = NewGUCNestLevel();

	indexRelation = index_open(indexId, RowExclusiveLock);

	/*
	 * Fetch info needed for index_insert.  (You might think this should be
	 * passed in from DefineIndex, but its copy is long gone due to having
	 * been built in a previous transaction.)
	 */
	indexInfo = BuildIndexInfo(indexRelation);

	/* mark build is concurrent just for consistency */
	indexInfo->ii_Concurrent = true;

	/*
	 * Scan the index and gather up all the TIDs into a tuplesort object.
	 */
	ivinfo.index = indexRelation;
	ivinfo.analyze_only = false;
	ivinfo.estimated_count = true;
	ivinfo.message_level = DEBUG2;
	ivinfo.num_heap_tuples = heapRelation->rd_rel->reltuples;
	ivinfo.strategy = NULL;

	/*
	 * Encode TIDs as int8 values for the sort, rather than directly sorting
	 * item pointers.  This can be significantly faster, primarily because TID
	 * is a pass-by-reference type on all platforms, whereas int8 is
	 * pass-by-value on most platforms.
	 */
	state.tuplesort = tuplesort_begin_datum(INT8OID, Int8LessOperator,
											InvalidOid, false,
											maintenance_work_mem,
											NULL, false);
	state.htups = state.itups = state.tups_inserted = 0;

	(void) index_bulk_delete(&ivinfo, NULL,
							 validate_index_callback, (void *) &state);

	/* Execute the sort */
	tuplesort_performsort(state.tuplesort);

	/*
	 * Now scan the heap and "merge" it with the index
	 */
	if (PX_ENABLE_BTBUILD_CIC_PHASE2(indexRelation))
	{
		polar_px_validate_index_heapscan(heapRelation,
										indexRelation,
										indexInfo,
										snapshot,
										&state);
	}
	else
	{
		validate_index_heapscan(heapRelation,
								indexRelation,
								indexInfo,
								snapshot,
								&state);
	}

	/* Done with tuplesort object */
	tuplesort_end(state.tuplesort);

	elog(DEBUG2,
		 "validate_index found %.0f heap tuples, %.0f index tuples; inserted %.0f missing tuples",
		 state.htups, state.itups, state.tups_inserted);

	/* Roll back any GUC changes executed by index functions */
	AtEOXact_GUC(false, save_nestlevel);

	/* Restore userid and security context */
	SetUserIdAndSecContext(save_userid, save_sec_context);

	/* Close rels, but keep locks */
	index_close(indexRelation, NoLock);
	heap_close(heapRelation, NoLock);
}

/*
 * itemptr_encode - Encode ItemPointer as int64/int8
 *
 * This representation must produce values encoded as int64 that sort in the
 * same order as their corresponding original TID values would (using the
 * default int8 opclass to produce a result equivalent to the default TID
 * opclass).
 *
 * As noted in validate_index(), this can be significantly faster.
 */
static inline int64
itemptr_encode(ItemPointer itemptr)
{
	BlockNumber block = ItemPointerGetBlockNumber(itemptr);
	OffsetNumber offset = ItemPointerGetOffsetNumber(itemptr);
	int64		encoded;

	/*
	 * Use the 16 least significant bits for the offset.  32 adjacent bits are
	 * used for the block number.  Since remaining bits are unused, there
	 * cannot be negative encoded values (We assume a two's complement
	 * representation).
	 */
	encoded = ((uint64) block << 16) | (uint16) offset;

	return encoded;
}

/*
 * itemptr_decode - Decode int64/int8 representation back to ItemPointer
 */
static inline void
itemptr_decode(ItemPointer itemptr, int64 encoded)
{
	BlockNumber block = (BlockNumber) (encoded >> 16);
	OffsetNumber offset = (OffsetNumber) (encoded & 0xFFFF);

	ItemPointerSet(itemptr, block, offset);
}

/*
 * validate_index_callback - bulkdelete callback to collect the index TIDs
 */
static bool
validate_index_callback(ItemPointer itemptr, void *opaque)
{
	v_i_state  *state = (v_i_state *) opaque;
	int64		encoded = itemptr_encode(itemptr);

	tuplesort_putdatum(state->tuplesort, Int64GetDatum(encoded), false);
	state->itups += 1;
	return false;				/* never actually delete anything */
}

/*
 * validate_index_heapscan - second table scan for concurrent index build
 *
 * This has much code in common with IndexBuildHeapScan, but it's enough
 * different that it seems cleaner to have two routines not one.
 */
static void
validate_index_heapscan(Relation heapRelation,
						Relation indexRelation,
						IndexInfo *indexInfo,
						Snapshot snapshot,
						v_i_state *state)
{
	HeapScanDesc scan;
	HeapTuple	heapTuple;
	Datum		values[INDEX_MAX_KEYS];
	bool		isnull[INDEX_MAX_KEYS];
	ExprState  *predicate;
	TupleTableSlot *slot;
	EState	   *estate;
	ExprContext *econtext;
	BlockNumber root_blkno = InvalidBlockNumber;
	OffsetNumber root_offsets[MaxHeapTuplesPerPage];
	bool		in_index[MaxHeapTuplesPerPage];

	/* state variables for the merge */
	ItemPointer indexcursor = NULL;
	ItemPointerData decoded;
	bool		tuplesort_empty = false;

	/*
	 * sanity checks
	 */
	Assert(OidIsValid(indexRelation->rd_rel->relam));

	/*
	 * Need an EState for evaluation of index expressions and partial-index
	 * predicates.  Also a slot to hold the current tuple.
	 */
	estate = CreateExecutorState();
	econtext = GetPerTupleExprContext(estate);
	slot = MakeSingleTupleTableSlot(RelationGetDescr(heapRelation));

	/* Arrange for econtext's scan tuple to be the tuple under test */
	econtext->ecxt_scantuple = slot;

	/* Set up execution state for predicate, if any. */
	predicate = ExecPrepareQual(indexInfo->ii_Predicate, estate);

	/*
	 * Prepare for scan of the base relation.  We need just those tuples
	 * satisfying the passed-in reference snapshot.  We must disable syncscan
	 * here, because it's critical that we read from block zero forward to
	 * match the sorted TIDs.
	 */
	scan = heap_beginscan_strat(heapRelation,	/* relation */
								snapshot,	/* snapshot */
								0,	/* number of keys */
								NULL,	/* scan key */
								true,	/* buffer access strategy OK */
								false); /* syncscan not OK */

	/*
	 * Scan all tuples matching the snapshot.
	 */
	while ((heapTuple = heap_getnext(scan, ForwardScanDirection)) != NULL)
	{
		ItemPointer heapcursor = &heapTuple->t_self;
		ItemPointerData rootTuple;
		OffsetNumber root_offnum;

		CHECK_FOR_INTERRUPTS();

		state->htups += 1;

		/*
		 * As commented in IndexBuildHeapScan, we should index heap-only
		 * tuples under the TIDs of their root tuples; so when we advance onto
		 * a new heap page, build a map of root item offsets on the page.
		 *
		 * This complicates merging against the tuplesort output: we will
		 * visit the live tuples in order by their offsets, but the root
		 * offsets that we need to compare against the index contents might be
		 * ordered differently.  So we might have to "look back" within the
		 * tuplesort output, but only within the current page.  We handle that
		 * by keeping a bool array in_index[] showing all the
		 * already-passed-over tuplesort output TIDs of the current page. We
		 * clear that array here, when advancing onto a new heap page.
		 */
		if (scan->rs_cblock != root_blkno)
		{
			Page		page = BufferGetPage(scan->rs_cbuf);

			LockBuffer(scan->rs_cbuf, BUFFER_LOCK_SHARE);
			heap_get_root_tuples(page, root_offsets);
			LockBuffer(scan->rs_cbuf, BUFFER_LOCK_UNLOCK);

			memset(in_index, 0, sizeof(in_index));

			root_blkno = scan->rs_cblock;
		}

		/* Convert actual tuple TID to root TID */
		rootTuple = *heapcursor;
		root_offnum = ItemPointerGetOffsetNumber(heapcursor);

		if (HeapTupleIsHeapOnly(heapTuple))
		{
			root_offnum = root_offsets[root_offnum - 1];
			if (!OffsetNumberIsValid(root_offnum))
				ereport(ERROR,
						(errcode(ERRCODE_DATA_CORRUPTED),
						 errmsg_internal("failed to find parent tuple for heap-only tuple at (%u,%u) in table \"%s\"",
										 ItemPointerGetBlockNumber(heapcursor),
										 ItemPointerGetOffsetNumber(heapcursor),
										 RelationGetRelationName(heapRelation))));
			ItemPointerSetOffsetNumber(&rootTuple, root_offnum);
		}

		/*
		 * "merge" by skipping through the index tuples until we find or pass
		 * the current root tuple.
		 */
		while (!tuplesort_empty &&
			   (!indexcursor ||
				ItemPointerCompare(indexcursor, &rootTuple) < 0))
		{
			Datum		ts_val;
			bool		ts_isnull;

			if (indexcursor)
			{
				/*
				 * Remember index items seen earlier on the current heap page
				 */
				if (ItemPointerGetBlockNumber(indexcursor) == root_blkno)
					in_index[ItemPointerGetOffsetNumber(indexcursor) - 1] = true;
			}

			tuplesort_empty = !tuplesort_getdatum(state->tuplesort, true,
												  &ts_val, &ts_isnull, NULL);
			Assert(tuplesort_empty || !ts_isnull);
			if (!tuplesort_empty)
			{
				itemptr_decode(&decoded, DatumGetInt64(ts_val));
				indexcursor = &decoded;

				/* If int8 is pass-by-ref, free (encoded) TID Datum memory */
#ifndef USE_FLOAT8_BYVAL
				pfree(DatumGetPointer(ts_val));
#endif
			}
			else
			{
				/* Be tidy */
				indexcursor = NULL;
			}
		}

		/*
		 * If the tuplesort has overshot *and* we didn't see a match earlier,
		 * then this tuple is missing from the index, so insert it.
		 */
		if ((tuplesort_empty ||
			 ItemPointerCompare(indexcursor, &rootTuple) > 0) &&
			!in_index[root_offnum - 1])
		{
			MemoryContextReset(econtext->ecxt_per_tuple_memory);

			/* Set up for predicate or expression evaluation */
			ExecStoreTuple(heapTuple, slot, InvalidBuffer, false);

			/*
			 * In a partial index, discard tuples that don't satisfy the
			 * predicate.
			 */
			if (predicate != NULL)
			{
				if (!ExecQual(predicate, econtext))
					continue;
			}

			/*
			 * For the current heap tuple, extract all the attributes we use
			 * in this index, and note which are null.  This also performs
			 * evaluation of any expressions needed.
			 */
			FormIndexDatum(indexInfo,
						   slot,
						   estate,
						   values,
						   isnull);

			/*
			 * You'd think we should go ahead and build the index tuple here,
			 * but some index AMs want to do further processing on the data
			 * first. So pass the values[] and isnull[] arrays, instead.
			 */

			/*
			 * If the tuple is already committed dead, you might think we
			 * could suppress uniqueness checking, but this is no longer true
			 * in the presence of HOT, because the insert is actually a proxy
			 * for a uniqueness check on the whole HOT-chain.  That is, the
			 * tuple we have here could be dead because it was already
			 * HOT-updated, and if so the updating transaction will not have
			 * thought it should insert index entries.  The index AM will
			 * check the whole HOT-chain and correctly detect a conflict if
			 * there is one.
			 */

			index_insert(indexRelation,
						 values,
						 isnull,
						 &rootTuple,
						 heapRelation,
						 indexInfo->ii_Unique ?
						 UNIQUE_CHECK_YES : UNIQUE_CHECK_NO,
						 indexInfo);

			state->tups_inserted += 1;
		}
	}

	heap_endscan(scan);

	ExecDropSingleTupleTableSlot(slot);

	FreeExecutorState(estate);

	/* These may have been pointing to the now-gone estate */
	indexInfo->ii_ExpressionsState = NIL;
	indexInfo->ii_PredicateState = NULL;
}


/*
 * index_set_state_flags - adjust pg_index state flags
 *
 * This is used during CREATE/DROP INDEX CONCURRENTLY to adjust the pg_index
 * flags that denote the index's state.  Because the update is not
 * transactional and will not roll back on error, this must only be used as
 * the last step in a transaction that has not made any transactional catalog
 * updates!
 *
 * Note that heap_inplace_update does send a cache inval message for the
 * tuple, so other sessions will hear about the update as soon as we commit.
 *
 * NB: In releases prior to PostgreSQL 9.4, the use of a non-transactional
 * update here would have been unsafe; now that MVCC rules apply even for
 * system catalog scans, we could potentially use a transactional update here
 * instead.
 */
void
index_set_state_flags(Oid indexId, IndexStateFlagsAction action)
{
	Relation	pg_index;
	HeapTuple	indexTuple;
	Form_pg_index indexForm;

	/* Assert that current xact hasn't done any transactional updates */
	Assert(GetTopTransactionIdIfAny() == InvalidTransactionId);

	/* Open pg_index and fetch a writable copy of the index's tuple */
	pg_index = heap_open(IndexRelationId, RowExclusiveLock);

	indexTuple = SearchSysCacheCopy1(INDEXRELID,
									 ObjectIdGetDatum(indexId));
	if (!HeapTupleIsValid(indexTuple))
		elog(ERROR, "cache lookup failed for index %u", indexId);
	indexForm = (Form_pg_index) GETSTRUCT(indexTuple);

	/* Perform the requested state change on the copy */
	switch (action)
	{
		case INDEX_CREATE_SET_READY:
			/* Set indisready during a CREATE INDEX CONCURRENTLY sequence */
			Assert(indexForm->indislive);
			Assert(!indexForm->indisready);
			Assert(!indexForm->indisvalid);
			indexForm->indisready = true;
			break;
		case INDEX_CREATE_SET_VALID:
			/* Set indisvalid during a CREATE INDEX CONCURRENTLY sequence */
			Assert(indexForm->indislive);
			Assert(indexForm->indisready);
			Assert(!indexForm->indisvalid);
			indexForm->indisvalid = true;
			break;
		case INDEX_DROP_CLEAR_VALID:

			/*
			 * Clear indisvalid during a DROP INDEX CONCURRENTLY sequence
			 *
			 * If indisready == true we leave it set so the index still gets
			 * maintained by active transactions.  We only need to ensure that
			 * indisvalid is false.  (We don't assert that either is initially
			 * true, though, since we want to be able to retry a DROP INDEX
			 * CONCURRENTLY that failed partway through.)
			 *
			 * Note: the CLUSTER logic assumes that indisclustered cannot be
			 * set on any invalid index, so clear that flag too.
			 */
			indexForm->indisvalid = false;
			indexForm->indisclustered = false;
			break;
		case INDEX_DROP_SET_DEAD:

			/*
			 * Clear indisready/indislive during DROP INDEX CONCURRENTLY
			 *
			 * We clear both indisready and indislive, because we not only
			 * want to stop updates, we want to prevent sessions from touching
			 * the index at all.
			 */
			Assert(!indexForm->indisvalid);
			indexForm->indisready = false;
			indexForm->indislive = false;
			break;
	}

	/* ... and write it back in-place */
	heap_inplace_update(pg_index, indexTuple);

	heap_close(pg_index, RowExclusiveLock);
}


/*
 * IndexGetRelation: given an index's relation OID, get the OID of the
 * relation it is an index on.  Uses the system cache.
 */
Oid
IndexGetRelation(Oid indexId, bool missing_ok)
{
	HeapTuple	tuple;
	Form_pg_index index;
	Oid			result;

	tuple = SearchSysCache1(INDEXRELID, ObjectIdGetDatum(indexId));
	if (!HeapTupleIsValid(tuple))
	{
		if (missing_ok)
			return InvalidOid;
		elog(ERROR, "cache lookup failed for index %u", indexId);
	}
	index = (Form_pg_index) GETSTRUCT(tuple);
	Assert(index->indexrelid == indexId);

	result = index->indrelid;
	ReleaseSysCache(tuple);
	return result;
}

/*
 * reindex_index - This routine is used to recreate a single index
 */
void
reindex_index(Oid indexId, bool skip_constraint_checks, char persistence,
			  int options)
{
	Relation	iRel,
				heapRelation;
	Oid			heapId;
	Oid			save_userid;
	int			save_sec_context;
	int			save_nestlevel;
	IndexInfo  *indexInfo;
	volatile bool skipped_constraint = false;
	PGRUsage	ru0;

	pg_rusage_init(&ru0);

	/*
	 * Open and lock the parent heap relation.  ShareLock is sufficient since
	 * we only need to be sure no schema or data changes are going on.
	 */
	heapId = IndexGetRelation(indexId, false);
	heapRelation = heap_open(heapId, ShareLock);

	/*
	 * Switch to the table owner's userid, so that any index functions are run
	 * as that user.  Also lock down security-restricted operations and
	 * arrange to make GUC variable changes local to this command.
	 */
	GetUserIdAndSecContext(&save_userid, &save_sec_context);
	SetUserIdAndSecContext(heapRelation->rd_rel->relowner,
						   save_sec_context | SECURITY_RESTRICTED_OPERATION);
	save_nestlevel = NewGUCNestLevel();

	/*
	 * Open the target index relation and get an exclusive lock on it, to
	 * ensure that no one else is touching this particular index.
	 */
	iRel = index_open(indexId, AccessExclusiveLock);

	/*
	 * The case of reindexing partitioned tables and indexes is handled
	 * differently by upper layers, so this case shouldn't arise.
	 */
	if (iRel->rd_rel->relkind == RELKIND_PARTITIONED_INDEX)
		elog(ERROR, "unsupported relation kind for index \"%s\"",
			 RelationGetRelationName(iRel));

	/*
	 * Don't allow reindex on temp tables of other backends ... their local
	 * buffer manager is not going to cope.
	 */
	if (RELATION_IS_OTHER_TEMP(iRel))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot reindex temporary tables of other sessions")));

	/*
	 * Also check for active uses of the index in the current transaction; we
	 * don't want to reindex underneath an open indexscan.
	 */
	CheckTableNotInUse(iRel, "REINDEX INDEX");

	/*
	 * All predicate locks on the index are about to be made invalid. Promote
	 * them to relation locks on the heap.
	 */
	TransferPredicateLocksToHeapRelation(iRel);

	/* Fetch info needed for index_build */
	indexInfo = BuildIndexInfo(iRel);

	/* If requested, skip checking uniqueness/exclusion constraints */
	if (skip_constraint_checks)
	{
		if (indexInfo->ii_Unique || indexInfo->ii_ExclusionOps != NULL)
			skipped_constraint = true;
		indexInfo->ii_Unique = false;
		indexInfo->ii_ExclusionOps = NULL;
		indexInfo->ii_ExclusionProcs = NULL;
		indexInfo->ii_ExclusionStrats = NULL;
	}

	/* Suppress use of the target index while rebuilding it */
	SetReindexProcessing(heapId, indexId);

	/* Create a new physical relation for the index */
	RelationSetNewRelfilenode(iRel, persistence, InvalidTransactionId,
							  InvalidMultiXactId);

	/* Initialize the index and rebuild */
	/* Note: we do not need to re-establish pkey setting */
	index_build(heapRelation, iRel, indexInfo, false, true, true);

	/* Re-allow use of target index */
	ResetReindexProcessing();

	/*
	 * If the index is marked invalid/not-ready/dead (ie, it's from a failed
	 * CREATE INDEX CONCURRENTLY, or a DROP INDEX CONCURRENTLY failed midway),
	 * and we didn't skip a uniqueness check, we can now mark it valid.  This
	 * allows REINDEX to be used to clean up in such cases.
	 *
	 * We can also reset indcheckxmin, because we have now done a
	 * non-concurrent index build, *except* in the case where index_build
	 * found some still-broken HOT chains. If it did, and we don't have to
	 * change any of the other flags, we just leave indcheckxmin alone (note
	 * that index_build won't have changed it, because this is a reindex).
	 * This is okay and desirable because not updating the tuple leaves the
	 * index's usability horizon (recorded as the tuple's xmin value) the same
	 * as it was.
	 *
	 * But, if the index was invalid/not-ready/dead and there were broken HOT
	 * chains, we had better force indcheckxmin true, because the normal
	 * argument that the HOT chains couldn't conflict with the index is
	 * suspect for an invalid index.  (A conflict is definitely possible if
	 * the index was dead.  It probably shouldn't happen otherwise, but let's
	 * be conservative.)  In this case advancing the usability horizon is
	 * appropriate.
	 *
	 * Another reason for avoiding unnecessary updates here is that while
	 * reindexing pg_index itself, we must not try to update tuples in it.
	 * pg_index's indexes should always have these flags in their clean state,
	 * so that won't happen.
	 *
	 * If early pruning/vacuuming is enabled for the heap relation, the
	 * usability horizon must be advanced to the current transaction on every
	 * build or rebuild.  pg_index is OK in this regard because catalog tables
	 * are not subject to early cleanup.
	 */
	if (!skipped_constraint)
	{
		Relation	pg_index;
		HeapTuple	indexTuple;
		Form_pg_index indexForm;
		bool		index_bad;
		bool		early_pruning_enabled = EarlyPruningEnabled(heapRelation);

		pg_index = heap_open(IndexRelationId, RowExclusiveLock);

		indexTuple = SearchSysCacheCopy1(INDEXRELID,
										 ObjectIdGetDatum(indexId));
		if (!HeapTupleIsValid(indexTuple))
			elog(ERROR, "cache lookup failed for index %u", indexId);
		indexForm = (Form_pg_index) GETSTRUCT(indexTuple);

		index_bad = (!indexForm->indisvalid ||
					 !indexForm->indisready ||
					 !indexForm->indislive);
		if (index_bad ||
			(indexForm->indcheckxmin && !indexInfo->ii_BrokenHotChain) ||
			early_pruning_enabled)
		{
			if (!indexInfo->ii_BrokenHotChain && !early_pruning_enabled)
				indexForm->indcheckxmin = false;
			else if (index_bad || early_pruning_enabled)
				indexForm->indcheckxmin = true;
			indexForm->indisvalid = true;
			indexForm->indisready = true;
			indexForm->indislive = true;
			CatalogTupleUpdate(pg_index, &indexTuple->t_self, indexTuple);

			/*
			 * Invalidate the relcache for the table, so that after we commit
			 * all sessions will refresh the table's index list.  This ensures
			 * that if anyone misses seeing the pg_index row during this
			 * update, they'll refresh their list before attempting any update
			 * on the table.
			 */
			CacheInvalidateRelcache(heapRelation);
		}

		heap_close(pg_index, RowExclusiveLock);
	}

	/* Log what we did */
	if (options & REINDEXOPT_VERBOSE)
		ereport(INFO,
				(errmsg("index \"%s\" was reindexed",
						get_rel_name(indexId)),
				 errdetail_internal("%s",
									pg_rusage_show(&ru0))));

	/* Roll back any GUC changes executed by index functions */
	AtEOXact_GUC(false, save_nestlevel);

	/* Restore userid and security context */
	SetUserIdAndSecContext(save_userid, save_sec_context);

	/* Close rels, but keep locks */
	index_close(iRel, NoLock);
	heap_close(heapRelation, NoLock);
}

/*
 * reindex_relation - This routine is used to recreate all indexes
 * of a relation (and optionally its toast relation too, if any).
 *
 * "flags" is a bitmask that can include any combination of these bits:
 *
 * REINDEX_REL_PROCESS_TOAST: if true, process the toast table too (if any).
 *
 * REINDEX_REL_SUPPRESS_INDEX_USE: if true, the relation was just completely
 * rebuilt by an operation such as VACUUM FULL or CLUSTER, and therefore its
 * indexes are inconsistent with it.  This makes things tricky if the relation
 * is a system catalog that we might consult during the reindexing.  To deal
 * with that case, we mark all of the indexes as pending rebuild so that they
 * won't be trusted until rebuilt.  The caller is required to call us *without*
 * having made the rebuilt table visible by doing CommandCounterIncrement;
 * we'll do CCI after having collected the index list.  (This way we can still
 * use catalog indexes while collecting the list.)
 *
 * REINDEX_REL_CHECK_CONSTRAINTS: if true, recheck unique and exclusion
 * constraint conditions, else don't.  To avoid deadlocks, VACUUM FULL or
 * CLUSTER on a system catalog must omit this flag.  REINDEX should be used to
 * rebuild an index if constraint inconsistency is suspected.  For optimal
 * performance, other callers should include the flag only after transforming
 * the data in a manner that risks a change in constraint validity.
 *
 * REINDEX_REL_FORCE_INDEXES_UNLOGGED: if true, set the persistence of the
 * rebuilt indexes to unlogged.
 *
 * REINDEX_REL_FORCE_INDEXES_PERMANENT: if true, set the persistence of the
 * rebuilt indexes to permanent.
 *
 * Returns true if any indexes were rebuilt (including toast table's index
 * when relevant).  Note that a CommandCounterIncrement will occur after each
 * index rebuild.
 */
bool
reindex_relation(Oid relid, int flags, int options)
{
	Relation	rel;
	Oid			toast_relid;
	List	   *indexIds;
	char		persistence;
	bool		result;
	ListCell   *indexId;

	/*
	 * Open and lock the relation.  ShareLock is sufficient since we only need
	 * to prevent schema and data changes in it.  The lock level used here
	 * should match ReindexTable().
	 */
	rel = heap_open(relid, ShareLock);

	/*
	 * This may be useful when implemented someday; but that day is not today.
	 * For now, avoid erroring out when called in a multi-table context
	 * (REINDEX SCHEMA) and happen to come across a partitioned table.  The
	 * partitions may be reindexed on their own anyway.
	 */
	if (rel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE)
	{
		ereport(WARNING,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("REINDEX of partitioned tables is not yet implemented, skipping \"%s\"",
						RelationGetRelationName(rel))));
		heap_close(rel, ShareLock);
		return false;
	}

	toast_relid = rel->rd_rel->reltoastrelid;

	/*
	 * Get the list of index OIDs for this relation.  (We trust to the
	 * relcache to get this with a sequential scan if ignoring system
	 * indexes.)
	 */
	indexIds = RelationGetIndexList(rel);

	if (flags & REINDEX_REL_SUPPRESS_INDEX_USE)
	{
		/* Suppress use of all the indexes until they are rebuilt */
		SetReindexPending(indexIds);

		/*
		 * Make the new heap contents visible --- now things might be
		 * inconsistent!
		 */
		CommandCounterIncrement();
	}

	/*
	 * Compute persistence of indexes: same as that of owning rel, unless
	 * caller specified otherwise.
	 */
	if (flags & REINDEX_REL_FORCE_INDEXES_UNLOGGED)
		persistence = RELPERSISTENCE_UNLOGGED;
	else if (flags & REINDEX_REL_FORCE_INDEXES_PERMANENT)
		persistence = RELPERSISTENCE_PERMANENT;
	else
		persistence = rel->rd_rel->relpersistence;

	/* Reindex all the indexes. */
	foreach(indexId, indexIds)
	{
		Oid			indexOid = lfirst_oid(indexId);

		reindex_index(indexOid, !(flags & REINDEX_REL_CHECK_CONSTRAINTS),
					  persistence, options);

		CommandCounterIncrement();

		/* Index should no longer be in the pending list */
		Assert(!ReindexIsProcessingIndex(indexOid));
	}

	/*
	 * Close rel, but continue to hold the lock.
	 */
	heap_close(rel, NoLock);

	result = (indexIds != NIL);

	/*
	 * If the relation has a secondary toast rel, reindex that too while we
	 * still hold the lock on the master table.
	 */
	if ((flags & REINDEX_REL_PROCESS_TOAST) && OidIsValid(toast_relid))
		result |= reindex_relation(toast_relid, flags, options);

	return result;
}


/* ----------------------------------------------------------------
 *		System index reindexing support
 *
 * When we are busy reindexing a system index, this code provides support
 * for preventing catalog lookups from using that index.  We also make use
 * of this to catch attempted uses of user indexes during reindexing of
 * those indexes.  This information is propagated to parallel workers;
 * attempting to change it during a parallel operation is not permitted.
 * ----------------------------------------------------------------
 */

static Oid	currentlyReindexedHeap = InvalidOid;
static Oid	currentlyReindexedIndex = InvalidOid;
static List *pendingReindexedIndexes = NIL;
static int	reindexingNestLevel = 0;

/*
 * ReindexIsProcessingHeap
 *		True if heap specified by OID is currently being reindexed.
 */
bool
ReindexIsProcessingHeap(Oid heapOid)
{
	return heapOid == currentlyReindexedHeap;
}

/*
 * ReindexIsCurrentlyProcessingIndex
 *		True if index specified by OID is currently being reindexed.
 */
static bool
ReindexIsCurrentlyProcessingIndex(Oid indexOid)
{
	return indexOid == currentlyReindexedIndex;
}

/*
 * ReindexIsProcessingIndex
 *		True if index specified by OID is currently being reindexed,
 *		or should be treated as invalid because it is awaiting reindex.
 */
bool
ReindexIsProcessingIndex(Oid indexOid)
{
	return indexOid == currentlyReindexedIndex ||
		list_member_oid(pendingReindexedIndexes, indexOid);
}

/*
 * SetReindexProcessing
 *		Set flag that specified heap/index are being reindexed.
 */
static void
SetReindexProcessing(Oid heapOid, Oid indexOid)
{
	Assert(OidIsValid(heapOid) && OidIsValid(indexOid));
	/* Reindexing is not re-entrant. */
	if (OidIsValid(currentlyReindexedHeap))
		elog(ERROR, "cannot reindex while reindexing");
	currentlyReindexedHeap = heapOid;
	currentlyReindexedIndex = indexOid;
	/* Index is no longer "pending" reindex. */
	RemoveReindexPending(indexOid);
	/* This may have been set already, but in case it isn't, do so now. */
	reindexingNestLevel = GetCurrentTransactionNestLevel();
}

/*
 * ResetReindexProcessing
 *		Unset reindexing status.
 */
static void
ResetReindexProcessing(void)
{
	currentlyReindexedHeap = InvalidOid;
	currentlyReindexedIndex = InvalidOid;
	/* reindexingNestLevel remains set till end of (sub)transaction */
}

/*
 * SetReindexPending
 *		Mark the given indexes as pending reindex.
 *
 * NB: we assume that the current memory context stays valid throughout.
 */
static void
SetReindexPending(List *indexes)
{
	/* Reindexing is not re-entrant. */
	if (pendingReindexedIndexes)
		elog(ERROR, "cannot reindex while reindexing");
	if (IsInParallelMode())
		elog(ERROR, "cannot modify reindex state during a parallel operation");
	pendingReindexedIndexes = list_copy(indexes);
	reindexingNestLevel = GetCurrentTransactionNestLevel();
}

/*
 * RemoveReindexPending
 *		Remove the given index from the pending list.
 */
static void
RemoveReindexPending(Oid indexOid)
{
	if (IsInParallelMode())
		elog(ERROR, "cannot modify reindex state during a parallel operation");
	pendingReindexedIndexes = list_delete_oid(pendingReindexedIndexes,
											  indexOid);
}

/*
 * ResetReindexState
 *		Clear all reindexing state during (sub)transaction abort.
 */
void
ResetReindexState(int nestLevel)
{
	/*
	 * Because reindexing is not re-entrant, we don't need to cope with nested
	 * reindexing states.  We just need to avoid messing up the outer-level
	 * state in case a subtransaction fails within a REINDEX.  So checking the
	 * current nest level against that of the reindex operation is sufficient.
	 */
	if (reindexingNestLevel >= nestLevel)
	{
		currentlyReindexedHeap = InvalidOid;
		currentlyReindexedIndex = InvalidOid;

		/*
		 * We needn't try to release the contents of pendingReindexedIndexes;
		 * that list should be in a transaction-lifespan context, so it will
		 * go away automatically.
		 */
		pendingReindexedIndexes = NIL;

		reindexingNestLevel = 0;
	}
}

/*
 * EstimateReindexStateSpace
 *		Estimate space needed to pass reindex state to parallel workers.
 */
Size
EstimateReindexStateSpace(void)
{
	return offsetof(SerializedReindexState, pendingReindexedIndexes)
		+ mul_size(sizeof(Oid), list_length(pendingReindexedIndexes));
}

/*
 * SerializeReindexState
 *		Serialize reindex state for parallel workers.
 */
void
SerializeReindexState(Size maxsize, char *start_address)
{
	SerializedReindexState *sistate = (SerializedReindexState *) start_address;
	int			c = 0;
	ListCell   *lc;

	sistate->currentlyReindexedHeap = currentlyReindexedHeap;
	sistate->currentlyReindexedIndex = currentlyReindexedIndex;
	sistate->numPendingReindexedIndexes = list_length(pendingReindexedIndexes);
	foreach(lc, pendingReindexedIndexes)
		sistate->pendingReindexedIndexes[c++] = lfirst_oid(lc);
}

/*
 * RestoreReindexState
 *		Restore reindex state in a parallel worker.
 */
void
RestoreReindexState(void *reindexstate)
{
	SerializedReindexState *sistate = (SerializedReindexState *) reindexstate;
	int			c = 0;
	MemoryContext oldcontext;

	currentlyReindexedHeap = sistate->currentlyReindexedHeap;
	currentlyReindexedIndex = sistate->currentlyReindexedIndex;

	Assert(pendingReindexedIndexes == NIL);
	oldcontext = MemoryContextSwitchTo(TopMemoryContext);
	for (c = 0; c < sistate->numPendingReindexedIndexes; ++c)
		pendingReindexedIndexes =
			lappend_oid(pendingReindexedIndexes,
						sistate->pendingReindexedIndexes[c]);
	MemoryContextSwitchTo(oldcontext);

	/* Note the worker has its own transaction nesting level */
	reindexingNestLevel = GetCurrentTransactionNestLevel();
}

/*
 * POLAR:
 * Input a ctid of tuple, and return its root ctid.
 */
Datum
polar_get_root_ctid(HeapTuple tuple, Buffer buffer, ExprContext *econtext)
{
	BlockNumber max_block_count;
	BlockNumber rel_size;
	BlockNumber blkno;
	OffsetNumber offnum;
	OffsetNumber root_offnum;
	char *root_ctid = NULL;
	Datum result;
	OffsetNumber *cached_root_offsets = NULL;
	bool is_memory_alloc = false;

	Assert(tuple);
	blkno = ItemPointerGetBlockNumberNoCheck(&(tuple->t_self));
	offnum = ItemPointerGetOffsetNumberNoCheck(&(tuple->t_self));

	/*
	 * If the cached_blkno is invalid or the current blkno is a new BlockNumber,
	 * we should get the new cached_root_offsets.
	 */
	if (buffer == InvalidBuffer || econtext == NULL ||
		econtext->cached_blkno == InvalidBlockNumber || blkno != econtext->cached_blkno)
	{
		Page page;
		Relation rel;
		if (buffer == InvalidBuffer || econtext == NULL)
		{
			cached_root_offsets = palloc0(sizeof(OffsetNumber) * MaxHeapTuplesPerPage);
			is_memory_alloc = true;
			rel = heap_open(tuple->t_tableOid, AccessShareLock);
			if (polar_bulk_read_size > 0)
			{
				rel_size = RelationGetNumberOfBlocks(rel);
				if (rel_size <= blkno)
					elog(ERROR, "get root ctid, block number out of range");
				max_block_count = rel_size - blkno;
				buffer = polar_bulk_read_buffer_extended(rel, MAIN_FORKNUM, blkno, RBM_NORMAL, NULL, max_block_count);
			}
			else
				buffer = ReadBuffer(rel, blkno);
			page = BufferGetPage(buffer);
			LockBuffer(buffer, BUFFER_LOCK_SHARE);
			heap_get_root_tuples(page, cached_root_offsets);
			UnlockReleaseBuffer(buffer);
			heap_close(rel, AccessShareLock);
		}
		else
		{
			Assert(buffer != InvalidBuffer && econtext != NULL);
			cached_root_offsets = econtext->cached_root_offsets;
			/* Buffer has already be locked by caller. */
			page = BufferGetPage(buffer);
			heap_get_root_tuples(page, cached_root_offsets);
		}
		econtext->cached_blkno = blkno;
	}

	if (HeapTupleIsHeapOnly(tuple))
	{
		/*
		 * For a heap-only tuple, pretend its TID is that of the root. See
		 * src/backend/access/heap/README.HOT for discussion.
		 */
		root_offnum = econtext->cached_root_offsets[offnum - 1];
		if (!OffsetNumberIsValid(root_offnum))
			ereport(ERROR,
					(errcode(ERRCODE_DATA_CORRUPTED),
						errmsg_internal("failed to find parent tuple for heap-only tuple at (%u,%u) in table \"%s\"",
										ItemPointerGetBlockNumber(&tuple->t_self),
										offnum,
										get_rel_name(tuple->t_tableOid))));

	}
	else
		root_offnum = offnum;

	root_ctid = psprintf("(%u, %u)", blkno, root_offnum);
	result = DirectFunctionCall1(tidin, CStringGetDatum(root_ctid));

	if (is_memory_alloc)
		pfree(cached_root_offsets);

	pfree(root_ctid);
	PG_RETURN_DATUM(result);
}

/*
 * POLAR: This has much code in common with validate_index_heapscan. Second
 * table scan for concurrent index build. We don't support expressions and
 * predicates index in pxbtbuild, so we drop some operations about expressions
 * and predicates index.
 */
static void
polar_px_validate_index_heapscan(Relation heapRelation,
						Relation indexRelation,
						IndexInfo *indexInfo,
						Snapshot snapshot,
						v_i_state *state)
{
	bool			in_index[MaxHeapTuplesPerPage];

	/* state variables for the merge */
	ItemPointer		indexcursor = NULL;
	ItemPointerData decoded;
	bool			tuplesort_empty = false;

	/* px: build px sql init */
	StringInfo	sql;
	SPIPlanPtr	plan;
	Portal		portal;
	bool 		old_enable_px = polar_enable_px;
	bool 		old_px_plancache = px_enable_plan_cache;
	bool 		old_px_index = px_optimizer_enable_indexscan;
	bool 		old_px_check_workers = px_enable_check_workers;
	bool 		old_px_tx = px_enable_transaction;
	bool 		old_px_replay_wait = px_enable_replay_wait;

	/*
	 * sanity checks
	 */
	Assert(OidIsValid(indexRelation->rd_rel->relam));

	/* px: generate sql */
	sql = makeStringInfo();
	{
		int i;
		StringInfo attrs = makeStringInfo();
		TupleDesc tupdes = RelationGetDescr(indexRelation);
		int natts = tupdes->natts;
		Form_pg_attribute lastattr = TupleDescAttr(tupdes, natts-1);
		Assert(natts > 0);

		for (i = 0; i < natts - 1; i++)
		{
			Form_pg_attribute att = TupleDescAttr(tupdes, i);
			appendStringInfo(attrs, "%s, ", NameStr(att->attname));
		}
		appendStringInfo(attrs, "%s", NameStr(lastattr->attname));

		/* should be order by ctid. PX scan table can not guarantee in sequence */
		appendStringInfo(sql, "select _root_ctid, %s from %s order by ctid", 
							attrs->data, RelationGetRelationName(heapRelation));
	}

	/* invoke SPI interface */
	polar_enable_px = true;
	px_optimizer_enable_indexscan = false;
	px_enable_check_workers = false;
	px_enable_plan_cache = false;
	px_enable_transaction = true;
	px_enable_replay_wait = true;

	SPI_connect();
	if ((plan = SPI_prepare_px(sql->data, 0, NULL)) == NULL)
		elog(ERROR, "SPI_prepare(\"%s\") failed", sql->data);
	if ((portal = SPI_cursor_open(NULL, plan, NULL, NULL, true)) == NULL)
		elog(ERROR, "SPI_cursor_open(\"%s\") failed", sql->data);

	/* reset SPI interface */
	polar_enable_px = old_enable_px;
	px_optimizer_enable_indexscan = old_px_index;
	px_enable_check_workers = old_px_check_workers;
	px_enable_plan_cache = old_px_plancache;
	px_enable_transaction = old_px_tx;
	px_enable_replay_wait = old_px_replay_wait;

	/* fetch heap tuple */
	SPI_cursor_fetch(portal, true, px_btbuild_batch_size);
	while (SPI_processed > 0)
	{
		uint64 i;
		for (i = 0; i < SPI_processed; i++)
		{
			BlockNumber	root_blkno = InvalidBlockNumber;
			OffsetNumber root_offnum;
			Datum values[INDEX_MAX_KEYS + 1];
			bool nulls[INDEX_MAX_KEYS + 1];
			ItemPointer heapcursor;
			ItemPointerData rootTuple;
			HeapTuple tup = SPI_tuptable->vals[i];

			heap_deform_tuple(tup, SPI_tuptable->tupdesc, values, nulls);

			/* ctid for current heap tuple */
			heapcursor = (ItemPointer)values[0];
			rootTuple = *heapcursor;
			root_blkno = ItemPointerGetBlockNumber(heapcursor);
			root_offnum = ItemPointerGetOffsetNumber(heapcursor);
			CHECK_FOR_INTERRUPTS();

			/*
			* "merge" by skipping through the index tuples until we find or pass
			* the current root tuple.
			*/
			while (!tuplesort_empty &&
				(!indexcursor ||
					ItemPointerCompare(indexcursor, &rootTuple) < 0))
			{
				Datum		ts_val;
				bool		ts_isnull;

				if (indexcursor)
				{
					/*
					* Remember index items seen earlier on the current heap page
					*/
					if (ItemPointerGetBlockNumber(indexcursor) == root_blkno)
						in_index[ItemPointerGetOffsetNumber(indexcursor) - 1] = true;
				}

				tuplesort_empty = !tuplesort_getdatum(state->tuplesort, true,
													&ts_val, &ts_isnull, NULL);
				Assert(tuplesort_empty || !ts_isnull);
				if (!tuplesort_empty)
				{
					itemptr_decode(&decoded, DatumGetInt64(ts_val));
					indexcursor = &decoded;

					/* If int8 is pass-by-ref, free (encoded) TID Datum memory */
	#ifndef USE_FLOAT8_BYVAL
					pfree(DatumGetPointer(ts_val));
	#endif
				}
				else
				{
					/* Be tidy */
					indexcursor = NULL;
				}
			}

			/*
			* If the tuplesort has overshot *and* we didn't see a match earlier,
			* then this tuple is missing from the index, so insert it.
			*/
			if ((tuplesort_empty ||
				ItemPointerCompare(indexcursor, &rootTuple) > 0) &&
				!in_index[root_offnum - 1])
			{
				index_insert(indexRelation,
								values + 1,
								nulls + 1,
								&rootTuple,
								heapRelation,
								indexInfo->ii_Unique ?
								UNIQUE_CHECK_YES : UNIQUE_CHECK_NO,
								indexInfo);

				state->tups_inserted += 1;
			}
		}
		SPI_freetuptable(SPI_tuptable);
		SPI_cursor_fetch(portal, true, px_btbuild_batch_size);
	}

	SPI_cursor_close(portal);
	SPI_freeplan(plan);
	SPI_finish();

	/* These may have been pointing to the now-gone estate */
	indexInfo->ii_ExpressionsState = NIL;
	indexInfo->ii_PredicateState = NULL;
}